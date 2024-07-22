use std::{
    collections::BTreeMap,
    marker::PhantomData,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use log::info;
use memmap2::Advice;
use mors_common::{
    file_id::{FileId, VlogId},
    mmap::MmapFileBuilder,
};
use mors_traits::{
    default::{WithDir, WithReadOnly, DEFAULT_DIR},
    kms::Kms,
    vlog::{VlogCtlBuilderTrait, VlogCtlTrait, VlogError},
};
use mors_wal::{read::LogFileIter, LogFile};

use crate::{
    discard::Discard,
    error::MorsVlogError,
    threshold::{VlogThreshold, VlogThresholdConfig},
};
type Result<T> = std::result::Result<T, MorsVlogError>;
type LogFileWrapper<K> = Arc<RwLock<LogFile<VlogId, K>>>;
pub struct VlogCtl<K: Kms> {
    inner: Arc<VlogCtlInner<K>>,
}
struct VlogCtlInner<K: Kms> {
    id_logfile: RwLock<BTreeMap<VlogId, LogFileWrapper<K>>>,
    max_id: RwLock<VlogId>,
    writeable_offset: AtomicUsize,
    kms: K,
    vlog_threshold: VlogThreshold,
    builder: VlogCtlBuilder<K>,
}
impl<K: Kms> VlogCtlTrait<K> for VlogCtl<K> {
    type ErrorType = MorsVlogError;

    type Discard = Discard;

    type VlogCtlBuilder = VlogCtlBuilder<K>;

    fn writeable_offset(&self) -> usize {
        self.inner.writeable_offset.load(Ordering::SeqCst)
    }
    fn vlog_file_size(&self) -> usize {
        self.inner.builder.vlog_file_size
    }

    const MAX_VLOG_SIZE: usize = 22;

    const MAX_VLOG_FILE_SIZE: usize = u32::MAX as usize;
}
impl<K: Kms> VlogCtl<K> {
    pub fn latest_logfile(&self) -> Result<Arc<RwLock<LogFile<VlogId, K>>>> {
        let id_r = self.inner.max_id.read()?;

        let id_logfile = self.inner.id_logfile.read()?;
        let id = *id_r;
        if let Some(log) = id_logfile.get(&id) {
            return Ok(log.clone());
        };
        Err(MorsVlogError::LogNotFound(id))
    }
    fn create_new(&self) -> Result<LogFileWrapper<K>> {
        let mut max_id_w = self.inner.max_id.write()?;

        let id = *max_id_w + 1;

        let log = self
            .inner
            .builder
            .open_logfile(id, self.inner.kms.clone())?;

        let mut id_logfile = self.inner.id_logfile.write()?;

        let log = Arc::new(RwLock::new(log));
        id_logfile.insert(id, log.clone());
        debug_assert!(id > *max_id_w);
        *max_id_w = id;
        self.inner
            .writeable_offset
            .store(LogFile::<VlogId, K>::LOG_HEADER_SIZE, Ordering::SeqCst);
        Ok(log)
    }
    pub fn woffset(&self) -> usize {
        self.inner.writeable_offset.load(Ordering::SeqCst)
    }
}
#[derive(Debug, Clone)]
pub struct VlogCtlBuilder<K: Kms> {
    read_only: bool,
    vlog_dir: PathBuf,
    vlog_file_size: usize,
    vlog_max_entries: usize,
    vlog_threshold: VlogThresholdConfig,
    kms: PhantomData<K>,
}
impl<K: Kms> Default for VlogCtlBuilder<K> {
    fn default() -> Self {
        Self {
            read_only: false,
            vlog_dir: PathBuf::from(DEFAULT_DIR),
            vlog_file_size: 1 << 30,
            vlog_max_entries: 1_000_000,
            kms: PhantomData,
            vlog_threshold: VlogThresholdConfig::default(),
        }
    }
}
impl<K: Kms> VlogCtlBuilderTrait<VlogCtl<K>, K> for VlogCtlBuilder<K> {
    async fn build(
        &mut self,
        kms: K,
    ) -> std::result::Result<VlogCtl<K>, VlogError> {
        Ok(self.build_impl(kms)?)
    }

    fn build_discard(
        &self,
    ) -> std::result::Result<<VlogCtl<K> as VlogCtlTrait<K>>::Discard, VlogError>
    {
        Discard::new(&self.vlog_dir).map_err(|e| e.into())
    }
}
impl<K: Kms> WithDir for VlogCtlBuilder<K> {
    fn set_dir(&mut self, dir: PathBuf) -> &mut Self {
        self.vlog_dir = dir;
        self
    }

    fn dir(&self) -> &PathBuf {
        &self.vlog_dir
    }
}
impl<K: Kms> WithReadOnly for VlogCtlBuilder<K> {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    fn read_only(&self) -> bool {
        self.read_only
    }
}
impl<K: Kms> VlogCtlBuilder<K> {
    pub fn build_impl(&mut self, kms: K) -> Result<VlogCtl<K>> {
        self.vlog_threshold.check_threshold_config()?;
        let vlog_threshold = VlogThreshold::new(self.vlog_threshold);
        let mut id_logfile = BTreeMap::new();
        let ids = VlogId::parse_set_from_dir(&self.vlog_dir);
        let mut max_id: VlogId = 0.into();
        for id in ids {
            let log = self.open_logfile(id, kms.clone())?;
            if log.is_empty() {
                info!("Empty log file: {:?}", &id.join_dir(&self.vlog_dir));
                log.delete()?;
            }
            id_logfile.insert(id, Arc::new(RwLock::new(log)));
            max_id = max_id.max(id);
        }

        let id_len = id_logfile.len();
        let vlog_ctl = VlogCtl {
            inner: Arc::new(VlogCtlInner {
                id_logfile: RwLock::new(id_logfile),
                builder: self.clone(),
                kms,
                max_id: RwLock::new(max_id),
                writeable_offset: AtomicUsize::new(0),
                vlog_threshold,
            }),
        };

        if self.read_only {
            return Ok(vlog_ctl);
        }
        if id_len == 0 {
            vlog_ctl.create_new()?;
            return Ok(vlog_ctl);
        }

        let latest = vlog_ctl.latest_logfile()?;
        let mut latest_w = latest.write()?;
        let mut logfile_iter =
            LogFileIter::new(&latest_w, LogFile::<VlogId, K>::LOG_HEADER_SIZE);
        loop {
            if logfile_iter.next_entry()?.is_none() {
                break;
            };
        }
        let end_offset = logfile_iter.valid_end_offset();
        latest_w.truncate(end_offset)?;
        vlog_ctl.create_new()?;
        Ok(vlog_ctl)
    }
    fn open_logfile(&self, id: VlogId, kms: K) -> Result<LogFile<VlogId, K>> {
        let path = id.join_dir(&self.vlog_dir);
        let mut builder = MmapFileBuilder::new();
        builder
            .read(true)
            .write(!self.read_only)
            .create(!self.read_only);
        builder.advice(Advice::Sequential);
        let log = LogFile::open(
            id,
            &path,
            2 * self.vlog_file_size as u64,
            builder,
            kms,
        )?;
        Ok(log)
    }
}
