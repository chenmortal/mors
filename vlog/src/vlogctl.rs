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
use mors_wal::LogFile;

use crate::{discard::Discard, error::MorsVlogError};
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
    builder: VlogCtlBuilder<K>,
}
impl<K: Kms> VlogCtlTrait<K> for VlogCtl<K> {
    type ErrorType = MorsVlogError;

    type Discard = Discard;

    type VlogCtlBuilder = VlogCtlBuilder<K>;
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
}
#[derive(Debug, Clone)]
pub struct VlogCtlBuilder<K: Kms> {
    read_only: bool,
    vlog_dir: PathBuf,
    vlog_file_size: usize,
    vlog_max_entries: usize,

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
        }
    }
}
impl<K: Kms> VlogCtlBuilderTrait<VlogCtl<K>, K> for VlogCtlBuilder<K> {
    async fn build(
        &self,
        kms: K,
    ) -> std::result::Result<VlogCtl<K>, VlogError> {
        todo!()
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
        todo!()
    }

    fn dir(&self) -> &PathBuf {
        todo!()
    }
}
impl<K: Kms> WithReadOnly for VlogCtlBuilder<K> {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        todo!()
    }

    fn read_only(&self) -> bool {
        todo!()
    }
}
impl<K: Kms> VlogCtlBuilder<K> {
    pub fn build_impl(&self, kms: K) -> Result<()> {
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
            }),
        };

        if self.read_only {
            return Ok(());
        }
        if id_len == 0 {
            vlog_ctl.create_new()?;
            return Ok(());
        }

        let latest = vlog_ctl.latest_logfile()?;
        latest.write()?;
        // latest.write().map_err(|e|MorsVlogError::PosionError(()));

        Ok(())
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
    // BTreeMap::new();
}
