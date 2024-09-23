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
// use memmap2::Advice;
use mors_common::{
    file_id::{FileId, VlogId},
    // mmap::MmapFileBuilder,
};
use mors_traits::{
    default::{WithDir, WithReadOnly, DEFAULT_DIR},
    file::{StorageBuilderTrait, StorageTrait},
    kms::Kms,
    vlog::{VlogCtlBuilderTrait, VlogCtlTrait, VlogError},
};
use mors_wal::LogFile;

use crate::Result;
use crate::{
    discard::Discard,
    error::MorsVlogError,
    threshold::{VlogThreshold, VlogThresholdConfig},
};
type LogFileWrapper<K, S> = Arc<LogFile<VlogId, K, S>>;
pub struct VlogCtl<K: Kms, S: StorageTrait> {
    inner: Arc<VlogCtlInner<K, S>>,
}
struct VlogCtlInner<K: Kms, S: StorageTrait> {
    id_logfile: RwLock<BTreeMap<VlogId, LogFileWrapper<K, S>>>,
    max_id: RwLock<VlogId>,
    writeable_offset: AtomicUsize,
    kms: K,
    vlog_threshold: VlogThreshold,
    builder: VlogCtlBuilder<K>,
}
impl<K: Kms, S: StorageTrait> VlogCtlTrait<K> for VlogCtl<K, S> {
    type ErrorType = MorsVlogError;

    type Discard = Discard;

    type VlogCtlBuilder = VlogCtlBuilder<K>;

    fn writeable_offset(&self) -> usize {
        self.inner.writeable_offset.load(Ordering::SeqCst)
    }
    fn vlog_file_size(&self) -> usize {
        self.inner.builder.vlog_file_size
    }
    fn value_threshold(&self) -> usize {
        self.inner.vlog_threshold.value_threshold()
    }

    const MAX_VLOG_SIZE: usize = 22;

    const MAX_VLOG_FILE_SIZE: usize = u32::MAX as usize;

    async fn write<'a>(
        &self,
        iter_mut: Vec<
            std::slice::IterMut<
                'a,
                (mors_common::kv::Entry, mors_common::kv::ValuePointer),
            >,
        >,
    ) -> std::result::Result<(), VlogError> {
        Ok(self.write_impl(iter_mut).await?)
    }
}
impl<K: Kms, S: StorageTrait> VlogCtlInner<K, S> {
    fn latest_logfile(&self) -> Result<LogFileWrapper<K, S>> {
        let id_r = self.max_id.read()?;

        let id_logfile = self.id_logfile.read()?;
        let id = *id_r;
        if let Some(log) = id_logfile.get(&id) {
            return Ok(log.clone());
        };
        Err(MorsVlogError::LogNotFound(id))
    }
}
impl<K: Kms, S: StorageTrait> VlogCtl<K, S> {
    pub fn latest_logfile(&self) -> Result<LogFileWrapper<K, S>> {
        self.inner.latest_logfile()
    }
    pub(crate) fn create_new(&self) -> Result<LogFileWrapper<K, S>> {
        let mut max_id_w = self.inner.max_id.write()?;

        let id = *max_id_w + 1;

        let log = self
            .inner
            .builder
            .open_logfile(id, self.inner.kms.clone())?;

        let mut id_logfile = self.inner.id_logfile.write()?;

        let log = Arc::new(log);
        id_logfile.insert(id, log.clone());
        debug_assert!(id > *max_id_w);
        *max_id_w = id;
        self.inner
            .writeable_offset
            .store(LogFile::<VlogId, K, S>::LOG_HEADER_SIZE, Ordering::SeqCst);
        Ok(log)
    }
    pub fn woffset(&self) -> usize {
        self.inner.writeable_offset.load(Ordering::SeqCst)
    }
    pub fn woffset_fetch_add(&self, offset: usize) -> usize {
        self.inner
            .writeable_offset
            .fetch_add(offset, Ordering::SeqCst)
    }

    pub(crate) fn threshold(&self) -> &VlogThreshold {
        &self.inner.vlog_threshold
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
impl<K: Kms, S: StorageTrait> VlogCtlBuilderTrait<VlogCtl<K, S>, K>
    for VlogCtlBuilder<K>
{
    async fn build(
        &mut self,
        kms: K,
    ) -> std::result::Result<VlogCtl<K, S>, VlogError> {
        Ok(self.build_impl(kms)?)
    }

    fn build_discard(
        &self,
    ) -> std::result::Result<
        <VlogCtl<K, S> as VlogCtlTrait<K>>::Discard,
        VlogError,
    > {
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
    pub fn build_impl<S: StorageTrait>(
        &mut self,
        kms: K,
    ) -> Result<VlogCtl<K, S>> {
        self.vlog_threshold.check_threshold_config()?;
        let vlog_threshold = VlogThreshold::new(self.vlog_threshold);
        let mut id_logfile = BTreeMap::new();
        let ids = VlogId::parse_set_from_dir(&self.vlog_dir);

        for id in ids {
            let log = self.open_logfile(id, kms.clone())?;
            if log.is_empty() {
                info!("Empty log file: {:?}", &id.join_dir(&self.vlog_dir));
                log.delete()?;
            } else {
                id_logfile.insert(id, Arc::new(log));
            }
        }
        let max_id = id_logfile
            .iter()
            .map(|x| x.0)
            .max()
            .copied()
            .unwrap_or_default();

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

        vlog_ctl.create_new()?;
        Ok(vlog_ctl)
    }
    fn open_logfile<S: StorageTrait>(
        &self,
        id: VlogId,
        kms: K,
    ) -> Result<LogFile<VlogId, K, S>> {
        let path = id.join_dir(&self.vlog_dir);
        let mut builder = S::StorageBuilder::default();

        builder
            .read(true)
            .write(!self.read_only)
            .create(!self.read_only);

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
// pub(crate) fn reset_valid_len<F: FileId, K: Kms, S: StorageTrait>(
//     log: &mut LogFile<F, K, S>,
// ) -> Result<()> {
//     let mut logfile_iter =
//         LogFileIter::new(log, LogFile::<VlogId, K, S>::LOG_HEADER_SIZE);
//     loop {
//         if logfile_iter.next_entry()?.is_none() {
//             break;
//         };
//     }
//     let valid_len = logfile_iter.valid_end_offset();
//     log.set_valid_len(valid_len as u64);
//     Ok(())
// }
impl<K: Kms, S: StorageTrait> Drop for VlogCtlInner<K, S> {
    fn drop(&mut self) {
        // match self.latest_logfile() {
        //     Ok(latest) => {
        //         if let Err(e) = reset_valid_len(&latest) {
        //             eprintln!("Error: {:?}", e);
        //         };
        //     }
        //     Err(e) => {
        //         eprintln!("Error: {:?}", e);
        //     }
        // }
    }
}
