use std::{
    collections::BTreeMap,
    marker::PhantomData,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
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
pub struct VlogCtl<K: Kms> {
    inner: Arc<VlogCtlInner<K>>,
}
struct VlogCtlInner<K: Kms> {
    id_logfile: RwLock<BTreeMap<VlogId, Arc<RwLock<LogFile<VlogId, K>>>>>,
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
        let id = self.inner.builder.max_id.load(Ordering::Relaxed);
        let id_logfile = self
            .inner
            .id_logfile
            .read()
            .map_err(|e| MorsVlogError::PosionError(e.to_string()))?;
        if let Some(log) = id_logfile.get(&id.into()) {
            return Ok(log.clone());
        };
        Err(MorsVlogError::LogNotFound(id.into()))
    }
}
#[derive(Debug, Clone)]
pub struct VlogCtlBuilder<K: Kms> {
    read_only: bool,
    vlog_dir: PathBuf,
    vlog_file_size: usize,
    vlog_max_entries: usize,
    max_id: Arc<AtomicU32>,
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
            max_id: Arc::new(AtomicU32::new(0)),
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

        for id in ids {
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
                kms.clone(),
            )?;
            if log.is_empty() {
                info!("Empty log file: {:?}", &path);
                log.delete()?;
            }
            id_logfile.insert(id, Arc::new(RwLock::new(log)));
            self.max_id.fetch_max(id.into(), Ordering::SeqCst);
        }

        let vlog_ctl = VlogCtl {
            inner: Arc::new(VlogCtlInner {
                id_logfile: RwLock::new(id_logfile),
                builder: self.clone(),
                kms,
            }),
        };
        // let vlog_ctl =
        if self.read_only {
            return Ok(());
        }
        Ok(())
    }
    fn create_new(){
        
    }
    // BTreeMap::new();
}
