use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::info;
use mors_common::{
    closer::{Closer, Throttle},
    file_id::SSTableId,
    ts::TxnTs,
};
use mors_traits::{
    default::{WithDir, WithReadOnly, DEFAULT_DIR},
    kms::Kms,
    levelctl::{Level, LevelCtlBuilderTrait, LevelCtlError, LevelCtlTrait},
    sstable::{TableBuilderTrait, TableTrait},
};

type Result<T> = std::result::Result<T, MorsLevelCtlError>;

use tokio::{select, task::JoinHandle};

use crate::{
    compaction::status::CompactStatus,
    error::MorsLevelCtlError,
    handler::LevelHandler,
    manifest::{Manifest, ManifestBuilder},
};
#[derive(Clone)]
pub struct LevelCtl<T: TableTrait<K::Cipher>, K: Kms> {
    inner: Arc<LevelCtlInner<T, K>>,
}
pub struct LevelCtlInner<T: TableTrait<K::Cipher>, K: Kms> {
    manifest: Manifest,
    table_builder: T::TableBuilder,
    handlers: Vec<LevelHandler<T, K>>,
    next_id: Arc<AtomicU32>,
    level0_stalls_ms: AtomicU64,
    level0_stalls: AtomicU64,
    max_level: Level,
    compact_status: CompactStatus,
    config: LevelCtlConfig,
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtlTrait<T, K> for LevelCtl<T, K> {
    type ErrorType = MorsLevelCtlError;

    type LevelCtlBuilder = LevelCtlBuilder<T, K>;

    fn max_version(&self) -> TxnTs {
        self.inner
            .handlers
            .iter()
            .map(|h| h.max_version())
            .max()
            .unwrap_or_default()
    }

    fn table_builder(&self) -> &<T as TableTrait<K::Cipher>>::TableBuilder {
        &self.inner.table_builder
    }
    fn next_id(&self) -> Arc<AtomicU32> {
        self.inner.next_id.clone()
    }

    async fn push_level0(
        &self,
        table: T,
    ) -> std::result::Result<(), LevelCtlError> {
        Ok(self.push_level0_impl(table).await?)
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.inner.manifest
    }
    pub(crate) fn config(&self) -> &LevelCtlConfig {
        &self.inner.config
    }
    pub(crate) fn compact_status(&self) -> &CompactStatus {
        &self.inner.compact_status
    }
    pub(crate) fn level0_stalls_ms(&self) -> &AtomicU64 {
        &self.inner.level0_stalls_ms
    }
    pub(crate) fn handler(
        &self,
        level: Level,
    ) -> Option<&LevelHandler<T, K>> {
        if level > self.inner.max_level {
            return None;
        }
        let handler = &self.inner.handlers[level.to_usize()];
        debug_assert_eq!(handler.level(), &level);
        Some(handler)
    }
    pub(crate) fn max_level(&self) -> Level {
        self.inner.max_level
    }
    pub(crate) fn handlers_len(&self) -> usize {
        let len = self.inner.handlers.len();
        debug_assert!(self.max_level().to_usize() + 1 == len);
        len
    }
}
#[derive(Debug, Clone, Copy)]
pub(crate) struct LevelCtlConfig {
    max_level: Level,
    level0_num_tables_stall: usize,
    num_compactors: usize,
    levelmax2max_compaction: bool,
    base_level_size: usize,
    level_size_multiplier: usize,
    table_size_multiplier: usize,
    level0_size: usize,
    level0_tables_len: usize,
}
impl LevelCtlConfig {
    pub(crate) fn max_level(&self) -> Level {
        self.max_level
    }
    pub(crate) fn level0_num_tables_stall(&self) -> usize {
        self.level0_num_tables_stall
    }
    pub(crate) fn num_compactors(&self) -> usize {
        self.num_compactors
    }
    pub(crate) fn levelmax2max_compaction(&self) -> bool {
        self.levelmax2max_compaction
    }
    pub(crate) fn base_level_size(&self) -> usize {
        self.base_level_size
    }
    pub(crate) fn level_size_multiplier(&self) -> usize {
        self.level_size_multiplier
    }
    pub(crate) fn table_size_multiplier(&self) -> usize {
        self.table_size_multiplier
    }
    pub(crate) fn level0_size(&self) -> usize {
        self.level0_size
    }
    pub(crate) fn level0_tables_len(&self) -> usize {
        self.level0_tables_len
    }
}
impl Default for LevelCtlConfig {
    fn default() -> Self {
        Self {
            max_level: 6_u8.into(),
            level0_num_tables_stall: 15,
            num_compactors: 4,
            levelmax2max_compaction: false,
            base_level_size: 10 << 20, //10 MB
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            level0_size: 64 << 20,
            level0_tables_len: 5,
        }
    }
}
pub struct LevelCtlBuilder<T: TableTrait<K::Cipher>, K: Kms> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    cache: Option<T::Cache>,
    config: LevelCtlConfig,
    dir: PathBuf,
    read_only: bool,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelCtlBuilder<T, K> {
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
            cache: None,
            config: LevelCtlConfig::default(),
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> WithDir for LevelCtlBuilder<T, K> {
    fn set_dir(&mut self, dir: PathBuf) -> &mut Self {
        self.dir = dir;
        self.manifest.set_dir(self.dir.clone());
        self.table.set_dir(self.dir.clone());
        self
    }

    fn dir(&self) -> &PathBuf {
        &self.dir
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> WithReadOnly for LevelCtlBuilder<T, K> {
    fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    fn read_only(&self) -> bool {
        self.read_only
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms>
    LevelCtlBuilderTrait<LevelCtl<T, K>, T, K> for LevelCtlBuilder<T, K>
{
    async fn build(
        &self,
        kms: K,
    ) -> std::result::Result<LevelCtl<T, K>, LevelCtlError> {
        Ok(self.build_impl(kms).await?)
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtlBuilder<T, K> {
    // set max_level,notice [0..max_level] is valid level
    pub fn set_max_level(&mut self, max_level: Level) -> &mut Self {
        self.config.max_level = max_level;
        self
    }

    pub fn set_level0_num_tables_stall(
        &mut self,
        level0_num_tables_stall: usize,
    ) -> &mut Self {
        self.config.level0_num_tables_stall = level0_num_tables_stall;
        self
    }
    pub fn set_num_compactors(&mut self, num_compactors: usize) -> &mut Self {
        self.config.num_compactors = num_compactors;
        self
    }

    async fn build_impl(&self, kms: K) -> Result<LevelCtl<T, K>> {
        let compact_status =
            CompactStatus::new(self.config.max_level.to_usize());
        let manifest = self.manifest.build()?;

        let (max_id, handlers) =
            self.open_tables_by_manifest(manifest.clone(), kms).await?;

        let next_id = Arc::new(AtomicU32::new(1 + Into::<u32>::into(max_id)));

        let ctl = LevelCtlInner {
            manifest,
            handlers,
            next_id,
            level0_stalls_ms: Default::default(),
            compact_status,
            table_builder: self.table.clone(),
            config: self.config,
            level0_stalls: Default::default(),
            max_level: self.config.max_level,
        };
        Ok(LevelCtl {
            inner: Arc::new(ctl),
        })
    }

    async fn open_tables_by_manifest(
        &self,
        manifest: Manifest,
        kms: K,
    ) -> Result<(SSTableId, Vec<LevelHandler<T, K>>)> {
        manifest.revert(&self.dir).await?;

        let num_opened = Arc::new(AtomicUsize::new(0));
        // let table_len = manifest.table_len();
        let manifest_lock = manifest.lock().await;
        let tables = manifest_lock.tables();

        let watch_closer =
            Self::watch_num_opened(num_opened.clone(), tables.len());

        let mut max_id: SSTableId = 0.into();
        let mut throttle = Throttle::<MorsLevelCtlError>::new(3);

        let mut tasks: HashMap<Level, Vec<JoinHandle<Option<T>>>> =
            HashMap::new();

        for (id, table) in tables.iter() {
            let permit = throttle.acquire().await?;
            let num_opened_clone = num_opened.clone();
            max_id = max_id.max(*id);

            let cipher_id = table.key_id();

            let mut table_builder = self.table.clone();
            if let Some(c) = self.cache.as_ref() {
                table_builder.set_cache(c.clone());
            }
            table_builder.set_compression(table.compress());
            table_builder.set_dir(self.dir.clone());
            let kms_clone = kms.clone();
            let table_id = *id;
            let future = async move {
                let cipher = cipher_id
                    .map(|id| kms_clone.get_cipher(id))
                    .transpose()?
                    .flatten();
                let table = table_builder.open(table_id, cipher).await?;
                Ok::<Option<T>, MorsLevelCtlError>(table)
            };

            let task = tokio::spawn(async move {
                let table = permit.do_future(future).await;
                num_opened_clone.fetch_add(1, Ordering::SeqCst);
                table.and_then(|x| x)
            });
            tasks
                .entry(table.level().min(self.config.max_level))
                .or_default()
                .push(task);
        }
        drop(manifest_lock);
        throttle.finish().await?;

        watch_closer.cancel();
        watch_closer.wait().await?;

        let mut handlers = Vec::new();
        for level in 0..(self.config.max_level.to_u8() + 1) {
            let level: Level = level.into();
            match tasks.remove(&level) {
                Some(task_vec) => {
                    let mut tables = Vec::with_capacity(task_vec.len());
                    for handle in task_vec {
                        if let Some(t) = handle.await? {
                            tables.push(t);
                        };
                    }
                    let handler = LevelHandler::new(level, tables);
                    handler.validate()?;
                    handlers.push(handler);
                }
                None => {
                    handlers.push(LevelHandler::new(level, Vec::new()));
                }
            }
        }
        Ok((max_id, handlers))
    }
    fn watch_num_opened(
        num_opened: Arc<AtomicUsize>,
        table_len: usize,
    ) -> Closer {
        use tokio::time::interval;
        use tokio::time::Instant;

        let start = Instant::now();
        let closer = Closer::new("levelctl init watch_num_opened");
        let closer_clone = closer.clone();

        closer.set_joinhandle(tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(3));
            loop {
                select! {
                    i = tick.tick() => {
                        info!("{} tables opened out of {} in {} ms",
                            num_opened.load(Ordering::Relaxed),
                            table_len,
                            i.duration_since(start).as_millis(),
                        )
                    }
                    _ =  closer_clone.cancelled() => {
                        info!("All {} tables opened in {} ms",
                            num_opened.load(Ordering::Relaxed),
                            start.elapsed().as_millis());
                        break;
                    }
                }
            }
        }));
        closer
    }
}
