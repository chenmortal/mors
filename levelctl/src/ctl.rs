use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{debug, info};
use mors_common::{
    closer::Closer,
    file_id::SSTableId,
    kv::ValueMeta,
    ts::{KeyTs, TxnTs},
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
    async fn get(
        &self,
        key: &KeyTs,
    ) -> std::result::Result<Option<(TxnTs, Option<ValueMeta>)>, LevelCtlError>
    {
        Ok(self.get_impl(key).await?)
    }
    async fn spawn_compact<D: mors_traits::vlog::DiscardTrait>(
        self,
        closer: Closer,
        kms: K,
        discard: D,
    ) {
        if let Err(e) = self.spawn_compact_impl(closer, kms, discard).await {
            panic!("spawn_compact error:{}", e);
        }
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
    pub(crate) fn next_id(&self) -> &Arc<AtomicU32> {
        &self.inner.next_id
    }
    pub(crate) fn handler(&self, level: Level) -> Option<&LevelHandler<T, K>> {
        if level > self.inner.max_level {
            return None;
        }
        let handler = &self.inner.handlers[level.to_usize()];
        debug_assert_eq!(handler.level(), level);
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
pub struct LevelCtlConfig {
    max_level: Level,
    level0_num_tables_stall: usize,
    num_compactors: usize,
    levelmax2max_compaction: bool,
    base_level_total_size: usize,
    level_size_multiplier: usize,
    table_size_multiplier: usize,
    level0_table_size: usize,
    level0_tables_len: usize,
    num_versions_to_keep: usize,
}
impl LevelCtlConfig {
    /// Maximum number of levels of compaction allowed in the LSM.
    /// The default value of MaxLevels is 6.
    /// notice \[0..max_level\] is valid level
    pub fn set_max_level(&mut self, max_level: Level) -> &mut Self {
        self.max_level = max_level;
        self
    }
    /// The default value of level0_num_tables_stall is 15.
    /// If the number of Level0 tables exceeds this value, writes will be blocked until compaction
    pub fn set_level0_num_tables_stall(
        &mut self,
        level0_num_tables_stall: usize,
    ) -> &mut Self {
        self.level0_num_tables_stall = level0_num_tables_stall;
        self
    }
    /// the number of compaction workers to run concurrently.  Setting this to
    /// zero stops compactions, which could eventually cause writes to block forever.
    /// The default value of num_compactors is 4. One is dedicated just for L0 and L1.
    pub fn set_num_compactors(&mut self, num_compactors: usize) -> &mut Self {
        self.num_compactors = num_compactors;
        self
    }
    /// If levelmax2max_compaction is true, then the compaction will compact the maximum level to the maximum level.
    pub fn set_levelmax2max_compaction(
        &mut self,
        levelmax2max_compaction: bool,
    ) -> &mut Self {
        self.levelmax2max_compaction = levelmax2max_compaction;
        self
    }
    /// The default value of base_level_size is 10 MB.
    /// sets the maximum total size target for the base level.
    pub fn set_base_level_total_size(
        &mut self,
        base_level_size: usize,
    ) -> &mut Self {
        self.base_level_total_size = base_level_size;
        self
    }
    /// level_size_multiplier sets the ratio between the maximum sizes of contiguous levels in the LSM.
    /// Once a level grows to be larger than this ratio allowed, the compaction process will be triggered.
    /// The default value of LevelSizeMultiplier is 10.
    pub fn set_level_size_multiplier(
        &mut self,
        level_size_multiplier: usize,
    ) -> &mut Self {
        self.level_size_multiplier = level_size_multiplier;
        self
    }
    /// table_size_multiplier sets the ratio between the maximum sizes of contiguous tables in the LSM.
    /// The default value of TableSizeMultiplier is 2.
    pub fn set_table_size_multiplier(
        &mut self,
        table_size_multiplier: usize,
    ) -> &mut Self {
        self.table_size_multiplier = table_size_multiplier;
        self
    }
    /// the size of level0's single table file in bytes.
    /// The default value of level0_size is 64 MB.
    pub fn set_level0_table_size(&mut self, level0_size: usize) -> &mut Self {
        self.level0_table_size = level0_size;
        self
    }
    /// the number of tables in level0.
    /// The default value of level0_tables_len is 5.
    pub fn set_level0_tables_len(
        &mut self,
        level0_tables_len: usize,
    ) -> &mut Self {
        self.level0_tables_len = level0_tables_len + 1;
        self
    }
    /// the number of versions to keep.
    /// The default value of num_versions_to_keep is 1.
    pub fn set_num_versions_to_keep(
        &mut self,
        num_versions_to_keep: usize,
    ) -> &mut Self {
        self.num_versions_to_keep = num_versions_to_keep;
        self
    }
    /// Maximum number of levels of compaction allowed in the LSM.
    pub fn max_level(&self) -> Level {
        self.max_level
    }
    /// The default value of level0_num_tables_stall is 15.
    pub fn level0_num_tables_stall(&self) -> usize {
        self.level0_num_tables_stall
    }
    /// the number of compaction workers to run concurrently.
    pub fn num_compactors(&self) -> usize {
        self.num_compactors
    }
    pub fn levelmax2max_compaction(&self) -> bool {
        self.levelmax2max_compaction
    }
    /// the maximum size target for the base level.
    pub fn base_level_total_size(&self) -> usize {
        self.base_level_total_size
    }
    /// level_size_multiplier sets the ratio between the maximum sizes of contiguous levels in the LSM.
    pub fn level_size_multiplier(&self) -> usize {
        self.level_size_multiplier
    }
    /// table_size_multiplier sets the ratio between the maximum sizes of contiguous tables in the LSM.
    pub fn table_size_multiplier(&self) -> usize {
        self.table_size_multiplier
    }
    pub fn level0_table_size(&self) -> usize {
        self.level0_table_size
    }
    /// the number of tables in level0.
    pub fn level0_tables_len(&self) -> usize {
        self.level0_tables_len
    }
    /// the number of versions to keep.
    pub fn num_versions_to_keep(&self) -> usize {
        self.num_versions_to_keep
    }
}
impl Default for LevelCtlConfig {
    fn default() -> Self {
        Self {
            max_level: 6_u8.into(),
            level0_num_tables_stall: 15,
            num_compactors: 4,
            levelmax2max_compaction: false,
            base_level_total_size: 10 << 20, //10 MB
            level_size_multiplier: 10,
            table_size_multiplier: 2,
            level0_table_size: 64 << 20,
            level0_tables_len: 5,
            num_versions_to_keep: 1,
        }
    }
}
pub struct LevelCtlBuilder<T: TableTrait<K::Cipher>, K: Kms> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    config: LevelCtlConfig,
    dir: PathBuf,
    read_only: bool,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Deref for LevelCtlBuilder<T, K> {
    type Target = LevelCtlConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> DerefMut for LevelCtlBuilder<T, K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelCtlBuilder<T, K> {
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
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

    fn set_cache(
        &mut self,
        cache: <T as TableTrait<K::Cipher>>::Cache,
    ) -> &mut Self {
        self.table.set_cache(cache);
        self
    }

    fn set_level0_table_size(&mut self, size: usize) -> &mut Self {
        self.config.set_level0_table_size(size);
        self
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtlBuilder<T, K> {
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
        let manifest_lock = manifest.lock().await;
        let tables = manifest_lock.tables();

        let watch_closer =
            Self::watch_num_opened(num_opened.clone(), tables.len());

        let mut max_id: SSTableId = 0.into();

        let mut tasks: HashMap<Level, Vec<JoinHandle<Result<Option<T>>>>> =
            HashMap::new();
        debug!("opening tables by manifest");
        for (id, table) in tables.iter() {
            debug!("prepare spawn task for table {}", id);
            let num_opened_clone = num_opened.clone();
            max_id = max_id.max(*id);

            let cipher_id = table.key_id();

            let mut table_builder = self.table.clone();

            table_builder.set_compression(table.compress());
            table_builder.set_dir(self.dir.clone());

            let kms_clone = kms.clone();
            let table_id = *id;
            debug!("spawning task for table {}", table_id);
            let task = tokio::spawn(async move {
                debug!("opening table {}", table_id);
                let cipher = cipher_id
                    .map(|id| kms_clone.get_cipher(id))
                    .transpose()?
                    .flatten();
                let table = table_builder.open(table_id, cipher).await?;
                num_opened_clone.fetch_add(1, Ordering::SeqCst);
                Ok::<Option<T>, MorsLevelCtlError>(table)
            });
            tasks
                .entry(table.level().min(self.config.max_level))
                .or_default()
                .push(task);
        }

        drop(manifest_lock);
        debug!("waiting for tables to open");
        let mut handlers = Vec::new();
        for level in 0..=self.config.max_level.to_u8() {
            let level: Level = level.into();
            match tasks.remove(&level) {
                Some(task_vec) => {
                    let mut tables = Vec::with_capacity(task_vec.len());
                    for handle in task_vec {
                        if let Some(t) = handle.await?? {
                            debug!(
                                "opened table {} for level {}",
                                t.id(),
                                level
                            );
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
        debug!("all tables opened");
        watch_closer.cancel();
        watch_closer.wait().await?;
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
