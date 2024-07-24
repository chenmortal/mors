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
    levelctl::{
        Level, LevelCtlBuilderTrait, LevelCtlError, LevelCtlTrait, LEVEL0,
    },
    sstable::{TableBuilderTrait, TableTrait},
};

type Result<T> = std::result::Result<T, MorsLevelCtlError>;

use tokio::{select, task::JoinHandle};

use crate::{
    compact::status::CompactStatus,
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
    handlers: Vec<LevelHandler<T, K::Cipher>>,
    next_id: Arc<AtomicU32>,
    level0_stalls_ms: AtomicU64,
    compact_status: CompactStatus,
    level0_num_tables_stall: usize,
    levelmax2max_compaction: bool,
    num_compactors: usize,
    level0_stalls: AtomicU64,
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
    pub(crate) fn num_compactors(&self) -> usize {
        self.inner.num_compactors
    }
    pub(crate) fn levelmax2max_compaction(&self) -> bool {
        self.inner.levelmax2max_compaction
    }
    pub(crate) fn level0_num_tables_stall(&self) -> usize {
        self.inner.level0_num_tables_stall
    }
    pub(crate) fn level0_stalls_ms(&self) -> &AtomicU64 {
        &self.inner.level0_stalls_ms
    }
    pub(crate) fn handler(
        &self,
        level: Level,
    ) -> Option<&LevelHandler<T, K::Cipher>> {
        self.inner.handlers.iter().find(|h| *h.level() == level)
    }
}

pub struct LevelCtlBuilder<T: TableTrait<K::Cipher>, K: Kms> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    max_level: Level,
    level0_num_tables_stall: usize,
    num_compactors: usize,
    levelmax2max_compaction: bool,
    cache: Option<T::Cache>,
    dir: PathBuf,
    read_only: bool,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for LevelCtlBuilder<T, K> {
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
            max_level: 6_u8.into(),
            dir: PathBuf::from(DEFAULT_DIR),
            cache: None,
            read_only: false,
            level0_num_tables_stall: 15,
            num_compactors: 4,
            levelmax2max_compaction: false,
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
    pub fn set_max_level(&mut self, max_level: Level) -> &mut Self {
        self.max_level = max_level;
        self
    }

    pub fn set_level0_num_tables_stall(
        &mut self,
        level0_num_tables_stall: usize,
    ) -> &mut Self {
        self.level0_num_tables_stall = level0_num_tables_stall;
        self
    }
    pub fn set_num_compactors(&mut self, num_compactors: usize) -> &mut Self {
        self.num_compactors = num_compactors;
        self
    }

    async fn build_impl(&self, kms: K) -> Result<LevelCtl<T, K>> {
        let compact_status = CompactStatus::new(self.max_level.to_usize());
        let manifest = self.manifest.build()?;

        let (max_id, level_tables) =
            self.open_tables_by_manifest(manifest.clone(), kms).await?;

        let next_id = Arc::new(AtomicU32::new(1 + Into::<u32>::into(max_id)));
        let mut handlers = Vec::with_capacity(level_tables.len());
        let mut level = LEVEL0;
        for tables in level_tables {
            let handler = LevelHandler::new(level, tables);
            handler.validate()?;
            handlers.push(handler);
            level += 1;
        }

        let ctl = LevelCtlInner {
            manifest,
            handlers,
            next_id,
            level0_stalls_ms: Default::default(),
            compact_status,
            table_builder: self.table.clone(),
            level0_num_tables_stall: self.level0_num_tables_stall,
            level0_stalls: Default::default(),
            num_compactors: self.num_compactors,
            levelmax2max_compaction: self.levelmax2max_compaction,
        };
        Ok(LevelCtl {
            inner: Arc::new(ctl),
        })
    }

    async fn open_tables_by_manifest(
        &self,
        manifest: Manifest,
        kms: K,
    ) -> Result<(SSTableId, Vec<Vec<T>>)> {
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
                .entry(table.level().min(self.max_level))
                .or_default()
                .push(task);
        }
        drop(manifest_lock);
        throttle.finish().await?;

        watch_closer.cancel();
        watch_closer.wait().await?;

        let mut level_tables = Vec::new();
        for level in 0..self.max_level.to_u8() {
            match tasks.remove(&level.into()) {
                Some(task_vec) => {
                    let mut tables = Vec::with_capacity(task_vec.len());
                    for handle in task_vec {
                        if let Some(t) = handle.await? {
                            tables.push(t);
                        };
                    }
                    level_tables.push(tables);
                }
                None => {
                    level_tables.push(Vec::new());
                }
            }
        }
        Ok((max_id, level_tables))
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
