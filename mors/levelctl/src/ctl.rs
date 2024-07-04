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
use mors_common::closer::{Closer, Throttle};
use mors_traits::{
    cache::CacheTrait,
    default::DEFAULT_DIR,
    file_id::SSTableId,
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
pub struct LevelCtl<
    T: TableTrait<C, K::Cipher>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: Kms,
> {
    manifest: Manifest,
    handlers: Vec<LevelHandler<T, C, K::Cipher>>,
    next_id: AtomicU32,
    level0_stalls_ms: AtomicU64,
    compact_status: CompactStatus,
}
impl<
        T: TableTrait<C, K::Cipher>,
        C: CacheTrait<T::Block, T::TableIndexBuf>,
        K: Kms,
    > LevelCtlTrait<T, C, K> for LevelCtl<T, C, K>
{
    type ErrorType = MorsLevelCtlError;

    type LevelCtlBuilder = LevelCtlBuilder<T, C, K>;

    fn max_version(&self) -> mors_traits::ts::TxnTs {
        todo!()
    }
}

pub struct LevelCtlBuilder<
    T: TableTrait<C, K::Cipher>,
    C: CacheTrait<T::Block, T::TableIndexBuf>,
    K: Kms,
> {
    manifest: ManifestBuilder,
    table: T::TableBuilder,
    max_level: Level,
    cache: Option<C>,
    dir: PathBuf,
}
impl<
        T: TableTrait<C, K::Cipher>,
        C: CacheTrait<T::Block, T::TableIndexBuf>,
        K: Kms,
    > Default for LevelCtlBuilder<T, C, K>
{
    fn default() -> Self {
        Self {
            manifest: ManifestBuilder::default(),
            table: T::TableBuilder::default(),
            max_level: 6_u8.into(),
            dir: PathBuf::from(DEFAULT_DIR),
            cache: None,
        }
    }
}
impl<
        T: TableTrait<C, K::Cipher>,
        C: CacheTrait<T::Block, T::TableIndexBuf>,
        K: Kms,
    > LevelCtlBuilderTrait<LevelCtl<T, C, K>, T, C, K>
    for LevelCtlBuilder<T, C, K>
{
    async fn build(
        &self,
        kms: K,
    ) -> std::result::Result<LevelCtl<T, C, K>, LevelCtlError> {
        Ok(self.build_impl(kms).await?)
    }
}
impl<
        T: TableTrait<C, K::Cipher>,
        C: CacheTrait<T::Block, T::TableIndexBuf>,
        K: Kms,
    > LevelCtlBuilder<T, C, K>
{
    async fn build_impl(&self, kms: K) -> Result<LevelCtl<T, C, K>> {
        let compact_status = CompactStatus::new(self.max_level.to_usize());
        let manifest = self.manifest.build()?;

        let (max_id, level_tables) =
            self.open_tables_by_manifest(manifest.clone(), kms).await?;

        let next_id = AtomicU32::new(1 + Into::<u32>::into(max_id));
        let mut handlers = Vec::with_capacity(level_tables.len());
        let mut level = LEVEL0;
        for tables in level_tables {
            let handler = LevelHandler::new(level, tables);
            handler.validate()?;
            handlers.push(handler);
            level += 1;
        }

        let ctl = LevelCtl {
            manifest,
            handlers,
            next_id,
            level0_stalls_ms: Default::default(),
            compact_status,
        };
        Ok(ctl)
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
                let cipher = kms_clone.get_cipher(cipher_id)?;
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
        let closer = Closer::new("levelctl init watch_num_opened".to_owned());
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
