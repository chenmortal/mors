use std::time::Duration;

use log::info;
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms,
    levelctl::{Level, LEVEL0},
    sstable::TableTrait,
    vlog::DiscardTrait,
};
use rand::Rng;
use target::CompactTarget;
use tokio::{
    select,
    time::{interval, sleep},
};

use crate::{ctl::LevelCtl, manifest::Manifest};

pub mod status;
mod target;
#[derive(Debug, Default)]
struct CompactPriority {
    level: Level,
    score: f64,
    adjusted: f64,
    target: CompactTarget,
}

#[derive(Debug, Clone)]
pub struct CompactContext<T: TableTrait<K::Cipher>, K: Kms, D: DiscardTrait> {
    kms: K,
    cache: T::Cache,
    manifest: Manifest,
    discard: D,
}
/// Implementation of the `LevelCtl` struct.
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    /// Spawns the compactors for the `LevelCtl` instance.
    ///
    /// # Arguments
    ///
    /// * `kms` - The key management system.
    /// * `cache` - The cache for table's block.
    /// * `discard` - The  vlog discard stats.
    pub fn spawn<D: DiscardTrait>(self, kms: K, cache: T::Cache, discard: D) {
        let closer = Closer::new("compact");
        let context = CompactContext::<T, K, D> {
            kms,
            cache,
            manifest: self.manifest().clone(),
            discard,
        };
        for task_id in 0..self.config().num_compactors() {
            tokio::spawn(self.clone().run_compactor(
                task_id,
                closer.clone(),
                context.clone(),
            ));
        }
    }
    fn pick_compact_levels(&self) {
        let mut prios = Vec::with_capacity(self.handlers_len());
        let target = self.target();

        let mut push_priority = |level: Level, score: f64| {
            let adjusted = score;
            let priority = CompactPriority {
                level,
                score,
                adjusted,
                target: target.clone(),
            };
            prios.push(priority);
        };

        push_priority(
            LEVEL0,
            self.handler(LEVEL0).unwrap().tables_len() as f64
                / self.config().level0_tables_len() as f64,
        );
    }
    /// Runs the compactor for the `LevelCtl` instance.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The ID of the task.
    /// * `closer` - The closer instance.
    /// * `context` - The compact context.
    async fn run_compactor<D: DiscardTrait>(
        self,
        task_id: usize,
        closer: Closer,
        context: CompactContext<T, K, D>,
    ) {
        let sleep =
            sleep(Duration::from_millis(rand::thread_rng().gen_range(0..1000)));

        select! {
            _=sleep => {
                info!("task {} started", task_id);
            }
            _=closer.cancelled() => {
                info!("task {} closed", task_id);
            }
        }

        let mut count = 0;
        let mut ticker = interval(Duration::from_millis(50));

        loop {
            select! {
                _=ticker.tick() => {
                    count += 1;
                    info!("task {} count {}", task_id, count);
                    if self.config().levelmax2max_compaction()
                    && task_id ==2 && count >= 200 {
                        let priority=CompactPriority{
                            level:self.max_level(),
                            target: self.target(),
                            ..Default::default()
                        };
                    }
                }
                _=closer.cancelled() => {
                    info!("task {} closed", task_id);
                    break;
                }
            }
        }
    }
    async fn do_compact<D: DiscardTrait>(
        &self,
        task_id: usize,
        priority: CompactPriority,
        context: CompactContext<T, K, D>,
    ) {
    }
}
