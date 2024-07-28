use std::time::Duration;

use log::info;
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms, levelctl::LEVEL0, sstable::TableTrait, vlog::DiscardTrait,
};
use rand::Rng;

use priority::CompactPriority;
use tokio::{
    select,
    time::{interval, sleep},
};

use crate::{ctl::LevelCtl, error::MorsLevelCtlError, manifest::Manifest};

mod plan;
mod priority;
pub mod status;
pub type Result<T> = std::result::Result<T, MorsLevelCtlError>;

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
    ) -> Result<()> {
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
                        let priority=CompactPriority::new(self.max_level(), self.target());
                        self.run_compact(task_id,priority,context.clone());
                    }else{
                        let mut prios=self.pick_compact_levels()?;
                        if task_id==0{
                            if let Some(index)=prios.iter().position(|p|p.level()==LEVEL0){
                                let level0=prios.remove(index);
                                prios.insert(0,level0);
                            }
                        }
                        for prio in prios{
                            if prio.adjusted() <1.0 && !(task_id==0 && prio.level()==LEVEL0)  {
                                break;
                            }
                            self.run_compact(task_id,prio,context.clone());
                        }
                    }
                }
                _=closer.cancelled() => {
                    info!("task {} closed", task_id);
                    break;
                }
            }
        }
        Ok(())
    }
    fn run_compact<D: DiscardTrait>(
        &self,
        task_id: usize,
        priority: CompactPriority,
        context: CompactContext<T, K, D>,
    ) {
        self.do_compact(task_id, priority, context);
    }
    // doCompact picks some table on level l and compacts it away to the next level.
    fn do_compact<D: DiscardTrait>(
        &self,
        task_id: usize,
        mut priority: CompactPriority,
        context: CompactContext<T, K, D>,
    ) {
        debug_assert!(priority.level() < self.max_level());
        // base level can't be LEVEL0 , update it
        if priority.target().base_level() == LEVEL0 {
            priority.set_target(self.target())
        };
        self.gen_plan(task_id, priority);
        // let this_level = self.handler(priority.level());
    }
}
