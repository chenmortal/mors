use std::time::Duration;

use log::{debug, info, warn};
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms, levelctl::LEVEL0, sstable::TableTrait, vlog::DiscardTrait,
};
use rand::Rng;

use priority::{fmt_compact_priorities, CompactPriority};
use tokio::{
    select,
    time::{interval, sleep},
};

use crate::{ctl::LevelCtl, error::MorsLevelCtlError, manifest::Manifest};

mod compact;
mod plan;
mod priority;
pub mod status;
pub type Result<T> = std::result::Result<T, MorsLevelCtlError>;

#[derive(Debug, Clone)]
pub(crate) struct CompactContext<K: Kms, D: DiscardTrait> {
    kms: K,
    manifest: Manifest,
    discard: D,
}
impl<K: Kms, D: DiscardTrait> CompactContext<K, D> {
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn discard(&self) -> &D {
        &self.discard
    }
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
    pub async fn spawn_compact_impl<D: DiscardTrait>(
        self,
        closer: Closer,
        kms: K,
        discard: D,
    ) -> Result<()> {
        let context = CompactContext::<K, D> {
            kms,
            manifest: self.manifest().clone(),
            discard,
        };
        let mut tasks = Vec::new();
        for task_id in 0..self.config().num_compactors() {
            tasks.push(tokio::spawn(self.clone().run_compactor(
                task_id,
                closer.clone(),
                context.clone(),
            )));
        }
        for t in tasks {
            t.await??;
        }
        Ok(())
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
        context: CompactContext<K, D>,
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
        let mut last_priorities = Vec::new();
        loop {
            select! {
                _=ticker.tick() => {
                    count += 1;

                    if self.config().levelmax2max_compaction()
                    && task_id ==2 && count >= 200 {
                        let priority=CompactPriority::new(self.max_level(), self.target());
                        self.run_compact(task_id,priority,context.clone()).await;
                        count=0;
                    }else{

                        let mut priorities = self.pick_compact_levels()?;

                        if priorities != last_priorities {
                            last_priorities=priorities.clone();
                            debug!(
                                "\n{}\n{}\n",
                                fmt_compact_priorities(
                                    &priorities,
                                    self.handler(LEVEL0).unwrap().tables_len(),
                                    self.config().level0_tables_len(),
                                ),
                                priorities[0].target()
                            );
                            // Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
                            // We'll still sort them by their adjusted score below. Having both these scores allows us to
                            // make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
                            // compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
                            let mut prios = priorities
                            .drain(..priorities.len() - 1)
                            .filter(|p| p.score() >= 1.)
                            .collect::<Vec<_>>();
                            if task_id==0{
                                if let Some(index)=prios.iter().position(|p|p.level()==LEVEL0){
                                    let level0=prios.remove(index);
                                    prios.insert(0,level0);
                                }
                            }
                            for prio in prios{
                                if prio.adjusted() >= 1.0 || (task_id == 0 && prio.level() == LEVEL0) {
                                    if self.run_compact(task_id, prio, context.clone()).await {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
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
    // doCompact picks some table on level l and compacts it away to the next level.
    async fn run_compact<D: DiscardTrait>(
        &self,
        task_id: usize,
        mut priority: CompactPriority,
        context: CompactContext<K, D>,
    ) -> bool {
        debug_assert!(priority.level() <= self.max_level());
        let priority_level = priority.level();
        // base level can't be LEVEL0 , update it
        if priority.target().base_level() == LEVEL0 {
            priority.set_target(self.target())
        };
        match self.gen_plan(task_id, priority) {
            Ok(mut plan) => {
                match self
                    .compact(task_id, priority_level, &mut plan, context)
                    .await
                {
                    Ok(_) => {
                        info!(
                            "[Compactor: {}] compact success for {}",
                            task_id,
                            plan.this_level().level(),
                        );
                        true
                    }
                    Err(e) => {
                        warn!("[Compactor: {}] compact error: {}", task_id, e);
                        false
                    }
                }
            }
            Err(MorsLevelCtlError::FillTablesError) => false,
            Err(e) => {
                warn!("task {} compact error: {}", task_id, e);
                false
            }
        }
    }
}
