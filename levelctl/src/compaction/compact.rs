use std::time::SystemTime;

use log::debug;
use mors_traits::{kms::Kms, sstable::TableTrait};

use crate::{ctl::LevelCtl, error::MorsLevelCtlError};

use super::plan::CompactPlan;
use super::Result;

impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn compact(
        &self,
        task_id: usize,
        plan: &mut CompactPlan<T, K>,
    ) -> Result<()> {
        let priority = plan.priority();
        let target = priority.target();

        if target.is_empty() {
            return Err(MorsLevelCtlError::EmptyCompactTarget);
        };

        let now = SystemTime::now();
        let this_level = plan.this_level();
        let next_level = plan.next_level();

        debug_assert!(plan.splits().is_empty());

        if this_level.level() != next_level.level() {
            plan.add_splits();
        }

        Ok(())
    }
    // compactBuildTables merges topTables and botTables to form a list of new tables.
    pub(crate) fn compact_build_tables(
        &self,
        task_id: usize,
        plan: &mut CompactPlan<T, K>,
    ) {
        let top = plan.top();
        let bottom = plan.bottom();
        debug!(
            "Top tables count: {} Bottom tables count {}",
            top.len(),
            bottom.len()
        );

        let valid = bottom
            .iter()
            .filter(|t| {
                !plan.priority().drop_prefixes().iter().any(|prefix| {
                    t.smallest().key().starts_with(prefix)
                        && t.biggest().key().starts_with(prefix)
                })
            })
            .map(|t| t.clone())
            .collect::<Vec<_>>();

        // let compact_task = Vec::new();
        for kr in plan.splits() {
            
        }
    }
}
