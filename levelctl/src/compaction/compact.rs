use std::sync::Arc;
use std::time::SystemTime;

use log::debug;
use mors_common::kv::ValueMeta;
use mors_traits::iter::{KvCacheIterator, KvCacheMergeIterator};
use mors_traits::levelctl::{Level, LEVEL0};
use mors_traits::sstable::CacheTableConcatIter;
use mors_traits::{kms::Kms, sstable::TableTrait};

use crate::{ctl::LevelCtl, error::MorsLevelCtlError};

use super::plan::{CompactPlan, CompactPlanReadGuard, KeyTsRange};
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
        level: Level,
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
            .cloned()
            .collect::<Vec<_>>();

        let new_iter = || {
            let mut out: Vec<Box<dyn KvCacheIterator<ValueMeta>>> = Vec::new();
            if level == LEVEL0 {
                for t in top.iter().rev() {
                    out.push(Box::new(t.iter(false)));
                }
            } else if !top.is_empty() {
                assert_eq!(top.len(), 1);
                out = vec![Box::new(top[0].iter(false))]
            };
            out.push(Box::new(CacheTableConcatIter::new(valid.clone(), false)));
            out
        };
        // let compact_task = Vec::new();
        let plan_clone = Arc::new(plan.clone());
        for kr in plan.splits() {
            let mut iters = new_iter();
            if let Some(merge) = KvCacheMergeIterator::new(iters) {};
            // compact_task.push(new_table);
        }
    }
    fn sub_compact(
        self,
        mut merge_iter: KvCacheMergeIterator,
        kr: KeyTsRange,
        plan: Arc<CompactPlan<T, K>>,
    ) {
        let mut all_tables = plan.top().to_vec();
        all_tables.extend_from_slice(plan.bottom());

        // let mut new_table = self.new_table();
        // let mut writer = new_table.writer();
        // while let Some((key, value)) = merge_iter.next() {
        // writer.put(key, value);

        // writer.finish();
    }
    fn check_overlap(&self, tables: &[T], level: Level) -> bool {
        let kr = KeyTsRange::from_slice::<T, K>(tables);
        for level in level.to_usize()..=self.max_level().to_usize() {
            let handler = self.handler(level.into()).unwrap();
            let guard = CompactPlanReadGuard {
                this_level: handler.read(),
                next_level: handler.read(),
            };
            let range = guard.this_level.table_index_by_range(&guard, &kr);
            if range.count() > 0 {
                return true;
            }
        }
        false
    }
}
