use std::collections::HashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use log::debug;
use mors_common::kv::{Meta, ValueMeta, ValuePointer};
use mors_common::ts::KeyTs;
use mors_traits::iter::{
    CacheIterator, KvCacheIter, KvCacheIterator, KvCacheMergeIterator,
    KvSeekIter,
};
use mors_traits::levelctl::{Level, LevelCtlTrait, LEVEL0};
use mors_traits::sstable::{CacheTableConcatIter, TableBuilderTrait};
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
    ) -> Result<()> {
        let mut all_tables = plan.top().to_vec();
        all_tables.extend_from_slice(plan.bottom());

        let is_intersect =
            self.check_intersect(&all_tables, plan.next_level().level());

        let target = plan.priority().target();

        if kr.left().is_empty() {
            merge_iter.next()?;
        } else {
            let left = kr.left().encode();
            let left_borrow = left.as_slice();
            merge_iter.seek(left_borrow.into())?;
        }

        let mut context = AddKeyContext {
            last_key: Default::default(),
            skip_key: Default::default(),
            num_versions: Default::default(),
            discard_stats: Default::default(),
            first_key_has_discard_set: Default::default(),
            ctl: &self,
            kr: &kr,
            is_intersect,
            table_builder: self.table_builder().clone(),
            plan: &plan,
        };
        while merge_iter.valid() {
            if !kr.right().is_empty()
                && merge_iter.key().unwrap() == *kr.right()
            {
                break;
            }
            context.table_builder = self.table_builder().clone();
            let target_size = target.file_size(plan.next_level().level());
            context.table_builder.set_table_size(target_size);
        }
        Ok(())
        // let mut new_table = self.new_table();
        // let mut writer = new_table.writer();
        // while let Some((key, value)) = merge_iter.next() {
        // writer.put(key, value);

        // writer.finish();
    }
    fn check_intersect(&self, tables: &[T], level: Level) -> bool {
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

struct AddKeyContext<'a, T: TableTrait<K::Cipher>, K: Kms> {
    last_key: KeyTs,
    skip_key: KeyTs,
    num_versions: usize,
    discard_stats: HashMap<u32, u64>,
    first_key_has_discard_set: bool,
    ctl: &'a LevelCtl<T, K>,
    plan: &'a CompactPlan<T, K>,
    kr: &'a KeyTsRange,
    is_intersect: bool,
    table_builder: T::TableBuilder,
}
impl<'a, T: TableTrait<K::Cipher>, K: Kms> AddKeyContext<'a, T, K> {
    fn push(&mut self, iter: &mut KvCacheMergeIterator) -> Result<()> {
        let start = SystemTime::now();
        let mut num_keys = 0;
        let mut num_skips = 0;
        let mut range_check = 0;
        let mut table_key_range = KeyTsRange::default();

        while iter.valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            if self.plan.drop_prefixes().iter().any(|p| key.starts_with(p)) {
                num_keys += 1;
                self.update_discard(&value);
                iter.next()?;
                continue;
            };

            if !self.skip_key.is_empty() {
                if key.key() == self.skip_key.key() {
                    num_skips += 1;
                    self.update_discard(&value);
                    iter.next()?;
                    continue;
                }
                self.skip_key = Default::default();
            }

            if key.key() != self.last_key.key() {
                self.first_key_has_discard_set = false;
                if !self.kr.right().is_empty()
                    && iter.key().unwrap() == *self.kr.right()
                {
                    break;
                }
            }
        }
        Ok(())
    }
    fn update_discard(&mut self, value: &ValueMeta) {
        if value.meta().contains(Meta::VALUE_POINTER) {
            let vp = ValuePointer::decode(value.value()).unwrap();
            match self.discard_stats.get_mut(&vp.fid()) {
                Some(v) => {
                    *v += vp.size() as u64;
                }
                None => {
                    self.discard_stats.insert(vp.fid(), vp.size() as u64);
                }
            };
        }
    }
}
