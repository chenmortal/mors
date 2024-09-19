use std::{
    ops::RangeInclusive,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use mors_common::ts::KeyTs;
use mors_traits::{kms::Kms, levelctl::LEVEL0, sstable::TableTrait};
use parking_lot::RwLockReadGuard;

use crate::{
    ctl::LevelCtl,
    error::MorsLevelCtlError,
    handler::{LevelHandler, LevelHandlerTables},
};

use super::priority::CompactPriority;
use super::Result;
#[derive(Clone, Debug)]
pub(crate) struct CompactPlan<T: TableTrait<K::Cipher>, K: Kms> {
    task_id: usize,
    priority: CompactPriority,
    this_level: LevelHandler<T, K>,
    next_level: LevelHandler<T, K>,
    top: Vec<T>,
    bottom: Vec<T>,
    this_range: KeyTsRange,
    next_range: KeyTsRange,
    this_size: usize,
    drop_prefixes: Vec<Bytes>,
    splits: Vec<KeyTsRange>,
}
impl<T: TableTrait<K::Cipher>, K: Kms> Default for CompactPlan<T, K> {
    fn default() -> Self {
        Self {
            task_id: Default::default(),
            priority: Default::default(),
            this_level: Default::default(),
            next_level: Default::default(),
            top: Default::default(),
            bottom: Default::default(),
            this_range: Default::default(),
            next_range: Default::default(),
            this_size: Default::default(),
            splits: Default::default(),
            drop_prefixes: Default::default(),
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> CompactPlan<T, K> {
    pub(crate) fn this_level(&self) -> &LevelHandler<T, K> {
        &self.this_level
    }
    pub(crate) fn next_level(&self) -> &LevelHandler<T, K> {
        &self.next_level
    }
    pub(crate) fn this_range(&self) -> &KeyTsRange {
        &self.this_range
    }
    pub(crate) fn next_range(&self) -> &KeyTsRange {
        &self.next_range
    }
    pub(crate) fn this_size(&self) -> usize {
        self.this_size
    }
    pub(crate) fn top(&self) -> &[T] {
        &self.top
    }
    pub(crate) fn bottom(&self) -> &[T] {
        &self.bottom
    }
    pub(crate) fn priority(&self) -> &CompactPriority {
        &self.priority
    }
    pub(crate) fn splits(&self) -> &[KeyTsRange] {
        &self.splits
    }
    pub(crate) fn drop_prefixes(&self) -> &[Bytes] {
        &self.drop_prefixes
    }
    // addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
    pub(crate) fn add_splits(&mut self) {
        self.splits.clear();

        // Let's say we have 10 tables in plan.bot and min width = 3. Then, we'll pick
        // 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
        // This gives us 4 picks for 10 tables.
        // In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
        // then uses up a lot of memory for table builder.
        // We should keep it so we have at max 5 splits.
        let width = ((self.bottom.len() as f64 / 5.0).ceil() as usize).max(3);

        let mut kr = self.this_range.clone();
        kr.extend(self.next_range.clone());

        for t in self.bottom.chunks(width) {
            let last = t.last().unwrap();
            let right = KeyTs::new(last.biggest().key().clone(), 0.into());
            kr.right = right.clone();
            self.splits.push(kr.clone());
            kr.left = right;
        }
    }
    pub(crate) fn push_split(&mut self, split: KeyTsRange) {
        self.splits.push(split);
    }
}
pub(crate) struct CompactPlanReadGuard<'a, T: TableTrait<K::Cipher>, K: Kms> {
    pub(crate) this_level: RwLockReadGuard<'a, LevelHandlerTables<T, K>>,
    pub(crate) next_level: RwLockReadGuard<'a, LevelHandlerTables<T, K>>,
}

impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn gen_plan(
        &self,
        task_id: usize,
        priority: CompactPriority,
    ) -> Result<CompactPlan<T, K>> {
        let this_level = self.handler(priority.level()).unwrap().clone();

        if priority.level() == LEVEL0 {
            let next_level =
                self.handler(self.target().base_level()).unwrap().clone();
            let mut plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
                ..Default::default()
            };
            if !self.fill_tables_l0(&mut plan)? {
                return Err(MorsLevelCtlError::FillTablesError);
            };
            Ok(plan)
        } else {
            let next_level = if priority.level() == self.max_level() {
                this_level.to_owned()
            } else {
                self.handler(priority.level() + 1).unwrap().clone()
            };

            let mut plan = CompactPlan {
                task_id,
                priority,
                this_level,
                next_level,
                ..Default::default()
            };
            if !self.fill_tables(&mut plan)? {
                return Err(MorsLevelCtlError::FillTablesError);
            };
            Ok(plan)
        }
    }

    // fillTablesL0 would try to fill tables from L0 to be compacted with Lbase.
    // If it can not do that, it would try to compact tables from L0 -> L0.
    // Say L0 has 10 tables.
    // fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
    // Next call to fill_level0_tables would run L0ToLbase again, which fails this time.
    // So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
    // be compacted within L0. Additionally, it would set the compaction range in
    // cstatus to inf, so no other L0 -> Lbase compactions can happen.
    fn fill_tables_l0(&self, plan: &mut CompactPlan<T, K>) -> Result<bool> {
        Ok(self.fill_tables_l0_to_lbase(plan)?
            || self.fill_tables_l0_to_l0(plan)?)
    }

    fn fill_tables_l0_to_lbase(
        &self,
        plan: &mut CompactPlan<T, K>,
    ) -> Result<bool> {
        if plan.next_level.level() == LEVEL0 {
            unreachable!("Base level can't be zero")
        }

        if (0.0..1.0).contains(&plan.priority.adjusted()) {
            return Ok(false);
        }
        let lock = CompactPlanReadGuard::<T, K> {
            this_level: plan.this_level.read(),
            next_level: plan.next_level.read(),
        };

        let top = lock.this_level.tables().to_vec();
        if top.is_empty() {
            return Ok(false);
        }

        if !plan.priority.drop_prefixes().is_empty() {
            // Use all tables if drop prefix is set. We don't want to compact only a
            // sub-range. We want to compact all the tables.
            plan.this_range =
                KeyTsRange::from_slice::<T, K>(lock.this_level.tables());
            plan.top = lock.this_level.tables().to_vec();
        } else {
            plan.top.clear();
            let mut s = KeyTsRange::from::<T, K>(&top[0]);
            plan.top.push(top[0].clone());
            for t in top.iter().skip(1) {
                let other = KeyTsRange::from::<T, K>(t);
                if s.intersects(&other) {
                    plan.top.push(t.clone());
                    s.extend(other);
                } else {
                    break;
                }
            }
            plan.this_range = s;
        }
        let index_range = lock
            .next_level
            .table_index_by_range(&lock, &plan.this_range);

        match index_range {
            Some(range) => {
                plan.bottom = lock.next_level.tables()[range].to_vec();
            }
            None => {
                plan.bottom.clear();
            }
        }

        if plan.bottom.is_empty() {
            plan.next_range = plan.this_range.clone();
        } else {
            plan.next_range = KeyTsRange::from_slice::<T, K>(&plan.bottom);
        }

        self.compact_status().check_update(&lock, plan)
    }
    fn fill_tables_l0_to_l0(
        &self,
        plan: &mut CompactPlan<T, K>,
    ) -> Result<bool> {
        if plan.task_id != 0 {
            return Ok(false);
        }
        plan.next_level = self.handler(LEVEL0).unwrap().clone();
        plan.next_range = KeyTsRange::default();
        plan.bottom.clear();

        // Because this level and next level are both level 0, we should NOT acquire
        // the read lock twice, because it can result in a deadlock. So, we don't
        // call compactDef.lockLevels, instead locking the level only once and
        // directly here.
        debug_assert!(plan.this_level.level() == LEVEL0);
        debug_assert!(plan.next_level.level() == LEVEL0);

        let this_level = plan.this_level.read();
        let mut status = self.compact_status().write()?;

        let target = plan.priority.target();
        let now = SystemTime::now();
        let out = this_level
            .tables()
            .iter()
            .filter(|t| t.size() < 2 * target.file_size(LEVEL0))
            .filter(|t| {
                now.duration_since(t.create_time()).unwrap()
                    > Duration::from_secs(10)
            })
            .filter(|t| !status.tables().contains(&t.id()))
            .cloned()
            .collect::<Vec<_>>();

        if out.len() < 2 {
            return Ok(false);
        }

        plan.this_range = KeyTsRange::inf();
        plan.top = out;

        // Avoid any other L0 -> Lbase from happening, while this is going on.
        status.levels_mut()[plan.this_level.level().to_usize()]
            .push(KeyTsRange::inf());

        plan.top.iter().for_each(|t| {
            status.tables_mut().insert(t.id());
        });

        // For L0->L0 compaction, we set the target file size to max, so the output is always one file.
        // This significantly decreases the L0 table stalls and improves the performance.
        plan.priority.target_mut().set_file_size(LEVEL0, usize::MAX);
        Ok(true)
    }
    fn fill_tables(&self, plan: &mut CompactPlan<T, K>) -> Result<bool> {
        let this_level = plan.this_level().clone();
        let next_level = plan.next_level().clone();
        let lock = CompactPlanReadGuard::<T, K> {
            this_level: this_level.read(),
            next_level: next_level.read(),
        };
        if plan.this_level.tables_len() == 0 {
            return Ok(false);
        }
        if plan.this_level.level() == self.max_level() {
            return self.fill_tables_max_level(&lock, plan);
        }

        let mut this_tables = lock.this_level.tables().to_vec();
        this_tables.sort_by_key(|a| a.max_version());

        let this_level = plan.this_level.level();
        for t in this_tables {
            plan.this_size = t.size();
            plan.this_range = KeyTsRange::from::<T, K>(&t);
            // If we're already compacting this range, don't do anything.
            if self
                .compact_status()
                .intersects(this_level, &plan.this_range)?
            {
                continue;
            };

            plan.top = vec![t.clone()];

            let index_range = lock
                .next_level
                .table_index_by_range(&lock, &plan.this_range);
            match index_range {
                Some(range) => {
                    plan.bottom = lock.next_level.tables()[range].to_vec();
                }
                None => {
                    plan.bottom.clear();
                }
            }

            if plan.bottom.is_empty() {
                plan.next_range = plan.this_range.clone();
                if !self.compact_status().check_update(&lock, plan)? {
                    continue;
                };
                return Ok(true);
            }
            plan.next_range = KeyTsRange::from_slice::<T, K>(&plan.bottom);

            if self
                .compact_status()
                .intersects(plan.next_level.level(), plan.next_range())?
            {
                continue;
            };

            if !self.compact_status().check_update(&lock, plan)? {
                continue;
            };
            return Ok(true);
        }
        Ok(false)
    }
    fn fill_tables_max_level(
        &self,
        lock: &CompactPlanReadGuard<T, K>,
        plan: &mut CompactPlan<T, K>,
    ) -> Result<bool> {
        let this_level = plan.this_level().level();
        let mut top = lock.this_level.tables().to_vec();
        top.sort_by_key(|t| std::cmp::Reverse(t.stale_data_size()));

        if !top.is_empty() && top[0].stale_data_size() == 0 {
            return Ok(false);
        }

        plan.bottom.clear();
        let collect_bottom =
            |plan: &mut CompactPlan<T, K>, t: &T, t_file_size: usize| {
                let tables = lock.next_level.tables();
                let mut total_size = t.size();

                let index = tables
                    .binary_search_by(|a| a.smallest().cmp(t.smallest()))
                    .unwrap_or_else(|i| i);
                assert_eq!(tables[index].id(), t.id());
                for new in tables[index + 1..].iter() {
                    total_size += new.size();
                    plan.bottom.push(new.clone());
                    plan.next_range.extend(KeyTsRange::from::<T, K>(new));
                    if total_size >= t_file_size {
                        break;
                    }
                }
            };
        let now = SystemTime::now();
        for t in top
            .drain(..)
            .filter(|t| {
                now.duration_since(t.create_time()).unwrap()
                    > Duration::from_secs(1)
            })
            .filter(|t| t.stale_data_size() >= 10 << 20)
        {
            plan.this_size = t.size();
            plan.this_range = KeyTsRange::from::<T, K>(&t);
            // Set the next range as the same as the current range. If we don't do
            // this, we won't be able to run more than one max level compactions.
            plan.next_range = plan.this_range.clone();

            if self
                .compact_status()
                .intersects(this_level, plan.this_range())?
            {
                continue;
            };

            // Found a valid table!
            plan.top = vec![t.clone()];

            let t_file_size = plan.priority.target().file_size(this_level);
            // The table size is what we want so no need to collect more tables.
            if t.size() > t_file_size {
                break;
            }

            collect_bottom(plan, &t, t_file_size);
            if !self.compact_status().check_update(lock, plan)? {
                plan.bottom.clear();
                plan.next_range = KeyTsRange::default();
            };
            return Ok(true);
        }

        if plan.top.is_empty() {
            return Ok(false);
        }
        self.compact_status().check_update(lock, plan)
    }
}
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct KeyTsRange {
    left: KeyTs,
    right: KeyTs,
    inf: bool,
}

impl KeyTsRange {
    pub(crate) fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty() && !self.inf
    }
    pub(crate) fn left(&self) -> &KeyTs {
        &self.left
    }
    pub(crate) fn set_left(&mut self, left: KeyTs) {
        self.left = left;
    }
    pub(crate) fn right(&self) -> &KeyTs {
        &self.right
    }
    pub(crate) fn set_right(&mut self, right: KeyTs) {
        self.right = right;
    }
    pub(crate) fn intersects(&self, other: &Self) -> bool {
        if self.is_empty() || other.is_empty() {
            return false;
        }
        if self.inf || other.inf {
            return true;
        }
        !(self.right < other.left || self.left > other.right)
    }
    pub(crate) fn extend(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        if self.left.is_empty() || other.left < self.left {
            self.left = other.left;
        }
        if self.right.is_empty() || self.right < other.right {
            self.right = other.right;
        }
        if other.inf {
            self.inf = true;
        }
    }
    pub(crate) fn from_slice<T: TableTrait<K::Cipher>, K: Kms>(
        value: &[T],
    ) -> Self {
        if value.is_empty() {
            return Self::default();
        }
        let (smallest, biggest) = value.iter().fold(
            (value[0].smallest(), value[0].biggest()),
            |(smallest, biggest), table| {
                (smallest.min(table.smallest()), biggest.max(table.biggest()))
            },
        );
        Self {
            left: KeyTs::new(smallest.key().clone(), u64::MAX.into()),
            right: KeyTs::new(biggest.key().clone(), 0.into()),
            inf: false,
        }
    }
    pub(crate) fn from<T: TableTrait<K::Cipher>, K: Kms>(value: &T) -> Self {
        Self {
            left: KeyTs::new(value.smallest().key().clone(), u64::MAX.into()),
            right: KeyTs::new(value.biggest().key().clone(), 0.into()),
            inf: false,
        }
    }
    pub(crate) fn inf() -> Self {
        Self {
            left: KeyTs::default(),
            right: KeyTs::default(),
            inf: true,
        }
    }
}
impl<T: TableTrait<K::Cipher>, K: Kms> LevelHandlerTables<T, K> {
    pub(crate) fn table_index_by_range(
        &self,
        _lock: &CompactPlanReadGuard<T, K>,
        kr: &KeyTsRange,
    ) -> Option<RangeInclusive<usize>> {
        if kr.left.is_empty() || kr.right.is_empty() {
            return None;
        }
        let table_len = self.tables().len();
        let left_index = self
            .tables()
            .binary_search_by(|t| t.biggest().cmp(&kr.left))
            .unwrap_or_else(|i| i);
        if left_index >= table_len {
            return None;
        }

        let right_index = self
            .tables()
            .binary_search_by(|t| t.smallest().cmp(&kr.right))
            .unwrap_or_else(|i| i); // if t.smallest==kr.right, so need this table.
        if right_index >= table_len {
            return None;
        }
        Some(left_index..=right_index)
    }
}

#[test]
fn test_a() {
    let k = 1.0;
    assert!((0.0..=1.0).contains(&k));
}
