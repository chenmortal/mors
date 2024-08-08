use std::cmp::Ordering;

use bytes::Bytes;
use mors_traits::{
    kms::Kms,
    levelctl::{Level, LevelCtlTrait, LEVEL0},
    sstable::{TableBuilderTrait, TableTrait},
};

use super::Result;
use crate::ctl::LevelCtl;
#[derive(Debug, Default, Clone)]
pub(crate) struct CompactPriority {
    level: Level,
    score: f64,
    adjusted: f64,
    drop_prefixes: Vec<Bytes>,
    target: CompactTarget,
}
impl CompactPriority {
    pub(crate) fn new(level: Level, target: CompactTarget) -> Self {
        Self {
            level,
            target,
            ..Default::default()
        }
    }
    pub(crate) fn level(&self) -> Level {
        self.level
    }
    pub(crate) fn target(&self) -> &CompactTarget {
        &self.target
    }
    pub(crate) fn target_mut(&mut self) -> &mut CompactTarget {
        &mut self.target
    }
    pub(crate) fn set_target(&mut self, target: CompactTarget) {
        self.target = target;
    }
    pub(crate) fn adjusted(&self) -> f64 {
        self.adjusted
    }
    pub(crate) fn drop_prefixes(&self) -> &[Bytes] {
        &self.drop_prefixes
    }
}
#[derive(Debug, Default, Clone)]
pub(crate) struct CompactTarget {
    base_level: Level,
    target_size: Vec<usize>,
    file_size: Vec<usize>,
}
impl CompactTarget {
    pub(crate) fn target_size(&self, level: Level) -> usize {
        self.target_size[level.to_usize()]
    }
    pub(crate) fn file_size(&self, level: Level) -> usize {
        self.file_size[level.to_usize()]
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.target_size.is_empty() || self.file_size.is_empty()
    }
    pub(crate) fn set_file_size(&mut self, level: Level, size: usize) {
        self.file_size[level.to_usize()] = size;
    }
    pub(crate) fn base_level(&self) -> Level {
        self.base_level
    }
}
// levelTargets calculates the targets for levels in the LSM tree.
// The idea comes from Dynamic Level Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB.
// The sizes of levels are calculated based on the size of the lowest level, typically L6.
// So, if L6 size is 1GB, then L5 target size is 100MB, L4 target size is 10MB and so on.
//
// L0 files don't automatically go to L1. Instead, they get compacted to Lbase, where Lbase is
// chosen based on the first level which is non-empty from top (check L1 through L6). For an empty
// DB, that would be L6.  So, L0 compactions go to L6, then L5, L4 and so on.
//
// Lbase is advanced to the upper levels when its target size exceeds BaseLevelSize. For
// example, when L6 reaches 1.1GB, then L4 target sizes becomes 11MB, thus exceeding the
// BaseLevelSize of 10MB. L3 would then become the new Lbase, with a target size of 1MB <
// BaseLevelSize.
impl<T: TableTrait<K::Cipher>, K: Kms> LevelCtl<T, K> {
    pub(crate) fn target(&self) -> CompactTarget {
        let mut target = CompactTarget {
            base_level: LEVEL0,
            target_size: vec![0; self.max_level().to_usize() + 1],
            file_size: vec![0; self.max_level().to_usize() + 1],
        };

        let max_handler = self.handler(self.max_level()).unwrap();

        let mut level_size = max_handler.total_size();
        let base_level_size = self.config().base_level_size();

        for i in (1..=self.max_level().into()).rev() {
            // L6->L1, if level size <= base level size and base_level not changed, then Lbase = i
            if level_size <= base_level_size && target.base_level == LEVEL0 {
                target.base_level = i.into();
            }
            // target size >= base level size
            let target_size = level_size.max(base_level_size);
            target.target_size[i] = target_size;
            // if L6 size is 1GB, then L5 target size is 100MB, L4 target size is 10MB and so on.
            level_size /= self.config().level_size_multiplier();
        }

        let mut table_size = self.table_builder().table_size();
        for i in 0..=self.max_level().to_usize() {
            if i == 0 {
                // level0_size == memtable_size
                target.file_size[i] = self.config().level0_size();
            } else if i <= target.base_level.to_usize() {
                target.file_size[i] = table_size;
            } else {
                table_size *= self.config().table_size_multiplier();
                target.file_size[i] = table_size;
            }
        }

        // Bring the base level down to the last empty level.
        for level in
            (target.base_level.to_usize() + 1)..self.max_level().to_usize()
        {
            if self.handler(level.into()).unwrap().total_size() > 0 {
                break;
            };
            target.base_level = level.into();
        }

        let base_level = target.base_level;

        // If the base level is empty and the next level size is less than the
        // target size, pick the next level as the base level.
        if base_level < self.max_level()
            && self.handler(base_level).unwrap().total_size() == 0
            && self.handler(base_level + 1).unwrap().total_size()
                < target.target_size[base_level.to_usize() + 1]
        {
            target.base_level = base_level + 1;
        }
        target
    }
    pub(crate) fn pick_compact_levels(&self) -> Result<Vec<CompactPriority>> {
        let mut prios = Vec::with_capacity(self.handlers_len());
        let target = self.target();

        let mut push_priority = |level: Level, score: f64| {
            let adjusted = score;
            let priority = CompactPriority {
                level,
                score,
                adjusted,
                drop_prefixes: vec![],
                target: target.clone(),
            };
            prios.push(priority);
        };

        push_priority(
            LEVEL0,
            self.handler(LEVEL0).unwrap().tables_len() as f64
                / self.config().level0_tables_len() as f64,
        );

        for level in 1..=self.max_level().to_usize() {
            let level = level.into();
            let delete_size = self.compact_status().delete_size(level)?;
            let total_size = self.handler(level).unwrap().total_size();
            let size = total_size as i64 - delete_size;
            push_priority(level, size as f64 / target.target_size(level) as f64)
        }

        assert_eq!(prios.len(), self.handlers_len());

        // The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
        // If Li-1 has score > 1.0, then we'll divide Li-1 score by Li.
        // If Li score is >= 1.0, then Li-1 score is reduced,
        // which means we'll prioritize the compaction of lower levels (L5, L4 and so on) over the higher levels (L0, L1 and so on).
        // On the other hand, if Li score is < 1.0, then we'll increase the priority of Li-1.
        // Overall what this means is, if the bottom level is already overflowing, then de-prioritize
        // compaction of the above level. If the bottom level is not full, then increase the priority of above level.
        let mut pre_level = 0;
        for level in target.base_level.to_usize()..=self.max_level().to_usize()
        {
            if prios[pre_level].adjusted >= 1.0 {
                // Avoid absurdly large scores by placing a floor on the score that we'll
                // adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
                const MIN_SCORE: f64 = 0.01;
                if prios[level].score >= MIN_SCORE {
                    prios[pre_level].adjusted /= prios[level].adjusted;
                } else {
                    prios[pre_level].adjusted /= MIN_SCORE;
                }
            }
            pre_level = level;
        }
        // Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
        // We'll still sort them by their adjusted score below. Having both these scores allows us to
        // make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
        // compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
        let mut prios = prios
            .drain(..prios.len() - 1)
            .filter(|p| p.score >= 1.)
            .collect::<Vec<_>>();
        // descend sort the levels by their adjusted score.
        prios.sort_by(|a, b| {
            b.adjusted
                .partial_cmp(&a.adjusted)
                .unwrap_or(Ordering::Greater)
        });

        Ok(prios)
    }
}
