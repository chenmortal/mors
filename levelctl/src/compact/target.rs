use mors_traits::{
    kms::Kms,
    levelctl::{Level, LevelCtlTrait, LEVEL0},
    sstable::{TableBuilderTrait, TableTrait},
};

use crate::ctl::LevelCtl;
#[derive(Debug, Default, Clone)]
pub(crate) struct CompactTarget {
    base_level: Level,
    target_size: Vec<usize>,
    file_size: Vec<usize>,
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
        let level_count = self.max_level().to_usize() + 1;
        let mut target = CompactTarget {
            base_level: LEVEL0,
            target_size: vec![0; level_count],
            file_size: vec![0; level_count],
        };

        let max_handler = self.handler(self.max_level()).unwrap();

        let mut level_size = max_handler.total_size();
        let base_level_size = self.config().base_level_size();

        for i in (1..level_count).rev() {
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
        for i in 0..level_count {
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
}
