use std::path::PathBuf;

use mors_encrypt::cipher::AesCipher;
use mors_encrypt::registry::MorsKms;
use mors_levelctl::ctl::LevelCtlBuilder;
use mors_sstable::table::Table;
use mors_traits::default::WithDir;

type TestTable = Table<AesCipher>;
type TestLevelCtlBuilder = LevelCtlBuilder<TestTable, MorsKms>;
#[test]
fn test_builder() {
    let mut builder = TestLevelCtlBuilder::default();
    let dir = "/tmp/levelctl";
    builder.set_dir(PathBuf::from(dir));
    builder.set_max_level(10u32.into());
    
}
