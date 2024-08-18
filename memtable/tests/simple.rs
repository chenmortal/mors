use mors_common::kv::Entry;
use mors_common::kv::Meta;
use mors_encrypt::registry::MorsKmsBuilder;
use mors_memtable::memtable::MemtableBuilder;
use mors_skip_list::skip_list::SkipList;
use mors_traits::default::WithDir;
use mors_traits::kms::KmsBuilder;
use mors_traits::memtable::MemtableBuilderTrait;
use mors_traits::memtable::MemtableTrait;
type TestMemtableBuilder = MemtableBuilder<SkipList>;
use proptest::prelude::ProptestConfig;
use proptest::proptest;
fn build_reload(count: u32, table_num: u32) {
    let tempdir = tempfile::tempdir().unwrap();
    let mut kms_builder = MorsKmsBuilder::default();
    kms_builder.set_dir(tempdir.path().to_path_buf());
    let kms = kms_builder.build().unwrap();

    let mut builder = TestMemtableBuilder::default();
    builder.set_dir(tempdir.path().to_path_buf());

    for i in 0..table_num {
        let mut memtable = builder.build(kms.clone()).unwrap();
        let prefix = format!("table{}", i);
        let entries = generate_entries(count, &prefix);
        for entry in &entries {
            memtable.push(entry).unwrap();
        }
    }

    let memtables = builder.open_exist(kms).unwrap();
    assert_eq!(memtables.len(), table_num as usize);
    for (i, memtable) in memtables.iter().enumerate() {
        let prefix = format!("table{}", i);
        let entries = generate_entries(count, &prefix);
        for (index, entry) in entries.iter().enumerate() {
            assert!(entry.key_ts().key().starts_with(prefix.as_bytes()));
            let (txn, value) = memtable.get(entry.key_ts()).unwrap().unwrap();
            assert_eq!(txn.to_u64() as usize, index);
            assert!(value.is_some());
            assert_eq!(value.unwrap(), *entry.value_meta());
        }
    }

    tempdir.close().unwrap();
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn test_table_iter(count in 1..10000u32,table_num in 1..5u32) {
        build_reload(count,table_num)
    }
}
fn generate_entries(count: u32, prefix: &str) -> Vec<Entry> {
    let mut entries = Vec::with_capacity(count as usize);
    for i in 0..count {
        let k = prefix.to_string() + &format!("{:06}", i);
        let v = format!("{}", i);
        let mut entry = Entry::new(k.into(), v.into());
        entry.set_meta(Meta::DELETE);
        entry.set_user_meta(5);
        entry.set_version(0.into());
        entry.set_version((i as u64).into());
        entries.push(entry);
    }
    entries
}
