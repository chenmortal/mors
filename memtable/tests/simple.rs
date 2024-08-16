use mors_common::kv::{Entry, Meta};
use mors_encrypt::registry::MorsKmsBuilder;
use mors_memtable::memtable::MemtableBuilder;
use mors_skip_list::skip_list::SkipList;
use mors_traits::default::WithDir;
use mors_traits::kms::KmsBuilder;
use mors_traits::memtable::MemtableBuilderTrait;
use mors_traits::memtable::MemtableTrait;
type TestMemtableBuilder = MemtableBuilder<SkipList>;
#[test]
fn test_build() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut kms_builder = MorsKmsBuilder::default();
    kms_builder.set_dir(tempdir.path().to_path_buf());
    let kms = kms_builder.build().unwrap();

    let mut builder = TestMemtableBuilder::default();
    builder.set_dir(tempdir.path().to_path_buf());
    let mut memtable = builder.build(kms.clone()).unwrap();
    let entries = generate_entries(5, "key");
    for entry in &entries {
        memtable.push(entry).unwrap();
    }
    let memtable = builder.open_exist(kms).unwrap().pop_front().unwrap();
    for entry in entries {
        let k = memtable.get(entry.key_ts()).unwrap();
        assert!(k.is_some());
    }
    // memtable.push();
    // let builder = MemtableBuilder::default();
}
fn generate_entries(count: u32, prefix: &str) -> Vec<Entry> {
    let mut entries = Vec::with_capacity(count as usize);
    for i in 0..count {
        let k = prefix.to_string() + &format!("{:06}", i);
        let v = format!("{}", i);
        let mut entry = Entry::new(k.into(), v.into());
        entry.set_meta(Meta::from_bits(b'A').unwrap());
        entry.set_user_meta(5);
        entry.set_version(0.into());
        entries.push(entry);
    }
    entries
}
