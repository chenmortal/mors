pub fn add(left: usize, right: usize) -> usize {
    left + right
}
use mors_traits::{memtable::Memtable, skip_list::SkipList};
struct MorsMemtable<T: SkipList> {
    data: Vec<(String, String)>,
    skip_list: T,
}
impl<T> Memtable for MorsMemtable<T>
where
    T: SkipList,
{
    fn insert(&mut self, key: String, value: String) {
        todo!()
    }

    fn get(&self, key: &str) -> Option<&str> {
        todo!()
    }

    fn remove(&mut self, key: &str) -> Option<String> {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}
