pub trait Memtable {
    fn insert(&mut self, key: String, value: String);
    fn get(&self, key: &str) -> Option<&str>;
    fn remove(&mut self, key: &str) -> Option<String>;
    fn size(&self) -> usize;
}