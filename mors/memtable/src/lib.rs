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

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;

    #[test]
    fn it_works() {
        let p: HashMap<u32, u32> = HashMap::new();
        p.is_empty();
        // p.get_key_value(k)
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
