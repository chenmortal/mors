use std::io::{self, Read};
use std::path::Path;
use std::sync::atomic::Ordering;

pub trait StorageTrait: Sized + Read + Send + Sync + 'static {
    type StorageBuilder: StorageBuilderTrait<Self>;
    fn append(&self, buf: &[u8], order: Ordering) -> io::Result<usize>;
    fn load_append_pos(&self, order: Ordering) -> usize;
    fn set_read_pos(&mut self, pos: usize);
    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()>;
    fn file_len(&self) -> io::Result<u64>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    fn delete(&self) -> io::Result<()>;
}
pub trait StorageBuilderTrait<S: StorageTrait>: Default + Sized {
    fn build<P: AsRef<Path>>(&self, path: P, size: u64) -> io::Result<S>;
    fn read(&mut self, read: bool) -> &mut Self;
    fn write(&mut self, write: bool) -> &mut Self;
    fn create(&mut self, create: bool) -> &mut Self;
}
