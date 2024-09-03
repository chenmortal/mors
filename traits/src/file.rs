use std::io;

use mors_common::mmap::MmapFile;

pub trait Append {
    fn append(&self, offset: usize, buf: &[u8]) -> io::Result<usize>;
    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()>;
}
impl Append for MmapFile {
    fn append(&self, offset: usize, buf: &[u8]) -> io::Result<usize> {
        self.append(offset, buf)
    }

    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()> {
        self.flush_range(offset, len)
    }
}
