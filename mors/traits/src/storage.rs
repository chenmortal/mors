use std::fmt::Debug;
use std::io;
use std::io::{Read, SeekFrom, Write};

pub trait StorageBackend: 'static + Debug + Send + Sync + Read + Write {
    /// Gets the current length of the storage.
    fn len(&self) -> Result<usize, io::Error>;

    fn is_empty(&self) -> Result<bool, io::Error> {
        self.len().map(|len| len == 0)
    }

    /// Sets the length of the storage.
    fn set_len(&mut self, len: usize) -> Result<(), io::Error>;

    /// Syncs all buffered data with the persistent storage.
    fn sync_data(&self) -> Result<(), io::Error>;

    fn sync_all(&self) -> Result<(), io::Error>;
    
    fn sync_range(&self, offset: usize, len: usize) -> Result<(), io::Error>;
    fn read_seek(&mut self, read_pos: SeekFrom) -> Result<(), io::Error>;

    fn write_seek(&mut self, write_pos: SeekFrom) -> Result<(), io::Error>;

    /// Reads the specified array of bytes from the storage.
    fn pread(&self, buf: &mut [u8], offset: usize) -> Result<usize, io::Error>;
    /// Writes the specified array of bytes to the storage.
    fn pwrite(&mut self, buf: &[u8], offset: usize) -> Result<usize, io::Error>;
}
