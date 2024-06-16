use std::io;

use mors_traits::file_id::FileId;

use crate::LogFile;

impl<F:FileId> LogFile<F> {
    pub fn truncate(&mut self, end_offset: usize) -> io::Result<()> {
        let file_size = self.mmap.file_len()? as usize;
        if end_offset == file_size {
            return Ok(());
        }
        self.set_size(end_offset);
        self.mmap.set_len(end_offset)?;
        Ok(())

    }
}