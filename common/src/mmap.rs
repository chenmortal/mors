use std::cmp::Ordering;
use std::io::{Error, Read, SeekFrom, Write};
use std::path::Path;
use std::{
    cmp::max,
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    path::PathBuf,
    slice,
    time::SystemTime,
};

use memmap2::{Advice, MmapRaw};

use crate::page_size;

#[derive(Debug)]
pub struct MmapFile {
    /// point to actual file
    w_pos: usize,
    ///point to actual file , already read from actual file
    r_pos: usize,

    last_flush_pos: usize,

    raw: MmapRaw,
    path: PathBuf,
    fd: File,
}
impl MmapFile {
    pub fn builder() -> MmapFileBuilder {
        MmapFileBuilder::new()
    }
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
    pub fn write_at(&self) -> usize {
        self.w_pos
    }
    pub fn delete(&self) -> Result<(), io::Error> {
        self.fd.set_len(0)?;
        self.fd.sync_all()?;
        Ok(())
    }
}

impl MmapFile {
    #[inline(always)]
    unsafe fn raw_write(
        raw: &MmapRaw,
        offset: usize,
        data: &[u8],
    ) -> io::Result<()> {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            raw.as_mut_ptr().add(offset),
            data.len(),
        );
        raw.flush_async_range(offset, data.len())?;
        Ok(())
    }

    #[inline]
    unsafe fn raw_read(raw: &MmapRaw, offset: usize, buf: &mut [u8]) -> usize {
        let buf_len = buf.len().min(raw.len() - offset);
        let s =
            slice::from_raw_parts(raw.as_mut_ptr().add(offset) as _, buf_len);
        if buf_len == 1 {
            buf[0] = s[0];
        } else {
            buf[..buf_len].copy_from_slice(s);
        }
        buf_len
    }

    fn check_len_satisfied(
        &mut self,
        write_at: usize,
        buf_len: usize,
    ) -> io::Result<()> {
        let new_write_at = write_at + buf_len;
        if new_write_at >= self.raw.len() {
            let align = new_write_at % page_size();
            let new_len = new_write_at - align + 2 * page_size();
            self.set_len(new_len)?;
        }
        Ok(())
    }
}
impl Read for MmapFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let buf_len = unsafe { Self::raw_read(&self.raw, self.r_pos, buf) };
        self.r_pos += buf_len;
        Ok(buf_len)
    }
}

impl Write for MmapFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let buf_len = buf.len();
        self.check_len_satisfied(self.w_pos, buf_len)?;
        unsafe { Self::raw_write(&self.raw, self.w_pos, buf) }?;
        self.w_pos += buf_len;
        Ok(buf_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        let (offset, len) = match self.w_pos.cmp(&self.last_flush_pos) {
            Ordering::Less => (self.w_pos, self.last_flush_pos - self.w_pos),
            Ordering::Equal => return Ok(()),
            Ordering::Greater => {
                (self.last_flush_pos, self.w_pos - self.last_flush_pos)
            }
        };

        self.raw.flush_range(offset, len)?;
        self.last_flush_pos = self.w_pos;
        Ok(())
    }
}

impl AsRef<[u8]> for MmapFile {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.raw.as_ptr() as _, self.raw.len()) }
    }
}
impl MmapFile {
    #[inline]
    pub fn len(&self) -> Result<usize, Error> {
        Ok(self.raw.len())
    }
    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.raw.len() == 0)
    }
    #[inline]
    pub fn file_len(&self) -> io::Result<u64> {
        Ok(self.fd.metadata()?.len())
    }
    pub fn file_modified(&self) -> io::Result<SystemTime> {
        self.fd.metadata()?.modified()
    }
    #[cfg(not(target_os = "linux"))]
    pub fn set_len(&mut self, size: usize) -> Result<(), io::Error> {
        use std::mem::replace;

        self.raw.flush()?;
        self.fd.set_len(size as u64)?;
        let _ = replace(&mut self.raw, MmapRaw::map_raw(&self.fd)?);
        Ok(())
    }
    #[cfg(target_os = "linux")]
    pub fn set_len(&mut self, size: usize) -> Result<(), io::Error> {
        use memmap2::RemapOptions;
        use std::mem::replace;
        self.raw.flush()?;
        self.fd.set_len(size as u64)?;
        unsafe { self.raw.remap(size, RemapOptions::new().may_move(true))? };
        Ok(())
    }

    pub fn sync_data(&self) -> Result<(), Error> {
        self.raw.flush()?;
        self.fd.sync_data()
    }
    pub fn sync_all(&self) -> Result<(), Error> {
        self.raw.flush()?;
        self.fd.sync_all()
    }
    pub fn sync_range(&self, offset: usize, len: usize) -> Result<(), Error> {
        self.raw.flush_range(offset, len)
    }
    pub fn read_seek(&mut self, read_pos: SeekFrom) -> Result<(), Error> {
        match read_pos {
            SeekFrom::Start(start) => {
                self.r_pos = start as usize;
            }
            SeekFrom::End(end) => {
                self.r_pos = self.raw.len() - end as usize;
            }
            SeekFrom::Current(current) => {
                self.r_pos += current as usize;
            }
        }
        Ok(())
    }

    pub fn write_seek(&mut self, write_pos: SeekFrom) -> Result<(), Error> {
        self.flush()?;
        match write_pos {
            SeekFrom::Start(start) => {
                self.w_pos = start as usize;
            }
            SeekFrom::End(end) => {
                self.w_pos = self.raw.len() - end as usize;
            }
            SeekFrom::Current(current) => {
                self.w_pos += current as usize;
            }
        }
        Ok(())
    }

    pub fn pread(&self, buf: &mut [u8], offset: usize) -> Result<usize, Error> {
        let buf_len = unsafe { Self::raw_read(&self.raw, offset, buf) };
        Ok(buf_len)
    }

    pub fn pread_ref(&self,offset: usize,len:usize)->&[u8]{
        let buf = unsafe { slice::from_raw_parts(self.raw.as_ptr().add(offset) as _, len) };
        buf
    }
    pub fn pwrite(
        &mut self,
        buf: &[u8],
        offset: usize,
    ) -> Result<usize, Error> {
        self.check_len_satisfied(offset, buf.len())?;
        unsafe { Self::raw_write(&self.raw, offset, buf) }?;
        Ok(buf.len())
    }
}
#[derive(Debug)]
pub struct MmapFileBuilder {
    advices: Vec<Advice>,
    open_option: OpenOptions,
}
impl Deref for MmapFileBuilder {
    type Target = OpenOptions;

    fn deref(&self) -> &Self::Target {
        &self.open_option
    }
}
impl DerefMut for MmapFileBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.open_option
    }
}
impl Default for MmapFileBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl MmapFileBuilder {
    pub fn new() -> Self {
        Self {
            advices: Vec::new(),
            open_option: OpenOptions::new(),
        }
    }

    // pub fn write(&mut self, write: bool) -> &mut Self {
    //     self.open_option.write(write);
    //     self
    // }
    // pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
    //     self.open_option.custom_flags(flags);
    //     self
    // }
    // pub fn mode(&mut self, mode: u32) -> &mut Self {
    //     self.open_option.mode(mode);
    //     self
    // }

    pub fn advice(&mut self, advice: Advice) -> &mut Self {
        self.advices.push(advice);
        self
    }
    pub fn build<P: AsRef<Path>>(
        &self,
        path: P,
        size: u64,
    ) -> Result<MmapFile, Error> {
        let file = self.open_option.open(&path)?;
        let file_len = file.metadata()?.len();
        let size = max(file_len, size);
        file.set_len(size)?;
        let mmap = MmapRaw::map_raw(&file)?;

        for advice in &self.advices {
            mmap.advise(*advice)?;
        }
        let mmap = MmapFile {
            w_pos: 0,
            r_pos: 0,
            last_flush_pos: 0,
            raw: mmap,
            path: path.as_ref().to_path_buf(),
            fd: file,
        };
        mmap.fd.sync_all()?;
        Ok(mmap)
    }
}
