use std::cmp::Ordering;
use std::io::{BufRead, Error, Read, SeekFrom, Write};
use std::mem::replace;
use std::{
    cmp::max,
    fs::{File, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    slice,
};

use memmap2::{Advice, MmapRaw};

use mors_traits::storage::StorageBackend;

use crate::page_size;

#[derive(Debug)]
pub struct MmapFile {
    /// like std::io::BufWriter
    w_buf: Vec<u8>,
    /// point to actual file
    w_pos: usize,

    ///like std::io::BufReader
    r_buf: Vec<u8>,
    ///point to actual file , already read from actual file
    r_pos: usize,
    ///point to r_buf，already read from r_buf, must <= r_buf.len()
    r_buf_pos: usize,

    last_flush_pos: usize,
    panicked: bool,
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
    pub fn sync_all(&self) -> Result<(), io::Error> {
        self.fd.sync_all()
    }
    pub fn delete(&self) -> Result<(), io::Error> {
        self.fd.set_len(0)?;
        self.fd.sync_all()?;
        Ok(())
    }
}

impl MmapFile {
    #[inline(always)]
    unsafe fn raw_write(raw: &MmapRaw, offset: usize, data: &[u8]) -> io::Result<()> {
        std::ptr::copy_nonoverlapping(data.as_ptr(), raw.as_mut_ptr().add(offset), data.len());
        raw.flush_async_range(offset, data.len())?;
        Ok(())
    }

    #[inline]
    unsafe fn raw_read(raw: &MmapRaw, offset: usize, buf: &mut [u8]) -> usize {
        let buf_len = buf.len().min(raw.len() - offset);
        let s = slice::from_raw_parts(raw.as_mut_ptr().add(offset) as _, buf_len);
        if buf_len == 1 {
            buf[0] = s[0];
        } else {
            buf[..buf_len].copy_from_slice(s);
        }
        buf_len
    }
    #[cold]
    #[inline(never)]
    fn write_cold(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() + self.w_buf.len() > self.w_buf.capacity() {
            self.flush_buf()?;
        }
        if buf.len() >= self.w_buf.capacity() {
            self.panicked = true;
            let r = self.check_len_satisfied(buf.len());
            self.panicked = false;
            r?;
            unsafe {
                Self::raw_write(&self.raw, self.w_pos, buf)?;
            };

            self.w_pos += buf.len();
        } else {
            unsafe { write_to_buffer_unchecked(&mut self.w_buf, buf) }
        };
        Ok(buf.len())
    }
    fn flush_buf(&mut self) -> io::Result<()> {
        self.panicked = true;
        let r = self.check_len_satisfied(self.w_buf.len());
        self.panicked = false;
        if let Err(e) = r {
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                _ => {
                    return Err(e);
                }
            }
        }
        self.panicked = true;
        let r = unsafe { Self::raw_write(&self.raw, self.w_pos, &self.w_buf) };
        self.panicked = false;
        if let Err(e) = r {
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                _ => {
                    return Err(e);
                }
            }
        }

        self.w_pos += self.w_buf.len();
        self.w_buf.clear();

        Ok(())
    }
    fn check_len_satisfied(&mut self, buf_len: usize) -> io::Result<()> {
        let write_at = self.w_pos;
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
        if self.r_buf_pos == self.r_buf.len() {
            if buf.len() >= self.r_buf.capacity() {
                self.r_buf.clear();
                self.r_buf_pos = 0;
                let size = unsafe { Self::raw_read(&self.raw, self.r_pos, buf) };
                self.r_pos += size;
                return Ok(size);
            }
            let remain = (self.raw.len() - self.r_pos).min(self.r_buf.capacity());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.raw.as_ptr().add(self.r_pos),
                    self.r_buf.as_mut_ptr(),
                    remain,
                );
                self.r_buf.set_len(remain);
            }
            self.r_pos += remain;
            self.r_buf_pos = 0;
        }
        let size = self.r_buf[self.r_buf_pos..].as_ref().read(buf)?;
        self.r_buf_pos += size;
        Ok(size)
    }
}

impl BufRead for MmapFile {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.r_buf_pos == self.r_buf.len() {
            let remain = (self.raw.len() - self.r_pos).min(self.r_buf.capacity());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.raw.as_ptr().add(self.r_pos),
                    self.r_buf.as_mut_ptr(),
                    remain,
                );
                self.r_buf.set_len(remain);
            }
            self.r_pos += remain;
            self.r_buf_pos = 0;
        }
        Ok(&self.r_buf[self.r_buf_pos..])
    }

    fn consume(&mut self, amt: usize) {
        self.r_buf_pos += amt;
    }
}
impl Write for MmapFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.w_buf.len() + buf.len() < self.w_buf.capacity() {
            unsafe {
                write_to_buffer_unchecked(&mut self.w_buf, buf);
            }
            Ok(buf.len())
        } else {
            self.write_cold(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()?;

        let (offset, len) = match self.w_pos.cmp(&self.last_flush_pos) {
            Ordering::Less => (self.w_pos, self.last_flush_pos - self.w_pos),
            Ordering::Equal => return Ok(()),
            Ordering::Greater => (self.last_flush_pos, self.w_pos - self.last_flush_pos),
        };

        self.raw.flush_range(offset, len)?;
        self.last_flush_pos = self.w_pos;
        Ok(())
    }
}

#[inline]
unsafe fn write_to_buffer_unchecked(buffer: &mut Vec<u8>, buf: &[u8]) {
    debug_assert!(buffer.len() + buf.len() <= buffer.capacity());
    let old_len = buffer.len();
    let buf_len = buf.len();
    let src = buf.as_ptr();
    let dst = buffer.as_mut_ptr().add(old_len);
    std::ptr::copy_nonoverlapping(src, dst, buf_len);
    buffer.set_len(old_len + buf_len);
}

impl StorageBackend for MmapFile {
    fn len(&self) -> Result<usize, Error> {
        Ok(self.raw.len())
    }

    #[cfg(not(target_os = "linux"))]
    fn set_len(&mut self, size: usize) -> Result<(), io::Error> {
        self.raw.flush()?;
        self.fd.set_len(size as u64)?;
        let _ = replace(&mut self.raw, MmapRaw::map_raw(&self.fd)?);
        Ok(())
    }
    #[cfg(target_os = "linux")]
    fn set_len(&mut self, size: usize) -> Result<(), io::Error> {
        use memmap2::RemapOptions;
        self.raw.flush()?;
        self.fd.set_len(size as u64)?;
        unsafe { self.raw.remap(size, RemapOptions::new().may_move(true))? };
        Ok(())
    }

    fn sync_data(&self) -> Result<(), Error> {
        self.raw.flush()?;
        self.fd.sync_data()
    }
    fn sync_all(&self) -> Result<(), Error> {
        self.raw.flush()?;
        self.fd.sync_all()
    }
    fn sync_range(&self, offset: usize, len: usize) -> Result<(), Error> {
        self.raw.flush_range(offset, len)
    }
    fn read_seek(&mut self, read_pos: SeekFrom) -> Result<(), Error> {
        match read_pos {
            SeekFrom::Start(start) => {
                self.r_pos = start as usize;
                self.r_buf.clear();
                self.r_buf_pos = 0;
            }
            SeekFrom::End(end) => {
                self.r_pos = self.raw.len() - end as usize;
                self.r_buf.clear();
                self.r_buf_pos = 0;
            }
            SeekFrom::Current(current) => {
                self.r_pos += current as usize;
                self.r_buf.clear();
                self.r_buf_pos = 0;
            }
        }
        Ok(())
    }

    fn write_seek(&mut self, write_pos: SeekFrom) -> Result<(), Error> {
        match write_pos {
            SeekFrom::Start(start) => {
                self.w_pos = start as usize;
                self.w_buf.clear();
            }
            SeekFrom::End(end) => {
                self.w_pos = self.raw.len() - end as usize;
                self.w_buf.clear();
            }
            SeekFrom::Current(current) => {
                self.w_pos += current as usize;
                self.w_buf.clear();
            }
        }
        Ok(())
    }

    fn pread(&self, buf: &mut [u8], offset: usize) -> Result<usize, Error> {
        let buf_len = buf.len().min(self.raw.len() - offset);
        let s = unsafe { slice::from_raw_parts(self.raw.as_ptr().add(offset) as _, buf_len) };
        if buf_len == 1 {
            buf[0] = s[0];
        } else {
            buf[..buf_len].copy_from_slice(s);
        }
        Ok(buf_len)
    }

    fn pwrite(&mut self, buf: &[u8], offset: usize) -> Result<usize, Error> {
        let new_write_at = offset + buf.len();
        if new_write_at >= self.raw.len() {
            let align = new_write_at % page_size();
            let new_len = new_write_at - align + 2 * page_size();
            self.set_len(new_len)?;
        }
        unsafe { Self::raw_write(&self.raw, offset, buf) }?;
        Ok(buf.len())
    }
}
pub struct MmapFileBuilder {
    advices: Vec<Advice>,
    open_option: OpenOptions,
}

impl MmapFileBuilder {
    pub fn new() -> Self {
        Self {
            advices: Vec::new(),
            open_option: OpenOptions::new(),
        }
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.open_option.write(write);
        self
    }
    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.open_option.custom_flags(flags);
        self
    }
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.open_option.mode(mode);
        self
    }

    pub fn advice(&mut self, advice: Advice) -> &mut Self {
        self.advices.push(advice);
        self
    }
    pub fn create(&self, path: PathBuf, max_size: u64) -> Result<MmapFile, io::Error> {
        let file = self.open_option.open(&path)?;
        let file_len = file.metadata()?.len();
        let size = max(file_len, max_size as u64);
        file.set_len(size)?;
        let mmap = MmapRaw::map_raw(&file)?;

        for advice in &self.advices {
            mmap.advise(advice.clone())?;
        }
        let mmap = MmapFile {
            w_buf: Vec::with_capacity(page_size()),
            w_pos: 0,
            r_buf: Vec::with_capacity(page_size()),
            r_pos: 0,
            r_buf_pos: 0,
            last_flush_pos: 0,
            panicked: false,
            raw: mmap,
            path,
            fd: file,
        };
        mmap.fd.sync_all()?;
        Ok(mmap)
    }
}
