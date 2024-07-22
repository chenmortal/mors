use std::{io, path::Path, sync::Arc};

use bytes::{Buf, BufMut};
use log::info;
use memmap2::Advice;
use mors_common::{mmap::MmapFile, util::search};
use mors_traits::vlog::DiscardTrait;
use parking_lot::Mutex;

use crate::error::MorsVlogError;

const DISCARD_FILE_NAME: &str = "DISCARD";
const DISCARD_FILE_SIZE: usize = 1 << 20; //1MB
const SLOT_SIZE: usize = 2 * size_of::<u64>();
const DISCARD_MAX_SLOT: usize = DISCARD_FILE_SIZE / SLOT_SIZE; //1MB file can store 65536 discard entries. Each entry is 16 bytes;
pub struct Discard {
    inner: Arc<Mutex<DiscardInner>>,
}
struct DiscardInner {
    mmap: MmapFile,
    next_slot: usize,
}
impl Discard {
    pub fn new(vlog_dir: &Path) -> Result<Self, MorsVlogError> {
        let path = vlog_dir.join(DISCARD_FILE_NAME);
        let mut mmap_builder = MmapFile::builder();
        mmap_builder.read(true).write(true).create(true);
        mmap_builder.advice(Advice::Sequential);
        let mmap = mmap_builder.build(path, DISCARD_FILE_SIZE as u64)?;
        let mut inner = DiscardInner { mmap, next_slot: 0 };
        for slot in 0..DISCARD_MAX_SLOT {
            if inner.get(slot * SLOT_SIZE) == 0 {
                inner.next_slot = slot;
                break;
            }
        }
        inner.sort()?;
        info!("Discard file loaded, next slot: {}", inner.next_slot);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
    pub fn update(&self, fd: u64, discard: i64) -> io::Result<u64> {
        let mut inner = self.inner.lock();
        // inner.mmap.as_ref().binary_search(x);
        let result = search(inner.next_slot, |slot| {
            inner.get(slot * SLOT_SIZE).cmp(&fd)
        });
        match result {
            Ok(index) => {
                let offset = index * SLOT_SIZE + 8;
                let mut now = inner.get(offset);
                match discard {
                    0 => Ok(now),
                    d if d < 0 => {
                        inner.set(offset, 0)?;
                        Ok(0)
                    }
                    d => {
                        now += d as u64;
                        inner.set(offset, now)?;
                        Ok(now)
                    }
                }
            }
            Err(_) => {
                if discard <= 0 {
                    Ok(0)
                } else {
                    let index = inner.next_slot;
                    inner.set(index * SLOT_SIZE, fd)?;
                    inner.set(index * SLOT_SIZE + 8, discard as u64)?;
                    while inner.next_slot >= inner.mmap.len()? / SLOT_SIZE {
                        let len = inner.mmap.len()?;
                        inner.mmap.set_len(2 * len)?;
                    }
                    inner.sort()?;
                    Ok(discard as u64)
                }
            }
        }
    }
}
impl DiscardInner {
    fn get(&self, offset: usize) -> u64 {
        (&self.mmap.as_ref()[offset..offset + 8]).get_u64()
    }
    fn set(&mut self, offset: usize, value: u64) -> io::Result<usize> {
        let mut buf = [0u8; 8];
        buf.as_mut().put_u64(value);
        self.mmap.pwrite(&buf, offset)
    }
    fn sort(&mut self) -> io::Result<()> {
        let slice = &mut self.mmap.as_mut()[..self.next_slot * SLOT_SIZE];
        let mut chunks: Vec<[u8; 16]> = Vec::new();
        for chunk in slice.chunks(SLOT_SIZE) {
            chunks.push(chunk.try_into().unwrap());
        }
        chunks.sort_unstable_by(|a, b| {
            let a_val = (&a[..8]).get_u64();
            let b_val = (&b[..8]).get_u64();
            a_val.cmp(&b_val)
        });
        for (i, chunk) in chunks.iter().enumerate() {
            let offset = i * SLOT_SIZE;
            let _ = self.mmap.pwrite(chunk, offset)?;
        }
        Ok(())
    }
}
impl DiscardTrait for Discard {}
