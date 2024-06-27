use std::path::PathBuf;

use bytes::Buf;
use memmap2::Advice;
use mors_common::compress::CompressionType;
use mors_common::mmap::{MmapFile, MmapFileBuilder};
use mors_traits::default::DEFAULT_DIR;
use mors_traits::file_id::{FileId, SSTableId};
use mors_traits::kms::KmsCipher;
use mors_traits::sstable::{TableBuilderTrait, TableTrait};
use prost::Message;

use crate::pb::proto::Checksum;
use crate::table_index::TableIndexBuf;
use crate::Result;
use crate::{error::MorsTableError, pb::proto::checksum};
// ChecksumVerificationMode tells when should DB verify checksum for SSTable blocks.
#[derive(Debug, Clone, Copy)]
pub enum ChecksumVerificationMode {
    // NoVerification indicates DB should not verify checksum for SSTable blocks.
    NoVerification,

    // OnTableRead indicates checksum should be verified while opening SSTtable.
    OnTableRead,

    // OnBlockRead indicates checksum should be verified on every SSTable block read.
    OnBlockRead,

    // OnTableAndBlockRead indicates checksum should be verified
    // on SSTable opening and on every block read.
    OnTableAndBlockRead,
}
impl Default for ChecksumVerificationMode {
    fn default() -> Self {
        Self::NoVerification
    }
}

pub struct TableBuilder {
    read_only: bool,
    dir: PathBuf,
    table_size: usize,
    table_capacity: usize,
    // ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
    checksum_verify_mode: ChecksumVerificationMode,
    checksum_algo: checksum::Algorithm,
    // BloomFalsePositive is the false positive probabiltiy of bloom filter.
    bloom_false_positive: f64,

    // BlockSize is the size of each block inside SSTable in bytes.
    block_size: usize,

    // Compression indicates the compression algorithm used for block compression.
    compression: CompressionType,

    zstd_compression_level: i32,
}
impl Default for TableBuilder {
    fn default() -> Self {
        Self {
            table_size: 2 << 20,
            table_capacity: ((2 << 20) as f64 * 0.95) as usize,
            checksum_verify_mode: ChecksumVerificationMode::default(),
            checksum_algo: checksum::Algorithm::Crc32c,
            bloom_false_positive: 0.01,
            block_size: 4 * 1024,
            compression: CompressionType::default(),
            zstd_compression_level: 1,
            read_only: false,
            dir: PathBuf::from(DEFAULT_DIR),
        }
    }
}

impl TableBuilderTrait<Table> for TableBuilder {
    fn set_compression(&mut self, compression: CompressionType) {
        self.compression = compression;
    }

    fn open<K: KmsCipher>(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> Result<Table> {
        if self.compression.is_none() && self.block_size == 0 {
            return Err(MorsTableError::InvalidConfig);
        }
        let path = id.join_dir(&self.dir);

        let mut mmap_builder = MmapFileBuilder::new();
        mmap_builder.advice(Advice::Sequential);
        mmap_builder.read(true).write(!self.read_only);
        let mmap = mmap_builder.build(path, 0)?;

        let table_size = mmap.file_len()?;
        let create_at = mmap.file_modified()?;

        let (index_buf, index_start, index_len) =
            TableBuilder::init_index(&mmap, cipher)?;

        todo!()
    }

    fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }
}
impl TableBuilder {
    fn init_index<K: KmsCipher>(
        mmap: &MmapFile,
        cipher: Option<K>,
    ) -> Result<(TableIndexBuf, usize, usize)> {
        let mut read_pos = mmap.file_len()? as usize;

        //read checksum len from the last 4 bytes;
        read_pos -= 4;

        let mut buf = [0; 4];
        debug_assert_eq!(mmap.pread(&mut buf, read_pos)?, 4);
        let checksum_len = buf.as_ref().get_u32() as usize;

        //read checksum
        read_pos -= checksum_len;
        let mut checksum_buf = vec![0; checksum_len];
        debug_assert_eq!(
            mmap.pread(&mut checksum_buf, read_pos)?,
            checksum_len
        );
        let checksum = Checksum::decode(checksum_buf.as_ref())?;

        //read index len from the footer;
        read_pos -= 4;
        let mut buf = [0; 4];
        debug_assert_eq!(mmap.pread(&mut buf, read_pos)?, 4);
        let index_len = buf.as_ref().get_u32() as usize;

        //read index
        read_pos -= index_len;
        let mut data = vec![0; index_len];
        debug_assert_eq!(mmap.pread(&mut data, read_pos)?, index_len);

        checksum.verify(data.as_ref())?;
        let index_buf = TableIndexBuf::from_vec(data)?;

        debug_assert!(!index_buf.offsets().is_empty());

        Ok((index_buf, read_pos, index_len))
    }
    fn smallest_biggest<K: KmsCipher>(
        &self,
        index_buf: &TableIndexBuf,
        mmap: &MmapFile,
        cipher: Option<K>,
    ) -> Result<()> {
        //get smallest
        let first_block_offset = index_buf
            .offsets()
            .first()
            .ok_or(MorsTableError::TableIndexOffsetEmpty)?;
        let smallest = first_block_offset.key_ts().to_owned();

        //get biggest
        let last_block_offset = index_buf
            .offsets()
            .last()
            .ok_or(MorsTableError::TableIndexOffsetEmpty)?;

        let data = &mmap.as_ref()[last_block_offset.offset() as usize
            ..last_block_offset.size() as usize];

        let plaintext = cipher
            .map(|c| c.decrypt(data))
            .transpose()?
            .unwrap_or_else(|| data.to_vec());

        let uncompress_data = self.compression.decompress(plaintext)?;
        todo!()
    }
}
pub struct Table {}
impl TableTrait for Table {
    type ErrorType = MorsTableError;
    type TableBuilder = TableBuilder;
}
