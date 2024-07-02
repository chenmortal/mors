use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Buf;
use log::error;
use memmap2::Advice;
use mors_common::compress::CompressionType;
use mors_common::mmap::{MmapFile, MmapFileBuilder};
use mors_traits::cache::{BlockCacheKey, CacheTrait};
use mors_traits::default::DEFAULT_DIR;
use mors_traits::file_id::{FileId, SSTableId};
use mors_traits::iter::{DoubleEndedCacheIterator, KvDoubleEndedCacheIter};
use mors_traits::kms::KmsCipher;
use mors_traits::sstable::{
    BlockIndex, SSTableError, TableBuilderTrait, TableTrait,
};
use mors_traits::ts::{KeyTs, TxnTs};
use prost::Message;

use crate::block::Block;
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
#[derive(Debug, Clone)]

pub struct TableBuilder<C: CacheTrait<Block, TableIndexBuf>> {
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
    cache: Option<C>,
}
impl<C: CacheTrait<Block, TableIndexBuf>> Default for TableBuilder<C> {
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
            cache: None,
        }
    }
}

impl<C: CacheTrait<Block, TableIndexBuf>, K: KmsCipher>
    TableBuilderTrait<Table<C, K>, C, K>
    for TableBuilder<C>
{
    fn set_compression(&mut self, compression: CompressionType) {
        self.compression = compression;
    }

    fn set_dir(&mut self, dir: PathBuf) {
        self.dir = dir;
    }

    fn set_cache(&mut self, cache: C) {
        self.cache = Some(cache);
    }

    async fn open(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> std::result::Result<Option<Table<C, K>>, SSTableError> {
        Ok(self.open_impl(id, cipher).await?)
    }
}
impl<C: CacheTrait<Block, TableIndexBuf>> TableBuilder<C> {
    async fn open_impl<K: KmsCipher>(
        &self,
        id: SSTableId,
        cipher: Option<K>,
    ) -> Result<Option<Table<C, K>>> {
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
            TableBuilder::<C>::init_index(&mmap, &cipher)?;

        let (smallest, biggest) =
            self.smallest_biggest(&index_buf, &mmap, &cipher)?;

        let cheap_index = CheapTableIndex::from(&index_buf);
        let table = Table(
            TableInner {
                id,
                mmap,
                table_size,
                create_at,
                index_buf,
                index_start,
                index_len,
                smallest,
                biggest,
                cheap_index,
                cache: self.cache.clone(),
                cipher,
                checksum_verify_mode: self.checksum_verify_mode,
            }
            .into(),
        );
        match table.0.checksum_verify_mode {
            ChecksumVerificationMode::OnBlockRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {
                if let Err(e) = table.verify().await {
                    if let MorsTableError::ChecksumVerify(_, _) = &e {
                        error!(
                            "Ignore table {} checksum verify error: {}",
                            id, e
                        );
                        return Ok(None);
                    }
                    return Err(e);
                }
            }
            _ => {}
        }
        Ok(table.into())
    }

    fn init_index<K: KmsCipher>(
        mmap: &MmapFile,
        cipher: &Option<K>,
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

        let data = cipher
            .as_ref()
            .map(|c| c.decrypt(&data))
            .transpose()?
            .unwrap_or(data);

        let index_buf = TableIndexBuf::from_vec(data)?;

        debug_assert!(!index_buf.offsets().is_empty());

        Ok((index_buf, read_pos, index_len))
    }
    fn smallest_biggest<K: KmsCipher>(
        &self,
        index_buf: &TableIndexBuf,
        mmap: &MmapFile,
        cipher: &Option<K>,
    ) -> Result<(KeyTs, KeyTs)> {
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
            .as_ref()
            .map(|c| c.decrypt(data))
            .transpose()?
            .unwrap_or_else(|| data.to_vec());

        let uncompress_data = self.compression.decompress(plaintext)?;
        let block = Block::decode(
            0.into(),     //here don't care about it.
            0_u32.into(), //here don't care about it.
            last_block_offset.offset(),
            uncompress_data,
        )?;

        block.verify()?;

        let mut cache_block_iter = block.iter();
        debug_assert!(cache_block_iter.next_back()?);
        let biggest: KeyTs = cache_block_iter.key_back().unwrap().into();
        Ok((smallest, biggest))
    }
}

pub struct Table<C: CacheTrait<Block, TableIndexBuf>, K: KmsCipher>(
    Arc<TableInner<C, K>>,
);
pub(crate) struct TableInner<C: CacheTrait<Block, TableIndexBuf>, K: KmsCipher> {
    id: SSTableId,
    mmap: MmapFile,
    table_size: u64,
    create_at: SystemTime,
    index_buf: TableIndexBuf,
    index_start: usize,
    index_len: usize,
    smallest: KeyTs,
    biggest: KeyTs,
    cheap_index: CheapTableIndex,
    cache: Option<C>,
    cipher: Option<K>,
    checksum_verify_mode: ChecksumVerificationMode,
}
impl<C: CacheTrait<Block, TableIndexBuf>, K: KmsCipher> TableTrait<C, K>
    for Table<C, K>
{
    type Block = Block;
    type TableIndexBuf = TableIndexBuf;
    type ErrorType = MorsTableError;
    type TableBuilder = TableBuilder<C>;

    fn size(&self) -> usize {
        self.0.table_size as usize
    }

    fn stale_data_size(&self) -> usize {
        self.0.cheap_index.stale_data_size as usize
    }

    fn id(&self) -> SSTableId {
        self.0.id
    }

    fn smallest(&self) -> &KeyTs {
        &self.0.smallest
    }
}

impl<C: CacheTrait<Block, TableIndexBuf>, K: KmsCipher> Table<C, K> {
    async fn verify(&self) -> Result<()> {
        for i in 0..self.0.cheap_index.offsets_len {
            let block = self.get_block(i.into(), true).await?;

            match self.0.checksum_verify_mode {
                ChecksumVerificationMode::OnBlockRead
                | ChecksumVerificationMode::OnTableAndBlockRead => {}
                _ => {
                    block.verify()?;
                }
            }
        }

        Ok(())
    }
    async fn table_index(&self) -> Result<TableIndexBuf> {
        if self.0.cipher.is_none() {
            return Ok(self.0.index_buf.clone());
        }

        let mut data = vec![0; self.0.index_len];
        debug_assert_eq!(
            self.0.mmap.pread(&mut data, self.0.index_start)?,
            self.0.index_len
        );
        let index_buf = TableIndexBuf::from_vec(
            self.0.cipher.as_ref().unwrap().decrypt(&data)?,
        )?;
        if let Some(c) = self.0.cache.as_ref() {
            c.insert_index(self.0.id, index_buf.clone()).await;
        }
        Ok(index_buf)
    }
    async fn get_block(
        &self,
        block_index: BlockIndex,
        insert_cache: bool,
    ) -> Result<Block> {
        if block_index >= self.0.cheap_index.offsets_len.into() {
            return Err(MorsTableError::BlockIndexOutOfRange);
        }
        let key: BlockCacheKey = (self.0.id, block_index).into();

        if let Some(c) = self.0.cache.as_ref() {
            if let Some(b) = c.get_block(&key).await {
                return Ok(b);
            };
        }

        let table_index = self.table_index().await?;

        let block_id: usize = block_index.into();
        let block = &table_index.offsets()[block_id];

        let raw_data_ref = self
            .0
            .mmap
            .pread_ref(block.offset() as usize, block.size() as usize);
        let data = self
            .0
            .cipher
            .as_ref()
            .map(|c| c.decrypt(raw_data_ref))
            .transpose()?
            .unwrap_or_else(|| raw_data_ref.to_vec());

        let block =
            Block::decode(self.0.id, block_index, block.offset(), data)?;

        match self.0.checksum_verify_mode {
            ChecksumVerificationMode::OnBlockRead
            | ChecksumVerificationMode::OnTableAndBlockRead => {
                block.verify()?;
            }
            _ => {}
        }

        if insert_cache {
            if let Some(c) = self.0.cache.as_ref() {
                c.insert_block(key, block.clone()).await;
            }
        }
        Ok(block)
    }
}
struct CheapTableIndex {
    max_version: TxnTs,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    stale_data_size: u32,
    offsets_len: usize,
    bloom_filter_len: usize,
}
impl From<&TableIndexBuf> for CheapTableIndex {
    fn from(value: &TableIndexBuf) -> Self {
        Self {
            max_version: value.max_version().into(),
            key_count: value.key_count(),
            uncompressed_size: value.uncompressed_size(),
            on_disk_size: value.on_disk_size(),
            stale_data_size: value.stale_data_size(),
            offsets_len: value.offsets().len(),
            bloom_filter_len: value
                .bloom_filter()
                .map(|x| x.len())
                .unwrap_or(0),
        }
    }
}
