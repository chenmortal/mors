use std::{
    mem::replace,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
};

use flatbuffers::FlatBufferBuilder;
use memmap2::Advice;
use mors_common::{
    bloom::Bloom,
    compress::CompressionType,
    file_id::{FileId, SSTableId},
    kv::{Meta, ValueMeta, ValuePointer},
    mmap::MmapFileBuilder,
    rayon::{self, AsyncRayonHandle},
    ts::{KeyTsBorrow, TxnTs},
};
use mors_traits::{
    default::WithDir,
    file::Append,
    iter::{CacheIterator, KvCacheIter},
    kms::KmsCipher,
    sstable::{SSTableError, TableWriterTrait},
};
use prost::Message;
use tokio::task::spawn_blocking;

use crate::error::MorsTableError;
use crate::fb::table_generated::{
    BlockOffset, BlockOffsetArgs, TableIndex, TableIndexArgs,
};
use crate::pb::proto::{checksum, Checksum};
use crate::Result;
use crate::{block::write::BlockWriter, table::TableBuilder};
pub struct TableWriter<K: KmsCipher> {
    tablebuilder: TableBuilder<K>,
    block_writer: BlockWriter,
    stale_data_size: u32,
    len_offsets: usize,
    uncompressed_size: AtomicU32,
    comressed_size: Arc<AtomicUsize>,
    cipher: Option<Arc<K>>,
    compress_task: Vec<AsyncRayonHandle<Result<BlockWriter>>>,
    key_hashes: Vec<u32>,
    max_version: TxnTs,
    on_disk_size: u32,
}
impl<K: KmsCipher> TableWriterTrait for TableWriter<K> {
    fn reached_capacity(&self) -> bool {
        let mut sum_block_sizes =
            self.comressed_size.load(Ordering::Acquire) as u32;
        if self.tablebuilder.compression() == CompressionType::None
            && self.cipher.is_none()
        {
            sum_block_sizes = self.uncompressed_size.load(Ordering::Acquire);
        }
        let blocks_size = sum_block_sizes
            + (self.block_writer.entry_offsets().len() * 4) as u32
            + 4
            + 8
            + 4;
        let estimate_size = blocks_size + 4 + self.len_offsets as u32;
        estimate_size as usize > self.tablebuilder.table_capacity()
    }
    fn push(
        &mut self,
        key: &KeyTsBorrow,
        value: &ValueMeta,
        vptr_len: Option<u32>,
    ) {
        self.push_internal(key, value, vptr_len, false);
    }
    fn push_stale(
        &mut self,
        key: &KeyTsBorrow,
        value: &ValueMeta,
        vptr_len: Option<u32>,
    ) {
        self.stale_data_size +=
            key.len() as u32 + value.value().len() as u32 + 4;
        self.push_internal(key, value, vptr_len, true);
    }

    async fn flush_to_disk(
        &mut self,
        path: PathBuf,
    ) -> std::result::Result<(), SSTableError> {
        let build_data = self.done().await?;

        fn write_data(path: PathBuf, data: TableBuildData) -> Result<()> {
            let mut builder = MmapFileBuilder::new();
            builder.advice(Advice::Sequential);
            builder.create_new(true).append(true).read(true);
            let mut mmap = builder.build(path, data.size)?;
            let mut offset = 0;
            data.write(&mut offset, &mut mmap)?;
            assert_eq!(offset, data.size as usize);
            mmap.flush_range(0, offset)?;
            mmap.set_len(data.size as usize)?;
            mmap.sync_all()?;
            Ok(())
        }
        spawn_blocking(move || write_data(path, build_data))
            .await
            .map_err(MorsTableError::from)??;
        Ok(())
    }
}
impl<K: KmsCipher> TableWriter<K> {
    pub(crate) fn new(builder: TableBuilder<K>, cipher: Option<K>) -> Self {
        let block_writer = BlockWriter::new(builder.block_size());
        Self {
            tablebuilder: builder,
            cipher: cipher.map(Arc::new),
            block_writer,
            stale_data_size: 0,
            uncompressed_size: Default::default(),
            len_offsets: 0,
            comressed_size: Arc::new(AtomicUsize::new(0)),
            compress_task: Vec::new(),
            key_hashes: Vec::new(),
            max_version: TxnTs::default(),
            on_disk_size: 0,
        }
    }

    fn push_internal(
        &mut self,
        key: &KeyTsBorrow,
        value: &ValueMeta,
        vptr_len: Option<u32>,
        is_stale: bool,
    ) {
        if self.block_writer.should_finish_block::<K>(
            key,
            value,
            self.tablebuilder.block_size(),
            self.cipher.is_some(),
        ) {
            if is_stale {
                self.stale_data_size += key.len() as u32 + 4;
            }
            self.finish_block();
        }
        self.key_hashes.push(Bloom::hash(key.key()));
        self.max_version = self.max_version.max(key.txn_ts());
        self.block_writer.push_entry(key, value);
        self.on_disk_size += vptr_len.unwrap_or(0);
        // self.block_writer.push_entry::<K>(key, value,vptr_len,is_stale);
    }
    fn finish_block(&mut self) {
        if self.block_writer.entry_offsets().is_empty() {
            return;
        }

        self.block_writer.finish_block(self.checksum_algo());
        self.uncompressed_size
            .fetch_add(self.block_writer.data().len() as u32, Ordering::AcqRel);
        self.len_offsets +=
            (self.block_writer.base_keyts().len() as f32 / 4.0) as usize + 4;

        let mut finished_block = replace(
            &mut self.block_writer,
            BlockWriter::new(self.tablebuilder.block_size()),
        );

        let compression = self.compression();
        let cipher = self.cipher.clone();
        let compressed_size = self.comressed_size.clone();
        let handle = rayon::spawn(move || -> Result<BlockWriter> {
            if !compression.is_none() {
                finished_block
                    .set_data(compression.compress(finished_block.data())?);
            }
            if let Some(cipher) = cipher.as_ref() {
                finished_block.set_data(cipher.encrypt(finished_block.data())?);
            }
            compressed_size
                .fetch_add(finished_block.data().len(), Ordering::Relaxed);
            Ok(finished_block)
        });
        self.compress_task.push(handle);
    }
    async fn done(&mut self) -> Result<TableBuildData> {
        self.finish_block();
        let mut block_list = Vec::with_capacity(self.compress_task.len());
        for task in self.compress_task.drain(..) {
            block_list.push(task.await?);
        }
        let bloom = self.tablebuilder.create_bloom(&self.key_hashes);
        let (index, data_size) =
            self.build_index(&block_list, bloom.as_ref())?;
        let checksum =
            Checksum::new(self.checksum_algo(), &index).encode_to_vec();
        let size = data_size as u64
            + index.len() as u64
            + 4
            + checksum.len() as u64
            + 4;
        let data = TableBuildData {
            block_list,
            index,
            checksum,
            size,
        };
        Ok(data)
    }
    fn build_index(
        &mut self,
        block_list: &Vec<BlockWriter>,
        bloom: Option<&Bloom>,
    ) -> Result<(Vec<u8>, u32)> {
        let mut builder = FlatBufferBuilder::with_capacity(3 << 20);
        let mut data_size = 0;
        let mut block_offset = Vec::new();
        for block in block_list {
            let args = BlockOffsetArgs {
                key_ts: builder
                    .create_vector(block.base_keyts().as_ref())
                    .into(),
                offset: data_size,
                len: block.data().len() as u32,
            };

            data_size += block.data().len() as u32;
            block_offset.push(BlockOffset::create(&mut builder, &args));
        }
        self.on_disk_size += data_size;
        let table_index_args = TableIndexArgs {
            offsets: builder.create_vector(&block_offset).into(),
            bloom_filter: bloom.and_then(|x| builder.create_vector(x).into()),
            max_version: self.max_version.to_u64(),
            key_count: self.key_hashes.len() as u32,
            uncompressed_size: self.uncompressed_size.load(Ordering::Acquire),
            on_disk_size: self.on_disk_size,
            stale_data_size: self.stale_data_size,
        };
        let table_index = TableIndex::create(&mut builder, &table_index_args);
        builder.finish(table_index, None);

        let data = match &self.cipher {
            Some(c) => c.encrypt(builder.finished_data())?,
            None => builder.finished_data().to_vec(),
        };
        Ok((data, data_size))
    }
    fn is_empty(&self) -> bool {
        self.key_hashes.len() == 0
    }
    fn block_size(&self) -> usize {
        self.tablebuilder.block_size()
    }
    fn checksum_algo(&self) -> checksum::Algorithm {
        self.tablebuilder.checksum_algo()
    }
    fn compression(&self) -> CompressionType {
        self.tablebuilder.compression()
    }
}
impl<K: KmsCipher> TableBuilder<K> {
    pub(crate) async fn build_l0_impl<
        I: KvCacheIter<V> + CacheIterator,
        V: Into<ValueMeta>,
    >(
        &self,
        mut iter: I,
        next_id: Arc<AtomicU32>,
        cipher: Option<K>,
    ) -> Result<Option<SSTableId>> {
        let mut writer = TableWriter::new(self.clone(), cipher);
        while iter.next()? {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                let vptr_size = v
                    .meta()
                    .contains(Meta::VALUE_POINTER)
                    .then(|| ValuePointer::decode(v.value()))
                    .flatten()
                    .map(|vp| vp.size());
                writer.push(&k, &v, vptr_size);
            }
        }
        if writer.is_empty() {
            return Ok(None);
        }
        let id: SSTableId = next_id.fetch_add(1, Ordering::SeqCst).into();
        let path = id.join_dir(self.dir());

        writer.flush_to_disk(path).await?;
        Ok(Some(id))
    }
}
struct TableBuildData {
    block_list: Vec<BlockWriter>,
    index: Vec<u8>,
    checksum: Vec<u8>,
    size: u64,
}
impl TableBuildData {
    fn write<W: Append>(
        &self,
        offset: &mut usize,
        writer: &mut W,
    ) -> Result<()> {
        for block in self.block_list.iter() {
            *offset += writer.append(*offset, block.data())?;
        }
        *offset += writer.append(*offset, &self.index)?;
        *offset +=
            writer.append(*offset, &(self.index.len() as u32).to_be_bytes())?;
        *offset += writer.append(*offset, &self.checksum)?;
        *offset += writer
            .append(*offset, &(self.checksum.len() as u32).to_be_bytes())?;
        Ok(())
    }
}
