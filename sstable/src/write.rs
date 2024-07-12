use std::mem::replace;
use std::sync::atomic::{AtomicUsize, Ordering};

use mors_traits::{kms::Kms, kv::ValueMeta, ts::KeyTsBorrow};
use tokio::task::spawn_blocking;

use crate::pb::proto::checksum;
use crate::Result;
use crate::{block::write::BlockWriter, table::TableBuilder};
pub(crate) struct TableWriter<K: Kms> {
    tablebuilder: TableBuilder,
    block_writer: BlockWriter,
    stale_data_size: usize,
    len_offsets: usize,
    uncompressed_size: AtomicUsize,
    cipher: Option<K::Cipher>,
}

impl<K: Kms> TableWriter<K> {
    pub(crate) fn new(
        builder: TableBuilder,
        cipher: Option<K::Cipher>,
    ) -> Self {
        let block_writer = BlockWriter::new(builder.block_size());
        Self {
            tablebuilder: builder,
            cipher,
            block_writer,
            stale_data_size: 0,
            uncompressed_size: AtomicUsize::new(0),
            len_offsets: 0,
        }
    }
    pub(crate) fn push(
        &mut self,
        key: &KeyTsBorrow,
        value: &ValueMeta,
        vptr_len: Option<u32>,
    ) -> Result<()> {
        Ok(())
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
                self.stale_data_size += key.len() + 4;
            }
        }
        // self.block_writer.push_entry::<K>(key, value,vptr_len,is_stale);
    }
    fn finish_block(&mut self) -> Result<()> {
        if self.block_writer.entry_offsets().is_empty() {
            return Ok(());
        }

        self.block_writer.finish_block(self.checksum_algo());
        self.uncompressed_size
            .fetch_add(self.block_writer.data().len(), Ordering::AcqRel);
        self.len_offsets +=
            (self.block_writer.base_keyts().len() as f32 / 4.0) as usize + 4;

        let mut finished_block = replace(
            &mut self.block_writer,
            BlockWriter::new(self.tablebuilder.block_size()),
        );
        spawn_blocking(move ||{});
        Ok(())
    }
    fn block_size(&self) -> usize {
        self.tablebuilder.block_size()
    }
    fn checksum_algo(&self) -> checksum::Algorithm {
        self.tablebuilder.checksum_algo()
    }
}
