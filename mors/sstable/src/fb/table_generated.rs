// automatically generated by the FlatBuffers compiler, do not modify


// @generated

use core::mem;
use core::cmp::Ordering;

extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};

pub enum TableIndexOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct TableIndex<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for TableIndex<'a> {
  type Inner = TableIndex<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> TableIndex<'a> {
  pub const VT_OFFSETS: flatbuffers::VOffsetT = 4;
  pub const VT_BLOOM_FILTER: flatbuffers::VOffsetT = 6;
  pub const VT_MAX_VERSION: flatbuffers::VOffsetT = 8;
  pub const VT_KEY_COUNT: flatbuffers::VOffsetT = 10;
  pub const VT_UNCOMPRESSED_SIZE: flatbuffers::VOffsetT = 12;
  pub const VT_ON_DISK_SIZE: flatbuffers::VOffsetT = 14;
  pub const VT_STALE_DATA_SIZE: flatbuffers::VOffsetT = 16;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    TableIndex { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr, A: flatbuffers::Allocator + 'bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
    args: &'args TableIndexArgs<'args>
  ) -> flatbuffers::WIPOffset<TableIndex<'bldr>> {
    let mut builder = TableIndexBuilder::new(_fbb);
    builder.add_max_version(args.max_version);
    builder.add_stale_data_size(args.stale_data_size);
    builder.add_on_disk_size(args.on_disk_size);
    builder.add_uncompressed_size(args.uncompressed_size);
    builder.add_key_count(args.key_count);
    if let Some(x) = args.bloom_filter { builder.add_bloom_filter(x); }
    if let Some(x) = args.offsets { builder.add_offsets(x); }
    builder.finish()
  }


  #[inline]
  pub fn offsets(&self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockOffset<'a>>>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockOffset>>>>(TableIndex::VT_OFFSETS, None)}
  }
  #[inline]
  pub fn bloom_filter(&self) -> Option<flatbuffers::Vector<'a, u8>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(TableIndex::VT_BLOOM_FILTER, None)}
  }
  #[inline]
  pub fn max_version(&self) -> u64 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u64>(TableIndex::VT_MAX_VERSION, Some(0)).unwrap()}
  }
  #[inline]
  pub fn key_count(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(TableIndex::VT_KEY_COUNT, Some(0)).unwrap()}
  }
  #[inline]
  pub fn uncompressed_size(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(TableIndex::VT_UNCOMPRESSED_SIZE, Some(0)).unwrap()}
  }
  #[inline]
  pub fn on_disk_size(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(TableIndex::VT_ON_DISK_SIZE, Some(0)).unwrap()}
  }
  #[inline]
  pub fn stale_data_size(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(TableIndex::VT_STALE_DATA_SIZE, Some(0)).unwrap()}
  }
}

impl flatbuffers::Verifiable for TableIndex<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<BlockOffset>>>>("offsets", Self::VT_OFFSETS, false)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>("bloom_filter", Self::VT_BLOOM_FILTER, false)?
     .visit_field::<u64>("max_version", Self::VT_MAX_VERSION, false)?
     .visit_field::<u32>("key_count", Self::VT_KEY_COUNT, false)?
     .visit_field::<u32>("uncompressed_size", Self::VT_UNCOMPRESSED_SIZE, false)?
     .visit_field::<u32>("on_disk_size", Self::VT_ON_DISK_SIZE, false)?
     .visit_field::<u32>("stale_data_size", Self::VT_STALE_DATA_SIZE, false)?
     .finish();
    Ok(())
  }
}
pub struct TableIndexArgs<'a> {
    pub offsets: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockOffset<'a>>>>>,
    pub bloom_filter: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    pub max_version: u64,
    pub key_count: u32,
    pub uncompressed_size: u32,
    pub on_disk_size: u32,
    pub stale_data_size: u32,
}
impl<'a> Default for TableIndexArgs<'a> {
  #[inline]
  fn default() -> Self {
    TableIndexArgs {
      offsets: None,
      bloom_filter: None,
      max_version: 0,
      key_count: 0,
      uncompressed_size: 0,
      on_disk_size: 0,
      stale_data_size: 0,
    }
  }
}

pub struct TableIndexBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> TableIndexBuilder<'a, 'b, A> {
  #[inline]
  pub fn add_offsets(&mut self, offsets: flatbuffers::WIPOffset<flatbuffers::Vector<'b , flatbuffers::ForwardsUOffset<BlockOffset<'b >>>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(TableIndex::VT_OFFSETS, offsets);
  }
  #[inline]
  pub fn add_bloom_filter(&mut self, bloom_filter: flatbuffers::WIPOffset<flatbuffers::Vector<'b , u8>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(TableIndex::VT_BLOOM_FILTER, bloom_filter);
  }
  #[inline]
  pub fn add_max_version(&mut self, max_version: u64) {
    self.fbb_.push_slot::<u64>(TableIndex::VT_MAX_VERSION, max_version, 0);
  }
  #[inline]
  pub fn add_key_count(&mut self, key_count: u32) {
    self.fbb_.push_slot::<u32>(TableIndex::VT_KEY_COUNT, key_count, 0);
  }
  #[inline]
  pub fn add_uncompressed_size(&mut self, uncompressed_size: u32) {
    self.fbb_.push_slot::<u32>(TableIndex::VT_UNCOMPRESSED_SIZE, uncompressed_size, 0);
  }
  #[inline]
  pub fn add_on_disk_size(&mut self, on_disk_size: u32) {
    self.fbb_.push_slot::<u32>(TableIndex::VT_ON_DISK_SIZE, on_disk_size, 0);
  }
  #[inline]
  pub fn add_stale_data_size(&mut self, stale_data_size: u32) {
    self.fbb_.push_slot::<u32>(TableIndex::VT_STALE_DATA_SIZE, stale_data_size, 0);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>) -> TableIndexBuilder<'a, 'b, A> {
    let start = _fbb.start_table();
    TableIndexBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<TableIndex<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for TableIndex<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("TableIndex");
      ds.field("offsets", &self.offsets());
      ds.field("bloom_filter", &self.bloom_filter());
      ds.field("max_version", &self.max_version());
      ds.field("key_count", &self.key_count());
      ds.field("uncompressed_size", &self.uncompressed_size());
      ds.field("on_disk_size", &self.on_disk_size());
      ds.field("stale_data_size", &self.stale_data_size());
      ds.finish()
  }
}
pub enum BlockOffsetOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct BlockOffset<'a> {
  pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for BlockOffset<'a> {
  type Inner = BlockOffset<'a>;
  #[inline]
  unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
    Self { _tab: flatbuffers::Table::new(buf, loc) }
  }
}

impl<'a> BlockOffset<'a> {
  pub const VT_KEY_TS: flatbuffers::VOffsetT = 4;
  pub const VT_OFFSET: flatbuffers::VOffsetT = 6;
  pub const VT_LEN: flatbuffers::VOffsetT = 8;

  #[inline]
  pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
    BlockOffset { _tab: table }
  }
  #[allow(unused_mut)]
  pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr, A: flatbuffers::Allocator + 'bldr>(
    _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
    args: &'args BlockOffsetArgs<'args>
  ) -> flatbuffers::WIPOffset<BlockOffset<'bldr>> {
    let mut builder = BlockOffsetBuilder::new(_fbb);
    builder.add_len(args.len);
    builder.add_offset(args.offset);
    if let Some(x) = args.key_ts { builder.add_key_ts(x); }
    builder.finish()
  }


  #[inline]
  pub fn key_ts(&self) -> Option<flatbuffers::Vector<'a, u8>> {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(BlockOffset::VT_KEY_TS, None)}
  }
  #[inline]
  pub fn offset(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(BlockOffset::VT_OFFSET, Some(0)).unwrap()}
  }
  #[inline]
  pub fn len(&self) -> u32 {
    // Safety:
    // Created from valid Table for this object
    // which contains a valid value in this slot
    unsafe { self._tab.get::<u32>(BlockOffset::VT_LEN, Some(0)).unwrap()}
  }
}

impl flatbuffers::Verifiable for BlockOffset<'_> {
  #[inline]
  fn run_verifier(
    v: &mut flatbuffers::Verifier, pos: usize
  ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
    use self::flatbuffers::Verifiable;
    v.visit_table(pos)?
     .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>("key_ts", Self::VT_KEY_TS, false)?
     .visit_field::<u32>("offset", Self::VT_OFFSET, false)?
     .visit_field::<u32>("len", Self::VT_LEN, false)?
     .finish();
    Ok(())
  }
}
pub struct BlockOffsetArgs<'a> {
    pub key_ts: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
    pub offset: u32,
    pub len: u32,
}
impl<'a> Default for BlockOffsetArgs<'a> {
  #[inline]
  fn default() -> Self {
    BlockOffsetArgs {
      key_ts: None,
      offset: 0,
      len: 0,
    }
  }
}

pub struct BlockOffsetBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
  fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
  start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> BlockOffsetBuilder<'a, 'b, A> {
  #[inline]
  pub fn add_key_ts(&mut self, key_ts: flatbuffers::WIPOffset<flatbuffers::Vector<'b , u8>>) {
    self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(BlockOffset::VT_KEY_TS, key_ts);
  }
  #[inline]
  pub fn add_offset(&mut self, offset: u32) {
    self.fbb_.push_slot::<u32>(BlockOffset::VT_OFFSET, offset, 0);
  }
  #[inline]
  pub fn add_len(&mut self, len: u32) {
    self.fbb_.push_slot::<u32>(BlockOffset::VT_LEN, len, 0);
  }
  #[inline]
  pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>) -> BlockOffsetBuilder<'a, 'b, A> {
    let start = _fbb.start_table();
    BlockOffsetBuilder {
      fbb_: _fbb,
      start_: start,
    }
  }
  #[inline]
  pub fn finish(self) -> flatbuffers::WIPOffset<BlockOffset<'a>> {
    let o = self.fbb_.end_table(self.start_);
    flatbuffers::WIPOffset::new(o.value())
  }
}

impl core::fmt::Debug for BlockOffset<'_> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    let mut ds = f.debug_struct("BlockOffset");
      ds.field("key_ts", &self.key_ts());
      ds.field("offset", &self.offset());
      ds.field("len", &self.len());
      ds.finish()
  }
}
#[inline]
/// Verifies that a buffer of bytes contains a `BlockOffset`
/// and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_block_offset_unchecked`.
pub fn root_as_block_offset(buf: &[u8]) -> Result<BlockOffset, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::root::<BlockOffset>(buf)
}
#[inline]
/// Verifies that a buffer of bytes contains a size prefixed
/// `BlockOffset` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `size_prefixed_root_as_block_offset_unchecked`.
pub fn size_prefixed_root_as_block_offset(buf: &[u8]) -> Result<BlockOffset, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::size_prefixed_root::<BlockOffset>(buf)
}
#[inline]
/// Verifies, with the given options, that a buffer of bytes
/// contains a `BlockOffset` and returns it.
/// Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_block_offset_unchecked`.
pub fn root_as_block_offset_with_opts<'b, 'o>(
  opts: &'o flatbuffers::VerifierOptions,
  buf: &'b [u8],
) -> Result<BlockOffset<'b>, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::root_with_opts::<BlockOffset<'b>>(opts, buf)
}
#[inline]
/// Verifies, with the given verifier options, that a buffer of
/// bytes contains a size prefixed `BlockOffset` and returns
/// it. Note that verification is still experimental and may not
/// catch every error, or be maximally performant. For the
/// previous, unchecked, behavior use
/// `root_as_block_offset_unchecked`.
pub fn size_prefixed_root_as_block_offset_with_opts<'b, 'o>(
  opts: &'o flatbuffers::VerifierOptions,
  buf: &'b [u8],
) -> Result<BlockOffset<'b>, flatbuffers::InvalidFlatbuffer> {
  flatbuffers::size_prefixed_root_with_opts::<BlockOffset<'b>>(opts, buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a BlockOffset and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid `BlockOffset`.
pub unsafe fn root_as_block_offset_unchecked(buf: &[u8]) -> BlockOffset {
  flatbuffers::root_unchecked::<BlockOffset>(buf)
}
#[inline]
/// Assumes, without verification, that a buffer of bytes contains a size prefixed BlockOffset and returns it.
/// # Safety
/// Callers must trust the given bytes do indeed contain a valid size prefixed `BlockOffset`.
pub unsafe fn size_prefixed_root_as_block_offset_unchecked(buf: &[u8]) -> BlockOffset {
  flatbuffers::size_prefixed_root_unchecked::<BlockOffset>(buf)
}
#[inline]
pub fn finish_block_offset_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(
    fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
    root: flatbuffers::WIPOffset<BlockOffset<'a>>) {
  fbb.finish(root, None);
}

#[inline]
pub fn finish_size_prefixed_block_offset_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>, root: flatbuffers::WIPOffset<BlockOffset<'a>>) {
  fbb.finish_size_prefixed(root, None);
}
