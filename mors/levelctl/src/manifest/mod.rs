use error::ManifestError;
use log::info;
use manifest_change::{
    manifest_change::Operation, EncryptionAlgo, ManifestChange,
};
use mors_common::compress::CompressionType;
use mors_traits::{
    file_id::{FileId, SSTableId},
    kms::CipherKeyId,
    levelctl::Level,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{remove_file, rename, File, OpenOptions},
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::PathBuf,
    sync::Arc,
};

use bytes::{Buf, BufMut};
use mors_traits::default::DEFAULT_DIR;
use prost::Message;

use crate::manifest::manifest_change::ManifestChangeSet;
use parking_lot::Mutex;
pub mod error;
pub(crate) mod manifest_change;

const MANIFEST_FILE_NAME: &str = "MANIFEST";
const MANIFEST_REWRITE_FILE_NAME: &str = "MANIFEST-REWRITE";
const DELETIONS_REWRITE_THRESHOLD: usize = 10_000;
const DELETIONS_RATIO: usize = 10;
const MAGIC_VERSION: u16 = 1;
const MAGIC_TEXT: &[u8; 4] = b"Mors";

type Result<T> = std::result::Result<T, ManifestError>;
#[derive(Clone)]
pub(crate) struct Manifest(Arc<Mutex<ManifestInner>>);
impl Deref for Manifest {
    type Target = Mutex<ManifestInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub(crate) struct ManifestInner {
    file: File,
    deletions_rewrite_threshold: usize,
    info: ManifestInfo,
}
#[derive(Debug, Default)]
pub(crate) struct ManifestInfo {
    levels: Vec<LevelManifest>,
    tables: HashMap<SSTableId, TableManifest>,
    creations: usize,
    deletions: usize,
}
#[derive(Debug, Default, Clone)]
pub(crate) struct LevelManifest {
    tables: HashSet<SSTableId>,
}
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TableManifest {
    level: Level,
    key_id: CipherKeyId,
    compress: CompressionType,
}

pub(crate) struct ManifestBuilder {
    dir: PathBuf,
    read_only: bool,
    // Magic version used by the application using badger to ensure that it doesn't open the DB
    // with incompatible data format.
    external_magic_version: u16,
}

impl Default for ManifestBuilder {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(DEFAULT_DIR),
            read_only: false,
            external_magic_version: 0,
        }
    }
}
impl ManifestBuilder {
    pub(crate) fn build(&self) -> Result<Manifest> {
        let path = self.dir.join(MANIFEST_FILE_NAME);
        match OpenOptions::new()
            .read(true)
            .write(!self.read_only)
            .open(path)
        {
            Ok(mut file) => {
                let (info, trunc_offset) = self.replay(&file)?;
                if !self.read_only {
                    file.set_len(trunc_offset)?;
                }
                file.seek(SeekFrom::End(0))?;

                let manifest = Manifest(
                    Mutex::new(ManifestInner {
                        file,
                        info,
                        deletions_rewrite_threshold:
                            DELETIONS_REWRITE_THRESHOLD,
                    })
                    .into(),
                );
                Ok(manifest)
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    if self.read_only {
                        return Err(ManifestError::NoManifest);
                    }
                    let info = ManifestInfo::default();
                    let (file, table_creations) = self.help_rewrite(&info)?;
                    assert_eq!(table_creations, 0);
                    let manifest = Manifest(
                        Mutex::new(ManifestInner {
                            file,
                            info,
                            deletions_rewrite_threshold:
                                DELETIONS_REWRITE_THRESHOLD,
                        })
                        .into(),
                    );
                    Ok(manifest)
                } else {
                    Err(e.into())
                }
            }
        }
    }
    fn replay(&self, file: &File) -> Result<(ManifestInfo, u64)> {
        let mut reader = BufReader::new(file);
        let mut magic_buf = [0; 8];
        let mut offset: u64 = 0;

        offset += reader
            .read(&mut magic_buf)
            .map_err(|_| ManifestError::BadMagic)? as u64;
        if &magic_buf[0..4] != MAGIC_TEXT {
            return Err(ManifestError::BadMagic);
        }

        let mut buf = &magic_buf[4..];
        let ext_version = buf.get_u16();
        if ext_version != self.external_magic_version {
            return Err(ManifestError::BadExternalMagicVersion(
                ext_version,
                self.external_magic_version,
            ));
        }
        let version = buf.get_u16();
        if version != MAGIC_VERSION {
            return Err(ManifestError::BadVersion(version));
        }

        let file_size = file.metadata()?.len();

        let mut manifest = ManifestInfo::default();
        loop {
            let mut read_size = 0;
            let mut crc_len = [0; 8];
            if let Err(e) = reader.read_exact(&mut crc_len) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }
            read_size += 8;

            let mut crc_len_ref = crc_len.as_ref();
            let change_len = crc_len_ref.get_u32() as usize;
            let crc = crc_len_ref.get_u32();

            if offset + change_len as u64 > file_size {
                return Err(ManifestError::Corrupted(offset, 8));
            }

            let mut change_buf = vec![0; change_len];
            if let Err(e) = reader.read_exact(&mut change_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e.into());
            }
            read_size += change_len;

            if crc32fast::hash(&change_buf) != crc {
                return Err(ManifestError::CheckSumMismatch);
            }

            offset += read_size as u64;
            let change_set = ManifestChangeSet::decode(change_buf.as_slice())?;
            manifest.apply_change_set(&change_set)?;
        }

        Ok((manifest, offset))
    }
    fn help_rewrite(&self, manifest: &ManifestInfo) -> Result<(File, usize)> {
        let rewrite_path = self.dir.join(MANIFEST_REWRITE_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&rewrite_path)?;
        // magic bytes are structured as
        // +---------------------+-------------------------+-----------------------+
        // | magicText (4 bytes) | externalMagic (2 bytes) | MorsMagic (2 bytes) |
        // +---------------------+-------------------------+-----------------------+
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(MAGIC_TEXT);
        buf.put_u16(self.external_magic_version);
        buf.put_u16(MAGIC_VERSION);

        let table_creations = manifest.tables.len();
        let changes = manifest.as_changes();
        let set = ManifestChangeSet { changes };
        let change_set_buf = set.encode_to_vec();

        let mut crc_len = Vec::with_capacity(8);
        crc_len.put_u32(change_set_buf.len() as u32);
        crc_len.put_u32(crc32fast::hash(&change_set_buf));

        buf.extend_from_slice(&crc_len);
        buf.extend_from_slice(&change_set_buf);
        file.write_all(&buf)?;
        file.sync_data()?;

        let manifest_path = self.dir.join(MANIFEST_FILE_NAME);
        rename(rewrite_path, &manifest_path)?;
        file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(manifest_path)?;
        file.seek(SeekFrom::End(0))?;
        file.sync_all()?;
        Ok((file, table_creations))
    }
}
impl ManifestInfo {
    fn as_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::with_capacity(self.tables.len());
        for (id, manifest) in self.tables.iter() {
            changes.push(ManifestChange::new_create(
                *id,
                manifest.level,
                manifest.key_id,
                manifest.compress,
            ));
        }
        changes
    }
    fn apply_change_set(
        &mut self,
        change_set: &ManifestChangeSet,
    ) -> Result<()> {
        for change in change_set.changes.iter() {
            self.apply_manifest_change(change)?;
        }
        Ok(())
    }
    fn apply_manifest_change(&mut self, change: &ManifestChange) -> Result<()> {
        match change.op() {
            Operation::Create => {
                if self.tables.contains_key(&change.table_id()) {
                    return Err(ManifestError::CreateError(change.table_id()));
                };
                self.tables.insert(
                    change.table_id(),
                    TableManifest {
                        level: change.level.into(),
                        key_id: change.key_id.into(),
                        compress: change.compression.into(),
                    },
                );

                if self.levels.len() <= change.level as usize {
                    self.levels.push(LevelManifest::default());
                }
                self.levels[change.level as usize]
                    .tables
                    .insert(change.table_id());
                self.creations += 1;
            }
            Operation::Delete => {
                if !self.tables.contains_key(&change.table_id()) {
                    return Err(ManifestError::DeleteError(change.table_id()));
                };
                self.levels[change.level as usize]
                    .tables
                    .remove(&change.table_id());
                self.tables.remove(&change.table_id());
                self.deletions += 1;
            }
        }
        Ok(())
    }
}
impl ManifestChange {
    pub fn new_create(
        table_id: SSTableId,
        level: Level,
        cipher_key_id: CipherKeyId,
        compression: CompressionType,
    ) -> Self {
        Self {
            id: table_id.into(),
            op: Operation::Create as i32,
            level: level.into(),
            key_id: cipher_key_id.into(),
            encryption_algo: EncryptionAlgo::Aes as i32,
            compression: compression.into(),
        }
    }
    pub fn new_delete(table_id: SSTableId) -> Self {
        Self {
            id: table_id.into(),
            op: Operation::Delete as i32,
            level: Default::default(),
            key_id: Default::default(),
            encryption_algo: Default::default(),
            compression: Default::default(),
        }
    }
    pub fn table_id(&self) -> SSTableId {
        self.id.into()
    }
}
impl Manifest {
    pub(crate) fn revert(&self, dir: &PathBuf) -> Result<()> {
        let sst_id_set = SSTableId::parse_set_from_dir(dir);
        let mut inner = self.lock();
        let info = &mut inner.info;

        //check all files in manifest exist;
        for (id, _) in info.tables.iter() {
            if !sst_id_set.contains(id) {
                return Err(ManifestError::TableNotFound(*id));
            }
        }
        //delete files that shouldn't exist
        for id in sst_id_set {
            if !info.tables.contains_key(&id) {
                info!(
                    "Table file {} not referenced in Manifest, Deleting it",
                    id
                );
                let sst_path = id.join_dir(dir);
                remove_file(sst_path)?;
            };
        }
        Ok(())
    }
}
impl ManifestInner {
    pub(crate) fn tables(&self)->&HashMap<SSTableId, TableManifest>{
        &self.info.tables
    }
}
impl TableManifest {
    pub(crate) fn compress(&self) -> CompressionType {
        self.compress
    }
    pub(crate) fn key_id(&self) -> CipherKeyId {
        self.key_id
    }
    pub(crate) fn level(&self) -> Level {
        self.level
    }
}