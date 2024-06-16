use std::collections::HashMap;
use std::fs::{File, OpenOptions, rename};
use std::io::{BufReader, Read, Seek, Write};
use std::ops::Deref;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use log::error;
use prost::bytes::{Buf, BufMut};
use prost::Message;

use mors_traits::default::DEFAULT_DIR;
use mors_traits::ts::PhyTs;

use crate::{KEY_REGISTRY_FILE_NAME, KEY_REGISTRY_REWRITE_FILE_NAME, SANITY_TEXT};
use crate::cipher::{AesCipher, CipherKeyId, Nonce};
use crate::error::EncryptError;
use crate::pb::encryption::DataKey;

type Result<T> = std::result::Result<T, EncryptError>;
#[derive(Debug, Default, Clone)]
pub struct Kms(Arc<RwLock<KmsInner>>);
impl Deref for Kms {
    type Target = Arc<RwLock<KmsInner>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
#[derive(Debug, Default)]
pub struct KmsInner {
    data_keys: HashMap<CipherKeyId, DataKey>,
    last_created: PhyTs, //last_created is the timestamp(seconds) of the last data key,
    next_key_id: CipherKeyId,
    file: Option<File>,
    cipher: Option<AesCipher>,
    data_key_rotation_duration: Duration,
}
#[derive(Debug, Clone)]
pub struct KeyRegistryBuilder {
    encrypt_key: Vec<u8>,                 // encryption key
    data_key_rotation_duration: Duration, // key rotation duration
    read_only: bool,
    dir: PathBuf,
}
impl Default for KeyRegistryBuilder {
    fn default() -> Self {
        Self {
            encrypt_key: Default::default(),
            data_key_rotation_duration: Duration::from_secs(10 * 24 * 60 * 60),
            read_only: false,
            dir: PathBuf::from(DEFAULT_DIR),
        }
    }
}
impl KeyRegistryBuilder {
    pub fn new(encrypt_key: Vec<u8>) -> Self {
        Self {
            encrypt_key,
            ..Default::default()
        }
    }
    pub fn with_data_key_rotation_duration(mut self, duration: Duration) -> Self {
        self.data_key_rotation_duration = duration;
        self
    }
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    pub fn with_dir(mut self, dir: PathBuf) -> Self {
        self.dir = dir;
        self
    }
    pub fn build(self) -> Result<Kms> {
        let keys_len = self.encrypt_key.len();

        if keys_len > 0 && !vec![16, 32].contains(&keys_len) {
            return Err(EncryptError::InvalidEncryptionKey);
        }
        let mut key_registry = KmsInner {
            data_keys: Default::default(),
            last_created: PhyTs::default(),
            next_key_id: 0.into(),
            file: None,
            cipher: if keys_len > 0 {
                AesCipher::new(&self.encrypt_key, 0.into()).ok()
            } else {
                None
            },
            data_key_rotation_duration: self.data_key_rotation_duration,
        };
        let key_registry_path = self.dir.join(KEY_REGISTRY_FILE_NAME);

        if !key_registry_path.exists() {
            if self.read_only {
                return Ok(Kms(Arc::new(RwLock::new(key_registry))));
            }
            key_registry.write_to_file(&self.dir)?;
        }

        let key_registry_file = OpenOptions::new()
            .read(true)
            .write(!self.read_only)
            .custom_flags(libc::O_DSYNC)
            .open(key_registry_path)?;

        key_registry.read(&key_registry_file)?;
        if !self.read_only {
            key_registry.file = Some(key_registry_file);
        }

        return Ok(Kms(Arc::new(RwLock::new(key_registry))));
    }
}
impl KmsInner {
    //     Structure of Key Registry.
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    // |   Nonce   |  SanityText.len() u32 | e_Sanity Text  | DataKey1(len_crc_buf(e_data_key.len,crc),e_data_key(..,e_data,..))     | DataKey2     | ...              |
    // +-------------------+---------------------+--------------------+--------------+------------------+------------------+------------------+
    fn write_to_file(&mut self, dir: &Path) -> Result<()> {
        let nonce: Nonce = AesCipher::generate_nonce();
        let mut e_sanity = SANITY_TEXT.to_vec();

        if let Some(c) = &self.cipher {
            e_sanity = c.encrypt(&nonce, &mut e_sanity)?;
        }
        let mut buf = Vec::with_capacity(12 + 4 + 12 + 16);
        buf.put_slice(nonce.as_slice());
        buf.put_u32(e_sanity.len() as u32);
        buf.put_slice(&e_sanity);

        for (_, data_key) in self.data_keys.iter_mut() {
            Self::store_data_key(&mut buf, &self.cipher, data_key)?;
        }

        let rewrite_path = dir.join(KEY_REGISTRY_REWRITE_FILE_NAME);
        let mut rewrite_fp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .custom_flags(libc::O_DSYNC)
            .open(&rewrite_path)?;

        rewrite_fp.write_all(&buf)?;

        rename(rewrite_path, dir.join(KEY_REGISTRY_FILE_NAME))?;

        rewrite_fp.sync_all()?;
        Ok(())
    }
    fn read(&mut self, fp: &File) -> Result<()> {
        let key_iter = KeyRegistryIter::new(fp, &self.cipher)?;
        for data_key in key_iter {
            self.next_key_id = self.next_key_id.max(data_key.key_id.into());
            self.last_created = self.last_created.max(data_key.created_at.into());
            self.data_keys.insert(data_key.key_id.into(), data_key);
        }
        Ok(())
    }
    fn store_data_key(
        buf: &mut Vec<u8>,
        cipher: &Option<AesCipher>,
        data_key: &mut DataKey,
    ) -> Result<()> {
        let nonce = Nonce::from_slice(&data_key.iv);
        if let Some(c) = &cipher {
            data_key.data = c.encrypt(nonce, &data_key.data)?;
        }

        let e_data_key = data_key.encode_to_vec();

        let mut len_crc_buf = Vec::with_capacity(8);
        len_crc_buf.put_u32(e_data_key.len() as u32);
        len_crc_buf.put_u32(crc32fast::hash(&e_data_key));

        buf.put(len_crc_buf.as_ref());
        buf.put(e_data_key.as_ref());

        if let Some(c) = &cipher {
            data_key.data = c.decrypt(nonce, &data_key.data)?;
        }
        Ok(())
    }
}
struct KeyRegistryIter<'a> {
    reader: BufReader<&'a File>,
    cipher: &'a Option<AesCipher>,
    len_crc_buf: Vec<u8>,
}
impl<'a> KeyRegistryIter<'a> {
    fn valid(&mut self) -> Result<()> {
        let mut nonce: Nonce = AesCipher::generate_nonce();
        self.reader.read_exact(nonce.as_mut())?;

        let mut len_e_saintytext_buf = vec![0u8; 4];
        self.reader.read_exact(len_e_saintytext_buf.as_mut())?;
        let mut len_e_saintytext_ref: &[u8] = len_e_saintytext_buf.as_ref();
        let len_e_saintytext = len_e_saintytext_ref.get_u32();

        let mut e_saintytext = vec![0; len_e_saintytext as usize];
        self.reader.read_exact(e_saintytext.as_mut())?;

        let saintytext = match self.cipher {
            Some(c) => c.decrypt(&nonce, &e_saintytext)?,
            None => e_saintytext.to_vec(),
        };

        if saintytext != SANITY_TEXT {
            return Err(EncryptError::EncryptionKeyMismatch);
        };
        Ok(())
    }
    fn new(fp: &'a File, cipher: &'a Option<AesCipher>) -> Result<Self> {
        let mut reader = BufReader::new(fp);
        reader.seek(std::io::SeekFrom::Start(0))?;
        let mut s = Self {
            reader,
            cipher,
            len_crc_buf: vec![0; 8],
        };
        s.valid()?;
        Ok(s)
    }
}
impl<'a> Iterator for KeyRegistryIter<'a> {
    type Item = DataKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_exact(self.len_crc_buf.as_mut()) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        error!("While reading data_key.len and crc in keyRegistryIter.next {e}",);
                    }
                }
                return None;
            }
        };
        let mut len_crc_buf_ref: &[u8] = self.len_crc_buf.as_ref();
        let e_data_key_len = len_crc_buf_ref.get_u32();
        let e_data_key_crc: u32 = len_crc_buf_ref.get_u32();

        let mut e_data_key = vec![0u8; e_data_key_len as usize];
        match self.reader.read_exact(e_data_key.as_mut()) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => {
                        error!("While reading data in keyRegistryIter.next {e}");
                    }
                }
                return None;
            }
        };

        if crc32fast::hash(&e_data_key) != e_data_key_crc {
            error!(
                "Error while checking checksum for data key. {:?}",
                e_data_key
            );
            //skip
            return self.next();
        };
        let mut data_key = match DataKey::decode(e_data_key.as_ref()) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Error while decode protobuf-bytes for data key. {:?} for {:?}",
                    e_data_key, e
                );
                //skip
                return self.next();
            }
        };
        if let Some(c) = self.cipher {
            match c.decrypt_with_slice(&data_key.iv, &data_key.data) {
                Ok(data) => {
                    data_key.data = data;
                }
                Err(e) => {
                    error!("Error while use aes cipher to decrypt datakey.data for {e}");
                    //skip
                    return self.next();
                }
            };
        }
        Some(data_key)
    }
}
impl Kms {
    pub fn latest_cipher(&self) -> Result<Option<AesCipher>> {
        if let Some(data_key) = self.latest_datakey()? {
            return Ok(AesCipher::new(&data_key.data, data_key.key_id.into())?.into());
        };
        Ok(None)
    }

    fn latest_datakey(&self) -> Result<Option<DataKey>> {
        let inner_r = self
            .read()
            .map_err(|e| EncryptError::RwLockPoisoned(format!("{e}")))?;
        if inner_r.cipher.is_none() {
            return Ok(None);
        }

        let valid_key = |inner: &KmsInner| {
            let last = inner.last_created.into();
            if let Ok(diff) = SystemTime::now().duration_since(last) {
                if diff < inner.data_key_rotation_duration {
                    return (
                        inner
                            .data_keys
                            .get(&inner.next_key_id)
                            .and_then(|x| x.clone().into()),
                        true,
                    );
                }
            };
            return (None, false);
        };
        let (key, valid) = valid_key(&inner_r);
        if valid {
            return Ok(key);
        }
        drop(inner_r);
        let mut inner_w = self
            .write()
            .map_err(|e| EncryptError::RwLockPoisoned(format!("{e}")))?;
        let (key, valid) = valid_key(&inner_w);
        if valid {
            return Ok(key);
        }

        let cipher = inner_w.cipher.as_ref().unwrap();

        let key = cipher.generate_key();
        let nonce: Nonce = AesCipher::generate_nonce();
        inner_w.next_key_id += 1;
        let key_id = inner_w.next_key_id;
        let created_at = PhyTs::now()?;
        let mut data_key = DataKey {
            key_id: key_id.into(),
            data: key,
            iv: nonce.to_vec(),
            created_at: created_at.into(),
        };
        let mut buf = Vec::new();
        KmsInner::store_data_key(&mut buf, &inner_w.cipher, &mut data_key)?;
        if let Some(f) = &mut inner_w.file {
            f.write_all(&buf)?;
        }

        inner_w.last_created = created_at;
        inner_w.data_keys.insert(key_id, data_key.clone());
        Ok(Some(data_key))
    }
    fn get_data_key(&self, cipher_key_id: CipherKeyId) -> Result<Option<DataKey>> {
        let inner_r = self
            .read()
            .map_err(|e| EncryptError::RwLockPoisoned(format!("{e}")))?;
        if cipher_key_id == CipherKeyId::default() {
            return Ok(None);
        }
        match inner_r.data_keys.get(&cipher_key_id) {
            Some(s) => Ok(Some(s.clone())),
            None => {
                return Err(EncryptError::InvalidDataKeyID(cipher_key_id));
            }
        }
    }
    pub fn get_cipher(&self, cipher_key_id: CipherKeyId) -> Result<Option<AesCipher>> {
        if let Some(dk) = self.get_data_key(cipher_key_id)? {
            let cipher = AesCipher::new(&dk.data, cipher_key_id)?;
            return Ok(cipher.into());
        };
        Ok(None)
    }
}
