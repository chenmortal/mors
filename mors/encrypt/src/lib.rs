pub mod cipher;
pub mod error;
mod pb;
pub mod registry;
// mod iter;

pub const KEY_REGISTRY_FILE_NAME: &str = "KEY_REGISTRY";
pub const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEY_REGISTRY";
const SANITY_TEXT: &[u8] = b"Hello Mors!!";
pub const NONCE_SIZE: usize = 12;
