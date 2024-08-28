use thiserror::Error;
#[derive(Error, Debug)]
pub enum TxnManageError {
    #[error("MpscSendError: {0}")]
    SendError(String),
}
