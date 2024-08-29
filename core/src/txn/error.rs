use thiserror::Error;
#[derive(Error, Debug)]
pub enum TxnError {
    #[error("MpscSendError: {0}")]
    SendError(String),
    #[error("This transaction has been discarded. Create a new one")]
    DiscardTxn,
    #[error("Key cannot be empty")]
    EmptyKey,
    #[error("{0} with size {1} exceeded {2} limit")]
    ExceedSize(&'static str, usize, usize),
    #[error("Key is using a reserved {0} prefix")]
    InvalidKey(&'static str),
    #[error("Txn is too big to fit into one request")]
    TxnTooBig,
}
