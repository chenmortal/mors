use mors_traits::txn::TxnManagerError;
use thiserror::Error;
#[derive(Error, Debug)]
pub enum MorsTxnError {}
impl From<MorsTxnError> for TxnManagerError {
    fn from(e: MorsTxnError) -> TxnManagerError {
        TxnManagerError::new(e)
    }
}
