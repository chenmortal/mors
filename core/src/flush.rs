use std::sync::Arc;

use mors_traits::{
    kms::Kms, levelctl::LevelCtlTrait, memtable::MemtableTrait,
    sstable::TableTrait, txn::TxnManagerTrait,
};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::core::{CoreBuilder, CoreInner};

impl<M, K, L, T, Txn> CoreBuilder<M, K, L, T, Txn>
where
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    Txn: TxnManagerTrait,
{
    pub(crate) fn init_flush_channel(
        num_memtables: usize,
    ) -> (Sender<Arc<M>>, Receiver<Arc<M>>) {
        mpsc::channel::<Arc<M>>(num_memtables)
    }
}
impl<M, K, L, T> CoreInner<M, K, L, T>
where
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
{
    pub(crate) fn do_flush_task(this: Arc<Self>) {}
}
#[tokio::test]
async fn test_recv() {
    let (sender, mut receiver) = mpsc::channel::<String>(3);
    let sender_c = sender.clone();

    tokio::spawn(async move {
        sender.send("hello".to_string()).await.unwrap();
    });
    tokio::spawn(async move {
        sender_c.send("world".to_string()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    while let Ok(r) = receiver.try_recv() {
        println!("{}", r);
    }
}
