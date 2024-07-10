use std::sync::Arc;

use log::{error, info};
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms, levelctl::LevelCtlTrait, memtable::MemtableTrait,
    sstable::TableTrait, txn::TxnManagerTrait,
};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::core::{CoreBuilder, CoreInner};
use crate::Result;
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
    pub(crate) async fn do_flush_task(
        this: Arc<Self>,
        mut receiver: Receiver<Arc<M>>,
        closer: Closer,
    ) {
        'a: loop {
            select! {
                Some(memtable) = receiver.recv() => {
                    'b:loop {
                        if let Err(e)=this.handle_flush(memtable.clone()).await{
                            error!("flushing memtable to disk for {}, retrying",e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        info!("flushed memtable {} to disk",memtable.id());
                        match this.immut_memtable().write() {
                            Ok(mut immut_w) => {
                                if let Some(p) = immut_w.front() {
                                    if p.id()==memtable.id(){
                                        info!("removing memtable {} from immut_memtable",memtable.id());
                                        immut_w.pop_front();
                                    }
                                }
                            }
                            Err(e) => {
                                error!("getting write lock for immut_memtable failed: {}", e);
                            }
                        };
                        break 'b;
                    }
                },
                _= closer.cancelled()=>{
                    break 'a;
                }
            }
        }
    }
    pub(crate) async fn handle_flush(&self, memtable: Arc<M>) -> Result<()> {
        let cipher = self.kms().latest_cipher()?;
        
        // this.flush_channel.send(memtable).await?;
        Ok(())
    }
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
