use std::sync::Arc;

use log::{debug, error, info};
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms,
    levelctl::LevelCtlTrait,
    memtable::MemtableTrait,
    skip_list::SkipListTrait,
    sstable::{TableBuilderTrait, TableTrait},
    txn::TxnManagerTrait,
    vlog::VlogCtlTrait,
};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::core::{CoreBuilder, CoreInner};
use crate::Result;
impl<M, K, L, T, S, Txn, V> CoreBuilder<M, K, L, T, S, Txn, V>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    Txn: TxnManagerTrait,
    V: VlogCtlTrait<K>,
{
    pub(crate) fn init_flush_channel(
        num_memtables: usize,
    ) -> (Sender<Arc<M>>, Receiver<Arc<M>>) {
        mpsc::channel::<Arc<M>>(num_memtables)
    }
}
impl<M, K, L, T, S, V> CoreInner<M, K, L, T, S, V>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    V: VlogCtlTrait<K>,
{
    pub(crate) async fn do_flush_task(
        this: Arc<Self>,
        mut receiver: Receiver<Arc<M>>,
        closer: Closer,
    ) {
        'a: loop {
            select! {
                Some(memtable) = receiver.recv() => {
                    debug!("received memtable {} for flushing",memtable.id());
                    'b:loop {
                        if let Err(e)=this.handle_flush(memtable.clone()).await{
                            error!("flushing memtable to disk for {}, retrying",e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        info!("flushed memtable {} to disk",memtable.id());
                        Self::immut_memtable_pop_front(&this).await;
                        break 'b;
                    }
                },
                _= closer.cancelled()=>{
                    break 'a;
                }
            }
        }
    }
    async fn immut_memtable_pop_front(this: &Arc<Self>) {
        match this.immut_memtable().write() {
            Ok(mut immut_w) => {
                if let Some(memtable) = immut_w.front() {
                    info!(
                        "removing memtable {} from immut_memtable",
                        memtable.id()
                    );
                    let memtable = immut_w.pop_front().unwrap();
                    drop(immut_w);
                    if let Err(e) = memtable.delete_wal() {
                        error!(
                            "deleting wal for memtable {} failed: {}",
                            memtable.id(),
                            e
                        );
                    }
                    info!(
                        "memtable {} removed from immut_memtable",
                        memtable.id()
                    );
                }
            }
            Err(e) => {
                error!("getting write lock for immut_memtable failed: {}", e);
            }
        };
    }
    pub(crate) async fn handle_flush(&self, memtable: Arc<M>) -> Result<()> {
        let cipher = self.kms().latest_cipher()?;
        let next_id = self.levelctl().next_id();
        debug!(
            "building table for memtable {} with next_id {:?}",
            memtable.id(),
            next_id
        );
        let skip_list = memtable.skip_list();
        if let Some(t) = self
            .levelctl()
            .table_builder()
            .build_l0(skip_list.iter(), next_id, cipher)
            .await?
        {
            debug!("pushing table for memtable {} to level 0", memtable.id());
            self.levelctl().push_level0(t).await?;
        };
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
