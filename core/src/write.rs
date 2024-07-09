use std::{
    mem::replace,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    core::{CoreBuilder, CoreInner},
    error::MorsError,
    Result,
};
use log::{debug, error};
use mors_common::closer::Closer;
use mors_traits::{
    kms::Kms,
    kv::{Entry, ValuePointer},
    levelctl::LevelCtlTrait,
    memtable::MemtableTrait,
    sstable::TableTrait,
    txn::TxnManagerTrait,
};
use tokio::{
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot, Notify,
    },
};
const CHANNEL_CAPACITY: usize = 1000;
pub(crate) struct WriteRequest {
    entries_vptrs: Vec<(Entry, ValuePointer)>,
    result: Result<()>,
    send_result: Option<oneshot::Sender<Result<()>>>,
}
impl WriteRequest {
    pub(crate) fn new(
        mut entries: Vec<Entry>,
        sender: oneshot::Sender<Result<()>>,
    ) -> Self {
        let entries_vptrs = entries
            .drain(..)
            .map(|x| (x, ValuePointer::default()))
            .collect();
        Self {
            entries_vptrs,
            result: Ok(()),
            send_result: sender.into(),
        }
    }
}
impl Drop for WriteRequest {
    fn drop(&mut self) {
        let old = replace(&mut self.result, Ok(()));
        if let Some(sender) = self.send_result.take() {
            let _ = sender.send(old);
        }
    }
}
impl<M, K, L, T, Txn> CoreBuilder<M, K, L, T, Txn>
where
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    Txn: TxnManagerTrait,
{
    pub(crate) fn init_write_channel(
    ) -> (Sender<WriteRequest>, Receiver<WriteRequest>) {
        mpsc::channel::<WriteRequest>(CHANNEL_CAPACITY)
    }
}
impl<M, K, L, T> CoreInner<M, K, L, T>
where
    M: MemtableTrait<K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
{
    pub(crate) async fn do_write_task(
        this: Arc<Self>,
        mut receiver: Receiver<WriteRequest>,
        closer: Closer,
    ) {
        let mut write_requests = Vec::with_capacity(10);
        let request_len = Arc::new(AtomicUsize::new(0));

        let notify_send = Arc::new(Notify::new());
        let notify_recv = notify_send.clone();
        notify_send.notify_one();
        'a: loop {
            select! {
                Some(write_req)=receiver.recv()=>{
                    write_requests.push(write_req);
                    request_len.store(write_requests.len(), Ordering::Relaxed);
                },
                _=closer.cancelled()=>{
                    while let Ok(w) = receiver.try_recv() {
                        write_requests.push(w);
                    }
                    notify_recv.notified().await;
                    Self::start_write_request(this.clone(),write_requests, notify_send.clone()).await;
                    break 'a;
                },
            }
            'b: loop {
                if write_requests.len() >= 3 * CHANNEL_CAPACITY {
                    notify_recv.notified().await;
                    tokio::spawn(Self::start_write_request(
                        this.clone(),
                        write_requests,
                        notify_send.clone(),
                    ));
                    write_requests = Vec::with_capacity(10);
                    request_len.store(0, Ordering::Relaxed);
                    break 'b;
                }
                select! {
                    Some(write_req)=receiver.recv()=>{
                        write_requests.push(write_req);
                        request_len.store(write_requests.len(), Ordering::Relaxed);
                    },
                    _=notify_recv.notified()=>{
                        tokio::spawn(Self::start_write_request(
                            this.clone(),
                            write_requests,
                            notify_send.clone(),
                        ));
                        write_requests=Vec::with_capacity(10);
                        request_len.store(0, Ordering::Relaxed);
                        break 'b;
                    }
                    _=closer.cancelled()=>{
                        while let Ok(w) = receiver.try_recv() {
                            write_requests.push(w);
                        }
                        notify_recv.notified().await;
                        Self::start_write_request(this.clone(),write_requests, notify_send.clone()).await;
                        break 'a;
                    },
                }
            }
        }
        notify_recv.notified().await;
    }
    async fn start_write_request(
        this: Arc<Self>,
        requests: Vec<WriteRequest>,
        notify_send: Arc<Notify>,
    ) {
        if let Err(e) = this.handle_write_request(requests).await {
            error!("write request error:{}", e);
        };
        notify_send.notify_one();
    }
    async fn handle_write_request(
        &self,
        mut requests: Vec<WriteRequest>,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        debug!("Writing to memtable :{}", requests.len());
        let mut count = 0;
        for request in requests.iter_mut() {
            if request.entries_vptrs.is_empty() {
                continue;
            }
            count += request.entries_vptrs.len();
            self.ensure_room_for_write().await?;
        }
        debug!("Writing to memtable done:{}", count);
        Ok(())
    }
    async fn ensure_room_for_write(&self) -> Result<()> {
        let memtable = self.memtable().unwrap();
        let memtable_r = memtable
            .read()
            .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;
        if !memtable_r.is_full() {
            return Ok(());
        }

        debug!("Memtable is full, making room for writes");
        let new_memtable = self.build_memtable()?;
        drop(memtable_r);

        let mut memtable_w = memtable
            .write()
            .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;
        let old_memtable = replace(&mut *memtable_w, new_memtable);
        drop(memtable_w);
        let old_memtable = Arc::new(old_memtable);

        Ok(())
    }
}
#[tokio::test]
async fn test_notify() {
    let notify = Arc::new(Notify::new());
    let notify_recv = notify.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let count_notify = count.clone();
    let count_notified = count.clone();
    notify.notify_one();
    let handle_notify = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            notify.notify_one();
            let count = count_notify.fetch_add(1, Ordering::SeqCst);
            println!("count_notify:{}", count);
            if count >= 4 {
                break;
            }
        }
    });
    let handle_notified = tokio::spawn(async move {
        loop {
            notify_recv.notified().await;
            let count = count_notified.load(Ordering::SeqCst);
            println!("count_notified:{}", count);
            if count >= 4 {
                break;
            }
        }
    });
    handle_notify.await.unwrap();
    handle_notified.await.unwrap();
}
