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
use log::{debug, error, info};
use mors_common::{
    closer::Closer,
    kv::{Entry, Meta, ValuePointer},
};
use mors_traits::{
    kms::Kms, levelctl::LevelCtlTrait, memtable::MemtableTrait,
    skip_list::SkipListTrait, sstable::TableTrait, txn::TxnManagerTrait,
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
impl<M, K, L, T, S, Txn> CoreBuilder<M, K, L, T, S, Txn>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
    Txn: TxnManagerTrait,
{
    pub(crate) fn init_write_channel(
    ) -> (Sender<WriteRequest>, Receiver<WriteRequest>) {
        mpsc::channel::<WriteRequest>(CHANNEL_CAPACITY)
    }
}
impl<M, K, L, T, S> CoreInner<M, K, L, T, S>
where
    M: MemtableTrait<S, K>,
    K: Kms,
    L: LevelCtlTrait<T, K>,
    T: TableTrait<K::Cipher>,
    S: SkipListTrait,
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
            if let Err(e) = self.ensure_room_for_write().await {
                request.result = Err(e);
            };
            if let Err(e) = self.write_to_memtable(request).await {
                request.result = Err(e);
                break;
            };
        }
        debug!("Writing to memtable done:{}", count);
        Ok(())
    }
    async fn ensure_room_for_write(&self) -> Result<()> {
        let memtable = self.memtable().unwrap();
        let new_memtable = {
            let memtable_r = memtable
                .read()
                .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;
            if !memtable_r.is_full() {
                return Ok(());
            }
            debug!(
                "Memtable {} is full, making room for writes",
                memtable_r.id()
            );
            let new_memtable = self.build_memtable()?;
            debug!("New memtable {} created", new_memtable.id());
            new_memtable
        };

        let old_memtable = {
            let mut memtable_w = memtable
                .write()
                .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;

            let old_memtable = replace(&mut *memtable_w, new_memtable);
            Arc::new(old_memtable)
        };

        self.flush_sender()
            .send(old_memtable.clone())
            .await
            .map_err(|e| MorsError::SendError(e.to_string()))?;
        debug!("Old memtable {} sent for flushing", old_memtable.id());
        let mut immut_w = self
            .immut_memtable()
            .write()
            .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;
        immut_w.push_back(old_memtable);
        debug!("Old memtable added to immut_memtable");
        Ok(())
    }
    async fn write_to_memtable(
        &self,
        request: &mut WriteRequest,
    ) -> Result<()> {
        let memtable = self.memtable().unwrap();
        let mut memtable_w = memtable
            .write()
            .map_err(|e| MorsError::RwLockPoisoned(e.to_string()))?;
        for (entry, vptr) in &mut request.entries_vptrs {
            if vptr.is_empty() {
                entry.meta_mut().remove(Meta::VALUE_POINTER);
            } else {
                entry.meta_mut().insert(Meta::VALUE_POINTER);
                entry.set_value(vptr.encode());
            }
            memtable_w.push(entry)?;
        }
        memtable_w.flush()?;
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

mod test {

    use std::{fs::create_dir, path::PathBuf};

    use log::LevelFilter;
    use mors_common::test::{gen_random_entries, get_rng};

    use crate::MorsBuilder;

    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write() {
        if let Err(e) = test_write_impl().await {
            eprintln!("Error: {:?}", e.to_string());
        }
    }
    async fn test_write_impl() -> Result<()> {
        let mut logger = env_logger::builder();
        logger.filter_level(LevelFilter::Trace);
        logger.init();

        let path = "../data/";
        let dir = PathBuf::from(path);
        if !dir.exists() {
            create_dir(&dir).unwrap();
        }
        let mut builder = MorsBuilder::default();
        builder.set_dir(dir).set_read_only(false);
        builder
            .set_num_memtables(1)
            .set_memtable_size(5 * 1024 * 1024);
        let mors = builder.build().await?;

        let mut rng = get_rng("abcd");
        let random = gen_random_entries(&mut rng, 100000);

        let mut entries = Vec::new();
        let mut receivers = Vec::new();
        for entry in random {
            entries.push(entry);
            if entries.len() == 10 {
                let (sender, receiver) = oneshot::channel();
                receivers.push(receiver);
                let write_request = WriteRequest::new(entries, sender);
                mors.inner()
                    .write_sender()
                    .send(write_request)
                    .await
                    .unwrap();
                entries = Vec::new();
            }
        }
        debug!("Waiting for write to complete");
        for recv in receivers {
            match recv.await {
                Ok(e) => {
                    if let Err(k) = e {
                        eprintln!("Error: {:?}", k.to_string());
                        return Err(MorsError::SendError(k.to_string()));
                    }
                }
                Err(k) => {
                    eprintln!("Error: {:?}", k.to_string());
                    return Err(MorsError::SendError(k.to_string()));
                }
            };
        }
        info!("Write completed");
        Ok(())
    }
}
