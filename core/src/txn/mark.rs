use super::{error::TxnError, Result};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use mors_common::closer::Closer;
use mors_common::ts::TxnTs;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
};
#[derive(Clone)]
pub(crate) struct WaterMark(Arc<WaterMarkInner>);
pub(crate) struct WaterMarkInner {
    done_until: AtomicU64,
    sender: Sender<Mark>,
    name: &'static str,
    closer: Closer,
}
pub(crate) struct Mark {
    txn: TxnTs,
    waiter: Option<Arc<Notify>>,
    indices: Vec<TxnTs>,
    done: bool,
}
impl WaterMark {
    pub(crate) fn new(name: &'static str, done_until: TxnTs) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Mark>(100);
        let closer = Closer::new(name);
        let water = Self(Arc::new(WaterMarkInner {
            done_until: AtomicU64::new(done_until.into()),
            sender,
            name,
            closer,
        }));

        water
            .0
            .closer
            .set_joinhandle(tokio::spawn(water.clone().process(receiver)));
        water
    }
    pub(crate) async fn begin(&self, txn: TxnTs) -> Result<()> {
        self.0
            .sender
            .send(Mark {
                txn,
                waiter: None,
                indices: Vec::new(),
                done: false,
            })
            .await
            .map_err(|e| TxnError::SendError(e.to_string()))
    }
    //just only use for txn_mark
    pub(crate) async fn wait_for_mark(&self, txn: TxnTs) -> Result<()> {
        if self.0.done_until.load(Ordering::Acquire) >= txn.into() {
            return Ok(());
        };
        let notify = Arc::new(Notify::new());
        self.0
            .sender
            .send(Mark {
                txn,
                waiter: notify.clone().into(),
                indices: Vec::new(),
                done: false,
            })
            .await
            .map_err(|e| TxnError::SendError(e.to_string()))?;
        notify.notified().await;
        Ok(())
    }
    async fn process(self, mut receiver: Receiver<Mark>) {
        let mut waiters = HashMap::<TxnTs, Vec<Arc<Notify>>>::new();
        let mut min_heap = BinaryHeap::<Reverse<TxnTs>>::new();
        let mut pending = HashMap::<TxnTs, isize>::new();

        let mut process_one = |txn_ts: TxnTs,
                               done: bool,
                               waiters: &mut HashMap<
            TxnTs,
            Vec<Arc<Notify>>,
        >| {
            match pending.get_mut(&txn_ts) {
                Some(prev) => {
                    *prev += if done { 1 } else { -1 };
                }
                None => {
                    min_heap.push(Reverse(txn_ts));
                    pending.insert(txn_ts, if done { 1 } else { -1 });
                }
            };

            let done_until = self.0.done_until.load(Ordering::Acquire).into();
            assert!(
                done_until <= txn_ts,
                "Name: {} done_util: {done_until}. txn_ts:{txn_ts}",
                self.0.name
            );

            let mut until = done_until;

            while !min_heap.is_empty() {
                let min = min_heap.peek().unwrap().0;
                if let Some(done) = pending.get(&min) {
                    if *done < 0 {
                        break;
                    }
                }
                min_heap.pop();
                pending.remove(&min);
                until = min;
            }

            if until != done_until {
                // self.done_until cheanged only here and one instance, one process task
                // so compare_exchange must be ok
                assert!(self
                    .0
                    .done_until
                    .compare_exchange(
                        done_until.to_u64(),
                        until.to_u64(),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok());
            }
            assert!(done_until <= until);
            if until.to_u64() - done_until.to_u64() <= waiters.len() as u64 {
                for idx in (done_until.to_u64() + 1)..=until.to_u64() {
                    let txn: TxnTs = idx.into();
                    if let Some(to_notifies) = waiters.get(&txn) {
                        to_notifies.iter().for_each(|x| x.notify_one());
                    };
                    waiters.remove(&txn);
                }
            } else {
                let mut need_remove = Vec::with_capacity(waiters.len());
                for (txn, to_notifies) in waiters.iter() {
                    if *txn <= until {
                        to_notifies.iter().for_each(|x| x.notify_one());
                        need_remove.push(*txn);
                    }
                }
                need_remove.iter().for_each(|x| {
                    waiters.remove(x);
                });
            }
        };

        loop {
            select! {
                _=self.0.closer.cancelled()=>{
                    return ;
                }
                Some(mark)=receiver.recv()=>{
                    match mark.waiter {
                        Some(notify) => {
                            if self.0.done_until.load(Ordering::Acquire) >= mark.txn.into() {
                                notify.notify_one();
                            } else {
                                match waiters.get_mut(&mark.txn) {
                                    Some(v) => {
                                        v.push(notify);
                                    }
                                    None => {
                                        waiters.insert(mark.txn, vec![notify]);
                                    }
                                };
                            };
                        }
                        None => {
                            if mark.txn > TxnTs::default() {
                                process_one(mark.txn, mark.done, &mut waiters);
                            }
                            for indice in mark.indices {
                                process_one(indice, mark.done, &mut waiters);
                            }
                        }
                    }
                }
            }
        }
    }
}
