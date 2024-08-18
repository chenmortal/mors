use std::{error::Error, sync::Arc};

use log::{debug, error};
use parking_lot::Mutex;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        AcquireError, Notify, OwnedSemaphorePermit, Semaphore,
    },
    task::JoinHandle,
};
#[derive(Debug, Default, Clone)]
pub struct CloseNotify(Arc<Notify>);
impl CloseNotify {
    pub fn new() -> Self {
        Self(Arc::new(Notify::new()))
    }

    pub fn notify(&self) {
        self.0.notify_one();
    }

    pub async fn wait(&self) {
        self.0.notified().await;
    }
}
pub struct Throttle<E: Error + From<AcquireError>> {
    semaphore: Arc<Semaphore>,
    max_permits: u32,
    receiver: Receiver<E>,
    sender: Sender<E>,
}
pub struct ThrottlePermit<E: Error> {
    _semaphore_permit: OwnedSemaphorePermit,
    sender: Sender<E>,
}
impl<E: Error + From<AcquireError>> Throttle<E> {
    pub fn new(max_permits: u32) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_permits as usize));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        Self {
            semaphore,
            max_permits,
            receiver,
            sender,
        }
    }
    pub async fn acquire(&mut self) -> Result<ThrottlePermit<E>, E> {
        loop {
            select! {
                permit =self.semaphore.clone().acquire_owned()=>{
                    let _semaphore_permit=permit?;
                    let sender = self.sender.clone();
                    return Ok(ThrottlePermit {
                        _semaphore_permit,
                        sender,
                    });
                },
                error=self.receiver.recv()=>{
                    if let Some(e) = error {
                        return Err(e);
                    }

                }
            }
        }
    }
    pub async fn finish(&mut self) -> Result<(), E> {
        let _ = self.semaphore.acquire_many(self.max_permits).await?;
        if let Ok(e) = self.receiver.try_recv() {
            return Err(e);
        }
        Ok(())
    }
}
impl<E: Error> ThrottlePermit<E> {
    pub async fn do_future<T: Send>(
        self,
        future: impl std::future::Future<Output = Result<T, E>>,
    ) -> Option<T> {
        match future.await {
            Ok(t) => t.into(),
            Err(error) => {
                if let Err(e) = self.sender.send(error).await {
                    panic!("ThrottlePermit::done_with_future: {}", e)
                };
                None
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Closer(Arc<CloserInner>);
#[derive(Debug)]
struct CloserInner {
    join_handle: Mutex<Option<JoinHandle<()>>>,
    sem: Semaphore,
    // need_permits: AtomicUsize,
    task: &'static str,
}
impl Default for Closer {
    fn default() -> Self {
        Self(Arc::new(CloserInner {
            join_handle: Mutex::new(None),
            task: Default::default(),
            sem: Semaphore::new(0),
            // need_permits: AtomicUsize::new(0),
        }))
    }
}
impl Closer {
    pub fn new(task: &'static str) -> Self {
        Self(Arc::new(CloserInner {
            join_handle: Mutex::new(None),
            task,
            sem: Semaphore::new(0),
            // need_permits: AtomicUsize::new(0),
        }))
    }
    pub fn cancel(&self) {
        debug!("cancelling for {} task", self.0.task);
        self.0.sem.add_permits(Semaphore::MAX_PERMITS);
    }
    pub fn cancel_one(&self) {
        self.0.sem.add_permits(1);
    }

    pub async fn cancelled(&self) {
        // self.0.need_permits.fetch_add(1, Ordering::SeqCst);
        let permit = self.0.sem.acquire().await;
        if let Err(e) = permit {
            error!("{}: cancelled: {}", self.0.task, e);
        }
    }

    pub fn set_joinhandle(&self, handle: JoinHandle<()>) {
        *self.0.join_handle.lock() = Some(handle);
    }
    pub async fn wait(&self) -> Result<(), tokio::task::JoinError> {
        let handle = {
            let mut lock = self.0.join_handle.lock();
            lock.take()
        };
        if let Some(handle) = handle {
            handle.await?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Semaphore;
    #[tokio::test]
    async fn test_sempahore() {
        let sem = Arc::new(Semaphore::new(0));
        let sem_c = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sem_c.add_permits(2);
        });
        let k = sem.acquire().await;

        assert!(k.is_ok());
    }
}
