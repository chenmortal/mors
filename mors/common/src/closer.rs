use std::{error::Error, sync::Arc};

use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        AcquireError, Notify, OwnedSemaphorePermit, Semaphore,
    },
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
    semaphore_permit: OwnedSemaphorePermit,
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
                    let semaphore_permit=permit?;
                    let sender = self.sender.clone();
                    return Ok(ThrottlePermit {
                        semaphore_permit,
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
    pub async fn do_future<T>(
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
