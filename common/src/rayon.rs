use std::{
    future::Future,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    task::{Context, Poll},
    thread,
};

use log::error;
// use rayon::ThreadBuilder;
pub use rayon::ThreadPoolBuilder;
use tokio::sync::oneshot::{self, Receiver};
//copy from  tokio-rayon
#[derive(Debug)]
pub struct AsyncRayonHandle<T> {
    pub(crate) rx: Receiver<thread::Result<T>>,
}

impl<T> Future for AsyncRayonHandle<T> {
    type Output = T;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let rx = Pin::new(&mut self.rx);
        rx.poll(cx).map(|result| {
            result
                .expect("Unreachable error: Tokio channel closed")
                .unwrap_or_else(|err| resume_unwind(err))
        })
    }
}
#[allow(dead_code)]
pub fn spawn<F, R>(func: F) -> AsyncRayonHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
    });

    AsyncRayonHandle { rx }
}
pub fn spawn_fifo<F, R>(func: F) -> AsyncRayonHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    rayon::spawn_fifo(move || {
        let _result = tx.send(catch_unwind(AssertUnwindSafe(func)));
    });

    AsyncRayonHandle { rx }
}
#[deny(unused)]
pub fn init_global_rayon_pool() {
    let cpus = num_cpus::get();

    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(cpus * 2)
        .build_global()
    {
        error!(
            "Failed to build rayon global thread pool: {:?}, maybe exists ",
            e
        );
    }
}
