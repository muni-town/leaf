use futures_util::pin_mut;
use std::{
    sync::{Arc, OnceLock},
    task::Poll,
};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct AsyncOnceLock<T> {
    lock: Arc<OnceLock<T>>,
    notify: Arc<Notify>,
}
impl<T> Default for AsyncOnceLock<T> {
    fn default() -> Self {
        Self {
            lock: Default::default(),
            notify: Default::default(),
        }
    }
}
impl<T> AsyncOnceLock<T> {
    pub fn new() -> Self {
        AsyncOnceLock {
            lock: Arc::new(OnceLock::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn has_initialized(&self) -> bool {
        self.lock.get().is_some()
    }

    pub fn set(&self, conn: T) -> Result<(), T> {
        if !self.has_initialized() {
            self.lock.set(conn)
        } else {
            Err(conn)
        }
    }
}

impl<'a, T> Future for &'a AsyncOnceLock<T> {
    type Output = &'a T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let AsyncOnceLock { lock, notify } = &**self;

        loop {
            if let Some(conn) = lock.get() {
                break Poll::Ready(conn);
            } else {
                let n = notify.notified();
                pin_mut!(n);
                if n.poll(cx).is_ready() {
                    continue;
                }
                break Poll::Pending;
            }
        }
    }
}
