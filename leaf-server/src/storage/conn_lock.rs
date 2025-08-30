use futures_util::pin_mut;
use std::{
    sync::{Arc, OnceLock},
    task::Poll,
};
use tokio::sync::Notify;

#[derive(Default, Clone)]
pub struct Conn {
    lock: Arc<OnceLock<libsql::Connection>>,
    notify: Arc<Notify>,
}
impl Conn {
    pub fn new() -> Self {
        Conn {
            lock: Arc::new(OnceLock::new()),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn has_initialized(&self) -> bool {
        self.lock.get().is_some()
    }

    pub fn set(&self, conn: libsql::Connection) -> Result<(), libsql::Connection> {
        if !self.has_initialized() {
            self.lock.set(conn)
        } else {
            Err(conn)
        }
    }
}

impl<'a> Future for &'a Conn {
    type Output = &'a libsql::Connection;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Conn { lock, notify } = &**self;

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
