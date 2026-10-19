//! Process-neutral ownership for publisher tasks. HTTP routers remain generic so
//! the lifecycle has no dependency on a server framework or product.
use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::sync::watch;

/// Cooperative stop notification. Owners observe it between complete operations.
#[derive(Clone)]
pub struct Shutdown(watch::Sender<bool>);
impl Default for Shutdown {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}
impl Shutdown {
    pub fn request(&self) {
        self.0.send_replace(true);
    }
    pub fn requested(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

pub type HttpFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
type Serve<R> = dyn Fn(SocketAddr, R) -> HttpFuture + Send + Sync;
/// The application supplies listener ownership; libraries supply unchanged routers.
pub struct Http<R>(Arc<Serve<R>>);
impl<R> Clone for Http<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<R> Http<R> {
    pub fn new<F, Fut>(serve: F) -> Self
    where
        F: Fn(SocketAddr, R) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        Self(Arc::new(move |addr, router| Box::pin(serve(addr, router))))
    }
    pub fn serve(&self, addr: SocketAddr, router: R) -> HttpFuture {
        (self.0)(addr, router)
    }
}

/// Cancels owned auxiliary tasks when an application returns, including errors.
/// Publication transactions themselves must finish before dropping this guard.
#[derive(Default)]
pub struct Tasks(Vec<tokio::task::AbortHandle>);
impl Tasks {
    pub fn own<T>(&mut self, task: &tokio::task::JoinHandle<T>) {
        self.0.push(task.abort_handle());
    }
}
impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Keeps one process in charge of a product's application state. The file is not
/// data: it remains empty and is never used as a recovery or publication input.
pub fn lock(root: &std::path::Path) -> io::Result<std::fs::File> {
    std::fs::create_dir_all(root)?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("publisher.lock"))?;
    file.try_lock().map_err(io::Error::other)?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn shutdown_before_subscription_is_not_lost() {
        let stop = Shutdown::default();
        stop.request();
        tokio::time::timeout(std::time::Duration::from_millis(100), stop.cancelled())
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn dropping_task_owner_stops_auxiliary_work() {
        let task = tokio::spawn(std::future::pending::<()>());
        let mut owner = Tasks::default();
        owner.own(&task);
        drop(owner);
        assert!(task.await.unwrap_err().is_cancelled());
    }
}
