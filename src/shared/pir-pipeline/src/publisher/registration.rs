use super::Result;
use crate::service::{Http, Shutdown};
use std::{future::Future, net::SocketAddr, path::PathBuf, pin::Pin};

/// The host always requires HTTP success. Products may additionally inspect a
/// bounded response body; their wire formats are not part of the host contract.
#[derive(Clone, Copy)]
pub enum ReadinessRule {
    HttpSuccess,
    Body(fn(&[u8]) -> bool),
}
#[derive(Clone)]
pub struct ReadinessProbe {
    pub address: SocketAddr,
    pub path: &'static str,
    pub rule: ReadinessRule,
}

/// Static declarations only. Constructing these never acquires state or credentials.
#[derive(Default)]
pub struct PublisherResources {
    pub listeners: Vec<SocketAddr>,
    pub writable_paths: Vec<PathBuf>,
    pub probes: Vec<ReadinessProbe>,
}

/// Capabilities injected after every configured listener has been reserved.
pub struct ServiceContext<R> {
    pub http: Http<R>,
    pub shutdown: Shutdown,
}
type ServiceFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type Start<R> = Box<dyn FnOnce(ServiceContext<R>) -> ServiceFuture + Send>;

/// One configured service. A product validates settings and declares resources
/// before creating this value; its factory opens product state only on start.
pub struct PublisherDefinition<R> {
    pub name: &'static str,
    pub resources: PublisherResources,
    start: Start<R>,
}
impl<R> PublisherDefinition<R> {
    pub fn new<F, Fut>(name: &'static str, resources: PublisherResources, start: F) -> Self
    where
        F: FnOnce(ServiceContext<R>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            name,
            resources,
            start: Box::new(move |context| Box::pin(start(context))),
        }
    }
    /// Consume the factory once. The returned future owns product state through
    /// cooperative completion; the host must join it before releasing ownership.
    pub fn start(self, context: ServiceContext<R>) -> impl Future<Output = Result<()>> + Send {
        (self.start)(context)
    }
}
