use crate::HttpsSource;
use pir_service_discovery as discovery;
use std::{fmt, time::Duration};

/// Whole-request deadline for one discovery fetch, including the body.
pub const DISCOVERY_DEADLINE: Duration = Duration::from_secs(10);

/// A publisher whose discovery document could not be accepted. Only
/// [`DiscoveryError::incompatible`] is a permanent condition; every other
/// reason may clear when the publisher or network recovers.
pub struct DiscoveryError {
    origin: String,
    reason: Reason,
}
#[derive(Debug)]
enum Reason {
    Unreachable(String),
    Deadline(u64),
    Status(reqwest::StatusCode),
    TooLarge,
    Document(discovery::Error),
}
impl DiscoveryError {
    pub fn origin(&self) -> &str {
        &self.origin
    }
    /// The confirmed contract version mismatch, if this is one.
    pub fn incompatible(&self) -> Option<&discovery::Incompatible> {
        match &self.reason {
            Reason::Document(error) => error.incompatible(),
            _ => None,
        }
    }
}
impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "publisher {} service discovery failed: ", self.origin)?;
        match &self.reason {
            Reason::Unreachable(error) => write!(f, "publisher unreachable: {error}"),
            Reason::Deadline(seconds) => write!(f, "no complete response within {seconds}s"),
            Reason::Status(status) if *status == reqwest::StatusCode::NOT_FOUND => write!(
                f,
                "HTTP 404 at {}; the publisher does not expose service discovery \
                 (upgrade it, or proxy that exact path to its management listener)",
                discovery::PATH
            ),
            Reason::Status(status) => write!(f, "HTTP {status} at {}", discovery::PATH),
            Reason::TooLarge => write!(
                f,
                "document exceeds {} bytes",
                discovery::MAX_DOCUMENT_BYTES
            ),
            Reason::Document(error) => error.fmt(f),
        }
    }
}
// A host `main` that returns this error prints it with `Debug`; operators get
// the same actionable message as `Display`.
impl fmt::Debug for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for DiscoveryError {}

/// Whether `error` is a confirmed contract version mismatch. Classification is
/// by type, never by message text.
pub fn is_incompatible(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<DiscoveryError>()
        .is_some_and(|error| error.incompatible().is_some())
}

impl HttpsSource {
    /// Fetch and check the publisher's discovery document. The request uses the
    /// configured origin and trust roots, follows no redirect, is bounded by
    /// [`DISCOVERY_DEADLINE`] and [`discovery::MAX_DOCUMENT_BYTES`], and does not
    /// count against the product download budget. No result is cached.
    pub async fn discover(&self) -> Result<discovery::Version, DiscoveryError> {
        self.discover_within(DISCOVERY_DEADLINE).await
    }

    /// [`discover`] with an explicit whole-request deadline. Production uses
    /// [`DISCOVERY_DEADLINE`]; tests pass a shorter bound and still require the
    /// stalled body to hit it.
    pub(crate) async fn discover_within(
        &self,
        deadline: Duration,
    ) -> Result<discovery::Version, DiscoveryError> {
        let fail = |reason| DiscoveryError {
            origin: self.origin.as_str().trim_end_matches('/').to_string(),
            reason,
        };
        let url = self
            .origin
            .join(discovery::PATH)
            .map_err(|error| fail(Reason::Unreachable(error.to_string())))?;
        let fetch = async {
            let mut response = self
                .client
                .get(url)
                .header("accept", "application/json")
                .header("cache-control", "no-cache")
                .send()
                .await
                .map_err(|error| Reason::Unreachable(error_chain(&error)))?;
            if response.status() != reqwest::StatusCode::OK {
                return Err(Reason::Status(response.status()));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| Reason::Unreachable(error_chain(&error)))?
            {
                if chunk.len() > discovery::MAX_DOCUMENT_BYTES.saturating_sub(body.len()) {
                    return Err(Reason::TooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            discovery::check_publisher(&body).map_err(Reason::Document)
        };
        match tokio::time::timeout(deadline, fetch).await {
            Ok(result) => result.map_err(fail),
            Err(_) => Err(fail(Reason::Deadline(deadline.as_secs()))),
        }
    }
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}
