//! Pins what a worker answers, byte for byte, to a checked-in transcript.
//!
//! Each request is recorded as one block: the request, then the status, the
//! content type, any retry delay and the body. Digests are replaced by stable
//! names so the transcript reads the same on every run. Setup answers carry
//! freshly generated public parameters, so those three fields are replaced
//! too; everything else, including every refusal's wording, is compared as
//! sent. Set `TRANSPARENT_GOLDEN_UPDATE=1` to rewrite a transcript after an
//! intended change, and review the diff.

#![allow(dead_code)]

use axum::body::{Body, Bytes};
use axum::http::Request;
use axum::Router;
use std::convert::Infallible;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::ServiceExt;

/// A request body, and whether its length is declared.
pub enum Upload {
    None,
    /// Sent with a `content-length`.
    Sized(Vec<u8>),
    /// Streamed with no declared length, as a chunked upload is.
    Unsized(Vec<u8>),
}

/// A one-frame body that does not say how long it is.
struct Undeclared(Option<Bytes>);

impl http_body::Body for Undeclared {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}

pub struct Transcript {
    names: Vec<(String, String)>,
    text: String,
}

impl Transcript {
    /// `names` maps each digest that may appear to the name it is shown as.
    pub fn new(names: &[(&str, &str)]) -> Self {
        let mut names: Vec<(String, String)> = names
            .iter()
            .map(|(value, name)| (value.to_string(), format!("<{name}>")))
            .collect();
        // Longer values first, so a digest is never replaced inside another.
        names.sort_by_key(|(value, _)| std::cmp::Reverse(value.len()));
        Self {
            names,
            text: String::new(),
        }
    }

    fn named(&self, text: &str) -> String {
        self.names
            .iter()
            .fold(text.to_string(), |text, (value, name)| {
                text.replace(value, name)
            })
    }

    pub async fn send(&mut self, app: &Router, path: &str, upload: Upload) {
        let builder = Request::builder().uri(path);
        let (method, request) = match upload {
            Upload::None => ("GET", builder.body(Body::empty())),
            Upload::Sized(bytes) => (
                "POST",
                builder
                    .method("POST")
                    .header("content-length", bytes.len().to_string())
                    .body(Body::from(bytes)),
            ),
            Upload::Unsized(bytes) => (
                "POST",
                builder
                    .method("POST")
                    .body(Body::new(Undeclared(Some(Bytes::from(bytes))))),
            ),
        };
        let response = app.clone().oneshot(request.unwrap()).await.unwrap();
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_string())
                .unwrap_or_else(|| "-".into())
        };
        let head = format!(
            "{method} {}\n  {} content-type={} retry-after={}\n",
            self.named(path),
            response.status().as_u16(),
            header("content-type"),
            header("retry-after"),
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(mut value) => {
                for field in [
                    "public_params",
                    "public_params_sha256",
                    "public_params_epoch",
                ] {
                    if let Some(slot) = value.get_mut(field) {
                        *slot = "<generated>".into();
                    }
                }
                value.to_string()
            }
            Err(_) => format!("<{} bytes>", body.len()),
        };
        self.text += &head;
        self.text += &format!("  {}\n", self.named(&body));
    }

    /// Compares with `tests/golden/<file>`, or rewrites it under
    /// `TRANSPARENT_GOLDEN_UPDATE=1`.
    pub fn check(self, file: &str) {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/transparent/golden")
            .join(file);
        if std::env::var_os("TRANSPARENT_GOLDEN_UPDATE").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &self.text).unwrap();
            return;
        }
        let pinned = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "{}: {e}; run with TRANSPARENT_GOLDEN_UPDATE=1",
                path.display()
            )
        });
        if pinned != self.text {
            let first = pinned
                .lines()
                .zip(self.text.lines())
                .position(|(a, b)| a != b)
                .unwrap_or_else(|| pinned.lines().count().min(self.text.lines().count()));
            panic!(
                "{} differs from what the worker answered, first at line {}:\n  pinned: {:?}\n  served: {:?}",
                path.display(),
                first + 1,
                pinned.lines().nth(first),
                self.text.lines().nth(first),
            );
        }
    }
}
