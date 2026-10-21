//! Streaming mechanics only. Products select verification and own file promotion.
use crate::{Error, Response};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Length is always checked. Products that validate bulk data after activation
/// explicitly select `Length`; selecting it grants no authority to serve.
pub enum Verification<'a> {
    Length,
    Sha256(&'a str),
}

pub struct Transfer {
    pub bytes: u64,
    pub elapsed: Duration,
}

impl Response<'_> {
    /// Copy a successful response without buffering the whole object. Every body
    /// chunk uses the source's existing aggregate budget. The caller owns flush,
    /// fsync, cleanup and promotion; errors may leave partial bytes in `output`.
    pub async fn copy_to(
        mut self,
        output: &mut (impl AsyncWrite + Unpin),
        expected_bytes: u64,
        verification: Verification<'_>,
    ) -> Result<Transfer, Error> {
        if !self.status().is_success() {
            let status = self.status();
            self.discard().await?;
            return Err(format!("artifact request refused with HTTP {status}").into());
        }
        let started = Instant::now();
        let mut remaining = expected_bytes;
        let mut hash = match verification {
            Verification::Length => None,
            Verification::Sha256(_) => Some(Sha256::new()),
        };
        while let Some(chunk) = self.chunk().await? {
            if chunk.len() as u64 > remaining {
                return Err("artifact exceeds declared length".into());
            }
            if let Some(hash) = &mut hash {
                hash.update(&chunk);
            }
            output.write_all(&chunk).await?;
            remaining -= chunk.len() as u64;
        }
        if remaining != 0 {
            return Err("artifact incomplete".into());
        }
        if let (Some(hash), Verification::Sha256(expected)) = (hash, verification) {
            if format!("{:x}", hash.finalize()) != expected {
                return Err("artifact digest mismatch".into());
            }
        }
        Ok(Transfer {
            bytes: expected_bytes,
            elapsed: started.elapsed(),
        })
    }
}
