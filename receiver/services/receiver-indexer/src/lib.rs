//! The receiver directory's indexer: it reads Ironwood blocks from a Zakura node,
//! keeps the payments recoverable with the zero outgoing viewing key, and publishes them
//! as receiver directory revisions. The binary is `receiver-directory`; see
//! `receiver/README.md`.
pub mod blocks;
pub mod near;
pub mod zakura;

/// A response body over its bound, or a failed read.
#[derive(Debug, thiserror::Error)]
pub enum BodyError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("response is larger than {0} bytes")]
    Oversized(usize),
}

/// `response`'s body, refused as soon as it declares or reaches more than `limit`
/// bytes, so an oversized body is never buffered past the limit.
pub async fn read_limited(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, BodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(BodyError::Oversized(limit));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > limit {
            return Err(BodyError::Oversized(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A server that answers one request with `body`, declaring its length when
    /// `declared` and otherwise ending it by closing the connection. Returns its URL.
    async fn serve(body: Vec<u8>, declared: bool) -> String {
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut stream, _) = socket.accept().await.unwrap();
            // Read the bodiless GET's head, so closing cannot reset the connection.
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                head.push(stream.read_u8().await.unwrap());
            }
            let length = match declared {
                true => format!("Content-Length: {}\r\n", body.len()),
                false => String::new(),
            };
            let head = format!("HTTP/1.1 200 OK\r\n{length}Connection: close\r\n\r\n");
            // The client may close first once it has seen enough.
            let _ = stream.write_all(&[head.as_bytes(), &body].concat()).await;
        });
        url
    }

    /// A body over the limit is refused whether or not it declares its length, and one
    /// at the limit is read whole.
    #[tokio::test]
    async fn bodies_are_bounded() {
        let read =
            |url: String| async move { read_limited(reqwest::get(url).await.unwrap(), 16).await };
        for declared in [true, false] {
            let exact = read(serve(vec![7; 16], declared).await).await;
            assert_eq!(exact.unwrap(), [7; 16]);
            let over = read(serve(vec![7; 17], declared).await).await;
            assert!(matches!(over, Err(BodyError::Oversized(16))), "{declared}");
        }
    }
}
