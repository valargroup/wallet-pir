use std::sync::atomic::{AtomicBool, Ordering};
use zakura_pir_status::{self as wallet, transport::Transport};

pub struct Listening {
    pub origin: String,
    pub task: tokio::task::JoinHandle<()>,
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn serve(router: axum::Router) -> Listening {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    Listening {
        origin,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    }
}

/// The wallet requires an HTTPS origin. Only this test transport maps its fixed
/// logical origin to a plain HTTP loopback listener; every response is real HTTP.
pub struct Loopback {
    pub origin: String,
    pub http: reqwest::Client,
    pub corrupt_answer: AtomicBool,
}
impl Loopback {
    async fn request(
        &self,
        url: &str,
        body: Option<Vec<u8>>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        let path = url
            .strip_prefix("https://status-wallet.invalid")
            .ok_or(wallet::Error::Malformed)?;
        let url = format!("{}{path}", self.origin);
        let request = match body {
            Some(body) => self.http.post(url).body(body),
            None => self.http.get(url),
        };
        let mut response = request
            .send()
            .await
            .map_err(|_| wallet::Error::Unavailable)?;
        if !response.status().is_success() {
            return Err(wallet::Error::Unavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| wallet::Error::Unavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > max_bytes {
                return Err(wallet::Error::Malformed);
            }
            bytes.extend_from_slice(&chunk);
        }
        if path.ends_with("/query") && self.corrupt_answer.load(Ordering::SeqCst) {
            bytes[0] ^= 1;
        }
        Ok(bytes)
    }
}
impl Transport for Loopback {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, wallet::Error> {
        self.request(url, None, max_bytes).await
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        self.request(url, Some(body), max_bytes).await
    }
}
