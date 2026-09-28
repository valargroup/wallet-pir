use std::sync::{Mutex, OnceLock};
use zakura_pir_enhance::{
    ClientError,
    transport::{Method, Request, ResponseBody, Transport},
};
static HTTP: OnceLock<Mutex<Vec<serde_json::Value>>> = OnceLock::new();
struct Logger;
static LOGGER: Logger = Logger;
impl log::Log for Logger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, r: &log::Record) {
        let s = r.args().to_string();
        if !s.starts_with("pir_http ") {
            return;
        }
        let mut v = serde_json::Map::new();
        for p in s.split_whitespace().skip(1) {
            if let Some((k, x)) = p.split_once('=') {
                v.insert(
                    k.into(),
                    x.parse::<u64>()
                        .map_or_else(|_| serde_json::Value::String(x.into()), Into::into),
                );
            }
        }
        HTTP.get().unwrap().lock().unwrap().push(v.into());
    }
    fn flush(&self) {}
}
pub fn init() {
    HTTP.get_or_init(Default::default);
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Info);
}
pub fn take() -> Vec<serde_json::Value> {
    std::mem::take(&mut *HTTP.get().unwrap().lock().unwrap())
}
pub struct Http(pub reqwest::Client);
impl Transport for Http {
    async fn execute(&self, request: Request) -> Result<ResponseBody, ClientError> {
        let start = std::time::Instant::now();
        let sent = request.body.len();
        let method = match request.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
        };
        let kind = if matches!(request.method, Method::Get) {
            "get"
        } else {
            "post"
        };
        let url =
            reqwest::Url::parse(&request.url).map_err(|e| ClientError::Transport(e.to_string()))?;
        if url.scheme() != "http" || url.host_str() != Some("127.0.0.1") {
            return Err(ClientError::Transport(
                "fixture route escaped loopback".into(),
            ));
        }
        let mut body = request.response_body();
        let mut received = 0;
        let mut response = self
            .0
            .request(method, url)
            .body(request.body)
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(ClientError::HttpStatus(response.status().as_u16()));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?
        {
            received += chunk.len();
            body.extend(&chunk)?;
        }
        HTTP.get().unwrap().lock().unwrap().push(serde_json::json!({"component":"enhance","kind":kind,"sent_bytes":sent,"received_bytes":received,"elapsed_us":start.elapsed().as_micros()}));
        Ok(body.finish())
    }
}
