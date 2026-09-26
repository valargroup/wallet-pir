use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};

use crate::schema::Schema;

#[derive(Clone, Debug)]
pub struct Config {
    pub worker_config: Option<PathBuf>,
    pub packing_router_config: Option<PathBuf>,
    pub scrape_url: String,
    pub query_scrape_urls: Vec<String>,
    pub status_apm_url: Option<String>,
    pub status_router_url: Option<String>,
    pub status_worker_url: Option<String>,
    pub status_host: String,
    pub metrics_path: String,
    pub health_path: String,
    pub ready_path: String,
    pub listen: SocketAddr,
    pub slack_webhook_url: Option<String>,
    pub title: String,
    pub environment: String,
    pub hostname: String,
    pub data_dir: PathBuf,
    pub interval: Duration,
    pub schema: Schema,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let vars: HashMap<String, String> = env::vars()
            .filter(|(name, _)| name.starts_with("PIR_APM_"))
            .collect();
        Self::from_map(&vars)
    }

    /// Build from an explicit map so tests never have to mutate process env.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Self> {
        let get = |name: &str| -> Option<String> {
            vars.get(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let get_or = |name: &str, default: &str| get(name).unwrap_or_else(|| default.to_string());

        let scrape_url = get_or("PIR_APM_SCRAPE_URL", "http://127.0.0.1:8080")
            .trim_end_matches('/')
            .to_string();
        let listen = get_or("PIR_APM_LISTEN", "127.0.0.1:3002")
            .parse()
            .context("PIR_APM_LISTEN must be an IP:port socket address")?;
        let interval_seconds: u64 = get_or("PIR_APM_INTERVAL_SECONDS", "15")
            .parse()
            .context("PIR_APM_INTERVAL_SECONDS must be an integer")?;
        if interval_seconds == 0 {
            anyhow::bail!("PIR_APM_INTERVAL_SECONDS must be greater than zero");
        }

        let defaults = Schema::enhance_default();
        let prefix = get_or("PIR_APM_METRIC_PREFIX", &defaults.prefix);
        let endpoints = match get("PIR_APM_ENDPOINTS") {
            Some(list) => split_list(&list),
            None => defaults.endpoints.clone(),
        };
        let processing_endpoints: BTreeSet<String> = match get("PIR_APM_PROCESSING_ENDPOINTS") {
            Some(list) => split_list(&list).into_iter().collect(),
            None => defaults.processing_endpoints.clone(),
        };
        let informational_endpoints: BTreeSet<String> = match get("PIR_APM_INFORMATIONAL_ENDPOINTS")
        {
            Some(list) => split_list(&list).into_iter().collect(),
            None => defaults.informational_endpoints.clone(),
        };
        let default_latency_p99 = match get("PIR_APM_LATENCY_P99_SECONDS") {
            Some(value) => value
                .parse()
                .context("PIR_APM_LATENCY_P99_SECONDS must be a number")?,
            None => defaults.default_latency_p99,
        };
        let latency_overrides = match get("PIR_APM_LATENCY_P99_OVERRIDES") {
            Some(list) => parse_overrides(&list)?,
            None => defaults.latency_overrides.clone(),
        };
        let schema = Schema::new(
            &prefix,
            endpoints,
            processing_endpoints,
            informational_endpoints,
            default_latency_p99,
            latency_overrides,
        )
        .map_err(anyhow::Error::msg)
        .context("invalid PIR_APM metric schema")?;

        let query_scrape_urls = get("PIR_APM_QUERY_SCRAPE_URLS")
            .map(|s| split_list(&s))
            .unwrap_or_default();
        if query_scrape_urls.len() > 8 {
            anyhow::bail!("at most eight query ingress metrics targets are supported");
        }
        let mut seen = BTreeSet::new();
        for url in &query_scrape_urls {
            let parsed = reqwest::Url::parse(url).context("invalid query metrics URL")?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.fragment().is_some()
                || !seen.insert(parsed.to_string())
            {
                anyhow::bail!("query metrics targets must be distinct HTTP(S) URLs without credentials or fragments");
            }
        }
        let status_apm_url = get("PIR_APM_STATUS_URL");
        let status_router_url = get("PIR_APM_STATUS_ROUTER_URL");
        let status_worker_url = get("PIR_APM_STATUS_WORKER_URL");
        if status_router_url.is_some() != status_worker_url.is_some()
            || (status_router_url.is_some() && status_apm_url.is_none())
        {
            anyhow::bail!("Status role monitoring requires all three URLs");
        }
        for url in [&status_apm_url, &status_router_url, &status_worker_url]
            .into_iter()
            .flatten()
        {
            let parsed = reqwest::Url::parse(url).context("invalid Status APM URL")?;
            if parsed.scheme() != "http"
                || parsed.host_str() != Some("127.0.0.1")
                || parsed.username() != ""
                || parsed.password().is_some()
                || parsed.fragment().is_some()
                || parsed.query().is_some()
                || parsed.path() != "/internal/status-apm"
            {
                anyhow::bail!("Status APM URL must be a credential-free loopback /internal/status-apm endpoint");
            }
        }

        Ok(Self {
            worker_config: get("PIR_APM_WORKER_CONFIG").map(PathBuf::from),
            packing_router_config: get("PIR_APM_PACKING_ROUTER_CONFIG").map(PathBuf::from),
            scrape_url,
            query_scrape_urls,
            status_apm_url,
            status_router_url,
            status_worker_url,
            status_host: get_or("PIR_APM_STATUS_HOST", "status-pir-p4000-ams1"),
            metrics_path: path_value(&get_or("PIR_APM_METRICS_PATH", "/metrics"))?,
            health_path: path_value(&get_or("PIR_APM_HEALTH_PATH", "/v1/health"))?,
            ready_path: path_value(&get_or("PIR_APM_READY_PATH", "/ready"))?,
            listen,
            slack_webhook_url: get("PIR_APM_SLACK_WEBHOOK_URL"),
            title: get_or("PIR_APM_TITLE", "PIR APM"),
            environment: get_or("PIR_APM_ENVIRONMENT", "unknown"),
            hostname: get("PIR_APM_HOSTNAME")
                .or_else(sysinfo::System::host_name)
                .unwrap_or_else(|| "unknown".to_string()),
            data_dir: PathBuf::from(get_or("PIR_APM_DATA_DIR", "/srv/zakura/enhance-data")),
            interval: Duration::from_secs(interval_seconds),
            schema,
        })
    }
}

fn split_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(String::from)
        .collect()
}

fn parse_overrides(list: &str) -> Result<BTreeMap<String, f64>> {
    let mut overrides = BTreeMap::new();
    for item in split_list(list) {
        let (endpoint, seconds) = item
            .split_once('=')
            .with_context(|| format!("override {item:?} must look like endpoint=seconds"))?;
        let seconds: f64 = seconds
            .trim()
            .parse()
            .with_context(|| format!("override {item:?} has a non-numeric budget"))?;
        overrides.insert(endpoint.trim().to_string(), seconds);
    }
    Ok(overrides)
}

fn path_value(value: &str) -> Result<String> {
    if !value.starts_with('/') {
        anyhow::bail!("probe path {value:?} must start with '/'");
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn defaults_describe_the_enhance_coordinator() {
        let config = Config::from_map(&HashMap::new()).unwrap();
        assert_eq!(config.scrape_url, "http://127.0.0.1:8080");
        assert_eq!(config.health_path, "/v1/health");
        assert_eq!(config.ready_path, "/ready");
        assert_eq!(config.metrics_path, "/metrics");
        assert_eq!(config.schema, Schema::enhance_default());
        assert_eq!(config.title, "PIR APM");
        assert!(config.slack_webhook_url.is_none());
        assert_eq!(config.interval, Duration::from_secs(15));
    }

    #[test]
    fn env_overrides_the_schema() {
        let config = Config::from_map(&vars(&[
            ("PIR_APM_METRIC_PREFIX", "nf"),
            ("PIR_APM_ENDPOINTS", "tier0, params_tier1,tier1_query"),
            ("PIR_APM_PROCESSING_ENDPOINTS", "tier1_query"),
            ("PIR_APM_LATENCY_P99_SECONDS", "0.5"),
            ("PIR_APM_LATENCY_P99_OVERRIDES", "tier0=1.0, tier1_query=2"),
            ("PIR_APM_HEALTH_PATH", "/health"),
            ("PIR_APM_SCRAPE_URL", "http://127.0.0.1:3000/"),
            ("PIR_APM_SLACK_WEBHOOK_URL", "   "),
        ]))
        .unwrap();
        assert_eq!(config.scrape_url, "http://127.0.0.1:3000");
        assert_eq!(config.schema.requests_total, "nf_http_requests_total");
        assert_eq!(
            config.schema.endpoints,
            ["tier0", "params_tier1", "tier1_query"]
        );
        assert_eq!(config.schema.latency_budget("params_tier1"), Some(0.5));
        assert_eq!(config.schema.latency_budget("tier1_query"), Some(2.0));
        assert!(config.schema.uses_processing("tier1_query"));
        assert!(config.schema.is_informational("health"));
        assert_eq!(config.health_path, "/health");
        assert!(config.slack_webhook_url.is_none());
    }

    #[test]
    fn production_env_block_parses() {
        // Mirrors the /etc/default/pir-apm block written by deploy-enhance-pir.sh.
        let config = Config::from_map(&vars(&[
            ("PIR_APM_HEALTH_PATH", "/v1/health"),
            ("PIR_APM_ENDPOINTS", "health,init,query"),
            ("PIR_APM_INFORMATIONAL_ENDPOINTS", "health"),
            ("PIR_APM_PROCESSING_ENDPOINTS", "query"),
            ("PIR_APM_LATENCY_P99_SECONDS", "1.0"),
            ("PIR_APM_LATENCY_P99_OVERRIDES", "query=5.0,init=2.0"),
        ]))
        .unwrap();
        assert_eq!(config.schema.latency_budget("init"), Some(2.0));
        assert!(config.schema.uses_processing("query"));
    }

    #[test]
    fn rejects_bad_values() {
        assert!(Config::from_map(&vars(&[("PIR_APM_INTERVAL_SECONDS", "0")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_LISTEN", "nope")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_READY_PATH", "ready")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_LATENCY_P99_OVERRIDES", "query")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_LATENCY_P99_OVERRIDES", "Bad=1")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_LATENCY_P99_OVERRIDES", "nope=1")])).is_ok());
        assert!(Config::from_map(&vars(&[("PIR_APM_PROCESSING_ENDPOINTS", "no pe")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_INFORMATIONAL_ENDPOINTS", "Bad")])).is_err());
        assert!(Config::from_map(&vars(&[("PIR_APM_ENDPOINTS", "Bad Name")])).is_err());
    }
}
