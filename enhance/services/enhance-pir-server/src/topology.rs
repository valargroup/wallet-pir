//! Durable, append-only operator topology. This is never exposed by Caddy.
use crate::coordinator::{WorkerGroup, WorkerTarget};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs, io::Write, path::PathBuf, sync::Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub const MAX_GROUPS: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Replica {
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub name: String,
    pub replicas: Vec<Replica>,
}

impl Group {
    pub fn target(&self) -> WorkerGroup {
        WorkerGroup {
            name: self.name.clone(),
            replicas: self
                .replicas
                .iter()
                .map(|r| WorkerTarget::Remote {
                    name: r.name.clone(),
                    base_url: r.url.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Append {
    pub operation_id: String,
    pub expected_revision: u64,
    pub groups: Vec<Group>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub shards_per_group: u64,
    pub revision: u64,
    pub groups: Vec<Group>,
    pub pending: Option<Append>,
    pub last_operation: Option<Append>,
    pub error: Option<String>,
}

pub struct TopologyStore {
    path: PathBuf,
    inner: Mutex<Status>,
}

pub fn validate(groups: &[Group]) -> Result<(), String> {
    if groups.is_empty() || groups.len() > MAX_GROUPS {
        return Err("topology must contain one to four groups".into());
    }
    let mut names = HashSet::new();
    let mut urls = HashSet::new();
    for group in groups {
        if group.replicas.len() != 2 {
            return Err("each group requires two replicas".into());
        }
        for name in std::iter::once(&group.name).chain(group.replicas.iter().map(|r| &r.name)) {
            if name.len() > 64
                || name.is_empty()
                || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                || !names.insert(name)
            {
                return Err("invalid or duplicate topology name".into());
            }
        }
        for replica in &group.replicas {
            if replica.url.len() > 256 {
                return Err("worker URL is too long".into());
            }
            let url = reqwest::Url::parse(&replica.url).map_err(|_| "invalid worker URL")?;
            if url.scheme() != "http"
                || url.host_str().is_none()
                || url.port().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
                || replica.url.ends_with('/')
                || !urls.insert(replica.url.clone())
            {
                return Err("worker URL must be a unique http origin with an explicit port".into());
            }
        }
    }
    Ok(())
}

impl TopologyStore {
    pub fn open(path: PathBuf, initial: Vec<Group>) -> Result<Self, String> {
        let status = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            Status {
                shards_per_group: crate::types::SHARDS_PER_GROUP,
                revision: 0,
                groups: initial,
                pending: None,
                last_operation: None,
                error: None,
            }
        };
        if status.shards_per_group != crate::types::SHARDS_PER_GROUP {
            return Err("persisted shard range differs from this binary".into());
        }
        validate(&status.groups)?;
        if let Some(pending) = &status.pending {
            validate(&pending.groups)?;
            if pending.expected_revision != status.revision
                || pending.groups.len() != status.groups.len() + 1
                || !pending.groups.starts_with(&status.groups)
            {
                return Err("invalid persisted pending topology".into());
            }
        }
        let store = Self {
            path,
            inner: Mutex::new(status),
        };
        store.save(&store.status())?;
        Ok(store)
    }

    pub fn status(&self) -> Status {
        self.inner.lock().expect("topology mutex poisoned").clone()
    }

    fn save(&self, status: &Status) -> Result<(), String> {
        let parent = self.path.parent().ok_or("topology path needs a parent")?;
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let temp = self.path.with_extension("next");
        let mut file = fs::File::create(&temp).map_err(|e| e.to_string())?;
        file.write_all(&serde_json::to_vec_pretty(status).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(temp, &self.path).map_err(|e| e.to_string())?;
        fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }

    pub fn append(&self, request: Append) -> Result<Status, String> {
        validate(&request.groups)?;
        if request.operation_id.is_empty() || request.operation_id.len() > 128 {
            return Err("invalid operation ID".into());
        }
        let mut current = self.inner.lock().map_err(|e| e.to_string())?;
        for old in current.pending.iter().chain(current.last_operation.iter()) {
            if old.operation_id == request.operation_id {
                return if old.groups == request.groups
                    && old.expected_revision == request.expected_revision
                {
                    Ok(current.clone())
                } else {
                    Err("operation ID reused for a different request".into())
                };
            }
        }
        if current.pending.is_some() {
            return Err("another append is pending".into());
        }
        if request.expected_revision != current.revision
            || request.groups.len() != current.groups.len() + 1
            || !request.groups.starts_with(&current.groups)
        {
            return Err("expected revision or append-only inventory mismatch".into());
        }
        let mut next = current.clone();
        next.pending = Some(request);
        next.error = None;
        self.save(&next)?;
        *current = next;
        Ok(current.clone())
    }

    /// Persist before publication. A restart resumes the committed inventory.
    pub fn finish(&self, operation_id: &str, error: Option<String>) -> Result<(), String> {
        let mut current = self.inner.lock().map_err(|e| e.to_string())?;
        let mut next = current.clone();
        let pending = next.pending.take().ok_or("no pending topology")?;
        if pending.operation_id != operation_id {
            return Err("pending operation changed".into());
        }
        if error.is_none() {
            next.groups = pending.groups.clone();
            next.revision += 1;
        }
        next.last_operation = Some(pending);
        next.error = error;
        self.save(&next)?;
        *current = next;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "kebab-case", deny_unknown_fields)]
enum Request {
    Status,
    AppendGroup { request: Append },
}

/// One bounded JSON line per connection, with a five-second request deadline.
pub async fn serve(path: PathBuf, store: std::sync::Arc<TopologyStore>) -> Result<(), String> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    if path.exists() {
        if !fs::symlink_metadata(&path)
            .map_err(|e| e.to_string())?
            .file_type()
            .is_socket()
        {
            return Err("control socket path is not a socket".into());
        }
        if tokio::net::UnixStream::connect(&path).await.is_ok() {
            return Err("control socket is already active".into());
        }
        fs::remove_file(&path).map_err(|e| e.to_string())?;
    }
    let listener = tokio::net::UnixListener::bind(&path).map_err(|e| e.to_string())?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    loop {
        let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let store = store.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
                let (read, mut write) = stream.into_split();
                let mut reader = BufReader::new(tokio::io::AsyncReadExt::take(read, 65537));
                let mut line = String::new();
                reader.read_line(&mut line).await?;
                let result = if line.len() > 65536 || !line.ends_with('\n') {
                    Err("request too large or incomplete".into())
                } else {
                    serde_json::from_str::<Request>(&line)
                        .map_err(|e| e.to_string())
                        .and_then(|request| match request {
                            Request::Status => Ok(store.status()),
                            Request::AppendGroup { request } => store.append(request),
                        })
                };
                let response = match result {
                    Ok(status) => serde_json::json!({"ok": true, "status": status}),
                    Err(error) => serde_json::json!({"ok": false, "error": error}),
                };
                write.write_all(format!("{response}\n").as_bytes()).await
            })
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn group(n: usize) -> Group {
        Group {
            name: format!("group-{n}"),
            replicas: (0..2)
                .map(|r| Replica {
                    name: format!("worker-{n}-{r}"),
                    url: format!("http://10.0.{n}.{}:8091", r + 1),
                })
                .collect(),
        }
    }
    #[test]
    fn append_is_durable_idempotent_and_preserves_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("topology.json");
        let store = TopologyStore::open(path.clone(), vec![group(1)]).unwrap();
        let append = Append {
            operation_id: "test".into(),
            expected_revision: 0,
            groups: vec![group(1), group(2)],
        };
        store.append(append.clone()).unwrap();
        store.append(append.clone()).unwrap();
        let reopened = TopologyStore::open(path.clone(), vec![group(3)]).unwrap();
        reopened.finish("test", None).unwrap();
        assert_eq!(reopened.append(append).unwrap().revision, 1);
        assert_eq!(
            TopologyStore::open(path, vec![])
                .unwrap()
                .status()
                .groups
                .len(),
            2
        );
        assert!(reopened
            .append(Append {
                operation_id: "bad".into(),
                expected_revision: 1,
                groups: vec![group(2), group(1), group(3)]
            })
            .is_err());
    }
    #[test]
    fn failed_append_keeps_active_topology() {
        let dir = tempfile::tempdir().unwrap();
        let store = TopologyStore::open(dir.path().join("state"), vec![group(1)]).unwrap();
        store
            .append(Append {
                operation_id: "fail".into(),
                expected_revision: 0,
                groups: vec![group(1), group(2)],
            })
            .unwrap();
        store
            .finish("fail", Some("qualification failed".into()))
            .unwrap();
        assert_eq!(store.status().groups, vec![group(1)]);
        assert_eq!(store.status().revision, 0);
    }
}
