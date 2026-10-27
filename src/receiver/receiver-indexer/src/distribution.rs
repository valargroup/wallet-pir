//! Publisher-only HTTPS distribution, with durable shared authority.
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_publication::{
    AuthorityCheckRequest, AuthorityCheckResponse, AuthorityTransition, PublicationAuthority,
    PublishedObjectStore, SubjectAnswer,
};
use receiver_pir_server::{distribution::*, Publications};
use std::{
    collections::HashMap,
    path::{Path as FilePath, PathBuf},
    sync::{Arc, Mutex},
};
use zakura_server_chain::{Network, NodeClient};

/// Bounds total work across retained views, before any node lookups. Repeated
/// anchors share one lookup; a disproven view stops checking immediately.
const MAX_CHECK_ANCHORS: usize = 4096;

#[cfg(test)]
use crate::probe::common;
#[cfg(test)]
#[path = "../../../pir-query-server/tests/support/status_tls.rs"]
mod tls;

#[derive(Clone)]
pub struct Distribution {
    authority: Arc<Mutex<PublicationAuthority<Descriptor>>>,
    objects: Arc<PublishedObjectStore>,
    rpc: NodeClient,
    data_dir: PathBuf,
    retention_ms: u64,
    exported: Arc<Mutex<HashMap<PathBuf, Exported>>>,
}
struct Exported {
    source: pir_publication::FileIdentity,
    object: pir_publication::FileIdentity,
    reference: pir_publication::ObjectReference,
    reusable: bool,
}
impl Distribution {
    /// Start in a new, withdrawn recovery epoch before exporting any material.
    pub fn open(data_dir: PathBuf, rpc: NodeClient, retention_seconds: u64) -> Result<Self, Error> {
        let root = data_dir.join("distribution");
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            authority: Arc::new(Mutex::new(PublicationAuthority::open(
                root.join("authority.json"),
                scope(
                    Network::Mainnet.genesis_hash(),
                    crate::blocks::ironwood_activation(),
                ),
            )?)),
            objects: Arc::new(PublishedObjectStore::open(root.join("objects"))?),
            rpc,
            data_dir,
            retention_ms: retention_seconds
                .checked_mul(1000)
                .ok_or("Receiver retention overflow")?,
            exported: Arc::default(),
        })
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/current", get(current))
            .route("/objects/:id", get(object))
            .route("/authority/check", post(authority_check))
            .with_state(self.clone())
    }
    /// Publish a negative envelope even if subsequent replacement work fails.
    pub fn withdraw(&self) -> Result<(), Error> {
        let mut authority = self.authority.lock().unwrap();
        // A failed write leaves current ineligible; another withdrawal retries
        // recovery, rather than silently considering that negative durable.
        authority.apply(AuthorityTransition::Withdraw)?;
        Ok(())
    }
    pub fn attach(&self, publications: &Publications) {
        let distribution = self.clone();
        publications.on_withdrawal(Arc::new(move || {
            if let Err(error) = distribution.withdraw() {
                tracing::error!(%error, "Receiver authority withdrawal is not durable");
            }
        }));
    }
    /// After a restart, publish the previous process's last inventory again
    /// under this process's recovery epoch, so query servers keep serving those
    /// tables while the first pass rebuilds them; the rebuilt export then finds
    /// them current. Only a publication of `history_revision` whose objects are
    /// all still retained and whose anchors are still canonical is restored, and
    /// query servers keep confirming those anchors through the authority check.
    /// Returns whether it published.
    pub async fn restore(
        &self,
        publications: &Publications,
        history_revision: u64,
    ) -> Result<bool, Error> {
        let epoch = publications.epoch();
        let (previous, recovery_epoch) = {
            let authority = self.authority.lock().unwrap();
            let current = authority.current();
            match current.publication.clone() {
                Some(previous)
                    if !current.eligible
                        && previous.descriptor.history_revision == history_revision
                        && previous.descriptor.recovery_epoch < current.fence.recovery_epoch =>
                {
                    (previous, current.fence.recovery_epoch)
                }
                _ => return Ok(false),
            }
        };
        let mut inventory: Inventory =
            serde_json::from_reader(self.objects.open_retained(&previous.inventory.sha256)?)?;
        inventory.recovery_epoch = recovery_epoch;
        inventory.validate(crate::blocks::ironwood_activation())?;
        for object in inventory.objects() {
            self.objects.open_retained(&object.sha256)?;
        }
        self.rpc.check_network(inventory.genesis).await?;
        for (height, expected) in inventory.anchors() {
            if self.rpc.block_hash(u64::from(height)).await? != expected {
                return Ok(false);
            }
        }
        let bytes = serde_json::to_vec(&inventory)?;
        let object = self.objects.store(&bytes)?;
        let reference = inventory.reference(object.clone());
        let mut objects = inventory.objects();
        objects.push(object);
        publications
            .in_epoch(epoch, || -> Result<bool, Error> {
                let mut authority = self.authority.lock().unwrap();
                let current = authority.current();
                // Another export or a withdrawal moved authority meanwhile.
                if current.eligible || current.fence.recovery_epoch != recovery_epoch {
                    return Ok(false);
                }
                let now = now_ms();
                self.objects.retain_replacement(
                    objects,
                    now.saturating_add(self.retention_ms),
                    now,
                )?;
                authority.apply(AuthorityTransition::Publish(reference))?;
                Ok(true)
            })
            .unwrap_or(Ok(false))
    }
    /// Export a pinned directory and independently advancing rolling hints.
    /// Objects never authorize themselves; the epoch guard protects final commit.
    pub async fn export<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        publications: &Publications,
        executor: &E,
    ) -> Result<(), Error> {
        let epoch = publications.epoch();
        let directory = publications.live().first().cloned();
        let filters = publications.filter_window();
        if directory.is_none() && filters.is_none() {
            return Ok(());
        }
        let revision = publications.history_revision();
        if directory.is_none() && self.serves_tables(revision) {
            return Ok(());
        }
        let recovery_epoch = self
            .authority
            .lock()
            .unwrap()
            .current()
            .fence
            .recovery_epoch;
        let descriptor = Descriptor {
            history_revision: revision,
            directory: directory.as_ref().map(|d| d.id()),
            filters: filters.as_ref().map(|f| f.head().clone()),
            recovery_epoch,
        };
        {
            let authority = self.authority.lock().unwrap();
            if authority.current().eligible
                && authority
                    .current()
                    .publication
                    .as_ref()
                    // Recovery moves the epoch inside one process without new
                    // tables; only a restart, which starts withdrawn, needs one.
                    .is_some_and(|p| {
                        p.descriptor
                            == Descriptor {
                                recovery_epoch: p.descriptor.recovery_epoch,
                                ..descriptor.clone()
                            }
                    })
            {
                return Ok(());
            }
        }
        executor
            .publish(Export {
                distribution: self.clone(),
                publications: publications.clone(),
                epoch,
                directory,
                filters,
                revision,
                recovery_epoch,
            })
            .await
    }
    /// Whether the eligible publication serves tables of `revision`. A restarted
    /// process restores its previous tables before it has built its own, and an
    /// export without tables must not replace them meanwhile. Losing tables any
    /// other way revokes them, which withdraws that publication first.
    fn serves_tables(&self, revision: u64) -> bool {
        let authority = self.authority.lock().unwrap();
        let current = authority.current();
        current.eligible
            && current.publication.as_ref().is_some_and(|p| {
                p.descriptor.directory.is_some() && p.descriptor.history_revision == revision
            })
    }
    fn file(&self, path: &FilePath) -> Result<pir_publication::ObjectReference, Error> {
        let file = pir_publication::open_object_file(path, None)?;
        let started = std::time::SystemTime::now();
        let source = pir_publication::FileIdentity::capture(path, &file)?;
        let mut cache = self.exported.lock().unwrap();
        if let Some(prior) = cache
            .get(path)
            .filter(|prior| prior.reusable && prior.source == source)
        {
            let object_path = self
                .data_dir
                .join("distribution/objects")
                .join(&prior.reference.sha256);
            if let Ok(object) = pir_publication::open_object_file(&object_path, None) {
                if prior.object.check(&object_path, &object).is_ok() {
                    source.check(path, &file)?;
                    return Ok(prior.reference.clone());
                }
            }
        }
        let reference = self.objects.store_stream(&file)?;
        source.check(path, &file)?;
        let object_path = self
            .data_dir
            .join("distribution/objects")
            .join(&reference.sha256);
        let object_file = pir_publication::open_object_file(&object_path, None)?;
        let object = pir_publication::FileIdentity::capture(&object_path, &object_file)?;
        cache.insert(
            path.into(),
            Exported {
                source,
                object,
                reference: reference.clone(),
                reusable: source.settled_before(started) && object.settled_before(started),
            },
        );
        Ok(reference)
    }
    fn material(
        &self,
        directory: Option<Arc<receiver_pir_server::Publication>>,
        filters: Option<Arc<receiver_pir_server::blocks::BlockWindow>>,
        revision: u64,
        recovery_epoch: u64,
    ) -> Result<Inventory, Error> {
        let root = self.data_dir.join(crate::durable::TABLES_DIR);
        let directory = directory
            .map(|publication| -> Result<Directory, Error> {
                let mut segments = Vec::new();
                for segment in publication.segments() {
                    let named = segment.publication();
                    let p = format!("p-{}", hex::encode(named.payments.directory.revision()?));
                    let w = format!("w-{}", hex::encode(named.witnesses.table.revision()?));
                    segments.push(Segment {
                        publication: named,
                        payments: self.file(&root.join(format!("{p}.rows")))?,
                        payment_public: self.file(&root.join(format!("{p}.public")))?,
                        witnesses: self.file(&root.join(format!("{w}.rows")))?,
                        witness_public: self.file(&root.join(format!("{w}.public")))?,
                    });
                }
                let filters = publication
                    .inventory()
                    .shards()
                    .iter()
                    .filter_map(|s| s.filter.as_ref())
                    .map(|filter| {
                        self.file(&root.join(format!("f-{}.filter", hex::encode(filter.digest))))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Directory {
                    head: publication.head().clone(),
                    shards: publication.inventory().shards().to_vec(),
                    segments,
                    filters,
                })
            })
            .transpose()?;
        let filters = filters
            .map(|window| -> Result<Filters, Error> {
                let head = window.head().clone();
                let mut from = head.first;
                let mut batches = Vec::new();
                while from <= head.tip.height {
                    let bytes = window
                        .batch(
                            head.revision,
                            from,
                            zakura_pir_query_server::receiver::blocks::MAX_BLOCKS,
                        )
                        .map_err(|_| "Receiver filter batch failed")?;
                    let decoded = zakura_pir_query_server::receiver::blocks::Batch::decode(&bytes)?;
                    from = from
                        .checked_add(u32::try_from(decoded.filters().len())?)
                        .ok_or("Receiver filter range overflow")?;
                    batches.push(self.objects.store(&bytes)?);
                }
                Ok(Filters { head, batches })
            })
            .transpose()?;
        Ok(Inventory {
            genesis: Network::Mainnet.genesis_hash(),
            history_revision: revision,
            directory,
            filters,
            recovery_epoch,
        })
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
async fn current(State(s): State<Distribution>, headers: HeaderMap) -> Response {
    let envelope = s.authority.lock().unwrap().current().clone();
    let token = pir_publication::conditional_token(&envelope);
    let mut response = if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some(&token)
    {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Json(envelope).into_response()
    };
    response
        .headers_mut()
        .insert("etag", token.parse().unwrap());
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}
async fn object(
    State(s): State<Distribution>,
    Path(id): Path<String>,
) -> Result<Response, StatusCode> {
    let file = s
        .objects
        .open_retained(&id)
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Body::from_stream(tokio_util::io::ReaderStream::new(
        tokio::fs::File::from_std(file),
    ))
    .into_response())
}
async fn authority_check(
    State(s): State<Distribution>,
    Json(request): Json<AuthorityCheckRequest<Subject>>,
) -> Result<Json<AuthorityCheckResponse<Subject>>, StatusCode> {
    let admit = || {
        request
            .admit(s.authority.lock().unwrap().current())
            .map_err(|refusal| match refusal {
                pir_publication::AuthorityCheckRefusal::MalformedRequest => StatusCode::BAD_REQUEST,
                pir_publication::AuthorityCheckRefusal::AuthorityAdvanced => StatusCode::CONFLICT,
            })
    };
    admit()?;
    let count = request.subjects.iter().try_fold(0usize, |count, subject| {
        if subject.is_empty() {
            return None;
        }
        count
            .checked_add(subject.len())
            .filter(|n| *n <= MAX_CHECK_ANCHORS)
    });
    if count.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    s.rpc
        .check_network(Network::Mainnet.genesis_hash())
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let mut answers = Vec::new();
    let mut checked = HashMap::new();
    for subject in &request.subjects {
        let mut canonical = true;
        for &(height, expected) in subject {
            let actual = match checked.get(&height) {
                Some(hash) => *hash,
                None => {
                    let hash = s
                        .rpc
                        .block_hash(u64::from(height))
                        .await
                        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                    checked.insert(height, hash);
                    hash
                }
            };
            if actual != expected {
                canonical = false;
                break;
            }
        }
        answers.push(SubjectAnswer {
            subject: subject.clone(),
            still_authoritative: canonical,
        });
    }
    admit()?;
    Ok(Json(AuthorityCheckResponse {
        authority: request.authority,
        answers,
    }))
}

struct Export {
    distribution: Distribution,
    publications: Publications,
    epoch: u64,
    directory: Option<Arc<receiver_pir_server::Publication>>,
    filters: Option<Arc<receiver_pir_server::blocks::BlockWindow>>,
    revision: u64,
    recovery_epoch: u64,
}
struct Candidate {
    inventory: Inventory,
    reference: pir_publication::PublicationReference<Descriptor>,
    objects: Vec<pir_publication::ObjectReference>,
}
impl pir_pipeline::publisher::Publication for Export {
    type Error = Error;
    type Material = Inventory;
    type Stored = Inventory;
    type Verified = Inventory;
    type Candidate = Candidate;
    type Report = ();
    fn check(&self) -> Result<(), Error> {
        if self.publications.epoch() != self.epoch {
            return Err("Receiver export invalidated during preparation".into());
        }
        Ok(())
    }
    async fn build(&mut self) -> Result<Inventory, Error> {
        let this = self.distribution.clone();
        let directory = self.directory.clone();
        let filters = self.filters.clone();
        let (revision, recovery_epoch) = (self.revision, self.recovery_epoch);
        tokio::task::spawn_blocking(move || {
            this.material(directory, filters, revision, recovery_epoch)
        })
        .await?
    }
    async fn store(&mut self, inventory: Inventory) -> Result<Inventory, Error> {
        Ok(inventory)
    }
    async fn verify(&mut self, inventory: Inventory) -> Result<Inventory, Error> {
        inventory.validate(crate::blocks::ironwood_activation())?;
        Ok(inventory)
    }
    async fn assemble(&mut self, inventory: Inventory) -> Result<Candidate, Error> {
        let bytes = serde_json::to_vec(&inventory)?;
        if bytes.len() as u64 > METADATA_LIMIT {
            return Err("Receiver distribution metadata exceeds its bound".into());
        }
        let object = self.distribution.objects.store(&bytes)?;
        let reference = inventory.reference(object.clone());
        let mut objects = inventory.objects();
        objects.push(object);
        Ok(Candidate {
            inventory,
            reference,
            objects,
        })
    }
    async fn recheck(&mut self, candidate: &mut Candidate) -> Result<(), Error> {
        self.distribution
            .rpc
            .check_network(candidate.inventory.genesis)
            .await?;
        for (height, expected) in candidate.inventory.anchors() {
            if self.distribution.rpc.block_hash(u64::from(height)).await? != expected {
                self.publications.revoke();
                return Err("Receiver export anchor changed".into());
            }
        }
        Ok(())
    }
    async fn commit(self, candidate: Candidate) -> Result<(), Error> {
        self.publications
            .in_epoch(self.epoch, || -> Result<(), Error> {
                let mut authority = self.distribution.authority.lock().unwrap();
                let now = now_ms();
                self.distribution.objects.retain_replacement(
                    candidate.objects,
                    now.saturating_add(self.distribution.retention_ms),
                    now,
                )?;
                match authority.apply(AuthorityTransition::Publish(candidate.reference.clone())) {
                    Err(pir_publication::PublicationAuthorityError::RecoveryRequired) => {
                        // A previous write may have reached disk. Recover with
                        // a durable negative fence before granting authority.
                        authority.apply(AuthorityTransition::Recover)?;
                        authority.apply(AuthorityTransition::Publish(candidate.reference))?;
                    }
                    result => {
                        result?;
                    }
                }
                self.distribution
                    .exported
                    .lock()
                    .unwrap()
                    .retain(|_, exported| {
                        self.distribution
                            .objects
                            .open_retained(&exported.reference.sha256)
                            .is_ok()
                    });
                Ok(())
            })
            .ok_or("Receiver export invalidated during commit")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use zakura_pir_query_server::receiver::blocks::{BlockFilter, BlockId};

    #[test]
    fn directory_objects_are_bound_to_the_prepared_session() {
        use std::io::Write;
        use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
        use zakura_pir_query_server::receiver::{session::MIN_ROWS, snapshot::Snapshot};
        let root = tempfile::tempdir().unwrap();
        let genesis = Network::Mainnet.genesis_hash();
        let mut manifest = common::manifest(MIN_ROWS);
        manifest.genesis = genesis;
        manifest.start_height = crate::blocks::ironwood_activation();
        manifest.end_height = manifest.start_height + 1;
        let snapshot = Snapshot::build(manifest, &[]).unwrap();
        let witnesses = crate::prepared::placeholder(&snapshot);
        let mut durable = crate::durable::Durable::open(
            root.path(),
            Arc::new(receiver_pir_server::tables::Tables::new(4 << 30)),
        )
        .unwrap();
        let segment = durable.segment(snapshot, witnesses).unwrap();
        let rows = root.path().join(crate::durable::TABLES_DIR).join(format!(
            "p-{}.rows",
            hex::encode(
                segment
                    .payments
                    .payment_manifest()
                    .unwrap()
                    .directory
                    .revision()
                    .unwrap()
            )
        ));
        let publication = Arc::new(
            receiver_pir_server::Publication::assemble(
                genesis,
                0,
                vec![(false, vec![segment], None)],
            )
            .unwrap(),
        );
        let distribution = Distribution::open(
            root.path().into(),
            NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
            600,
        )
        .unwrap();
        let inventory = distribution
            .material(Some(publication.clone()), None, 0, 0)
            .unwrap();
        inventory
            .validate(crate::blocks::ironwood_activation())
            .unwrap();
        let mut invalid_setup = inventory.clone();
        invalid_setup.directory.as_mut().unwrap().segments[0]
            .payment_public
            .sha256 = "00".repeat(32);
        assert!(invalid_setup
            .validate(crate::blocks::ironwood_activation())
            .is_err());
        // Damage an existing source in place: a new whole-file object digest
        // must not be accepted under the old session's rows commitment.
        std::fs::OpenOptions::new()
            .write(true)
            .open(rows)
            .unwrap()
            .write_all(&[1])
            .unwrap();
        let damaged = distribution
            .material(Some(publication), None, 0, 0)
            .unwrap();
        assert!(damaged
            .validate(crate::blocks::ironwood_activation())
            .is_err());
    }

    /// While a restarted process has only block filters, its exports leave the
    /// restored publication and its tables in place.
    #[tokio::test]
    async fn an_export_without_tables_keeps_restored_tables() {
        use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
        use zakura_pir_query_server::receiver::{session::MIN_ROWS, snapshot::Snapshot};
        let root = tempfile::tempdir().unwrap();
        let genesis = Network::Mainnet.genesis_hash();
        let activation = crate::blocks::ironwood_activation();
        let mut manifest = common::manifest(MIN_ROWS);
        manifest.genesis = genesis;
        manifest.start_height = activation;
        manifest.end_height = activation + 1;
        let snapshot = Snapshot::build(manifest, &[]).unwrap();
        let witnesses = crate::prepared::placeholder(&snapshot);
        let mut durable = crate::durable::Durable::open(
            root.path(),
            Arc::new(receiver_pir_server::tables::Tables::new(4 << 30)),
        )
        .unwrap();
        let segment = durable.segment(snapshot, witnesses).unwrap();
        let publication = Arc::new(
            receiver_pir_server::Publication::assemble(
                genesis,
                0,
                vec![(false, vec![segment], None)],
            )
            .unwrap(),
        );
        // No node answers: keeping the restored tables needs no chain check.
        let distribution = Distribution::open(
            root.path().into(),
            NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
            600,
        )
        .unwrap();
        let inventory = distribution
            .material(Some(publication), None, 0, 0)
            .unwrap();
        let object = distribution
            .objects
            .store(&serde_json::to_vec(&inventory).unwrap())
            .unwrap();
        distribution
            .authority
            .lock()
            .unwrap()
            .apply(AuthorityTransition::Publish(inventory.reference(object)))
            .unwrap();
        let restored = distribution.authority.lock().unwrap().current().clone();
        let publications = Publications::default();
        let filter = BlockFilter::build(
            &genesis,
            BlockId {
                height: activation,
                hash: [3; 32],
            },
            [0; 32],
            &[],
        )
        .unwrap();
        assert!(publications.publish_filters(
            receiver_pir_server::blocks::BlockWindow::new(genesis, 0, vec![filter]).unwrap(),
            0
        ));
        distribution
            .export(&publications, &pir_pipeline::engine::PublisherEngine)
            .await
            .unwrap();
        assert_eq!(*distribution.authority.lock().unwrap().current(), restored);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn https_hints_reuse_objects_and_durable_withdrawals_fence_restarts() {
        let genesis = Network::Mainnet.genesis_hash();
        let activation = crate::blocks::ironwood_activation();
        let fork = Arc::new(AtomicBool::new(false));
        let node = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = NodeClient::new(format!("http://{}", node.local_addr().unwrap()), None).unwrap();
        let changed = fork.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let task = tokio::spawn(async move {
            axum::serve(node, Router::new().route("/", post(move |Json(request): Json<serde_json::Value>| {
            let changed = changed.clone(); let counted = counted.clone(); async move {
                counted.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request["method"], "getblockhash");
                let hash = if request["params"][0] == 0 { genesis } else if changed.load(Ordering::SeqCst) { [4;32] } else { [3;32] };
                Json(serde_json::json!({"result":zakura_server_chain::hash::display(&hash),"error":null}))
            }
        }))).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let distribution = Distribution::open(root.path().into(), rpc.clone(), 600).unwrap();
        let publications = Publications::default();
        distribution.attach(&publications);
        let filter = BlockFilter::build(
            &genesis,
            BlockId {
                height: activation,
                hash: [3; 32],
            },
            [0; 32],
            &[],
        )
        .unwrap();
        assert!(publications.publish_filters(
            receiver_pir_server::blocks::BlockWindow::new(genesis, 0, vec![filter]).unwrap(),
            0
        ));
        let authority_path = root.path().join("distribution/authority.json");
        let before = distribution.authority.lock().unwrap().current().clone();
        std::fs::remove_file(&authority_path).unwrap();
        std::fs::create_dir(&authority_path).unwrap();
        assert!(distribution
            .export(&publications, &pir_pipeline::engine::PublisherEngine)
            .await
            .is_err());
        assert!(!distribution.authority.lock().unwrap().current().eligible);
        std::fs::remove_dir(&authority_path).unwrap();
        distribution
            .export(&publications, &pir_pipeline::engine::PublisherEngine)
            .await
            .unwrap();
        let recovered = distribution.authority.lock().unwrap().current().clone();
        assert!(recovered.eligible);
        assert!(recovered.fence.recovery_epoch > before.fence.recovery_epoch);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", socket.local_addr().unwrap());
        let router = Router::new().nest(PREFIX.trim_end_matches('/'), distribution.router());
        let server = tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        });
        let tls = tls::Tls::start(&origin).await;
        let client = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&std::fs::read(&tls.certificate).unwrap()).unwrap(),
            )
            .build()
            .unwrap();
        let url = format!("{}{PREFIX}current", tls.origin);
        let current: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
        assert!(current.eligible);
        assert!(current
            .publication
            .as_ref()
            .unwrap()
            .descriptor
            .directory
            .is_none());
        let reference = current.inventory().unwrap();
        let bytes = client
            .get(format!(
                "{}{PREFIX}objects/{}",
                tls.origin, reference.sha256
            ))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        reference.verify(&bytes).unwrap();
        let inventory: Inventory = serde_json::from_slice(&bytes).unwrap();
        inventory.validate(activation).unwrap();
        assert_eq!(
            inventory.filters.as_ref().unwrap().head.tip.height,
            activation
        );
        // Unchanged observations do not rebuild objects or advance authority.
        distribution
            .export(&publications, &pir_pipeline::engine::PublisherEngine)
            .await
            .unwrap();
        let unchanged: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
        assert_eq!(unchanged, current);
        let request = AuthorityCheckRequest {
            authority: current.authority().unwrap(),
            subjects: vec![inventory.anchors()],
        };
        let response: AuthorityCheckResponse<Subject> = client
            .post(format!("{}{PREFIX}authority/check", tls.origin))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(request.verdicts(response).unwrap(), [true]);
        let duplicate = AuthorityCheckRequest {
            authority: current.authority().unwrap(),
            subjects: vec![vec![(activation, [3; 32]); 4]; 3],
        };
        calls.store(0, Ordering::SeqCst);
        let response: AuthorityCheckResponse<Subject> = client
            .post(format!("{}{PREFIX}authority/check", tls.origin))
            .json(&duplicate)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(duplicate.verdicts(response).unwrap(), [true, true, true]);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one network check and one shared anchor lookup"
        );
        let too_many = AuthorityCheckRequest {
            authority: current.authority().unwrap(),
            subjects: vec![vec![(activation, [3; 32]); MAX_CHECK_ANCHORS / 2 + 1]; 2],
        };
        calls.store(0, Ordering::SeqCst);
        let response = client
            .post(format!("{}{PREFIX}authority/check", tls.origin))
            .json(&too_many)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "reject total nested work before node RPC"
        );
        let disproven = AuthorityCheckRequest {
            authority: current.authority().unwrap(),
            subjects: vec![vec![(activation, [9; 32]), (activation + 1, [3; 32])]],
        };
        calls.store(0, Ordering::SeqCst);
        let response: AuthorityCheckResponse<Subject> = client
            .post(format!("{}{PREFIX}authority/check", tls.origin))
            .json(&disproven)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(disproven.verdicts(response).unwrap(), [false]);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "stop when a subject is disproven"
        );
        fork.store(true, Ordering::SeqCst);
        let response: AuthorityCheckResponse<Subject> = client
            .post(format!("{}{PREFIX}authority/check", tls.origin))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(request.verdicts(response).unwrap(), [false]);
        publications.revoke();
        let withdrawn: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
        assert!(!withdrawn.eligible);
        assert!(withdrawn.fence.latest_withdrawal > current.sequence);
        let restarted = Distribution::open(root.path().into(), rpc, 600).unwrap();
        let reopened = restarted.authority.lock().unwrap().current().clone();
        assert!(!reopened.eligible);
        assert!(reopened.sequence > withdrawn.sequence);
        assert!(reopened.fence.recovery_epoch > withdrawn.fence.recovery_epoch);
        server.abort();
        task.abort();
    }

    #[tokio::test]
    async fn a_restarted_publisher_republishes_the_same_tables_under_a_new_publication() {
        let genesis = Network::Mainnet.genesis_hash();
        let activation = crate::blocks::ironwood_activation();
        let node = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc = NodeClient::new(format!("http://{}", node.local_addr().unwrap()), None).unwrap();
        let task = tokio::spawn(async move {
            axum::serve(node, Router::new().route("/", post(move |Json(request): Json<serde_json::Value>| async move {
                let hash = if request["params"][0] == 0 { genesis } else { [3; 32] };
                Json(serde_json::json!({"result":zakura_server_chain::hash::display(&hash),"error":null}))
            }))).await.unwrap();
        });
        let filter = BlockFilter::build(
            &genesis,
            BlockId {
                height: activation,
                hash: [3; 32],
            },
            [0; 32],
            &[],
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let export = |distribution: Distribution| {
            let filter = filter.clone();
            async move {
                let publications = Publications::default();
                distribution.attach(&publications);
                assert!(publications.publish_filters(
                    receiver_pir_server::blocks::BlockWindow::new(genesis, 0, vec![filter])
                        .unwrap(),
                    0
                ));
                distribution
                    .export(&publications, &pir_pipeline::engine::PublisherEngine)
                    .await
                    .unwrap();
                let current = distribution.authority.lock().unwrap().current().clone();
                assert!(current.eligible);
                current.publication.unwrap()
            }
        };
        let first = export(Distribution::open(root.path().into(), rpc.clone(), 600).unwrap()).await;
        // A restart serves its last tables again under its own recovery epoch,
        // but only for the history revision they were built from, and once.
        let restarted = Distribution::open(root.path().into(), rpc.clone(), 600).unwrap();
        let publications = Publications::default();
        restarted.attach(&publications);
        let revision = first.descriptor.history_revision;
        assert!(!restarted
            .restore(&publications, revision + 1)
            .await
            .unwrap());
        assert!(restarted.restore(&publications, revision).await.unwrap());
        let current = restarted.authority.lock().unwrap().current().clone();
        assert!(current.eligible);
        let restored = current.publication.unwrap();
        assert!(restored.descriptor.recovery_epoch > first.descriptor.recovery_epoch);
        assert_eq!(restored.descriptor.filters, first.descriptor.filters);
        assert!(!restarted.restore(&publications, revision).await.unwrap());
        // The restart offers no authority until it exports again; a query server
        // that saw that withdrawal never serves the earlier publication again.
        let second = export(Distribution::open(root.path().into(), rpc, 600).unwrap()).await;
        assert_ne!(second.publication, first.publication);
        assert!(second.descriptor.recovery_epoch > first.descriptor.recovery_epoch);
        assert_eq!(second.descriptor.filters, first.descriptor.filters);
        task.abort();
    }
}
