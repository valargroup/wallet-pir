//! The recovery objects a publication's catalog names, fetched through
//! wallet-libraries' receiver client from the production router over real HTTP.
#[path = "../../../shared/pir-publisher/receiver-store/tests/common/mod.rs"]
mod common;
use common::{catalog, manifest, receiver, Catalog};
use receiver_directory::{
    paid::{PaidSet, Span},
    snapshot::{Manifest, Snapshot},
    Hash,
};
use receiver_pir::{
    server::Server,
    transport::{DirectoryClient, Transport},
    Error, MIN_ROWS,
};
use receiver_pir_server::{Object, Publication, Publications, Recovery};

/// The cache policy of every digest-addressed object.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// A wallet's plain transport over reqwest.
struct Http(reqwest::Client);

impl Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> Result<Vec<u8>, Error> {
        let response = self
            .0
            .get(url)
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(Error::Transport(response.status().to_string()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if body.len() > limit {
            return Err(Error::Malformed);
        }
        Ok(body.to_vec())
    }

    async fn post(&self, _: &str, _: Vec<u8>, _: usize) -> Result<Vec<u8>, Error> {
        unreachable!("objects are only fetched")
    }
}

/// `c`'s objects, checked.
fn recovery(c: &Catalog) -> Recovery {
    Recovery::new(&c.head, &c.index, &c.buckets, &c.paid, &c.seen).unwrap()
}

/// A publication ending at `end_hash` that commits to `c`'s catalog, with its
/// objects.
fn publication(c: &Catalog, end_hash: Hash) -> Publication {
    let mut manifest: Manifest = manifest(MIN_ROWS);
    manifest.end_hash = end_hash;
    manifest.catalog_sha256 = c.head.digest().unwrap();
    let server = Server::new(Snapshot::build(manifest, &[], &[]).unwrap()).unwrap();
    Publication::verified(server, None)
        .with_recovery(recovery(c))
        .unwrap()
}

/// Serves `publications` through the production router; returns its origin.
async fn serve(publications: Publications) -> String {
    let app = receiver_pir_server::router_with_publications(publications);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
    origin
}

/// `GET <origin>/v1/receiver/<path>`'s status and `Cache-Control`.
async fn get(origin: &str, path: &str) -> (u16, String) {
    let response = reqwest::get(format!("{origin}/v1/receiver/{path}"))
        .await
        .unwrap();
    let cache = response.headers()["cache-control"].to_str().unwrap().into();
    (response.status().as_u16(), cache)
}

/// Every object the served manifest's catalog names is there whenever the manifest
/// is, served immutably; unknown or noncanonical routes are uncached 404s.
#[tokio::test]
async fn a_served_manifest_names_served_immutable_objects() {
    let c = catalog([1; 32]);
    let publications = Publications::default();
    assert!(publications.publish(publication(&c, [3; 32]), 0));
    let origin = serve(publications).await;
    let http = Http(reqwest::Client::new());
    let manifest = DirectoryClient::fetch_manifest(&origin, &http)
        .await
        .unwrap();
    let head = DirectoryClient::fetch_catalog(&origin, &http, &manifest)
        .await
        .unwrap();
    assert_eq!(head, c.head);
    let index = DirectoryClient::fetch_bucket_index(&origin, &http, &head)
        .await
        .unwrap();
    let bucket = DirectoryClient::fetch_bucket(&origin, &http, &head, &index, 0)
        .await
        .unwrap();
    assert_eq!(bucket, c.buckets[0]);
    let paid = DirectoryClient::fetch_paid(&origin, &http, &head)
        .await
        .unwrap();
    assert_eq!(paid.matches(&[receiver()]), [true]);
    let seen = DirectoryClient::fetch_seen(&origin, &http, &head)
        .await
        .unwrap();
    assert_eq!(seen, c.seen);

    for (object, _) in recovery(&c).objects() {
        assert_eq!(head_of(&origin, object).await, (200, IMMUTABLE.into()));
    }
    let digest = hex::encode(c.head.index);
    for path in [
        // Another kind's route, an unlisted start, uppercase or short digests.
        format!("catalog/{digest}"),
        format!("bucket/48/{}", hex::encode(c.buckets[0].digest())),
        format!("bucket/00/{}", hex::encode(c.buckets[0].digest())),
        format!("bucket-index/{}", digest.to_uppercase()),
        format!("bucket-index/{}", &digest[2..]),
        format!("unknown/{digest}"),
    ] {
        assert_eq!(
            get(&origin, &path).await,
            (404, "no-store".into()),
            "{path}"
        );
    }
    let (_, init) = get(&origin, "init").await;
    assert_eq!(init, "no-store");
}

/// `object`'s status and `Cache-Control` at its route.
async fn head_of(origin: &str, object: &Object) -> (u16, String) {
    get(origin, &object.path()).await
}

/// A displaced publication's objects stay served through its grace, alongside its
/// successor's; a revocation stops serving both.
#[tokio::test]
async fn objects_follow_their_publication() {
    let old = catalog([1; 32]);
    let mut new = catalog([1; 32]);
    let span = Span {
        start: 48,
        start_hash: [48; 32],
        end: 96,
        end_hash: [96; 32],
    };
    new.paid = PaidSet::build([1; 32], 0, span, &[[7; 43]]).unwrap();
    new.head.paid = new.paid.digest();
    let publications = Publications::default();
    assert!(publications.publish(publication(&old, [3; 32]), 0));
    let origin = serve(publications.clone()).await;
    assert!(publications.publish(publication(&new, [4; 32]), 0));
    let old_paid = Object::Paid(old.head.paid);
    let new_paid = Object::Paid(new.head.paid);
    assert_eq!(head_of(&origin, &old_paid).await.0, 200);
    assert_eq!(head_of(&origin, &new_paid).await.0, 200);
    publications.revoke();
    assert_eq!(head_of(&origin, &old_paid).await.0, 404);
    assert_eq!(head_of(&origin, &new_paid).await.0, 404);
}

/// A publication refuses a catalog its manifest does not commit to, and a catalog
/// whose objects do not match it is refused before serving.
#[test]
fn a_publication_carries_only_its_own_catalog() {
    let c = catalog([1; 32]);
    let server = Server::new(Snapshot::build(manifest(MIN_ROWS), &[], &[]).unwrap()).unwrap();
    assert!(Publication::verified(server, None)
        .with_recovery(recovery(&c))
        .is_err());
    let other = catalog([2; 32]);
    assert!(Recovery::new(&c.head, &c.index, &c.buckets, &c.paid, &other.seen).is_err());
    assert!(Recovery::new(&c.head, &c.index, &[], &c.paid, &c.seen).is_err());
}
