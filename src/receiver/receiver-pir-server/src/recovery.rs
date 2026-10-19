//! The recovery objects a publication's catalog head names, served by digest from
//! immutable routes that every wallet requests alike.
use axum::body::Bytes;
use receiver_directory::{
    catalog::{BucketIndex, CatalogHead},
    paid::PaidSet,
    seen::SeenSet,
    Error, Hash,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

/// One recovery object by kind and digest, with a bucket's start height.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Object {
    Catalog(Hash),
    BucketIndex(Hash),
    Bucket(u32, Hash),
    Paid(Hash),
    Seen(Hash),
}

impl Object {
    /// The object's route below `/v1/receiver/`, which is also its file below a staged
    /// publication directory's `recovery/`: `catalog/<digest>`, `bucket-index/<digest>`,
    /// `bucket/<start>/<digest>`, `paid/<digest>` or `seen/<digest>`, the digest in
    /// lowercase hex.
    pub fn path(&self) -> String {
        match self {
            Self::Catalog(d) => format!("catalog/{}", hex::encode(d)),
            Self::BucketIndex(d) => format!("bucket-index/{}", hex::encode(d)),
            Self::Bucket(start, d) => format!("bucket/{start}/{}", hex::encode(d)),
            Self::Paid(d) => format!("paid/{}", hex::encode(d)),
            Self::Seen(d) => format!("seen/{}", hex::encode(d)),
        }
    }
}

/// How long a recovery object stays served once no publication that accepts requests
/// names it, whether displaced or revoked, so a wallet that pinned its digest can
/// still fetch it. Each is measured from when its last such publication stopped
/// accepting requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retention {
    /// For buckets: the publisher's bucket retention window, so any bucket a wallet's
    /// index listed stays fetchable as long as the index could list it.
    pub buckets: Duration,
    /// For catalog heads, bucket indexes and cumulative paid and seen sets, which change
    /// far more often than buckets: long enough for a wallet's bootstrap to retry
    /// against the snapshot it pinned, after which it starts again from the current
    /// catalog.
    pub others: Duration,
}

impl Default for Retention {
    /// 30 days for buckets, 720 buckets at 75-second spacing, and 6 hours for the
    /// rest, which bounds memory to a few hours of seen sets.
    fn default() -> Self {
        Self {
            buckets: Duration::from_secs(30 * 24 * 60 * 60),
            others: Duration::from_secs(6 * 60 * 60),
        }
    }
}

impl Retention {
    /// The retention of `object`.
    pub(crate) fn of(&self, object: &Object) -> Duration {
        match object {
            Object::Bucket(..) => self.buckets,
            _ => self.others,
        }
    }
}

/// A catalog head and every object it names, checked against each other.
#[derive(Clone)]
pub struct Recovery {
    catalog: Hash,
    objects: Arc<BTreeMap<Object, Bytes>>,
}

impl Recovery {
    /// Checks `index`, every bucket it lists, `paid` and `seen` against `head` with
    /// the catalog's checks, and that `buckets` are exactly the listed ones, then
    /// encodes them all. Any mismatch is [`Error::Malformed`].
    pub fn new(
        head: &CatalogHead,
        index: &BucketIndex,
        buckets: &[PaidSet],
        paid: &PaidSet,
        seen: &SeenSet,
    ) -> Result<Self, Error> {
        head.check_index(index)?;
        head.check_paid(paid)?;
        head.check_seen(seen)?;
        if buckets.len() != index.iter().count() {
            return Err(Error::Malformed);
        }
        let catalog = head.digest()?;
        let mut objects = BTreeMap::from([
            (Object::Catalog(catalog), head.encode()?.into()),
            (Object::BucketIndex(head.index), index.encode().into()),
            (Object::Paid(head.paid), paid.encode().into()),
            (Object::Seen(head.seen), seen.encode()?.into()),
        ]);
        for bucket in buckets {
            head.check_bucket(index, bucket)?;
            let object = Object::Bucket(bucket.span().start, bucket.digest());
            if objects.insert(object, bucket.encode().into()).is_some() {
                return Err(Error::Malformed);
            }
        }
        Ok(Self {
            catalog,
            objects: Arc::new(objects),
        })
    }

    /// The catalog head's digest, which the publication's manifest must commit to.
    pub fn catalog(&self) -> Hash {
        self.catalog
    }

    /// Every object with its encoding.
    pub fn objects(&self) -> impl Iterator<Item = (&Object, &Bytes)> {
        self.objects.iter()
    }

    /// The encoding of `object`, if this catalog names it.
    pub(crate) fn get(&self, object: &Object) -> Option<Bytes> {
        self.objects.get(object).cloned()
    }
}
