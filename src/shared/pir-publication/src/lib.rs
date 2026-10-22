//! The internal publisher-query publication contract shared by every PIR product.
//!
//! A publisher distributes immutable content-addressed objects and one mutable
//! [`PublicationEnvelope`] per product namespace, advanced only through its
//! durable [`PublicationAuthority`]. A query server accepts an envelope only as
//! a successor of its durable [`AuthorityFloor`], fetches the objects the
//! envelope references into its [`ObjectCache`], and serves only after a live
//! authority check bound to the envelope's exact publication and fence.
//! Nothing here performs I/O over a network, and holding bytes, a cached file
//! or a saved floor never confers permission to serve.
//!
//! Products supply only a typed envelope descriptor and material types that
//! bind object references.
mod authority_check;
mod authority_floor;
mod object_cache;
mod object_file;
mod object_installation;
mod object_reference;
mod publication_authority;
mod publication_envelope;
mod published_objects;
mod source_observation;

use serde::{Deserialize, Serialize};

pub use authority_check::{
    AuthorityCheckRefusal, AuthorityCheckRequest, AuthorityCheckResponse,
    AuthorityResponseMismatch, SubjectAnswer, AUTHORITY_CHECK_PATH, MAX_AUTHORITY_SUBJECTS,
};
pub use authority_floor::{
    AuthorityFloor, AuthorityFloorError, CheckedSuccession, PendingFloorWrite, SourceBinding,
    StaleSuccessionCheck, WrittenFloor,
};
pub use object_cache::{ObjectCache, PartialObject};
pub use object_file::{hash_object_file, FileIdentity, ObjectFileError};
pub use object_installation::{InstalledObject, ObjectInstaller};
pub use object_reference::{
    is_object_digest, object_digest, ObjectReference, ObjectReferenceError, OBJECTS_PATH,
};
pub use publication_authority::{
    AuthorityTransition, PublicationAuthority, PublicationAuthorityError,
};
pub use publication_envelope::{
    conditional_token, AuthorityBinding, AuthorityFence, EnvelopeSuccession, MalformedEnvelope,
    PublicationEnvelope, PublicationIdentity, PublicationIdentityMismatch, PublicationReference,
    PublicationReplay, PublicationScope, CURRENT_PATH,
};
pub use published_objects::PublishedObjectStore;
pub use source_observation::SourceObservation;

/// Identity of reusable material bytes, independent of the publication that
/// names them: a metadata-only refresh keeps it.
pub type MaterialIdentity = [u8; 32];

/// Identity of one prepared serving view, chosen by the product (for Status,
/// the manifest identity). Floors invalidate views and query servers retain
/// objects by this identity; a metadata-only refresh of the same material is
/// a new view.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ViewIdentity(pub [u8; 32]);
