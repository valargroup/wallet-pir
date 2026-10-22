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
mod object_reference;
mod publication_authority;
mod publication_envelope;
mod published_objects;

pub use authority_check::{
    AuthorityCheckRefusal, AuthorityCheckRequest, AuthorityCheckResponse, AUTHORITY_CHECK_PATH,
    MAX_AUTHORITY_SUBJECTS,
};
pub use authority_floor::{
    AuthorityFloor, AuthorityFloorError, CheckedSuccession, PendingFloorWrite, SourceBinding,
    StaleSuccessionCheck, WrittenFloor,
};
pub use object_cache::{ObjectCache, PartialObject};
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

/// Identity of material bytes or of one prepared serving view, chosen by the
/// product (for Status views, the manifest identity). Floors invalidate views
/// by this identity.
pub type MaterialIdentity = [u8; 32];
