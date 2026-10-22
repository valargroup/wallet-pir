//! The internal publisher-query publication contract shared by every PIR product.
//!
//! A publisher distributes immutable content-addressed objects and one mutable
//! publication pointer per product namespace. A query server accepts a pointer
//! only as a successor of its durable [`AuthorityFloor`], fetches the objects the
//! pointer references into its [`ObjectCache`], and serves only after a live
//! authority check. Nothing here performs I/O over a network, and holding bytes,
//! a cached file or a saved floor never confers permission to serve.
//!
//! Products supply typed adapters: their pointer type implements
//! [`PublicationPointer`] and their material types bind object references.
mod authority_check;
mod authority_floor;
mod object_cache;
mod object_reference;
mod publication_pointer;
mod published_objects;

pub use authority_check::{
    AuthorityCheckRefusal, AuthorityCheckRequest, AuthorityCheckResponse, AUTHORITY_CHECK_PATH,
    MAX_AUTHORITY_SUBJECTS,
};
pub use authority_floor::{AuthorityFloor, AuthorityFloorError, PendingFloorWrite, SourceBinding};
pub use object_cache::{ObjectCache, PartialObject};
pub use object_reference::{
    is_object_digest, object_digest, ObjectReference, ObjectReferenceError, OBJECTS_PATH,
};
pub use publication_pointer::{
    conditional_token, PointerSuccession, PublicationPointer, PublicationReplay, CURRENT_PATH,
};
pub use published_objects::PublishedObjectStore;

/// Identity of one prepared serving view, chosen by the product (for Status the
/// manifest identity). Floors invalidate views by this identity.
pub type MaterialIdentity = [u8; 32];
