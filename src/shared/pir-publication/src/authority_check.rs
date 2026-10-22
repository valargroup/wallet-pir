//! The live authority check a query server asks before committing or retaining
//! a view: "under this exact pointer, are these subjects still authoritative?"
use serde::{Deserialize, Serialize};
use std::fmt;

/// Distribution path, relative to the product prefix, of authority checks.
pub const AUTHORITY_CHECK_PATH: &str = "authority/check";

/// Upper bound on subjects in one request, so a publisher's work is bounded.
pub const MAX_AUTHORITY_SUBJECTS: usize = 256;

/// Product-encoded subjects (for Status, anchor heights and hashes) checked
/// against the publisher's chain view while `pointer` is still current.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheckRequest<P, S> {
    pub pointer: P,
    pub subjects: Vec<S>,
}

/// One answer per request subject, in order. The publisher answers only when
/// `pointer` was current and eligible both before and after the check.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheckResponse<P> {
    pub pointer: P,
    pub still_authoritative: Vec<bool>,
}

/// Why a publisher refused to answer an authority check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityCheckRefusal {
    /// Empty or above [`MAX_AUTHORITY_SUBJECTS`].
    MalformedRequest,
    /// The request names a pointer that is not the publisher's current eligible
    /// pointer, or the pointer changed during the check. The query server must
    /// re-observe the pointer before retaining anything.
    PointerAdvanced,
}
impl fmt::Display for AuthorityCheckRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedRequest => f.write_str("authority check request is malformed"),
            Self::PointerAdvanced => f.write_str("publication pointer advanced during check"),
        }
    }
}
impl std::error::Error for AuthorityCheckRefusal {}

impl<P: PartialEq + crate::PublicationPointer, S> AuthorityCheckRequest<P, S> {
    /// Publisher-side admission: the request is bounded and names `current`,
    /// which must be eligible. Call it again after the check, with the then
    /// current pointer, before answering.
    pub fn admit(&self, current: Option<&P>) -> Result<(), AuthorityCheckRefusal> {
        if self.subjects.is_empty() || self.subjects.len() > MAX_AUTHORITY_SUBJECTS {
            return Err(AuthorityCheckRefusal::MalformedRequest);
        }
        match current {
            Some(current) if *current == self.pointer && current.eligible() => Ok(()),
            _ => Err(AuthorityCheckRefusal::PointerAdvanced),
        }
    }
}
