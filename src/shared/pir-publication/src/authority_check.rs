//! The live authority check a query server asks before committing or retaining
//! a view: "under this exact publication and fence, are these subjects still
//! authoritative?"
use crate::{AuthorityBinding, PublicationEnvelope};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Distribution path, relative to the product prefix, of authority checks.
pub const AUTHORITY_CHECK_PATH: &str = "authority/check";

/// Upper bound on subjects in one request, so a publisher's work is bounded.
pub const MAX_AUTHORITY_SUBJECTS: usize = 256;

/// Product-encoded subjects (for Status, anchor heights and hashes) checked
/// against the publisher's chain view while `authority` is still current.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheckRequest<S> {
    pub authority: AuthorityBinding,
    pub subjects: Vec<S>,
}

/// One answer per request subject, in order. The publisher answers only when
/// `authority` was current both before and after the check.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheckResponse {
    pub authority: AuthorityBinding,
    pub still_authoritative: Vec<bool>,
}

/// Why a publisher refused to answer an authority check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityCheckRefusal {
    /// Empty or above [`MAX_AUTHORITY_SUBJECTS`].
    MalformedRequest,
    /// The request names an authority that is not the publisher's current
    /// eligible envelope, or the envelope changed during the check. The query
    /// server must re-observe the envelope before retaining anything.
    AuthorityAdvanced,
}
impl fmt::Display for AuthorityCheckRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedRequest => f.write_str("authority check request is malformed"),
            Self::AuthorityAdvanced => f.write_str("publication authority advanced during check"),
        }
    }
}
impl std::error::Error for AuthorityCheckRefusal {}

impl<S> AuthorityCheckRequest<S> {
    /// Publisher-side admission: the request is bounded and binds exactly the
    /// authority of `current`, which must be eligible. Call it again after the
    /// check, with the then current envelope, before answering.
    pub fn admit<D>(&self, current: &PublicationEnvelope<D>) -> Result<(), AuthorityCheckRefusal> {
        if self.subjects.is_empty() || self.subjects.len() > MAX_AUTHORITY_SUBJECTS {
            return Err(AuthorityCheckRefusal::MalformedRequest);
        }
        if current.authority() != Some(self.authority) {
            return Err(AuthorityCheckRefusal::AuthorityAdvanced);
        }
        Ok(())
    }
}
