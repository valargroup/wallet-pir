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

/// One answer per request subject, in request order, each naming the subject
/// it answers. The publisher answers only when `authority` was current both
/// before and after the check.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityCheckResponse<S> {
    pub authority: AuthorityBinding,
    pub answers: Vec<SubjectAnswer<S>>,
}

/// Whether one subject was still authoritative when the publisher checked
/// its source. A positive answer confirms the subject at that check only; it
/// grants no serving time, which stays the product's own freshness rule.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubjectAnswer<S> {
    pub subject: S,
    pub still_authoritative: bool,
}

/// An authority check response that does not answer exactly the request: it
/// binds another publication, fence or sequence, or answers other subjects,
/// a different number of them or them in another order. It confirms and
/// refutes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityResponseMismatch;
impl fmt::Display for AuthorityResponseMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("authority check response does not answer the request")
    }
}
impl std::error::Error for AuthorityResponseMismatch {}

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

    /// Query-server side: whether each requested subject, in request order,
    /// is still authoritative. Refuses, before anything acts on an answer, a
    /// response bound to another authority or to subjects other than exactly
    /// the requested ones in their order.
    pub fn verdicts(
        &self,
        response: AuthorityCheckResponse<S>,
    ) -> Result<Vec<bool>, AuthorityResponseMismatch>
    where
        S: PartialEq,
    {
        if response.authority != self.authority
            || response.answers.len() != self.subjects.len()
            || response
                .answers
                .iter()
                .zip(&self.subjects)
                .any(|(answer, subject)| answer.subject != *subject)
        {
            return Err(AuthorityResponseMismatch);
        }
        Ok(response
            .answers
            .into_iter()
            .map(|answer| answer.still_authoritative)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthorityFence;

    fn binding(sequence: u64, recovery_epoch: u64, publication: u8) -> AuthorityBinding {
        AuthorityBinding {
            sequence,
            fence: AuthorityFence {
                recovery_epoch,
                latest_withdrawal: 0,
            },
            publication: [publication; 32],
        }
    }

    fn request(subjects: &[&str]) -> AuthorityCheckRequest<String> {
        AuthorityCheckRequest {
            authority: binding(7, 1, 1),
            subjects: subjects.iter().map(|subject| subject.to_string()).collect(),
        }
    }

    fn response(
        authority: AuthorityBinding,
        answers: &[(&str, bool)],
    ) -> AuthorityCheckResponse<String> {
        AuthorityCheckResponse {
            authority,
            answers: answers
                .iter()
                .map(|(subject, still_authoritative)| SubjectAnswer {
                    subject: subject.to_string(),
                    still_authoritative: *still_authoritative,
                })
                .collect(),
        }
    }

    #[test]
    fn an_exact_answer_yields_one_verdict_per_requested_subject_in_order() {
        let request = request(&["a", "b"]);
        assert_eq!(
            request.verdicts(response(binding(7, 1, 1), &[("a", true), ("b", false)])),
            Ok(vec![true, false])
        );
    }

    #[test]
    fn answers_for_other_subjects_of_the_same_count_confirm_and_refute_nothing() {
        let request = request(&["a", "b"]);
        for answers in [
            [("b", false), ("a", true)],
            [("a", true), ("c", false)],
            [("c", true), ("d", true)],
        ] {
            assert_eq!(
                request.verdicts(response(binding(7, 1, 1), &answers)),
                Err(AuthorityResponseMismatch),
                "{answers:?}"
            );
        }
        assert_eq!(
            request.verdicts(response(binding(7, 1, 1), &[("a", true)])),
            Err(AuthorityResponseMismatch)
        );
    }

    #[test]
    fn answers_under_another_sequence_fence_or_publication_are_refused() {
        let request = request(&["a"]);
        for authority in [binding(8, 1, 1), binding(7, 2, 1), binding(7, 1, 2)] {
            assert_eq!(
                request.verdicts(response(authority, &[("a", true)])),
                Err(AuthorityResponseMismatch)
            );
        }
    }
}
