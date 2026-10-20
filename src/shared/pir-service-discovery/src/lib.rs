//! Service discovery between Wallet PIR publishers and query servers.
//!
//! A publisher serves a small JSON document at [`PATH`] naming its service and
//! the contract version it implements. A query server fetches that document
//! before it starts and before every synchronization pass, and compares the
//! publisher's version with its own [`VERSION`] using [`check_publisher`].
//!
//! The version is this crate's package version: one number covers the
//! discovery document and the whole Publisher-Query Server HTTPS contract. It is
//! not a host software release or a product version. Compatibility follows
//! SemVer with one rule: the same major version is compatible in both
//! directions, whatever the minor and patch versions. A different major version
//! is incompatible. The document and endpoint stay readable across majors so a
//! mismatch can always be named.
//!
//! The document is informational compatibility metadata. It is not a health
//! check, and it never authorizes a publication; products keep their own chain,
//! schema, canonicality and revocation checks.
use serde::{Deserialize, Serialize};
use std::fmt;

pub use semver::Version;

/// Exact HTTPS path of the discovery document on a publisher origin.
pub const PATH: &str = "/.well-known/pir-service-discovery";
/// Service identity a Wallet PIR publisher advertises.
pub const PUBLISHER_SERVICE: &str = "pir-publisher";
/// Contract version implemented by this build, from the package version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Largest discovery document a consumer reads.
pub const MAX_DOCUMENT_BYTES: usize = 16 << 10;

/// The discovery document. Unknown fields are ignored so a later minor
/// version can add optional fields without breaking older readers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Document {
    pub service: String,
    pub version: String,
}

impl Document {
    /// The document a publisher built with this crate serves.
    pub fn publisher() -> Self {
        Self {
            service: PUBLISHER_SERVICE.into(),
            version: VERSION.into(),
        }
    }

    /// JSON body for the discovery endpoint.
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("discovery document serializes")
    }

    /// Parse a discovery body of at most [`MAX_DOCUMENT_BYTES`].
    pub fn parse(body: &[u8]) -> Result<Self, Error> {
        if body.len() > MAX_DOCUMENT_BYTES {
            return Err(Error::Malformed(format!(
                "document exceeds {MAX_DOCUMENT_BYTES} bytes"
            )));
        }
        serde_json::from_slice(body).map_err(|error| Error::Malformed(error.to_string()))
    }
}

/// This build's contract version.
pub fn version() -> Version {
    parse_version(VERSION).expect("package version is a stable contract version")
}

/// Parse a contract version. Only stable versions with a major version of at
/// least 1 are accepted; build metadata is kept but never affects comparison.
pub fn parse_version(text: &str) -> Result<Version, Error> {
    let invalid = |reason: &str| Error::InvalidVersion {
        version: text.into(),
        reason: reason.into(),
    };
    let version = Version::parse(text).map_err(|error| invalid(&error.to_string()))?;
    if version.major == 0 {
        return Err(invalid("0.x versions are not stable contract versions"));
    }
    if !version.pre.is_empty() {
        return Err(invalid("prerelease versions are not contract versions"));
    }
    Ok(version)
}

/// Whether a query server at `query_server` may use a publisher at `publisher`.
pub fn compatible(query_server: &Version, publisher: &Version) -> Result<(), Incompatible> {
    if query_server.major == publisher.major {
        Ok(())
    } else {
        Err(Incompatible {
            query_server: query_server.clone(),
            publisher: publisher.clone(),
        })
    }
}

/// Validate a publisher's discovery body against this build. Returns the
/// publisher's contract version when it is compatible.
pub fn check_publisher(body: &[u8]) -> Result<Version, Error> {
    let document = Document::parse(body)?;
    if document.service != PUBLISHER_SERVICE {
        return Err(Error::UnexpectedService {
            expected: PUBLISHER_SERVICE.into(),
            found: document.service,
        });
    }
    let publisher = parse_version(&document.version)?;
    compatible(&version(), &publisher)?;
    Ok(publisher)
}

/// Why a discovery document was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The body is not a discovery document.
    Malformed(String),
    /// The document names another service.
    UnexpectedService { expected: String, found: String },
    /// The advertised version is not a stable contract version.
    InvalidVersion { version: String, reason: String },
    /// Both sides are valid but implement different major versions.
    Incompatible(Incompatible),
}

impl Error {
    /// The confirmed version mismatch, if this is one. Only a mismatch is
    /// permanent; other errors may clear when the publisher is repaired.
    pub fn incompatible(&self) -> Option<&Incompatible> {
        match self {
            Self::Incompatible(incompatible) => Some(incompatible),
            _ => None,
        }
    }
}

impl From<Incompatible> for Error {
    fn from(value: Incompatible) -> Self {
        Self::Incompatible(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(reason) => write!(f, "malformed discovery document: {reason}"),
            Self::UnexpectedService { expected, found } => {
                write!(
                    f,
                    "discovery names service {found:?}, expected {expected:?}"
                )
            }
            Self::InvalidVersion { version, reason } => {
                write!(f, "invalid publisher version {version:?}: {reason}")
            }
            Self::Incompatible(incompatible) => incompatible.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

/// A confirmed major-version mismatch between a query server and a publisher.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incompatible {
    pub query_server: Version,
    pub publisher: Version,
}

impl Incompatible {
    /// Whether the query server implements an older major than the publisher.
    pub fn query_server_is_older(&self) -> bool {
        self.query_server.major < self.publisher.major
    }
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (query, publisher) = (&self.query_server, &self.publisher);
        write!(
            f,
            "publisher contract version {publisher} is incompatible with query server \
             contract version {query} (this query server supports major version {})",
            query.major
        )?;
        if self.query_server_is_older() {
            write!(
                f,
                "; upgrade the query server to a release supporting major version {}",
                publisher.major
            )
        } else {
            write!(
                f,
                "; the publisher is older and incompatible: upgrade the publisher to major \
                 version {}, or run a query server release supporting major version {}",
                query.major, publisher.major
            )
        }
    }
}

impl std::error::Error for Incompatible {}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(text: &str) -> Version {
        Version::parse(text).unwrap()
    }

    fn body(service: &str, version: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"service": service, "version": version})).unwrap()
    }

    #[test]
    fn document_is_generated_from_the_package_version() {
        assert_eq!(VERSION, "1.0.0");
        assert_eq!(
            String::from_utf8(Document::publisher().to_json()).unwrap(),
            r#"{"service":"pir-publisher","version":"1.0.0"}"#
        );
        assert_eq!(
            check_publisher(&Document::publisher().to_json()),
            Ok(v(VERSION))
        );
    }

    #[test]
    fn same_major_is_compatible_in_both_directions() {
        for (query, publisher) in [
            ("1.0.0", "1.0.0"),
            ("1.0.0", "1.0.9"),
            ("1.0.9", "1.0.0"),
            ("1.0.0", "1.7.0"),
            ("1.7.0", "1.0.0"),
            ("1.3.2", "1.9.1"),
            ("2.0.0", "2.5.3"),
        ] {
            assert_eq!(
                compatible(&v(query), &v(publisher)),
                Ok(()),
                "{query} vs {publisher}"
            );
        }
    }

    #[test]
    fn major_mismatch_is_incompatible_with_direction_specific_advice() {
        let older = compatible(&v("1.4.0"), &v("2.0.0")).unwrap_err();
        assert!(older.query_server_is_older());
        let text = older.to_string();
        assert!(text.contains("publisher contract version 2.0.0"), "{text}");
        assert!(
            text.contains("query server contract version 1.4.0"),
            "{text}"
        );
        assert!(text.contains("supports major version 1"), "{text}");
        assert!(text.contains("upgrade the query server"), "{text}");

        let newer = compatible(&v("2.1.0"), &v("1.9.0")).unwrap_err();
        assert!(!newer.query_server_is_older());
        let text = newer.to_string();
        assert!(
            text.contains("publisher is older and incompatible"),
            "{text}"
        );
        assert!(
            text.contains("upgrade the publisher to major version 2"),
            "{text}"
        );
        assert!(text.contains("supporting major version 1"), "{text}");
    }

    #[test]
    fn publisher_major_mismatch_is_a_typed_error() {
        let error = check_publisher(&body(PUBLISHER_SERVICE, "2.0.0")).unwrap_err();
        let incompatible = error.incompatible().expect("typed mismatch");
        assert_eq!(incompatible.publisher, v("2.0.0"));
        assert_eq!(incompatible.query_server, version());
    }

    #[test]
    fn build_metadata_is_ignored() {
        assert_eq!(
            check_publisher(&body(PUBLISHER_SERVICE, "1.2.3+build.7")).unwrap(),
            v("1.2.3+build.7")
        );
        assert_eq!(compatible(&v("1.0.0+a"), &v("1.0.0+b")), Ok(()));
    }

    #[test]
    fn malformed_unstable_and_prerelease_versions_are_refused() {
        for version in ["", "1", "1.0", "v1.0.0", "1.0.0.0", "01.0.0", " 1.0.0", "x"] {
            assert!(
                matches!(parse_version(version), Err(Error::InvalidVersion { .. })),
                "{version:?}"
            );
        }
        for version in ["0.1.0", "0.9.9", "0.0.1"] {
            let error = check_publisher(&body(PUBLISHER_SERVICE, version)).unwrap_err();
            assert!(error.to_string().contains("0.x"), "{error}");
            assert!(error.incompatible().is_none());
        }
        for version in ["1.0.0-rc.1", "2.0.0-alpha", "1.1.0-0+build"] {
            let error = check_publisher(&body(PUBLISHER_SERVICE, version)).unwrap_err();
            assert!(error.to_string().contains("prerelease"), "{error}");
            assert!(error.incompatible().is_none());
        }
    }

    #[test]
    fn malformed_documents_and_other_services_are_refused() {
        for raw in [
            &b""[..],
            b"{",
            b"[]",
            b"{\"service\":\"pir-publisher\"}",
            b"{\"version\":\"1.0.0\"}",
            b"{\"service\":1,\"version\":\"1.0.0\"}",
            b"{\"service\":\"pir-publisher\",\"version\":100}",
        ] {
            assert!(
                matches!(check_publisher(raw), Err(Error::Malformed(_))),
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
        assert_eq!(
            check_publisher(&body("pir-query-server", "1.0.0")),
            Err(Error::UnexpectedService {
                expected: PUBLISHER_SERVICE.into(),
                found: "pir-query-server".into()
            })
        );
        let oversized = vec![b' '; MAX_DOCUMENT_BYTES + 1];
        assert!(matches!(
            check_publisher(&oversized),
            Err(Error::Malformed(_))
        ));
    }

    #[test]
    fn unknown_additive_fields_are_ignored() {
        let raw =
            br#"{"service":"pir-publisher","version":"1.4.0","features":["x"],"extra":{"a":1}}"#;
        assert_eq!(check_publisher(raw).unwrap(), v("1.4.0"));
    }
}
