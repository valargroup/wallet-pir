//! Host identities and readiness declarations; product publication types stay
//! in their owning libraries.
use std::net::SocketAddr;

// A feature selection may use only some identities and readiness rules.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum PublisherProduct {
    // Preserve the lexical product order used by management probe requests.
    Enhance,
    Receiver,
    Status,
    TransactionDetails,
    TransparentFilters,
    TransparentHistory,
}

impl PublisherProduct {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Enhance => "enhance",
            Self::Status => "status",
            Self::TransparentHistory => "transparent_history",
            Self::TransparentFilters => "transparent_filters",
            Self::TransactionDetails => "transaction_details",
            Self::Receiver => "receiver",
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) enum ReadinessRule {
    /// A successful response suffices; its body is not inspected.
    HttpSuccess,
    /// History and filters report readiness with a serving phase.
    ServingPhase,
    /// Details must be running, published, and neither halted nor stalled.
    TransactionDetailsPublished,
}

#[derive(Clone)]
pub(crate) struct ReadinessProbe {
    pub(crate) address: SocketAddr,
    pub(crate) path: &'static str,
    pub(crate) rule: ReadinessRule,
}
