//! One Enhance serving role per process: worker, query ingress, or packing router.
//! Publication and chain reading stay in the publisher; `pir-query-server`
//! binds the listeners and runs the role.
use crate::matvec::MatvecConfig;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

fn default_sealed_shards() -> usize {
    6
}
fn default_ingress_requests() -> usize {
    16
}
fn default_uploads() -> usize {
    crate::query_ingress::DEFAULT_UPLOADS
}
fn default_router_objects() -> usize {
    6
}
fn default_router_requests() -> usize {
    4
}

/// One Enhance role in a query-server process.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ServingRole {
    Worker {
        listen: SocketAddr,
        data_dir: PathBuf,
        #[serde(default = "default_sealed_shards")]
        sealed_shards: usize,
        #[serde(default)]
        matvec: MatvecConfig,
    },
    QueryIngress {
        data_dir: PathBuf,
        listen: SocketAddr,
        control_listen: SocketAddr,
        #[serde(default = "default_ingress_requests")]
        requests: usize,
        #[serde(default = "default_uploads")]
        uploads: usize,
    },
    PackingRouter {
        data_dir: PathBuf,
        artifact_origin: String,
        listen: SocketAddr,
        control_listen: SocketAddr,
        #[serde(default = "default_router_objects")]
        max_objects: usize,
        #[serde(default = "default_router_requests")]
        requests: usize,
    },
}

impl ServingRole {
    pub fn listens(&self) -> Vec<SocketAddr> {
        match self {
            Self::Worker { listen, .. } => vec![*listen],
            Self::QueryIngress {
                listen,
                control_listen,
                ..
            }
            | Self::PackingRouter {
                listen,
                control_listen,
                ..
            } => vec![*listen, *control_listen],
        }
    }
    pub fn state_dir(&self) -> &std::path::Path {
        match self {
            Self::Worker { data_dir, .. }
            | Self::QueryIngress { data_dir, .. }
            | Self::PackingRouter { data_dir, .. } => data_dir,
        }
    }
    pub fn probe(&self) -> (SocketAddr, &'static str) {
        match self {
            Self::Worker { listen, .. } => (*listen, "/internal/health"),
            Self::QueryIngress { control_listen, .. }
            | Self::PackingRouter { control_listen, .. } => (*control_listen, "/internal/health"),
        }
    }
}
