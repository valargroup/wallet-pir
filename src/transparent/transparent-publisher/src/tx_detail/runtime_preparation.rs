//! Required runtime construction from already verified display tables.
use crate::BoxError;
use std::path::{Path, PathBuf};
use transparent_shared::tx_detail::{
    prebuild::{build_shipped_verified_until, Shipped},
    set::DisplayRevision,
};

/// The runtime builder consumes verified source handles. Successful output must
/// subsequently pass inventory integrity and semantic checks before activation.
pub trait RuntimePreparation: Send + Sync {
    fn prepare(
        &self,
        revision: &DisplayRevision,
        output: &Path,
        retained: &[PathBuf],
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Vec<Shipped>, BoxError>;
}
#[derive(Default)]
pub struct RequiredRuntimes;
impl RuntimePreparation for RequiredRuntimes {
    fn prepare(
        &self,
        revision: &DisplayRevision,
        output: &Path,
        retained: &[PathBuf],
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Vec<Shipped>, BoxError> {
        build_shipped_verified_until(revision, output, retained, stop).map_err(Into::into)
    }
}
