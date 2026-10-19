//! Evaluator construction is the only Status boundary that activates CUDA.
use ipir_sp::{
    server::{KernelError, MatvecBackend},
    IPIRServer, ProductionSimplePirParams,
};

pub(crate) fn server(
    profile: &ProductionSimplePirParams,
    coefficients: impl Iterator<Item = u16>,
    backend: MatvecBackend,
) -> Result<IPIRServer<u16>, KernelError> {
    #[cfg(feature = "cuda")]
    if let MatvecBackend::Cuda { device } = backend {
        return pir_cuda::server_from_profile(profile, coefficients, false, true, device);
    }
    // The CPU path and the explicit error in a CPU-only build stay upstream.
    IPIRServer::try_from_profile_with_backend(profile, coefficients, false, true, backend)
}
