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
    if let MatvecBackend::Cuda { device } = backend {
        #[cfg(feature = "cuda")]
        {
            return pir_cuda::server_from_profile(profile, coefficients, false, true, device);
        }
        #[cfg(not(feature = "cuda"))]
        {
            let _ = device;
            return Err(KernelError("CUDA backend requires the cuda feature".into()));
        }
    }
    // Never pass CUDA to upstream from a CPU-only Status build: another product
    // can enable upstream's feature in the same Cargo invocation.
    IPIRServer::try_from_profile_with_backend(profile, coefficients, false, true, backend)
}

#[cfg(all(test, not(feature = "cuda")))]
mod tests {
    use super::*;

    #[test]
    fn cuda_requires_the_status_feature_before_reading_coefficients() {
        let coefficients = std::iter::from_fn(|| {
            panic!("a CPU-only build must reject CUDA before reading the database")
        });
        let error = server(
            crate::profile(),
            coefficients,
            MatvecBackend::Cuda { device: 0 },
        )
        .err()
        .expect("CUDA must require the Status feature");
        assert!(error.to_string().contains("requires the cuda feature"));
    }
}
