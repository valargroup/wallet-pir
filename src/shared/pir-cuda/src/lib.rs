//! Optional GPU construction boundary. Default workspace builds load no CUDA code.
//! Products own runtime selection, database lifetime and error handling.

#[cfg(feature = "cuda")]
pub use simplepir_kernel::FirstDimKernel;

#[cfg(feature = "cuda")]
pub fn kernel(
    device: usize,
) -> Result<Box<dyn simplepir_kernel::FirstDimKernel<u16>>, ipir_sp::server::KernelError> {
    Ok(Box::new(simplepir_kernel::cuda::CudaKernel::new(device)?))
}

/// Construct through the upstream profile API so the selected arithmetic profile
/// remains attached to the prepared database, including native-ring profiles.
#[cfg(feature = "cuda")]
pub fn server_from_profile(
    profile: &ipir_sp::ProductionSimplePirParams,
    coefficients: impl Iterator<Item = u16>,
    transposed: bool,
    pad_rows: bool,
    device: usize,
) -> Result<ipir_sp::IPIRServer<u16>, ipir_sp::server::KernelError> {
    ipir_sp::IPIRServer::try_from_profile_with_backend(
        profile,
        coefficients,
        transposed,
        pad_rows,
        ipir_sp::server::MatvecBackend::Cuda { device },
    )
}
