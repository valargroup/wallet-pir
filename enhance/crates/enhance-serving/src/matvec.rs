//! Process-local matrix evaluator configuration; never part of artifact identity.
use ipir_sp::{server::KernelError, IPIRServer, YpirSchemeParams};
use serde::Serialize;

#[cfg(test)]
#[path = "matvec_test.rs"]
pub(crate) mod testing;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    #[default]
    Cpu,
    Cuda,
}

/// CLI and environment configuration, shared by the worker library entrypoint.
#[derive(Clone, Copy, Debug, Default, clap::Args, Serialize)]
pub struct MatvecConfig {
    /// Matrix-vector evaluator; packing always runs on the CPU.
    #[arg(
        long,
        env = "ENHANCE_MATVEC_BACKEND",
        value_enum,
        default_value = "cpu"
    )]
    pub matvec_backend: Backend,
    /// CUDA device ordinal (defaults to zero when CUDA is selected).
    #[arg(long, env = "ENHANCE_CUDA_DEVICE")]
    pub cuda_device: Option<usize>,
}

impl MatvecConfig {
    /// Validate before opening persistent worker state, even for an empty worker.
    pub fn validate(self) -> Result<(), KernelError> {
        match self.matvec_backend {
            Backend::Cpu if self.cuda_device.is_some() => Err(KernelError(
                "--cuda-device requires --matvec-backend cuda".into(),
            )),
            Backend::Cpu => Ok(()),
            Backend::Cuda => {
                #[cfg(feature = "cuda")]
                {
                    self.cuda_kernel().map(drop)
                }
                #[cfg(not(feature = "cuda"))]
                {
                    Err(missing_cuda())
                }
            }
        }
    }

    #[cfg(feature = "cuda")]
    fn cuda_kernel(self) -> Result<simplepir_kernel::cuda::CudaKernel, KernelError> {
        simplepir_kernel::cuda::CudaKernel::new(self.cuda_device.unwrap_or(0))
    }

    pub(crate) fn server(
        self,
        params: YpirSchemeParams,
        coefficients: impl Iterator<Item = u16>,
        transposed: bool,
    ) -> Result<IPIRServer<u16>, KernelError> {
        #[cfg(test)]
        if let Some(faults) = testing::current() {
            return testing::server(self, params, coefficients, transposed, faults);
        }
        match self.matvec_backend {
            Backend::Cpu => {
                if self.cuda_device.is_some() {
                    self.validate()?;
                }
                Ok(IPIRServer::new_auto_kernel(
                    params,
                    coefficients,
                    transposed,
                    true,
                ))
            }
            Backend::Cuda => {
                #[cfg(feature = "cuda")]
                {
                    IPIRServer::try_with_kernel(
                        params,
                        coefficients,
                        transposed,
                        true,
                        Box::new(self.cuda_kernel()?),
                    )
                }
                #[cfg(not(feature = "cuda"))]
                {
                    Err(missing_cuda())
                }
            }
        }
    }
}

#[cfg(not(feature = "cuda"))]
fn missing_cuda() -> KernelError {
    KernelError("CUDA selection requires an enhance-pir-server build with --features cuda".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[derive(Parser)]
    struct Args {
        #[command(flatten)]
        config: MatvecConfig,
    }
    #[test]
    fn explicit_configuration_validation() {
        let cpu = Args::try_parse_from(["worker"]).unwrap().config;
        assert_eq!(cpu.matvec_backend, Backend::Cpu);
        cpu.validate().unwrap();
        let invalid = Args::try_parse_from(["worker", "--cuda-device", "0"])
            .unwrap()
            .config;
        assert!(invalid.validate().is_err());
        assert!(Args::try_parse_from(["worker", "--matvec-backend", "auto"]).is_err());
        let invalid_device = usize::MAX.to_string();
        let gpu = Args::try_parse_from([
            "worker",
            "--matvec-backend",
            "cuda",
            "--cuda-device",
            &invalid_device,
        ])
        .unwrap()
        .config;
        assert!(gpu.validate().is_err());
    }
    #[cfg(not(feature = "cuda"))]
    #[test]
    fn cuda_without_feature_fails_before_creating_worker_state() {
        let root = tempfile::tempdir().unwrap();
        let config = MatvecConfig {
            matvec_backend: Backend::Cuda,
            cuda_device: None,
        };
        let result =
            crate::worker::Worker::open_with_backend(root.path(), Default::default(), config);
        assert!(matches!(result, Err(e) if e.contains("--features cuda")));
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
