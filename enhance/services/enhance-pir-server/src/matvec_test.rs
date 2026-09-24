//! Test-only faults wrap real kernels. Construction scope is thread-local; each
//! constructed kernel retains its own fault state across HTTP blocking threads.
use super::*;
use simplepir_kernel::{ChunkedSplitKernel, FirstDimKernel};
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

#[derive(Default)]
pub(crate) struct Faults {
    pub prepare_calls: AtomicUsize,
    pub fail_prepare_at: AtomicUsize,
    pub fail_evaluation: AtomicBool,
    pub evaluation_calls: AtomicUsize,
    pub live_bytes: AtomicUsize,
}
thread_local! {
    static ACTIVE: RefCell<Option<Arc<Faults>>> = const { RefCell::new(None) };
}
pub(super) fn current() -> Option<Arc<Faults>> {
    ACTIVE.with(|active| active.borrow().clone())
}
pub(crate) fn scoped<T>(faults: &Arc<Faults>, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<Faults>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACTIVE.with(|active| *active.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(ACTIVE.with(|active| active.replace(Some(faults.clone()))));
    f()
}
pub(super) fn server(
    config: MatvecConfig,
    params: YpirSchemeParams,
    coefficients: impl Iterator<Item = u16>,
    transposed: bool,
    faults: Arc<Faults>,
) -> Result<IPIRServer<u16>, KernelError> {
    let inner: Box<dyn FirstDimKernel<u16>> = match config.matvec_backend {
        Backend::Cpu => Box::new(ChunkedSplitKernel::default()),
        Backend::Cuda => {
            #[cfg(feature = "cuda")]
            {
                Box::new(config.cuda_kernel()?)
            }
            #[cfg(not(feature = "cuda"))]
            {
                return Err(missing_cuda());
            }
        }
    };
    IPIRServer::try_with_kernel(
        params,
        coefficients,
        transposed,
        true,
        Box::new(FaultKernel {
            inner,
            faults,
            bytes: 0,
        }),
    )
}
struct FaultKernel {
    inner: Box<dyn FirstDimKernel<u16>>,
    faults: Arc<Faults>,
    bytes: usize,
}
impl Drop for FaultKernel {
    fn drop(&mut self) {
        self.faults.live_bytes.fetch_sub(self.bytes, SeqCst);
    }
}
impl FirstDimKernel<u16> for FaultKernel {
    fn try_prepare(&mut self, db: &[u16], rows: usize, cols: usize) -> Result<(), KernelError> {
        self.inner.try_prepare(db, rows, cols)?;
        self.faults.live_bytes.fetch_sub(self.bytes, SeqCst);
        self.bytes = std::mem::size_of_val(db);
        self.faults.live_bytes.fetch_add(self.bytes, SeqCst);
        let call = self.faults.prepare_calls.fetch_add(1, SeqCst) + 1;
        // Fail after actual allocation/upload, including on the failing unit.
        if self.faults.fail_prepare_at.load(SeqCst) == call {
            return Err(KernelError("injected device preparation failure".into()));
        }
        Ok(())
    }
    fn multiply_query(
        &self,
        _: &inspiring::RlweParams,
        _: &[u16],
        _: usize,
        _: usize,
        _: &[u64],
        _: u64,
        _: &mut [u64],
    ) {
        panic!("worker must use fallible kernel evaluation")
    }
    fn try_multiply_query(
        &self,
        rlwe: &inspiring::RlweParams,
        db: &[u16],
        rows: usize,
        cols: usize,
        query: &[u64],
        max: u64,
        out: &mut [u64],
    ) -> Result<(), KernelError> {
        self.faults.evaluation_calls.fetch_add(1, SeqCst);
        if self.faults.fail_evaluation.load(SeqCst) {
            out.fill(123); // A partial result must never escape as a successful response.
            return Err(KernelError(
                "private injected device evaluation detail".into(),
            ));
        }
        self.inner
            .try_multiply_query(rlwe, db, rows, cols, query, max, out)
    }
}
