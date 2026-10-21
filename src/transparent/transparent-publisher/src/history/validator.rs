//! The History publisher's low-priority body validator. A restored
//! publication serves first; this thread then hashes its artifacts on one CPU
//! (integrity) and loads and checks its runtimes one at a time (correctness).
//! It keeps the running job and only the newest pending one.
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use transparent_shared::publication::{
    CheckedRuntimes, CorrectnessError, IntegrityError, Inventory, Validation,
};

/// What a validation result is about. A result only applies to the exact
/// inventory it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub directory: PathBuf,
    pub map_sha256: String,
    pub inventory_sha256: String,
    pub height: u64,
    pub hash: String,
    /// The authority epoch the job was submitted under.
    pub epoch: u64,
}

pub(crate) struct Job {
    pub identity: Identity,
    pub inventory: Inventory,
    pub validation: Validation,
}

/// One validation phase's outcome for `Identity`.
pub(crate) enum Event {
    Started(&'static str),
    Progress(&'static str, u64),
    Integrity(Result<u64, IntegrityError>),
    Correctness(Result<CheckedRuntimes, CorrectnessError>),
}

type Report = dyn Fn(&Identity, Event) + Send + Sync;
/// Answers true while foreground distribution or publication work runs.
type Busy = dyn Fn() -> bool + Send + Sync;

#[derive(Default)]
struct State {
    pending: Option<Job>,
    running: Option<PathBuf>,
    stop: bool,
}

/// Submits jobs; dropping the owning [`Validator`] stops and joins the thread.
#[derive(Clone, Default)]
pub(crate) struct Queue(Arc<(Mutex<State>, Condvar)>);
impl Queue {
    /// Replace any pending job with `job`; the running one continues.
    pub fn submit(&self, job: Job) {
        let (state, wake) = &*self.0;
        state.lock().unwrap().pending = Some(job);
        wake.notify_all();
    }
    /// Directories a job still reads; candidate collection keeps them.
    pub fn held(&self) -> Vec<PathBuf> {
        let state = self.0 .0.lock().unwrap();
        state
            .running
            .iter()
            .cloned()
            .chain(state.pending.iter().map(|j| j.identity.directory.clone()))
            .collect()
    }
    fn stopping(&self) -> bool {
        self.0 .0.lock().unwrap().stop
    }
}

pub(crate) struct Validator {
    queue: Queue,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Validator {
    pub fn spawn(
        queue: Queue,
        memory: Arc<transparent_shared::runtime::memory::WorkMemory>,
        busy: Box<Busy>,
        report: Box<Report>,
    ) -> std::io::Result<Self> {
        let owned = queue.clone();
        let thread = std::thread::Builder::new()
            .name("history-validator".into())
            .spawn(move || run(owned, memory, busy, report))?;
        Ok(Self {
            queue,
            thread: Some(thread),
        })
    }
}
impl Drop for Validator {
    /// Stops between 1 MiB reads or runtime checks; a runtime check already
    /// running finishes first. Joined before the controller lock is released.
    fn drop(&mut self) {
        let (state, wake) = &*self.queue.0;
        state.lock().unwrap().stop = true;
        wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run(
    queue: Queue,
    memory: Arc<transparent_shared::runtime::memory::WorkMemory>,
    busy: Box<Busy>,
    report: Box<Report>,
) {
    loop {
        let job = {
            let (state, wake) = &*queue.0;
            let mut state = state.lock().unwrap();
            state.running = None;
            loop {
                if state.stop {
                    return;
                }
                if let Some(job) = state.pending.take() {
                    state.running = Some(job.identity.directory.clone());
                    break job;
                }
                state = wake.wait(state).unwrap();
            }
        };
        // An unexpected panic is a failure to validate, never a reason to stop
        // the publisher.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate(&queue, &memory, &*busy, &*report, &job)
        }));
        if outcome.is_err() {
            report(
                &job.identity,
                Event::Integrity(Err(IntegrityError::Unavailable(
                    "validator panicked".into(),
                ))),
            );
        }
    }
}

fn validate(
    queue: &Queue,
    memory: &Arc<transparent_shared::runtime::memory::WorkMemory>,
    busy: &Busy,
    report: &Report,
    job: &Job,
) {
    let chunks = std::sync::atomic::AtomicU64::new(0);
    // Asked between 1 MiB reads and runtime checks: foreground distribution
    // and publication work go first; a stop ends the job.
    let pause = || {
        while busy() && !queue.stopping() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let read = chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if read % 256 == 0 {
            report(&job.identity, Event::Progress("integrity", read << 20));
        }
        queue.stopping()
    };
    loop {
        report(&job.identity, Event::Started("integrity"));
        match job.validation.integrity(&job.inventory, &|_| false, &pause) {
            Err(IntegrityError::Unavailable(reason)) if !queue.stopping() => {
                report(
                    &job.identity,
                    Event::Integrity(Err(IntegrityError::Unavailable(reason))),
                );
                if !retry_later(queue) {
                    return;
                }
            }
            result => {
                let passed = result.is_ok();
                report(&job.identity, Event::Integrity(result));
                if !passed {
                    return;
                }
                break;
            }
        }
    }
    loop {
        report(&job.identity, Event::Started("correctness"));
        let pause = || {
            while busy() && !queue.stopping() {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            queue.stopping()
        };
        match job.validation.correctness(&job.inventory, memory, &pause) {
            Err(error @ (CorrectnessError::NoCapacity | CorrectnessError::Unavailable(_)))
                if !queue.stopping() =>
            {
                report(&job.identity, Event::Correctness(Err(error)));
                if !retry_later(queue) {
                    return;
                }
            }
            result => {
                report(&job.identity, Event::Correctness(result));
                return;
            }
        }
    }
}

/// Wait before retrying a job that could not be validated now. False when a
/// stop or a newer job ends it.
fn retry_later(queue: &Queue) -> bool {
    let (state, wake) = &*queue.0;
    let state = state.lock().unwrap();
    let (state, _) = wake
        .wait_timeout_while(state, std::time::Duration::from_secs(30), |s| {
            !s.stop && s.pending.is_none()
        })
        .unwrap();
    !state.stop && state.pending.is_none()
}
