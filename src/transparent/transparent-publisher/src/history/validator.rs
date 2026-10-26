//! The History publisher's low-priority body validator. A restored
//! publication serves first; this thread then hashes its artifacts on one CPU
//! (integrity) and loads and checks its runtimes one at a time (correctness).
//! It keeps the running job and only the newest pending one; a newer job or a
//! cancellation stops the running one between reads or runtime checks.
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use transparent_shared::publication::{
    CheckedRuntimes, CorrectnessError, IntegrityError, Inventory, Validation, Verified,
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
    /// The authority epoch the job runs under; [`Queue::retain`] moves it
    /// forward when an invalidation keeps this publication public.
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

/// Publication roots whose validators tests hold as if foreground work ran.
#[cfg(test)]
pub(crate) static HOLD: std::sync::LazyLock<Mutex<std::collections::BTreeSet<PathBuf>>> =
    std::sync::LazyLock::new(Default::default);
/// Whether a test holds the validator of `root`.
pub(crate) fn held_by_test(_root: &std::path::Path) -> bool {
    #[cfg(test)]
    return HOLD.lock().unwrap().contains(_root);
    #[cfg(not(test))]
    false
}
/// Answers true while foreground distribution or publication work runs.
type Busy = dyn Fn() -> bool + Send + Sync;

#[derive(Default)]
struct State {
    pending: Option<Job>,
    /// The running job's directory and current epoch.
    running: Option<(PathBuf, u64)>,
    /// Advanced by every submission and cancellation; a running job whose
    /// generation is older is obsolete.
    generation: u64,
    stop: bool,
    collected: Option<PathBuf>,
}

/// Submits jobs; dropping the owning [`Validator`] stops and joins the thread.
#[derive(Clone, Default)]
pub(crate) struct Queue(Arc<(Mutex<State>, Condvar)>);
impl Queue {
    /// Replace any pending job with `job` and stop the running one.
    pub fn submit(&self, job: Job) {
        let (state, wake) = &*self.0;
        let mut state = state.lock().unwrap();
        state.pending = Some(job);
        state.generation += 1;
        wake.notify_all();
    }
    /// Drop the pending job and stop the running one: its publication was
    /// withdrawn or replaced.
    pub fn cancel(&self) {
        let (state, wake) = &*self.0;
        let mut state = state.lock().unwrap();
        state.pending = None;
        state.generation += 1;
        wake.notify_all();
    }
    /// An invalidation advanced the authority epoch to `epoch` but kept the
    /// validated publication public; later results are about it under `epoch`.
    pub fn retain(&self, epoch: u64) {
        let mut state = self.0 .0.lock().unwrap();
        if let Some((_, running)) = &mut state.running {
            *running = epoch;
        }
        if let Some(job) = &mut state.pending {
            job.identity.epoch = epoch;
        }
    }
    /// Directories a job still reads; candidate collection keeps them.
    pub fn held(&self) -> Vec<PathBuf> {
        let state = self.0 .0.lock().unwrap();
        state
            .running
            .iter()
            .map(|(directory, _)| directory.clone())
            .chain(state.pending.iter().map(|j| j.identity.directory.clone()))
            .collect()
    }
    /// Ordinary collection bounds retained validation progress too. This holds
    /// no directory or file against collection and never interrupts foreground.
    pub fn collected(&self, root: &std::path::Path) {
        let (state, wake) = &*self.0;
        state.lock().unwrap().collected = Some(root.to_path_buf());
        wake.notify_all();
    }

    /// Whether the job of `generation` must end now.
    fn obsolete(&self, generation: u64) -> bool {
        let state = self.0 .0.lock().unwrap();
        state.stop || state.generation != generation
    }
    fn identity(&self, job: &Job) -> Identity {
        let mut identity = job.identity.clone();
        if let Some((_, epoch)) = &self.0 .0.lock().unwrap().running {
            identity.epoch = *epoch;
        }
        identity
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

#[derive(Default)]
struct Progress {
    integrity: Verified,
    correctness: CheckedRuntimes,
}

fn run(
    queue: Queue,
    memory: Arc<transparent_shared::runtime::memory::WorkMemory>,
    busy: Box<Busy>,
    report: Box<Report>,
) {
    // Artifacts verified unchanged by an earlier job of this process.
    let mut progress = Progress::default();
    loop {
        let (job, generation) = {
            let (state, wake) = &*queue.0;
            let mut state = state.lock().unwrap();
            state.running = None;
            loop {
                if state.stop {
                    return;
                }
                if let Some(root) = state.collected.take() {
                    drop(state);
                    progress.integrity.retain_stored(&root);
                    progress.correctness.retain_stored(&root);
                    state = queue.0 .0.lock().unwrap();
                    continue;
                }
                if let Some(job) = state.pending.take() {
                    state.running = Some((job.identity.directory.clone(), job.identity.epoch));
                    break (job, state.generation);
                }
                state = wake.wait(state).unwrap();
            }
        };
        // Any parallel work inside a runtime load or check stays on one
        // low-priority thread. An unexpected panic is a failure to validate,
        // never a reason to stop the publisher.
        let outcome = transparent_shared::runtime::validation_pool().install(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                validate(
                    &queue,
                    generation,
                    &memory,
                    &*busy,
                    &*report,
                    &job,
                    &mut progress,
                )
            }))
        });
        if outcome.is_err() {
            report(
                &queue.identity(&job),
                Event::Integrity(Err(IntegrityError::Unavailable(
                    "validator panicked".into(),
                ))),
            );
        }
    }
}

fn validate(
    queue: &Queue,
    generation: u64,
    memory: &Arc<transparent_shared::runtime::memory::WorkMemory>,
    busy: &Busy,
    report: &Report,
    job: &Job,
    progress: &mut Progress,
) {
    let send = |event| report(&queue.identity(job), event);
    let chunks = std::sync::atomic::AtomicU64::new(0);
    // Asked between 1 MiB reads and runtime checks: foreground distribution
    // and publication work go first; a stop or newer job ends this one.
    let wait = || {
        while busy() && !queue.obsolete(generation) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        queue.obsolete(generation)
    };
    let pause = || {
        let read = chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if read.is_multiple_of(256) {
            send(Event::Progress("integrity", read << 20));
        }
        wait()
    };
    loop {
        send(Event::Started("integrity"));
        let verified = match job.validation.integrity_continuing(
            &job.inventory,
            &mut progress.integrity,
            &pause,
        ) {
            Ok(verified) => verified,
            Err(IntegrityError::Unavailable(reason)) if !queue.obsolete(generation) => {
                send(Event::Integrity(Err(IntegrityError::Unavailable(reason))));
                if !retry_later(queue, generation) {
                    return;
                }
                continue;
            }
            Err(error) => {
                send(Event::Integrity(Err(error)));
                return;
            }
        };
        send(Event::Integrity(Ok(verified.hashed)));
        send(Event::Started("correctness"));
        match job.validation.correctness_continuing(
            &job.inventory,
            &verified,
            memory,
            &wait,
            &mut progress.correctness,
        ) {
            // A file replaced since it was hashed is hashed again first.
            Err(error @ (CorrectnessError::NoCapacity | CorrectnessError::Unavailable(_)))
                if !queue.obsolete(generation) =>
            {
                send(Event::Correctness(Err(error)));
                if !retry_later(queue, generation) {
                    return;
                }
            }
            result => {
                send(Event::Correctness(result));
                return;
            }
        }
    }
}

/// Wait before retrying a job that could not be validated now. False when a
/// stop or a newer job ends it; never waits on memory for obsolete work.
fn retry_later(queue: &Queue, generation: u64) -> bool {
    let (state, wake) = &*queue.0;
    let state = state.lock().unwrap();
    let (state, _) = wake
        .wait_timeout_while(state, std::time::Duration::from_secs(30), |s| {
            !s.stop && s.generation == generation
        })
        .unwrap();
    !state.stop && state.generation == generation
}
