//! Publisher completion policy; execution lives in the shared engine.
/// Continue polling or finish a bounded publication run.
#[derive(Clone, Copy)]
pub enum Control {
    Continue,
    Complete,
}
