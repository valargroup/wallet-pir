//! Status PIR serving core: the controller that decides which generation a
//! request may use, the private role protocol and the Status service.
//!
//! Generations, the wire protocol and the publication contract live in
//! `status-material`. The [`service`] module owns Status listeners, admission
//! and outbound HTTP. `pir-query-server` reserves listeners and supervises the
//! product task; chain observation and publication remain in
//! `status-publisher`, which this crate must never depend on.
mod controller;
pub mod distributed;
pub mod service;
pub use controller::Controller;
