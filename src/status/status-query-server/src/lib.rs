//! Status PIR serving core: the controller that decides which generation a
//! request may use, the private role protocol and the Status service.
//!
//! Generations, the wire protocol and the publication contract live in
//! `status-shared`. The [`service`] module owns Status listeners, admission
//! and outbound HTTP. `pir-query-server` reserves listeners and supervises the
//! product task; chain observation and publication remain in
//! `status-publisher`, which this crate must never depend on.
mod controller;
pub mod distributed;
pub mod service;
pub use controller::Controller;

/// Where the wallet-facing Status operations (`/init`, `/session/:id`,
/// `/query`) are served: the canonical `/status/v1` namespace and the deployed
/// `/v1/status` alias reach the same router handlers. Control, provider and
/// worker routes are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/status/v1", "/v1/status");
