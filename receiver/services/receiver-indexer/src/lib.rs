//! The receiver directory's indexer: it reads Ironwood blocks from a Zakura node,
//! keeps the payments recoverable with the zero outgoing viewing key, and publishes them
//! as receiver directory revisions. The binary is `receiver-directory`; see
//! `receiver/README.md`.
pub mod blocks;
pub mod near;
pub mod zakura;
