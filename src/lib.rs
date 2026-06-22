//! # ce-drive-client
//!
//! The **consumer** side of the CE Drive mesh API: open a remote drive `(host, drive_id)` over the
//! CE mesh by presenting a `ce-cap` chain, and call the `ce-drive/v1` op set — list, read, write,
//! mkdir, move, copy, delete, share, poll, watch — addressed by NodeId over libp2p (relay/NAT
//! traversed), never by stored ip:port.
//!
//! Two faces:
//! - [`RemoteDrive`] — the typed request/reply client; each method is one `AppRequest`.
//! - [`Mirror`] — bootstraps a local [`ce_drive_core::Drive`] replica from the `Open` snapshot CID
//!   and keeps it live via `Poll` (+ the change beacon), so `ls` is served locally with zero RPC.
//!   This is `rdev watch` reimplemented over the Drive API.
//!
//! Bulk bytes never travel on the metadata channel: [`RemoteDrive::read`] gets a [`ReadPlan`] of
//! chunk CIDs and fetches them directly from the content-addressed data layer ([`readplan`]),
//! verifying each against its CID (content addressing IS the integrity proof).
//!
//! ## Standards
//! Edition 2024, `anyhow::Result`, `tracing` (no `println!` in the library), no `unsafe`, no
//! `unwrap()` on production paths.

pub mod client;
pub mod mirror;
pub mod readplan;

pub use client::{Opened, RemoteDrive, Written, DEFAULT_TIMEOUT_MS};
pub use mirror::Mirror;

// Re-export the wire types so downstreams need only depend on this crate.
pub use ce_drive_serve::{
    Change, ChangeKind, ChunkRef, DriveErr, Entry, EntryKind, Quota, ReadPlan, ShareCaveats,
};
