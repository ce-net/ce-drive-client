# ce-drive-client — AI agent context

The **consumer** side of the CE Drive mesh API. An app over CE primitives (no node changes).

## What this crate is
Open a remote drive `(host, drive_id)` over the CE mesh by presenting a `ce-cap` chain, and call the
`ce-drive/v1` op set addressed by NodeId (relay/NAT-traversed), never by stored ip:port.

## Modules
- `client.rs` — `RemoteDrive`: typed methods over `ce_rs::CeClient::request(host, "ce-drive/v1", ...)`
  (open/stat/list/read/write/mkdir/move/copy/delete/share/poll/watch). `read` gets a `ReadPlan` and
  fetches chunks itself; `write` does `put_object` then commits.
- `readplan.rs` — ranged chunk fetch + CID-verify + slice to the exact `[offset,len)` window.
- `mirror.rs` — `Mirror`: bootstrap a local `ce-drive-core::Drive` replica from the `Open` snapshot
  CID, keep it live via `Poll` (+ beacon). `rdev watch` over the Drive API; `ls` served zero-RPC.
- `main.rs` — the `ce-drive` CLI (ls/cat/put/mkdir/mv/cp/rm/share/poll/mirror; `--name`/`--host`/`--cap`).

## Dependencies (all by path)
`ce-rs`, `ce-cap`, `ce-drive-core`, `ce-drive-serve` (the shared wire). Same `[patch]` block as serve.

## Example
`cargo run --example host_and_access -p ce-drive-client` — boots two in-process CE nodes, hosts a
drive on B, accesses it from A by capability (open/write/read/mirror). The CLI demo from the plan.

## Standards
Edition 2024, `anyhow::Result`, `tracing` (no `println!` in the lib), no `unsafe`/`unwrap()` in prod,
no emojis. Author: Leif Rydenfalk. No co-author lines.

## Build/verify
`cargo build && cargo test`. Shared cargo target-dir at `~/ce-net/.cargo-shared`.
