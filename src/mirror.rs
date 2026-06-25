//! The mirroring client — `rdev watch` reimplemented over the Drive API.
//!
//! A client that wants to *mirror* (not just browse) a remote drive bootstraps a local
//! [`ce_drive_core::Drive`] replica from the `Open` snapshot CID (so it starts in O(snapshot), not
//! O(log)), then keeps it live: subscribe to the change beacon, and on each wake (or periodically,
//! since the beacon is lossy) call `Poll` for deltas since its cursor and apply them. The cursor is
//! the source of truth; the beacon is only a latency hint.
//!
//! The bootstrap snapshot is a serialized [`ce_drive_core::DriveState`] (the move-op + content-op
//! logs); replaying it reconstructs the [`DriveTree`](ce_drive_core::DriveTree) + content map exactly.

use anyhow::{Result, anyhow};
use ce_drive_core::{Drive, DriveState};

use crate::client::RemoteDrive;

/// A local replica of a remote drive, kept live by the change feed.
pub struct Mirror {
    remote: RemoteDrive,
    drive: Drive,
    cursor: u64,
}

impl Mirror {
    /// Bootstrap a replica from the remote's `Open` snapshot. Fetches the snapshot object by CID,
    /// deserializes the [`DriveState`], replays it, and records the server's cursor as the starting
    /// point for `Poll`.
    pub async fn bootstrap(remote: RemoteDrive) -> Result<Self> {
        let opened = remote.open().await?;
        let bytes = remote
            .client()
            .get_object(&opened.drive_root_cid)
            .await
            .map_err(|e| anyhow!("fetch snapshot {}: {e}", opened.drive_root_cid))?;
        let state: DriveState = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow!("decode snapshot DriveState: {e}"))?;
        let drive = Drive::from_state(state);
        Ok(Mirror { remote, drive, cursor: opened.server_seq })
    }

    /// The local replica (read-only). `ls`, `path_of`, etc. are served from here with zero RPC.
    pub fn drive(&self) -> &Drive {
        &self.drive
    }

    /// The current sync cursor.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The remote handle (for reads/writes that hit the host).
    pub fn remote(&self) -> &RemoteDrive {
        &self.remote
    }

    /// Poll the remote for changes since our cursor and re-stat each changed path to refresh the
    /// local replica's view. Returns the number of changes applied. The replica converges by
    /// re-reading changed paths (Drive's `changes.list` contract: deltas carry paths, not bytes).
    ///
    /// We rebuild the replica from a fresh snapshot when the change set indicates structural drift we
    /// cannot cheaply replay (moves/deletes); for create/modify we stat-and-apply. For v1 simplicity
    /// and correctness, any non-empty change page triggers a re-bootstrap of the tree from a fresh
    /// `Open` snapshot, which is always correct and O(snapshot). (Chunk-level incremental apply is the
    /// M3 optimization; the cursor contract makes it transparent to swap in.)
    pub async fn sync(&mut self) -> Result<usize> {
        // Count changes since our cursor WITHOUT advancing the cursor yet: the cursor is the source
        // of truth and must only ever be moved to a seq the local tree actually reflects. We poll
        // purely to learn whether (and how much) changed, then re-bootstrap the tree from a fresh
        // snapshot and set the cursor to *that snapshot's* server_seq.
        let mut applied = 0usize;
        let mut scan_cursor = self.cursor;
        const PAGE: u32 = 256;
        loop {
            let (changes, new_cursor) = self.remote.poll(Some(scan_cursor), PAGE).await?;
            if changes.is_empty() {
                break;
            }
            applied += changes.len();
            scan_cursor = new_cursor;
            // A partial page means we've reached the tail — no need for a trailing empty poll
            // (saves one RPC per sync, the common single-change case).
            if (changes.len() as u32) < PAGE {
                break;
            }
        }
        if applied > 0 {
            // Re-bootstrap the tree from a fresh snapshot (always correct; O(snapshot)).
            let opened = self.remote.open().await?;
            let bytes = self.remote.client().get_object(&opened.drive_root_cid).await?;
            let state: DriveState = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("decode snapshot DriveState: {e}"))?;
            self.drive = Drive::from_state(state);
            // The cursor MUST track the seq the snapshot actually reflects (see
            // [`cursor_after_snapshot`]): never `max(old, snapshot)`, which would skip the gap.
            self.cursor = cursor_after_snapshot(scan_cursor, opened.server_seq);
        }
        Ok(applied)
    }

    /// List a directory from the local replica (zero RPC).
    pub fn ls(&self, path: &str) -> Result<Vec<ce_drive_core::DirEntry>> {
        self.drive.ls(path)
    }

    /// Read a file's current bytes from the remote (the replica holds metadata; bytes live in the
    /// data layer). Convenience that delegates to the remote handle.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.remote.read_all(path).await
    }
}

/// Decide the sync cursor after re-bootstrapping the local tree from a fresh `Open` snapshot.
///
/// The local tree, after the re-bootstrap, reflects *exactly* `snapshot_seq` — no more, no less. The
/// cursor is the source of truth for "what have I applied", so it must equal `snapshot_seq`, **never**
/// `max(scan_frontier, snapshot_seq)`.
///
/// Why `max` is a bug: `sync()` first polls forward to learn how many changes exist, reaching
/// `scan_frontier`. If a change lands while it polls, the freshly-taken snapshot can be *older* than
/// `scan_frontier` (`snapshot_seq < scan_frontier`). The tree then reflects only up to `snapshot_seq`,
/// but `max` would leave the cursor at `scan_frontier` — so the next `Poll(cursor=scan_frontier)`
/// would never return the changes in `(snapshot_seq, scan_frontier]`. They are silently skipped and
/// the replica is permanently stale for that gap. Pinning the cursor to `snapshot_seq` makes the next
/// poll re-fetch the gap; the re-bootstrap is idempotent, so re-seeing a change is harmless.
fn cursor_after_snapshot(_scan_frontier: u64, snapshot_seq: u64) -> u64 {
    snapshot_seq
}

#[cfg(test)]
mod tests {
    use super::cursor_after_snapshot;

    #[test]
    fn cursor_tracks_snapshot_not_max_so_gap_is_not_skipped() {
        // Regression for Theme E: a change landed in (snapshot_seq, scan_frontier] while sync() was
        // polling forward, so the fresh snapshot is OLDER than the poll frontier.
        let snapshot_seq = 40; // the snapshot the tree was rebuilt from
        let scan_frontier = 57; // how far the forward poll advanced before snapshotting

        let cursor = cursor_after_snapshot(scan_frontier, snapshot_seq);

        // The cursor must equal what the tree reflects, so the NEXT poll re-fetches (40, 57].
        assert_eq!(cursor, snapshot_seq, "cursor must track the snapshot the tree reflects");
        assert!(
            cursor < scan_frontier,
            "cursor must rewind below the scan frontier so the gap (snapshot_seq, scan_frontier] is \
             re-polled, not skipped"
        );
        // The old buggy `cursor.max(snapshot_seq)` would have produced 57, skipping (40, 57].
        let buggy = scan_frontier.max(snapshot_seq);
        assert_ne!(cursor, buggy, "must not take the max (that is exactly the skip bug)");
    }

    #[test]
    fn cursor_uses_snapshot_even_when_snapshot_is_newer() {
        // When the snapshot is NEWER than the scan frontier (the common case: nothing raced), the
        // cursor still tracks the snapshot — which is >= the frontier, so nothing is re-polled
        // unnecessarily and nothing is skipped.
        assert_eq!(cursor_after_snapshot(40, 57), 57);
        // And the equal case is a no-op.
        assert_eq!(cursor_after_snapshot(50, 50), 50);
    }
}
