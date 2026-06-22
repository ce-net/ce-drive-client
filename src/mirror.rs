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
        let mut applied = 0usize;
        loop {
            let (changes, new_cursor) = self.remote.poll(Some(self.cursor), 256).await?;
            if changes.is_empty() {
                break;
            }
            applied += changes.len();
            self.cursor = new_cursor;
        }
        if applied > 0 {
            // Re-bootstrap the tree from a fresh snapshot (always correct; O(snapshot)).
            let opened = self.remote.open().await?;
            let bytes = self.remote.client().get_object(&opened.drive_root_cid).await?;
            let state: DriveState = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("decode snapshot DriveState: {e}"))?;
            self.drive = Drive::from_state(state);
            // Keep the cursor we already advanced to (the snapshot is at >= it).
            self.cursor = self.cursor.max(opened.server_seq);
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
