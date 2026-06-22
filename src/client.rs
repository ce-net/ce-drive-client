//! The typed remote-drive client — open a drive `(host, drive_id)` over the mesh and call the
//! `ce-drive/v1` ops by presenting a `ce-cap` chain on every request.
//!
//! Transport is `ce_rs::CeClient::request(host, "ce-drive/v1", payload, timeout)`: libp2p routes it
//! (relay/DCUtR), the host's serve loop authorizes + replies. No address is ever stored; the host is
//! addressed by NodeId, discoverable by `resolve_name` / `find_service`.

use anyhow::{Result, anyhow, bail};
use ce_drive_serve::{
    DRIVE_TOPIC, DriveErr, DriveOk, DriveOp, DriveReq, Entry, Quota, ReadPlan, ShareCaveats,
    decode_reply, encode_req,
};
use ce_rs::CeClient;

use crate::readplan;

/// Default request timeout for a metadata op (ms).
pub const DEFAULT_TIMEOUT_MS: u64 = 15_000;

/// A handle to a remote drive: the local node client, the host NodeId, the drive id, and the
/// capability token presented on every request.
#[derive(Clone)]
pub struct RemoteDrive {
    client: CeClient,
    host: String,
    drive: String,
    cap: String,
    timeout_ms: u64,
}

impl RemoteDrive {
    /// Build a handle to drive `drive_id` on `host_hex`, authorized by the hex `cap` chain token.
    pub fn new(client: CeClient, host_hex: &str, drive_id: &str, cap: &str) -> Self {
        RemoteDrive {
            client,
            host: host_hex.to_string(),
            drive: drive_id.to_string(),
            cap: cap.to_string(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    /// Resolve a drive by name (`resolve_name`) into a handle. Errors if the name is unclaimed.
    pub async fn open_by_name(
        client: CeClient,
        name: &str,
        drive_id: &str,
        cap: &str,
    ) -> Result<Self> {
        let host = client
            .resolve_name(name)
            .await?
            .ok_or_else(|| anyhow!("drive name '{name}' is not claimed"))?;
        Ok(Self::new(client, &host, drive_id, cap))
    }

    /// Override the per-request timeout.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// The host NodeId hex.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The underlying node client (for `get_blob` during ranged reads).
    pub fn client(&self) -> &CeClient {
        &self.client
    }

    /// Send one op and decode the reply, surfacing a [`DriveErr`] as an `anyhow` error.
    async fn call(&self, op: DriveOp) -> Result<DriveOk> {
        let req = DriveReq { drive: self.drive.clone(), cap: self.cap.clone(), op };
        let bytes = encode_req(&req);
        let raw = self
            .client
            .request(&self.host, DRIVE_TOPIC, &bytes, self.timeout_ms)
            .await
            .map_err(|e| anyhow!("ce-drive request failed: {e}"))?;
        let reply = decode_reply(&raw)?;
        reply.result.map_err(|e: DriveErr| anyhow!("ce-drive: {e}"))
    }

    // ----- the op set -----

    /// Handshake: returns the bootstrap snapshot CID, current cursor, granted abilities, and quota.
    pub async fn open(&self) -> Result<Opened> {
        match self.call(DriveOp::Open).await? {
            DriveOk::Opened { drive_root_cid, server_seq, granted_abilities, quota } => {
                Ok(Opened { drive_root_cid, server_seq, granted_abilities, quota })
            }
            other => bail!("unexpected reply to Open: {other:?}"),
        }
    }

    /// Stat one path.
    pub async fn stat(&self, path: &str) -> Result<Entry> {
        match self.call(DriveOp::Stat { path: path.to_string() }).await? {
            DriveOk::Entry(e) => Ok(e),
            other => bail!("unexpected reply to Stat: {other:?}"),
        }
    }

    /// List a directory page. `cursor` continues a previous listing; `limit` bounds the page.
    pub async fn list(
        &self,
        path: &str,
        cursor: Option<String>,
        limit: u32,
    ) -> Result<(Vec<Entry>, Option<String>)> {
        match self.call(DriveOp::List { path: path.to_string(), cursor, limit }).await? {
            DriveOk::Listing { entries, next_cursor } => Ok((entries, next_cursor)),
            other => bail!("unexpected reply to List: {other:?}"),
        }
    }

    /// List a whole directory (paging internally until exhausted).
    pub async fn list_all(&self, path: &str) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = self.list(path, cursor, 256).await?;
            out.extend(page);
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(out)
    }

    /// Get a [`ReadPlan`] for a ranged read (chunk CIDs, not bytes).
    pub async fn read_plan(&self, path: &str, offset: u64, len: Option<u64>) -> Result<ReadPlan> {
        match self.call(DriveOp::Read { path: path.to_string(), offset, len }).await? {
            DriveOk::ReadPlan(p) => Ok(p),
            other => bail!("unexpected reply to Read: {other:?}"),
        }
    }

    /// Read a byte range of a file: get the plan, fetch the intersecting chunks directly from the
    /// data layer, verify each against its CID, and return exactly `[offset, offset+len)`.
    pub async fn read(&self, path: &str, offset: u64, len: Option<u64>) -> Result<Vec<u8>> {
        let plan = self.read_plan(path, offset, len).await?;
        readplan::fetch_range(&self.client, &plan, offset, len).await
    }

    /// Read a whole file.
    pub async fn read_all(&self, path: &str) -> Result<Vec<u8>> {
        self.read(path, 0, None).await
    }

    /// Write a file: upload the bytes to the data layer (`put_object`), then commit `path ->
    /// object_cid` with optimistic concurrency on `base_etag`. Returns the new etag + version seq.
    pub async fn write(
        &self,
        path: &str,
        bytes: &[u8],
        base_etag: Option<String>,
    ) -> Result<Written> {
        let object_cid = self.client.put_object(bytes).await?;
        match self
            .call(DriveOp::Write {
                path: path.to_string(),
                object_cid,
                size: bytes.len() as u64,
                base_etag,
            })
            .await?
        {
            DriveOk::Written { etag, node_id, version_seq } => {
                Ok(Written { etag, node_id, version_seq })
            }
            other => bail!("unexpected reply to Write: {other:?}"),
        }
    }

    /// Create a directory.
    pub async fn mkdir(&self, path: &str) -> Result<String> {
        match self.call(DriveOp::Mkdir { path: path.to_string() }).await? {
            DriveOk::Made { node_id } => Ok(node_id),
            other => bail!("unexpected reply to Mkdir: {other:?}"),
        }
    }

    /// Move/rename a node.
    pub async fn mv(&self, from: &str, to: &str) -> Result<String> {
        match self.call(DriveOp::Move { from: from.to_string(), to: to.to_string() }).await? {
            DriveOk::Made { node_id } => Ok(node_id),
            other => bail!("unexpected reply to Move: {other:?}"),
        }
    }

    /// Server-side copy (free; shares chunk CIDs).
    pub async fn cp(&self, from: &str, to: &str) -> Result<String> {
        match self.call(DriveOp::Copy { from: from.to_string(), to: to.to_string() }).await? {
            DriveOk::Made { node_id } => Ok(node_id),
            other => bail!("unexpected reply to Copy: {other:?}"),
        }
    }

    /// Delete a node (to TRASH).
    pub async fn rm(&self, path: &str, recursive: bool) -> Result<()> {
        match self.call(DriveOp::Delete { path: path.to_string(), recursive }).await? {
            DriveOk::Deleted => Ok(()),
            other => bail!("unexpected reply to Delete: {other:?}"),
        }
    }

    /// Mint an attenuated sub-capability for `audience`, scoped to `path`. Returns the extended chain
    /// token (the recipient stores it and presents it on future requests).
    pub async fn share(
        &self,
        path: &str,
        audience: &str,
        abilities: Vec<String>,
        caveats: ShareCaveats,
    ) -> Result<String> {
        match self
            .call(DriveOp::Share {
                path: path.to_string(),
                audience: audience.to_string(),
                abilities,
                caveats,
            })
            .await?
        {
            DriveOk::Shared { chain } => Ok(chain),
            other => bail!("unexpected reply to Share: {other:?}"),
        }
    }

    /// Poll the change feed for deltas since `cursor`. Returns `(changes, new_cursor)`.
    pub async fn poll(
        &self,
        cursor: Option<u64>,
        limit: u32,
    ) -> Result<(Vec<ce_drive_serve::Change>, u64)> {
        match self.call(DriveOp::Poll { cursor, limit }).await? {
            DriveOk::Changes { changes, new_cursor } => Ok((changes, new_cursor)),
            other => bail!("unexpected reply to Poll: {other:?}"),
        }
    }

    /// Get the change-beacon topic + current cursor (subscribe to be woken on changes).
    pub async fn watch(&self) -> Result<(String, u64)> {
        match self.call(DriveOp::Watch).await? {
            DriveOk::Watching { topic, cursor } => Ok((topic, cursor)),
            other => bail!("unexpected reply to Watch: {other:?}"),
        }
    }
}

/// The `Open` handshake result.
#[derive(Debug, Clone)]
pub struct Opened {
    pub drive_root_cid: String,
    pub server_seq: u64,
    pub granted_abilities: Vec<String>,
    pub quota: Quota,
}

/// The `Write` commit result.
#[derive(Debug, Clone)]
pub struct Written {
    pub etag: String,
    pub node_id: String,
    pub version_seq: u64,
}
