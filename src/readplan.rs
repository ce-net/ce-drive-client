//! Ranged chunk fetch + reassemble for a [`ReadPlan`].
//!
//! A `Read` returns a plan (chunk CIDs), not bytes. The client fetches those chunks directly from
//! the content-addressed data layer (`get_blob`) and verifies each against its CID before use —
//! content addressing IS the integrity proof, so a lying host can never serve bytes the publisher
//! didn't commit. Ranged reads fetch only the intersecting chunks.

use anyhow::{Result, anyhow};
use ce_drive_serve::ReadPlan;
use ce_rs::{CeClient, data};

/// Fetch all chunks named by `plan` and reassemble the covered byte range. Each chunk is verified
/// against its CID; a mismatch aborts. Returns the concatenated bytes of the plan's chunks (the
/// caller slices to the exact sub-range it asked for, since chunks are whole).
pub async fn fetch_plan(client: &CeClient, plan: &ReadPlan) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(plan.chunks.iter().map(|c| c.len as usize).sum());
    for chunk in &plan.chunks {
        let bytes = client.get_blob(&chunk.cid).await?;
        let got = data::cid(&bytes);
        if got != chunk.cid {
            return Err(anyhow!("chunk verification failed: expected {}, got {got}", chunk.cid));
        }
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

/// Fetch a plan and return exactly the sub-range `[offset, offset+len)` of the object (trimming the
/// chunk-aligned bytes from [`fetch_plan`]). `plan.chunks[0].offset` is the absolute object offset
/// of the first returned chunk, so we slice relative to it.
pub async fn fetch_range(
    client: &CeClient,
    plan: &ReadPlan,
    offset: u64,
    len: Option<u64>,
) -> Result<Vec<u8>> {
    let whole = fetch_plan(client, plan).await?;
    let base = plan.chunks.first().map(|c| c.offset).unwrap_or(0);
    Ok(slice_range(whole, base, offset, len))
}

/// Slice the chunk-aligned bytes (whose first byte is object-offset `base`) down to the exact
/// requested `[offset, offset+len)` window. Pure — factored out so it is unit-testable.
fn slice_range(whole: Vec<u8>, base: u64, offset: u64, len: Option<u64>) -> Vec<u8> {
    let rel_start = offset.saturating_sub(base) as usize;
    if rel_start >= whole.len() {
        return Vec::new();
    }
    let end = match len {
        Some(l) => (rel_start + l as usize).min(whole.len()),
        None => whole.len(),
    };
    whole[rel_start..end].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_trims_to_exact_window() {
        // Chunk-aligned bytes start at object offset 1000 and span [1000, 2000).
        let whole: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        // Ask for [1500, 1600): relative [500, 600).
        let got = slice_range(whole.clone(), 1000, 1500, Some(100));
        assert_eq!(got.len(), 100);
        assert_eq!(got[0], whole[500]);
        // No len = to end.
        let rest = slice_range(whole.clone(), 1000, 1500, None);
        assert_eq!(rest.len(), 500);
    }

    #[test]
    fn slice_past_end_is_empty() {
        let whole = vec![0u8; 100];
        assert!(slice_range(whole, 1000, 5000, Some(10)).is_empty());
    }
}
