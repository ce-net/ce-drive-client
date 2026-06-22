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

    #[test]
    fn slice_offset_before_base_clamps_to_base() {
        // offset < base: saturating_sub yields rel_start 0 (we never index negative).
        let whole: Vec<u8> = (0..100u32).map(|i| i as u8).collect();
        let got = slice_range(whole.clone(), 1000, 500, Some(10));
        assert_eq!(got, &whole[0..10]);
    }

    #[test]
    fn slice_len_overruns_clamp_to_end() {
        let whole: Vec<u8> = (0..100u32).map(|i| i as u8).collect();
        // Ask for way more than available from rel_start.
        let got = slice_range(whole.clone(), 0, 90, Some(1000));
        assert_eq!(got, &whole[90..100]);
    }

    #[test]
    fn slice_zero_len_is_empty() {
        let whole = vec![7u8; 50];
        assert_eq!(slice_range(whole, 0, 10, Some(0)), Vec::<u8>::new());
    }

    #[test]
    fn slice_exact_base_no_len_returns_all() {
        let whole: Vec<u8> = (0..30u32).map(|i| i as u8).collect();
        assert_eq!(slice_range(whole.clone(), 100, 100, None), whole);
    }
}

#[cfg(test)]
mod proptests {
    use super::slice_range;
    use ce_rs::data;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(500))]

        /// `slice_range` never panics and never reads outside the buffer, for any inputs.
        #[test]
        fn slice_range_is_total_and_in_bounds(
            len_whole in 0usize..512,
            base in any::<u64>(),
            offset in any::<u64>(),
            req in proptest::option::of(any::<u64>()),
        ) {
            let whole: Vec<u8> = (0..len_whole).map(|i| i as u8).collect();
            let out = slice_range(whole, base, offset, req);
            // The result is always a sub-slice length: <= the original buffer.
            prop_assert!(out.len() <= len_whole);
            // If a definite len was requested, the result never exceeds it.
            if let Some(l) = req {
                prop_assert!(out.len() as u64 <= l);
            }
        }

        /// When the window lands fully inside the chunk-aligned buffer, the bytes returned are exactly
        /// the requested window of the original object (verifies the slice math, not just bounds).
        #[test]
        fn slice_range_returns_exact_window(
            base in 0u64..1000,
            extra in 0u64..200,        // offset above base, kept within the buffer
            want in 1u64..100,
        ) {
            // Build a buffer big enough to fully contain [offset, offset+want).
            let total = (extra + want + 10) as usize;
            let whole: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
            let offset = base + extra;
            let out = slice_range(whole.clone(), base, offset, Some(want));
            let rel = extra as usize;
            prop_assert_eq!(out.as_slice(), &whole[rel..rel + want as usize]);
        }
    }

    /// The CID-verification invariant `readplan` relies on: `data::cid(bytes)` is deterministic and
    /// distinguishes any tampering, so a host that returns wrong bytes for a chunk CID is always
    /// caught by `fetch_plan` (which compares `data::cid(got) == chunk.cid`).
    #[test]
    fn cid_detects_tampering() {
        let good = b"the original committed chunk bytes";
        let cid_good = data::cid(good);
        // Re-hashing identical bytes yields the same CID (the verification passes for honest bytes).
        assert_eq!(data::cid(good), cid_good);
        // A single flipped byte yields a different CID (tampering is always detected).
        let mut bad = good.to_vec();
        bad[0] ^= 0x01;
        assert_ne!(data::cid(&bad), cid_good, "tampered bytes must not match the committed CID");
    }
}
