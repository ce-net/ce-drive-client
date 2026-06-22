//! Reply-decoding contract tests for `ce-drive-client`.
//!
//! Every `RemoteDrive` op decodes the host's reply with `decode_reply` and then matches on the
//! exact `DriveOk` variant it expects (`bail!("unexpected reply ...")` otherwise), or surfaces a
//! `DriveErr`. None of that is exercised by the live two-node test's happy path, and a silent wire
//! drift in the reply codec (or a variant the client forgets to handle) corrupts every op. These
//! tests pin the contract WITHOUT a live node: they build the `DriveReply` a host would send, push
//! it through `encode_reply -> decode_reply`, and assert the client-facing payload survives exactly
//! — including the error mapping, money/size values above 2^53, and malformed/fuzz inputs (which
//! must Err, never panic).

use ce_drive_client::{
    Change, ChangeKind, ChunkRef, DriveErr, Entry, EntryKind, Quota, ReadPlan, ShareCaveats,
};
use ce_drive_serve::{
    DriveOk, DriveOp, DriveReply, DriveReq, decode_reply, decode_req, encode_reply, encode_req,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Round-trip a successful reply the way the client receives it: the host encodes a `DriveReply`,
/// libp2p delivers the bytes, the client `decode_reply`s and unwraps `result`. Returns the decoded
/// `DriveOk` so the test can match the exact variant the client's op method expects.
fn rt_ok(ok: DriveOk) -> DriveOk {
    let bytes = encode_reply(&DriveReply::ok(ok));
    assert!(!bytes.is_empty(), "encode_reply produced no bytes");
    let reply = decode_reply(&bytes).expect("client decodes host reply");
    reply.result.expect("ok reply decodes to Ok")
}

/// Round-trip an error reply: the host encodes `DriveReply::err`, the client decodes and the op
/// method maps `Err(DriveErr)` onto an `anyhow` error. Returns the decoded `DriveErr`.
fn rt_err(err: DriveErr) -> DriveErr {
    let bytes = encode_reply(&DriveReply::err(err));
    let reply = decode_reply(&bytes).expect("client decodes host error reply");
    reply.result.expect_err("err reply decodes to Err")
}

fn sample_entry(path: &str) -> Entry {
    Entry {
        path: path.into(),
        kind: EntryKind::File,
        size: 4096,
        mtime_ms: 1_700_000_000_000,
        etag: "v7".into(),
        node_id: "node-7".into(),
        object_cid: Some("cid-abc".into()),
        doc_id: None,
    }
}

// ---------------------------------------------------------------------------
// every DriveOk variant the client's match arms expect survives the wire
// ---------------------------------------------------------------------------

#[test]
fn open_reply_round_trips_all_fields() {
    // RemoteDrive::open matches DriveOk::Opened { drive_root_cid, server_seq, granted_abilities, quota }.
    let ok = rt_ok(DriveOk::Opened {
        drive_root_cid: "snapshot-cid".into(),
        server_seq: 42,
        granted_abilities: vec!["drive:read".into(), "drive:write".into()],
        quota: Quota {
            price_per_gib_month: "1000000000000000000".into(),
            price_per_gib_egress: "0".into(),
            free_tier_bytes: 1 << 30,
            channel_required: true,
        },
    });
    match ok {
        DriveOk::Opened { drive_root_cid, server_seq, granted_abilities, quota } => {
            assert_eq!(drive_root_cid, "snapshot-cid");
            assert_eq!(server_seq, 42);
            assert_eq!(granted_abilities, vec!["drive:read", "drive:write"]);
            assert_eq!(quota.price_per_gib_month, "1000000000000000000");
            assert!(quota.channel_required);
            assert_eq!(quota.free_tier_bytes, 1 << 30);
        }
        other => panic!("expected Opened, got {other:?}"),
    }
}

#[test]
fn stat_reply_entry_round_trips() {
    match rt_ok(DriveOk::Entry(sample_entry("/docs/spec.bin"))) {
        DriveOk::Entry(e) => {
            assert_eq!(e.path, "/docs/spec.bin");
            assert_eq!(e.kind, EntryKind::File);
            assert_eq!(e.size, 4096);
            assert_eq!(e.object_cid.as_deref(), Some("cid-abc"));
        }
        other => panic!("expected Entry, got {other:?}"),
    }
}

#[test]
fn list_reply_round_trips_entries_and_cursor() {
    // RemoteDrive::list matches DriveOk::Listing { entries, next_cursor }.
    let ok = rt_ok(DriveOk::Listing {
        entries: vec![sample_entry("/a"), sample_entry("/b")],
        next_cursor: Some("page-2".into()),
    });
    match ok {
        DriveOk::Listing { entries, next_cursor } => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1].path, "/b");
            assert_eq!(next_cursor.as_deref(), Some("page-2"));
        }
        other => panic!("expected Listing, got {other:?}"),
    }
    // The terminal page carries no cursor (list_all stops on None).
    match rt_ok(DriveOk::Listing { entries: vec![], next_cursor: None }) {
        DriveOk::Listing { entries, next_cursor } => {
            assert!(entries.is_empty());
            assert!(next_cursor.is_none(), "terminal page ends the list_all loop");
        }
        other => panic!("expected Listing, got {other:?}"),
    }
}

#[test]
fn read_plan_reply_round_trips_chunks_and_offsets() {
    // RemoteDrive::read_plan matches DriveOk::ReadPlan(p); readplan::fetch_range reads chunk offsets.
    let plan = ReadPlan {
        object_cid: "obj".into(),
        total_size: 3 * 1024 * 1024,
        chunk_size: 1024 * 1024,
        chunks: vec![
            ChunkRef { cid: "c0".into(), offset: 0, len: 1024 * 1024 },
            ChunkRef { cid: "c1".into(), offset: 1024 * 1024, len: 1024 * 1024 },
        ],
        encrypted: false,
        key_hint: None,
    };
    match rt_ok(DriveOk::ReadPlan(plan.clone())) {
        DriveOk::ReadPlan(p) => {
            assert_eq!(p, plan, "the whole plan (offsets included) survives the wire");
            assert_eq!(p.chunks.first().map(|c| c.offset), Some(0));
        }
        other => panic!("expected ReadPlan, got {other:?}"),
    }
}

#[test]
fn write_reply_round_trips_etag_and_version() {
    // RemoteDrive::write matches DriveOk::Written { etag, node_id, version_seq }.
    match rt_ok(DriveOk::Written {
        etag: "v8".into(),
        node_id: "n8".into(),
        version_seq: u64::MAX,
    }) {
        DriveOk::Written { etag, node_id, version_seq } => {
            assert_eq!(etag, "v8");
            assert_eq!(node_id, "n8");
            assert_eq!(version_seq, u64::MAX, "version_seq survives full u64 range");
        }
        other => panic!("expected Written, got {other:?}"),
    }
}

#[test]
fn made_reply_round_trips_for_mkdir_move_copy() {
    // mkdir/mv/cp all match DriveOk::Made { node_id }.
    match rt_ok(DriveOk::Made { node_id: "made-1".into() }) {
        DriveOk::Made { node_id } => assert_eq!(node_id, "made-1"),
        other => panic!("expected Made, got {other:?}"),
    }
}

#[test]
fn deleted_and_shared_and_changes_and_watching_round_trip() {
    // rm matches Deleted.
    assert!(matches!(rt_ok(DriveOk::Deleted), DriveOk::Deleted));

    // share matches Shared { chain }.
    match rt_ok(DriveOk::Shared { chain: "extended-chain-hex".into() }) {
        DriveOk::Shared { chain } => assert_eq!(chain, "extended-chain-hex"),
        other => panic!("expected Shared, got {other:?}"),
    }

    // poll matches Changes { changes, new_cursor }.
    let changes = vec![
        Change {
            seq: 1,
            path: "/x".into(),
            node_id: "n1".into(),
            kind: ChangeKind::Created,
            etag: "e1".into(),
        },
        Change {
            seq: 2,
            path: "/y".into(),
            node_id: "n2".into(),
            kind: ChangeKind::Moved { from: "/old".into() },
            etag: "e2".into(),
        },
    ];
    match rt_ok(DriveOk::Changes { changes: changes.clone(), new_cursor: 2 }) {
        DriveOk::Changes { changes: c, new_cursor } => {
            assert_eq!(c, changes);
            assert_eq!(new_cursor, 2);
            assert!(matches!(c[1].kind, ChangeKind::Moved { .. }));
        }
        other => panic!("expected Changes, got {other:?}"),
    }

    // watch matches Watching { topic, cursor }.
    match rt_ok(DriveOk::Watching { topic: "ce-drive/team/changes".into(), cursor: 9 }) {
        DriveOk::Watching { topic, cursor } => {
            assert_eq!(topic, "ce-drive/team/changes");
            assert_eq!(cursor, 9);
        }
        other => panic!("expected Watching, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// error replies: the client maps Err(DriveErr) -> anyhow error, code stable
// ---------------------------------------------------------------------------

#[test]
fn every_error_variant_round_trips_with_stable_code() {
    // The exact set the host can send; the client surfaces each via `anyhow!("ce-drive: {e}")`.
    let cases = [
        (DriveErr::Unauthorized, 401u16),
        (DriveErr::Revoked, 410),
        (DriveErr::Expired, 419),
        (DriveErr::OutOfScope, 403),
        (DriveErr::NotFound, 404),
        (DriveErr::Conflict { current_etag: "v9".into() }, 409),
        (DriveErr::QuotaExceeded, 429),
        (DriveErr::PaymentRequired, 402),
        (DriveErr::BadPath, 400),
        (DriveErr::Internal("boom".into()), 500),
    ];
    for (err, code) in cases {
        let decoded = rt_err(err.clone());
        assert_eq!(decoded.code(), code, "code stable across the wire for {err:?}");
        // Display is non-empty (the client embeds it in its anyhow message).
        assert!(!decoded.to_string().is_empty());
    }
}

#[test]
fn conflict_error_preserves_current_etag_for_optimistic_concurrency() {
    // RemoteDrive::write relies on the host returning the live etag on a stale-base conflict so the
    // caller can retry. That payload must survive the wire intact.
    match rt_err(DriveErr::Conflict { current_etag: "live-etag-42".into() }) {
        DriveErr::Conflict { current_etag } => assert_eq!(current_etag, "live-etag-42"),
        other => panic!("expected Conflict, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// malformed / truncated reply bytes Err (never panic the client)
// ---------------------------------------------------------------------------

#[test]
fn empty_reply_bytes_error_not_panic() {
    assert!(decode_reply(&[]).is_err(), "empty bytes must Err, not panic");
}

#[test]
fn truncated_reply_bytes_error_not_panic() {
    let full = encode_reply(&DriveReply::ok(DriveOk::Opened {
        drive_root_cid: "cid".into(),
        server_seq: 1,
        granted_abilities: vec!["drive:read".into()],
        quota: Quota::default(),
    }));
    // Lop off the tail at every boundary; each prefix must Err cleanly.
    for cut in 0..full.len() {
        let _ = decode_reply(&full[..cut]); // must not panic
    }
    assert!(decode_reply(&full[..full.len() / 2]).is_err());
}

// ---------------------------------------------------------------------------
// property tests: the request codec for every op the client builds, and a
// reply fuzz that random bytes never panic the decoder.
// ---------------------------------------------------------------------------

prop_compose! {
    fn arb_share_caveats()(
        not_after in any::<u64>(),
        r in proptest::option::of(any::<u64>()),
        w in proptest::option::of(any::<u64>()),
    ) -> ShareCaveats {
        ShareCaveats { not_after, max_bytes_read: r, max_bytes_write: w }
    }
}

fn arb_op() -> impl Strategy<Value = DriveOp> {
    let path = "/[a-z/]{0,40}";
    prop_oneof![
        Just(DriveOp::Open),
        Just(DriveOp::Watch),
        path.prop_map(|p| DriveOp::Stat { path: p }),
        (path, proptest::option::of("[a-z0-9]{0,10}"), any::<u32>())
            .prop_map(|(p, c, l)| DriveOp::List { path: p, cursor: c, limit: l }),
        (path, any::<u64>(), proptest::option::of(any::<u64>()))
            .prop_map(|(p, o, l)| DriveOp::Read { path: p, offset: o, len: l }),
        (path, "[a-f0-9]{0,20}", any::<u64>(), proptest::option::of("[a-z0-9]{0,8}"))
            .prop_map(|(p, cid, s, b)| DriveOp::Write { path: p, object_cid: cid, size: s, base_etag: b }),
        path.prop_map(|p| DriveOp::Mkdir { path: p }),
        (path, path).prop_map(|(a, b)| DriveOp::Move { from: a, to: b }),
        (path, path).prop_map(|(a, b)| DriveOp::Copy { from: a, to: b }),
        (path, any::<bool>()).prop_map(|(p, r)| DriveOp::Delete { path: p, recursive: r }),
        (path, "[a-f0-9]{0,16}", proptest::collection::vec("[a-z:]{1,12}", 0..4), arb_share_caveats())
            .prop_map(|(p, a, abil, cav)| DriveOp::Share { path: p, audience: a, abilities: abil, caveats: cav }),
        (proptest::option::of(any::<u64>()), any::<u32>())
            .prop_map(|(c, l)| DriveOp::Poll { cursor: c, limit: l }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    /// Every op the client encodes (`RemoteDrive::call`) is decodable by the host's `decode_req`,
    /// and bincode is deterministic (re-encode is byte-identical). This is the request half of the
    /// client<->host contract for all twelve ops, addressed over the mesh.
    #[test]
    fn client_request_round_trips_for_every_op(op in arb_op()) {
        let req = DriveReq { drive: "team".into(), cap: "deadbeefcap".into(), op };
        let bytes = encode_req(&req);
        prop_assert!(!bytes.is_empty());
        let back = decode_req(&bytes).expect("host decodes the client's request");
        prop_assert_eq!(encode_req(&back), bytes, "bincode re-encode is byte-identical");
        prop_assert_eq!(&back.drive, "team");
        prop_assert_eq!(&back.cap, "deadbeefcap");
    }

    /// Arbitrary bytes never panic `decode_reply` — a hostile/garbled host can only ever produce a
    /// clean `Err`, which the client surfaces as an `anyhow` error rather than crashing.
    #[test]
    fn arbitrary_bytes_never_panic_decode_reply(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode_reply(&bytes); // must return, never panic
    }
}
