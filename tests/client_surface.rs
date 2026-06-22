//! Pure-surface tests for `ce-drive-client` that need no live node: handle construction, the
//! re-exported wire vocabulary, and the request encoding the client puts on the wire (a `DriveReq`
//! the host's `decode_req` accepts). The full request/reply path is covered by the live two-node
//! test (`two_node_live`).

use ce_drive_client::{DriveErr, Entry, EntryKind, RemoteDrive, ShareCaveats};
use ce_drive_serve::{DriveOp, DriveReq, decode_req, encode_req};
use ce_rs::CeClient;

#[test]
fn remote_drive_construction_exposes_host() {
    let client = CeClient::new("http://127.0.0.1:1");
    let host = "ab".repeat(32);
    let remote = RemoteDrive::new(client, &host, "team", "deadbeef");
    assert_eq!(remote.host(), host);
}

#[test]
fn with_timeout_ms_is_chainable() {
    let client = CeClient::new("http://127.0.0.1:1");
    let remote = RemoteDrive::new(client, "abcd", "team", "cap").with_timeout_ms(42);
    // The handle is still usable (host unchanged); timeout is internal but the builder returns Self.
    assert_eq!(remote.host(), "abcd");
}

/// The client encodes each op as a `DriveReq` the host can decode — assert the exact wire contract
/// for a representative op the client builds in `call`.
#[test]
fn client_request_encoding_is_host_decodable() {
    let req = DriveReq {
        drive: "team".into(),
        cap: "deadbeef".into(),
        op: DriveOp::Write {
            path: "/docs/f.bin".into(),
            object_cid: "cid".into(),
            size: u64::MAX,
            base_etag: Some("etag".into()),
        },
    };
    let bytes = encode_req(&req);
    let decoded = decode_req(&bytes).expect("host decodes the client's request");
    assert_eq!(decoded.drive, "team");
    match decoded.op {
        DriveOp::Write { size, .. } => assert_eq!(size, u64::MAX),
        other => panic!("unexpected op {other:?}"),
    }
}

/// The wire vocabulary is re-exported so downstreams depend only on the client crate.
#[test]
fn reexported_wire_types_are_usable() {
    let e = Entry {
        path: "/x".into(),
        kind: EntryKind::File,
        size: 1,
        mtime_ms: 0,
        etag: "e".into(),
        node_id: "n".into(),
        object_cid: None,
        doc_id: None,
    };
    assert_eq!(e.kind, EntryKind::File);
    let _ = ShareCaveats::default();
    assert_eq!(DriveErr::NotFound.code(), 404);
}
