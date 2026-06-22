# ce-drive-client

The **consumer** side of the CE Drive mesh API. Open a remote drive `(host, drive_id)` over the CE
mesh by presenting a `ce-cap` chain, and call the `ce-drive/v1` op set — list, read, write, mkdir,
move, copy, delete, share, poll, watch — addressed by NodeId over libp2p (relay/NAT traversed),
never by stored ip:port.

## Two faces

- **`RemoteDrive`** — the typed request/reply client; each method is one `AppRequest`
  (`request(host, "ce-drive/v1", payload, timeout)`).
- **`Mirror`** — bootstraps a local [`ce-drive-core`](../ce-drive) `DriveTree` replica from the
  `Open` snapshot CID and keeps it live via `Poll` (+ the change beacon), so `ls` is served from the
  replica with zero RPC. This is `rdev watch` reimplemented over the Drive API.

Bulk bytes never travel on the metadata channel: `RemoteDrive::read` gets a `ReadPlan` of chunk CIDs
and fetches them directly from the content-addressed data layer, verifying each against its CID.

## CLI

```bash
# By claimed name (resolve_name) or raw --host <node-id-hex>:
ce-drive --name acme-eng --drive team --cap <hex-cap> open
ce-drive --name acme-eng --drive team --cap <hex-cap> ls /docs
ce-drive --name acme-eng --drive team --cap <hex-cap> cat /docs/spec.md
echo "hello" | ce-drive --host <id> --drive team --cap <hex> put /docs/note.txt
ce-drive --host <id> --drive team --cap <hex> mkdir /docs/new
ce-drive --host <id> --drive team --cap <hex> share /docs <peer-id> --can drive:read --expires 0
ce-drive --host <id> --drive team --cap <hex> mirror /docs
```

## Layering

`ce-drive-client → { ce-rs (AppRequest/blobs), ce-cap, ce-drive-core (replica), ce-drive-serve (wire) }`

See `PLAN/11-ce-drive-mesh-api.md` for the full design.
