//! CLI demo: host a drive on one in-process CE node and access it from another by capability.
//!
//! Boots two in-process CE nodes wired over the mesh, runs a [`DriveServer`] against node B (the
//! host = the capability root), has B self-issue a `drive:write` capability to A, then drives the
//! full op set from A: open, mkdir, write, list, read (ranged + full), and a mirror bootstrap +
//! sync. Run with:
//!
//! ```bash
//! cargo run --example host_and_access -p ce-drive-client
//! ```
//!
//! This is the same shape as the production path (host = `ce-drive-serve`, client =
//! `ce-drive`), collapsed into one process so it needs no external node.

use std::time::Duration;

use anyhow::Result;
use ce_cap::{Caveats, Resource, SignedCapability, encode_chain};
use ce_drive_client::{Mirror, RemoteDrive};
use ce_drive_serve::{DriveServer, Quota, Registry};
use ce_identity::Identity;
use ce_node::{Node, NodeConfig};
use ce_rs::CeClient;
use tokio::time::sleep;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    unsafe { std::env::set_var("CE_API_TOKEN", "ce-drive-demo-token") };

    // ----- boot two nodes -----
    let id_a = Identity::load_or_generate(&dir_a.path().join("identity"))?;
    let peer_a = ce_node::peer_id_from_identity(&id_a)?;
    let node_a = Node::start(NodeConfig {
        listen_port: 14_990,
        data_dir: dir_a.path().to_path_buf(),
        api_port: 18_990,
        mine: false,
        disable_local_discovery: true,
        ephemeral: true,
        ..Default::default()
    })
    .await?;
    let _ = &node_a;
    sleep(Duration::from_millis(600)).await;

    let bootstrap = format!("/ip4/127.0.0.1/tcp/14990/p2p/{peer_a}");
    let node_b = Node::start(NodeConfig {
        listen_port: 14_991,
        bootstrap_peers: vec![bootstrap],
        data_dir: dir_b.path().to_path_buf(),
        api_port: 18_991,
        mine: false,
        disable_local_discovery: true,
        ephemeral: true,
        ..Default::default()
    })
    .await?;
    let _ = &node_b;

    // ----- node B hosts a drive (B = the capability root) -----
    let b_key_dir = dir_b.path().join("identity");
    let b_identity = Identity::load_or_generate(&b_key_dir)?;
    let b_id = b_identity.node_id();
    let a_id = id_a.node_id();

    let b_client = CeClient::new("http://127.0.0.1:18991");
    let mut registry = Registry::new(&b_key_dir)?;
    registry.create("team", Quota::default())?;
    let server = DriveServer::new(b_client.clone(), registry, &b_key_dir, Vec::new())?;
    let handle = tokio::spawn(async move { server.run(100).await });

    // ----- B grants A drive:write on the whole drive -----
    let cap = SignedCapability::issue(
        &b_identity,
        a_id,
        vec!["drive:read".into(), "drive:write".into(), "drive:admin".into()],
        Resource::Any,
        Caveats { path_prefix: Some("/".into()), ..Default::default() },
        1,
        None,
    );
    let cap_token = encode_chain(&[cap]);

    // ----- node A accesses the drive by capability -----
    let a_client = CeClient::new("http://127.0.0.1:18990");
    let remote = RemoteDrive::new(a_client, &hex::encode(b_id), "team", &cap_token);

    // Wait for the mesh.
    let mut ready = false;
    for _ in 0..30 {
        if remote.open().await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    if !ready {
        println!("mesh did not converge (sandboxed network?); skipping demo");
        handle.abort();
        return Ok(());
    }

    let opened = remote.open().await?;
    println!("opened drive 'team' on {}", &hex::encode(b_id)[..16]);
    println!("  snapshot_cid = {}", opened.drive_root_cid);
    println!("  abilities    = {}", opened.granted_abilities.join(", "));

    remote.mkdir("/docs").await?;
    let payload: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
    let w = remote.write("/docs/report.bin", &payload, None).await?;
    println!("wrote /docs/report.bin ({} bytes), etag={}", payload.len(), w.etag);

    for e in remote.list_all("/docs").await? {
        println!("  ls /docs -> {} ({} bytes)", e.path, e.size);
    }

    let full = remote.read_all("/docs/report.bin").await?;
    println!("read back {} bytes; matches = {}", full.len(), full == payload);
    let ranged = remote.read("/docs/report.bin", 1000, Some(16)).await?;
    println!("ranged read [1000,1016) = {} bytes; matches = {}", ranged.len(), ranged == payload[1000..1016]);

    // Mirror: local replica + sync forward over Poll.
    let mut mirror = Mirror::bootstrap(remote.clone()).await?;
    println!("mirror bootstrapped at cursor {}", mirror.cursor());
    remote.mkdir("/docs/archive").await?;
    let applied = mirror.sync().await?;
    println!("mirror synced {applied} change(s); /docs now has:");
    for e in mirror.ls("/docs")? {
        println!("  - {}", e.name);
    }

    handle.abort();
    println!("demo complete");
    Ok(())
}
