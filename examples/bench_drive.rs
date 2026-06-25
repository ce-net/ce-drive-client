//! bench_drive — latency/throughput benchmark of the ce-drive mesh app over a real two-node mesh.
//!
//! Boots two in-process CE nodes wired over libp2p (same shape as `host_and_access`): node B hosts a
//! drive via `DriveServer`, node A drives the `ce-drive/v1` op set as a `RemoteDrive` by capability.
//! Every op is a real mesh request/reply between two distinct nodes. We time each op family and print
//! p50/p90/p99 + MB/s, so ce-drive numbers sit beside the primitive numbers from ce-bench.
//!
//!   cargo run --release --example bench_drive -p ce-drive-client      (or omit --release for a
//!   faster debug build when the cache is warm)
//!
//! Co-located nodes => ~0 network latency, so this isolates the APP + protocol + node overhead
//! (DriveTree CRDT, content-addressed chunking, authorize-on-every-op). Add real cross-node RTT from
//! the ce-bench primitive report on top.

use std::time::{Duration, Instant};

use anyhow::Result;
use ce_cap::{Caveats, Resource, SignedCapability, encode_chain};
use ce_drive_client::{Mirror, RemoteDrive};
use ce_drive_serve::{DriveServer, Quota, Registry};
use ce_identity::Identity;
use ce_node::{Node, NodeConfig};
use ce_rs::CeClient;
use tokio::time::sleep;

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (((p / 100.0) * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

struct Stat {
    n: usize,
    p50: f64,
    p90: f64,
    p99: f64,
    mean: f64,
    min: f64,
    max: f64,
}

fn stat(mut v: Vec<f64>) -> Stat {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    let sum: f64 = v.iter().sum();
    Stat {
        n,
        p50: pct(&v, 50.0),
        p90: pct(&v, 90.0),
        p99: pct(&v, 99.0),
        mean: if n > 0 { sum / n as f64 } else { 0.0 },
        min: *v.first().unwrap_or(&0.0),
        max: *v.last().unwrap_or(&0.0),
    }
}

/// Time `iters` runs of an async op, returning the per-op milliseconds.
async fn time_op<F, Fut>(iters: usize, mut f: F) -> Result<Vec<f64>>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut out = Vec::with_capacity(iters);
    for i in 0..iters {
        let t0 = Instant::now();
        f(i).await?;
        out.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(out)
}

fn row(label: &str, s: &Stat, bytes: Option<usize>) {
    let mbps = match bytes {
        Some(b) if s.p50 > 0.0 => format!("{:>8.1}", (b as f64 / (1024.0 * 1024.0)) / (s.p50 / 1000.0)),
        _ => "       -".to_string(),
    };
    println!(
        "{label:<22} n={:<3} p50={:>8.2} p90={:>8.2} p99={:>8.2} mean={:>8.2} MB/s={mbps}",
        s.n, s.p50, s.p90, s.p99, s.mean
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();

    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    unsafe { std::env::set_var("CE_API_TOKEN", "ce-drive-bench-token") };

    let id_a = Identity::load_or_generate(&dir_a.path().join("identity"))?;
    let peer_a = ce_node::peer_id_from_identity(&id_a)?;
    let node_a = Node::start(NodeConfig {
        listen_port: 15_990,
        data_dir: dir_a.path().to_path_buf(),
        api_port: 19_990,
        mine: false,
        disable_local_discovery: true,
        ephemeral: true,
        ..Default::default()
    })
    .await?;
    let _ = &node_a;
    sleep(Duration::from_millis(600)).await;

    let bootstrap = format!("/ip4/127.0.0.1/tcp/15990/p2p/{peer_a}");
    let node_b = Node::start(NodeConfig {
        listen_port: 15_991,
        bootstrap_peers: vec![bootstrap],
        data_dir: dir_b.path().to_path_buf(),
        api_port: 19_991,
        mine: false,
        disable_local_discovery: true,
        ephemeral: true,
        ..Default::default()
    })
    .await?;
    let _ = &node_b;

    let b_key_dir = dir_b.path().join("identity");
    let b_identity = Identity::load_or_generate(&b_key_dir)?;
    let b_id = b_identity.node_id();
    let a_id = id_a.node_id();

    let b_client = CeClient::new("http://127.0.0.1:19991");
    let mut registry = Registry::new(&b_key_dir)?;
    registry.create("team", Quota::default())?;
    let server = DriveServer::new(b_client.clone(), registry, &b_key_dir, Vec::new())?;
    let handle = tokio::spawn(async move { server.run(100).await });

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

    let a_client = CeClient::new("http://127.0.0.1:19990");
    let remote = RemoteDrive::new(a_client, &hex::encode(b_id), "team", &cap_token);

    // Wait for mesh convergence.
    let mut ready = false;
    for _ in 0..30 {
        if remote.open().await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }
    if !ready {
        println!("mesh did not converge (sandboxed network?); aborting bench");
        handle.abort();
        return Ok(());
    }

    println!("# ce-drive benchmark — two in-process CE nodes over libp2p mesh");
    println!("# host=B {} client=A {}", &hex::encode(b_id)[..12], &hex::encode(a_id)[..12]);
    println!();

    // --- open / handshake ---
    let s = stat(time_op(50, |_| { let r = remote.clone(); async move { r.open().await.map(|_| ()) } }).await?);
    row("open/handshake", &s, None);

    // --- mkdir ---
    let s = stat(time_op(40, |i| { let r = remote.clone(); async move { r.mkdir(&format!("/d{i}")).await.map(|_| ()) } }).await?);
    row("mkdir", &s, None);

    // --- write across sizes ---
    let kib = 1024usize;
    let mib = 1024 * kib;
    for &size in &[4 * kib, 64 * kib, 256 * kib, mib, 4 * mib] {
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let iters = if size >= mib { 12 } else { 25 };
        let s = stat(
            time_op(iters, |i| {
                let r = remote.clone();
                let p = payload.clone();
                async move { r.write(&format!("/w/{size}-{i}"), &p, None).await.map(|_| ()) }
            })
            .await?,
        );
        row(&format!("write {}", human(size)), &s, Some(size));
    }

    // --- read_all across sizes (write once, read repeatedly) ---
    for &size in &[4 * kib, 64 * kib, 256 * kib, mib, 4 * mib] {
        let payload: Vec<u8> = (0..size).map(|i| ((i * 7) % 251) as u8).collect();
        let path = format!("/r/{size}");
        remote.write(&path, &payload, None).await?;
        let iters = if size >= mib { 12 } else { 25 };
        let s = stat(time_op(iters, |_| { let r = remote.clone(); let p = path.clone(); async move { r.read_all(&p).await.map(|_| ()) } }).await?);
        row(&format!("read {}", human(size)), &s, Some(size));
    }

    // --- ranged read (64 KiB window out of a 4 MiB file) ---
    {
        let size = 4 * mib;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        remote.write("/ranged.bin", &payload, None).await?;
        let s = stat(time_op(30, |i| {
            let r = remote.clone();
            async move { r.read("/ranged.bin", ((i * 65536) % (size - 65536)) as u64, Some(65536u64)).await.map(|_| ()) }
        }).await?);
        row("read ranged 64KiB", &s, Some(64 * kib));
    }

    // --- list ---
    let s = stat(time_op(40, |_| { let r = remote.clone(); async move { r.list_all("/w").await.map(|_| ()) } }).await?);
    row("list_all /w", &s, None);

    // --- mirror bootstrap + sync-one-change ---
    {
        let t0 = Instant::now();
        let mut mirror = Mirror::bootstrap(remote.clone()).await?;
        let boot_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let mut syncs = Vec::new();
        for i in 0..20 {
            remote.mkdir(&format!("/sync{i}")).await?;
            let t = Instant::now();
            let _ = mirror.sync().await?;
            syncs.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let s = stat(syncs);
        println!("mirror bootstrap         = {boot_ms:.2} ms");
        row("mirror sync(1 change)", &s, None);
    }

    handle.abort();
    println!("\n# done");
    Ok(())
}

fn human(b: usize) -> String {
    let mib = 1024 * 1024;
    let kib = 1024;
    if b >= mib {
        format!("{}MiB", b / mib)
    } else {
        format!("{}KiB", b / kib)
    }
}
