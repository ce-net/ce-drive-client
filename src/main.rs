//! `ce-drive` — CLI to access a remote CE drive over the mesh by capability.
//!
//! Each subcommand is a thin wrapper over a `ce-drive/v1` request. The drive is addressed either by
//! a claimed name (`--name acme-eng`, resolved via `resolve_name`) or by raw host NodeId
//! (`--host <64-hex>`); a capability chain token (`--cap <hex>`) authorizes every request.

use anyhow::{Result, bail};
use ce_drive_client::{Mirror, RemoteDrive, ShareCaveats};
use ce_rs::CeClient;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "ce-drive", about = "Access a remote CE drive over the mesh by capability")]
struct Args {
    /// Local CE node HTTP API base URL.
    #[arg(long, default_value = ce_rs::DEFAULT_BASE_URL)]
    api: String,
    /// CE node API token (else discovered from $CE_API_TOKEN / data dir).
    #[arg(long)]
    token: Option<String>,
    /// Host NodeId hex (mutually exclusive with --name).
    #[arg(long)]
    host: Option<String>,
    /// Claimed drive name to resolve to a host NodeId.
    #[arg(long)]
    name: Option<String>,
    /// Drive id on the host.
    #[arg(long, default_value = "default")]
    drive: String,
    /// Capability chain token (hex), as minted by `ce grant` / a `share`.
    #[arg(long)]
    cap: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Handshake: print the snapshot CID, cursor, granted abilities, and quota.
    Open,
    /// List a directory.
    Ls { path: String },
    /// Print a file's bytes to stdout.
    Cat { path: String },
    /// Write stdin (or --text) to a file path.
    Put {
        path: String,
        /// Inline text to write (else read stdin).
        #[arg(long)]
        text: Option<String>,
    },
    /// Create a directory.
    Mkdir { path: String },
    /// Move/rename a node.
    Mv { from: String, to: String },
    /// Server-side copy.
    Cp { from: String, to: String },
    /// Delete a node.
    Rm {
        path: String,
        #[arg(long)]
        recursive: bool,
    },
    /// Mint an attenuated sub-capability for a peer.
    Share {
        path: String,
        /// Audience NodeId hex.
        audience: String,
        /// Abilities (e.g. drive:read drive:write).
        #[arg(long = "can", num_args = 1.., default_values_t = vec!["drive:read".to_string()])]
        can: Vec<String>,
        /// Expiry, unix seconds (0 = inherit).
        #[arg(long, default_value_t = 0)]
        expires: u64,
    },
    /// Poll the change feed once and print deltas.
    Poll {
        #[arg(long)]
        cursor: Option<u64>,
    },
    /// Bootstrap a local replica and sync once, printing the root listing.
    Mirror { path: Option<String> },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let args = Args::parse();
    let client = CeClient::with_token(&args.api, args.token.or_else(ce_rs::discover_api_token));

    let remote = match (&args.host, &args.name) {
        (Some(h), _) => RemoteDrive::new(client.clone(), h, &args.drive, &args.cap),
        (None, Some(n)) => {
            RemoteDrive::open_by_name(client.clone(), n, &args.drive, &args.cap).await?
        }
        (None, None) => bail!("provide --host <node-id-hex> or --name <claimed-name>"),
    };

    match args.cmd {
        Cmd::Open => {
            let o = remote.open().await?;
            println!("snapshot_cid {}", o.drive_root_cid);
            println!("server_seq   {}", o.server_seq);
            println!("abilities    {}", o.granted_abilities.join(", "));
            println!(
                "quota        storage={}/GiB-mo egress={}/GiB free_tier={}B channel_required={}",
                o.quota.price_per_gib_month,
                o.quota.price_per_gib_egress,
                o.quota.free_tier_bytes,
                o.quota.channel_required
            );
        }
        Cmd::Ls { path } => {
            let entries = remote.list_all(&path).await?;
            for e in entries {
                let kind = match e.kind {
                    ce_drive_client::EntryKind::Dir => "d",
                    ce_drive_client::EntryKind::File => "-",
                };
                println!("{kind} {:>10}  {}", e.size, e.path);
            }
        }
        Cmd::Cat { path } => {
            let bytes = remote.read_all(&path).await?;
            use std::io::Write;
            std::io::stdout().write_all(&bytes)?;
        }
        Cmd::Put { path, text } => {
            let bytes = match text {
                Some(t) => t.into_bytes(),
                None => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    std::io::stdin().read_to_end(&mut buf)?;
                    buf
                }
            };
            // Read current etag for optimistic concurrency (None on first write).
            let base_etag = remote.stat(&path).await.ok().map(|e| e.etag);
            let w = remote.write(&path, &bytes, base_etag).await?;
            println!("written etag={} node={} seq={}", w.etag, w.node_id, w.version_seq);
        }
        Cmd::Mkdir { path } => {
            let id = remote.mkdir(&path).await?;
            println!("made {id}");
        }
        Cmd::Mv { from, to } => {
            let id = remote.mv(&from, &to).await?;
            println!("moved {id}");
        }
        Cmd::Cp { from, to } => {
            let id = remote.cp(&from, &to).await?;
            println!("copied {id}");
        }
        Cmd::Rm { path, recursive } => {
            remote.rm(&path, recursive).await?;
            println!("deleted {path}");
        }
        Cmd::Share { path, audience, can, expires } => {
            let caveats = ShareCaveats { not_after: expires, ..Default::default() };
            let chain = remote.share(&path, &audience, can, caveats).await?;
            println!("{chain}");
        }
        Cmd::Poll { cursor } => {
            let (changes, new_cursor) = remote.poll(cursor, 256).await?;
            for c in &changes {
                println!("seq {} {:?} {} ({})", c.seq, c.kind, c.path, c.node_id);
            }
            println!("cursor {new_cursor}");
        }
        Cmd::Mirror { path } => {
            let mut m = Mirror::bootstrap(remote).await?;
            let n = m.sync().await?;
            let p = path.unwrap_or_else(|| "/".to_string());
            println!("synced {n} changes; listing {p} from local replica:");
            for e in m.ls(&p)? {
                let kind = if e.is_dir { "d" } else { "-" };
                println!("{kind} {}", e.name);
            }
        }
    }
    Ok(())
}
