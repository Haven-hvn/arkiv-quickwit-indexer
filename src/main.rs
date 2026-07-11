//! `arkiv-quickwit-bridge` — external daemon that tails an arkiv-op-reth
//! node's `EntityOperation` stream and ingests it into a stock Quickwit
//! cluster over the public HTTP ingest / delete-task APIs. One binary,
//! one config file, one SQLite state store.

mod audit;
mod config;
mod control_loop;
mod delete_scheduler;
mod doc;
mod error;
mod extract;
mod metrics;
mod quickwit_client;
mod reorg;
mod rpc;
mod state_store;

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::BridgeConfig;
use crate::control_loop::ControlLoop;
use crate::quickwit_client::QuickwitClient;
use crate::rpc::{ArkivClient, EthClient, JsonRpcTransport};
use crate::state_store::StateStore;

#[derive(Parser)]
#[command(
    name = "arkiv-quickwit-bridge",
    about = "arkiv → Quickwit ingestion bridge"
)]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(long, env = "ARKIV_BRIDGE_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = BridgeConfig::load(&cli.config)?;
    info!(
        rpc_url = %config.arkiv.rpc_url,
        quickwit = %config.quickwit.base_url,
        index_id = %config.quickwit.index_id,
        "starting arkiv-quickwit-bridge"
    );

    if let Some(parent_dir) = config.bridge.state_store_path.parent() {
        std::fs::create_dir_all(parent_dir)?;
    }
    let store = StateStore::open(&config.bridge.state_store_path)?;

    let arkiv_timeout = Duration::from_secs(config.arkiv.http_timeout_secs);
    let primary_transport = JsonRpcTransport::new(&config.arkiv.rpc_url, arkiv_timeout)?;
    let schema = config.arkiv.schema;
    let arkiv_address = config.arkiv.resolve_address();
    info!(?schema, arkiv_address = %arkiv_address, "event schema resolved");
    let eth = EthClient::new(primary_transport.clone(), &arkiv_address, schema);
    let arkiv = ArkivClient::new(primary_transport, schema);
    let arkiv_backup = match &config.arkiv.rpc_url_backup {
        Some(backup_url) => {
            info!(backup_url = %backup_url, "paranoid mode: dual-hydration enabled");
            Some(ArkivClient::new(
                JsonRpcTransport::new(backup_url, arkiv_timeout)?,
                schema,
            ))
        }
        None => None,
    };
    let quickwit = QuickwitClient::new(&config.quickwit)?;

    // Fail fast if the node is on the wrong network.
    let node_chain_id = eth.chain_id().await?;
    if node_chain_id != config.arkiv.chain_id {
        anyhow::bail!(
            "chain id mismatch: node reports {node_chain_id}, config expects {}",
            config.arkiv.chain_id
        );
    }

    // Fail fast if the target index does not exist.
    quickwit.check_index_exists().await.map_err(|check_error| {
        anyhow::anyhow!(
            "quickwit index `{}` is not reachable ({check_error}). install the shipped index \
             config first: curl -XPOST {}/api/v1/indexes --data @index-config/arkiv-index.yaml \
             -H 'content-type: application/yaml'",
            config.quickwit.index_id,
            config.quickwit.base_url,
        )
    })?;

    let shutdown = CancellationToken::new();
    let metrics_addr = config.bridge.metrics_listen_addr.clone();
    let metrics_shutdown = shutdown.clone();
    let metrics_task = tokio::spawn(async move {
        if let Err(metrics_error) = crate::metrics::serve_metrics(&metrics_addr, metrics_shutdown).await
        {
            error!(%metrics_error, "metrics server failed");
        }
    });

    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("shutdown signal received");
        signal_shutdown.cancel();
    });

    let mut control_loop = ControlLoop {
        config,
        eth,
        arkiv,
        arkiv_backup,
        quickwit,
        store,
        shutdown: shutdown.clone(),
    };
    let run_result = control_loop.run().await;
    shutdown.cancel();
    if let Err(join_error) = metrics_task.await {
        error!(%join_error, "metrics task panicked");
    }

    match run_result {
        Ok(()) => {
            info!("bridge stopped cleanly");
            Ok(())
        }
        Err(bridge_error) => {
            error!(%bridge_error, "bridge halted");
            Err(bridge_error.into())
        }
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
