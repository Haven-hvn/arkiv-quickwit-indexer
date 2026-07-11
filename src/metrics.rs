//! Prometheus metrics (§13 of the design) plus the HTTP exporter.

use std::sync::LazyLock;

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

pub struct BridgeMetrics {
    pub registry: Registry,
    pub cursor_block: IntGauge,
    pub head_block: IntGauge,
    pub lag_blocks: IntGauge,
    pub logs_fetched: IntCounter,
    pub docs_emitted: IntCounterVec,
    pub docs_skipped_unretained: IntCounterVec,
    pub docs_skipped_filtered: IntCounterVec,
    pub deletes_submitted: IntCounter,
    pub reorgs_observed: IntCounter,
    pub reorg_max_depth_observed: IntGauge,
    pub audit_mismatch: IntCounter,
    pub quickwit_http_errors: IntCounterVec,
    pub arkiv_rpc_errors: IntCounterVec,
    pub hydration_latency: Histogram,
    pub ingest_batch_size_docs: Histogram,
}

pub static METRICS: LazyLock<BridgeMetrics> = LazyLock::new(|| {
    let registry = Registry::new();
    let cursor_block = IntGauge::with_opts(Opts::new(
        "arkiv_bridge_cursor_block",
        "current cursor position",
    ))
    .expect("valid metric");
    let head_block = IntGauge::with_opts(Opts::new(
        "arkiv_bridge_head_block",
        "rpc-reported head block",
    ))
    .expect("valid metric");
    let lag_blocks = IntGauge::with_opts(Opts::new(
        "arkiv_bridge_lag_blocks",
        "head minus cursor",
    ))
    .expect("valid metric");
    let logs_fetched = IntCounter::with_opts(Opts::new(
        "arkiv_bridge_logs_fetched_total",
        "cumulative EntityOperation logs fetched",
    ))
    .expect("valid metric");
    let docs_emitted = IntCounterVec::new(
        Opts::new("arkiv_bridge_docs_emitted_total", "docs POSTed to quickwit"),
        &["op_type"],
    )
    .expect("valid metric");
    let docs_skipped_unretained = IntCounterVec::new(
        Opts::new(
            "arkiv_bridge_docs_skipped_unretained_total",
            "docs skipped because state history was not retained (entity expired/deleted)",
        ),
        &["op_type"],
    )
    .expect("valid metric");
    let docs_skipped_filtered = IntCounterVec::new(
        Opts::new(
            "arkiv_bridge_docs_skipped_filtered_total",
            "docs skipped because the entity did not match entity_filters",
        ),
        &["op_type"],
    )
    .expect("valid metric");
    let deletes_submitted = IntCounter::with_opts(Opts::new(
        "arkiv_bridge_deletes_submitted_total",
        "delete tasks POSTed to quickwit",
    ))
    .expect("valid metric");
    let reorgs_observed = IntCounter::with_opts(Opts::new(
        "arkiv_bridge_reorgs_observed_total",
        "reorg retractions performed",
    ))
    .expect("valid metric");
    let reorg_max_depth_observed = IntGauge::with_opts(Opts::new(
        "arkiv_bridge_reorg_max_depth_observed",
        "largest reorg depth ever seen",
    ))
    .expect("valid metric");
    let audit_mismatch = IntCounter::with_opts(Opts::new(
        "arkiv_bridge_audit_mismatch_total",
        "sampling audit code-hash mismatches (should be zero)",
    ))
    .expect("valid metric");
    let quickwit_http_errors = IntCounterVec::new(
        Opts::new(
            "arkiv_bridge_quickwit_http_error_total",
            "non-2xx responses from quickwit",
        ),
        &["code"],
    )
    .expect("valid metric");
    let arkiv_rpc_errors = IntCounterVec::new(
        Opts::new(
            "arkiv_bridge_arkiv_rpc_error_total",
            "failed arkiv rpc calls",
        ),
        &["method"],
    )
    .expect("valid metric");
    let hydration_latency = Histogram::with_opts(HistogramOpts::new(
        "arkiv_bridge_hydration_latency_seconds",
        "per-log hydration latency",
    ))
    .expect("valid metric");
    let ingest_batch_size_docs = Histogram::with_opts(
        HistogramOpts::new(
            "arkiv_bridge_ingest_batch_size_docs",
            "docs per ingest batch",
        )
        .buckets(vec![1.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1000.0]),
    )
    .expect("valid metric");

    for collector in [
        Box::new(cursor_block.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(head_block.clone()),
        Box::new(lag_blocks.clone()),
        Box::new(logs_fetched.clone()),
        Box::new(docs_emitted.clone()),
        Box::new(docs_skipped_unretained.clone()),
        Box::new(docs_skipped_filtered.clone()),
        Box::new(deletes_submitted.clone()),
        Box::new(reorgs_observed.clone()),
        Box::new(reorg_max_depth_observed.clone()),
        Box::new(audit_mismatch.clone()),
        Box::new(quickwit_http_errors.clone()),
        Box::new(arkiv_rpc_errors.clone()),
        Box::new(hydration_latency.clone()),
        Box::new(ingest_batch_size_docs.clone()),
    ] {
        registry.register(collector).expect("unique metric names");
    }

    BridgeMetrics {
        registry,
        cursor_block,
        head_block,
        lag_blocks,
        logs_fetched,
        docs_emitted,
        docs_skipped_unretained,
        docs_skipped_filtered,
        deletes_submitted,
        reorgs_observed,
        reorg_max_depth_observed,
        audit_mismatch,
        quickwit_http_errors,
        arkiv_rpc_errors,
        hydration_latency,
        ingest_batch_size_docs,
    }
});

/// Serves `GET /metrics` in Prometheus text format until `shutdown` fires.
pub async fn serve_metrics(
    listen_addr: &str,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    use axum::Router;
    use axum::routing::get;

    let app = Router::new().route(
        "/metrics",
        get(|| async {
            let metric_families = METRICS.registry.gather();
            let mut buffer = Vec::new();
            let encoder = prometheus::TextEncoder::new();
            use prometheus::Encoder;
            if let Err(encode_error) = encoder.encode(&metric_families, &mut buffer) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("metrics encode error: {encode_error}"),
                );
            }
            (
                axum::http::StatusCode::OK,
                String::from_utf8_lossy(&buffer).into_owned(),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}
