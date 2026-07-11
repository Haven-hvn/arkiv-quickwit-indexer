//! The core loop (§6.2): fetch confirmed logs → verify hashes → hydrate →
//! batch → ingest → schedule deletes → advance cursor. One block is one
//! atomic unit of progress; the cursor never advances past a block whose
//! docs were not accepted by Quickwit.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::audit::run_audit_pass;
use crate::config::{BridgeConfig, StartBlock};
use crate::delete_scheduler::{drain_pending_deletes, supersede_query, tombstone_query};
use crate::doc::{
    OP_CREATE, OP_DELETE, OP_EXPIRE, QuickwitDoc, entity_matches_filters, op_type_name,
    to_quickwit_doc,
};
use crate::error::{BridgeError, Result};
use crate::metrics::METRICS;
use crate::quickwit_client::QuickwitClient;
use crate::rpc::{ArkivClient, EthClient, EthLog};
use crate::state_store::StateStore;

pub struct ControlLoop {
    pub config: BridgeConfig,
    pub eth: EthClient,
    pub arkiv: ArkivClient,
    /// Present in paranoid mode (§9.3): every entity hydrates twice.
    pub arkiv_backup: Option<ArkivClient>,
    pub quickwit: QuickwitClient,
    pub store: StateStore,
    pub shutdown: CancellationToken,
}

impl ControlLoop {
    /// Runs until shutdown or an unrecoverable error (deep reorg, audit
    /// failure). Transient RPC/HTTP errors are logged and retried on the
    /// next poll interval; the cursor guarantees no gaps.
    pub async fn run(&mut self) -> Result<()> {
        let mut iteration: u64 = 0;
        let mut last_audit_block: u64 = 0;
        loop {
            if self.shutdown.is_cancelled() {
                info!("shutdown requested, stopping control loop");
                return Ok(());
            }
            iteration += 1;
            match self.run_iteration(iteration, &mut last_audit_block).await {
                Ok(made_progress) => {
                    if !made_progress {
                        self.sleep_poll_interval().await;
                    }
                }
                Err(BridgeError::ReorgDetected { fork_point }) => {
                    // A reorg can only be detected after at least one
                    // iteration set the cursor; 0 is a defensive fallback.
                    let cursor = self.store.cursor()?.unwrap_or(0);
                    crate::reorg::retract(
                        &self.store,
                        &self.config.bridge.reorg,
                        self.config.bridge.max_delete_terms_per_task,
                        cursor,
                        fork_point,
                    )?;
                    // Push retraction deletes out before re-crawling.
                    drain_pending_deletes(&self.store, &self.quickwit).await?;
                }
                Err(error @ (BridgeError::ReorgTooDeep { .. } | BridgeError::AuditMismatch { .. })) => {
                    // Unrecoverable by design: operator intervention required.
                    return Err(error);
                }
                Err(error) => {
                    warn!(%error, "iteration failed, retrying after poll interval");
                    self.sleep_poll_interval().await;
                }
            }
        }
    }

    /// One fetch-hydrate-emit pass. Returns whether it processed blocks.
    async fn run_iteration(&self, iteration: u64, last_audit_block: &mut u64) -> Result<bool> {
        // Flush any deletes left over from a crash before new work.
        drain_pending_deletes(&self.store, &self.quickwit).await?;

        let head = self.eth.block_number().await?;
        METRICS.head_block.set(head as i64);
        let target = head.saturating_sub(self.config.arkiv.confirmation_depth);
        let cursor = match self.store.cursor()? {
            Some(cursor) => cursor,
            None => {
                // First launch only: resolve the configured start. Restarts
                // resume from the persisted cursor, so downtime has no gap.
                let start = match self.config.arkiv.start_block {
                    StartBlock::Genesis => 0,
                    StartBlock::Number(block_number) => block_number,
                    // Tail-only mode: no backfill, no historical state
                    // reads. Events from the current height onward are
                    // emitted once they clear the confirmation depth.
                    StartBlock::Head => {
                        info!(head, "tail-only mode: starting at current chain height");
                        head
                    }
                };
                self.store.set_cursor(start)?;
                start
            }
        };
        METRICS.cursor_block.set(cursor as i64);
        METRICS.lag_blocks.set(head.saturating_sub(cursor) as i64);
        if cursor > target {
            return Ok(false);
        }

        // Defense in depth: re-check buffered hashes before emitting more.
        crate::reorg::check_buffer(&self.eth, &self.store, &self.config.bridge.reorg, iteration)
            .await?;

        let end_block = (cursor + self.config.arkiv.fetch_batch_size_blocks - 1).min(target);
        let logs = self.eth.entity_operation_logs(cursor, end_block).await?;
        METRICS.logs_fetched.inc_by(logs.len() as u64);

        // Verify each touched block's hash against the reorg buffer.
        let mut seen_blocks: HashMap<u64, String> = HashMap::new();
        for log in &logs {
            seen_blocks
                .entry(log.block_number)
                .or_insert_with(|| log.block_hash.clone());
        }
        for (block_number, block_hash) in &seen_blocks {
            if let Some(stored_hash) = self.store.observe_block(*block_number, block_hash)? {
                warn!(
                    block = block_number,
                    stored = %stored_hash,
                    observed = %block_hash,
                    "reorg buffer mismatch while ingesting"
                );
                return Err(BridgeError::ReorgDetected {
                    fork_point: block_number.saturating_sub(1),
                });
            }
        }
        // Blocks without logs still advance the buffer at a coarse grain:
        // record the range end so the buffer tracks chain height. (Hash
        // checks for empty blocks happen lazily via check_buffer sampling.)
        if !seen_blocks.contains_key(&end_block) {
            let (end_hash, _timestamp) = self.eth.block_hash_and_timestamp(end_block).await?;
            self.store.observe_block(end_block, &end_hash)?;
        }

        // Group logs by block; emit block-by-block so the cursor is exact.
        let mut logs_by_block: Vec<(u64, Vec<&EthLog>)> = Vec::new();
        for log in &logs {
            match logs_by_block.last_mut() {
                Some((block_number, block_logs)) if *block_number == log.block_number => {
                    block_logs.push(log);
                }
                _ => logs_by_block.push((log.block_number, vec![log])),
            }
        }

        let mut block_timestamps: HashMap<u64, u64> = HashMap::new();
        for (block_number, block_logs) in &logs_by_block {
            let timestamp = match block_timestamps.get(block_number) {
                Some(cached) => *cached,
                None => {
                    let (_hash, timestamp) = self.eth.block_hash_and_timestamp(*block_number).await?;
                    block_timestamps.insert(*block_number, timestamp);
                    timestamp
                }
            };
            self.process_block(*block_number, timestamp, block_logs).await?;
            self.store.set_cursor(block_number + 1)?;
            METRICS.cursor_block.set((block_number + 1) as i64);
        }
        // No logs in the whole range → jump the cursor over it.
        self.store.set_cursor(end_block + 1)?;
        METRICS.cursor_block.set((end_block + 1) as i64);

        self.store.prune_reorg_buffer(self.config.bridge.reorg.buffer_size)?;
        self.store
            .prune_emitted_before(end_block.saturating_sub(self.config.bridge.dedup_window_blocks))?;

        // Periodic sampling audit (§9.2).
        let audit = &self.config.bridge.audit;
        if audit.enabled && end_block.saturating_sub(*last_audit_block) >= audit.sample_every_n_blocks
        {
            run_audit_pass(&self.eth, &self.arkiv, &self.store, audit, end_block).await?;
            *last_audit_block = end_block;
        }
        Ok(true)
    }

    /// Emits one block's logs: hydrate mutations, build docs, POST the
    /// batch(es), then enqueue + submit delete tasks. Idempotent on replay
    /// (same `_doc_id`s, idempotent delete queries).
    async fn process_block(
        &self,
        block_number: u64,
        block_timestamp: u64,
        block_logs: &[&EthLog],
    ) -> Result<()> {
        let mut docs: Vec<QuickwitDoc> = Vec::with_capacity(block_logs.len());
        let mut delete_queries: Vec<String> = Vec::new();

        for log in block_logs {
            match log.operation_type {
                OP_DELETE | OP_EXPIRE => {
                    delete_queries.push(tombstone_query(&log.entity_key));
                }
                _mutation => {
                    let hydration_started = Instant::now();
                    let entity = match self.hydrate(&log.entity_key, block_number).await {
                        Ok(entity) => entity,
                        Err(hydration_error) => {
                            // Nodes are not expected to retain state history
                            // for entities that have since expired or been
                            // deleted (retention/BTL). If the entity is dead
                            // at head, the historical state is legitimately
                            // gone: skip this doc — its content can never be
                            // indexed or hydrated by anyone, so a pointer
                            // would be useless. If it is alive at head, the
                            // failure is a real node problem: propagate and
                            // retry the block.
                            if self.arkiv.is_live_at_head(&log.entity_key).await? {
                                return Err(hydration_error);
                            }
                            METRICS
                                .docs_skipped_unretained
                                .with_label_values(&[op_type_name(log.operation_type)])
                                .inc();
                            info!(
                                entity_key = %log.entity_key,
                                block = block_number,
                                %hydration_error,
                                "skipping doc: state history not retained and entity dead at head"
                            );
                            continue;
                        }
                    };
                    METRICS
                        .hydration_latency
                        .observe(hydration_started.elapsed().as_secs_f64());
                    let Some(entity) = entity else {
                        // Raced a same-block delete: the delete log follows
                        // in this same block and will emit the tombstone.
                        continue;
                    };
                    if !entity_matches_filters(&self.config.entity_filters, &entity) {
                        METRICS
                            .docs_skipped_filtered
                            .with_label_values(&[op_type_name(log.operation_type)])
                            .inc();
                        // An update can move an entity OUT of the filtered
                        // set (annotation removed/changed): retire any
                        // previously indexed versions.
                        if log.operation_type != OP_CREATE {
                            delete_queries.push(tombstone_query(&log.entity_key));
                        }
                        continue;
                    }
                    let doc = to_quickwit_doc(
                        &self.config.payload_extractors,
                        log,
                        &entity,
                        block_timestamp,
                    )?;
                    if log.operation_type != OP_CREATE {
                        delete_queries.push(supersede_query(&log.entity_key, block_number));
                    }
                    let payload_len = entity
                        .value
                        .as_deref()
                        .map(|payload_hex| {
                            payload_hex.strip_prefix("0x").unwrap_or(payload_hex).len() / 2
                        })
                        .unwrap_or(0);
                    let emitted_count = self.store.record_emitted_doc(
                        &doc._doc_id,
                        &log.entity_key,
                        block_number,
                        &doc.code_hash,
                        payload_len as u64,
                    )?;
                    if emitted_count > 1 {
                        info!(doc_id = %doc._doc_id, "re-emitting doc after crash replay");
                    }
                    METRICS
                        .docs_emitted
                        .with_label_values(&[op_type_name(log.operation_type)])
                        .inc();
                    docs.push(doc);
                }
            }
        }

        // Ingest before deletes: a superseding delete for block N only makes
        // sense once the block-N doc is on its way in.
        for batch in self.split_batches(&docs)? {
            METRICS
                .ingest_batch_size_docs
                .observe(batch.lines().count() as f64);
            self.quickwit.ingest(batch).await?;
        }
        for query in delete_queries {
            self.store.enqueue_delete(&query)?;
        }
        drain_pending_deletes(&self.store, &self.quickwit).await?;
        Ok(())
    }

    /// Hydrates one entity, cross-checking against the backup RPC in
    /// paranoid mode. A discrepancy is unrecoverable (lying node).
    async fn hydrate(
        &self,
        entity_key: &str,
        block_number: u64,
    ) -> Result<Option<crate::rpc::EntityData>> {
        let primary = self.arkiv.get_entity_at(entity_key, block_number).await?;
        if let Some(backup_client) = &self.arkiv_backup {
            let backup = backup_client.get_entity_at(entity_key, block_number).await?;
            let primary_payload = primary.as_ref().and_then(|entity| entity.value.clone());
            let backup_payload = backup.as_ref().and_then(|entity| entity.value.clone());
            if primary_payload != backup_payload {
                return Err(BridgeError::AuditMismatch {
                    entity_key: entity_key.to_string(),
                    block_number,
                });
            }
        }
        Ok(primary)
    }

    /// Splits docs into ND-JSON payloads bounded by both doc count and
    /// byte size (§6.1 Batcher).
    fn split_batches(&self, docs: &[QuickwitDoc]) -> Result<Vec<String>> {
        let max_docs = self.config.quickwit.ingest_batch_size_docs;
        let max_bytes = self.config.quickwit.ingest_batch_max_bytes;
        let mut batches = Vec::new();
        let mut current = String::new();
        let mut current_docs = 0usize;
        for doc in docs {
            let line = serde_json::to_string(doc)?;
            let line_len = line.len() + 1;
            if current_docs > 0 && (current_docs >= max_docs || current.len() + line_len > max_bytes)
            {
                batches.push(std::mem::take(&mut current));
                current_docs = 0;
            }
            current.push_str(&line);
            current.push('\n');
            current_docs += 1;
        }
        if !current.is_empty() {
            batches.push(current);
        }
        Ok(batches)
    }

    async fn sleep_poll_interval(&self) {
        let interval = Duration::from_millis(self.config.bridge.poll_interval_ms);
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = self.shutdown.cancelled() => {}
        }
    }
}
