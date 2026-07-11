//! Reorg detection and retraction (§7 of the design).
//!
//! Baseline safety is confirmation-depth trailing (the control loop never
//! emits blocks above `head - confirmation_depth`). This module is the
//! defense-in-depth layer: a rolling `(block, hash)` buffer re-checked
//! every iteration, retraction delete tasks on mismatch, and a halt
//! escalation for runaway reorgs.

use tracing::{error, warn};

use crate::config::ReorgConfig;
use crate::delete_scheduler::retraction_queries;
use crate::error::{BridgeError, Result};
use crate::metrics::METRICS;
use crate::rpc::EthClient;
use crate::state_store::StateStore;

/// Re-checks a sample of buffered block hashes against the chain. On a
/// mismatch, binary-searches the fork point and returns
/// `Err(ReorgDetected { fork_point })`.
pub async fn check_buffer(
    eth: &EthClient,
    store: &StateStore,
    config: &ReorgConfig,
    iteration: u64,
) -> Result<()> {
    let buffered = store.buffered_blocks()?;
    if buffered.is_empty() {
        return Ok(());
    }
    // Deterministic rotating sample: newest block always included (reorgs
    // touch the tip first), plus a window that shifts with the iteration
    // counter so the whole buffer gets covered over time.
    let mut sample_indexes = Vec::with_capacity(config.recheck_sample_size);
    sample_indexes.push(buffered.len() - 1);
    for sample_slot in 0..config.recheck_sample_size.saturating_sub(1) {
        let index = ((iteration as usize).wrapping_mul(7).wrapping_add(sample_slot * 13))
            % buffered.len();
        if !sample_indexes.contains(&index) {
            sample_indexes.push(index);
        }
    }
    for index in sample_indexes {
        let buffered_block = &buffered[index];
        let (chain_hash, _timestamp) = eth
            .block_hash_and_timestamp(buffered_block.block_number)
            .await?;
        if chain_hash != buffered_block.block_hash {
            warn!(
                block = buffered_block.block_number,
                buffered_hash = %buffered_block.block_hash,
                chain_hash = %chain_hash,
                "block hash mismatch, locating fork point"
            );
            let fork_point = find_fork_point(eth, store, buffered_block.block_number).await?;
            return Err(BridgeError::ReorgDetected { fork_point });
        }
    }
    Ok(())
}

/// Binary search for the highest buffered block whose hash still matches
/// the chain. `first_bad` is a block known to mismatch.
async fn find_fork_point(eth: &EthClient, store: &StateStore, first_bad: u64) -> Result<u64> {
    let buffered = store.buffered_blocks()?;
    let candidates: Vec<_> = buffered
        .into_iter()
        .filter(|block| block.block_number < first_bad)
        .collect();
    if candidates.is_empty() {
        // The entire buffer is suspect; fork point is below the oldest
        // buffered block. Report just below it — escalation depth checks
        // in `retract` decide whether that is survivable.
        return Ok(first_bad.saturating_sub(1).min(
            store
                .buffered_blocks()?
                .first()
                .map(|block| block.block_number.saturating_sub(1))
                .unwrap_or(0),
        ));
    }
    let mut low = 0usize;
    let mut high = candidates.len(); // exclusive; invariant: blocks below `low` match
    while low < high {
        let middle = (low + high) / 2;
        let candidate = &candidates[middle];
        let (chain_hash, _timestamp) = eth.block_hash_and_timestamp(candidate.block_number).await?;
        if chain_hash == candidate.block_hash {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if low == 0 {
        // Nothing in the buffer matches — fork is older than the buffer.
        Ok(candidates[0].block_number.saturating_sub(1))
    } else {
        Ok(candidates[low - 1].block_number)
    }
}

/// Executes the retraction protocol for a detected fork point: escalation
/// check, enqueue retraction delete tasks, roll the state store back, reset
/// the cursor. The caller resumes crawling.
pub fn retract(
    store: &StateStore,
    config: &ReorgConfig,
    max_delete_terms_per_task: usize,
    cursor_block: u64,
    fork_point: u64,
) -> Result<u64> {
    let depth = cursor_block.saturating_sub(fork_point);
    if depth > config.retract_max_depth {
        error!(depth, max_depth = config.retract_max_depth, "reorg too deep, halting");
        return Err(BridgeError::ReorgTooDeep {
            depth,
            max_depth: config.retract_max_depth,
        });
    }
    let retracted_keys = store.entity_keys_after(fork_point)?;
    for query in retraction_queries(&retracted_keys, fork_point, max_delete_terms_per_task) {
        store.enqueue_delete(&query)?;
    }
    store.truncate_reorg_buffer_after(fork_point)?;
    store.prune_emitted_after(fork_point)?;
    let new_cursor = fork_point + 1;
    store.set_cursor(new_cursor)?;

    METRICS.reorgs_observed.inc();
    if depth as i64 > METRICS.reorg_max_depth_observed.get() {
        METRICS.reorg_max_depth_observed.set(depth as i64);
    }
    error!(
        fork_point,
        depth,
        retracted_entities = retracted_keys.len(),
        "reorg retraction complete, resuming from fork point"
    );
    Ok(new_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retract_rolls_back_state_and_enqueues_deletes() {
        let store = StateStore::open_in_memory().unwrap();
        let config = ReorgConfig::default();
        for block in 1..=20u64 {
            store.observe_block(block, &format!("0x{block:x}")).unwrap();
        }
        store.record_emitted_doc("doc-1", "0xkey1", 15, "0xh", 4).unwrap();
        store.record_emitted_doc("doc-2", "0xkey2", 18, "0xh", 4).unwrap();
        store.set_cursor(21).unwrap();

        let new_cursor = retract(&store, &config, 500, 21, 12).unwrap();
        assert_eq!(new_cursor, 13);
        assert_eq!(store.cursor().unwrap(), Some(13));
        assert!(store.buffered_blocks().unwrap().iter().all(|block| block.block_number <= 12));
        assert!(store.entity_keys_after(0).unwrap().is_empty());
        let pending = store.pending_deletes().unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].query.contains("0xkey1"));
        assert!(pending[0].query.contains("0xkey2"));
        assert!(pending[0].query.contains("block_number:>12"));
    }

    #[test]
    fn retract_halts_on_excessive_depth() {
        let store = StateStore::open_in_memory().unwrap();
        let config = ReorgConfig {
            retract_max_depth: 10,
            ..ReorgConfig::default()
        };
        store.set_cursor(100).unwrap();
        let result = retract(&store, &config, 500, 100, 5);
        assert!(matches!(result, Err(BridgeError::ReorgTooDeep { .. })));
        // Cursor untouched on halt.
        assert_eq!(store.cursor().unwrap(), Some(100));
    }
}
