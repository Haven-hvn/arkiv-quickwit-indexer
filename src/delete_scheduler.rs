//! Delete-task query construction and batched submission.
//!
//! Delete tasks are idempotent ("delete everything matching X" repeated is
//! a no-op), so the flow is: persist to `pending_deletes` first, submit,
//! then mark submitted. A crash anywhere in between re-submits harmlessly.

use crate::error::Result;
use crate::metrics::METRICS;
use crate::quickwit_client::QuickwitClient;
use crate::state_store::StateStore;

/// Retire the previous version of an upserted entity: everything for this
/// key strictly below the new block.
pub fn supersede_query(entity_key: &str, new_block: u64) -> String {
    format!("entity_key:{entity_key} AND block_number:[* TO {}]", new_block.saturating_sub(1))
}

/// Remove every doc for a deleted / expired entity.
pub fn tombstone_query(entity_key: &str) -> String {
    format!("entity_key:{entity_key}")
}

/// Reorg retraction: everything emitted for these keys above the fork
/// point. Keys are chunked to `max_terms` per task to respect query-length
/// limits (§15.6); each chunk gets its own persisted task.
pub fn retraction_queries(
    entity_keys: &[String],
    fork_point: u64,
    max_terms: usize,
) -> Vec<String> {
    let mut queries = Vec::with_capacity(entity_keys.len().div_ceil(max_terms));
    for chunk in entity_keys.chunks(max_terms) {
        let keys_clause = chunk
            .iter()
            .map(|key| format!("entity_key:{key}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        queries.push(format!("({keys_clause}) AND block_number:>{fork_point}"));
    }
    queries
}

// Dedup design note (§6.3 deviation): Quickwit delete tasks remove *every*
// doc matching the query, so "delete _doc_id:X to drop duplicates" would
// also drop the copy we want to keep. The bridge instead re-emits with the
// same deterministic `_doc_id` and consumers group on it; re-emissions only
// happen on a crash between the ingest POST and the cursor commit, so
// duplicates are rare and bounded to one block's worth of docs.

/// Drains the `pending_deletes` queue into Quickwit. Errors abort the
/// drain (remaining rows stay pending and retry next iteration).
pub async fn drain_pending_deletes(
    store: &StateStore,
    quickwit: &QuickwitClient,
) -> Result<usize> {
    let pending = store.pending_deletes()?;
    let mut submitted_count = 0;
    for pending_delete in &pending {
        quickwit.create_delete_task(&pending_delete.query).await?;
        store.mark_delete_submitted(pending_delete.id)?;
        METRICS.deletes_submitted.inc();
        submitted_count += 1;
    }
    if submitted_count > 0 {
        store.prune_submitted_deletes()?;
    }
    Ok(submitted_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0x1122334455667788990011223344556677889900aabbccddeeff001122334455";

    #[test]
    fn supersede_targets_strictly_older_blocks() {
        let query = supersede_query(KEY, 100);
        assert_eq!(query, format!("entity_key:{KEY} AND block_number:[* TO 99]"));
    }

    #[test]
    fn tombstone_targets_all_versions() {
        assert_eq!(tombstone_query(KEY), format!("entity_key:{KEY}"));
    }

    #[test]
    fn retraction_chunks_respect_max_terms() {
        let keys: Vec<String> = (0..7).map(|key_index| format!("0xkey{key_index}")).collect();
        let queries = retraction_queries(&keys, 50, 3);
        assert_eq!(queries.len(), 3);
        assert!(queries[0].contains("0xkey0 OR entity_key:0xkey1 OR entity_key:0xkey2"));
        assert!(queries.iter().all(|query| query.ends_with("AND block_number:>50")));
    }

    #[test]
    fn retraction_of_empty_set_is_empty() {
        assert!(retraction_queries(&[], 50, 500).is_empty());
    }
}
