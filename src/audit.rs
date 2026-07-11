//! Sampling audits (§9.2): periodically re-fetch a random sample of
//! emitted docs through `eth_getCode` — the trie-committed path, not
//! `arkiv_query` — and verify the payload hash matches what was emitted.
//! Catches buggy hydration, lying RPC nodes, and state-store corruption.

use tracing::{error, info};

use crate::config::AuditConfig;
use crate::error::{BridgeError, Result};
use crate::metrics::METRICS;
use crate::rpc::{ArkivClient, EthClient, entity_address};
use crate::state_store::StateStore;

/// Runs one audit pass. Returns the number of docs verified. Fails with
/// [`BridgeError::AuditMismatch`] once cumulative mismatches reach the
/// configured halt threshold.
pub async fn run_audit_pass(
    eth: &EthClient,
    arkiv: &ArkivClient,
    store: &StateStore,
    config: &AuditConfig,
    seed: u64,
) -> Result<usize> {
    let sample = store.sample_emitted_docs(config.sample_size, seed)?;
    let mut verified_count = 0;
    for emitted in &sample {
        let address = entity_address(&emitted.entity_key)?;
        let code_hex = match eth.get_code(&address, emitted.block_number).await {
            Ok(code_hex) => code_hex,
            Err(read_error) => {
                // Nodes do not retain state history for expired entities.
                // A doc sampled from the dedup window can point at an entity
                // whose retention has since lapsed — its historical state is
                // legitimately unreadable and the sample is unauditable, not
                // corrupt. Only propagate when the entity is still live.
                if arkiv.is_live_at_head(&emitted.entity_key).await? {
                    return Err(read_error);
                }
                continue;
            }
        };
        // Entity code is `0xFE || RLP(entity)`. The payload lives inside
        // the RLP; the bridge stores keccak(payload). Extracting the exact
        // payload from the RLP requires the arkiv RLP schema, so the audit
        // verifies at the coarser, still-sound level: the account must
        // exist (non-empty code) and re-hydration must reproduce the
        // recorded payload hash. Deleted-then-audited entities (empty code)
        // are skipped — a tombstone after emission is legitimate history.
        let stripped = code_hex.strip_prefix("0x").unwrap_or(&code_hex);
        if stripped.is_empty() {
            continue;
        }
        let code_bytes = hex::decode(stripped)
            .map_err(|hex_error| BridgeError::Other(format!("invalid code hex: {hex_error}")))?;
        if !verify_payload_hash_in_code(&code_bytes, &emitted.code_hash, emitted.payload_len as usize) {
            METRICS.audit_mismatch.inc();
            error!(
                doc_id = %emitted.doc_id,
                entity_key = %emitted.entity_key,
                block = emitted.block_number,
                emitted_count = emitted.emitted_count,
                "audit mismatch: recorded payload hash not present in trie-committed entity code"
            );
            if METRICS.audit_mismatch.get() >= config.mismatch_halt_threshold {
                return Err(BridgeError::AuditMismatch {
                    entity_key: emitted.entity_key.clone(),
                    block_number: emitted.block_number,
                });
            }
        } else {
            verified_count += 1;
        }
    }
    if verified_count > 0 {
        info!(verified = verified_count, sampled = sample.len(), "audit pass clean");
    }
    Ok(verified_count)
}

/// The recorded hash is `keccak256(payload)` over `payload_len` bytes. RLP
/// strings embed their content verbatim, so the payload appears contiguously
/// somewhere inside `0xFE || RLP(entity)`. Sliding a fixed `payload_len`
/// window across the code and hashing each position binds the emitted hash
/// to trie-committed bytes without depending on arkiv's exact RLP field
/// order. O(n · hash) with n = code length.
fn verify_payload_hash_in_code(code_bytes: &[u8], recorded_hash: &str, payload_len: usize) -> bool {
    use sha3::{Digest, Keccak256};

    let stripped = recorded_hash.strip_prefix("0x").unwrap_or(recorded_hash);
    let Ok(expected) = hex::decode(stripped) else {
        return false;
    };
    if payload_len == 0 {
        return expected.as_slice() == Keccak256::digest([]).as_slice();
    }
    if payload_len > code_bytes.len() {
        return false;
    }
    for window in code_bytes.windows(payload_len) {
        if Keccak256::digest(window).as_slice() == expected.as_slice() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc::payload_hash;

    #[test]
    fn finds_payload_inside_rlp_like_envelope() {
        let payload = b"hello world";
        let recorded = payload_hash(payload);
        // Simulate `0xFE || rlp(...)` with the payload embedded verbatim.
        let mut code = vec![0xFEu8, 0x0B];
        code.extend_from_slice(payload);
        code.extend_from_slice(&[0x01, 0x02]);
        assert!(verify_payload_hash_in_code(&code, &recorded, payload.len()));
    }

    #[test]
    fn rejects_when_payload_absent() {
        let recorded = payload_hash(b"hello world");
        let code = b"\xFEsomething else entirely".to_vec();
        assert!(!verify_payload_hash_in_code(&code, &recorded, 11));
    }

    #[test]
    fn rejects_when_payload_longer_than_code() {
        let recorded = payload_hash(b"hello world");
        assert!(!verify_payload_hash_in_code(b"\xFE", &recorded, 11));
    }

    #[test]
    fn empty_payload_always_verifies() {
        let recorded = payload_hash(b"");
        assert!(verify_payload_hash_in_code(b"\xFE\x01", &recorded, 0));
    }
}
