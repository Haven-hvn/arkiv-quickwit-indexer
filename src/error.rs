//! Bridge error type.

use thiserror::Error;

/// Top-level error for the bridge daemon.
#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("state store error: {0}")]
    StateStore(#[from] rusqlite::Error),

    #[error("arkiv rpc error: {0}")]
    ArkivRpc(String),

    #[error("quickwit http error: status {status}, body: {body}")]
    QuickwitHttp { status: u16, body: String },

    #[error("http transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A block hash in the reorg buffer no longer matches the chain.
    /// Carries the fork point: the highest block whose hash still matches.
    #[error("reorg detected: fork point at block {fork_point}")]
    ReorgDetected { fork_point: u64 },

    /// Reorg deeper than `retract_max_depth` — halt for operator review.
    #[error("reorg depth {depth} exceeds retract_max_depth {max_depth}; halting")]
    ReorgTooDeep { depth: u64, max_depth: u64 },

    /// Sampling audit found a code-hash mismatch — data integrity broken.
    #[error("audit mismatch for entity {entity_key} at block {block_number}")]
    AuditMismatch {
        entity_key: String,
        block_number: u64,
    },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BridgeError>;
