//! `eth_*` namespace client: log fetching, block headers, code reads.

use std::sync::LazyLock;

use serde_json::{Value, json};
use sha3::{Digest, Keccak256};

use super::{JsonRpcTransport, parse_hex_u64};
use crate::error::{BridgeError, Result};

use crate::config::EventSchema;
use crate::doc::{OP_CREATE, OP_DELETE, OP_EXPIRE, OP_EXTEND, OP_TRANSFER, OP_UPDATE};

/// Canonical event signature from `EntityRegistry.sol`. A signature change
/// upstream is an explicit, reviewed update here.
pub const ENTITY_OPERATION_SIGNATURE: &str =
    "EntityOperation(bytes32,uint8,address,uint32,bytes32)";

/// topic0 of every arkiv entity mutation event:
/// `keccak256(ENTITY_OPERATION_SIGNATURE)`, `0x`-prefixed hex.
pub static ENTITY_OPERATION_TOPIC0: LazyLock<String> = LazyLock::new(|| {
    let hash = Keccak256::digest(ENTITY_OPERATION_SIGNATURE.as_bytes());
    format!("0x{}", hex::encode(hash))
});

/// Legacy schema (deployed networks, e.g. hoodi): six per-operation events.
/// Signatures verified against live logs at `0x…61726b6976`. All carry the
/// entity key as topic1 and an owner address as topic2 (`OwnerChanged` puts
/// the new owner in topic3).
const LEGACY_EVENT_SIGNATURES: [(&str, u8); 6] = [
    ("ArkivEntityCreated(uint256,address,uint256,uint256)", OP_CREATE),
    ("ArkivEntityUpdated(uint256,address,uint256,uint256,uint256)", OP_UPDATE),
    ("ArkivEntityBTLExtended(uint256,address,uint256,uint256,uint256)", OP_EXTEND),
    ("ArkivEntityOwnerChanged(uint256,address,address)", OP_TRANSFER),
    ("ArkivEntityDeleted(uint256,address)", OP_DELETE),
    ("ArkivEntityExpired(uint256,address)", OP_EXPIRE),
];

/// `topic0 → op_type` for the legacy schema.
static LEGACY_TOPIC0_TO_OP: LazyLock<Vec<(String, u8)>> = LazyLock::new(|| {
    LEGACY_EVENT_SIGNATURES
        .iter()
        .map(|(signature, op_type)| {
            let hash = Keccak256::digest(signature.as_bytes());
            (format!("0x{}", hex::encode(hash)), *op_type)
        })
        .collect()
});

/// One decoded `EntityOperation` log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EthLog {
    pub block_number: u64,
    pub block_hash: String,
    pub log_index: u64,
    /// `bytes32` entity key, `0x`-prefixed hex (topic1).
    pub entity_key: String,
    /// Operation discriminant 1..=6 (topic2).
    pub operation_type: u8,
    /// Owner after the op (topic3), `0x`-prefixed 20-byte hex.
    pub owner: String,
    /// `expiresAt` from the (non-indexed) data section.
    pub expires_at: u64,
}

#[derive(Clone)]
pub struct EthClient {
    transport: JsonRpcTransport,
    arkiv_address: String,
    schema: EventSchema,
}

impl EthClient {
    pub fn new(transport: JsonRpcTransport, arkiv_address: &str, schema: EventSchema) -> Self {
        Self {
            transport,
            arkiv_address: arkiv_address.to_string(),
            schema,
        }
    }

    pub async fn block_number(&self) -> Result<u64> {
        let result = self.transport.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(&result)
    }

    /// `eth_chainId` — used at startup to catch pointing the bridge at the
    /// wrong network.
    pub async fn chain_id(&self) -> Result<u64> {
        let result = self.transport.call("eth_chainId", json!([])).await?;
        parse_hex_u64(&result)
    }

    /// `(block_hash, block_timestamp)` for a block number. Errors if the
    /// block does not exist (the caller always asks about confirmed blocks).
    pub async fn block_hash_and_timestamp(&self, block_number: u64) -> Result<(String, u64)> {
        let result = self
            .transport
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{block_number:x}"), false]),
            )
            .await?;
        if result.is_null() {
            return Err(BridgeError::ArkivRpc(format!(
                "eth_getBlockByNumber: block {block_number} not found"
            )));
        }
        let block_hash = result
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::ArkivRpc("block response missing hash".to_string()))?
            .to_lowercase();
        let timestamp_value = result
            .get("timestamp")
            .ok_or_else(|| BridgeError::ArkivRpc("block response missing timestamp".to_string()))?;
        let timestamp = parse_hex_u64(timestamp_value)?;
        Ok((block_hash, timestamp))
    }

    /// Fetches and decodes entity mutation logs in `[from_block, to_block]`,
    /// using the configured schema's topic filter and decoder.
    pub async fn entity_operation_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<EthLog>> {
        let topic0_filter: Vec<&str> = match self.schema {
            EventSchema::EntityOperation => vec![ENTITY_OPERATION_TOPIC0.as_str()],
            EventSchema::Legacy => LEGACY_TOPIC0_TO_OP
                .iter()
                .map(|(topic0, _op)| topic0.as_str())
                .collect(),
        };
        let filter = json!([{
            "address": self.arkiv_address,
            // An array in topic position 0 is an OR-filter.
            "topics": [topic0_filter],
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
        }]);
        let result = self.transport.call("eth_getLogs", filter).await?;
        let raw_logs = result
            .as_array()
            .ok_or_else(|| BridgeError::ArkivRpc("eth_getLogs: expected array".to_string()))?;
        let mut logs = Vec::with_capacity(raw_logs.len());
        for raw_log in raw_logs {
            let decoded = match self.schema {
                EventSchema::EntityOperation => decode_entity_operation_log(raw_log)?,
                EventSchema::Legacy => decode_legacy_log(raw_log)?,
            };
            logs.push(decoded);
        }
        // eth_getLogs returns logs in block order, but be explicit: the
        // control loop depends on (block, log_index) ordering.
        logs.sort_by_key(|log| (log.block_number, log.log_index));
        Ok(logs)
    }

    /// `eth_getCode(address, block)` — raw code bytes as `0x`-hex. Used by
    /// the sampling audit to verify entity payloads against `code_hash`.
    pub async fn get_code(&self, address: &str, block_number: u64) -> Result<String> {
        let result = self
            .transport
            .call(
                "eth_getCode",
                json!([address, format!("0x{block_number:x}")]),
            )
            .await?;
        result
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| BridgeError::ArkivRpc("eth_getCode: expected hex string".to_string()))
    }
}

/// Decodes one raw `eth_getLogs` entry into an [`EthLog`].
///
/// Topic layout (see `EntityRegistry.sol`):
///   topic0 = signature hash, topic1 = entityKey, topic2 = operationType,
///   topic3 = owner. Data = abi.encode(expiresAt: uint32, entityHash: bytes32).
fn decode_entity_operation_log(raw_log: &Value) -> Result<EthLog> {
    let topics = raw_log
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| BridgeError::ArkivRpc("log missing topics".to_string()))?;
    if topics.len() != 4 {
        return Err(BridgeError::ArkivRpc(format!(
            "EntityOperation log has {} topics, expected 4",
            topics.len()
        )));
    }
    let topic_str = |index: usize| -> Result<String> {
        topics[index]
            .as_str()
            .map(|topic| topic.to_lowercase())
            .ok_or_else(|| BridgeError::ArkivRpc(format!("topic {index} is not a string")))
    };
    let entity_key = topic_str(1)?;
    let operation_type_word = topic_str(2)?;
    let operation_type = parse_hex_u64(&Value::String(operation_type_word))? as u8;
    // topic3 is the owner address left-padded to 32 bytes.
    let owner_word = topic_str(3)?;
    let owner_hex = owner_word
        .strip_prefix("0x")
        .unwrap_or(&owner_word);
    if owner_hex.len() != 64 {
        return Err(BridgeError::ArkivRpc(format!(
            "owner topic has unexpected length: {owner_word}"
        )));
    }
    let owner = format!("0x{}", &owner_hex[24..]);

    let block_number = parse_hex_u64(
        raw_log
            .get("blockNumber")
            .ok_or_else(|| BridgeError::ArkivRpc("log missing blockNumber".to_string()))?,
    )?;
    let block_hash = raw_log
        .get("blockHash")
        .and_then(Value::as_str)
        .ok_or_else(|| BridgeError::ArkivRpc("log missing blockHash".to_string()))?
        .to_lowercase();
    let log_index = parse_hex_u64(
        raw_log
            .get("logIndex")
            .ok_or_else(|| BridgeError::ArkivRpc("log missing logIndex".to_string()))?,
    )?;

    // Data section: two 32-byte words — expiresAt (uint32, right-aligned)
    // then entityHash (always zero today).
    let data = raw_log
        .get("data")
        .and_then(Value::as_str)
        .unwrap_or("0x");
    let data_hex = data.strip_prefix("0x").unwrap_or(data);
    let expires_at = if data_hex.len() >= 64 {
        u64::from_str_radix(&data_hex[48..64], 16).map_err(|parse_error| {
            BridgeError::ArkivRpc(format!("invalid expiresAt in log data: {parse_error}"))
        })?
    } else {
        0
    };

    Ok(EthLog {
        block_number,
        block_hash,
        log_index,
        entity_key,
        operation_type,
        owner,
        expires_at,
    })
}

/// Decodes one legacy-schema log. Layout (verified against live hoodi
/// logs): topic1 = entity key, topic2 = owner (except `OwnerChanged`,
/// where topic3 is the new owner). `expires_at` is best-effort from the
/// data section — the doc builder prefers the hydrated entity's value and
/// only falls back to this.
fn decode_legacy_log(raw_log: &Value) -> Result<EthLog> {
    let topics = raw_log
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| BridgeError::ArkivRpc("log missing topics".to_string()))?;
    if topics.len() < 3 {
        return Err(BridgeError::ArkivRpc(format!(
            "legacy entity log has {} topics, expected at least 3",
            topics.len()
        )));
    }
    let topic_str = |index: usize| -> Result<String> {
        topics[index]
            .as_str()
            .map(|topic| topic.to_lowercase())
            .ok_or_else(|| BridgeError::ArkivRpc(format!("topic {index} is not a string")))
    };
    let topic0 = topic_str(0)?;
    let operation_type = LEGACY_TOPIC0_TO_OP
        .iter()
        .find(|(known_topic0, _op)| *known_topic0 == topic0)
        .map(|(_topic0, op_type)| *op_type)
        .ok_or_else(|| BridgeError::ArkivRpc(format!("unknown legacy event topic0 {topic0}")))?;

    let entity_key = topic_str(1)?;
    // OwnerChanged: topic2 = old owner, topic3 = new owner (owner after op).
    let owner_topic_index = if operation_type == OP_TRANSFER && topics.len() >= 4 {
        3
    } else {
        2
    };
    let owner_word = topic_str(owner_topic_index)?;
    let owner_hex = owner_word.strip_prefix("0x").unwrap_or(&owner_word);
    if owner_hex.len() != 64 {
        return Err(BridgeError::ArkivRpc(format!(
            "owner topic has unexpected length: {owner_word}"
        )));
    }
    let owner = format!("0x{}", &owner_hex[24..]);

    let block_number = parse_hex_u64(
        raw_log
            .get("blockNumber")
            .ok_or_else(|| BridgeError::ArkivRpc("log missing blockNumber".to_string()))?,
    )?;
    let block_hash = raw_log
        .get("blockHash")
        .and_then(Value::as_str)
        .ok_or_else(|| BridgeError::ArkivRpc("log missing blockHash".to_string()))?
        .to_lowercase();
    let log_index = parse_hex_u64(
        raw_log
            .get("logIndex")
            .ok_or_else(|| BridgeError::ArkivRpc("log missing logIndex".to_string()))?,
    )?;

    // Data words: Created/Updated carry the expiration block first;
    // BTLExtended carries (old expiration, new expiration, …).
    let data = raw_log.get("data").and_then(Value::as_str).unwrap_or("0x");
    let data_hex = data.strip_prefix("0x").unwrap_or(data);
    let data_word_u64 = |word_index: usize| -> u64 {
        let start = word_index * 64;
        let end = start + 64;
        if data_hex.len() < end {
            return 0;
        }
        // Take the low 8 bytes of the 32-byte word (block numbers fit).
        u64::from_str_radix(&data_hex[start + 48..end], 16).unwrap_or(0)
    };
    let expires_at = match operation_type {
        OP_CREATE | OP_UPDATE => data_word_u64(0),
        OP_EXTEND => data_word_u64(1),
        _ => 0,
    };

    Ok(EthLog {
        block_number,
        block_hash,
        log_index,
        entity_key,
        operation_type,
        owner,
        expires_at,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn topic0_is_derived_from_signature() {
        // 32-byte keccak digest, 0x + 64 hex chars.
        assert_eq!(ENTITY_OPERATION_TOPIC0.len(), 66);
        assert!(ENTITY_OPERATION_TOPIC0.starts_with("0x"));
    }

    #[test]
    fn decodes_a_create_log() {
        let raw = json!({
            "blockNumber": "0x10",
            "blockHash": "0xAABB",
            "logIndex": "0x2",
            "topics": [
                ENTITY_OPERATION_TOPIC0.as_str(),
                "0x1111111111111111111111111111111111111111111111111111111111111111",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
                "0x000000000000000000000000deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            ],
            "data": "0x00000000000000000000000000000000000000000000000000000000000000ff0000000000000000000000000000000000000000000000000000000000000000",
        });
        let log = decode_entity_operation_log(&raw).unwrap();
        assert_eq!(log.block_number, 16);
        assert_eq!(log.log_index, 2);
        assert_eq!(log.operation_type, 1);
        assert_eq!(log.owner, "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        assert_eq!(log.expires_at, 255);
        assert_eq!(log.block_hash, "0xaabb");
    }

    #[test]
    fn rejects_wrong_topic_count() {
        let raw = json!({
            "blockNumber": "0x10",
            "blockHash": "0xaabb",
            "logIndex": "0x0",
            "topics": [ENTITY_OPERATION_TOPIC0.as_str()],
            "data": "0x",
        });
        assert!(decode_entity_operation_log(&raw).is_err());
    }
}
