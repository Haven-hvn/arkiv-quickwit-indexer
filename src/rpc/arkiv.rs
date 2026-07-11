//! `arkiv_*` namespace client. Wire shapes mirror
//! `arkiv-node/src/rpc.rs` (`QueryResponse`, `EntityData`, `Attribute`).

use serde::Deserialize;
use serde_json::json;

use super::JsonRpcTransport;
use crate::error::{BridgeError, Result};

/// Attribute value-type discriminants (`Entity` library constants).
pub const ATTR_UINT: u8 = 1;
pub const ATTR_STRING: u8 = 2;
pub const ATTR_ENTITY_KEY: u8 = 3;

/// One attribute on the wire: `value`'s encoding depends on `value_type`
/// (`ATTR_UINT` → decimal string, `ATTR_STRING` → UTF-8,
/// `ATTR_ENTITY_KEY` → 0x-hex).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityAttribute {
    pub key: String,
    pub value_type: u8,
    pub value: String,
}

/// Per-entity payload in `arkiv_query` responses. Mirrors the node's
/// `EntityData`. All metadata fields are optional on the wire; the bridge
/// always requests full `includeData` so they should be present.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityData {
    pub key: String,
    /// Payload bytes, `0x`-prefixed hex.
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub expires_at: Option<u64>,
    /// Wire-shape parity with the node; the doc builder uses the owner
    /// from the event topic (authoritative for the op) instead.
    #[serde(default)]
    #[allow(dead_code)]
    pub owner: Option<String>,
    #[serde(default)]
    pub creator: Option<String>,
    #[serde(default, deserialize_with = "de_opt_u64_flexible")]
    pub created_at_block: Option<u64>,
    #[serde(default, deserialize_with = "de_opt_u64_flexible")]
    pub last_modified_at_block: Option<u64>,
    #[serde(default)]
    pub attributes: Vec<EntityAttribute>,
    /// Legacy wire shape: typed attribute lists instead of `attributes`.
    /// `{"key": "seq", "value": 6582}` — value is a JSON number.
    #[serde(default)]
    pub numeric_attributes: Vec<LegacyAttribute>,
    /// Legacy wire shape: `{"key": "type", "value": "telemetry"}`.
    #[serde(default)]
    pub string_attributes: Vec<LegacyAttribute>,
}

/// Legacy attribute entry: value is a raw JSON value (number or string).
#[derive(Debug, Clone, Deserialize)]
pub struct LegacyAttribute {
    pub key: String,
    pub value: serde_json::Value,
}

impl EntityData {
    /// All attributes normalized to the unified `EntityAttribute` shape,
    /// merging the modern `attributes` list with the legacy typed lists.
    pub fn all_attributes(&self) -> Vec<EntityAttribute> {
        let capacity = self.attributes.len()
            + self.numeric_attributes.len()
            + self.string_attributes.len();
        let mut merged = Vec::with_capacity(capacity);
        merged.extend(self.attributes.iter().cloned());
        for legacy in &self.numeric_attributes {
            let rendered = match &legacy.value {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            merged.push(EntityAttribute {
                key: legacy.key.clone(),
                value_type: ATTR_UINT,
                value: rendered,
            });
        }
        for legacy in &self.string_attributes {
            let rendered = match &legacy.value {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            merged.push(EntityAttribute {
                key: legacy.key.clone(),
                value_type: ATTR_STRING,
                value: rendered,
            });
        }
        merged
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub data: Vec<EntityData>,
    #[serde(deserialize_with = "de_u64_flexible")]
    pub block_number: u64,
    /// Wire-shape parity; the bridge fetches one entity per call and never
    /// paginates.
    #[serde(default)]
    #[allow(dead_code)]
    pub cursor: Option<String>,
}

#[derive(Clone)]
pub struct ArkivClient {
    transport: JsonRpcTransport,
    schema: crate::config::EventSchema,
}

impl ArkivClient {
    pub fn new(transport: JsonRpcTransport, schema: crate::config::EventSchema) -> Self {
        Self { transport, schema }
    }

    /// Hydrates one entity at a specific block. Returns `None` when the
    /// entity does not exist at that block (e.g. raced a same-block delete).
    ///
    /// Legacy-schema nodes ignore `atBlock` (verified live: the response
    /// reports `blockNumber: 0x0` and reads head regardless), so in that
    /// mode hydration reads head state — correct for tail-only crawling
    /// where the log block is within `confirmation_depth` of head, but it
    /// means backfill on legacy networks re-reads *current* payloads.
    pub async fn get_entity_at(
        &self,
        entity_key: &str,
        block_number: u64,
    ) -> Result<Option<EntityData>> {
        let query = format!("$key = {entity_key}");
        // `resultsPerPage` is hex-encoded: live nodes deserialize it as
        // `hexutil.Uint64` and reject bare JSON numbers.
        let mut options = json!({
            "resultsPerPage": "0x1",
            // Full projection: every field the Quickwit doc needs.
            "includeData": {
                "key": true,
                "payload": true,
                "attributes": true,
                "contentType": true,
                "expiration": true,
                "owner": true,
                "creator": true,
                "createdAtBlock": true,
                "lastModifiedAtBlock": true,
            },
        });
        let historical = self.schema == crate::config::EventSchema::EntityOperation;
        if historical {
            options["atBlock"] = json!(format!("0x{block_number:x}"));
        }
        let result = self
            .transport
            .call("arkiv_query", json!([query, options]))
            .await?;
        let response: QueryResponse = serde_json::from_value(result).map_err(|json_error| {
            BridgeError::ArkivRpc(format!("arkiv_query: bad response shape: {json_error}"))
        })?;
        // The node reports which block it actually evaluated against; a
        // mismatch means the historical read silently fell back to head.
        if historical && response.block_number != block_number {
            return Err(BridgeError::ArkivRpc(format!(
                "arkiv_query evaluated at block {} instead of requested {block_number}",
                response.block_number
            )));
        }
        Ok(response.data.into_iter().next())
    }

    /// Checks whether the entity is live at head. Used to classify a failed
    /// historical hydration: nodes are not expected to retain state history
    /// for expired entities, so "history unreadable AND dead at head" means
    /// the data is legitimately gone (skip), while "history unreadable but
    /// alive at head" is a real node problem (retry).
    pub async fn is_live_at_head(&self, entity_key: &str) -> Result<bool> {
        let query = format!("$key = {entity_key}");
        let options = json!({
            "resultsPerPage": "0x1",
            // Existence check only: key is always returned.
            "includeData": { "key": true },
        });
        let result = self
            .transport
            .call("arkiv_query", json!([query, options]))
            .await?;
        let response: QueryResponse = serde_json::from_value(result).map_err(|json_error| {
            BridgeError::ArkivRpc(format!("arkiv_query: bad response shape: {json_error}"))
        })?;
        Ok(!response.data.is_empty())
    }
}

/// The node hex-encodes u64 block fields (`ser_opt_u64_hex`); accept both
/// hex strings and bare numbers, mirroring the node's own flexibility.
fn de_opt_u64_flexible<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        Num(u64),
        Hex(String),
    }
    let parsed: Option<Either> = Option::deserialize(deserializer)?;
    match parsed {
        None => Ok(None),
        Some(Either::Num(number)) => Ok(Some(number)),
        Some(Either::Hex(text)) => {
            let stripped = text.strip_prefix("0x").unwrap_or(&text);
            u64::from_str_radix(stripped, 16)
                .map(Some)
                .map_err(|parse_error| D::Error::custom(format!("invalid hex u64: {parse_error}")))
        }
    }
}

fn de_u64_flexible<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let value: Option<u64> = de_opt_u64_flexible(deserializer)?;
    value.ok_or_else(|| D::Error::custom("missing required u64"))
}

/// Derives the entity account address from the entity key:
/// `entity_address = entityKey[:20]` (see arkiv `2_state-model.md`).
pub fn entity_address(entity_key: &str) -> Result<String> {
    let stripped = entity_key.strip_prefix("0x").unwrap_or(entity_key);
    if stripped.len() != 64 {
        return Err(BridgeError::Other(format!(
            "entity key `{entity_key}` is not 32 bytes"
        )));
    }
    Ok(format!("0x{}", &stripped[..40]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_query_response_with_hex_blocks() {
        let raw = json!({
            "data": [{
                "key": "0x1111111111111111111111111111111111111111111111111111111111111111",
                "value": "0x68656c6c6f",
                "contentType": "text/plain",
                "expiresAt": 100,
                "owner": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "creator": "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "createdAtBlock": "0x10",
                "lastModifiedAtBlock": 17,
                "attributes": [
                    {"key": "score", "valueType": 1, "value": "42"}
                ]
            }],
            "blockNumber": "0x11",
        });
        let response: QueryResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(response.block_number, 17);
        let entity = &response.data[0];
        assert_eq!(entity.created_at_block, Some(16));
        assert_eq!(entity.last_modified_at_block, Some(17));
        assert_eq!(entity.attributes[0].value_type, ATTR_UINT);
    }

    #[test]
    fn entity_address_is_key_prefix() {
        let key = "0x1122334455667788990011223344556677889900aabbccddeeff001122334455";
        assert_eq!(
            entity_address(key).unwrap(),
            "0x1122334455667788990011223344556677889900"
        );
        assert!(entity_address("0x1234").is_err());
    }

}
