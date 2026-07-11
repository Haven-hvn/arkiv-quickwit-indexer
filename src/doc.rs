//! Quickwit document construction: the `EntityOperation` log + hydrated
//! `EntityData` → the JSON document POSTed to the ingest API (§8).

use serde::Serialize;
use serde_json::{Map, Value};
use sha3::{Digest, Keccak256};

use crate::config::{EntityFilterRule, PayloadExtractorRule};
use crate::error::{BridgeError, Result};
use crate::extract::extract_body;
use crate::rpc::{ATTR_ENTITY_KEY, ATTR_STRING, ATTR_UINT, EntityData, EthLog, entity_address};

pub const OP_CREATE: u8 = 1;
pub const OP_UPDATE: u8 = 2;
pub const OP_EXTEND: u8 = 3;
pub const OP_TRANSFER: u8 = 4;
pub const OP_DELETE: u8 = 5;
pub const OP_EXPIRE: u8 = 6;

/// Operation names as indexed in the `op_type` field (raw tokenizer).
pub fn op_type_name(operation_type: u8) -> &'static str {
    match operation_type {
        OP_CREATE => "create",
        OP_UPDATE => "update",
        OP_EXTEND => "extend",
        OP_TRANSFER => "transfer",
        OP_DELETE => "delete",
        OP_EXPIRE => "expire",
        _ => "unknown",
    }
}

/// Typed annotation sub-bags matching the shipped index config: arkiv's
/// `Attribute.valueType` splits into `string` / `uint` / `entity_key`
/// JSON objects, each with its natural query semantics.
#[derive(Debug, Default, Serialize)]
pub struct Annotations {
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub string: Map<String, Value>,
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub uint: Map<String, Value>,
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub entity_key: Map<String, Value>,
}

/// One Quickwit ingest document. Field names must match
/// `index-config/arkiv-index.yaml` exactly (strict mapping mode).
#[derive(Debug, Serialize)]
pub struct QuickwitDoc {
    pub _doc_id: String,
    pub entity_key: String,
    pub entity_address: String,
    pub code_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub op_type: &'static str,
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at_block: Option<u64>,
    pub expires_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified_at_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    pub annotations: Annotations,
}

/// Deterministic dedup handle (§6.3):
/// `keccak256(entity_key || block_number_be8 || log_index_be4 || op_type_be1)`.
/// Re-emitting the same log yields the same `_doc_id`.
pub fn doc_id(entity_key: &str, block_number: u64, log_index: u64, operation_type: u8) -> Result<String> {
    let key_stripped = entity_key.strip_prefix("0x").unwrap_or(entity_key);
    let key_bytes = hex::decode(key_stripped)
        .map_err(|hex_error| BridgeError::Other(format!("invalid entity key hex: {hex_error}")))?;
    let mut hasher = Keccak256::new();
    hasher.update(&key_bytes);
    hasher.update(block_number.to_be_bytes());
    hasher.update((log_index as u32).to_be_bytes());
    hasher.update([operation_type]);
    Ok(format!("0x{}", hex::encode(hasher.finalize())))
}

/// Builds the ingest document for a mutation log + its hydrated entity.
///
/// `code_hash` is computed from the hydrated payload as
/// `keccak256(0xFE || rlp)` would be — but the bridge does not have the RLP,
/// so it carries the node-reported hash obtained separately by the caller
/// (via `eth_getCode` during audits) or, on the hot path, the hash of the
/// raw payload bytes as a payload-integrity handle. See `payload_hash`.
pub fn to_quickwit_doc(
    extractor_rules: &[PayloadExtractorRule],
    log: &EthLog,
    entity: &EntityData,
    block_timestamp: u64,
) -> Result<QuickwitDoc> {
    let payload_bytes = decode_payload(entity)?;
    let content_type = entity.content_type.clone().unwrap_or_default();
    let body = if payload_bytes.is_empty() {
        None
    } else {
        extract_body(extractor_rules, &content_type, &payload_bytes)
    };

    let mut annotations = Annotations::default();
    for attribute in &entity.all_attributes() {
        match attribute.value_type {
            ATTR_UINT => {
                // Store numerically when it fits u64 so Quickwit range
                // queries work; larger uints fall back to strings.
                let numeric_value = attribute
                    .value
                    .parse::<u64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| Value::String(attribute.value.clone()));
                annotations.uint.insert(attribute.key.clone(), numeric_value);
            }
            ATTR_ENTITY_KEY => {
                annotations
                    .entity_key
                    .insert(attribute.key.clone(), Value::String(attribute.value.clone()));
            }
            ATTR_STRING => {
                annotations
                    .string
                    .insert(attribute.key.clone(), Value::String(attribute.value.clone()));
            }
            unknown_type => {
                return Err(BridgeError::Other(format!(
                    "entity {} attribute `{}` has unknown value type {unknown_type}",
                    entity.key, attribute.key
                )));
            }
        }
    }

    Ok(QuickwitDoc {
        _doc_id: doc_id(&log.entity_key, log.block_number, log.log_index, log.operation_type)?,
        entity_key: log.entity_key.clone(),
        entity_address: entity_address(&log.entity_key)?,
        code_hash: payload_hash(&payload_bytes),
        block_number: log.block_number,
        block_timestamp,
        op_type: op_type_name(log.operation_type),
        owner: log.owner.clone(),
        creator: entity.creator.clone(),
        created_at_block: entity.created_at_block,
        expires_at: entity.expires_at.unwrap_or(log.expires_at),
        last_modified_at_block: entity.last_modified_at_block,
        content_type: entity.content_type.clone(),
        body,
        annotations,
    })
}

/// Entity filter (config `entity_filters`): decides whether an entity is
/// indexed at all. Empty rule list = index everything. Otherwise the
/// entity must match at least one rule (OR across rules); within a rule,
/// every condition must hold (AND). A condition holds when the entity
/// carries a string annotation with the condition's key — and, when the
/// condition pins a value, exactly that value.
pub fn entity_matches_filters(rules: &[EntityFilterRule], entity: &EntityData) -> bool {
    if rules.is_empty() {
        return true;
    }
    let attributes = entity.all_attributes();
    let condition_holds = |condition: &crate::config::AnnotationCondition| {
        attributes.iter().any(|attribute| {
            if attribute.value_type != crate::rpc::ATTR_STRING {
                return false;
            }
            if attribute.key != condition.string_annotation_key {
                return false;
            }
            match &condition.string_annotation_value {
                Some(required_value) => &attribute.value == required_value,
                None => true,
            }
        })
    };
    rules
        .iter()
        .any(|rule| rule.conditions().iter().all(condition_holds))
}

/// keccak256 of the raw payload bytes — the integrity handle carried in
/// every doc. A client verifies a hit by hydrating the payload from arkiv
/// (`arkiv_query` / decoding `eth_getCode`) and re-hashing; the sampling
/// audit does the same server-side.
pub fn payload_hash(payload: &[u8]) -> String {
    format!("0x{}", hex::encode(Keccak256::digest(payload)))
}

fn decode_payload(entity: &EntityData) -> Result<Vec<u8>> {
    let Some(payload_hex) = &entity.value else {
        return Ok(Vec::new());
    };
    let stripped = payload_hex.strip_prefix("0x").unwrap_or(payload_hex);
    hex::decode(stripped)
        .map_err(|hex_error| BridgeError::Other(format!("invalid payload hex: {hex_error}")))
}

#[cfg(test)]
mod tests {
    use crate::rpc::EntityAttribute;

    use super::*;

    const KEY: &str = "0x1122334455667788990011223344556677889900aabbccddeeff001122334455";

    fn sample_log(operation_type: u8) -> EthLog {
        EthLog {
            block_number: 100,
            block_hash: "0xabc".to_string(),
            log_index: 3,
            entity_key: KEY.to_string(),
            operation_type,
            owner: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            expires_at: 500,
        }
    }

    fn sample_entity() -> EntityData {
        EntityData {
            key: KEY.to_string(),
            value: Some(format!("0x{}", hex::encode(b"hello world"))),
            content_type: Some("text/plain".to_string()),
            expires_at: Some(500),
            owner: Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
            creator: Some("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string()),
            created_at_block: Some(90),
            last_modified_at_block: Some(100),
            attributes: vec![
                EntityAttribute {
                    key: "score".to_string(),
                    value_type: ATTR_UINT,
                    value: "42".to_string(),
                },
                EntityAttribute {
                    key: "tag".to_string(),
                    value_type: ATTR_STRING,
                    value: "approved".to_string(),
                },
                EntityAttribute {
                    key: "parent".to_string(),
                    value_type: ATTR_ENTITY_KEY,
                    value: KEY.to_string(),
                },
            ],
            numeric_attributes: Vec::new(),
            string_attributes: Vec::new(),
        }
    }

    #[test]
    fn doc_id_is_deterministic_and_distinct() {
        let id_a = doc_id(KEY, 100, 3, OP_CREATE).unwrap();
        let id_b = doc_id(KEY, 100, 3, OP_CREATE).unwrap();
        let id_c = doc_id(KEY, 100, 4, OP_CREATE).unwrap();
        assert_eq!(id_a, id_b);
        assert_ne!(id_a, id_c);
        assert_eq!(id_a.len(), 66);
    }

    #[test]
    fn doc_carries_provenance_and_typed_annotations() {
        let doc = to_quickwit_doc(&[], &sample_log(OP_CREATE), &sample_entity(), 1_700_000_000)
            .unwrap();
        assert_eq!(doc.entity_key, KEY);
        assert_eq!(doc.entity_address, "0x1122334455667788990011223344556677889900");
        assert_eq!(doc.op_type, "create");
        assert_eq!(doc.block_number, 100);
        assert_eq!(doc.block_timestamp, 1_700_000_000);
        assert_eq!(doc.body.as_deref(), Some("hello world"));
        assert_eq!(doc.code_hash, payload_hash(b"hello world"));
        assert_eq!(doc.annotations.uint["score"], Value::from(42u64));
        assert_eq!(doc.annotations.string["tag"], Value::from("approved"));
        assert_eq!(doc.annotations.entity_key["parent"], Value::from(KEY));
    }

    #[test]
    fn serialized_doc_matches_strict_mapping() {
        let doc = to_quickwit_doc(&[], &sample_log(OP_UPDATE), &sample_entity(), 1_700_000_000)
            .unwrap();
        let line = serde_json::to_string(&doc).unwrap();
        assert!(!line.contains('\n'));
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["op_type"], "update");
        // Strict mapping: no unexpected top-level fields.
        assert!(parsed.get("value").is_none());
    }

    #[test]
    fn oversized_uint_annotation_falls_back_to_string() {
        let mut entity = sample_entity();
        entity.attributes = vec![EntityAttribute {
            key: "big".to_string(),
            value_type: ATTR_UINT,
            value: "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                .to_string(),
        }];
        let doc = to_quickwit_doc(&[], &sample_log(OP_CREATE), &entity, 0).unwrap();
        assert!(doc.annotations.uint["big"].is_string());
    }

    #[test]
    fn entity_filters_empty_match_everything() {
        assert!(entity_matches_filters(&[], &sample_entity()));
    }

    #[test]
    fn entity_filters_key_only_and_key_value() {
        use crate::config::{AnnotationCondition, EntityFilterRule};
        let entity = sample_entity(); // has string annotation tag=approved

        let key_only = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "tag".to_string(),
            string_annotation_value: None,
        })];
        assert!(entity_matches_filters(&key_only, &entity));

        let key_value = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "tag".to_string(),
            string_annotation_value: Some("approved".to_string()),
        })];
        assert!(entity_matches_filters(&key_value, &entity));

        let wrong_value = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "tag".to_string(),
            string_annotation_value: Some("rejected".to_string()),
        })];
        assert!(!entity_matches_filters(&wrong_value, &entity));

        let missing_key = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "category".to_string(),
            string_annotation_value: None,
        })];
        assert!(!entity_matches_filters(&missing_key, &entity));

        // uint annotation `score` must not satisfy a *string* key filter.
        let uint_key = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "score".to_string(),
            string_annotation_value: None,
        })];
        assert!(!entity_matches_filters(&uint_key, &entity));
    }

    #[test]
    fn entity_filters_or_across_rules_and_within_rule() {
        use crate::config::{AnnotationCondition, EntityFilterRule};
        let entity = sample_entity(); // tag=approved only

        // OR: first rule misses, second matches.
        let or_rules = vec![
            EntityFilterRule::Single(AnnotationCondition {
                string_annotation_key: "category".to_string(),
                string_annotation_value: None,
            }),
            EntityFilterRule::Single(AnnotationCondition {
                string_annotation_key: "tag".to_string(),
                string_annotation_value: None,
            }),
        ];
        assert!(entity_matches_filters(&or_rules, &entity));

        // AND within a rule: tag present but category absent → no match.
        let and_rule = vec![EntityFilterRule::AllOf {
            all_of: vec![
                AnnotationCondition {
                    string_annotation_key: "tag".to_string(),
                    string_annotation_value: Some("approved".to_string()),
                },
                AnnotationCondition {
                    string_annotation_key: "category".to_string(),
                    string_annotation_value: None,
                },
            ],
        }];
        assert!(!entity_matches_filters(&and_rule, &entity));

        // AND where every condition holds.
        let and_rule_ok = vec![EntityFilterRule::AllOf {
            all_of: vec![AnnotationCondition {
                string_annotation_key: "tag".to_string(),
                string_annotation_value: Some("approved".to_string()),
            }],
        }];
        assert!(entity_matches_filters(&and_rule_ok, &entity));
    }

    #[test]
    fn entity_filters_match_legacy_string_attributes() {
        use crate::config::{AnnotationCondition, EntityFilterRule};
        use crate::rpc::LegacyAttribute;
        let mut entity = sample_entity();
        entity.attributes = Vec::new();
        entity.string_attributes = vec![LegacyAttribute {
            key: "type".to_string(),
            value: serde_json::json!("w3pups-telemetry"),
        }];
        let rules = vec![EntityFilterRule::Single(AnnotationCondition {
            string_annotation_key: "type".to_string(),
            string_annotation_value: Some("w3pups-telemetry".to_string()),
        })];
        assert!(entity_matches_filters(&rules, &entity));
    }

    #[test]
    fn unknown_attribute_type_is_an_error_not_silence() {
        let mut entity = sample_entity();
        entity.attributes = vec![EntityAttribute {
            key: "weird".to_string(),
            value_type: 9,
            value: "x".to_string(),
        }];
        assert!(to_quickwit_doc(&[], &sample_log(OP_CREATE), &entity, 0).is_err());
    }
}
