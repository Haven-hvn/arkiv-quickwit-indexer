//! Bridge configuration: one YAML file, validated at startup.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{BridgeError, Result};

/// Root configuration, deserialized from the `--config` YAML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeConfig {
    pub arkiv: ArkivConfig,
    pub quickwit: QuickwitConfig,
    pub bridge: BridgeSection,
    #[serde(default)]
    pub payload_extractors: Vec<PayloadExtractorRule>,
    /// Which entities to index, by string annotation. Empty (default) =
    /// index everything. Non-empty = an entity is indexed only if it
    /// matches at least one rule (OR across rules; conditions within a
    /// rule AND together).
    #[serde(default)]
    pub entity_filters: Vec<EntityFilterRule>,
}

/// One filter rule. Two YAML forms:
///
/// Flat — a single condition:
/// ```yaml
/// - string_annotation_key: type                       # has the key
/// - string_annotation_key: type                       # has key AND value
///   string_annotation_value: w3pups-telemetry
/// ```
///
/// `all_of` — several conditions that must ALL hold on the same entity:
/// ```yaml
/// - all_of:
///     - string_annotation_key: type
///       string_annotation_value: w3pups-telemetry
///     - string_annotation_key: device_id              # any value
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EntityFilterRule {
    AllOf {
        all_of: Vec<AnnotationCondition>,
    },
    Single(AnnotationCondition),
}

impl EntityFilterRule {
    pub fn conditions(&self) -> &[AnnotationCondition] {
        match self {
            EntityFilterRule::AllOf { all_of } => all_of,
            EntityFilterRule::Single(condition) => std::slice::from_ref(condition),
        }
    }
}

/// The entity must carry a string annotation with this key — and, when
/// `string_annotation_value` is set, exactly that value.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationCondition {
    pub string_annotation_key: String,
    #[serde(default)]
    pub string_annotation_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArkivConfig {
    pub rpc_url: String,
    /// Optional second endpoint. When set, paranoid mode hydrates every
    /// entity twice and halts on discrepancy.
    #[serde(default)]
    pub rpc_url_backup: Option<String>,
    pub chain_id: u64,
    /// On-chain event schema. `entity_operation` (default) matches
    /// arkiv-op-reth's single `EntityOperation` event at `0x4400…0044`.
    /// `legacy` matches deployed networks (e.g. hoodi) that emit six
    /// `ArkivEntity*` events at `0x…61726b6976` and whose `arkiv_query`
    /// has no historical (`atBlock`) reads.
    #[serde(default)]
    pub schema: EventSchema,
    /// Precompile address emitting entity events. Defaults per schema.
    #[serde(default)]
    pub arkiv_address: Option<String>,
    /// First block to crawl. A number backfills from that block (`0` =
    /// genesis); `"head"` skips backfill entirely and starts at the chain
    /// height observed on first launch (tail-only mode). Applies only when
    /// the state store has no cursor yet — restarts always resume from the
    /// persisted cursor, so downtime never creates a gap.
    #[serde(default)]
    pub start_block: StartBlock,
    #[serde(default = "default_confirmation_depth")]
    pub confirmation_depth: u64,
    #[serde(default = "default_fetch_batch_size_blocks")]
    pub fetch_batch_size_blocks: u64,
    #[serde(default = "default_http_timeout_secs")]
    pub http_timeout_secs: u64,
}

/// On-chain event schema variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSchema {
    /// arkiv-op-reth `EntityOperation(bytes32,uint8,address,uint32,bytes32)`
    /// at `0x4400…0044`.
    #[default]
    EntityOperation,
    /// Deployed networks (e.g. hoodi testnet): six `ArkivEntityCreated /
    /// Updated / Deleted / Expired / BTLExtended / OwnerChanged` events at
    /// `0x…61726b6976`. These nodes also ignore `atBlock` on `arkiv_query`,
    /// so hydration reads head state.
    Legacy,
}

impl EventSchema {
    pub fn default_address(&self) -> &'static str {
        match self {
            EventSchema::EntityOperation => "0x4400000000000000000000000000000000000044",
            EventSchema::Legacy => "0x00000000000000000000000000000061726b6976",
        }
    }
}

impl ArkivConfig {
    /// The event-emitting address: explicit config wins, else the schema's
    /// canonical address.
    pub fn resolve_address(&self) -> String {
        match &self.arkiv_address {
            Some(address) => address.clone(),
            None => self.schema.default_address().to_string(),
        }
    }
}

/// Where the first-ever crawl begins. Restarts ignore this and resume from
/// the persisted cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartBlock {
    /// Backfill from a fixed block number (`0` = genesis, the default).
    #[default]
    Genesis,
    Number(u64),
    /// Tail-only: resolve the chain head at first launch and start there —
    /// no backfill, no historical state reads.
    Head,
}

impl<'de> serde::Deserialize<'de> for StartBlock {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Number(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Number(0) => Ok(StartBlock::Genesis),
            Raw::Number(block_number) => Ok(StartBlock::Number(block_number)),
            Raw::Text(text) if text == "head" => Ok(StartBlock::Head),
            Raw::Text(other) => Err(D::Error::custom(format!(
                "invalid start_block `{other}`: expected a block number or \"head\""
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuickwitConfig {
    pub base_url: String,
    pub index_id: String,
    #[serde(default = "default_commit_mode")]
    pub ingest_commit_mode: CommitMode,
    #[serde(default = "default_ingest_batch_size_docs")]
    pub ingest_batch_size_docs: usize,
    #[serde(default = "default_ingest_batch_max_bytes")]
    pub ingest_batch_max_bytes: usize,
    #[serde(default = "default_http_timeout_secs")]
    pub http_timeout_secs: u64,
    #[serde(default)]
    pub http_retry: HttpRetryConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitMode {
    Auto,
    WaitFor,
    Force,
}

impl CommitMode {
    pub fn as_query_param(&self) -> &'static str {
        match self {
            CommitMode::Auto => "auto",
            CommitMode::WaitFor => "wait_for",
            CommitMode::Force => "force",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_true")]
    pub respect_429: bool,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            respect_429: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeSection {
    pub state_store_path: PathBuf,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub reorg: ReorgConfig,
    #[serde(default = "default_metrics_listen_addr")]
    pub metrics_listen_addr: String,
    /// How many recently emitted doc IDs to retain for the dedup window,
    /// expressed in blocks. Entries older than `cursor - dedup_window_blocks`
    /// are pruned.
    #[serde(default = "default_dedup_window_blocks")]
    pub dedup_window_blocks: u64,
    /// Cap on `entity_key:X OR …` terms bundled into one delete task.
    #[serde(default = "default_max_delete_terms_per_task")]
    pub max_delete_terms_per_task: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_sample_every_n_blocks")]
    pub sample_every_n_blocks: u64,
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
    /// Halt after this many cumulative mismatches. `1` = halt on first.
    #[serde(default = "default_mismatch_halt_threshold")]
    pub mismatch_halt_threshold: u64,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_every_n_blocks: default_sample_every_n_blocks(),
            sample_size: default_sample_size(),
            mismatch_halt_threshold: default_mismatch_halt_threshold(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReorgConfig {
    #[serde(default = "default_retract_max_depth")]
    pub retract_max_depth: u64,
    /// Size of the rolling `(block, hash)` buffer.
    #[serde(default = "default_reorg_buffer_size")]
    pub buffer_size: u64,
    /// How many buffered blocks to re-check per fetch iteration.
    #[serde(default = "default_recheck_sample_size")]
    pub recheck_sample_size: usize,
}

impl Default for ReorgConfig {
    fn default() -> Self {
        Self {
            retract_max_depth: default_retract_max_depth(),
            buffer_size: default_reorg_buffer_size(),
            recheck_sample_size: default_recheck_sample_size(),
        }
    }
}

/// One `content_type → extraction strategy` routing rule (§11 of the design).
/// Exactly one of `content_type` / `content_type_prefix` must be set.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadExtractorRule {
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub content_type_prefix: Option<String>,
    pub strategy: ExtractorStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractorStrategy {
    Utf8Body,
    Utf8BodyStripMarkdown,
    JsonFlatten,
    HtmlStripTags,
    TryUtf8,
    None,
}

impl BridgeConfig {
    /// Loads and validates the config from a YAML file.
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|io_error| {
            BridgeError::Config(format!("cannot read config file {}: {io_error}", path.display()))
        })?;
        let config: BridgeConfig = serde_yaml::from_str(&raw)
            .map_err(|yaml_error| BridgeError::Config(format!("invalid config: {yaml_error}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Startup validation: refuse to run with malformed URLs, a zero batch
    /// size, or ambiguous extractor rules.
    pub fn validate(&self) -> Result<()> {
        validate_url("arkiv.rpc_url", &self.arkiv.rpc_url)?;
        if let Some(backup_url) = &self.arkiv.rpc_url_backup {
            validate_url("arkiv.rpc_url_backup", backup_url)?;
        }
        validate_url("quickwit.base_url", &self.quickwit.base_url)?;
        let resolved_address = self.arkiv.resolve_address();
        if !resolved_address.starts_with("0x") || resolved_address.len() != 42 {
            return Err(BridgeError::Config(format!(
                "arkiv.arkiv_address `{resolved_address}` is not a 20-byte hex address"
            )));
        }
        if self.quickwit.index_id.is_empty() {
            return Err(BridgeError::Config("quickwit.index_id is empty".to_string()));
        }
        if self.arkiv.fetch_batch_size_blocks == 0 {
            return Err(BridgeError::Config(
                "arkiv.fetch_batch_size_blocks must be positive".to_string(),
            ));
        }
        if self.quickwit.ingest_batch_size_docs == 0 || self.quickwit.ingest_batch_max_bytes == 0 {
            return Err(BridgeError::Config(
                "quickwit ingest batch limits must be positive".to_string(),
            ));
        }
        if self.bridge.max_delete_terms_per_task == 0 {
            return Err(BridgeError::Config(
                "bridge.max_delete_terms_per_task must be positive".to_string(),
            ));
        }
        for (rule_index, rule) in self.payload_extractors.iter().enumerate() {
            let has_exact = rule.content_type.is_some();
            let has_prefix = rule.content_type_prefix.is_some();
            if has_exact == has_prefix {
                return Err(BridgeError::Config(format!(
                    "payload_extractors[{rule_index}]: exactly one of `content_type` / \
                     `content_type_prefix` must be set"
                )));
            }
        }
        for (rule_index, rule) in self.entity_filters.iter().enumerate() {
            let conditions = rule.conditions();
            if conditions.is_empty() {
                return Err(BridgeError::Config(format!(
                    "entity_filters[{rule_index}]: all_of list is empty"
                )));
            }
            for condition in conditions {
                if condition.string_annotation_key.is_empty() {
                    return Err(BridgeError::Config(format!(
                        "entity_filters[{rule_index}]: string_annotation_key is empty"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn validate_url(field: &str, url: &str) -> Result<()> {
    if url.starts_with("http://") || url.starts_with("https://") {
        return Ok(());
    }
    Err(BridgeError::Config(format!(
        "{field} `{url}` must start with http:// or https://"
    )))
}

fn default_confirmation_depth() -> u64 {
    32
}
fn default_fetch_batch_size_blocks() -> u64 {
    1024
}
fn default_http_timeout_secs() -> u64 {
    30
}
fn default_commit_mode() -> CommitMode {
    CommitMode::Auto
}
fn default_ingest_batch_size_docs() -> usize {
    500
}
fn default_ingest_batch_max_bytes() -> usize {
    1_048_576
}
fn default_max_attempts() -> u32 {
    8
}
fn default_initial_backoff_ms() -> u64 {
    200
}
fn default_max_backoff_ms() -> u64 {
    30_000
}
fn default_true() -> bool {
    true
}
fn default_poll_interval_ms() -> u64 {
    2_000
}
fn default_metrics_listen_addr() -> String {
    "127.0.0.1:9464".to_string()
}
fn default_dedup_window_blocks() -> u64 {
    100_000
}
fn default_max_delete_terms_per_task() -> usize {
    500
}
fn default_sample_every_n_blocks() -> u64 {
    10_000
}
fn default_sample_size() -> usize {
    20
}
fn default_mismatch_halt_threshold() -> u64 {
    1
}
fn default_retract_max_depth() -> u64 {
    512
}
fn default_reorg_buffer_size() -> u64 {
    256
}
fn default_recheck_sample_size() -> usize {
    8
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_CONFIG: &str = r#"
arkiv:
  rpc_url: "http://127.0.0.1:8545"
  chain_id: 42069
quickwit:
  base_url: "http://127.0.0.1:7280"
  index_id: "arkiv"
bridge:
  state_store_path: "/tmp/bridge-state.sqlite"
"#;

    #[test]
    fn minimal_config_parses_with_defaults() {
        let config: BridgeConfig = serde_yaml::from_str(MINIMAL_CONFIG).unwrap();
        config.validate().unwrap();
        assert_eq!(config.arkiv.confirmation_depth, 32);
        assert_eq!(config.arkiv.schema, EventSchema::EntityOperation);
        assert_eq!(
            config.arkiv.resolve_address(),
            "0x4400000000000000000000000000000000000044"
        );
        assert_eq!(config.quickwit.ingest_commit_mode, CommitMode::Auto);
        assert_eq!(config.quickwit.ingest_batch_size_docs, 500);
        assert_eq!(config.bridge.reorg.retract_max_depth, 512);
        assert!(config.bridge.audit.enabled);
    }

    #[test]
    fn legacy_schema_resolves_legacy_address() {
        let legacy =
            MINIMAL_CONFIG.replace("chain_id: 42069", "chain_id: 42069\n  schema: legacy");
        let config: BridgeConfig = serde_yaml::from_str(&legacy).unwrap();
        assert_eq!(config.arkiv.schema, EventSchema::Legacy);
        assert_eq!(
            config.arkiv.resolve_address(),
            "0x00000000000000000000000000000061726b6976"
        );
        // Explicit address overrides the schema default.
        let with_addr = legacy.replace(
            "schema: legacy",
            "schema: legacy\n  arkiv_address: \"0x1111111111111111111111111111111111111111\"",
        );
        let config: BridgeConfig = serde_yaml::from_str(&with_addr).unwrap();
        assert_eq!(
            config.arkiv.resolve_address(),
            "0x1111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn start_block_parses_number_and_head() {
        let config: BridgeConfig = serde_yaml::from_str(MINIMAL_CONFIG).unwrap();
        assert_eq!(config.arkiv.start_block, StartBlock::Genesis);

        let numbered = MINIMAL_CONFIG.replace("chain_id: 42069", "chain_id: 42069\n  start_block: 123");
        let config: BridgeConfig = serde_yaml::from_str(&numbered).unwrap();
        assert_eq!(config.arkiv.start_block, StartBlock::Number(123));

        let zero = MINIMAL_CONFIG.replace("chain_id: 42069", "chain_id: 42069\n  start_block: 0");
        let config: BridgeConfig = serde_yaml::from_str(&zero).unwrap();
        assert_eq!(config.arkiv.start_block, StartBlock::Genesis);

        let head = MINIMAL_CONFIG.replace("chain_id: 42069", "chain_id: 42069\n  start_block: \"head\"");
        let config: BridgeConfig = serde_yaml::from_str(&head).unwrap();
        assert_eq!(config.arkiv.start_block, StartBlock::Head);

        let bogus = MINIMAL_CONFIG.replace("chain_id: 42069", "chain_id: 42069\n  start_block: \"tip\"");
        assert!(serde_yaml::from_str::<BridgeConfig>(&bogus).is_err());
    }

    #[test]
    fn rejects_bad_rpc_url() {
        let mut config: BridgeConfig = serde_yaml::from_str(MINIMAL_CONFIG).unwrap();
        config.arkiv.rpc_url = "ftp://nope".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_ambiguous_extractor_rule() {
        let mut config: BridgeConfig = serde_yaml::from_str(MINIMAL_CONFIG).unwrap();
        config.payload_extractors.push(PayloadExtractorRule {
            content_type: Some("text/plain".to_string()),
            content_type_prefix: Some("text/".to_string()),
            strategy: ExtractorStrategy::Utf8Body,
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn entity_filters_parse_flat_and_all_of() {
        let yaml = format!(
            "{MINIMAL_CONFIG}
entity_filters:
  - string_annotation_key: type
  - string_annotation_key: type
    string_annotation_value: w3pups-telemetry
  - all_of:
      - string_annotation_key: type
        string_annotation_value: w3pups-telemetry
      - string_annotation_key: device_id
"
        );
        let config: BridgeConfig = serde_yaml::from_str(&yaml).unwrap();
        config.validate().unwrap();
        assert_eq!(config.entity_filters.len(), 3);
        assert_eq!(config.entity_filters[0].conditions().len(), 1);
        assert_eq!(
            config.entity_filters[0].conditions()[0].string_annotation_value,
            None
        );
        assert_eq!(
            config.entity_filters[1].conditions()[0]
                .string_annotation_value
                .as_deref(),
            Some("w3pups-telemetry")
        );
        assert_eq!(config.entity_filters[2].conditions().len(), 2);
    }

    #[test]
    fn entity_filters_reject_empty_key_and_empty_all_of() {
        let empty_key = format!(
            "{MINIMAL_CONFIG}\nentity_filters:\n  - string_annotation_key: \"\"\n"
        );
        let config: BridgeConfig = serde_yaml::from_str(&empty_key).unwrap();
        assert!(config.validate().is_err());

        let empty_all_of = format!("{MINIMAL_CONFIG}\nentity_filters:\n  - all_of: []\n");
        let config: BridgeConfig = serde_yaml::from_str(&empty_all_of).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_strategy() {
        let yaml = format!(
            "{MINIMAL_CONFIG}\npayload_extractors:\n  - content_type: text/plain\n    strategy: \
             bogus\n"
        );
        assert!(serde_yaml::from_str::<BridgeConfig>(&yaml).is_err());
    }
}
