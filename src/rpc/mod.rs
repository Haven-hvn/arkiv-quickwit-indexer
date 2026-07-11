//! JSON-RPC clients: the standard `eth_*` namespace and the arkiv-specific
//! `arkiv_*` namespace, both over plain HTTP POST.

mod arkiv;
mod eth;

pub use arkiv::{
    ATTR_ENTITY_KEY, ATTR_STRING, ATTR_UINT, ArkivClient, EntityData, entity_address,
};
#[cfg(test)]
pub use arkiv::{EntityAttribute, LegacyAttribute};
pub use eth::{EthClient, EthLog};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::{BridgeError, Result};

/// Minimal JSON-RPC 2.0 transport shared by both namespaces.
#[derive(Clone)]
pub struct JsonRpcTransport {
    http_client: reqwest::Client,
    url: String,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<JsonRpcErrorBody>,
}

#[derive(Deserialize)]
struct JsonRpcErrorBody {
    code: i64,
    message: String,
}

impl JsonRpcTransport {
    pub fn new(url: &str, timeout: std::time::Duration) -> Result<Self> {
        let http_client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            http_client,
            url: url.to_string(),
        })
    }

    /// Issues one JSON-RPC call and returns the `result` value.
    pub async fn call(&self, method: &str, rpc_params: Value) -> Result<Value> {
        let call_result = self.call_inner(method, rpc_params).await;
        if call_result.is_err() {
            crate::metrics::METRICS
                .arkiv_rpc_errors
                .with_label_values(&[method])
                .inc();
        }
        call_result
    }

    async fn call_inner(&self, method: &str, rpc_params: Value) -> Result<Value> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": rpc_params,
        });
        let response = self
            .http_client
            .post(&self.url)
            .json(&request_body)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BridgeError::ArkivRpc(format!(
                "{method}: http {status}: {body}"
            )));
        }
        let rpc_response: JsonRpcResponse = response.json().await?;
        if let Some(rpc_error) = rpc_response.error {
            return Err(BridgeError::ArkivRpc(format!(
                "{method}: rpc error {}: {}",
                rpc_error.code, rpc_error.message
            )));
        }
        rpc_response
            .result
            .ok_or_else(|| BridgeError::ArkivRpc(format!("{method}: response missing result")))
    }
}

/// Parses `"0x1a"` (or a bare JSON number) into a u64.
pub(crate) fn parse_hex_u64(value: &Value) -> Result<u64> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    let text = value
        .as_str()
        .ok_or_else(|| BridgeError::ArkivRpc(format!("expected hex quantity, got {value}")))?;
    let stripped = text.strip_prefix("0x").unwrap_or(text);
    u64::from_str_radix(stripped, 16)
        .map_err(|parse_error| BridgeError::ArkivRpc(format!("invalid hex u64 {text:?}: {parse_error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_and_numeric_quantities() {
        assert_eq!(parse_hex_u64(&json!("0x1a")).unwrap(), 26);
        assert_eq!(parse_hex_u64(&json!(26)).unwrap(), 26);
        assert!(parse_hex_u64(&json!("zz")).is_err());
        assert!(parse_hex_u64(&json!(null)).is_err());
    }
}
