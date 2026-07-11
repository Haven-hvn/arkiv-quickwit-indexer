//! Quickwit HTTP client: the ingest API and the delete-task API — the only
//! two Quickwit surfaces the bridge touches (§5 of the design).

use std::time::Duration;

use serde_json::json;
use tracing::warn;

use crate::config::{CommitMode, HttpRetryConfig, QuickwitConfig};
use crate::error::{BridgeError, Result};
use crate::metrics::METRICS;

#[derive(Clone)]
pub struct QuickwitClient {
    http_client: reqwest::Client,
    base_url: String,
    index_id: String,
    commit_mode: CommitMode,
    retry: HttpRetryConfig,
}

impl QuickwitClient {
    pub fn new(config: &QuickwitConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.http_timeout_secs))
            .build()?;
        Ok(Self {
            http_client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            index_id: config.index_id.clone(),
            commit_mode: config.ingest_commit_mode,
            retry: config.http_retry.clone(),
        })
    }

    /// POSTs one ND-JSON payload to `/api/v1/{index}/ingest`, retrying on
    /// 429 (backpressure) and 5xx with exponential backoff and jitter.
    pub async fn ingest(&self, ndjson_payload: String) -> Result<()> {
        let url = format!(
            "{}/api/v1/{}/ingest?commit={}",
            self.base_url,
            self.index_id,
            self.commit_mode.as_query_param()
        );
        self.post_with_retry(&url, ndjson_payload, "application/x-ndjson")
            .await
    }

    /// POSTs a delete task to `/api/v1/{index}/delete-tasks`. Repeating the
    /// same delete query is a no-op, so this is safe to retry blindly.
    pub async fn create_delete_task(&self, query: &str) -> Result<()> {
        let url = format!("{}/api/v1/{}/delete-tasks", self.base_url, self.index_id);
        let body = serde_json::to_string(&json!({ "query": query }))?;
        self.post_with_retry(&url, body, "application/json").await
    }

    /// Startup check: the index must exist before the bridge starts
    /// emitting. Fails fast with a pointer at the shipped index config.
    pub async fn check_index_exists(&self) -> Result<()> {
        let url = format!("{}/api/v1/indexes/{}", self.base_url, self.index_id);
        let response = self.http_client.get(&url).send().await?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        Err(BridgeError::QuickwitHttp { status, body })
    }

    async fn post_with_retry(&self, url: &str, body: String, content_type: &str) -> Result<()> {
        let mut backoff_ms = self.retry.initial_backoff_ms;
        let mut last_error: Option<BridgeError> = None;
        for attempt in 1..=self.retry.max_attempts {
            let send_result = self
                .http_client
                .post(url)
                .header("content-type", content_type)
                .body(body.clone())
                .send()
                .await;
            match send_result {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status().as_u16();
                    METRICS.quickwit_http_errors.with_label_values(&[&status.to_string()]).inc();
                    let response_body = response.text().await.unwrap_or_default();
                    let retryable = status >= 500 || (status == 429 && self.retry.respect_429);
                    let error = BridgeError::QuickwitHttp {
                        status,
                        body: response_body,
                    };
                    if !retryable {
                        return Err(error);
                    }
                    warn!(url, status, attempt, "quickwit returned retryable error, backing off");
                    last_error = Some(error);
                }
                Err(transport_error) => {
                    METRICS.quickwit_http_errors.with_label_values(&["transport"]).inc();
                    warn!(url, attempt, %transport_error, "quickwit request failed, backing off");
                    last_error = Some(BridgeError::Transport(transport_error));
                }
            }
            let jitter_ms = jitter(backoff_ms / 4);
            tokio::time::sleep(Duration::from_millis(backoff_ms + jitter_ms)).await;
            backoff_ms = (backoff_ms * 2).min(self.retry.max_backoff_ms);
        }
        Err(last_error.unwrap_or_else(|| {
            BridgeError::Other("quickwit retry loop exhausted with no attempts".to_string())
        }))
    }
}

fn jitter(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }
    use rand::Rng;
    rand::thread_rng().gen_range(0..max_ms)
}
