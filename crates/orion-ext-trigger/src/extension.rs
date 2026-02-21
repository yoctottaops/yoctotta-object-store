use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use orion_core::extension::*;
use orion_core::types::*;
use orion_core::Result;

use crate::types::*;

pub struct TriggerExtension {
    config: TriggerConfig,
    client: reqwest::Client,
    semaphore: Arc<Semaphore>,
    enabled: bool,
}

impl TriggerExtension {
    pub fn new() -> Self {
        let config = TriggerConfig::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .expect("Failed to build HTTP client");
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));

        Self {
            config,
            client,
            semaphore,
            enabled: true,
        }
    }

    /// Find all rules that match a given event.
    fn matching_rules(&self, event: &StorageEvent) -> Vec<&TriggerRule> {
        let event_name = event.event_name();
        let bucket = event.bucket();

        let (key, _meta) = match event {
            StorageEvent::ObjectCreated { key, meta } => {
                (Some(key.key.as_str()), Some(meta))
            }
            StorageEvent::ObjectDeleted { key } => (Some(key.key.as_str()), None),
            StorageEvent::ObjectAccessed { key } => (Some(key.key.as_str()), None),
            _ => (None, None),
        };

        self.config
            .rules
            .iter()
            .filter(|rule| {
                if !rule.enabled {
                    return false;
                }

                // Check event name match.
                if !rule.events.iter().any(|e| e == event_name || e == "*") {
                    return false;
                }

                // Check bucket filter.
                if let Some(ref rule_bucket) = rule.bucket {
                    if rule_bucket != bucket {
                        return false;
                    }
                }

                // Check key prefix filter.
                if let Some(ref prefix) = rule.key_prefix {
                    if let Some(k) = key {
                        if !k.starts_with(prefix.as_str()) {
                            return false;
                        }
                    }
                }

                // Check key suffix filter.
                if let Some(ref suffix) = rule.key_suffix {
                    if let Some(k) = key {
                        if !k.ends_with(suffix.as_str()) {
                            return false;
                        }
                    }
                }

                true
            })
            .collect()
    }

    /// Fire a webhook for a matched rule.
    async fn fire_webhook(
        &self,
        rule: &TriggerRule,
        event: &StorageEvent,
    ) -> std::result::Result<(), String> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| format!("Semaphore error: {}", e))?;

        let (key, size, etag, content_type) = match event {
            StorageEvent::ObjectCreated { key, meta } => (
                Some(key.key.clone()),
                Some(meta.size),
                Some(meta.etag.clone()),
                Some(meta.content_type.clone()),
            ),
            StorageEvent::ObjectDeleted { key } => {
                (Some(key.key.clone()), None, None, None)
            }
            StorageEvent::ObjectAccessed { key } => {
                (Some(key.key.clone()), None, None, None)
            }
            _ => (None, None, None, None),
        };

        let payload = WebhookPayload {
            event: event.event_name().to_string(),
            timestamp: chrono::Utc::now(),
            bucket: event.bucket().to_string(),
            key,
            size,
            etag,
            content_type,
            rule: rule.name.clone(),
            delivery_id: uuid::Uuid::new_v4().to_string(),
        };

        let mut retries = 0;
        loop {
            let mut req = match rule.method.to_uppercase().as_str() {
                "GET" => self.client.get(&rule.url),
                "PUT" => self.client.put(&rule.url),
                _ => self.client.post(&rule.url),
            };

            // Add custom headers.
            for (k, v) in &rule.headers {
                req = req.header(k, v);
            }

            let result = req
                .json(&payload)
                .header("X-Orion-Event", event.event_name())
                .header("X-Orion-Delivery", &payload.delivery_id)
                .send()
                .await;

            match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        tracing::info!(
                            rule = rule.name,
                            url = rule.url,
                            status = status.as_u16(),
                            delivery_id = payload.delivery_id,
                            "Webhook delivered"
                        );
                        return Ok(());
                    } else if status.is_server_error() && retries < self.config.retries {
                        retries += 1;
                        let delay = Duration::from_millis(100 * 2u64.pow(retries));
                        tracing::warn!(
                            rule = rule.name,
                            status = status.as_u16(),
                            retry = retries,
                            "Webhook failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    } else {
                        let msg = format!("Webhook returned {}", status);
                        tracing::error!(rule = rule.name, url = rule.url, msg);
                        return Err(msg);
                    }
                }
                Err(e) => {
                    if retries < self.config.retries {
                        retries += 1;
                        let delay = Duration::from_millis(100 * 2u64.pow(retries));
                        tracing::warn!(
                            rule = rule.name,
                            error = %e,
                            retry = retries,
                            "Webhook request failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    let msg = format!("Webhook request failed: {}", e);
                    tracing::error!(rule = rule.name, url = rule.url, msg);
                    return Err(msg);
                }
            }
        }
    }
}

impl Default for TriggerExtension {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Extension for TriggerExtension {
    fn info(&self) -> ExtensionInfo {
        ExtensionInfo {
            name: "trigger".into(),
            version: "0.1.0".into(),
            description: "Webhook triggers on storage events".into(),
            enabled: self.enabled,
        }
    }

    async fn init(&mut self, config: &toml::Value) -> Result<()> {
        if let Some(trigger_config) = config.get("trigger") {
            self.config = trigger_config
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| orion_core::OrionError::Extension {
                    extension: "trigger".into(),
                    message: format!("Invalid config: {}", e),
                })?;

            // Rebuild client with new timeout.
            self.client = reqwest::Client::builder()
                .timeout(Duration::from_secs(self.config.timeout_secs))
                .build()
                .map_err(|e| orion_core::OrionError::Extension {
                    extension: "trigger".into(),
                    message: format!("Failed to build HTTP client: {}", e),
                })?;

            self.semaphore = Arc::new(Semaphore::new(self.config.max_concurrent));
        }

        tracing::info!(
            rules = self.config.rules.len(),
            "Trigger extension initialized"
        );
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        tracing::info!("Trigger extension shutting down");
        Ok(())
    }

    fn hooks(&self) -> &dyn ExtensionHooks {
        self
    }
}

#[async_trait]
impl ExtensionHooks for TriggerExtension {
    async fn on_event(&self, event: &StorageEvent) -> Result<()> {
        let rules = self.matching_rules(event);
        if rules.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            event = event.event_name(),
            matched_rules = rules.len(),
            "Dispatching triggers"
        );

        // Fire all matching webhooks concurrently.
        let futures: Vec<_> = rules
            .iter()
            .map(|rule| self.fire_webhook(rule, event))
            .collect();

        let results = futures::future::join_all(futures).await;

        let failures: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
        if !failures.is_empty() {
            tracing::warn!(
                total = results.len(),
                failed = failures.len(),
                "Some webhooks failed"
            );
        }

        Ok(())
    }
}
