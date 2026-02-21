use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::management::read_process_stats;

/// Configuration for the metrics collector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// How often to sample metrics (seconds). Default: 30.
    #[serde(default = "default_sample_interval")]
    pub sample_interval_secs: u64,
    /// How long to retain samples (seconds). Default: 18000 (5 hours).
    #[serde(default = "default_retention")]
    pub retention_secs: u64,
}

fn default_sample_interval() -> u64 {
    5
}
fn default_retention() -> u64 {
    18000
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            sample_interval_secs: default_sample_interval(),
            retention_secs: default_retention(),
        }
    }
}

/// A single metrics snapshot at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSample {
    /// Unix timestamp (seconds).
    pub timestamp: i64,
    pub cpu_percent: f64,
    pub rss_bytes: u64,
    pub vms_bytes: u64,
    pub threads: u32,
    pub open_fds: u32,
    pub request_count: u64,
}

/// Collects time-series metrics and persists them to an append-only JSONL file.
///
/// On startup, loads existing samples from disk and prunes anything older than
/// the retention window. New samples are appended as single JSON lines.
/// The file is periodically compacted to remove expired entries.
pub struct MetricsCollector {
    samples: Arc<RwLock<VecDeque<MetricsSample>>>,
    request_counter: Arc<AtomicU64>,
    config: MetricsConfig,
    file_path: PathBuf,
}

impl MetricsCollector {
    /// Create a new collector, load history from disk, and start background sampling.
    pub fn new(config: MetricsConfig, data_dir: &std::path::Path) -> Arc<Self> {
        let file_path = data_dir.join("metrics.jsonl");

        // Load existing samples from disk.
        let existing = load_from_file(&file_path, &config);

        let collector = Arc::new(Self {
            samples: Arc::new(RwLock::new(VecDeque::from(existing))),
            request_counter: Arc::new(AtomicU64::new(0)),
            config,
            file_path,
        });

        // Start background sampling task.
        let c = collector.clone();
        tokio::spawn(async move {
            // Take an initial sample immediately.
            c.take_sample().await;

            let interval =
                tokio::time::Duration::from_secs(c.config.sample_interval_secs.max(1));
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // skip the first immediate tick

            let mut compaction_counter: u32 = 0;
            loop {
                tick.tick().await;
                c.take_sample().await;

                // Compact the file every 60 samples (~30 min at default interval)
                // to prune expired entries and keep the file small.
                compaction_counter += 1;
                if compaction_counter >= 60 {
                    compaction_counter = 0;
                    c.compact_file().await;
                }
            }
        });

        collector
    }

    /// Increment the request counter (call on every incoming request).
    pub fn increment_requests(&self) {
        self.request_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Return a snapshot of all stored samples.
    pub async fn snapshot(&self) -> Vec<MetricsSample> {
        self.samples.read().await.iter().cloned().collect()
    }

    /// Return the current config so the UI knows interval/retention.
    pub fn config(&self) -> &MetricsConfig {
        &self.config
    }

    async fn take_sample(&self) {
        let stats = read_process_stats();
        let sample = MetricsSample {
            timestamp: chrono::Utc::now().timestamp(),
            cpu_percent: stats.cpu_percent,
            rss_bytes: stats.rss_bytes,
            vms_bytes: stats.vms_bytes,
            threads: stats.threads,
            open_fds: stats.open_fds,
            request_count: self.request_counter.load(Ordering::Relaxed),
        };

        let max_samples =
            (self.config.retention_secs / self.config.sample_interval_secs.max(1)) as usize + 1;

        // Append to file first (single line, fast).
        append_to_file(&self.file_path, &sample);

        // Then update in-memory buffer.
        let mut buf = self.samples.write().await;
        if buf.len() >= max_samples {
            buf.pop_front();
        }
        buf.push_back(sample);
    }

    /// Rewrite the file with only current (non-expired) samples.
    async fn compact_file(&self) {
        let samples = self.samples.read().await;
        let cutoff = chrono::Utc::now().timestamp() - self.config.retention_secs as i64;
        let valid: Vec<&MetricsSample> = samples.iter().filter(|s| s.timestamp >= cutoff).collect();

        let mut content = String::new();
        for s in &valid {
            if let Ok(line) = serde_json::to_string(s) {
                content.push_str(&line);
                content.push('\n');
            }
        }

        if let Err(e) = std::fs::write(&self.file_path, content) {
            tracing::warn!(error = %e, "Failed to compact metrics file");
        }
    }
}

/// Append a single sample as a JSON line to the file.
fn append_to_file(path: &std::path::Path, sample: &MetricsSample) {
    use std::io::Write;
    if let Ok(line) = serde_json::to_string(sample) {
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(f, "{}", line);
        }
    }
}

/// Load samples from an existing JSONL file, pruning anything outside the retention window.
fn load_from_file(path: &std::path::Path, config: &MetricsConfig) -> Vec<MetricsSample> {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let cutoff = chrono::Utc::now().timestamp() - config.retention_secs as i64;
    let max_samples =
        (config.retention_secs / config.sample_interval_secs.max(1)) as usize + 1;

    let mut samples: Vec<MetricsSample> = contents
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|s: &MetricsSample| s.timestamp >= cutoff)
        .collect();

    // Keep only the most recent max_samples.
    if samples.len() > max_samples {
        samples.drain(0..samples.len() - max_samples);
    }

    tracing::info!(
        count = samples.len(),
        file = %path.display(),
        "Loaded metrics history from disk"
    );

    samples
}
