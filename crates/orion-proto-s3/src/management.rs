use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use serde::Serialize;

use crate::handler::S3Handler;
use crate::metrics::MetricsConfig;

const UI_HTML: &str = include_str!("ui.html");

// ── Process metrics (Linux /proc) ──

#[derive(Serialize, Default)]
pub(crate) struct ProcessStats {
    pub(crate) pid: u32,
    pub(crate) uptime_secs: f64,
    pub(crate) rss_bytes: u64,
    pub(crate) vms_bytes: u64,
    pub(crate) cpu_percent: f64,
    pub(crate) cpu_user_secs: f64,
    pub(crate) cpu_system_secs: f64,
    pub(crate) threads: u32,
    pub(crate) open_fds: u32,
}

pub(crate) fn read_process_stats() -> ProcessStats {
    let mut stats = ProcessStats {
        pid: std::process::id(),
        ..Default::default()
    };

    // Read /proc/self/status for memory and thread info.
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(val) = line.strip_prefix("VmRSS:") {
                stats.rss_bytes = parse_kb(val) * 1024;
            } else if let Some(val) = line.strip_prefix("VmSize:") {
                stats.vms_bytes = parse_kb(val) * 1024;
            } else if let Some(val) = line.strip_prefix("Threads:") {
                stats.threads = val.trim().parse().unwrap_or(0);
            }
        }
    }

    // Read /proc/self/stat for CPU times and start time.
    let clock_ticks = 100_f64; // sysconf(_SC_CLK_TCK) is almost always 100 on Linux
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        // Fields are space-separated, but field 2 (comm) is in parens and may contain spaces.
        // Find the last ')' to skip past the comm field.
        if let Some(after_comm) = stat.rfind(')') {
            let fields: Vec<&str> = stat[after_comm + 2..].split_whitespace().collect();
            // Index 0 = state, 1 = ppid, ..., 11 = utime, 12 = stime, ..., 19 = starttime
            if fields.len() > 19 {
                let utime: f64 = fields[11].parse().unwrap_or(0.0);
                let stime: f64 = fields[12].parse().unwrap_or(0.0);
                let starttime: f64 = fields[19].parse().unwrap_or(0.0);

                stats.cpu_user_secs = utime / clock_ticks;
                stats.cpu_system_secs = stime / clock_ticks;

                // Process uptime from /proc/uptime (system uptime in seconds).
                if let Ok(uptime_str) = std::fs::read_to_string("/proc/uptime") {
                    if let Some(sys_uptime_str) = uptime_str.split_whitespace().next() {
                        let sys_uptime: f64 = sys_uptime_str.parse().unwrap_or(0.0);
                        let proc_start_secs = starttime / clock_ticks;
                        stats.uptime_secs = (sys_uptime - proc_start_secs).max(0.0);

                        // CPU usage = total cpu time / wall time * 100.
                        if stats.uptime_secs > 0.0 {
                            stats.cpu_percent =
                                ((utime + stime) / clock_ticks / stats.uptime_secs) * 100.0;
                        }
                    }
                }
            }
        }
    }

    // Count open file descriptors.
    if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
        stats.open_fds = entries.count() as u32;
    }

    stats
}

fn parse_kb(s: &str) -> u64 {
    s.trim().trim_end_matches("kB").trim().parse().unwrap_or(0)
}

// ── JSON response helpers ──

fn json_response<T: Serialize>(status: u16, data: &T) -> Response<Full<Bytes>> {
    let body = serde_json::to_string(data).unwrap_or_else(|_| "{}".into());
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn json_error(status: u16, message: &str) -> Response<Full<Bytes>> {
    #[derive(Serialize)]
    struct ErrorBody<'a> {
        error: &'a str,
    }
    json_response(status, &ErrorBody { error: message })
}

// ── Response types ──

#[derive(Serialize)]
struct StatsResponse {
    process: ProcessStats,
    storage: StorageStats,
    buckets: Vec<BucketStats>,
    metrics_config: MetricsConfig,
}

#[derive(Serialize)]
struct StorageStats {
    backend: String,
    total_bytes: u64,
    used_bytes: u64,
    object_count: u64,
    is_healthy: bool,
}

#[derive(Serialize)]
struct BucketStats {
    name: String,
    created_at: String,
    object_count: u64,
    total_size: u64,
}

#[derive(Serialize)]
struct ExtensionResponse {
    extensions: Vec<ExtensionStatus>,
}

#[derive(Serialize)]
struct ExtensionStatus {
    name: String,
    version: String,
    description: String,
    enabled: bool,
    healthy: bool,
}

#[derive(Serialize)]
struct SearchResponse {
    results: Vec<SearchHitJson>,
    query: String,
}

#[derive(Serialize)]
struct SearchHitJson {
    id: String,
    bucket: String,
    key: String,
    chunk_index: u32,
    text: String,
    score: f32,
}

// ── Handlers ──

pub fn handle_ui() -> Response<Full<Bytes>> {
    Response::builder()
        .status(200)
        .header("Content-Type", "text/html; charset=utf-8")
        .header("Cache-Control", "no-cache")
        .body(Full::new(Bytes::from(UI_HTML)))
        .unwrap()
}

pub fn handle_cors() -> Response<Full<Bytes>> {
    Response::builder()
        .status(204)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type, x-amz-meta-*")
        .header("Access-Control-Max-Age", "86400")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

pub async fn handle_api_stats(handler: &S3Handler) -> Response<Full<Bytes>> {
    let backend_stats = match handler.store.stats().await {
        Ok(s) => s,
        Err(e) => return json_error(500, &format!("Failed to get stats: {}", e)),
    };

    let bucket_list = match handler.buckets.list_buckets().await {
        Ok(b) => b,
        Err(e) => return json_error(500, &format!("Failed to list buckets: {}", e)),
    };

    let mut buckets = Vec::new();
    for b in &bucket_list {
        let count = handler.meta.object_count(&b.name).await.unwrap_or(0);
        let size = handler.meta.total_size(&b.name).await.unwrap_or(0);
        buckets.push(BucketStats {
            name: b.name.clone(),
            created_at: b.created_at.to_rfc3339(),
            object_count: count,
            total_size: size,
        });
    }

    let resp = StatsResponse {
        process: read_process_stats(),
        storage: StorageStats {
            backend: handler.store.name().to_string(),
            total_bytes: backend_stats.total_bytes,
            used_bytes: backend_stats.used_bytes,
            object_count: backend_stats.object_count,
            is_healthy: backend_stats.is_healthy,
        },
        buckets,
        metrics_config: handler.metrics.config().clone(),
    };

    json_response(200, &resp)
}

pub async fn handle_api_extensions(handler: &S3Handler) -> Response<Full<Bytes>> {
    let health_results = handler.extensions.health_all().await;
    let extensions: Vec<ExtensionStatus> = health_results
        .into_iter()
        .map(|(info, healthy)| ExtensionStatus {
            name: info.name,
            version: info.version,
            description: info.description,
            enabled: info.enabled,
            healthy,
        })
        .collect();

    json_response(200, &ExtensionResponse { extensions })
}

pub async fn handle_api_search(
    handler: &S3Handler,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let has_search = handler
        .extensions
        .list()
        .iter()
        .any(|e| e.name == "rag" && e.enabled);
    if !has_search {
        return json_error(404, "RAG extension is not enabled");
    }

    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return json_error(400, &format!("Failed to read body: {}", e)),
    };

    #[derive(serde::Deserialize)]
    struct SearchRequest {
        query: String,
        #[serde(default = "default_top_k")]
        top_k: usize,
        bucket: Option<String>,
    }
    fn default_top_k() -> usize {
        10
    }

    let search_req: SearchRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("Invalid JSON: {}", e)),
    };

    match handler
        .extensions
        .search(&search_req.query, search_req.top_k, search_req.bucket.as_deref())
        .await
    {
        Ok(results) => {
            let hits: Vec<SearchHitJson> = results
                .into_iter()
                .map(|r| SearchHitJson {
                    id: r.id,
                    bucket: r.bucket,
                    key: r.key,
                    chunk_index: r.chunk_index,
                    text: r.text,
                    score: r.score,
                })
                .collect();
            json_response(
                200,
                &SearchResponse {
                    results: hits,
                    query: search_req.query,
                },
            )
        }
        Err(e) => json_error(500, &format!("Search failed: {}", e)),
    }
}

pub async fn handle_api_metrics(handler: &S3Handler) -> Response<Full<Bytes>> {
    let samples = handler.metrics.snapshot().await;
    json_response(200, &samples)
}

pub async fn handle_api_login(
    handler: &S3Handler,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let auth = match &handler.auth {
        Some(a) => a,
        None => return json_error(404, "Authentication is not enabled"),
    };

    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return json_error(400, &format!("Failed to read body: {}", e)),
    };

    #[derive(serde::Deserialize)]
    struct LoginRequest {
        username: String,
        password: String,
    }

    let login_req: LoginRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(e) => return json_error(400, &format!("Invalid JSON: {}", e)),
    };

    match auth.authenticate(&login_req.username, &login_req.password).await {
        Some(token) => {
            #[derive(Serialize)]
            struct LoginResponse<'a> {
                token: &'a str,
                username: &'a str,
            }
            let cookie = format!(
                "orion_session={}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
                token,
                auth.session_expiry_secs()
            );
            let body = serde_json::to_string(&LoginResponse {
                token: &token,
                username: &login_req.username,
            })
            .unwrap_or_else(|_| "{}".into());
            Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .header("Set-Cookie", cookie)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Credentials", "true")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        None => json_error(401, "Invalid username or password"),
    }
}

pub async fn handle_api_logout(
    handler: &S3Handler,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let auth = match &handler.auth {
        Some(a) => a,
        None => return json_error(404, "Authentication is not enabled"),
    };

    // Extract token from cookie or Authorization header.
    if let Some(token) = extract_session_token(req.headers()) {
        auth.logout(&token).await;
    }

    let cookie = "orion_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0";
    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .header("Set-Cookie", cookie)
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from("{\"ok\":true}")))
        .unwrap()
}

/// Extract session token from Cookie header or Authorization: Bearer header.
pub(crate) fn extract_session_token(headers: &http::HeaderMap) -> Option<String> {
    // Try Cookie header first.
    if let Some(cookie) = headers.get("cookie").and_then(|v| v.to_str().ok()) {
        for pair in cookie.split(';') {
            let pair = pair.trim();
            if let Some(val) = pair.strip_prefix("orion_session=") {
                let val = val.trim();
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }

    // Try Authorization: Bearer header.
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }

    None
}

pub fn json_unauthorized() -> Response<Full<Bytes>> {
    json_error(401, "Authentication required")
}
