use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

/// Configuration for the auth extension.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    /// Whether authentication is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Session expiry in seconds. Default: 3600 (1 hour).
    #[serde(default = "default_session_expiry")]
    pub session_expiry_secs: u64,
    /// List of users.
    #[serde(default)]
    pub users: Vec<UserConfig>,
}

fn default_session_expiry() -> u64 {
    3600
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            session_expiry_secs: default_session_expiry(),
            users: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserConfig {
    pub username: String,
    /// SHA-256 hex-encoded password hash.
    pub password_hash: String,
}

struct Session {
    username: String,
    created_at: Instant,
}

/// Simple in-memory session-based authentication provider.
pub struct AuthProvider {
    config: AuthConfig,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
}

impl AuthProvider {
    pub fn new(config: AuthConfig) -> Arc<Self> {
        let provider = Arc::new(Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        });

        // Start background cleanup task.
        let p = provider.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                p.cleanup_expired().await;
            }
        });

        provider
    }

    pub fn session_expiry_secs(&self) -> u64 {
        self.config.session_expiry_secs
    }

    /// Authenticate a user. Returns a session token on success.
    pub async fn authenticate(&self, username: &str, password: &str) -> Option<String> {
        let password_hash = hash_password(password);

        let valid = self.config.users.iter().any(|u| {
            u.username == username && u.password_hash == password_hash
        });

        if !valid {
            return None;
        }

        let token = generate_token();
        let session = Session {
            username: username.to_string(),
            created_at: Instant::now(),
        };

        self.sessions.write().await.insert(token.clone(), session);
        Some(token)
    }

    /// Validate a session token. Returns the username if valid.
    pub async fn validate_session(&self, token: &str) -> Option<String> {
        let sessions = self.sessions.read().await;
        if let Some(session) = sessions.get(token) {
            if session.created_at.elapsed().as_secs() < self.config.session_expiry_secs {
                return Some(session.username.clone());
            }
        }
        None
    }

    /// Remove a session (logout).
    pub async fn logout(&self, token: &str) {
        self.sessions.write().await.remove(token);
    }

    /// Check if authentication is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    async fn cleanup_expired(&self) {
        let expiry = self.config.session_expiry_secs;
        self.sessions
            .write()
            .await
            .retain(|_, s| s.created_at.elapsed().as_secs() < expiry);
    }
}

/// Hash a password with SHA-256, returning hex-encoded string.
pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

fn generate_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    hex::encode(bytes)
}

// ── Extension trait implementation ──

#[async_trait]
impl orion_core::Extension for AuthProvider {
    fn info(&self) -> orion_core::ExtensionInfo {
        orion_core::ExtensionInfo {
            name: "auth".to_string(),
            version: "0.1.0".to_string(),
            description: "Simple username/password authentication".to_string(),
            enabled: self.config.enabled,
        }
    }

    async fn init(&mut self, _config: &toml::Value) -> orion_core::Result<()> {
        Ok(())
    }

    async fn shutdown(&self) -> orion_core::Result<()> {
        Ok(())
    }

    fn hooks(&self) -> &dyn orion_core::ExtensionHooks {
        self
    }

    async fn health(&self) -> orion_core::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl orion_core::ExtensionHooks for AuthProvider {}
