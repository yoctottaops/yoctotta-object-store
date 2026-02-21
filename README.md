# Yoctotta Object Store

S3-compatible object storage with pluggable metadata backends and built-in extensions for vector search and webhook triggers.

Designed to run anywhere from a Raspberry Pi to a multi-region cluster — same binary, same config.

## Quick Start

```bash
# Build with defaults (SQLite + all extensions)
cargo build --release

# Run
./target/release/orion

# Or with options
./target/release/orion --listen 0.0.0.0:9000 --data ./storage
```

## Management UI

Visit `http://localhost:9000/_/ui` for the built-in management console:

- **Dashboard** — live process metrics (CPU, memory, threads) with trend charts, storage stats, extension health
- **Buckets** — create, delete, browse buckets
- **Object Browser** — upload, download, delete objects with drag-and-drop
- **RAG Search** — semantic search across indexed objects

Auto-refresh and configurable metrics sampling (default: 30s intervals, 5h retention).

## Metadata Backends

Yoctotta separates object data (always on the filesystem) from metadata (bucket/object index). The metadata backend is pluggable:

### SQLite (default)

Zero dependencies, single file, perfect for single-node and edge deployments.

```bash
orion --meta sqlite
```

### PostgreSQL

Scales horizontally with connection pooling and read replicas. Auto-runs migrations on startup.

```bash
orion --meta postgres --pg-url "postgres://user:pass@localhost:5432/yoctotta"
```

Build with: `cargo build --release --features meta-postgres`

### Raft-Replicated SQLite

Multi-node consensus without external dependencies. Each node keeps a full SQLite copy, writes go through Raft. Same single binary — no Zookeeper, no etcd, no external coordinator.

```bash
# Node 1 — bootstrap the cluster
orion --meta raft --node-id 1 --bootstrap

# Node 2
orion --meta raft --node-id 2

# Node 3
orion --meta raft --node-id 3
```

Build with: `cargo build --release --features meta-raft`

Raft gives you:
- **Automatic leader election** — nodes vote, one becomes leader
- **Replicated writes** — leader replicates to majority before acknowledging
- **Local reads** — fast, eventually consistent (or linearizable via leader)
- **Snapshot recovery** — new nodes catch up via SQLite file transfer
- **Scales to ~5-10 nodes** before leader bottleneck; use Postgres beyond that

## Authentication

Optional username/password auth for the management UI and API:

```toml
[auth]
enabled = true
session_expiry_secs = 3600

[[auth.users]]
username = "admin"
password_hash = "<sha256 hex of password>"
```

Generate a password hash: `echo -n "yourpassword" | sha256sum`

## Usage with AWS CLI

```bash
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt ./downloaded.txt
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                   S3 Protocol (HTTP)                     │
├──────────────────────────────────────────────────────────┤
│                   Extension Hooks                        │
│    ┌─────────┐  ┌───────────────┐  ┌──────┐             │
│    │   RAG   │  │   Triggers    │  │ Auth │  ...more    │
│    └─────────┘  └───────────────┘  └──────┘             │
├────────────────────────┬─────────────────────────────────┤
│   Storage Backend      │       Metadata Store            │
│   (filesystem)         │  ┌─────────┬────────┬────────┐  │
│                        │  │ SQLite  │Postgres│  Raft  │  │
│                        │  └─────────┴────────┴────────┘  │
└────────────────────────┴─────────────────────────────────┘
```

Every layer is a Rust trait — swap implementations without touching other layers.

## Extensions

### RAG (Vector Search)

Automatically indexes text objects into a vector store on upload. Pluggable embedder and vector store backends.

### Triggers (Webhooks)

Fire HTTP webhooks on storage events. Pattern matching on bucket, key prefix/suffix, event type. Retry with exponential backoff.

### Auth (Login Management)

Simple username/password authentication with session tokens. Cookie-based for the UI, Bearer token for API access.

All extensions can be disabled at runtime (`--no-rag`, `--no-triggers`, `--no-extensions`) or at compile time via feature flags.

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `meta-sqlite` | yes | SQLite metadata backend |
| `meta-postgres` | | PostgreSQL metadata backend |
| `meta-raft` | | Raft-replicated SQLite backend |
| `rag` | yes | Vector search extension |
| `trigger` | yes | Webhook trigger extension |

```bash
# Minimal: SQLite only, no extensions
cargo build --release --no-default-features --features meta-sqlite

# Production: Postgres + all extensions
cargo build --release --features meta-postgres

# Cluster: Raft + all extensions
cargo build --release --features meta-raft

# Everything
cargo build --release --features meta-sqlite,meta-postgres,meta-raft,rag,trigger
```

## Cross-Compile for Raspberry Pi

```bash
rustup target add aarch64-unknown-linux-gnu
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu
```

## Project Structure

```
yoctotta-object-store/
├── crates/
│   ├── orion-core/           # Traits, types, extension system
│   ├── orion-store-fs/       # Filesystem storage backend
│   ├── orion-meta-sqlite/    # SQLite metadata (single-node)
│   ├── orion-meta-postgres/  # PostgreSQL metadata (scalable)
│   ├── orion-meta-raft/      # Raft-replicated SQLite (multi-node)
│   ├── orion-proto-s3/       # S3 protocol + management UI
│   ├── orion-ext-rag/        # RAG vector search extension
│   ├── orion-ext-trigger/    # Webhook trigger extension
│   ├── orion-ext-auth/       # Authentication extension
│   └── orion-server/         # Binary entry point + CLI
└── orion.toml                # Configuration reference
```

## Writing Extensions

```rust
use orion_core::extension::*;

struct MyExtension;

#[async_trait]
impl ExtensionHooks for MyExtension {
    async fn post_write(
        &self, key: &ObjectKey, meta: &ObjectMeta, data: &[u8], _ctx: &ExtensionContext,
    ) -> Result<()> {
        println!("New object: {} ({} bytes)", key, meta.size);
        Ok(())
    }
}
```

## License

Apache-2.0
