# Yoctotta Object Store

**A world-class, S3-compatible, AI-first object storage solution.**

Yoctotta is an attempt to build a storage engine that treats AI workloads as first-class citizens — not an afterthought bolted on top. Every object you store is automatically indexed for semantic search via built-in vector embeddings, so your data is instantly queryable by meaning, not just by key. Pair that with a full S3-compatible API, pluggable metadata backends (SQLite, PostgreSQL, Raft-replicated SQLite), and a built-in management UI, and you get a single binary that runs anywhere from a Raspberry Pi to a multi-region cluster.

## Screenshots

### Dashboard
Live process metrics, storage health, extension status, and bucket overview — all in one place.

![Dashboard](docs/dashboard.png)

### Bucket Management
Create, browse, and manage buckets with a clean interface.

![Buckets](docs/buckets.png)

### RAG Search
Semantic search across all stored objects using vector embeddings. Upload a document and immediately search it by meaning.

![RAG Search](docs/rag-search.png)

## Why Yoctotta?

Most object stores treat data as opaque blobs. You upload a file, you get it back by key. If you want search, you build a separate pipeline — extract text, generate embeddings, push to a vector database, keep it all in sync.

Yoctotta eliminates that entire pipeline. When you `PUT` an object, it is automatically chunked, embedded (using a fast on-device model via ONNX Runtime), and indexed. When you need to find something, you search by meaning — not by filename. All of this happens in the background without affecting normal read/write performance.

**Key design goals:**
- **AI-native** — built-in vector embeddings and semantic search, not a plugin
- **S3-compatible** — drop-in replacement, works with AWS CLI, any S3 SDK, existing tools
- **Single binary** — no external dependencies, no separate vector database, no message queue
- **Runs anywhere** — same binary on a laptop, a Raspberry Pi, or a 10-node cluster
- **Extensible** — trait-based architecture, swap any layer without touching the rest

## Quick Start

```bash
# Build with defaults (SQLite + all extensions)
cargo build --release

# Run
./target/release/orion

# Or with options
./target/release/orion --listen 0.0.0.0:9000 --data ./storage
```

The management UI is available at `http://localhost:9000/_/ui`.

## Usage with AWS CLI

```bash
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt ./downloaded.txt
```

## Built-in AI: Vector Search

Every text object uploaded to Yoctotta is automatically:

1. **Chunked** into overlapping segments (configurable size/overlap)
2. **Embedded** using `all-MiniLM-L6-v2` — a fast, high-quality model that runs entirely on CPU via ONNX Runtime
3. **Indexed** in an in-memory vector store for instant semantic search

All embedding runs asynchronously on background threads (`spawn_blocking`), so S3 reads, writes, and metadata operations are never blocked.

Search via the UI or the API:

```bash
# Semantic search across all buckets
curl "http://localhost:9000/_/api/search?q=authentication+flow&top_k=10"

# Search within a specific bucket
curl "http://localhost:9000/_/api/search?q=deployment+guide&bucket=docs&top_k=5"
```

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

Automatically indexes text objects into a vector store on upload. Uses `all-MiniLM-L6-v2` (384-dim, ONNX, CPU-only) for fast, high-quality embeddings. All inference is async and background — never blocks the request path.

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
yoctotta/
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
├── docs/                     # Screenshots and documentation
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
