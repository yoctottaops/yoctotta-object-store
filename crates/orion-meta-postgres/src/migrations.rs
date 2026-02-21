use sqlx::PgPool;
use tracing;

/// Run all migrations in order. Each is idempotent.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Create migrations tracking table.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _orion_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"
    )
    .execute(pool)
    .await?;

    let migrations: &[(i32, &str, &str)] = &[
        (1, "initial_schema", MIGRATION_001),
        (2, "add_indexes", MIGRATION_002),
        (3, "add_partitioning_support", MIGRATION_003),
    ];

    for (version, name, sql) in migrations {
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM _orion_migrations WHERE version = $1)"
        )
        .bind(version)
        .fetch_one(pool)
        .await?;

        if !applied {
            tracing::info!(version, name, "Applying migration");

            // Run in a transaction.
            let mut tx = pool.begin().await?;
            sqlx::query(sql).execute(&mut *tx).await?;
            sqlx::query(
                "INSERT INTO _orion_migrations (version, name) VALUES ($1, $2)"
            )
            .bind(version)
            .bind(name)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            tracing::info!(version, name, "Migration applied");
        }
    }

    Ok(())
}

const MIGRATION_001: &str = "
    CREATE TABLE IF NOT EXISTS buckets (
        name TEXT PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        region TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE TABLE IF NOT EXISTS objects (
        bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
        key TEXT NOT NULL,
        size BIGINT NOT NULL,
        etag TEXT NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        user_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        checksum TEXT,
        version_id TEXT,
        PRIMARY KEY (bucket, key)
    );
";

const MIGRATION_002: &str = "
    -- Prefix listing: enables efficient ListObjectsV2 with prefix filter.
    CREATE INDEX IF NOT EXISTS idx_objects_prefix
        ON objects (bucket, key text_pattern_ops);

    -- Modified time: useful for lifecycle rules and time-based queries.
    CREATE INDEX IF NOT EXISTS idx_objects_modified
        ON objects (bucket, modified_at DESC);

    -- Size aggregation per bucket.
    CREATE INDEX IF NOT EXISTS idx_objects_size
        ON objects (bucket, size);
";

const MIGRATION_003: &str = "
    -- Support table for future partitioning by bucket.
    -- When object counts get very high, you can convert the objects table
    -- to a partitioned table by bucket using:
    --   ALTER TABLE objects RENAME TO objects_old;
    --   CREATE TABLE objects (...) PARTITION BY LIST (bucket);
    --   -- then create partitions per bucket
    --
    -- This migration just adds a helper view for partition planning.

    CREATE OR REPLACE VIEW bucket_stats AS
    SELECT
        bucket,
        COUNT(*) as object_count,
        SUM(size) as total_size,
        MIN(created_at) as oldest_object,
        MAX(modified_at) as newest_object
    FROM objects
    GROUP BY bucket;
";
