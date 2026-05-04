// SPDX-License-Identifier: MIT

//! Relay's own bookkeeping tables and schema-drift detection.

use serde_json::json;
use sqlx::{Pool, Postgres, Row};
use tracing::debug;

use relay_protocol::MirroredSchema;

use crate::ddl::{create_table_sql, drop_table_sql, TableSpec, MIRROR_DDL_VERSION};
use crate::StorageError;

#[derive(Debug)]
pub enum SyncOutcome {
    /// The schema fingerprint matched what we already had.
    Unchanged,
    /// First time we've ever mirrored this upstream database.
    CreatedFresh {
        created: Vec<String>,
    },
    /// The schema fingerprint changed since the last sync.
    DriftWiped {
        wiped: Vec<String>,
        created: Vec<String>,
    },
}

pub async fn run_initial_migrations(pool: &Pool<Postgres>) -> Result<(), StorageError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS relay_meta_database (
            upstream_database  TEXT        PRIMARY KEY,
            upstream_host      TEXT        NOT NULL,
            schema_fingerprint TEXT        NOT NULL,
            schema_json        JSONB       NOT NULL,
            last_synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS relay_meta_table (
            id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            upstream_database  TEXT        NOT NULL REFERENCES relay_meta_database(upstream_database) ON DELETE CASCADE,
            upstream_table     TEXT        NOT NULL,
            postgres_table     TEXT        NOT NULL UNIQUE,
            column_defs        JSONB       NOT NULL,
            primary_key_cols   TEXT[]      NOT NULL,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(upstream_database, upstream_table)
        )
        "#,
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn sync_schema(
    pool: &Pool<Postgres>,
    upstream_host: &str,
    upstream_database: &str,
    schema: &MirroredSchema,
    specs: &[TableSpec],
) -> Result<SyncOutcome, StorageError> {
    let new_fingerprint = format!("v{MIRROR_DDL_VERSION}:{}", schema.fingerprint_hex());

    let existing: Option<(String, Vec<String>)> = sqlx::query(
        r#"
        SELECT
            d.schema_fingerprint,
            COALESCE(
                ARRAY(
                    SELECT postgres_table FROM relay_meta_table t
                    WHERE t.upstream_database = d.upstream_database
                ),
                ARRAY[]::text[]
            ) AS postgres_tables
        FROM relay_meta_database d
        WHERE d.upstream_database = $1
        "#,
    )
    .bind(upstream_database)
    .fetch_optional(pool)
    .await?
    .map(|row| {
        let fp: String = row.get("schema_fingerprint");
        let pts: Vec<String> = row.get("postgres_tables");
        (fp, pts)
    });

    if let Some((stored_fp, _)) = &existing {
        if stored_fp == &new_fingerprint {
            return Ok(SyncOutcome::Unchanged);
        }
    }

    let mut tx = pool.begin().await?;
    let mut wiped = Vec::new();

    if let Some((_, postgres_tables)) = &existing {
        for pt in postgres_tables {
            let sql = drop_table_sql(pt);
            debug!(target: "relay::storage", sql, "dropping drifted table");
            sqlx::query(&sql).execute(&mut *tx).await?;
            wiped.push(pt.clone());
        }
        sqlx::query("DELETE FROM relay_meta_table WHERE upstream_database = $1")
            .bind(upstream_database)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM relay_meta_database WHERE upstream_database = $1")
            .bind(upstream_database)
            .execute(&mut *tx)
            .await?;
    }

    let schema_json = serde_json::to_value(schema).expect("MirroredSchema always serializable");
    sqlx::query(
        r#"
        INSERT INTO relay_meta_database
            (upstream_database, upstream_host, schema_fingerprint, schema_json)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(upstream_database)
    .bind(upstream_host)
    .bind(&new_fingerprint)
    .bind(&schema_json)
    .execute(&mut *tx)
    .await?;

    let mut created = Vec::with_capacity(specs.len());
    for spec in specs {
        let sql = create_table_sql(spec);
        debug!(target: "relay::storage", sql, "creating mirror table");
        sqlx::query(&sql).execute(&mut *tx).await?;

        let column_defs = json!(spec
            .columns
            .iter()
            .map(|c| json!({
                "upstream_name": c.upstream_name,
                "postgres_name": c.postgres_name,
                "sql_type": c.sql_type,
                "nullable": c.nullable,
            }))
            .collect::<Vec<_>>());
        sqlx::query(
            r#"
            INSERT INTO relay_meta_table
                (upstream_database, upstream_table, postgres_table, column_defs, primary_key_cols)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(upstream_database)
        .bind(&spec.upstream_name)
        .bind(&spec.postgres_name)
        .bind(&column_defs)
        .bind(&spec.primary_key_columns)
        .execute(&mut *tx)
        .await?;
        created.push(spec.postgres_name.clone());
    }

    tx.commit().await?;

    if existing.is_some() {
        Ok(SyncOutcome::DriftWiped { wiped, created })
    } else {
        Ok(SyncOutcome::CreatedFresh { created })
    }
}
