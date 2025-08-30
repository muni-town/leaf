use std::{collections::HashSet, sync::LazyLock, time::Duration};

use blake3::Hash;
use libsql::Connection;
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use conn_lock::Conn;
mod conn_lock;

pub use convert::*;
mod convert;

use crate::{ARGS, wasm::validate_wasm};

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

#[derive(Default)]
pub struct Storage {
    conn: Conn,
}

impl Storage {
    async fn conn(&self) -> &libsql::Connection {
        (&self.conn).await
    }

    #[instrument(skip(self), err)]
    pub async fn initialize(&self) -> anyhow::Result<()> {
        if self.conn.has_initialized() {
            tracing::warn!("Database already initialized.");
            return Ok(());
        }

        // Open database file
        tokio::fs::create_dir_all(&ARGS.data_dir).await?;
        let database = libsql::Builder::new_local(ARGS.data_dir.join("leaf.db"))
            .build()
            .await?;
        let c = database.connect()?;
        tracing::info!("database connected");

        // Run migrations
        run_database_migrations(&c).await?;

        // Start the background storage tasks
        start_background_tasks();

        self.conn.set(c).ok();

        Ok(())
    }

    /// Returns the list of hashes that could be found in the database out of the list that is
    /// provided to search for.
    #[instrument(skip(self, hashes), err)]
    pub async fn find_wasm_blobs(
        &self,
        hashes: &HashSet<blake3::Hash>,
    ) -> anyhow::Result<HashSet<blake3::Hash>> {
        let placeholders = hashes.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let rows = self
            .conn()
            .await
            .query(
                &format!("select hash from wasm_blobs where hash in ({placeholders})"),
                hashes
                    .iter()
                    .map(|x| x.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
            .await?;
        let found_hashes = Vec::<Hash>::from_rows(rows).await?.into_iter().collect();
        return Ok(found_hashes);
    }

    #[instrument(skip(self, data), err)]
    pub async fn upload_wasm(&self, owner: &str, data: Vec<u8>) -> anyhow::Result<blake3::Hash> {
        if data.len() > 1024 * 1024 * 10 {
            anyhow::bail!("WASM module larger than 10MB maximum size.");
        }
        validate_wasm(&data)?;
        let trans = self.conn().await.transaction().await?;
        let hash = blake3::hash(&data);
        trans
            .execute(
                r#"insert or ignore into wasm_blobs (hash, data) values (:hash, :data)"#,
                ((":hash", hash.as_bytes().to_vec()), (":data", data)),
            )
            .await?;
        trans
            .execute(
                r#"insert into staged_wasm (owner, hash) values (:owner, :hash)"#,
                ((":owner", owner), (":hash", hash.as_bytes().to_vec())),
            )
            .await?;
        trans.commit().await?;
        Ok(hash)
    }

    #[instrument(skip(self), err)]
    pub async fn garbage_collect_wasm(&self) -> anyhow::Result<()> {
        let mut deleted = self
            .conn()
            .await
            .execute_batch(
                r#"
                delete from staged_wasm where (unixepoch() - timestamp) > 500 returning owner, hash;
                delete from wasm_blobs where not exists (
                    select 1 from staged_wasm s where s.hash=wasm_blobs.hash
                        union
                    select 1 from streams_wasm_blobs s where s.blob_hash=wasm_blobs.hash
                ) returning hash;
                "#,
            )
            .await?;
        let mut deleted_staged: Vec<(String, Hash)> = Vec::new();
        let mut deleted_blobs: Vec<Hash> = Vec::new();
        let r = async {
            if let Some(rows) = deleted.next_stmt_row().flatten() {
                deleted_staged = Vec::from_rows(rows).await?;
            }
            if let Some(rows) = deleted.next_stmt_row().flatten() {
                deleted_blobs = Vec::from_rows(rows).await?;
            }
            anyhow::Ok(())
        }
        .await;
        if let Err(error) = r {
            tracing::warn!(%error, "Error parsing deleted WASM blobs")
        }

        if !deleted_staged.is_empty() || !deleted_blobs.is_empty() {
            tracing::info!(
                deleted_staged_wasm_modules=?deleted_staged,
                deleted_wasm_blobs=?deleted_blobs,
                "Garbage collected WASM records"
            );
        }
        let span = Span::current();
        span.set_attribute("wasm_blobs_deleted", deleted_staged.len() as i64);
        span.set_attribute("staged_wasm_modules_deleted", deleted_blobs.len() as i64);

        Ok(())
    }
}

#[instrument(skip(db))]
pub async fn run_database_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_transactional_batch(include_str!("schema/schema_00.sql"))
        .await?;
    Ok(())
}

pub fn start_background_tasks() {
    // Garbage collect WASM files periodically
    tokio::spawn(async move {
        loop {
            STORAGE.garbage_collect_wasm().await.ok();
            tokio::time::sleep(Duration::from_secs(500)).await;
        }
    });
}
