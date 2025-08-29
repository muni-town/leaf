use std::{
    sync::{Arc, LazyLock, OnceLock},
    time::Duration,
};

use libsql::Connection;
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub use conv::*;
mod conv;

use crate::{ARGS, wasm::validate_wasm};

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

#[derive(Default, Clone)]
struct Conn(Arc<OnceLock<Connection>>);

impl std::ops::Deref for Conn {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.0.get().unwrap()
    }
}

#[derive(Default)]
pub struct Storage {
    conn: Conn,
}

impl Storage {
    #[instrument(skip(self), err)]
    pub async fn initialize(&self) -> anyhow::Result<()> {
        if self.conn.0.get().is_some() {
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

        self.conn.0.get_or_init(|| c);

        Ok(())
    }

    #[instrument(skip(self, data), err)]
    pub async fn upload_wasm(&self, owner: &str, data: Vec<u8>) -> anyhow::Result<()> {
        validate_wasm(&data)?;
        let trans = self.conn.transaction().await?;
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
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn garbage_collect_wasm(&self) -> anyhow::Result<()> {
        let mut deleted = self
            .conn
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
        let mut deleted_staged: Vec<(String, StringOrBinaryAsHex)> = Vec::new();
        let mut deleted_blobs: Vec<String> = Vec::new();
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
