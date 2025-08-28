use std::sync::{Arc, LazyLock, OnceLock};

use libsql::Connection;
use tracing::instrument;

use crate::ARGS;

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
        tokio::fs::create_dir_all(&ARGS.data_dir).await?;
        let database = libsql::Builder::new_local(ARGS.data_dir.join("leaf.db"))
            .build()
            .await?;
        let c = database.connect()?;
        tracing::info!("database connected");

        sql::init_database_schema(&c).await?;

        self.conn.0.get_or_init(|| c);

        Ok(())
    }
}

mod sql {
    use libsql::Connection;
    use tracing::instrument;

    #[instrument(skip(db))]
    pub async fn init_database_schema(db: &Connection) -> anyhow::Result<()> {
        db.execute_transactional_batch(include_str!("schema/schema_00.sql"))
            .await?;
        Ok(())
    }
}
