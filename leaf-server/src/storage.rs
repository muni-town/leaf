use std::sync::{Arc, LazyLock, OnceLock};

use libsql::Connection;
use tracing::Instrument;

use crate::{ARGS, EXIT_SIGNAL};

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
    pub async fn initialize(&self) {
        let result = async move {
            tokio::fs::create_dir_all(&ARGS.data_dir).await?;
            let database = libsql::Builder::new_local(ARGS.data_dir.join("leaf.db"))
                .build()
                .await?;
            let c = database.connect()?;
            tracing::info!("database connected");

            sql::init_database_schema(&c).await?;

            self.conn.0.get_or_init(|| c);

            Ok::<_, anyhow::Error>(())
        }
        .instrument(tracing::info_span!("connect_to_database"))
        .await;

        if let Err(e) = result {
            tracing::error!("Error connecting to database: {e}");
            EXIT_SIGNAL.trigger_exit_signal();
        }
    }
}

mod sql {
    use libsql::Connection;
    // use sea_query::*;
    use tracing::instrument;

    #[instrument(skip(db))]
    pub async fn init_database_schema(db: &Connection) -> anyhow::Result<()> {
        // let sql = Table::create()
        //     .table(Test::Table)
        //     .if_not_exists()
        //     .col(ColumnDef::new(Test::Id).uuid().not_null().primary_key())
        //     .col(ColumnDef::new(Test::Name).string().not_null())
        //     .to_string(SqliteQueryBuilder);

        // db.execute(&sql, ()).await?;

        Ok(())
    }

    // #[derive(Iden)]
    // enum Test {
    //     Table,
    //     Id,
    //     Name,
    // }
}
