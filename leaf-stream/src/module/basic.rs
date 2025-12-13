use libsql::ffi::SQLITE_CREATE_INDEX;

use crate::drisl_extract::extract_sql_value_from_drisl;

use super::*;

pub struct BasicModule {
    id: Cid,
    def: Arc<BasicModuleDef>,
}

impl LeafModule for BasicModule {
    fn module_type_id() -> &'static str
    where
        Self: Sized,
    {
        "muni.town.leaf.module.basic.0"
    }
    fn module_id(&self) -> Cid {
        self.id
    }

    fn load(codec: ModuleCodec) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let id = codec.compute_id();

        let def = codec.decode_def::<BasicModuleDef>()?;
        Ok(BasicModule {
            id,
            def: Arc::new(def),
        })
    }

    fn save(&self) -> anyhow::Result<ModuleCodec> {
        Ok(ModuleCodec::new((*self.def).clone())?)
    }

    fn init_db_conn(&'_ self, module_db: &libsql::Connection) -> BoxFuture<'_, anyhow::Result<()>> {
        let module_db = module_db.clone();
        Box::pin(async move {
            // Install our user-defined SQL functions
            install_udfs(&module_db)?;
            Ok(())
        })
    }

    fn init_db_schema(
        &'_ self,
        module_db: &libsql::Connection,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let def = self.def.clone();
        let module_db = module_db.clone();
        Box::pin(async move {
            module_db.execute_batch(&def.init_sql).await?;
            Ok(())
        })
    }

    fn materialize(
        &'_ self,
        module_db: &libsql::Connection,
        Event { idx, user, payload }: Event,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let def = self.def.clone();
        let module_db = module_db.clone();
        Box::pin(async move {
            // Setup the temporary `event` table to contain the next event to materialize.
            module_db
                .execute("drop table if exists temp.event", ())
                .await?;
            module_db
            .execute(
                r#"
                            create temp table if not exists event as select ? as idx, ? as user, ? as payload
                        "#,
                (idx, user, payload),
            )
            .await?;

            module_db.execute_batch(&def.materializer).await?;
            Ok(())
        })
    }

    fn authorize(
        &'_ self,
        module_db: &libsql::Connection,
        IncomingEvent { user, payload }: IncomingEvent,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let def = self.def.clone();
        let module_db = module_db.clone();
        Box::pin(async move {
            // Setup the temporary `event` table to contain the next event to authorize / materialize.
            module_db
                .execute("drop table if exists temp.event", ())
                .await?;
            module_db
                .execute(
                    r#"
                        create temp table if not exists event as select ? as user, ? as payload
                    "#,
                    (user, payload),
                )
                .await?;

            module_db.execute_batch(&def.authorizer).await?;
            Ok(())
        })
    }

    fn query(
        &'_ self,
        module_db: &libsql::Connection,
        query: LeafQuery,
    ) -> BoxFuture<'_, anyhow::Result<SqlRows>> {
        let def = self.def.clone();
        let module_db = module_db.clone();
        Box::pin(async move {
            // Get the module's query definition by name
            let query_def = def
                .queries
                .iter()
                .find(|x| x.name == query.query_name)
                .ok_or_else(|| {
                    anyhow::format_err!("Query with name `{}` not in module.", query.query_name)
                })?;

            // Make sure the query is valid for it's definition
            query_def.validate_query(&query)?;

            // TODO: replace this with a less naïve statement splitter
            let sql_statements = SQL_COMMENT_REGEX.replace_all(&query_def.sql, "");
            let sql_statements = sql_statements
                .split(";")
                .map(|x| x.trim())
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            // Execute the query statements
            let mut query_result = None;
            for statement in sql_statements {
                let mut r = module_db
                    .query(
                        statement,
                        query
                            .params
                            .clone()
                            .into_iter()
                            .map(|(k, v)| (format!("${k}"), leaf_sql_value_to_libsql(v)))
                            .chain([
                                (
                                    "$start".to_string(),
                                    libsql::Value::Integer(query.start.unwrap_or(1)),
                                ),
                                (
                                    "$limit".to_string(),
                                    libsql::Value::Integer(query.limit.unwrap_or(100)),
                                ),
                                (
                                    "$requesting_user".to_string(),
                                    match query.requesting_user.clone() {
                                        Some(t) => libsql::Value::Text(t),
                                        None => libsql::Value::Null,
                                    },
                                ),
                            ])
                            .collect::<Vec<_>>(),
                    )
                    .await?;

                // Convert the query result to our Leaf SqlRows type
                let column_count = r.column_count();
                let column_names = (0..column_count)
                    .map(|i| r.column_name(i).unwrap_or("").to_string())
                    .collect::<Vec<_>>();
                let mut rows = Vec::new();
                while let Some(row) = r.next().await? {
                    rows.push(SqlRow {
                        values: (0..column_count)
                            .map(|i| row.get_value(i).map(libsql_value_to_leaf))
                            .collect::<Result<Vec<_>, _>>()?,
                    })
                }

                query_result = Some(SqlRows { rows, column_names });
            }

            let Some(query_result) = query_result else {
                anyhow::bail!("Query did not return a result.")
            };

            Ok(query_result)
        })
    }
}

/// Install Leaf's user-defined functions on the database connection
fn install_udfs(db: &libsql::Connection) -> libsql::Result<()> {
    use libsql::Value;

    // A panic function that can be used to intentionally stop a transaction such as in the event
    // authorizer.
    db.create_scalar_function(ScalarFunctionDef {
        name: "throw".to_string(),
        num_args: -1,
        deterministic: true,
        innocuous: false,
        direct_only: true,
        callback: Arc::new(|values| {
            anyhow::bail!(
                "Exception thrown: {}",
                values
                    .into_iter()
                    .map(|x| match x {
                        Value::Null => "NULL".to_string(),
                        Value::Integer(i) => i.to_string(),
                        Value::Real(r) => r.to_string(),
                        Value::Text(t) => t.to_string(),
                        Value::Blob(bytes) => bytes
                            .iter()
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<String>>()
                            .join(""),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }),
    })?;

    db.create_scalar_function(ScalarFunctionDef {
        name: "unauthorized".to_string(),
        num_args: -1,
        deterministic: true,
        innocuous: false,
        direct_only: true,
        callback: Arc::new(|values| {
            anyhow::bail!(
                "Unauthorized: {}",
                values
                    .into_iter()
                    .map(|x| match x {
                        Value::Null => "NULL".to_string(),
                        Value::Integer(i) => i.to_string(),
                        Value::Real(r) => r.to_string(),
                        Value::Text(t) => t.to_string(),
                        Value::Blob(bytes) => bytes
                            .iter()
                            .map(|b| format!("{:02X}", b))
                            .collect::<Vec<String>>()
                            .join(""),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }),
    })?;

    db.create_scalar_function(ScalarFunctionDef {
        name: "dasl_extract".to_string(),
        num_args: 2,
        deterministic: true,
        innocuous: true,
        direct_only: false,
        callback: Arc::new(|values| {
            let Value::Blob(blob) = values.first().unwrap() else {
                anyhow::bail!("First argument to scale_extract must be blob");
            };
            let Value::Text(path) = values.get(1).unwrap() else {
                anyhow::bail!("Second argument to scale_extract must be sring");
            };
            let value = dasl::drisl::from_slice(blob)?;
            extract_sql_value_from_drisl(value, path)
        }),
    })?;

    Ok(())
}

fn libsql_value_to_leaf(value: libsql::Value) -> SqlValue {
    use SqlValue as S;
    use libsql::Value as V;
    match value {
        V::Null => S::Null,
        V::Integer(i) => S::Integer(i),
        V::Real(r) => S::Real(r),
        V::Text(t) => S::Text(t),
        V::Blob(b) => S::Blob(b),
    }
}

fn leaf_sql_value_to_libsql(value: SqlValue) -> libsql::Value {
    use SqlValue as S;
    use libsql::Value as V;
    match value {
        S::Null => V::Null,
        S::Integer(i) => V::Integer(i),
        S::Real(r) => V::Real(r),
        S::Text(t) => V::Text(t),
        S::Blob(b) => V::Blob(b),
    }
}
