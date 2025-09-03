use std::{
    iter,
    mem::size_of,
    pin::Pin,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use blake3::Hash;
use leaf_stream_types::{
    Inbound, ModuleInput, Outbound, Process, SqlQuery, SqlRow, SqlRows, SqlValue,
};
use parity_scale_codec::{Decode, Encode};
use wasmtime::{Config, Engine, FuncType, Store, Val, ValType};

use crate::LeafModule;

pub static ENGINE: LazyLock<Engine> =
    LazyLock::new(|| Engine::new(Config::new().async_support(true)).unwrap());

#[derive(Debug)]
pub struct LeafWasmModule {
    id: Hash,
    module: wasmtime::Module,
    linker: Arc<wasmtime::Linker<libsql::Connection>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ModuleError {
    #[error("Error with WASM module interface: {0}")]
    InterfaceError(#[from] WasmInterfaceError),
    #[error("WASM error: {0}")]
    WasmError(#[from] wasmtime::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum WasmInterfaceError {
    #[error("WASM module does not export the malloc function.")]
    InvalidOrMissingMallocExport,
    #[error("WASM module does not export memory.")]
    MissingMemoryExport,
    #[error("The process_event function is either not exported or has an invalid type.")]
    InvalidOrMissingProcessEventExport,
}

impl LeafWasmModule {
    pub fn new(wasm: &[u8]) -> Result<Self, ModuleError> {
        let id = blake3::hash(wasm);
        let module = wasmtime::Module::new(&ENGINE, wasm)?;
        let mut linker = wasmtime::Linker::<libsql::Connection>::new(&ENGINE);
        linker.func_wrap_async(
            "host",
            "query",
            |mut caller, (query_ptr, query_len): (i32, i32)| {
                Box::new(async move {
                    let memory = caller
                        .get_export("memory")
                        .and_then(|x| x.into_memory())
                        .ok_or_else(|| anyhow::format_err!("Could not get WASM memory export"))?;
                    let Some(malloc) = caller.get_export("malloc").and_then(|x| x.into_func())
                    else {
                        anyhow::bail!("WASM module does not export `malloc` function");
                    };
                    let conn = caller.data();

                    let mut query_bytes = Vec::from_iter(iter::repeat_n(0u8, query_len as usize));
                    memory.read(&caller, query_ptr as usize, &mut query_bytes)?;
                    let query = SqlQuery::decode(&mut &query_bytes[..])?;

                    let mut rows = conn
                        .query(
                            &query.sql,
                            query
                                .params
                                .into_iter()
                                .map(|(key, value)| (key, to_libsql(value)))
                                .collect::<Vec<_>>(),
                        )
                        .await?;

                    let col_count = rows.column_count();
                    let mut result = SqlRows {
                        column_names: (0..col_count)
                            .map(|i| rows.column_name(i).unwrap_or_default().to_owned())
                            .collect(),
                        ..Default::default()
                    };
                    while let Some(row) = rows.next().await? {
                        result.rows.push(SqlRow {
                            values: (0..col_count)
                                .map(|i| Ok::<_, libsql::Error>(from_libsql(row.get_value(i)?)))
                                .collect::<Result<Vec<_>, _>>()?,
                        });
                    }

                    // Allocate the query result and write it to wasm memory
                    let result_bytes = result.encode();
                    let result_len = result_bytes.len() as i32;
                    let mut results = [Val::null_any_ref()];
                    malloc
                        .call_async(&mut caller, &[result_len.into(), 1.into()], &mut results)
                        .await?;
                    let Some(input_ptr) = results[0].i32() else {
                        anyhow::bail!("Invalid return type from malloc");
                    };
                    memory.write(&mut caller, input_ptr as usize, &result_bytes)?;

                    // Return the pointer to the query result
                    Ok((input_ptr, result_len))
                })
            },
        )?;

        Self::validate_module_interface(&module)?;

        Ok(Self {
            id,
            module,
            linker: Arc::new(linker),
        })
    }

    fn validate_module_interface(w_module: &wasmtime::Module) -> Result<(), WasmInterfaceError> {
        if !w_module
            .exports()
            .any(|x| x.name() == "memory" && x.ty().memory().is_some())
        {
            return Err(WasmInterfaceError::MissingMemoryExport);
        }
        if let Some(x) = w_module.get_export("malloc")
            && let Some(f) = x.func()
            && f.matches(&FuncType::new(
                &ENGINE,
                [ValType::I32, ValType::I32],
                [ValType::I32],
            ))
        {
        } else {
            return Err(WasmInterfaceError::InvalidOrMissingMallocExport);
        };
        if let Some(x) = w_module.get_export("filter_inbound")
            && let Some(f) = x.func()
            && f.matches(&FuncType::new(
                &ENGINE,
                [ValType::I32, ValType::I32, ValType::I32],
                [],
            ))
        {
        } else {
            return Err(WasmInterfaceError::InvalidOrMissingProcessEventExport);
        };
        if let Some(x) = w_module.get_export("filter_outbound")
            && let Some(f) = x.func()
            && f.matches(&FuncType::new(
                &ENGINE,
                [ValType::I32, ValType::I32, ValType::I32],
                [],
            ))
        {
        } else {
            return Err(WasmInterfaceError::InvalidOrMissingProcessEventExport);
        };
        if let Some(x) = w_module.get_export("process_event")
            && let Some(f) = x.func()
            && f.matches(&FuncType::new(
                &ENGINE,
                [ValType::I32, ValType::I32, ValType::I32],
                [],
            ))
        {
        } else {
            return Err(WasmInterfaceError::InvalidOrMissingProcessEventExport);
        };

        Ok(())
    }
}

impl LeafModule for LeafWasmModule {
    fn id(&self) -> blake3::Hash {
        self.id
    }

    fn filter_inbound(
        &mut self,
        input: ModuleInput,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Inbound>> + Sync + Send>> {
        let module = self.module.clone();
        let linker = self.linker.clone();

        Box::pin(async move {
            let mut store = Store::new(&ENGINE, db);
            let instance = linker.instantiate_async(&mut store, &module).await?;
            let Some(memory) = instance.get_memory(&mut store, "memory") else {
                anyhow::bail!("Could not load wasm memory.");
            };
            let malloc = instance
                .get_typed_func::<(i32, i32), i32>(&mut store, "malloc")
                .context("invalid type for malloc function")?;
            let filter_inbound = instance
                .get_typed_func::<(i32, i32, i32), ()>(&mut store, "filter_inbound")
                .context("Invalid type for filter_inbound function")?;

            // Allocate the input in the WASM module
            let input_bytes = input.encode();
            let input_len = input_bytes.len() as i32;
            let input_ptr = malloc.call_async(&mut store, (input_len, 1)).await?;
            memory.write(&mut store, input_ptr as usize, &input_bytes)?;

            // Allocate space for the return value
            let output_ptr = malloc
                .call_async(
                    &mut store,
                    (size_of::<i32>() as i32 * 2, align_of::<i32>() as i32),
                )
                .await?;

            // Run the inbound filter function from the WASM module
            filter_inbound
                .call_async(&mut store, (input_ptr, input_len, output_ptr))
                .await
                .context("error calling wasm func")?;

            let mut output_ptr_bytes = [0u8; size_of::<i32>()];
            memory.read(&store, output_ptr as usize, &mut output_ptr_bytes)?;
            let mut output_len_bytes = [0u8; size_of::<i32>()];
            memory.read(
                &store,
                output_ptr as usize + size_of::<i32>(),
                &mut output_len_bytes,
            )?;
            let output_ptr = i32::from_le_bytes(output_ptr_bytes);
            let output_len = i32::from_le_bytes(output_len_bytes);

            let mut response_bytes = Vec::from_iter(iter::repeat_n(0u8, output_len as usize));
            memory.read(&store, output_ptr as usize, &mut response_bytes)?;
            let response = Inbound::decode(&mut &response_bytes[..])?;

            Ok(response)
        })
    }

    fn filter_outbound(
        &mut self,
        input: ModuleInput,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Outbound>> + Sync + Send>> {
        let module = self.module.clone();
        let linker = self.linker.clone();

        Box::pin(async move {
            let mut store = Store::new(&ENGINE, db);
            let instance = linker.instantiate_async(&mut store, &module).await?;
            let Some(memory) = instance.get_memory(&mut store, "memory") else {
                anyhow::bail!("Could not load wasm memory.");
            };
            let malloc = instance
                .get_typed_func::<(i32, i32), i32>(&mut store, "malloc")
                .context("invalid type for malloc function")?;
            let filter_outbound = instance
                .get_typed_func::<(i32, i32, i32), ()>(&mut store, "filter_outbound")
                .context("Invalid type for filter_outbound function")?;

            // Allocate the input in the WASM module
            let input_bytes = input.encode();
            let input_len = input_bytes.len() as i32;
            let input_ptr = malloc.call_async(&mut store, (input_len, 1)).await?;
            memory.write(&mut store, input_ptr as usize, &input_bytes)?;

            // Allocate space for the return value
            let output_ptr = malloc
                .call_async(
                    &mut store,
                    (size_of::<i32>() as i32 * 2, align_of::<i32>() as i32),
                )
                .await?;

            // Run the inbound filter function from the WASM module
            filter_outbound
                .call_async(&mut store, (input_ptr, input_len, output_ptr))
                .await
                .context("error calling wasm func")?;

            let mut output_ptr_bytes = [0u8; size_of::<i32>()];
            memory.read(&store, output_ptr as usize, &mut output_ptr_bytes)?;
            let mut output_len_bytes = [0u8; size_of::<i32>()];
            memory.read(
                &store,
                output_ptr as usize + size_of::<i32>(),
                &mut output_len_bytes,
            )?;
            let output_ptr = i32::from_le_bytes(output_ptr_bytes);
            let output_len = i32::from_le_bytes(output_len_bytes);

            let mut response_bytes = Vec::from_iter(iter::repeat_n(0u8, output_len as usize));
            memory.read(&store, output_ptr as usize, &mut response_bytes)?;
            let response = Outbound::decode(&mut &response_bytes[..])?;

            Ok(response)
        })
    }

    fn process_event(
        &mut self,
        input: ModuleInput,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Process>> + Sync + Send>> {
        let module = self.module.clone();
        let linker = self.linker.clone();

        Box::pin(async move {
            let mut store = Store::new(&ENGINE, db);
            let instance = linker.instantiate_async(&mut store, &module).await?;
            let Some(memory) = instance.get_memory(&mut store, "memory") else {
                anyhow::bail!("Could not load wasm memory.");
            };
            let malloc = instance
                .get_typed_func::<(i32, i32), i32>(&mut store, "malloc")
                .context("invalid type for malloc function")?;
            let process_event = instance
                .get_typed_func::<(i32, i32, i32), ()>(&mut store, "process_event")
                .context("Invalid type for process_event function")?;

            // Allocate the input in the WASM module
            let input_bytes = input.encode();
            let input_len = input_bytes.len() as i32;
            let input_ptr = malloc.call_async(&mut store, (input_len, 1)).await?;
            memory.write(&mut store, input_ptr as usize, &input_bytes)?;

            // Allocate space for the return value
            let output_ptr = malloc
                .call_async(
                    &mut store,
                    (size_of::<i32>() as i32 * 2, align_of::<i32>() as i32),
                )
                .await?;

            // Run the inbound filter function from the WASM module
            process_event
                .call_async(&mut store, (input_ptr, input_len, output_ptr))
                .await
                .context("error calling wasm func")?;

            let mut output_ptr_bytes = [0u8; size_of::<i32>()];
            memory.read(&store, output_ptr as usize, &mut output_ptr_bytes)?;
            let mut output_len_bytes = [0u8; size_of::<i32>()];
            memory.read(
                &store,
                output_ptr as usize + size_of::<i32>(),
                &mut output_len_bytes,
            )?;
            let output_ptr = i32::from_le_bytes(output_ptr_bytes);
            let output_len = i32::from_le_bytes(output_len_bytes);

            let mut response_bytes = Vec::from_iter(iter::repeat_n(0u8, output_len as usize));
            memory.read(&store, output_ptr as usize, &mut response_bytes)?;
            let response = Process::decode(&mut &response_bytes[..])?;

            Ok(response)
        })
    }
}

fn to_libsql(v: SqlValue) -> libsql::Value {
    match v {
        SqlValue::Null => libsql::Value::Null,
        SqlValue::Integer(x) => libsql::Value::Integer(x),
        SqlValue::Real(x) => libsql::Value::Real(x),
        SqlValue::Text(x) => libsql::Value::Text(x),
        SqlValue::Blob(x) => libsql::Value::Blob(x),
    }
}

fn from_libsql(v: libsql::Value) -> SqlValue {
    match v {
        libsql::Value::Null => SqlValue::Null,
        libsql::Value::Integer(x) => SqlValue::Integer(x),
        libsql::Value::Real(x) => SqlValue::Real(x),
        libsql::Value::Text(x) => SqlValue::Text(x),
        libsql::Value::Blob(x) => SqlValue::Blob(x),
    }
}
