pub use anyhow;
pub use anyhow::Result;
pub use leaf_stream_types::*;

#[link(wasm_import_module = "host")]
unsafe extern "C" {
    #[link_name = "query"]
    unsafe fn __wasm_query(query_ptr: *const u8, query_len: usize, out_ptr: *mut u8);
}

pub fn query(query: &str, params: Vec<(String, SqlValue)>) -> SqlRows {
    let query = SqlQuery {
        sql: query.into(),
        params,
    };
    let query_bytes = query.encode();
    let query_ptr = query_bytes.as_ptr();
    let query_len = query_bytes.len();

    let mut rows_bytes = unsafe {
        let out_ptr =
            std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(8, 4)) as *mut usize;
        __wasm_query(query_ptr, query_len, out_ptr as *mut u8);

        let rows_ptr = out_ptr.read() as *mut u8;
        let rows_len = out_ptr.add(1).read();
        std::slice::from_raw_parts(rows_ptr, rows_len)
    };
    SqlRows::decode(&mut rows_bytes).unwrap()
}

#[macro_export]
macro_rules! register_handlers {
    ($init_db:ident, $filter_inbound:ident, $filter_outbound:ident, $fetch:ident, $process_event:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn malloc(size: usize, align: usize) -> *mut u8 {
            unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(size, align).unwrap()) }
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "init_db")]
        unsafe extern "C" fn __wasm_init_db(input_ptr: *mut u8, input_len: usize) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = ModuleInit::decode(&mut input_bytes).unwrap();

            $init_db(input.creator, input.params);
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "filter_inbound")]
        unsafe extern "C" fn __wasm_filter_inbound(
            input_ptr: *mut u8,
            input_len: usize,
            out_ptr: *mut (*mut u8, usize),
        ) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = IncomingEvent::decode(&mut input_bytes).unwrap();

            let resp = $filter_inbound(input);
            let response = match resp {
                Ok(r) => r,
                Err(e) => Inbound::Block {
                    reason: format!("Error processing filter: {e}"),
                },
            };

            let mut response_bytes = response.encode();
            let ptr = response_bytes.as_mut_ptr();
            let len = response_bytes.len();
            std::mem::forget(response_bytes);
            unsafe { out_ptr.write((ptr, len)) };
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "filter_outbound")]
        unsafe extern "C" fn __wasm_filter_outbound(
            input_ptr: *mut u8,
            input_len: usize,
            out_ptr: *mut (*mut u8, usize),
        ) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = EventRequest::decode(&mut input_bytes).unwrap();

            let resp = $filter_outbound(input);
            let response = match resp {
                Ok(r) => r,
                // TODO: provide a way to return errors.
                Err(_) => Outbound::Block,
            };

            let mut response_bytes = response.encode();
            let ptr = response_bytes.as_mut_ptr();
            let len = response_bytes.len();
            std::mem::forget(response_bytes);
            unsafe { out_ptr.write((ptr, len)) };
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "fetch")]
        unsafe extern "C" fn __wasm_fetch(input_ptr: *mut u8, input_len: usize) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = FetchInput::decode(&mut input_bytes).unwrap();

            let resp = $fetch(input);
            let response: () = match resp {
                Ok(r) => r,
                // TODO: provide a way to return errors.
                Err(_) => (),
            };
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "process_event")]
        unsafe extern "C" fn __wasm_process_event(
            input_ptr: *mut u8,
            input_len: usize,
            out_ptr: *mut (*mut u8, usize),
        ) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = IncomingEvent::decode(&mut input_bytes).unwrap();

            let resp = $process_event(input);
            let response = match resp {
                Ok(r) => r,
                // TODO: provide a way to return errors.
                Err(_) => Process {
                    new_module: None,
                    new_params: None,
                },
            };

            let mut response_bytes = response.encode();
            let ptr = response_bytes.as_mut_ptr();
            let len = response_bytes.len();
            std::mem::forget(response_bytes);
            unsafe { out_ptr.write((ptr, len)) };
        }
    };
}
