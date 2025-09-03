pub use anyhow;
pub use anyhow::Result;
pub use leaf_stream_types::*;

#[macro_export]
macro_rules! register_handlers {
    ($filter_inbound:ident, $filter_outbound:ident, $process_event:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn malloc(size: usize, align: usize) -> *mut u8 {
            unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(size, align).unwrap()) }
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "filter_inbound")]
        unsafe extern "C" fn __wasm_filter_inbound(
            input_ptr: *mut u8,
            input_len: usize,
            out_ptr: *mut (*mut u8, usize),
        ) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = ModuleInput::<String, String>::decode(&mut input_bytes).unwrap();

            let resp = $filter_inbound(input);
            let response = match resp {
                Ok(r) => r,
                Err(e) => InboundFilterResponse::Block {
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
            let input = ModuleInput::<String, String>::decode(&mut input_bytes).unwrap();

            let resp = $filter_outbound(input);
            let response = match resp {
                Ok(r) => r,
                // TODO: provide a way to return errors.
                Err(_) => OutboundFilterResponse::Block,
            };

            let mut response_bytes = response.encode();
            let ptr = response_bytes.as_mut_ptr();
            let len = response_bytes.len();
            std::mem::forget(response_bytes);
            unsafe { out_ptr.write((ptr, len)) };
        }

        #[unsafe(no_mangle)]
        #[unsafe(export_name = "process_event")]
        unsafe extern "C" fn __wasm_process_event(
            input_ptr: *mut u8,
            input_len: usize,
            out_ptr: *mut (*mut u8, usize),
        ) {
            let mut input_bytes = unsafe { std::slice::from_raw_parts(input_ptr, input_len) };
            let input = ModuleInput::<String, String>::decode(&mut input_bytes).unwrap();

            let resp = $process_event(input);
            let response = match resp {
                Ok(r) => r,
                // TODO: provide a way to return errors.
                Err(_) => ModuleUpdate {
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
