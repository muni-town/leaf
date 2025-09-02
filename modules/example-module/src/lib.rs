use std::alloc::Layout;

#[unsafe(no_mangle)]
extern "C" fn malloc(size: usize, align: usize) -> *mut u8 {
    unsafe { std::alloc::alloc(Layout::from_size_align(size, align).unwrap()) }
}


