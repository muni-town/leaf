#[unsafe(no_mangle)]
pub extern "C" fn add(left: i32, right: i32) -> i32 {
    let mut v = Vec::new();
    v.push(left);
    v.push(right);
    v.iter().sum()
}
