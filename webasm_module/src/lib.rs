use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn add1000000000_wasm() -> i32 {
    let mut i: i32 = 0;
    while i < 1000000000 {
        i += 1;
    }
    return i;
}

#[wasm_bindgen]
pub fn fibonacci_wasm(n: i32) -> i32 {
    if n <= 0 {
        return 0;
    } else if n == 1 {
        return 1;
    } else {
        return fibonacci_wasm(n - 1) + fibonacci_wasm(n - 2);
    }
}
