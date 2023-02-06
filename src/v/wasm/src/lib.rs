pub fn add(left: usize, right: usize) -> usize {
    left + right
}

pub fn hello_from_rust() {
    println!("Hello from rust");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

#[cxx::bridge(namespace="wasm")]
mod ffi {
    extern "Rust" {
        fn hello_from_rust();
    }
}
