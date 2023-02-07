use sha2::{Sha256, Digest};

#[no_mangle]
pub fn test_function() {
    let num_hashes = 10_000_000;
    for i in 1..num_hashes{
        let mut hasher = Sha256::new();
        hasher.update(format!("{}", i).as_bytes());
        let _result = hasher.finalize();
        if i % 200_000 == 0 {
            println!("In progress...");
        }
    }
    println!("{} hashes computed", num_hashes);
}

fn main() {
    println!("Hello, world!");
}
