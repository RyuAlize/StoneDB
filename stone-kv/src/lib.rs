mod skiplist;
mod arena;
mod comparator;
mod memory;

use std::fmt::Display;

use anyhow::Result;

const BRANCHING: u32 = 4;
const MAX_HEIGHT: usize = 20;
const BLOCK_SIZE: usize = 4096;

pub trait Store: Send + Sync {

    /// Gets a value for a key, if it exists.
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
