extern crate core;

mod skiplist;
mod arena;
mod comparator;
mod memory;
mod mvcc;

use std::{fmt::Display, ops::{Bound, RangeBounds}};
use bytes::Bytes;
use anyhow::Result;

const BRANCHING: u32 = 4;
const MAX_HEIGHT: usize = 20;
const BLOCK_SIZE: usize = 4096;

pub trait Store: Send + Sync {

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>>;

    /// Iterates over an ordered range of key/value pairs.
    fn scan(&self, range: Range) -> Scan;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: Bytes, value: Bytes) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &Bytes) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;
}


pub struct Range {
    start: Bound<Bytes>,
    end: Bound<Bytes>
}

impl Range {
    pub fn from<R:RangeBounds<Bytes>>(range: R) -> Self{
        Self{
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_owned()),
                Bound::Excluded(v) => Bound::Excluded(v.to_owned()),
                Bound::Unbounded => Bound::Unbounded
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_owned()),
                Bound::Excluded(v) => Bound::Excluded(v.to_owned()),
                Bound::Unbounded => Bound::Unbounded
            }
        }
    }

    fn contains(&self, v: &Bytes) -> bool {
        (match &self.start {
            Bound::Included(start) => &**start <= v,
            Bound::Excluded(start) => &**start < v,
            Bound::Unbounded => true,
        }) && (match &self.end {
            Bound::Included(end) => v <= &**end,
            Bound::Excluded(end) => v < &**end,
            Bound::Unbounded => true,
        })
    }
}

impl RangeBounds<Bytes> for Range {
    fn start_bound(&self) -> Bound<&Bytes> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&Bytes> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

pub type Scan = Box<dyn Iterator<Item = Result<(Bytes, Bytes)>> + Send>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
