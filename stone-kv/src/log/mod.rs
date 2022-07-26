mod hybird;

use std::fmt::Display;
use std::ops::{Bound, RangeBounds};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;

pub trait LogStore: Sync + Send {
    /// Appends a log entry, returning its index.
    fn append(&mut self, entry: Bytes) -> Result<u64>;

    /// Commits log entries up to and including the given index, making them immutable.
    fn commit(&mut self, index: u64) -> Result<()>;

    /// Returns the committed index, if any.
    fn committed(&self) -> u64;

    /// Fetches a log entry, if it exists.
    fn get(&self, index: u64) -> Result<Option<Bytes>>;

    /// Returns the number of entries in the log.
    fn len(&self) -> u64;

    /// Scans the log between the given indexes.
    fn scan(&self, range: Range) -> Scan;

    /// Returns the size of the log, in bytes.
    fn size(&self) -> u64;

    /// Truncates the log be removing any entries above the given index, and returns the
    /// highest index. Errors if asked to truncate any committed entries.
    fn truncate(&mut self, index: u64) -> Result<u64>;

    /// Gets a metadata value.
    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Sets a metadata value.
    fn set_metadata(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Returns true if the log has no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct Range {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl Range {
    pub fn from(range: impl RangeBounds<u64>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}


pub type Scan<'a> = Box<dyn Iterator<Item = Result<Bytes>> + 'a>;