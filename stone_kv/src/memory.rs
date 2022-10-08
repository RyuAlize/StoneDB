use crate::skiplist::Node;
use anyhow::{Ok, Result};

use super::arena::*;
use super::comparator::*;
use super::skiplist::Skiplist;
use super::{Bound, Range, Store};

#[derive(Clone)]
pub struct Memory {
    skiplist: Skiplist<BytewiseComparator, BlockArena>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            skiplist: Skiplist::new(BytewiseComparator::default(), BlockArena::default()),
        }
    }
}

impl Store for Memory {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let node = self.skiplist.get(key);
        return if !node.is_null() {
            unsafe { Ok(Some((*node).get_value().to_owned())) }
        } else {
            Ok(None)
        };
    }

    fn scan(&self, range: Range) -> crate::Scan {
        Box::new(Iter::new(self.skiplist.clone(), range))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.skiplist.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.skiplist.delete(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

struct Iter<C: Comparator, A: Arena> {
    skl: Skiplist<C, A>,
    range: Range,
    front_cursor: *mut Node,
    back_cursor: *mut Node,
}

impl<C: Comparator, A: Arena> Iter<C, A> {
    fn new(skl: Skiplist<C, A>, range: Range) -> Self {
        Self {
            skl,
            range,
            front_cursor: std::ptr::null_mut(),
            back_cursor: std::ptr::null_mut(),
        }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let next = match self.front_cursor.is_null() {
            true => match &self.range.start {
                Bound::Included(k) => {
                    let node = self.skl.get_greater_or_equal(k);
                    match self.skl.is_tail(node) {
                        true => Ok(None),
                        false => {
                            self.front_cursor = node as *mut _;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
                Bound::Excluded(k) => {
                    let node = self.skl.get_first_greater(k);
                    match node.is_null() {
                        true => Ok(None),
                        false => {
                            self.front_cursor = node as *mut _;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
                Bound::Unbounded => {
                    let node = self.skl.get_first();
                    match self.skl.is_tail(node) {
                        true => Ok(None),
                        false => {
                            self.front_cursor = node as *mut _;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
            },
            false => {
                let next_node = unsafe { (*self.front_cursor).get_next_at_first_level() };
                match self.skl.is_tail(next_node) {
                    true => Ok(None),
                    false => match &self.range.end {
                        Bound::Included(k) => {
                            if self.skl.key_is_greater_than_or_equal(k, next_node) {
                                self.front_cursor = next_node;
                                unsafe { Ok(Some((*next_node).get_key_value())) }
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Excluded(k) => {
                            if self.skl.key_is_greater_than(k, next_node) {
                                self.front_cursor = next_node;
                                unsafe { Ok(Some((*next_node).get_key_value())) }
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Unbounded => {
                            self.front_cursor = next_node;
                            unsafe { Ok(Some((*next_node).get_key_value())) }
                        }
                    },
                }
            }
        };
        next
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let next = match self.back_cursor.is_null() {
            true => match &self.range.end {
                Bound::Included(key) => {
                    let node = self.skl.get_less_or_equal(key) as *mut Node;
                    match self.skl.is_head(node) {
                        true => Ok(None),
                        false => {
                            self.back_cursor = node;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
                Bound::Excluded(key) => {
                    let node = self.skl.get_first_less(key) as *mut Node;
                    match node.is_null() {
                        true => Ok(None),
                        false => {
                            self.back_cursor = node;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
                Bound::Unbounded => {
                    let node = self.skl.get_last();
                    match self.skl.is_head(node) {
                        true => Ok(None),
                        false => {
                            self.back_cursor = node as *mut _;
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
            },
            false => {
                let prev_node = unsafe { (*self.back_cursor).get_prev() };
                return match self.skl.is_head(prev_node) {
                    true => Ok(None),
                    false => match &self.range.start {
                        Bound::Included(k) => {
                            if self.skl.key_is_less_than_or_equal(k, prev_node) {
                                self.back_cursor = prev_node;
                                unsafe { Ok(Some((*prev_node).get_key_value())) }
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Excluded(k) => {
                            if self.skl.key_is_less_than(k, prev_node) {
                                self.back_cursor = prev_node;
                                unsafe { Ok(Some((*prev_node).get_key_value())) }
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Unbounded => {
                            self.back_cursor = prev_node;
                            unsafe { Ok(Some((*prev_node).get_key_value())) }
                        }
                    },
                };
            }
        };
        next
    }
}

impl<C: Comparator, A: Arena> Iterator for Iter<C, A> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
impl<C: Comparator, A: Arena> DoubleEndedIterator for Iter<C, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_scan() -> Result<()> {
        let mut mem = Memory::new();
        for i in 0..10 {
            mem.set(&vec![i], &vec![i])?;
        }
        mem.delete(&vec![8]);

        let range = Range {
            start: Bound::Included(vec![2]),
            end: Bound::Unbounded, //Bound::Included(vec![11]),
        };
        let mut scan = mem.scan(range);
        while let Some(item) = scan.next() {
            let item = item.unwrap();
            println!("{:?}", item.0);
        }
        while let Some(item) = scan.next_back() {
            let item = item.unwrap();
            println!("{:?}", item.0);
        }
        Ok(())
    }
}
