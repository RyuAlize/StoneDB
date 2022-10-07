use crate::skiplist::Node;
use anyhow::{Ok, Result};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use super::arena::*;
use super::comparator::*;
use super::skiplist::Skiplist;
use super::{Bound, Range, RangeBounds, Store};

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
    front_cursor: AtomicPtr<Node>,
}

impl<C: Comparator, A: Arena> Iter<C, A> {
    fn new(skl: Skiplist<C, A>, range: Range) -> Self {
        Self {
            skl,
            range,
            front_cursor: AtomicPtr::default(),
        }
    }

    fn try_next(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let cursor = self.front_cursor.load(Ordering::Relaxed);
        let next = match cursor.is_null() {
            true => match &self.range.start {
                Bound::Included(k) => {
                    let node = self.skl.get(k);
                    match node.is_null() {
                        true => Ok(None),
                        false => {
                            self.front_cursor.store(node, Ordering::SeqCst);
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
                Bound::Excluded(k) => {
                    let node = self.skl.get(k);
                    match node.is_null() {
                        true => Ok(None),
                        false => unsafe {
                            let next_node = (*node).get_next_at_first_level();
                            match next_node.is_null() {
                                true => Ok(None),
                                false => {
                                    self.front_cursor.store(next_node, Ordering::SeqCst);
                                    Ok(Some((*next_node).get_key_value()))
                                }
                            }
                        },
                    }
                }
                Bound::Unbounded => {
                    let node = self.skl.get_first();
                    match node.is_null() {
                        true => Ok(None),
                        false => {
                            self.front_cursor.store(node as *mut _, Ordering::SeqCst);
                            unsafe { Ok(Some((*node).get_key_value())) }
                        }
                    }
                }
            },
            false => unsafe {
                let next = (*cursor).get_next_at_first_level();
                match next.is_null() {
                    true => Ok(None),
                    false => match &self.range.end {
                        Bound::Included(k) => {
                            if self.skl.key_is_greater_than_or_equal(k, next) {
                                self.front_cursor.store(next, Ordering::SeqCst);
                                Ok(Some((*next).get_key_value()))
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Excluded(k) => {
                            if self.skl.key_is_greater_than(k, next) {
                                self.front_cursor.store(next, Ordering::SeqCst);
                                Ok(Some((*next).get_key_value()))
                            } else {
                                Ok(None)
                            }
                        }
                        Bound::Unbounded => {
                            self.front_cursor.store(next, Ordering::SeqCst);
                            Ok(Some((*next).get_key_value()))
                        }
                    },
                }
            },
        };
        next
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        
    }
}

impl<C: Comparator, A: Arena> Iterator for Iter<C, A> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
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

        let range = Range {
            start: Bound::Included(vec![2]),
            end: Bound::Excluded(vec![11]),
        };
        let mut scan = mem.scan(range);
        while let Some(item) = scan.next() {
            let item = item.unwrap();
            println!("{:?}", item.0);
        }

        Ok(())
    }
}
