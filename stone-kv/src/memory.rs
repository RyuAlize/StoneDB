use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use anyhow::{Result, Ok};

use crate::skiplist::Node;

use super::{Store, Range, RangeBounds, Bound};
use super::skiplist::Skiplist;
use super::arena::*;
use super::comparator::*;

pub struct Memory {
    inner: Arc<Skiplist<BytewiseComparator, BlockArena>>
}

impl Memory{
    pub fn new() -> Self {
        Self { inner: Arc::new(
            Skiplist::new(
                BytewiseComparator::default(),
                BlockArena::default()
                )) 
            }
    }
}

impl Store for Memory {
    
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.read_grard();
        let node = self.inner.get(key);      
        return if !node.is_null(){
            unsafe{
                let bytes = (*node).get_value();
   
                Ok(Some(bytes.to_vec()))
            }
        }
        else {
            Ok(None)
        }
     }

     fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.inner.write_guard();
        self.inner.insert(key, value);
        Ok(())
     }

     fn delete(&mut self, key: &[u8]) -> Result<()>{
        self.inner.write_guard();
        self.inner.delete(key);
        Ok(())
     }
 
     fn flush(&mut self) -> Result<()> {
        Ok(())
     }
}

struct Iter<C: Comparator, A: Arena> {
    skl: Arc<Skiplist<C, A>>,
    range: Range,
    front_cursor: AtomicPtr<Node>,
    
}

impl<C: Comparator, A: Arena> Iter<C, A> {
    fn new(skl: Arc<Skiplist<C, A>>, range: Range) -> Self{
        Self { skl, range, front_cursor: AtomicPtr::default() }
    }

    fn try_next(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.skl.read_grard();
        let cursor = self.front_cursor.load(Ordering::Relaxed);
        let next = match cursor.is_null(){
            true => match &self.range.start {
                Bound::Included(k) => {
                   let node =  self.skl.get(k);      
                   match node.is_null() {
                        true => {Ok(None)},
                        false => {
                            self.front_cursor.store(node, Ordering::SeqCst);
                            unsafe{
                                Ok(Some((*node).get_key_value()))
                            }
                        }
                   }
                },
                Bound::Excluded(k) => {
                    let node =  self.skl.get(k.as_slice());      
                    match node.is_null() {
                        true => {Ok(None)},
                        false => {
                            unsafe{
                                let next_node = (*node).get_next_at_first_level();
                                match next_node.is_null() {
                                    true => {Ok(None)},
                                    false => {
                                        self.front_cursor.store(next_node, Ordering::SeqCst);
                                        Ok(Some((*node).get_key_value()))
                                    }
                                }    
                            }
                        }
                   }
                },
                Bound::Unbounded => {
                    let node = self.skl.get_first();
                    match node.is_null() {
                        true => {Ok(None)},
                        false => {
                            self.front_cursor.store(node as *mut _, Ordering::SeqCst);
                            unsafe{        
                                Ok(Some((*node).get_key_value()))
                            }
                        }
                    }
                }
            },
            false => {
                unsafe{
                    let next = (*cursor).get_next_at_first_level();
                    match next.is_null() {
                        true => {Ok(None)},
                        false => {
                            
                            match &self.range.end {
                                Bound::Included(k) =>{
                                    if !self.skl.key_is_less_than_or_equal(k.as_slice(), next){
                                        Ok(None)
                                    }
                                    else{
                                        self.front_cursor.store(next, Ordering::SeqCst);
                                        Ok(Some((*next).get_key_value()))
                                    }
                                },
                                Bound::Excluded(k) =>{
                                    if !self.skl.key_is_less_than(k.as_slice(), next) {
                                        Ok(None)
                                    }
                                    else{
                                        self.front_cursor.store(next, Ordering::SeqCst);
                                        Ok(Some((*next).get_key_value()))
                                    }
                                },
                                Bound::Unbounded => {
                                    self.front_cursor.store(next, Ordering::SeqCst);
                                    Ok(Some((*next).get_key_value()))
                                }
                            }
                        }
                    }
                }
            }
        };
        next
    }

}