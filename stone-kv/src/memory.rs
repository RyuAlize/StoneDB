use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use anyhow::{Result, Ok};
use bytes::Bytes;
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
    fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        self.inner.read_grard();
        let node = self.inner.get(key);      
        return if !node.is_null(){
            unsafe{
                Ok(Some((*node).get_value().to_owned()))
            }
        }
        else {
            Ok(None)
        }
     }

     fn scan(&self, range: Range) -> crate::Scan {
         Box::new(Iter::new(self.inner.clone(), range))
     }

     fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.inner.write_guard();
        self.inner.insert(key, value);
        Ok(())
     }

     fn delete(&mut self, key: &Bytes) -> Result<()>{
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
    front_cursor: AtomicPtr<Node>
}

impl<C: Comparator, A: Arena> Iter<C, A> {
    fn new(skl: Arc<Skiplist<C, A>>, range: Range) -> Self{
        Self { skl, range, front_cursor: AtomicPtr::default() }
    }

    fn try_next(&self) -> Result<Option<(Bytes, Bytes)>> {
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
                    let node =  self.skl.get(k);      
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
                                    if self.skl.key_is_greater_than_or_equal(k, next){
                                        self.front_cursor.store(next, Ordering::SeqCst);
                                        Ok(Some((*next).get_key_value()))                                      
                                    }
                                    else{
                                        Ok(None)
                                    }
                                },
                                Bound::Excluded(k) =>{
                                    if self.skl.key_is_greater_than(k, next) {
                                        self.front_cursor.store(next, Ordering::SeqCst);
                                        Ok(Some((*next).get_key_value()))                                  
                                    }
                                    else{
                                        Ok(None)
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

impl<C: Comparator, A: Arena> Iterator for Iter<C, A>{
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[cfg(test)]
mod test{
    use bytes::Buf;

    use super::*;   
    #[test]
    fn test_crud() {
        let mut db = Memory::new();
        db.set(Bytes::from("aaa"), Bytes::from("aaa"));
        db.set(Bytes::from("bbb"), Bytes::from("bbb"));
        db.set(Bytes::from("ccc"), Bytes::from("ccc"));
        db.set(Bytes::from("aaa"), Bytes::from("aac"));
        assert_eq!(db.get(&Bytes::from("aaa")).unwrap(), Some(Bytes::from("aac")));
        assert_eq!(db.get(&Bytes::from("bbb")).unwrap(), Some(Bytes::from("bbb")));
        assert_eq!(db.get(&Bytes::from("ccc")).unwrap(), Some(Bytes::from("ccc")));
       
        for i in 1..1000 as i32 {
            db.set(Bytes::from(i.to_be_bytes().to_vec()), Bytes::from(i.to_be_bytes().to_vec()));
        }

        let range = Range{
            start:Bound::Included(Bytes::from(30i32.to_be_bytes().to_vec())),
            end: Bound::Excluded(Bytes::from(900i32.to_be_bytes().to_vec()))
        };
        let mut scan = db.scan(range);
        let mut i = 30;
        while let Some(item) = scan.next() {
            let item = item.unwrap();
            let mut key = item.0;
            let mut value = item.1;
            assert_eq!(key.get_i32(), i);
            assert_eq!(value.get_i32(), i);
            i += 1;
        }
    }

}