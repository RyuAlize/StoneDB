use std::ptr;
use anyhow::Result;

use super::Store;
use super::skiplist::Skiplist;
use super::arena::*;
use super::comparator::*;

pub struct Memory {
    inner: Skiplist<BytewiseComparator, BlockArena>
}

impl Store for Memory {
    
    fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
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
        self.inner.insert(key, value);
        Ok(())
     }
 

     fn delete(&mut self, key: Vec<u8>) -> Result<()>{
        self.inner.delete(key);
        Ok(())
     }
 
     fn flush(&mut self) -> Result<()> {
        Ok(())
     }
}