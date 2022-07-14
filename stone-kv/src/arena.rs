use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::BLOCK_SIZE;

pub trait Arena: Send + Sync {

    unsafe fn allocate<T>(&self, chunk: usize, align: usize) -> *mut T;

    fn memory_used(&self) -> usize;
}

#[derive(Default)]
pub struct BlockArena {
    ptr: AtomicPtr<u8>,
    bytes_remaining: AtomicUsize,
    blocks: Arc<Mutex<Vec<Vec<u8>>>>,
    memory_usage: AtomicUsize,
}

impl BlockArena {
    fn allocate_fallback(&self, size: usize) -> *mut u8 {
        if size > BLOCK_SIZE / 4 {

            return self.allocate_new_block(size);
        }

        let new_block_ptr = self.allocate_new_block(BLOCK_SIZE);
        unsafe {
            let ptr = new_block_ptr.add(size);
            self.ptr.store(ptr, Ordering::Release);
        };
        self.bytes_remaining
            .store(BLOCK_SIZE - size, Ordering::Release);
        new_block_ptr
    }

    fn allocate_new_block(&self, block_bytes: usize) -> *mut u8 {
        let mut new_block = vec![0; block_bytes];
        let p = new_block.as_mut_ptr();
        let mut guard = self.blocks.lock().unwrap();
        guard.push(new_block);
        self.memory_usage.fetch_add(block_bytes, Ordering::Relaxed);
        p
    }
}

impl Arena for BlockArena {
    unsafe fn allocate<T>(&self, chunk: usize, align: usize) -> *mut T {
        assert!(chunk > 0);
        let ptr_size = mem::size_of::<usize>();

        assert_eq!(align & (align - 1), 0);

        let slop = {
            let current_mod = self.ptr.load(Ordering::Acquire) as usize & (align - 1);
            if current_mod == 0 {
                0
            } else {
                align - current_mod
            }
        };
        let needed = chunk + slop;
        let result = if needed <= self.bytes_remaining.load(Ordering::Acquire) {

            let p = self.ptr.load(Ordering::Acquire).add(slop);
            self.ptr.store(p.add(chunk), Ordering::Release);
            self.bytes_remaining.fetch_sub(needed, Ordering::SeqCst);
            p
        } else {
            self.allocate_fallback(chunk)
        };
        assert_eq!(
            result as usize & (align - 1),
            0,
            "allocated memory should be aligned with {}",
            ptr_size
        );
        result as *mut T
    }

    #[inline]
    fn memory_used(&self) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }
}


#[cfg(test)]
mod test{
    use super::*;
    
    #[test]
    fn test_new_arena() {
        let a = BlockArena::default();
        assert_eq!(a.memory_used(), 0);
        assert_eq!(a.bytes_remaining.load(Ordering::Acquire), 0);
        assert_eq!(a.ptr.load(Ordering::Acquire), ptr::null_mut());
        
    }

    #[test]
    fn test_allocate_new_block() {
        let a = BlockArena::default();
        let mut expect_size = 0;
        for (i, size) in [1, 128, 256, 1000, 4096, 10000].iter().enumerate() {
            a.allocate_new_block(*size);
            expect_size += *size;
            assert_eq!(a.memory_used(), expect_size, "memory used should match");
            
            assert_eq!(
                a.blocks.lock().unwrap().len(),
                i + 1,
                "number of blocks should match"
            )
        }
    }
}
