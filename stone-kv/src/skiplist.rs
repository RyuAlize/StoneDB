use std::{mem, ptr};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use bytes::Bytes;

use super::MAX_HEIGHT;
use super::arena::*;

#[derive(Debug)]
#[repr(C)]
struct Node {
    key: Bytes,
    value: Bytes,
    height: usize,
    tower: [AtomicPtr<Node>; 0],
}

impl Node {
    fn new<A: Arena>(arena: &A, key: Bytes, value: Bytes, height: usize) -> *const Self {
        let pointers_size = height * mem::size_of::<AtomicPtr<Self>>();
        let size = mem::size_of::<Self>() + pointers_size;
        let align = mem::align_of::<Self>();
        let p = unsafe { arena.allocate(size, align) } as *const Self as *mut Self;
        unsafe {
            let node = &mut *p;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value,value);
            ptr::write(&mut node.height, height);
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height);
            p as *const Self
        }
    }

    #[inline]
    fn get_next(&self, height: usize) -> *mut Node {
        unsafe {
            self.tower
                .get_unchecked(height - 1)
                .load(Ordering::Acquire)
        }
    }

    #[inline]
    fn set_next(&self, height: usize, node: *mut Node) {
        unsafe {
            self.tower
                .get_unchecked(height - 1)
                .store(node, Ordering::Release);
        }
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.key.as_ref()
    }
}