use std::{cmp, mem, ptr};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::RwLock;
use bytes::Bytes;
use rand::random;

use super::{MAX_HEIGHT, BRANCHING};
use super::arena::*;
use super::comparator::*;

#[derive(Debug)]
#[repr(C)]
pub struct Node {
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
    pub fn get_value(&self) -> &Bytes {
        &self.value
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

pub struct Skiplist<C: Comparator, A: Arena> {
    head: AtomicPtr<Node>,
    max_height: AtomicUsize,
    arena: A,
    comparator: C,
    count: AtomicUsize,
    size: AtomicUsize,
    lock: RwLock<()>,
}

impl<C: Comparator, A: Arena> Skiplist<C, A> {
    pub fn new(cmp: C, arena: A) -> Self {
        let head = Node::new(&arena, Bytes::new(), Bytes::new(), MAX_HEIGHT);
        Skiplist {
            head: AtomicPtr::new(head as *mut Node),
            max_height: AtomicUsize::new(1),
            arena,
            comparator: cmp,
            count: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
            lock: RwLock::new(())
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn total_size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }

    pub fn get(&self, key: impl Into<Bytes>) -> *mut Node {
        self.lock.read();
        let key = key.into();
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if !node.is_null() {
            unsafe{
                if self.comparator.compare((&(*node)).key(), &key) == cmp::Ordering::Equal {
                    return node;
                }
            }
        }
        ptr::null_mut()
    }

    pub fn insert(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.lock.write();
        let key = key.into();
        let length = key.len();
        let val = value.into();
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if !node.is_null() {
            unsafe {
                assert_ne!(
                    self.comparator.compare((&(*node)).key(), &key),
                    cmp::Ordering::Equal,
                    "[skiplist] duplicate insertion [key={:?}] is not allowed",
                    &key
                );
            }
        }
        let height = rand_height();
        let max_height = self.max_height.load(Ordering::Acquire);
        if height > max_height {
            for p in prev.iter_mut().take(height).skip(max_height) {
                *p = self.head.load(Ordering::Relaxed);
            }
            self.max_height.store(height, Ordering::Release);
        }
 
        let new_node = Node::new(&self.arena, key, val, height );
        unsafe {
            for i in 1..=height {
                (*new_node).set_next(i, (*(prev[i - 1])).get_next(i));
                (*(prev[i - 1])).set_next(i, new_node as *mut Node);
            }
        }
        self.count.fetch_add(1, Ordering::SeqCst);
        self.size.fetch_add(length, Ordering::SeqCst);
    }

    pub fn delete(&self, key: impl Into<Bytes>) -> *mut Node{
        self.lock.write();
        let key = key.into();
        let length = key.len();
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if node.is_null() {return ptr::null_mut();}
        unsafe {
            assert_eq!(
                self.comparator.compare((&(*node)).key(), &key),
                cmp::Ordering::Equal,
                "[skiplist] delete [key={:?}] is not found",
                &key
            );
            let height = (*node).height;
            for i in 1..=height {
                (*(prev[i - 1])).set_next(i, (*node).get_next(i));
            }
            let max_height =self.max_height.load(Ordering::Relaxed); 
            let head = self.head.load(Ordering::Relaxed);
            for i in (1..=max_height).rev() {
                if (*head).get_next(i).is_null() {
                    self.max_height.fetch_sub(1, Ordering::Relaxed);
                }
                else{
                    break;
                }
            }
            node
        }
        
    }

    fn find_greater_or_equal(
        &self,
        key: &[u8],
        mut prev_nodes: Option<&mut [*const Node]>,
    ) -> *mut Node {
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head.load(Ordering::Relaxed);
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if self.key_is_less_than_or_equal(key, next) {
                    if let Some(ref mut p) = prev_nodes {
                        p[level - 1] = node;
                    }
                    if level == 1 {
                        return next;
                    }
                    level -= 1;
                } else {
                    node = next;
                }
            }
        }
    }

    fn find_less_than(&self, key: &[u8]) -> *const Node {
        self.lock.read();
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head.load(Ordering::Relaxed);
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null()
                    || self.comparator.compare((*next).key(), key) != cmp::Ordering::Less
                {
                    if level == 1 {
                        return node;
                    } else {
                        level -= 1;
                    }
                } else {
                    node = next;
                }
            }
        }
    }

    fn find_last(&self) -> *const Node {
        self.lock.read();
        let mut level = self.max_height.load(Ordering::Acquire);
        let mut node = self.head.load(Ordering::Relaxed);
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null() {
                    if level == 1 {
                        return node;
                    }
                    level -= 1;
                } else {
                    node = next;
                }
            }
        }
    }

    fn key_is_less_than_or_equal(&self, key: &[u8], n: *const Node) -> bool {
        if n.is_null() {
            true
        } else {
            let node_key = unsafe { (*n).key() };
            !matches!(self.comparator.compare(key, node_key), cmp::Ordering::Greater)
        }
    }

}

fn rand_height() -> usize {
    let mut height = 1;
    loop {
        if height < MAX_HEIGHT && random::<u32>() % BRANCHING == 0 {
            height += 1;
        } else {
            break;
        }
    }
    height
}

#[cfg(test)]
mod test{
    use super::*;
    #[test]
    fn test_skiplist() {
        let skiplist = Skiplist::new(BytewiseComparator::default(), BlockArena::default());
        for i in 0..100 {       
            skiplist.insert(vec![i], vec![i]);
        }
        for i in 0..100 {
            let node = skiplist.delete(vec![i]);
            if !node.is_null() {
                unsafe{assert!((*node).value.as_ref().eq(&vec![i]));}
            }
        }
    }
}