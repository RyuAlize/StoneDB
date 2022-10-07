use rand::random;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, mem, ptr};

use super::arena::*;
use super::comparator::*;
use super::{BRANCHING, MAX_HEIGHT};

#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Vec<u8>,
    value: Vec<u8>,
    height: usize,
    pub prev: [AtomicPtr<Node>; 1],
    pub tower: [AtomicPtr<Node>; 0],
}

impl Node {
    fn new<A: Arena>(arena: &A, key: Vec<u8>, value: Vec<u8>, height: usize) -> *const Self {
        let pointers_size = (height + 1) * mem::size_of::<AtomicPtr<Self>>();
        let size = mem::size_of::<Self>() + pointers_size;
        let align = mem::align_of::<Self>();
        let p = unsafe { arena.allocate(size, align) } as *const Self as *mut Self;
        unsafe {
            let node = &mut *p;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value, value);
            ptr::write(&mut node.height, height);
            ptr::write_bytes(node.prev.as_mut_ptr(), 0, 1);
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height);
            p as *const Self
        }
    }

    #[inline]
    pub fn get_key_value(&self) -> (Vec<u8>, Vec<u8>) {
        let key = self.get_key().to_owned();
        let value = self.get_value().to_owned();
        (key, value)
    }
    #[inline]
    pub fn set_value(&mut self, value: Vec<u8>) {
        self.value = value;
    }

    #[inline]
    pub fn get_key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    pub fn get_value(&self) -> &[u8] {
        &self.value
    }

    #[inline]
    pub fn get_next_at_first_level(&self) -> *mut Node {
        self.get_next(1)
    }

    #[inline]
    fn get_next(&self, height: usize) -> *mut Node {
        unsafe { self.tower.get_unchecked(height - 1).load(Ordering::Acquire) }
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
    fn get_prev(&self) -> *mut Node {
        unsafe { self.prev.get_unchecked(0).load(Ordering::Acquire) }
    }

    #[inline]
    fn set_prev(&self, node: *mut Node) {
        unsafe {
            self.prev.get_unchecked(0).store(node, Ordering::Release);
        }
    }
}

pub struct Skiplist<C: Comparator, A: Arena> {
    inner: Arc<RwLock<Inner<C, A>>>,
}

impl<C: Comparator, A: Arena> Clone for Skiplist<C, A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<C: Comparator, A: Arena> {
    head: AtomicPtr<Node>,
    max_height: AtomicUsize,
    arena: A,
    comparator: C,
    count: AtomicUsize,
    size: AtomicUsize,
}

impl<C: Comparator, A: Arena> Skiplist<C, A> {
    pub fn new(cmp: C, arena: A) -> Self {
        let head = Node::new(&arena, Vec::new(), Vec::new(), MAX_HEIGHT);

        let inner = Inner {
            head: AtomicPtr::new(head as *mut Node),
            max_height: AtomicUsize::new(1),
            arena,
            comparator: cmp,
            count: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn total_size(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.size.load(Ordering::Acquire)
    }

    pub fn get(&self, key: &[u8]) -> *mut Node {
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(key, Some(&mut prev));
        if !node.is_null() {
            unsafe {
                let inner = self.inner.read().unwrap();
                if inner.comparator.compare((&(*node)).get_key(), key) == cmp::Ordering::Equal {
                    return node;
                }
            }
        }
        ptr::null_mut()
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {
        let length = key.len();
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        let inner = self.inner.write().unwrap();
        if !node.is_null() {
            unsafe {
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Equal {
                    (*node).set_value(value.to_owned());
                    return;
                }
            }
        }
        let height = rand_height();
        let max_height = inner.max_height.load(Ordering::Acquire);
        if height > max_height {
            for p in prev.iter_mut().take(height).skip(max_height) {
                *p = inner.head.load(Ordering::Relaxed);
            }
            inner.max_height.store(height, Ordering::Release);
        }

        let new_node = Node::new(&inner.arena, key.to_owned(), value.to_owned(), height);
        unsafe {
            (*new_node).set_prev(prev[0] as *mut Node);
            for i in 1..=height {
                (*new_node).set_next(i, (*(prev[i - 1])).get_next(i));
                (*(prev[i - 1])).set_next(i, new_node as *mut Node);
            }
        }
        inner.count.fetch_add(1, Ordering::SeqCst);
        inner.size.fetch_add(length, Ordering::SeqCst);
    }

    pub fn delete(&self, key: &[u8]) -> *mut Node {
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(key, Some(&mut prev));
        let inner = self.inner.write().unwrap();
        if node.is_null() {
            return ptr::null_mut();
        }
        unsafe {
            assert_eq!(
                inner.comparator.compare((&(*node)).get_key(), &key),
                cmp::Ordering::Equal,
                "[skiplist] delete [key={:?}] is not found",
                &key
            );
            let next_node = (*node).get_next(1);
            (*next_node).set_prev(prev[0] as *mut Node);
            let height = (*node).height;
            for i in 1..=height {
                (*(prev[i - 1])).set_next(i, (*node).get_next(i));
            }
            let max_height = inner.max_height.load(Ordering::Relaxed);
            let head = inner.head.load(Ordering::Relaxed);
            for i in (1..=max_height).rev() {
                if (*head).get_next(i).is_null() {
                    inner.max_height.fetch_sub(1, Ordering::Relaxed);
                } else {
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
        let inner = self.inner.read().unwrap();
        let mut level = inner.max_height.load(Ordering::Acquire);
        let mut node = inner.head.load(Ordering::Relaxed);
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
        let inner = self.inner.read().unwrap();
        let mut level = inner.max_height.load(Ordering::Acquire);
        let mut node = inner.head.load(Ordering::Relaxed);
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if next.is_null()
                    || inner.comparator.compare((*next).get_key(), key) != cmp::Ordering::Less
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

    pub fn get_first(&self) -> *const Node {
        let inner = self.inner.read().unwrap();
        let node = inner.head.load(Ordering::Relaxed);
        unsafe { (*node).get_next(1) }
    }

    fn find_last(&self) -> *const Node {
        let inner = self.inner.read().unwrap();
        let mut level = inner.max_height.load(Ordering::Acquire);
        let mut node = inner.head.load(Ordering::Relaxed);
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

    pub fn key_is_less_than_or_equal(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if n.is_null() {
            true
        } else {
            let node_key = unsafe { (*n).get_key() };
            !matches!(
                inner.comparator.compare(key, node_key),
                cmp::Ordering::Greater
            )
        }
    }

    pub fn key_is_greater_than_or_equal(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if n.is_null() {
            false
        } else {
            let node_key = unsafe { (*n).get_key() };
            !matches!(inner.comparator.compare(key, node_key), cmp::Ordering::Less)
        }
    }

    pub fn key_is_less_than(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if n.is_null() {
            true
        } else {
            let node_key = unsafe { (*n).get_key() };
            matches!(inner.comparator.compare(key, node_key), cmp::Ordering::Less)
        }
    }

    pub fn key_is_greater_than(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if n.is_null() {
            false
        } else {
            let node_key = unsafe { (*n).get_key() };
            matches!(
                inner.comparator.compare(key, node_key),
                cmp::Ordering::Greater
            )
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
mod test {

    use super::*;
    #[test]
    fn test_skiplist() {
        let skiplist = Skiplist::new(BytewiseComparator::default(), BlockArena::default());
        for i in 0..100 {
            skiplist.insert(&vec![i], &vec![i]);
        }
        for i in 10..90 {
            skiplist.delete(&vec![i]);
        }
        skiplist.delete(&vec![5]);
        let mut head = skiplist.get(&vec![99]);
        print_skiplist_reverse(head);
    }

    fn print_skiplist(mut head: *mut Node) {
        unsafe {
            while !head.is_null() {
                head = (*head).get_next(1);
                print!("{:?} ", (*head).get_value().to_owned());
            }
        }
    }
    fn print_skiplist_reverse(mut head: *mut Node) {
        unsafe {
            while !head.is_null() {
                print!("{:?} ", (*head).get_value().to_owned());
                head = (*head).get_prev();
            }
        }
    }
}
