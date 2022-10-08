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
    pub prev: [*mut Node; 1],
    pub tower: [*mut Node; 0],
}

impl Node {
    fn new<A: Arena>(arena: &A, key: Vec<u8>, value: Vec<u8>, height: usize) -> *const Self {
        let pointers_size = (height + 1) * mem::size_of::<Self>();
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
        unsafe { *self.tower.get_unchecked(height - 1) }
    }

    #[inline]
    fn set_next(&mut self, height: usize, node: *mut Node) {
        unsafe {
            *self.tower.get_unchecked_mut(height - 1) = node;
        }
    }

    #[inline]
    pub fn get_prev(&self) -> *mut Node {
        unsafe { *self.prev.get_unchecked(0) }
    }

    #[inline]
    fn set_prev(&mut self, node: *mut Node) {
        unsafe {
            *self.prev.get_unchecked_mut(0) = node;
        }
    }
}

pub struct Skiplist<C: Comparator, A: Arena> {
    inner: Arc<RwLock<Inner<C, A>>>,
}

unsafe impl<C: Comparator, A: Arena> Send for Skiplist<C, A> {}
unsafe impl<C: Comparator, A: Arena> Sync for Skiplist<C, A> {}

impl<C: Comparator, A: Arena> Clone for Skiplist<C, A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<C: Comparator, A: Arena> {
    head: *const Node,
    tail: *const Node,
    max_height: usize,
    arena: A,
    comparator: C,
    count: usize,
    size: usize,
}

impl<C: Comparator, A: Arena> Skiplist<C, A> {
    pub fn new(cmp: C, arena: A) -> Self {
        let head = Node::new(&arena, Vec::new(), Vec::new(), MAX_HEIGHT) as *mut Node;
        let tail = Node::new(&arena, Vec::new(), Vec::new(), MAX_HEIGHT) as *mut Node;

        unsafe {
            (*tail).set_prev(head as *mut _);
            for i in 1..MAX_HEIGHT {
                (*head).set_next(i, tail as *mut _);
            }
        }

        let inner = Inner {
            head,
            tail,
            max_height: 1,
            arena,
            comparator: cmp,
            count: 0,
            size: 0,
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.count
    }

    #[inline]
    pub fn total_size(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.size
    }

    pub fn get(&self, key: &[u8]) -> *mut Node {
        let node = self.get_greater_or_equal(key);
        let inner = self.inner.read().unwrap();
        if !self.is_tail(node) {
            unsafe {
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Equal {
                    return node as *mut _;
                }
            }
        }
        ptr::null_mut()
    }

    pub fn get_first_greater(&self, key: &[u8]) -> *const Node {
        let node = self.get_greater_or_equal(key);
        let inner = self.inner.read().unwrap();
        if !self.is_tail(node) {
            unsafe {
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Greater {
                    return node;
                }
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Equal {
                    let next = (*node).get_next_at_first_level();
                    return match self.is_tail(next) {
                        true => ptr::null(),
                        false => next,
                    };
                }
            }
        }
        ptr::null()
    }

    pub fn get_greater_or_equal(&self, key: &[u8]) -> *const Node {
        let mut prev = [ptr::null(); MAX_HEIGHT];
        self.find_greater_or_equal(key, Some(&mut prev))
    }

    pub fn get_first_less(&self, key: &[u8]) -> *const Node {
        let node = self.get_less_or_equal(key);
        let inner = self.inner.read().unwrap();
        if !self.is_head(node) {
            unsafe {
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Less {
                    return node;
                }
                if inner.comparator.compare((*node).get_key(), key) == cmp::Ordering::Equal {
                    let next = (*node).get_prev();
                    return match self.is_head(next as *const _) {
                        true => ptr::null(),
                        false => next,
                    };
                }
            }
        }
        ptr::null()
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));
        if !node.is_null() {
            let inner = self.inner.read().unwrap();
            unsafe {
                if inner.comparator.compare(key, (*node).get_key()) == cmp::Ordering::Equal {
                    (*(node as *mut Node)).set_value(value.to_owned());
                    return;
                }
            }
        }
        let height = rand_height();
        let new_node = {
            let mut inner = self.inner.write().unwrap();
            let max_height = inner.max_height;
            if height > max_height {
                for p in prev.iter_mut().take(height).skip(max_height) {
                    *p = inner.head;
                }
                inner.max_height = height;
            }
            let new_node =
                Node::new(&inner.arena, key.to_owned(), value.to_owned(), height) as *mut Node;
            unsafe {
                let tmp = (*(prev[0] as *mut Node)).get_next_at_first_level();
                if std::ptr::eq(tmp, inner.tail) {
                    (*tmp).set_prev(new_node);
                }
            }
            inner.count += 1;
            inner.size += 1;
            new_node
        };

        unsafe {
            (*new_node).set_prev(prev[0] as *mut Node);
            for i in 1..=height {
                (*new_node).set_next(i, (*(prev[i - 1])).get_next(i));
                (*(prev[i - 1] as *mut Node)).set_next(i, new_node);
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> *const Node {
        let mut prev = [ptr::null(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(key, Some(&mut prev));
        if self.is_tail(node) {
            return ptr::null();
        }
        let mut inner = self.inner.write().unwrap();
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
                (*(prev[i - 1] as *mut Node)).set_next(i, (*node).get_next(i));
            }
            let max_height = inner.max_height;
            let head = inner.head;
            for i in (1..=max_height).rev() {
                if (*head).get_next(i).is_null() {
                    inner.max_height -= 1;
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
    ) -> *const Node {
        let inner = self.inner.read().unwrap();
        let mut level = inner.max_height;
        let mut node = inner.head;
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

    pub fn get_less_or_equal(&self, key: &[u8]) -> *const Node {
        let inner = self.inner.read().unwrap();
        let mut level = inner.max_height;
        let mut node = inner.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if self.is_tail(next)
                    || inner.comparator.compare((*next).get_key(), key) == cmp::Ordering::Greater
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

        unsafe { (*inner.head).get_next(1) }
    }

    pub fn get_last(&self) -> *const Node {
        let inner = self.inner.read().unwrap();
        unsafe { (*inner.tail).get_prev() }
    }

    pub fn key_is_less_than_or_equal(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();

        if std::ptr::eq(n, inner.head) {
            false
        } else if std::ptr::eq(n, inner.tail) {
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
        if std::ptr::eq(n, inner.head) {
            true
        } else if std::ptr::eq(n, inner.tail) {
            false
        } else {
            let node_key = unsafe { (*n).get_key() };
            !matches!(inner.comparator.compare(key, node_key), cmp::Ordering::Less)
        }
    }

    pub fn key_is_less_than(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if std::ptr::eq(n, inner.head) {
            false
        } else if std::ptr::eq(n, inner.tail) {
            true
        } else {
            let node_key = unsafe { (*n).get_key() };
            matches!(inner.comparator.compare(key, node_key), cmp::Ordering::Less)
        }
    }

    pub fn key_is_greater_than(&self, key: &[u8], n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        if std::ptr::eq(n, inner.head) {
            true
        } else if std::ptr::eq(n, inner.tail) {
            false
        } else {
            let node_key = unsafe { (*n).get_key() };
            matches!(
                inner.comparator.compare(key, node_key),
                cmp::Ordering::Greater
            )
        }
    }

    pub fn is_head(&self, n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        std::ptr::eq(n, inner.head)
    }

    pub fn is_tail(&self, n: *const Node) -> bool {
        let inner = self.inner.read().unwrap();
        std::ptr::eq(n, inner.tail)
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
