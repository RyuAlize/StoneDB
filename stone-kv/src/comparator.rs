use std::cmp::{min, Ordering};

pub trait Comparator: Send + Sync + Clone + Default {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn name(&self) -> &str;

    fn successor(&self, key: &[u8]) -> Vec<u8>;
}

#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator {}

impl Comparator for BytewiseComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    #[inline]
    fn name(&self) -> &str {
        "BytewiseComparator"
    }

    #[inline]
    fn successor(&self, key: &[u8]) -> Vec<u8> {
        for i in 0..key.len() {
            let byte = key[i];
            if byte != 0xff {
                let mut res: Vec<u8> = vec![0; i + 1];
                res[0..=i].copy_from_slice(&key[0..=i]);
                *(res.last_mut().unwrap()) += 1;
                return res;
            }
        }
        key.to_owned()
    }
}
