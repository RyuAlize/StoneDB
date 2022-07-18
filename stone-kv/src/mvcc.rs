use std::borrow::Cow;
use std::collections::HashSet;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_derive::{Deserialize, Serialize};
use serde::{Serialize, Deserialize};

use super::{Scan, Store, Range};


pub struct MVCC {
    stroe: Arc<RwLock<Box<dyn Store>>>
}

impl Clone for MVCC {
    fn clone(&self) -> Self {
        Self {stroe: self.stroe.clone()}
    }
}

impl MVCC {
    pub fn new(store: Box<dyn Store>) -> Self {
        Self{stroe: Arc::new(RwLock::new(store))}
    }
}

/// Serializes MVCC metadata.
fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

/// Deserializes MVCC metadata.
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}


pub struct Transaction {
    store: Arc<RwLock<Box<dyn Store>>>,
    id: u64,
    mode: Mode,
    snapshot: Snapshot,
}


#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
    ReadWrite,
    ReadOnly,
    Snapshot{version: u64}
}

impl Mode {
    pub fn mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::Snapshot { .. } => false,
        }
    }

    pub fn satisfies(&self, other: &Mode) -> bool {
        match (self, other) {
            (Mode::ReadWrite, Mode::ReadOnly) => true,
            (Mode::Snapshot { .. }, Mode::ReadOnly) => true,
            (_, _) if self == other => true,
            (_, _) => false,
        }
    }
}

#[derive(Clone)]
pub struct Snapshot {
    version: u64,
    invisible: HashSet<u64>,
}

impl Snapshot {
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut snapshot = Self { version, invisible: HashSet::new() };
        let mut scan = session.scan(
            Range::from(Key::TxnActive(0).encode()..Key::TxnActive(version).encode()));
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(key)? {
                Key::TxnActive(id) => snapshot.invisible.insert(id),
                k => return Err(anyhow!(format!("Expected TxnActive, got {:?}", k))),
            };
        }
        std::mem::drop(scan);
        session.set(Key::TxnSnapshot(version).encode(), serialize(&snapshot.invisible)?.into())?;
        Ok(snapshot)
    }

    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(ref v) => Ok(Self { version, invisible: deserialize(v)? }),
            None => Err(anyhow!(format!("Snapshot not found for version {}", version))),
        }
    }

    fn is_visible(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

#[derive(Debug)]
enum Key<'a> {
    TxnNext,
    TxnActive(u64),
    TxnSnapshot(u64),
    TxnUpdate(u64, Cow<'a, [u8]>),
    Record(Cow<'a, [u8]>, u64),
    Metadata(Cow<'a, [u8]>),
}

impl<'a> Key<'a> {
    fn encode(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            Self::TxnNext => {
                bytes.put_u8(0x01)
            },
            Self::TxnActive(id) => {
                bytes.put_u8(0x02);
                bytes.put_u64(id);
            }
            Self::TxnSnapshot(version) => {
                bytes.put_u8(0x03);
                bytes.put_u64(version);
            },
            Self::TxnUpdate(id, key) => {
                bytes.put_u8(0x04);
                bytes.put_u64(id);
                bytes.put_slice(&*key);
            },
            Self::Metadata(key) => {
                bytes.put_u8(0x05);
                bytes.put_slice(&*key);
            },
            Self::Record(key, version) => {
                bytes.put_u8(0xff);
                bytes.put_slice(&*key);
                bytes.put_u64(version);
            }
        }
        bytes.into()
    }

    fn decode(mut bytes: Bytes) -> Result<Self> {
        let key = match bytes.get_u8() {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(bytes.get_u64()),
            0x03 => Self::TxnSnapshot(bytes.get_u64()),
            0x04 => {
                let id = bytes.get_u64();
                let mut key = vec![0; bytes.remaining()];
                bytes.copy_to_slice(&mut key[..]);
                Self::TxnUpdate(id, Cow::from(key))
            }
            0x05 => {
                let mut key = vec![0; bytes.remaining()];
                bytes.copy_to_slice(&mut key[..]);
                Self::Metadata(Cow::from(key))
            },
            0xff => {
                let mut key = vec![0; bytes.remaining() - 8];
                bytes.copy_to_slice(&mut key[..]);
                let version = bytes.get_u64();
                Self::Record(Cow::from(key), version)
            }
            _ => unreachable!()
        };
        if bytes.remaining() > 0 {
            return Err(anyhow!("Unexpected data remaining at end of key"))
        }
        Ok(key)
    }
}


pub struct KeyScan {
    scan: Peekable<Scan>
}

impl KeyScan {
    fn new(mut scan: Scan, snapshot: Snapshot) -> Self {

        Self { scan: scan.peekable()}
    }
}