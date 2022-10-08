use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::iter::Peekable;
use std::ops::{Bound, RangeBounds};
use std::path::Iter;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::{anyhow, Result};
use serde_derive::{Deserialize, Serialize};
use serde::{Serialize, Deserialize, Serializer};

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

impl Transaction {
    /// Begins a new transaction in the given mode.
    fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write().unwrap();

        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        session.set(Key::TxnNext.encode().to_owned(), serialize(&(id + 1))?.into())?;
        session.set(Key::TxnActive(id).encode().to_owned(), serialize(&mode)?.into())?;

        // We always take a new snapshot, even for snapshot transactions, because all transactions
        // increment the transaction ID and we need to properly record currently active transactions
        // for any future snapshot transactions looking at this one.
        let mut snapshot = Snapshot::take(&mut session, id)?;
        std::mem::drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read().unwrap(), *version)?
        }

        Ok(Self { store, id, mode, snapshot })
    }

    /// Resumes an active transaction with the given ID. Errors if the transaction is not active.
    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read().unwrap();
        let mode = match session.get(&Key::TxnActive(id).encode())? {
            Some(v) => deserialize(&v)?,
            None => return Err(anyhow!(format!("No active transaction {}", id))),
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };
        std::mem::drop(session);
        Ok(Self { store, id, mode, snapshot })
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        let mut session = self.store.write().unwrap();
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        let mut session = self.store.write().unwrap();
        if self.mode.mutable() {
            let mut rollback = Vec::new();
            let mut scan = session.scan(Range::from(
                Key::TxnUpdate(self.id, vec![].into()).encode()
                    ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match Key::decode(key.clone())? {
                    Key::TxnUpdate(_, updated_key) => rollback.push(updated_key.into_owned()),
                    k => return Err(anyhow!(format!("Expected TxnUpdate, got {:?}", k))),
                };
                rollback.push(key.to_vec());
            }
            std::mem::drop(scan);
            for key in rollback.into_iter() {
                session.delete(&key.into())?;
            }
        }
        session.delete(&Key::TxnActive(self.id).encode())
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: &Bytes) -> Result<()> {
        self.write(key, None)
    }

    /// Fetches a key.
    pub fn get(&self, key: &Bytes) -> Result<Option<Vec<u8>>> {
        let session = self.store.read().unwrap();
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.to_vec().into(), 0).encode()
                    ..=Key::Record(key.to_vec().into(), self.id).encode(),
            ));
        while let Some((k, v)) = scan.next().transpose()? {
            match Key::decode(k)? {
                Key::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&v);
                    }
                }
                k => return Err(anyhow!("Expected Txn::Record, got {:?}", k)),
            };
        }
        Ok(None)
    }

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.to_vec().into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.to_vec().into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.to_vec().into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.to_vec().into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read().unwrap().scan(Range::from((start, end)));
        Ok(Box::new(KeyScan::new(scan, self.snapshot.clone())))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::Scan> {
        if prefix.is_empty() {
            return Err(anyhow!("Scan prefix cannot be empty"));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // If all 0xff we could in principle use Range::Unbounded, but it won't happen
                0xff if i == 0 => return Err(anyhow!("Invalid prefix scan range")),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(Bytes::from(start)..Bytes::from(end))
    }

    /// Sets a key.
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value.to_vec()))
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(anyhow!("Read Only"));
        }
        let mut session = self.store.write().unwrap();

        // Check if the key is dirty, i.e. if it has any uncommitted changes, by scanning for any
        // versions that aren't visible to us.
        let min = self.snapshot.invisible.iter().min().cloned().unwrap_or(self.id + 1);
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.to_vec().into(), min).encode()
                    ..=Key::Record(key.to_vec().into(), std::u64::MAX).encode(),
            ));
        while let Some((k, _)) = scan.next().transpose()? {
            match Key::decode(k)? {
                Key::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        return Err(anyhow!("Serialize error"));
                    }
                }
                k => return Err(anyhow!(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        std::mem::drop(scan);

        // Write the key and its update record.
        let key = Key::Record(key.to_vec().into(), self.id).encode();
        let update = Key::TxnUpdate(self.id, key.to_vec().into()).encode();
        session.set(update, Bytes::new())?;
        session.set(key, serialize(&value)?.into())
    }
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

impl Iterator for KeyScan {
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}