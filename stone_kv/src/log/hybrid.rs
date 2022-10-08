use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Formatter;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use super::*;

use bytes::Bytes;

pub struct Hybrid<F>
where
    F: Read + Write + Seek,
{
    file: Mutex<F>,
    index: BTreeMap<u64, (u64, u32)>,
    uncommitted: VecDeque<Bytes>,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
    metadata_file: F,
    sync: bool,
}

impl Hybrid<File> {
    pub fn open_from_dir_path(dir: &Path, sync: bool) -> Result<Self> {
        create_dir_all(dir)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-log"))?;

        let metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-metadata"))?;

        Ok(Self {
            index: Self::build_index(&file)?,
            file: Mutex::new(file),
            uncommitted: VecDeque::new(),
            metadata: Self::load_metadata(&metadata_file)?,
            metadata_file,
            sync,
        })
    }

    fn build_index(file: &File) -> Result<BTreeMap<u64, (u64, u32)>> {
        let filesize = file.metadata()?.len();
        let mut bufreader = BufReader::new(file);
        let mut index = BTreeMap::new();
        let mut sizebuf = [0; 4];
        let mut pos = 0;
        let mut i = 1;
        while pos < filesize {
            bufreader.read_exact(&mut sizebuf)?;
            pos += 4;
            let size = u32::from_be_bytes(sizebuf);
            index.insert(i, (pos, size));
            let mut buf = vec![0; size as usize];
            bufreader.read_exact(&mut buf)?;
            pos += size as u64;
            i += 1;
        }
        Ok(index)
    }

    fn load_metadata(file: &File) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        match bincode::deserialize_from(file) {
            Ok(metadata) => Ok(metadata),
            Err(err) => {
                if let bincode::ErrorKind::Io(err) = &*err {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(HashMap::new());
                    }
                }
                Err(err.into())
            }
        }
    }
}

impl LogStore for Hybrid<File> {
    fn append(&mut self, entry: Bytes) -> Result<u64> {
        self.uncommitted.push_back(entry);
        Ok(self.len())
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(anyhow!("Cannot commit non-existant index {}", index));
        }
        if index < self.index.len() as u64 {
            return Err(anyhow!(
                "Cannot commit non-existant index {}",
                self.index.len() as u64
            ));
        }
        if index == self.index.len() as u64 {
            return Ok(());
        }
        let mut file = self.file.lock().unwrap();
        let mut pos = file.seek(SeekFrom::End(0))?;
        let mut bufwriter = BufWriter::new(&mut *file);
        for i in (self.index.len() as u64 + 1)..=index {
            match self.uncommitted.pop_front() {
                Some(entry) => {
                    bufwriter.write_all(&(entry.len() as u32).to_be_bytes())?;
                    pos += 4;
                    self.index.insert(i, (pos, entry.len() as u32));
                    bufwriter.write_all(entry.as_ref())?;
                    pos += entry.len() as u64;
                }
                None => {
                    return Err(anyhow!("Unexpected end of uncommitted entries"));
                }
            }
        }
        bufwriter.flush()?;
        drop(bufwriter);
        if self.sync {
            file.sync_data()?;
        }
        Ok(())
    }

    fn committed(&self) -> u64 {
        self.index.len() as u64
    }

    fn get(&self, index: u64) -> Result<Option<Bytes>> {
        match index {
            0 => Ok(None),
            i if i <= self.index.len() as u64 => {
                let (pos, size) = self
                    .index
                    .get(&i)
                    .copied()
                    .context(format!("Indexed position not found for entry {}", i))?;
                let mut buf = vec![0; size as usize];
                let mut file = self.file.lock().unwrap();
                file.seek(SeekFrom::Start(pos))?;
                file.read_exact(&mut buf)?;
                Ok(Some(Bytes::from(buf)))
            }
            i => Ok(self
                .uncommitted
                .get(i as usize - self.index.len() - 1)
                .cloned()),
        }
    }

    fn len(&self) -> u64 {
        self.index.len() as u64 + self.uncommitted.len() as u64
    }

    fn scan(&self, range: Range) -> Scan {
        let start = match range.start {
            Bound::Included(0) => 1,
            Bound::Included(n) => n,
            Bound::Excluded(n) => n + 1,
            Bound::Unbounded => 1,
        };
        let end = match range.end {
            Bound::Included(n) => n,
            Bound::Excluded(0) => 0,
            Bound::Excluded(n) => n - 1,
            Bound::Unbounded => self.len(),
        };

        let mut scan: Scan = Box::new(std::iter::empty());
        if start > end {
            return scan;
        }

        // Scan committed entries in file
        if let Some((offset, _)) = self.index.get(&start) {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(*offset - 4)).unwrap(); // seek to length prefix
            let mut bufreader = BufReader::new(MutexReader(file)); // FIXME Avoid MutexReader
            scan = Box::new(scan.chain(self.index.range(start..=end).map(
                move |(_, (_, size))| {
                    let mut sizebuf = vec![0; 4];
                    bufreader.read_exact(&mut sizebuf)?;
                    let mut buf = vec![0; *size as usize];
                    bufreader.read_exact(&mut buf)?;
                    Ok(Bytes::from(buf))
                },
            )));
        }

        // Scan uncommitted entries in memory
        if end > self.index.len() as u64 {
            scan = Box::new(
                scan.chain(
                    self.uncommitted
                        .iter()
                        .skip(start as usize - min(start as usize, self.index.len() + 1))
                        .take(end as usize - max(start as usize, self.index.len()) + 1)
                        .cloned()
                        .map(Ok),
                ),
            )
        }
        scan
    }

    fn size(&self) -> u64 {
        self.index
            .iter()
            .next_back()
            .map(|(_, (pos, size))| *pos + *size as u64)
            .unwrap_or(0)
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.index.len() as u64 {
            return Err(anyhow!(
                "Cannot truncate below committed index {}",
                self.index.len() as u64
            ));
        }
        self.uncommitted.truncate(index as usize - self.index.len());
        Ok(self.len())
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key, value);
        self.metadata_file.set_len(0)?;
        self.metadata_file.seek(SeekFrom::Start(0))?;
        bincode::serialize_into(&mut self.metadata_file, &self.metadata)?;
        if self.sync {
            self.metadata_file.sync_data()?;
        }
        Ok(())
    }
}

struct MutexReader<'a>(MutexGuard<'a, File>);

impl<'a> Read for MutexReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}
