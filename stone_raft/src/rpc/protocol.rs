use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut};

pub trait TInputProtocol {
    fn read_bytes(&mut self) -> Result<Vec<u8>>;

    fn read_byte(&mut self) -> Result<u8>;

    fn read_u64(&mut self) -> Result<u64>;
}

pub trait TOutputProtocol {
    fn write_bytes(&mut self, b: &[u8]) -> Result<()>;

    fn write_byte(&mut self, b: u8) -> Result<()>;

    fn write_u64(&mut self, i: u64) -> Result<()>;
}

pub struct BinaryInputProtocol<T> {
    buf: T,
}

impl<T> BinaryInputProtocol<T> {
    pub fn new(buf: T) -> Self {
        Self { buf }
    }
}

impl<T: Buf> TInputProtocol for BinaryInputProtocol<T> {
    #[inline]
    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        protocol_len_check(&self.buf, 8)?;
        let num_bytes = self.buf.get_u64() as usize;
        let mut output = vec![0; num_bytes];
        protocol_len_check(&self.buf, num_bytes)?;
        self.buf.copy_to_slice(&mut output);

        Ok(output)
    }

    #[inline]
    fn read_byte(&mut self) -> Result<u8> {
        protocol_len_check(&self.buf, 1)?;
        Ok(self.buf.get_u8())
    }

    #[inline]
    fn read_u64(&mut self) -> Result<u64> {
        protocol_len_check(&self.buf, 8)?;
        Ok(self.buf.get_u64())
    }
}

pub struct BinaryOutputProtocol<T> {
    buf: T,
}

impl<T> BinaryOutputProtocol<T> {
    pub fn new(buf: T) -> Self {
        Self { buf }
    }
}

impl<T: BufMut> TOutputProtocol for BinaryOutputProtocol<T> {
    #[inline]
    fn write_bytes(&mut self, b: &[u8]) -> Result<()> {
        self.write_u64(b.len() as u64)?;
        self.buf.put_slice(b);
        Ok(())
    }

    #[inline]
    fn write_byte(&mut self, b: u8) -> Result<()> {
        self.buf.put_u8(b);
        Ok(())
    }

    #[inline]
    fn write_u64(&mut self, i: u64) -> Result<()> {
        self.buf.put_u64(i);
        Ok(())
    }
}

#[inline]
fn protocol_len_check<T>(buf: &T, required_len: usize) -> Result<()>
where
    T: bytes::Buf,
{
    #[cfg(not(feature = "unstable"))]
    if buf.remaining() >= required_len {
        return Ok(());
    }
    #[cfg(feature = "unstable")]
    if std::intrinsics::likely(buf.remaining() >= required_len) {
        return Ok(());
    }
    Err(anyhow!("unexpected data length"))
}
