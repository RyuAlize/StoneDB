use super::error::DecodeError;
use super::message::Message;
use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub struct Codec;

impl Decoder for Codec {
    type Item = Message;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let res: Result<Option<Self::Item>, Self::Error>;
        let position = {
            let mut buf = io::Cursor::new(&src);
            loop {
                match Message::decode(&mut buf) {
                    Ok(message) => {
                        res = Ok(Some(message));
                        break;
                    }
                    Err(err) => match err {
                        DecodeError::Truncated => return Ok(None),
                        DecodeError::Invalid => continue,
                        DecodeError::UnknownIo(io_err) => {
                            res = Err(io_err);
                            break;
                        }
                    },
                }
            }
            buf.position() as usize
        };
        let _ = src.split_to(position);
        res
    }
}

impl Encoder<Message> for Codec {
    type Error = io::Error;

    fn encode(&mut self, msg: Message, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = msg.pack()?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }
}
