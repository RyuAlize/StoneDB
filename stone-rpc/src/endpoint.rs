use super::error::DecodeError;

use tokio;
use tokio::codec::{Decoder, Framed};
use tokio::io::{AsyncRead, AsyncWrite};

pub trait Server {}
