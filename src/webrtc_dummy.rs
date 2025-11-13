use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::io::Error;

use bytes::{Bytes, BytesMut};

use crate::{
    protobuf::Message,
    sodiumoxide::crypto::secretbox::Key,
    ResultType,
};

pub struct WebRTCStream {
    // mock struct
}

impl WebRTCStream {

    #[inline]
    pub fn set_raw(&mut self) {
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    }

    #[inline]
    pub fn set_send_timeout(&mut self, _ms: u64) {
    }

    #[inline]
    pub fn set_key(&mut self, _key: Key) {
    }

    #[inline]
    pub fn is_secured(&self) -> bool {
        false
    }

    #[inline]
    pub async fn send(&mut self, _msg: &impl Message) -> ResultType<()> {
        Ok(())
    }

    #[inline]
    pub async fn send_raw(&mut self, _msg: Vec<u8>) -> ResultType<()> {
        Ok(())
    }

    pub async fn send_bytes(&mut self, _bytes: Bytes) -> ResultType<()> {
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<BytesMut, Error>> {
        None
    }

    #[inline]
    pub async fn next_timeout(&mut self, _ms: u64) -> Option<Result<BytesMut, Error>> {
        None
    }
}

pub fn is_webrtc_endpoint(_endpoint: &str) -> bool {
    false
}
