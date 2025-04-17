use crate::{
    config::Socks5Server,
    protobuf::Message,
    sodiumoxide::crypto::secretbox::{self, Key, Nonce},
    ResultType,
};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::{
    io::{Error, ErrorKind},
    net::SocketAddr,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tungstenite::protocol::Role;

#[derive(Clone)]
pub struct Encrypt(Key, u64, u64);

pub struct WsFramedStream {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    addr: SocketAddr,
    encrypt: Option<Encrypt>,
    send_timeout: u64,
    read_buf: BytesMut,
}

impl WsFramedStream {
    pub async fn new<T: AsRef<str>>(
        url: T,
        local_addr: Option<SocketAddr>,
        _proxy_conf: Option<&Socks5Server>,
        ms_timeout: u64,
    ) -> ResultType<Self> {
        let (stream, _) = connect_async(url.as_ref()).await?;

        // 获取底层TCP流的peer_addr
        let addr = match stream.get_ref() {
            MaybeTlsStream::Plain(tcp) => tcp.peer_addr()?,
            #[cfg(feature = "native-tls")]
            MaybeTlsStream::NativeTls(tls) => tls.get_ref().peer_addr()?,
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(tls) => tls.get_ref().0.peer_addr()?,
            // 处理其他可能的情况
            _ => return Err(Error::new(ErrorKind::Other, "Unsupported stream type").into()),
        };

        Ok(Self {
            stream,
            addr,
            encrypt: None,
            send_timeout: ms_timeout,
            read_buf: BytesMut::new(),
        })
    }

    pub fn set_raw(&mut self) {
        // WebSocket不需要特殊处理，保持空实现
    }

    pub async fn from_tcp_stream(stream: TcpStream, addr: SocketAddr) -> ResultType<Self> {
        let ws_stream =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(stream), Role::Server, None)
                .await;

        Ok(Self {
            stream: ws_stream,
            addr,
            encrypt: None,
            send_timeout: 0,
            read_buf: BytesMut::new(),
        })
    }

    pub async fn from(stream: TcpStream, addr: SocketAddr) -> Self {
        let ws_stream = WebSocketStream::from_raw_socket(
            MaybeTlsStream::Plain(stream), // 包装为MaybeTlsStream
            Role::Client,
            None,
        )
        .await;

        Self {
            stream: ws_stream,
            addr,
            encrypt: None,
            send_timeout: 0,
            read_buf: BytesMut::new(),
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn set_send_timeout(&mut self, ms: u64) {
        self.send_timeout = ms;
    }

    pub fn set_key(&mut self, key: Key) {
        self.encrypt = Some(Encrypt::new(key));
    }

    pub fn is_secured(&self) -> bool {
        self.encrypt.is_some()
    }

    #[inline]
    pub async fn send(&mut self, msg: &impl Message) -> ResultType<()> {
        self.send_raw(msg.write_to_bytes()?).await
    }

    #[inline]
    pub async fn send_raw(&mut self, msg: Vec<u8>) -> ResultType<()> {
        let mut msg = msg;
        if let Some(key) = self.encrypt.as_mut() {
            msg = key.enc(&msg);
        }
        self.send_bytes(bytes::Bytes::from(msg)).await
    }

    #[inline]
    pub async fn send_bytes(&mut self, bytes: Bytes) -> ResultType<()> {
        // 转换为Vec<u8>时需要处理加密
        let data = if let Some(key) = self.encrypt.as_mut() {
            key.enc(&bytes.to_vec())
        } else {
            bytes.to_vec()
        };

        let msg = WsMessage::Binary(Bytes::from(data));
        if self.send_timeout > 0 {
            let send_future = self.stream.send(msg);
            timeout(Duration::from_millis(self.send_timeout), send_future)
                .await
                .map_err(|_| Error::new(ErrorKind::TimedOut, "Send timeout"))?
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        } else {
            self.stream
                .send(msg)
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        }
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<BytesMut, Error>> {
        loop {
            if let Some((frame, _)) = self.read_buf.split_first() {
                if let Some(decrypted) = self.try_decrypt() {
                    return Some(Ok(decrypted));
                }
            }

            match self.stream.next().await? {
                Ok(WsMessage::Binary(data)) => {
                    self.read_buf.extend_from_slice(&data);
                    if let Some(decrypted) = self.try_decrypt() {
                        return Some(Ok(decrypted));
                    }
                }
                Ok(_) => continue, // 忽略非二进制消息
                Err(e) => return Some(Err(Error::new(ErrorKind::Other, e))),
            }
        }
    }

    fn try_decrypt(&mut self) -> Option<BytesMut> {
        if let Some(key) = self.encrypt.as_mut() {
            if let Ok(()) = key.dec(&mut self.read_buf) {
                let data = self.read_buf.split();
                return Some(data);
            }
        } else {
            let data = self.read_buf.split();
            return Some(data);
        }
        None
    }

    #[inline]
    pub async fn next_timeout(&mut self, ms: u64) -> Option<Result<BytesMut, Error>> {
        match timeout(Duration::from_millis(ms), self.next()).await {
            Ok(res) => res,
            Err(_) => None,
        }
    }
}

impl Encrypt {
    pub fn new(key: Key) -> Self {
        Self(key, 0, 0)
    }

    pub fn dec(&mut self, bytes: &mut BytesMut) -> Result<(), Error> {
        if bytes.len() <= 1 {
            return Ok(());
        }
        self.2 += 1;
        let nonce = get_nonce(self.2);
        match secretbox::open(bytes, &nonce, &self.0) {
            Ok(res) => {
                bytes.clear();
                bytes.put_slice(&res);
                Ok(())
            }
            Err(()) => Err(Error::new(ErrorKind::Other, "decryption error")),
        }
    }

    pub fn enc(&mut self, data: &[u8]) -> Vec<u8> {
        self.1 += 1;
        let nonce = get_nonce(self.1);
        secretbox::seal(data, &nonce, &self.0)
    }
}

fn get_nonce(seqnum: u64) -> Nonce {
    let mut nonce = Nonce([0u8; secretbox::NONCEBYTES]);
    nonce.0[..std::mem::size_of_val(&seqnum)].copy_from_slice(&seqnum.to_le_bytes());
    nonce
}
