use crate::{
    config::Socks5Server, protobuf::Message, sodiumoxide::crypto::secretbox::Key, tcp::Encrypt,
    ResultType,
};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::{
    io::{Error, ErrorKind},
    net::SocketAddr,
    time::Duration,
};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::Role;

pub struct WsFramedStream {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    addr: SocketAddr,
    encrypt: Option<Encrypt>,
    send_timeout: u64,
}

impl WsFramedStream {
    pub async fn new<T: AsRef<str>>(
        url: T,
        local_addr: Option<SocketAddr>,
        proxy_conf: Option<&Socks5Server>,
        ms_timeout: u64,
    ) -> ResultType<Self> {
        let url_str = url.as_ref();

        // to-do: websocket proxy.
        log::info!("{:?}", url_str);

        let request = url_str
            .into_client_request()
            .map_err(|e| Error::new(ErrorKind::Other, e))?;

        let (stream, _) =
            timeout(Duration::from_millis(ms_timeout), connect_async(request)).await??;

        let addr = match stream.get_ref() {
            MaybeTlsStream::Plain(tcp) => tcp.peer_addr()?,
            _ => return Err(Error::new(ErrorKind::Other, "Unsupported stream type").into()),
        };

        let ws = Self {
            stream,
            addr,
            encrypt: None,
            send_timeout: ms_timeout,
        };

        Ok(ws)
    }

    #[inline]
    pub fn set_raw(&mut self) {
        self.encrypt = None;
    }

    #[inline]
    pub async fn from_tcp_stream(stream: TcpStream, addr: SocketAddr) -> ResultType<Self> {
        let ws_stream =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(stream), Role::Client, None)
                .await;

        Ok(Self {
            stream: ws_stream,
            addr,
            encrypt: None,
            send_timeout: 0,
        })
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    #[inline]
    pub fn set_send_timeout(&mut self, ms: u64) {
        self.send_timeout = ms;
    }

    #[inline]
    pub fn set_key(&mut self, key: Key) {
        self.encrypt = Some(Encrypt::new(key));
    }

    #[inline]
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
        self.send_bytes(Bytes::from(msg)).await
    }

    pub async fn send_bytes(&mut self, bytes: Bytes) -> ResultType<()> {
        let msg = WsMessage::Binary(bytes);
        if self.send_timeout > 0 {
            timeout(
                Duration::from_millis(self.send_timeout),
                self.stream.send(msg),
            )
            .await??
        } else {
            self.stream.send(msg).await?
        };
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<BytesMut, Error>> {
        log::debug!("Waiting for next message");

        while let Some(msg) = self.stream.next().await {
            log::debug!("receive msg: {:?}", msg);
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    log::debug!("{}", e);
                    return Some(Err(Error::new(
                        ErrorKind::Other,
                        format!("WebSocket protocol error: {}", e),
                    )));
                }
            };

            log::debug!("Received message type: {}", msg.to_string());
            match msg {
                WsMessage::Binary(data) => {
                    log::info!("Received binary data ({} bytes)", data.len());
                    let mut bytes = BytesMut::from(&data[..]);
                    if let Some(key) = self.encrypt.as_mut() {
                        if let Err(err) = key.dec(&mut bytes) {
                            return Some(Err(err));
                        }
                    }
                    return Some(Ok(bytes));
                }
                WsMessage::Text(text) => {
                    log::debug!("Received text message, converting to binary");
                    let bytes = BytesMut::from(text.as_bytes());
                    return Some(Ok(bytes));
                }
                WsMessage::Close(_) => {
                    log::info!("Received close frame");
                    return None;
                }
                _ => {
                    log::debug!("Unhandled message type: {}", msg.to_string());
                    continue;
                }
            }
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
