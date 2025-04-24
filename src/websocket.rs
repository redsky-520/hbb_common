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
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::Role;

#[derive(Clone)]
pub struct Encrypt(Key, u64, u64);

pub struct WsFramedStream {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    addr: SocketAddr,
    encrypt: Option<Encrypt>,
    send_timeout: u64,
    // read_buf: BytesMut,
}

impl WsFramedStream {
    pub async fn new<T: AsRef<str>>(
        url: T,
        local_addr: Option<SocketAddr>,
        proxy_conf: Option<&Socks5Server>,
        ms_timeout: u64,
    ) -> ResultType<Self> {
        let url_str = url.as_ref();

        // if let Some(proxy_conf) = proxy_conf {
        //     // use proxy connect
        //     let url_obj = url::Url::parse(url_str)?;
        //     let host = url_obj
        //         .host_str()
        //         .ok_or_else(|| Error::new(ErrorKind::Other, "Invalid URL: no host"))?;

        //     let port = url_obj
        //         .port()
        //         .unwrap_or(if url_obj.scheme() == "wss" { 443 } else { 80 });

        //     let socket =
        //         tokio_socks::tcp::Socks5Stream::connect(proxy_conf.proxy.as_str(), (host, port))
        //             .await?;

        //     let tcp_stream = socket.into_inner();
        //     let maybe_tls_stream = MaybeTlsStream::Plain(tcp_stream);
        //     let ws_stream =
        //         WebSocketStream::from_raw_socket(maybe_tls_stream, Role::Client, None).await;

        //     let addr = match ws_stream.get_ref() {
        //         MaybeTlsStream::Plain(tcp) => tcp.peer_addr()?,
        //         _ => return Err(Error::new(ErrorKind::Other, "Unsupported stream type").into()),
        //     };

        //     let ws = Self {
        //         stream: ws_stream,
        //         addr,
        //         encrypt: None,
        //         send_timeout: ms_timeout,
        //     };

        //     Ok(ws)
        // } else {
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
        // }
    }

    pub fn set_raw(&mut self) {}

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
        self.send_bytes(Bytes::from(msg)).await
    }

    #[inline]
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
                        log::debug!("Decrypting data with seq: {}", key.2);
                        match key.dec(&mut bytes) {
                            Ok(_) => {
                                log::debug!("Decryption successful");
                                return Some(Ok(bytes));
                            }
                            Err(e) => {
                                log::error!("Decryption failed: {}", e);
                                return Some(Err(e));
                            }
                        }
                    }
                    log::error!("not encrypt set.");
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
