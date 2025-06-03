use crate::tcp::{DynTcpStream, FramedStream};
use kcp_sys::{
    endpoint::*,
    packet_def::{Bytes, BytesMut, KcpPacket},
    stream,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::UdpSocket, sync::mpsc};

pub struct KcpStream {
    pub endpoint: Arc<KcpEndpoint>,
    pub stream: FramedStream,
}

impl KcpStream {
    fn create_framed(stream: stream::KcpStream, local_addr: Option<SocketAddr>) -> FramedStream {
        FramedStream(
            tokio_util::codec::Framed::new(
                DynTcpStream(Box::new(stream)),
                crate::bytes_codec::BytesCodec::new(),
            ),
            local_addr.unwrap_or(crate::config::Config::get_any_listen_addr(true)),
            None,
            0,
        )
    }

    pub async fn accept(
        udp_socket: Arc<UdpSocket>,
        from_addr: SocketAddr,
    ) -> crate::ResultType<Self> {
        let mut endpoint = KcpEndpoint::new();
        endpoint.run().await;

        let (input, output) = (endpoint.input_sender(), endpoint.output_receiver().unwrap());
        udp_socket.connect(&[from_addr][..]).await?;
        Self::kcp_io(udp_socket.clone(), input, output).await;

        let conn_id = endpoint.accept().await?;
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok(Self {
                endpoint: Arc::new(endpoint),
                stream: Self::create_framed(stream, udp_socket.local_addr().ok()),
            })
        } else {
            Err(anyhow::anyhow!("Failed to create KcpStream"))
        }
    }

    pub async fn connect(
        udp_socket: Arc<UdpSocket>,
        to_addr: SocketAddr,
        timeout: std::time::Duration,
    ) -> crate::ResultType<Self> {
        let mut endpoint = KcpEndpoint::new();
        endpoint.run().await;

        let (input, output) = (endpoint.input_sender(), endpoint.output_receiver().unwrap());
        udp_socket.connect(&[to_addr][..]).await?;
        Self::kcp_io(udp_socket.clone(), input, output).await;

        let conn_id = endpoint.connect(timeout, 0, 0, Bytes::new()).await.unwrap();
        if let Some(stream) = stream::KcpStream::new(&endpoint, conn_id) {
            Ok(Self {
                endpoint: Arc::new(endpoint),
                stream: Self::create_framed(stream, udp_socket.local_addr().ok()),
            })
        } else {
            Err(anyhow::anyhow!("Failed to create KcpStream"))
        }
    }

    async fn kcp_io(
        udp_socket: Arc<UdpSocket>,
        input: mpsc::Sender<KcpPacket>,
        mut output: mpsc::Receiver<KcpPacket>,
    ) {
        let udp = udp_socket.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(data) = output.recv() => {
                        if let Err(e) = udp.send(&data.inner()).await {
                            // Break on fatal errors, but ignore WouldBlock or Interrupted
                            if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::Interrupted {
                                log::error!("kcp send error: {:?}", e);
                                break;
                            }
                        }
                    }
                    else => {
                        log::debug!("kcp endpoint output closed");
                        break;
                    }
                }
            }
        });

        let udp = udp_socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 10240];
            loop {
                tokio::select! {
                    result = udp.recv_from(&mut buf) => {
                        match result {
                            Ok((size, _)) => {
                                input
                                    .send(BytesMut::from(&buf[..size]).into())
                                    .await.ok();
                            }
                            Err(e) => {
                                // Break on fatal errors, but ignore WouldBlock or Interrupted
                                if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::Interrupted {
                                    log::error!("kcp recv_from error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    }
                    else => {
                        log::debug!("kcp endpoint input closed");
                        break;
                    }
                }
            }
        });
    }
}
