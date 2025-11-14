use std::sync::Arc;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::io::{Error, ErrorKind};
use std::time::Duration;
use std::collections::HashMap;

use webrtc::api::APIBuilder;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::ice::mdns::MulticastDnsMode;

use crate::{
    protobuf::Message,
    sodiumoxide::crypto::secretbox::Key,
    ResultType,
};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use tokio::time::timeout;
use tokio::sync::watch;
use tokio::sync::Mutex;

pub struct WebRTCStream {
    pc: Arc<RTCPeerConnection>,
    stream: Arc<Mutex<Arc<RTCDataChannel>>>,
    state_notify: watch::Receiver<bool>,
    send_timeout: u64,
}

/// message size limit for Chromium
const DATA_CHANNEL_BUFFER_SIZE: u16 = u16::MAX;

lazy_static::lazy_static! {
    static ref SESSIONS: Arc::<Mutex<HashMap<String, WebRTCStream>>> = Default::default();
}

impl Clone for WebRTCStream {
    fn clone(&self) -> Self {
        WebRTCStream {
            pc: self.pc.clone(),
            stream: self.stream.clone(),
            state_notify: self.state_notify.clone(),
            send_timeout: self.send_timeout,
        }
    }
}

impl WebRTCStream {

    pub fn get_remote_offer(endpoint: &str) -> ResultType<String> {
        // Ensure the endpoint starts with the "webrtc://" prefix
        if !endpoint.starts_with("webrtc://") {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid WebRTC endpoint format").into());
        }

        // Extract the Base64-encoded SDP part
        let encoded_sdp = &endpoint["webrtc://".len()..];
        // Decode the Base64 string
        let decoded_bytes = BASE64_STANDARD.decode(encoded_sdp).map_err(|_|
            Error::new(ErrorKind::InvalidInput, "Failed to decode Base64 SDP")
        )?;
        Ok(String::from_utf8(decoded_bytes).map_err(|_| {
            Error::new(ErrorKind::InvalidInput, "Failed to convert decoded bytes to UTF-8")
        })?)
    }

    pub fn sdp_to_endpoint(sdp: &str) -> String {
        let encoded_sdp = BASE64_STANDARD.encode(sdp);
        format!("webrtc://{}", encoded_sdp)
    }

    async fn get_key_for_peer(pc: &Arc<RTCPeerConnection>) -> String {
        if let Some(local_desc) = pc.local_description().await {
            if local_desc.sdp_type != webrtc::peer_connection::sdp::sdp_type::RTCSdpType::Offer {
                let Some(remote_desc) = pc.remote_description().await else {
                    return "".into();
                };
                return serde_json::to_string(&remote_desc).unwrap_or_default();
            }
            return serde_json::to_string(&local_desc).unwrap_or_default();
        }
        "".into()
    }

    pub async fn new(
        remote_endpoint: &str,
        ms_timeout: u64,
    ) -> ResultType<Self> {
        log::debug!("New webrtc stream to endpoint: {}", remote_endpoint);
        let remote_offer = if remote_endpoint.is_empty() {
            "".into()
        } else {
            Self::get_remote_offer(remote_endpoint)?
        };

        let mut key = remote_offer.clone();
        let mut lock = SESSIONS.lock().await;
        if let Some(cached_stream) = lock.get(&key) {
            if !key.is_empty() {
                log::debug!("Start webrtc with cached peer");
                return Ok(cached_stream.clone());
            }
        }

        // Create a SettingEngine and enable Detach
        let mut s = SettingEngine::default();
        s.detach_data_channels();
        s.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);

        // Create the API object
        let api = APIBuilder::new()
            .with_setting_engine(s)
            .build();

        // Prepare the configuration
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.cloudflare.com:3478".to_owned()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let (notify_tx, notify_rx) = watch::channel(false);
        let dc_open_notify = notify_tx.clone();
        // Create a new RTCPeerConnection
        let pc = Arc::new(api.new_peer_connection(config).await?);
        let bootstrap_dc = if remote_offer.is_empty() {
            // Create a data channel with label "bootstrap"
            pc.create_data_channel("bootstrap", None).await?
        } else {
            // Wait for the data channel to be created by the remote peer
            // Here we create a dummy data channel to satisfy the type system
            Arc::new(RTCDataChannel::default())
        };
        bootstrap_dc.on_open(Box::new(move || {
            log::debug!("Local data channel bootstrap open.");
            let _ = dc_open_notify.send(true);
            Box::pin(async {})
        }));

        let stream = Arc::new(Mutex::new(bootstrap_dc.clone()));

        // This will notify you when the peer has connected/disconnected
        let on_connection_notify = notify_tx.clone();
        let stream_for_close = stream.clone();
        let pc_for_close = pc.clone();
        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            let stream_for_close2 = stream_for_close.clone();
            let on_connection_notify2 = on_connection_notify.clone();
            let pc_for_close2 = pc_for_close.clone();
            Box::pin(async move {
                log::debug!("Peer connection state : {}", s);
                if s == RTCPeerConnectionState::Disconnected {
                    let _ = on_connection_notify2.send(true);
                    log::debug!("WebRTC session closing due to disconnected");
                    let _ = stream_for_close2.lock().await.close().await;
                    log::debug!("WebRTC session stream closed");
                } else if s == RTCPeerConnectionState::Failed || s == RTCPeerConnectionState::Closed {
                    let mut lock = SESSIONS.lock().await;
                    let key = WebRTCStream::get_key_for_peer(&pc_for_close2).await;
                    log::debug!("WebRTC session removing key from cache: {}", key);
                    lock.remove(&key);
                }
            })
        }));

        // Register data channel creation handling
        let remote_dc_open_notify = notify_tx.clone();
        let stream_for_dc = stream.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let d_label = dc.label().to_owned();
            let notify = remote_dc_open_notify.clone();
            let stream_for_dc_clone = stream_for_dc.clone();
            log::debug!("Remote data channel {} ready", d_label);
            Box::pin(async move {
                let mut stream_lock = stream_for_dc_clone.lock().await;
                *stream_lock = dc.clone();
                drop(stream_lock);
                dc.on_open(Box::new(move || {
                    let _ = notify.send(true);
                    Box::pin(async {})
                }));
            })
        }));

        // process offer/answer
        if remote_offer.is_empty() {
            let sdp = pc.create_offer(None).await?;
            let mut gather_complete = pc.gathering_complete_promise().await;
            pc.set_local_description(sdp.clone()).await?;
            let _ = gather_complete.recv().await;

            key = Self::get_key_for_peer(&pc).await;
            log::debug!("Start webrtc with local: {}", key);
        } else {
            let sdp = serde_json::from_str::<RTCSessionDescription>(&remote_offer)?;
            pc.set_remote_description(sdp).await?;
            let answer = pc.create_answer(None).await?;
            let mut gather_complete = pc.gathering_complete_promise().await;
            pc.set_local_description(answer).await?;
            let _ = gather_complete.recv().await;
            log::debug!("Start webrtc with remote: {}", remote_offer);
        }

        let webrtc_stream = WebRTCStream {
            pc,
            stream,
            state_notify: notify_rx,
            send_timeout: ms_timeout,
        };

        lock.insert(key, webrtc_stream.clone());
        Ok(webrtc_stream)
    }

    #[inline]
    pub async fn get_local_endpoint(&self) -> ResultType<String> {
        if let Some(local_desc) = self.pc.local_description().await {
            let sdp = serde_json::to_string(&local_desc).unwrap_or_default();
            let endpoint = Self::sdp_to_endpoint(&sdp);
            Ok(endpoint)
        } else {
            Err(anyhow::anyhow!("Local description is not set"))
        }
    }

    #[inline]
    pub async fn set_remote_endpoint(&self, endpoint: &str) -> ResultType<()> {
        let offer = Self::get_remote_offer(endpoint)?;
        log::debug!("WebRTC set remote sdp: {}", offer);
        let sdp = serde_json::from_str::<RTCSessionDescription>(&offer)?;
        self.pc.set_remote_description(sdp).await?;
        Ok(())
    }

    #[inline]
    pub fn set_raw(&mut self) {
        // not-supported
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    }

    #[inline]
    pub fn set_send_timeout(&mut self, ms: u64) {
        self.send_timeout = ms;
    }

    #[inline]
    pub fn set_key(&mut self, _key: Key) {
        // not-supported
        // WebRTC uses built-in DTLS encryption for secure communication.
        // DTLS handles key exchange and encryption automatically, so explicit key management is not required.
    }

    #[inline]
    pub fn is_secured(&self) -> bool {
        true
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
    async fn wait_for_connect_result(&mut self) {
        if *self.state_notify.borrow() {
            return;
        }
        let _ = self.state_notify.changed().await;
    }

    pub async fn send_bytes(&mut self, bytes: Bytes) -> ResultType<()> {
        self.wait_for_connect_result().await;
        let stream = self.stream.lock().await.clone();
        stream.send(&bytes).await?;
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<BytesMut, Error>> {
        self.wait_for_connect_result().await;
        let stream = self.stream.lock().await.clone();

        // TODO reuse buffer?
        let mut buffer = BytesMut::zeroed(DATA_CHANNEL_BUFFER_SIZE as usize);
        let dc = stream.detach().await.ok()?;
        let n = match dc.read(&mut buffer).await {
            Ok(n) => n,
            Err(err) => {
                self.pc.close().await.ok();
                return Some(Err(Error::new(
                    ErrorKind::Other,
                    format!("data channel read error: {}", err),
                )));
            }
        };
        if n == 0 {
            self.pc.close().await.ok();
            return Some(Err(Error::new(
                ErrorKind::Other,
                "data channel read exited with 0 bytes",
            )));
        }
        buffer.truncate(n);
        Some(Ok(buffer))
    }

    #[inline]
    pub async fn next_timeout(&mut self, ms: u64) -> Option<Result<BytesMut, Error>> {
        match timeout(Duration::from_millis(ms), self.next()).await {
            Ok(res) => res,
            Err(_) => None,
        }
    }
}

pub fn is_webrtc_endpoint(endpoint: &str) -> bool {
    // use sdp base64 json string as endpoint, or prefix webrtc:
    endpoint.starts_with("webrtc://")
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_dc() {
    }
}
