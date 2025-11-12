use std::sync::{Arc};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::io::{Error, ErrorKind};
use std::time::Duration;
use std::collections::HashMap;

use webrtc::api::APIBuilder;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::{
    protobuf::Message,
    sodiumoxide::crypto::secretbox::Key,
    ResultType,
};
use bytes::{Bytes, BytesMut};
use tokio::{time::timeout};
use tokio::sync::Notify;
use tokio::sync::Mutex;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

pub struct WebRTCStream {
    pc: Arc<RTCPeerConnection>,
    stream: Arc<RTCDataChannel>,
    notify: Arc<Notify>,
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
            notify: self.notify.clone(),
            send_timeout: self.send_timeout,
        }
    }
}

impl WebRTCStream {

    pub fn get_remote_offer(endpoint: &str) -> Option<String> {
        // Ensure the endpoint starts with the "webrtc://" prefix
        if !endpoint.starts_with("webrtc://") {
            return None;
        }

        // Extract the Base64-encoded SDP part
        let encoded_sdp = &endpoint["webrtc://".len()..];

        // Decode the Base64 string
        let decoded_bytes = BASE64_STANDARD.decode(encoded_sdp).ok()?;
        let decoded_sdp = String::from_utf8(decoded_bytes).ok()?;

        Some(decoded_sdp)
    }

    pub async fn new<T: AsRef<str>>(
        webrtc_endpoint: T,
        ms_timeout: u64,
    ) -> ResultType<Self> {
        log::debug!("Start webrtc with endpoint: {}", webrtc_endpoint.as_ref());
        let remote_offer: String = match Self::get_remote_offer(webrtc_endpoint.as_ref()) {
            Some(offer) => offer,
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Invalid WebRTC endpoint format",
                ).into());
            }
        };

        let key = remote_offer.to_string();
        let mut lock = SESSIONS.lock().await;
        let contains = lock.contains_key(&key);
        if contains {
            log::debug!("Start webrtc with cached peer");
            return Ok(lock.get(&key).unwrap().clone());
        }

        log::debug!("Start webrtc with offer: {}", remote_offer);
        // Create a SettingEngine and enable Detach
        let mut s = SettingEngine::default();
        s.detach_data_channels();

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

        let notify = Arc::new(Notify::new());
        let notify_tx = notify.clone();
        // Create a new RTCPeerConnection
        let peer_connection = Arc::new(api.new_peer_connection(config).await?);
        let bootstrap = peer_connection.create_data_channel("bootstrap", None).await?;
        bootstrap.on_open(Box::new(move || {
            log::debug!("Data channel bootstrap open.");
            notify_tx.notify_waiters();
            Box::pin(async {})
        }));

        // This will notify you when the peer has connected/disconnected
        let notify_tx2 = notify.clone();
        peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            log::debug!("Peer Connection State has changed: {}", s);
            if s == RTCPeerConnectionState::Disconnected {
                notify_tx2.notify_waiters();
            }

            // TODO clear SESSIONS entry?
            Box::pin(async {})
        }));

        let offer = serde_json::from_str::<RTCSessionDescription>(&remote_offer)?;
        // Set the remote SessionDescription
        peer_connection.set_remote_description(offer).await?;
        // Create an answer
        let answer = peer_connection.create_answer(None).await?;
        // Create channel that is blocked until ICE Gathering is complete
        let mut gather_complete = peer_connection.gathering_complete_promise().await;
        // Sets the LocalDescription, and starts our UDP listeners
        peer_connection.set_local_description(answer).await?;
        let _ = gather_complete.recv().await;

        let ds = WebRTCStream {
            pc: peer_connection,
            stream: bootstrap,
            notify: notify,
            send_timeout: ms_timeout,
        };

        // log the answer
        match ds.get_local_endpoint().await {
            Some(local_endpoint) => log::debug!("WebRTC local endpoint: {}", local_endpoint),
            None => log::debug!("WebRTC local endpoint: <none>"),
        }

        lock.insert(key, ds.clone());
        Ok(ds)
    }

    #[inline]
    pub async fn get_local_endpoint(&self) -> Option<String> {
        if let Some(local_desc) = self.pc.local_description().await {
            let sdp = serde_json::to_string(&local_desc).ok()?;
            Some(format!("webrtc://{}", BASE64_STANDARD.encode(sdp)))
        } else {
            None
        }
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

    pub async fn send_bytes(&mut self, bytes: Bytes) -> ResultType<()> {
        // wait for connected or disconnected
        self.notify.notified().await;
        self.stream.send(&bytes).await?;
        Ok(())
    }

    #[inline]
    pub async fn next(&mut self) -> Option<Result<BytesMut, Error>> {
        // wait for connected or disconnected
        self.notify.notified().await;
        if self.stream.ready_state() != RTCDataChannelState::Open {
            return Some(Err(Error::new(
                ErrorKind::Other,
                "data channel is closed",
            )));
        }

        // TODO reuse buffer?
        let mut buffer = BytesMut::zeroed(DATA_CHANNEL_BUFFER_SIZE as usize);
        let dc = self.stream.detach().await.ok()?;
        let n = match dc.read(&mut buffer).await {
            Ok(n) => n,
            Err(err) => {
                return Some(Err(Error::new(
                    ErrorKind::Other,
                    format!("data channel read error: {}", err),
                )));
            }
        };
        if n == 0 {
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
