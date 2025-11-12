use std::io::Write;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use clap::{Arg, Command};
use anyhow::Result;
use tokio::time::Duration;

use webrtc::api::APIBuilder;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::math_rand_alpha;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use webrtc_signal::{self as signal};

// example from https://github.com/webrtc-rs/webrtc/tree/master/examples/examples/data-channels
#[tokio::main]
async fn main() -> Result<()> {
    let mut app = Command::new("data-channels")
        .version("0.1.0")
        .author("Rain Liu <yliu@webrtc.rs>")
        .about("An example of Data-Channels.")
        .arg(
            Arg::new("FULLHELP")
                .help("Prints more detailed help information")
                .long("fullhelp"),
        )
        .arg(
            Arg::new("debug")
                .long("debug")
                .short('d')
                .help("Prints debug log information"),
        );

    let matches = app.clone().get_matches();

    if matches.contains_id("FULLHELP") {
        app.print_long_help().unwrap();
        std::process::exit(0);
    }

    let debug = matches.contains_id("debug");
    if debug {
        env_logger::Builder::new()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} [{}] {} - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    chrono::Local::now().format("%H:%M:%S.%6f"),
                    record.args()
                )
            })
            .filter(None, log::LevelFilter::Trace)
            .init();
    }

    // Everything below is the WebRTC-rs API! Thanks for using it ❤️.
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
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Create a new RTCPeerConnection
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);

    let bootstrap = peer_connection.create_data_channel("bootstrap", None).await?;
    let bootstrap_clone = Arc::clone(&bootstrap);
    bootstrap.on_open(Box::new(move || {
        println!("Data channel bootstrap open.");
        Box::pin(async move {
            let _raw = match bootstrap_clone.detach().await {
                Ok(raw) => raw,
                Err(err) => {
                    println!("data channel detach got err: {err}");
                    return;
                }
            };
        })
    }));

    // Set the handler for Peer connection state
    // This will notify you when the peer has connected/disconnected
    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        println!("Peer Connection State has changed: {s}");

        if s == RTCPeerConnectionState::Failed {
            // Wait until PeerConnection has had no network activity for 30 seconds or another failure.
            // It may be reconnected using an ICE Restart.
            // Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
            // Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
            println!("Peer Connection has gone to failed exiting");
            let _ = done_tx.try_send(());
        }

        Box::pin(async {})
    }));

    
    // Register data channel creation handling
    peer_connection.on_data_channel(Box::new(move |d: Arc<RTCDataChannel>| {
        let d_label = d.label().to_owned();
        let d_id = d.id();
        println!("New DataChannel {d_label} {d_id}");

        // Register channel opening handling
        Box::pin(async move {
            let d2 = Arc::clone(&d);
            let d3 = Arc::clone(&d);
            let d_label2 = d_label.clone();
            let d_id2 = d_id;
            d.on_open(Box::new(move || {
                println!("Data channel '{d_label2}'-'{d_id2}' open.");

                Box::pin(async move {
                    tokio::spawn(async move {
                        let _ = read_loop(d2).await;
                    });

                    // Handle writing to the data channel
                    tokio::spawn(async move {
                        let _ = write_loop(d3).await;
                    });
                })
            }));
        })
    }));

    // Wait for the offer to be pasted
    println!("Wait for the offer to be pasted");
    let line = signal::must_read_stdin()?;
    let desc_data = signal::decode(line.as_str())?;
    let offer = serde_json::from_str::<RTCSessionDescription>(&desc_data)?;

    // Set the remote SessionDescription
    peer_connection.set_remote_description(offer).await?;

    // Create an answer
    let answer = peer_connection.create_answer(None).await?;

    // Create channel that is blocked until ICE Gathering is complete
    let mut gather_complete = peer_connection.gathering_complete_promise().await;

    // Sets the LocalDescription, and starts our UDP listeners
    peer_connection.set_local_description(answer).await?;

    // Block until ICE Gathering is complete, disabling trickle ICE
    // we do this because we only can exchange one signaling message
    // in a production application you should exchange ICE Candidates via OnICECandidate
    let _ = gather_complete.recv().await;

    // Output the answer in base64 so we can paste it in browser
    if let Some(local_desc) = peer_connection.local_description().await {
        let json_str = serde_json::to_string(&local_desc)?;
        println!("{json_str}");
        let b64 = signal::encode(&json_str);
        println!("--------------------- Copy the below base64 to browser --------------------");
        println!("{b64}");
    } else {
        println!("generate local_description failed!");
    }

    println!("Press ctrl-c to stop");
    tokio::select! {
        _ = done_rx.recv() => {
            println!("received done signal!");
        }
        _ = tokio::signal::ctrl_c() => {
            println!();
        }
    };

    peer_connection.close().await?;

    Ok(())
}

// read_loop shows how to read from the datachannel directly
async fn read_loop(dc: Arc<RTCDataChannel>) -> Result<()> {
    let mut buffer = BytesMut::zeroed(4096);
    loop {
        let d = dc.detach().await?;
        println!("RTCDatachannel detach ok");
        let n = match d.read(&mut buffer).await {
            Ok(n) => n,
            Err(err) => {
                println!("Datachannel closed; Exit the read_loop: {err}");
                return Ok(());
            }
        };

        if n == 0 {
            println!("Datachannel read 0 byte; Exit the read_loop");
            return Ok(());
        }
        println!(
            "Message from DataChannel: {}",
            String::from_utf8(buffer[..n].to_vec())?
        );
    }
}

// write_loop shows how to write to the datachannel directly
async fn write_loop(d: Arc<RTCDataChannel>) -> Result<()> {
    let mut result = Result::<usize>::Ok(0);
    while result.is_ok() {
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        tokio::select! {
            _ = timeout.as_mut() =>{
                let message = math_rand_alpha(15);
                println!("Sending '{message}'");
                result = d.send(&Bytes::from(message)).await.map_err(Into::into);
            }
        };
    }
    println!("Datachannel write not ok; Exit the write_loop");

    Ok(())
}