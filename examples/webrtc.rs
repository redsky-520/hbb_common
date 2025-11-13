extern crate hbb_common;

use std::io::Write;
use bytes::Bytes;

use clap::{Arg, Command};
use anyhow::Result;
use tokio::time::Duration;

use webrtc::peer_connection::math_rand_alpha;

#[tokio::main]
async fn main() -> Result<()> {
    let app = Command::new("webrtc-stream")
        .about("An example of webrtc stream using hbb_common and webrtc-rs")
        .arg(
            Arg::new("debug")
                .long("debug")
                .short('d')
                .action(clap::ArgAction::SetTrue)
                .help("Prints debug log information"),
        )
        .arg(
            Arg::new("offer")
                .long("offer")
                .short('o')
                .help("set offer from other endpoint"),
        );

    let matches = app.clone().get_matches();

    let debug = matches.contains_id("debug");
    if debug {
        println!("Debug log enabled");
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
            .filter(None, log::LevelFilter::Debug)
            .init();
    }

    let remote_endpoint = if let Some(endpoint) = matches.get_one::<String>("offer") {
        endpoint.to_string()
    } else {
        "".to_string()
    };

    let webrtc_stream = hbb_common::webrtc::WebRTCStream::new(&remote_endpoint, 30000).await?;
    // Print the offer to be sent to the other peer
    webrtc_stream.get_local_endpoint().await;

    if remote_endpoint.is_empty() {
        // Wait for the answer to be pasted
        println!("Wait for the answer to be pasted");
        // readline blocking
        let line = std::io::stdin()
            .lines()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No input received"))??;
        webrtc_stream.set_remote_endpoint(&line).await?;
    }

    let s1 = hbb_common::Stream::WebRTC(webrtc_stream.clone());
    tokio::spawn(async move {
        let _ = read_loop(s1).await;
    });

    let s2 = hbb_common::Stream::WebRTC(webrtc_stream.clone());
    tokio::spawn(async move {
        let _ = write_loop(s2).await;
    });

    println!("Press ctrl-c to stop");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!();
        }
    };

    Ok(())
}

// read_loop shows how to read from the datachannel directly
async fn read_loop(mut stream: hbb_common::Stream) -> Result<()> {
    loop {
        let Some(res) = stream.next().await else {
            println!("Datachannel closed; Exit the read_loop");
            return Ok(());
        };
        println!("Message from DataChannel: {}",
            String::from_utf8(res.unwrap().to_vec())?
        );
    }
}

// write_loop shows how to write to the datachannel directly
async fn write_loop(mut stream: hbb_common::Stream) -> Result<()> {
    let mut result = Result::<()>::Ok(());
    while result.is_ok() {
        let timeout = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(timeout);

        tokio::select! {
            _ = timeout.as_mut() =>{
                let message = math_rand_alpha(15);
                println!("Sending '{message}'");
                result = stream.send_bytes(Bytes::from(message)).await;
            }
        };
    }
    println!("Datachannel write not ok; Exit the write_loop");

    Ok(())
}