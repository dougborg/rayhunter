//! Real WebSocket -> separate GPL executable -> versioned IPC, using synthetic bytes only.
use futures::SinkExt;
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

// Tungstenite fixes this callback error type to an HTTP Response.
#[allow(clippy::result_large_err)]
#[tokio::test]
async fn real_helper_subprotocol_raw_before_decode_and_clean_peer_close() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/raw-diag", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_hdr_async(stream, |req: &Request, mut response: Response| {
            assert_eq!(req.uri().path(), "/raw-diag");
            assert_eq!(
                req.headers()["sec-websocket-protocol"],
                "rayhunter.raw-diag.v1"
            );
            assert!(req.headers().get("origin").is_none());
            response.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                "rayhunter.raw-diag.v1".parse().unwrap(),
            );
            Ok(response)
        })
        .await
        .unwrap();
        ws.send(Message::Text(json!({"type":"hello","version":1,"stream_epoch":"fixture","next_sequence":10,"replay":false,"format":"u32le_json_length+json+exact_device_read"}).to_string().into())).await.unwrap();
        // Unknown container is still retained losslessly with a decode-error event.
        let raw = [32u8, 0, 0, 0, 255, 255, 255, 255];
        let meta=serde_json::to_vec(&json!({"sequence":10,"recording_id":null,"read_monotonic_ns":42,"read_system_unix_ns":null,"raw_length":raw.len()})).unwrap();
        let mut frame = (meta.len() as u32).to_le_bytes().to_vec();
        frame.extend(meta);
        frame.extend(raw);
        ws.send(Message::Binary(frame.into())).await.unwrap();
        ws.send(Message::Text(
            json!({"type":"gap","from_sequence":11,"to_sequence_exclusive":14})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        ws.close(None).await.unwrap();
    });
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_rayhunter-airhound-source"));
    command.args(["--endpoint", &endpoint]).kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(8), command.output())
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let records: Vec<Value> = String::from_utf8(result.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        records
            .iter()
            .map(|v| v["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["hello", "raw", "decode_error", "gap"]
    );
    assert!(records.iter().all(|v| v["ipc_version"] == 1));
    assert_eq!(records[1]["hex"], "20000000ffffffff");
    assert_eq!(records[1]["frame"]["sequence"], 10);
    assert_eq!(records[3]["to_sequence_exclusive"], 14);
}
