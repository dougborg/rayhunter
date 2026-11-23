use super::*;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message as ClientMessage, client::IntoClientRequest},
};

fn publisher() -> Publisher {
    Publisher {
        feed: Feed::new(CancellationToken::new()).unwrap(),
        next: 0,
        origin: Instant::now(),
    }
}
fn decode(frame: &[u8]) -> (serde_json::Value, &[u8]) {
    let n = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
    (
        serde_json::from_slice(&frame[4..4 + n]).unwrap(),
        &frame[4 + n..],
    )
}
#[test]
fn exact_bytes_framing_and_recording_context() {
    let mut p = publisher();
    p.feed.set_recording(Some("owned-test-recording"));
    let raw = [0, 255, 0x7e, 0x7d, 1, 2, 3];
    p.publish(&raw);
    let Some(Item::Record(record)) = p.feed.item(0) else {
        panic!("missing record")
    };
    let (meta, bytes) = decode(&record.frame);
    assert_eq!(bytes, raw);
    assert_eq!(meta["sequence"], 0);
    assert_eq!(meta["recording_id"], "owned-test-recording");
    assert_eq!(meta["raw_length"], raw.len());
    assert!(meta["read_monotonic_ns"].is_u64());
    assert!(
        meta["read_system_unix_ns"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .is_ok()
    );
    p.feed.set_recording(None);
    p.publish(&[4]);
    let Some(Item::Record(record)) = p.feed.item(1) else {
        panic!("missing record")
    };
    assert!(decode(&record.frame).0["recording_id"].is_null());
}
#[test]
fn count_and_byte_limits_evict_with_explicit_cursor_gap() {
    let mut p = publisher();
    for _ in 0..100 {
        p.publish(&[0]);
    }
    assert!(matches!(
        p.feed.item(0),
        Some(Item::Gap { from: 0, to: 36 })
    ));
    assert_eq!(p.feed.buffer.lock().unwrap().records.len(), MAX_RECORDS);
    for _ in 0..100 {
        p.publish(&vec![7; MAX_READ]);
    }
    let buffer = p.feed.buffer.lock().unwrap();
    assert!(buffer.bytes <= MAX_BYTES);
    assert!(buffer.records.len() < MAX_RECORDS);
    assert!(buffer.records.back().unwrap().sequence == 199);
}
#[test]
fn oversized_and_contention_drop_do_not_block_or_hide_tail_gaps() {
    let mut p = publisher();
    p.publish(&vec![0; MAX_READ + 1]);
    let feed = p.feed.clone();
    let guard = feed.buffer.lock().unwrap();
    p.publish(&[1, 2, 3]); // must return even while a consumer holds the lock
    drop(guard);
    assert!(matches!(feed.item(0), Some(Item::Gap { from: 0, to: 2 })));
    assert_eq!(feed.status()["oversized_reads"], 1);
    assert_eq!(feed.status()["contended_reads"], 1);
    p.publish(&[4]);
    assert!(matches!(feed.item(2), Some(Item::Record(_))));
}
#[test]
fn independently_created_readers_have_distinct_epochs() {
    assert_ne!(publisher().feed.epoch, publisher().feed.epoch);
}
#[test]
fn bounded_recording_metadata_including_escape_expansion() {
    let mut p = publisher();
    p.feed.set_recording(Some(&"\0".repeat(256)));
    p.publish(&vec![0; MAX_READ]);
    let Some(Item::Record(r)) = p.feed.item(0) else {
        panic!("missing")
    };
    assert!(r.frame.len() <= MAX_FRAME);
    p.feed.set_recording(Some(&"a".repeat(257)));
    p.publish(&[1]);
    let Some(Item::Record(r)) = p.feed.item(1) else {
        panic!("missing")
    };
    assert!(decode(&r.frame).0["recording_id"].is_null());
}
#[tokio::test]
async fn config_default_and_debug_mode_leave_listener_disabled() {
    assert!(crate::config::Config::default().raw_stream_port.is_none());
    let tracker = TaskTracker::new();
    let shutdown = CancellationToken::new();
    assert!(
        start(None, false, &tracker, shutdown.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        start(Some(9), true, &tracker, shutdown.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(start(Some(0), false, &tracker, shutdown).await.is_err());
    assert_eq!(tracker.len(), 0);
}
async fn test_server(p: &Publisher) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/raw-diag", listener.local_addr().unwrap());
    let app = router(p.feed.clone());
    let shutdown = p.feed.shutdown.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    (url, task)
}
async fn connect(
    url: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", PROTOCOL.parse().unwrap());
    let (ws, response) = connect_async(request).await.unwrap();
    assert_eq!(response.headers()["sec-websocket-protocol"], PROTOCOL);
    ws
}
#[tokio::test]
async fn websocket_exact_frame_live_baseline_limits_and_cancellation() {
    let mut p = publisher();
    p.publish(b"before subscription");
    let (url, task) = test_server(&p).await;
    let mut ws = connect(&url).await;
    let hello = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(hello["type"], "hello");
    assert_eq!(hello["next_sequence"], 1);
    assert_eq!(hello["replay"], false);
    p.publish(&[0, 255, 7, 0x7e]);
    let data = timeout(Duration::from_secs(2), async {
        loop {
            if let ClientMessage::Binary(data) = ws.next().await.unwrap().unwrap() {
                break data;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(decode(&data).0["sequence"], 1);
    assert_eq!(decode(&data).1, &[0, 255, 7, 0x7e]);
    let mut second = connect(&url).await;
    let _ = second.next().await.unwrap().unwrap();
    let mut third = url.as_str().into_client_request().unwrap();
    third
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", PROTOCOL.parse().unwrap());
    let error = connect_async(third).await.unwrap_err();
    assert!(matches!(error, tokio_tungstenite::tungstenite::Error::Http(ref r) if r.status()==429));
    p.feed.shutdown.cancel();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while p.feed.clients.available_permits() != MAX_CLIENTS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn websocket_rejects_browser_origin_and_unknown_protocol() {
    let p = publisher();
    let (url, task) = test_server(&p).await;
    let mut request = url.as_str().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("Origin", "https://untrusted.invalid".parse().unwrap());
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", PROTOCOL.parse().unwrap());
    let error = connect_async(request).await.unwrap_err();
    assert!(matches!(error, tokio_tungstenite::tungstenite::Error::Http(ref r) if r.status()==403));
    let error = connect_async(&url).await.unwrap_err();
    assert!(matches!(error, tokio_tungstenite::tungstenite::Error::Http(ref r) if r.status()==400));
    assert_eq!(p.feed.clients.available_permits(), MAX_CLIENTS);
    p.feed.shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn websocket_lag_reports_byte_eviction_and_reader_end() {
    let mut p = publisher();
    let feed = p.feed.clone();
    let (url, task) = test_server(&p).await;
    let mut ws = connect(&url).await;
    let _hello = ws.next().await.unwrap().unwrap();
    // Current-thread runtime: no network consumer runs during this burst.
    for _ in 0..100 {
        p.publish(&vec![42; MAX_READ]);
    }
    assert!(feed.buffer.lock().unwrap().bytes <= MAX_BYTES);
    let first_retained = feed
        .buffer
        .lock()
        .unwrap()
        .records
        .front()
        .unwrap()
        .sequence;
    assert!(first_retained > 36); // byte budget, not just the 64-record limit
    drop(p);
    let mut saw_gap = false;
    let mut saw_end = false;
    let mut expected = first_retained;
    timeout(Duration::from_secs(3), async {
        while let Some(Ok(message)) = ws.next().await {
            match message {
                ClientMessage::Text(text) => {
                    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
                    match event["type"].as_str().unwrap() {
                        "gap" => {
                            assert_eq!(event["from_sequence"], 0);
                            assert_eq!(event["to_sequence_exclusive"], first_retained);
                            saw_gap = true;
                        }
                        "end" => {
                            saw_end = true;
                            assert!(saw_gap);
                            assert_eq!(expected, 100);
                            break;
                        }
                        "status" => {}
                        other => panic!("unexpected {other}"),
                    }
                }
                ClientMessage::Binary(frame) => {
                    assert!(saw_gap);
                    let (meta, raw) = decode(&frame);
                    assert_eq!(meta["sequence"], expected);
                    assert_eq!(raw, vec![42; MAX_READ]);
                    expected += 1;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(expected, 100);
    assert!(saw_end);
    feed.shutdown.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn disconnect_releases_slot_and_reconnect_has_new_live_baseline() {
    let mut p = publisher();
    let (url, task) = test_server(&p).await;
    let mut ws = connect(&url).await;
    let _ = ws.next().await.unwrap().unwrap();
    ws.close(None).await.unwrap();
    drop(ws);
    timeout(Duration::from_secs(2), async {
        while p.feed.clients.available_permits() != MAX_CLIENTS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    p.publish(b"while disconnected");
    let mut ws = connect(&url).await;
    let hello = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let hello: serde_json::Value = serde_json::from_str(&hello).unwrap();
    assert_eq!(hello["next_sequence"], 1);
    // Idle cancellation must close the upgraded connection as well as listener.
    p.feed.shutdown.cancel();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if !matches!(ws.next().await, Some(Ok(ClientMessage::Text(_)))) {
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stalled_sink_has_a_send_deadline() {
    struct Stalled;
    impl futures::Sink<Message> for Stalled {
        type Error = ();
        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Pending
        }
        fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), ()> {
            unreachable!()
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Pending
        }
        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ()>> {
            std::task::Poll::Pending
        }
    }
    assert!(
        timeout(
            SEND_TIMEOUT + Duration::from_secs(1),
            send(&mut Stalled, Message::Text("test".into()))
        )
        .await
        .unwrap()
        .is_err()
    );
}

#[tokio::test]
async fn busy_stream_cancellation_releases_clients() {
    let mut p = publisher();
    let (url, task) = test_server(&p).await;
    let mut ws = connect(&url).await;
    let _ = ws.next().await.unwrap().unwrap();
    for _ in 0..100 {
        p.publish(&vec![42; MAX_READ]);
    }
    p.feed.shutdown.cancel();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while p.feed.clients.available_permits() != MAX_CLIENTS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
