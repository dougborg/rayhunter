//! Opt-in raw device-read feed. Capture never waits for a network consumer.
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use futures::{SinkExt, StreamExt, stream::SplitSink};
use serde_json::json;
use std::{
    collections::VecDeque,
    io::{self, Read},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore},
    time::{interval, timeout},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const PROTOCOL: &str = "rayhunter.raw-diag.v1";
const MAX_READ: usize = 256 * 1024;
const MAX_FRAME: usize = MAX_READ + 2048;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_RECORDS: usize = 64;
const MAX_CLIENTS: usize = 2;
const SEND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Record {
    sequence: u64,
    frame: Bytes,
}
#[derive(Default)]
struct Buffer {
    records: VecDeque<Record>,
    bytes: usize,
    recording: Option<String>,
}

/// The source clock epoch is a fresh random ID per daemon configuration lifetime.
/// Device boot identity is not known; timestamps here describe reads, not RF arrival.
pub struct Feed {
    epoch: String,
    buffer: Mutex<Buffer>,
    next_sequence: AtomicU64,
    source_alive: AtomicBool,
    recording_known: AtomicBool,
    oversized: AtomicU64,
    contended: AtomicU64,
    notify: Notify,
    clients: Arc<Semaphore>,
    shutdown: CancellationToken,
}

pub struct Publisher {
    feed: Arc<Feed>,
    next: u64,
    origin: Instant,
}
impl Publisher {
    pub fn feed(&self) -> Arc<Feed> {
        self.feed.clone()
    }
    /// Called only by the single diagnostic reader. No I/O, await or waiting lock.
    pub fn publish(&mut self, raw: &[u8]) {
        let sequence = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("raw stream sequence exhausted");
        if raw.len() > MAX_READ {
            self.feed.oversized.fetch_add(1, Ordering::Relaxed);
        } else if let Ok(mut buffer) = self.feed.buffer.try_lock() {
            let recording = if self.feed.recording_known.load(Ordering::Acquire) {
                buffer.recording.as_deref()
            } else {
                None
            };
            let metadata = json!({"sequence":sequence, "recording_id":recording,
                "read_monotonic_ns":self.origin.elapsed().as_nanos() as u64,
                "read_system_unix_ns":SystemTime::now().duration_since(UNIX_EPOCH).ok().map(|t|t.as_nanos().to_string()),
                "raw_length":raw.len()});
            let header = serde_json::to_vec(&metadata).expect("fixed raw metadata serialization");
            // Recording names are bounded by set_recording, including JSON escaping.
            let mut frame = Vec::with_capacity(4 + header.len() + raw.len());
            frame.extend_from_slice(&(header.len() as u32).to_le_bytes());
            frame.extend_from_slice(&header);
            frame.extend_from_slice(raw);
            assert!(frame.len() <= MAX_FRAME);
            while buffer.records.len() >= MAX_RECORDS || buffer.bytes + frame.len() > MAX_BYTES {
                let old = buffer.records.pop_front().expect("nonempty bounded ring");
                buffer.bytes -= old.frame.len();
            }
            buffer.bytes += frame.len();
            buffer.records.push_back(Record {
                sequence,
                frame: frame.into(),
            });
            // Commit the sequence while holding the ring lock so readers cannot
            // report an in-progress publication as a gap.
            self.feed.next_sequence.store(self.next, Ordering::Release);
            drop(buffer);
            self.feed.notify.notify_waiters();
            return;
        } else {
            self.feed.contended.fetch_add(1, Ordering::Relaxed);
        }
        self.feed.next_sequence.store(self.next, Ordering::Release);
        self.feed.notify.notify_waiters();
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.feed.source_alive.store(false, Ordering::Release);
        self.feed.notify.notify_waiters();
    }
}

enum Item {
    Record(Record),
    Gap { from: u64, to: u64 },
}
impl Feed {
    fn new(shutdown: CancellationToken) -> io::Result<Arc<Self>> {
        let mut random = [0u8; 16];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        Ok(Arc::new(Self {
            epoch: random.iter().map(|b| format!("{b:02x}")).collect(),
            buffer: Mutex::new(Buffer::default()),
            next_sequence: AtomicU64::new(0),
            source_alive: AtomicBool::new(true),
            recording_known: AtomicBool::new(true),
            oversized: AtomicU64::new(0),
            contended: AtomicU64::new(0),
            notify: Notify::new(),
            clients: Arc::new(Semaphore::new(MAX_CLIENTS)),
            shutdown,
        }))
    }
    pub fn set_recording(&self, name: Option<&str>) {
        // Called by the same diagnostic task as publish; never wait for a
        // consumer. If the update loses the lock, report unknown rather than
        // attaching the previous recording to subsequent reads.
        if let Ok(mut buffer) = self.buffer.try_lock() {
            buffer.recording = name.filter(|s| s.len() <= 256).map(str::to_owned);
            self.recording_known.store(true, Ordering::Release);
        } else {
            self.recording_known.store(false, Ordering::Release);
        }
    }
    fn item(&self, cursor: u64) -> Option<Item> {
        let buffer = self.buffer.lock().expect("raw ring poisoned");
        let next = buffer.records.iter().find(|r| r.sequence >= cursor);
        match next {
            Some(record) if record.sequence == cursor => Some(Item::Record(record.clone())),
            Some(record) => Some(Item::Gap {
                from: cursor,
                to: record.sequence,
            }),
            None => {
                let tip = self.next_sequence.load(Ordering::Acquire);
                (tip > cursor).then_some(Item::Gap {
                    from: cursor,
                    to: tip,
                })
            }
        }
    }
    fn status(&self) -> serde_json::Value {
        json!({"type":"status", "next_sequence":self.next_sequence.load(Ordering::Acquire),
            "oversized_reads":self.oversized.load(Ordering::Relaxed),
            "contended_reads":self.contended.load(Ordering::Relaxed),
            "source_alive":self.source_alive.load(Ordering::Acquire), "rf_loss":"unknown"})
    }
}

/// A separate loopback listener deliberately avoids the unauthenticated hotspot API.
/// No listener or diagnostic observer exists by default or in debug mode.
pub async fn start(
    port: Option<u16>,
    debug: bool,
    tracker: &TaskTracker,
    shutdown: CancellationToken,
) -> io::Result<Option<Publisher>> {
    let Some(port) = port.filter(|_| !debug) else {
        return Ok(None);
    };
    if port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw_stream_port must be nonzero",
        ));
    }
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    let feed = Feed::new(shutdown.clone())?;
    let app = router(feed.clone());
    tracker.spawn(async move {
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
        {
            log::error!("Raw diagnostic listener failed: {error}");
        }
    });
    Ok(Some(Publisher {
        feed,
        next: 0,
        origin: Instant::now(),
    }))
}

fn router(feed: Arc<Feed>) -> Router {
    Router::new()
        .route("/raw-diag", get(upgrade))
        .with_state(feed)
}
async fn upgrade(
    State(feed): State<Arc<Feed>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Browser origins are rejected even on loopback (cross-site WebSocket hijacking).
    if headers.contains_key("origin") {
        return StatusCode::FORBIDDEN.into_response();
    }
    let protocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|p| p.trim() == PROTOCOL));
    if !protocol {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(permit) = feed.clients.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    ws.protocols([PROTOCOL])
        .max_message_size(1024)
        .max_frame_size(1024)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_FRAME * 2)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            client(socket, feed).await
        })
}
async fn client(socket: WebSocket, feed: Arc<Feed>) {
    let (mut sink, mut source) = socket.split();
    let cursor = feed.next_sequence.load(Ordering::Acquire);
    let hello = json!({"type":"hello", "version":1, "stream_epoch":feed.epoch,
        "source":"/dev/diag", "device_boot_id":null, "next_sequence":cursor,
        "rayhunter_version":env!("CARGO_PKG_VERSION"), "replay":false,
        "format":"u32le_json_length+json+exact_device_read", "max_read_bytes":MAX_READ,
        "retained_bytes_limit":MAX_BYTES, "retained_records_limit":MAX_RECORDS,
        "max_clients":MAX_CLIENTS, "clock":"reader_relative_monotonic_and_system_wall",
        "rf_loss":"unknown"});
    let writer = async {
        send(&mut sink, Message::Text(hello.to_string().into())).await?;
        write_events(&mut sink, &feed, cursor).await
    };
    let reader = async {
        while let Some(Ok(message)) = source.next().await {
            // No client-side recording or modem controls. Ping/Pong are transport only.
            if !matches!(message, Message::Ping(_) | Message::Pong(_)) {
                break;
            }
        }
    };
    tokio::select! { _ = feed.shutdown.cancelled() => {}, _ = reader => {}, _ = writer => {} }
}
async fn send<S: futures::Sink<Message> + Unpin>(sink: &mut S, message: Message) -> Result<(), ()> {
    timeout(SEND_TIMEOUT, sink.send(message))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}
async fn write_events(
    sink: &mut SplitSink<WebSocket, Message>,
    feed: &Feed,
    mut cursor: u64,
) -> Result<(), ()> {
    let mut heartbeat = interval(Duration::from_secs(1));
    loop {
        let notified = feed.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        match feed.item(cursor) {
            Some(Item::Record(record)) => {
                send(sink, Message::Binary(record.frame)).await?;
                cursor = record.sequence + 1;
            }
            Some(Item::Gap { from, to }) => {
                send(
                    sink,
                    Message::Text(
                        json!({"type":"gap", "from_sequence":from,
                    "to_sequence_exclusive":to, "reason":"not_retained"})
                        .to_string()
                        .into(),
                    ),
                )
                .await?;
                cursor = to;
            }
            None if !feed.source_alive.load(Ordering::Acquire) => {
                send(
                    sink,
                    Message::Text(
                        json!({"type":"end", "reason":"diagnostic_reader_ended"})
                            .to_string()
                            .into(),
                    ),
                )
                .await?;
                return Ok(());
            }
            None => tokio::select! {
                _ = notified => {},
                _ = heartbeat.tick() => { send(sink, Message::Text(feed.status().to_string().into())).await?; }
            },
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests;
