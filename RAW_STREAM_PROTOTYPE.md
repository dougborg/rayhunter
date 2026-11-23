# Raw diagnostic stream prototype

Fork-local experiment for AirHound integration. This code and its tests/docs were
created with AI assistance and still require human review and device acceptance.
It is not an upstream proposal or a production firmware release.

## Enable and connect

Add `raw_stream_port = 8082` to the top level of the existing Rayhunter TOML
configuration and restart the daemon. Omit the field to disable it (the default).
Debug mode always disables it. Port zero is rejected. Failure to bind the port
fails startup rather than silently claiming the stream is available.

The **separate** listener binds only `127.0.0.1`; no raw route is added to the
normal HTTP server, which may listen on the hotspot. Connect with WebSocket
subprotocol `rayhunter.raw-diag.v1` to `ws://127.0.0.1:8082/raw-diag`.
For a USB-attached Orbic, an explicitly established local ADB tunnel such as
`adb forward tcp:18082 tcp:8082` permits a client to use localhost port 18082.
This tunnel has not yet been validated on hardware with this prototype.
There is no authentication or TLS on this loopback-only listener. Local processes
and explicitly forwarded connections can read it. Browser Origin headers are
rejected. Do not expose the forward on a public interface.

Connecting, disconnecting, or lagging clients do not start/stop recording, change
modem logging masks, or open another `/dev/diag` reader. The single existing
reader observes every positive device read **before** short-read filtering,
container parsing, QMDL writing, analysis and export filtering. This includes
malformed data and logging-configuration responses. The feed remains active
between recordings because the existing reader does. This is raw diagnostic
output under Rayhunter's existing logging configuration, not I/Q, arbitrary RF,
or every cellular packet in the area.

## Wire format, version 1

The first WebSocket text message is a JSON `hello`, containing:

- `version: 1`, `stream_epoch`: random 128-bit hex identity for this source
  lifetime, `source: "/dev/diag"`, and Rayhunter's package version.
- `next_sequence`: the live starting cursor for this connection, `replay: false`.
- `device_boot_id: null`: the hardware boot identity is not established.
- Format, clock description and byte/record/client limits.

Each binary WebSocket **message** is one positive device read, not necessarily
one modem message or a complete protocol packet. It contains:

1. Four bytes: unsigned little-endian JSON-header length.
2. That many bytes of UTF-8 JSON metadata.
3. Exact, unmodified device-read bytes; no QMDL conversion or HDLC removal.

Metadata contains `sequence` (u64), `raw_length`, `recording_id` (nullable),
`read_monotonic_ns` (u64, elapsed since source creation) and
`read_system_unix_ns` (decimal string, or null before UNIX epoch).
A recording ID is the active store entry name at read time, not a guarantee
that those bytes reached disk. Unknown, contended-update or over-256-byte names are null.
Timestamp provenance is the userspace read callback, **not RF arrival**.
Wall time can jump when the device clock is corrected. No GNSS synchronization,
offset estimate or uncertainty bound is claimed. Use the stream epoch together
with the sequence; neither device boot nor monotonic origins survive restarts.
Consumers must preserve 64-bit integer precision.

Text messages after the hello:

- `gap`: `from_sequence`, `to_sequence_exclusive`, `reason: "not_retained"`.
  The interval identifies missing reads, not missing bytes or RF packets.
- `status`: next sequence, cumulative oversized/contended read counters,
  `source_alive`, and `rf_loss: "unknown"`. Status is sent approximately each
  second when the client is caught up; it is not a liveness guarantee under load.
- `end`: `reason: "diagnostic_reader_ended"`, after retained records have drained.
  Transport loss/cancellation may occur without an end message.

Sequence numbers advance for each positive read, including discarded reads.
A client detects retained-history loss through explicit gap messages. Reconnection
starts at the current tip, even if older data remains in memory: **no replay API**
or durable/resumable delivery guarantee is provided. Compare the previous cursor
and the next hello to recognize reconnect loss within the same epoch.

## Bounds and overload behavior

- At most 256 KiB per accepted raw read; larger reads are entirely discarded
  from the feed and counted. The original diagnostic path still receives them.
- At most 64 retained records **and** 2 MiB total encoded ring bytes.
- At most two upgraded clients; extra clients receive HTTP 429.
- Each client holds at most one outgoing record, plus bounded WebSocket buffering
  (twice the maximum encoded frame). Kernel/network buffers are additional.
- Inbound frames/messages are capped at 1 KiB. Only Ping/Pong are accepted;
  client data messages close the connection. There are no control commands.
- Each send has a two-second deadline; idle and busy clients stop on cancellation.
- The producer never awaits a consumer, network write or contended ring lock.
  On lock contention it discards that feed read and advances the sequence.
  Old records are evicted for byte/count limits. Copies and allocations up to
  the per-read limit still add capture overhead, to be measured on the Orbic.

The sequence/gaps describe this fork's feed loss only. Modem/kernel/RF loss is
unknown. No raw ambient samples are committed in this branch.

## Validation

The synthetic Rust tests exercise binary framing, exact byte preservation,
recording context, byte/count bounds, oversized and contended reads, actual
WebSocket subscription/lag/reconnect/client limits, browser-origin rejection,
disabled defaults and cancellation. A library test runs the real device-read
hook against an ordinary temporary file with malformed bytes, before parse
failure. These do not establish real-device throughput or source compatibility.

Build the normal web assets first, then run:

```sh
npm ci --workspace=daemon/web --include-workspace-root
npm run build --workspace=daemon/web
cargo test -p rayhunter --lib raw_observer_tests
cargo test -p rayhunter-daemon --lib raw_stream --no-default-features
cargo check -p rayhunter-daemon --bin rayhunter-daemon --no-default-features
```

`scripts/raw-stream-client.py --self-test` checks a synthetic framing fixture
without dependencies, hardware, raw ambient samples or a running daemon. With
`websockets` installed in a private Python virtual environment, passing a local
WebSocket URL prints metadata and byte counts only. It does not persist raw data.

Before device deployment: independent adversarial review, target cross-build,
Orbic CPU/RSS/throughput measurement, source-read-size measurement, USB-forward
verification, recorder integrity comparison and deliberate slow/disconnected
client soak. There is no firmware installation or modem change in these tests.
