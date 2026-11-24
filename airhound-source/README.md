# AirHound live-source helper (experimental)

This GPL-licensed executable is a separate process from the MIT AirHound host. It
subscribes to the opt-in Rayhunter `rayhunter.raw-diag.v1` WebSocket feed and uses
Rayhunter's existing Qualcomm/RRC decoding library. It never opens `/dev/diag`,
starts/stops recordings, changes masks, installs a device binary or runs ADB.

Build with `cargo build --locked -p rayhunter-airhound-source`; invoke with
`--endpoint ws://127.0.0.1:<explicit-forward>/raw-diag`. Only numeric loopback
endpoints are accepted. The corresponding device forward must be prepared for an
explicitly selected device outside this helper. Stdout is versioned JSON IPC;
stderr is never parsed as observations.

The helper emits `hello`, exact `raw` chunks, `cell`, `decode_error`, `gap`, and
`status` records. Frames are at most 256 KiB; raw chunks are at most 32 KiB and share
the full frame SHA-256 and original metadata. Derived cells follow raw chunks and
reference sequence plus diagnostic-message index. Initial useful fields are LTE
serving/neighbor measurements and SIB1 identities; unsupported messages remain in
raw evidence. Complete PLMN identity requires an explicit or SIB1-inherited MCC;
MNC leading zeros and two/three-digit length are preserved. Full EARFCN comes from
the original diagnostic record, not the narrowed GSMTAP header.

No modem timestamp is asserted to be current UTC, and no cell is asserted to be a
physical tower position or malicious transmitter. Sequence gaps measure unavailable
feed reads, not RF loss. Decoder failures do not erase the preceding raw bytes.
The parent bounds its queues, closes/reaps this process on Pause/Finish, and applies
a finite reconnect policy. Closing a subscription leaves device capture untouched.

Tests use synthetic bytes and a temporary loopback WebSocket server. There is no
live-device acceptance claim in this package. Successful future deployments retain
the prototype as requested; official rollback remains available separately.
