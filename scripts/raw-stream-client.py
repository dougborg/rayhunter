#!/usr/bin/env python3
"""Metadata-only client for the fork-local raw DIAG prototype; no raw-data files."""
import argparse
import asyncio
import json
import struct

PROTOCOL = "rayhunter.raw-diag.v1"
MAX_FRAME = 256 * 1024 + 2048


def decode(frame):
    if not 4 <= len(frame) <= MAX_FRAME:
        raise ValueError("invalid frame size")
    size = struct.unpack_from("<I", frame)[0]
    if size > 2044 or 4 + size > len(frame):
        raise ValueError("invalid header size")
    metadata = json.loads(frame[4:4 + size])
    raw = frame[4 + size:]
    if metadata["raw_length"] != len(raw):
        raise ValueError("raw length mismatch")
    return metadata, raw


async def watch(url):
    from websockets.asyncio.client import connect
    async with connect(url, subprotocols=[PROTOCOL], max_size=MAX_FRAME,
                       max_queue=2) as socket:
        async for message in socket:
            if isinstance(message, bytes):
                metadata, raw = decode(message)
                print(json.dumps({"type": "raw", **metadata, "received_bytes": len(raw)}))
            else:
                print(json.dumps(json.loads(message)))


def self_test():
    raw = bytes([0, 255, 126, 125, 1])
    metadata = {"sequence": 0, "recording_id": None, "raw_length": len(raw),
                "read_monotonic_ns": 42, "read_system_unix_ns": "123456789"}
    header = json.dumps(metadata).encode()
    frame = struct.pack("<I", len(header)) + header + raw
    assert decode(frame) == (metadata, raw)
    for invalid in (b"", struct.pack("<I", 9999), frame[:-1]):
        try:
            decode(invalid)
        except ValueError:
            continue
        raise AssertionError("accepted invalid frame")
    print("synthetic frame checks passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", default="ws://127.0.0.1:18082/raw-diag")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        asyncio.run(watch(args.url))
