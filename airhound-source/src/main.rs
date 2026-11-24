//! GPL decoder/transport executable. AirHound communicates only over versioned JSON IPC.
use futures::StreamExt;
use rayhunter::diag::{Message as DiagMessage, diaglog::LogBody};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    time::Duration,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{Message, WebSocketConfig},
    },
};
const MAX_READ: usize = 256 * 1024;
const CHUNK: usize = 32 * 1024;
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid raw diagnostic feed")
}
fn emit(mut v: Value) -> io::Result<()> {
    v["ipc_version"] = 1.into();
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &v)?;
    out.write_all(b"\n")?;
    out.flush()
}
fn cell(
    sink: &mut impl FnMut(Value) -> io::Result<()>,
    sequence: u64,
    index: usize,
    timestamp: u64,
    observation: Value,
) -> io::Result<()> {
    sink(
        json!({"kind":"cell","sequence":sequence,"message_index":index,"modem_timestamp":timestamp,"observation":observation}),
    )
}
fn decode(
    sink: &mut impl FnMut(Value) -> io::Result<()>,
    sequence: u64,
    index: usize,
    msg: DiagMessage,
) -> io::Result<()> {
    let DiagMessage::Log {
        timestamp, body, ..
    } = &msg
    else {
        return Ok(());
    };
    let ts = timestamp.ts;
    match body {
        LogBody::LteMl1ServingCellMeasurementAndEvaluation { data } => {
            return cell(
                sink,
                sequence,
                index,
                ts,
                json!({"rat":"lte","kind":"serving_measurement","earfcn":data.get_earfcn(),"pci":data.get_pci(),"rsrp_dbm":data.get_meas_rsrp(),"rsrq_db":data.get_meas_rsrq(),"rssi_dbm":data.get_meas_rssi()}),
            );
        }
        LogBody::LteMl1NeighborCellsMeasurements { data } => {
            for c in &data.cells {
                cell(
                    sink,
                    sequence,
                    index,
                    ts,
                    json!({"rat":"lte","kind":"neighbor_measurement","earfcn":data.get_earfcn(),"pci":c.pci,"rsrp_dbm":c.get_meas_rsrp(),"rsrq_db":c.get_meas_rsrq(),"rssi_dbm":c.get_meas_rssi()}),
                )?;
            }
            return Ok(());
        }
        _ => {}
    }
    use rayhunter::diag::diaglog::rrc::LteRrcOtaPacket as P;
    let key = match body {
        LogBody::LteRrcOtaMessage { packet, .. } => Some((
            packet.get_earfcn(),
            match packet {
                P::V0 { phy_cell_id, .. }
                | P::V5 { phy_cell_id, .. }
                | P::V8 { phy_cell_id, .. }
                | P::V25 { phy_cell_id, .. } => *phy_cell_id,
            },
        )),
        _ => None,
    };
    let Some((earfcn, pci)) = key else {
        return Ok(());
    };
    use rayhunter::analysis::information_element::{
        InformationElement as I, LteInformationElement as L,
    };
    use telcom_parser::lte_rrc::{BCCH_DL_SCH_MessageType as B, BCCH_DL_SCH_MessageType_c1 as C};
    if let Ok(Some((_, g))) = rayhunter::gsmtap::parser::parse(msg)
        && let Ok(I::LTE(ie)) = I::try_from(&g)
        && let L::BcchDlSch(b) = *ie
        && let B::C1(C::SystemInformationBlockType1(s)) = b.message
    {
        let a = s.cell_access_related_info;
        let mut inherited = None::<String>;
        if a.tracking_area_code.0.len() != 16 || a.cell_identity.0.len() != 28 {
            return Err(invalid());
        }
        let tac = a
            .tracking_area_code
            .0
            .iter()
            .fold(0u32, |v, b| (v << 1) | u32::from(*b));
        let eci = a
            .cell_identity
            .0
            .iter()
            .fold(0u32, |v, b| (v << 1) | u32::from(*b));
        for p in &a.plmn_identity_list.0 {
            if let Some(mcc) = &p.plmn_identity.mcc {
                inherited = Some(mcc.0.iter().map(|d| char::from(b'0' + d.0)).collect());
            }
            if let Some(mcc) = &inherited {
                let mnc: String = p
                    .plmn_identity
                    .mnc
                    .0
                    .iter()
                    .map(|d| char::from(b'0' + d.0))
                    .collect();
                cell(
                    sink,
                    sequence,
                    index,
                    ts,
                    json!({"rat":"lte","kind":"cell_identity","earfcn":earfcn,"pci":pci,"mcc":mcc,"mnc":mnc,"tac":tac,"eci":eci}),
                )?;
            }
        }
    }
    Ok(())
}
// Validate length-prefixed kernel container before invoking any allocating decoder.
fn messages(raw: &[u8]) -> io::Result<Vec<&[u8]>> {
    if raw.len() < 8 || u32::from_le_bytes(raw[..4].try_into().unwrap()) != 32 {
        return Err(invalid());
    }
    let n = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
    if n > (raw.len() - 8) / 4 {
        return Err(invalid());
    }
    let mut at = 8;
    let mut out = Vec::new();
    for _ in 0..n {
        let length = raw.get(at..at + 4).ok_or_else(invalid)?;
        let len = u32::from_le_bytes(length.try_into().unwrap()) as usize;
        at += 4;
        let body = raw.get(at..at + len).ok_or_else(invalid)?;
        at += len;
        for message in body.split_inclusive(|b| *b == 0x7e) {
            if out.len() >= 4096 {
                return Err(invalid());
            }
            out.push(message);
        }
    }
    if at != raw.len() {
        return Err(invalid());
    }
    Ok(out)
}
fn frame(bytes: &[u8], sink: &mut impl FnMut(Value) -> io::Result<()>) -> io::Result<()> {
    if bytes.len() < 4 || bytes.len() > MAX_READ + 2048 {
        return Err(invalid());
    }
    let n = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    if n > 2044 || bytes.len() < 4 + n {
        return Err(invalid());
    }
    let meta: Value = serde_json::from_slice(&bytes[4..4 + n])?;
    let sequence = meta["sequence"].as_u64().ok_or_else(invalid)?;
    let raw = &bytes[4 + n..];
    if raw.is_empty()
        || raw.len() > MAX_READ
        || meta["raw_length"].as_u64() != Some(raw.len() as u64)
    {
        return Err(invalid());
    }
    let hash = format!("{:x}", Sha256::digest(raw));
    for (i, chunk) in raw.chunks(CHUNK).enumerate() {
        let hex: String = chunk.iter().map(|b| format!("{b:02x}")).collect();
        sink(json!({"kind":"raw","frame":meta,"offset":i*CHUNK,"hex":hex,"sha256":hash}))?;
    }
    match messages(raw) {
        Ok(parts) => {
            for (index, bytes) in parts.into_iter().enumerate() {
                match DiagMessage::from_hdlc(bytes) {
                    Ok(msg) => decode(sink, sequence, index, msg)?,
                    Err(_) => sink(
                        json!({"kind":"decode_error","sequence":sequence,"message_index":index,"reason":"diagnostic_message"}),
                    )?,
                }
            }
        }
        Err(_) => {
            sink(json!({"kind":"decode_error","sequence":sequence,"reason":"container_format"}))?
        }
    }
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--endpoint") {
        return Err("--endpoint required".into());
    }
    let endpoint = args.next().ok_or("endpoint required")?;
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let address = endpoint
        .strip_prefix("ws://")
        .and_then(|s| s.strip_suffix("/raw-diag"))
        .and_then(|s| s.parse::<std::net::SocketAddr>().ok());
    if !address.is_some_and(|a| a.ip().is_loopback() && a.port() != 0) {
        return Err("explicit loopback raw endpoint required".into());
    }
    let mut req = endpoint.into_client_request()?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", "rayhunter.raw-diag.v1".parse()?);
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_READ + 2048))
        .max_frame_size(Some(MAX_READ + 2048));
    let (mut ws, response) = tokio::time::timeout(
        Duration::from_secs(5),
        connect_async_with_config(req, Some(config), true),
    )
    .await??;
    if response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|v| v.to_str().ok())
        != Some("rayhunter.raw-diag.v1")
    {
        return Err("raw subprotocol mismatch".into());
    }
    let mut hello = false;
    while let Some(message) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await? {
        match message? {
            Message::Text(text) => {
                let v: Value = serde_json::from_str(&text)?;
                match v["type"].as_str() {
                    Some("hello") if !hello => {
                        emit(
                            json!({"kind":"hello","stream":v,"decoder_version":env!("CARGO_PKG_VERSION")}),
                        )?;
                        hello = true;
                    }
                    Some("gap") if hello => emit(
                        json!({"kind":"gap","from_sequence":v["from_sequence"],"to_sequence_exclusive":v["to_sequence_exclusive"]}),
                    )?,
                    Some("status") if hello => emit(json!({"kind":"status","source":v}))?,
                    Some("end") => break,
                    _ => return Err(invalid().into()),
                }
            }
            Message::Binary(bytes) if hello => frame(&bytes, &mut emit)?,
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => break,
            _ => return Err(invalid().into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn packet() -> Vec<u8> {
        // Existing GPL Rayhunter ML1 regression fixture; synthetic identity/clock.
        let hex = "040100009C18D60AECC44E00E2244E00FFFCE30FFED80A0047AD56021D310100A2624100";
        let body: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect();
        let len = (12 + body.len()) as u16;
        let mut msg = vec![0x10, 0];
        msg.extend(len.to_le_bytes());
        msg.extend(len.to_le_bytes());
        msg.extend(0xb17fu16.to_le_bytes());
        msg.extend(123u64.to_le_bytes());
        msg.extend(body);
        rayhunter::hdlc::hdlc_encapsulate(&msg, &rayhunter::diag::CRC_CCITT)
    }
    fn raw_frame(raw: &[u8]) -> Vec<u8> {
        let meta=serde_json::to_vec(&json!({"sequence":9,"recording_id":"fixture","read_monotonic_ns":10,"read_system_unix_ns":null,"raw_length":raw.len()})).unwrap();
        let mut b = (meta.len() as u32).to_le_bytes().to_vec();
        b.extend(meta);
        b.extend(raw);
        b
    }
    #[test]
    fn real_decoder_keeps_raw_before_measurement_and_modem_clock_uninterpreted() {
        let msg = packet();
        let mut raw = 32u32.to_le_bytes().to_vec();
        raw.extend(1u32.to_le_bytes());
        raw.extend((msg.len() as u32).to_le_bytes());
        raw.extend(msg);
        let mut records = Vec::new();
        frame(&raw_frame(&raw), &mut |v| {
            records.push(v);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["kind"], "raw");
        assert_eq!(records[1]["kind"], "cell");
        let cell = &records[1];
        assert_eq!(cell["sequence"], 9);
        assert_eq!(cell["modem_timestamp"], 123);
        assert_eq!(cell["observation"]["pci"], 214);
        assert_eq!(cell["observation"]["earfcn"], 6300);
        assert_eq!(cell["observation"]["rsrp_dbm"], -101.25);
        assert!(cell.get("capture_time").is_none());
    }
    #[test]
    fn malformed_container_retains_exact_raw_and_reports_error_without_cell() {
        let raw = [32, 0, 0, 0, 255, 255, 255, 255];
        let mut records = Vec::new();
        frame(&raw_frame(&raw), &mut |v| {
            records.push(v);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["hex"], "20000000ffffffff");
        assert_eq!(records[1]["kind"], "decode_error");
        assert!(messages(&[32, 0, 0, 0, 1, 0, 0, 0, 255, 255, 255, 255]).is_err());
    }
    #[test]
    fn frame_bound_and_output_failure_prevent_fabricated_cells() {
        assert!(frame(&[255, 255, 255, 255], &mut |_| Ok(())).is_err());
        assert!(frame(&raw_frame(&[1; MAX_READ + 1]), &mut |_| Ok(())).is_err());
        let mut calls = 0;
        assert!(
            frame(&raw_frame(&[32, 0, 0, 0, 0, 0, 0, 0]), &mut |_| {
                calls += 1;
                Err(io::Error::other("closed pipe"))
            })
            .is_err()
        );
        assert_eq!(calls, 1);
    }
}
