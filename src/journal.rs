//! Journal writer and reader for graphd's write-ahead journal.
//!
//! Binary entry format:
//! `[4B CRC32C le][4B payload_len le][8B sequence le][32B prev_hash][protobuf payload]`
//! CRC32C covers everything after itself (bytes 4..).

use crate::graphd as proto;
use crate::types::{param_values_to_map_entries, ParamValue};
use crate::{current_timestamp_ms, graphj};
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, info};

// ─── Types ───

pub struct PendingEntry {
    pub query: String,
    pub params: Vec<(String, ParamValue)>,
}

pub enum JournalCommand {
    Write(PendingEntry),
    Flush(std::sync::mpsc::SyncSender<()>),
    /// Force-seal the current segment so it can be uploaded to S3.
    /// Returns the path of the sealed segment, or None if no active segment.
    SealForUpload(std::sync::mpsc::SyncSender<Option<PathBuf>>),
    Shutdown,
}

pub type JournalSender = std::sync::mpsc::Sender<JournalCommand>;

pub struct JournalState {
    pub sequence: AtomicU64,
    pub chain_hash: Mutex<[u8; 32]>,
    /// Set to false when the journal writer thread exits (crash or shutdown).
    pub alive: AtomicBool,
}

impl JournalState {
    pub fn with_sequence_and_hash(seq: u64, hash: [u8; 32]) -> Self {
        Self {
            sequence: AtomicU64::new(seq),
            chain_hash: Mutex::new(hash),
            alive: AtomicBool::new(true),
        }
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

// ─── Binary format ───
// [4B CRC32C le][4B payload_len le][8B sequence le][32B prev_hash][protobuf payload]
// CRC32C covers everything after itself (bytes 4..).

const FIXED_HEADER: usize = 4 + 4 + 8 + 32; // 48 bytes

fn encode_entry(seq: u64, prev_hash: &[u8; 32], payload: &[u8]) -> Vec<u8> {
    let total = FIXED_HEADER + payload.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&[0u8; 4]); // CRC32C placeholder
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(prev_hash);
    buf.extend_from_slice(payload);
    let crc = crc32c::crc32c(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

struct DecodedEntry {
    sequence: u64,
    prev_hash: [u8; 32],
    payload: Vec<u8>,
}

fn read_entry_from<R: Read>(reader: &mut R) -> Result<Option<DecodedEntry>, String> {
    // Read fixed header (48 bytes).
    let mut header = [0u8; FIXED_HEADER];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(format!("Failed to read journal header: {e}")),
    }

    let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let sequence = u64::from_le_bytes([
        header[8], header[9], header[10], header[11], header[12], header[13], header[14],
        header[15],
    ]);
    let mut prev_hash = [0u8; 32];
    prev_hash.copy_from_slice(&header[16..48]);

    let mut payload = vec![0u8; payload_len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| format!("Truncated journal payload at seq {sequence}: {e}"))?;

    // Validate CRC32C: covers bytes[4..] of the full entry.
    let mut crc_buf = Vec::with_capacity(44 + payload_len);
    crc_buf.extend_from_slice(&header[4..]);
    crc_buf.extend_from_slice(&payload);
    let computed_crc = crc32c::crc32c(&crc_buf);
    if computed_crc != stored_crc {
        return Err(format!(
            "CRC32C mismatch at seq {sequence}: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
        ));
    }

    Ok(Some(DecodedEntry {
        sequence,
        prev_hash,
        payload,
    }))
}

// ─── Writer ───

pub fn spawn_journal_writer(
    journal_dir: PathBuf,
    segment_max_bytes: u64,
    fsync_ms: u64,
    state: Arc<JournalState>,
) -> JournalSender {
    std::fs::create_dir_all(&journal_dir).expect("Failed to create journal directory");

    let (tx, rx) = std::sync::mpsc::channel::<JournalCommand>();

    std::thread::Builder::new()
        .name("journal-writer".into())
        .spawn(move || {
            writer_loop(rx, &journal_dir, segment_max_bytes, fsync_ms, &state);
            // Mark the writer as dead on exit (normal or panic).
            state.alive.store(false, Ordering::SeqCst);
        })
        .expect("Failed to spawn journal writer thread");

    tx
}

/// Zstd compression level for sealed journal segments.
const JOURNAL_ZSTD_LEVEL: i32 = 3;

/// Seal a .graphj segment: compress body with zstd, rewrite header with
/// SEALED + COMPRESSED + HAS_CHAIN_HASH flags, append 32-byte chain hash trailer.
/// The entire seal (header + body + trailer) is written atomically with fsync.
fn seal_segment(
    file: &mut File,
    first_seq: u64,
    last_seq: u64,
    entry_count: u64,
    _body_len: u64,
    _body_hasher: Sha256,
    created_ms: i64,
    chain_hash: [u8; 32],
) -> io::Result<()> {
    // Read the raw body (everything after the 128-byte header).
    let file_len = file.seek(SeekFrom::End(0))?;
    let raw_body_len = file_len - graphj::HEADER_SIZE as u64;
    file.seek(SeekFrom::Start(graphj::HEADER_SIZE as u64))?;
    let mut raw_body = vec![0u8; raw_body_len as usize];
    file.read_exact(&mut raw_body)?;

    // Compress with zstd.
    let compressed = zstd::encode_all(raw_body.as_slice(), JOURNAL_ZSTD_LEVEL)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("zstd compress: {e}")))?;

    // Checksum of the compressed body (what's stored on disk).
    let mut hasher = Sha256::new();
    hasher.update(&compressed);
    let body_checksum: [u8; 32] = hasher.finalize().into();

    let header = graphj::GraphjHeader {
        flags: graphj::FLAG_SEALED | graphj::FLAG_COMPRESSED | graphj::FLAG_HAS_CHAIN_HASH,
        compression: graphj::COMPRESSION_ZSTD,
        encryption: graphj::ENCRYPTION_NONE,
        zstd_level: JOURNAL_ZSTD_LEVEL,
        first_seq,
        last_seq,
        entry_count,
        body_len: compressed.len() as u64,
        body_checksum,
        nonce: [0u8; 24],
        created_ms,
    };

    // Rewrite: header + compressed body + chain hash trailer, then truncate + fsync.
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&graphj::encode_header(&header))?;
    file.write_all(&compressed)?;
    file.write_all(&chain_hash)?;
    let total_len = graphj::HEADER_SIZE as u64
        + compressed.len() as u64
        + graphj::CHAIN_HASH_TRAILER_SIZE as u64;
    file.set_len(total_len)?;
    file.sync_all()?;
    Ok(())
}

fn writer_loop(
    rx: std::sync::mpsc::Receiver<JournalCommand>,
    journal_dir: &Path,
    segment_max_bytes: u64,
    fsync_ms: u64,
    state: &JournalState,
) {
    let mut current_file: Option<File> = None;
    let mut current_size: u64 = 0;
    let mut segment_first_seq: u64 = 0;
    let mut segment_last_seq: u64 = 0;
    let mut segment_entry_count: u64 = 0;
    let mut body_hasher: Option<Sha256> = None;
    let timeout = Duration::from_millis(fsync_ms);

    loop {
        let cmd = rx.recv_timeout(timeout);
        match cmd {
            Ok(JournalCommand::Write(entry)) => {
                // Hold chain lock for the entire compute + write + update cycle.
                let mut chain = state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());
                let prev_hash = *chain;
                let new_seq = state.sequence.load(Ordering::SeqCst) + 1;

                let proto_entry = proto::JournalEntry {
                    sequence: new_seq,
                    query: entry.query,
                    params: param_values_to_map_entries(&entry.params),
                    timestamp_ms: current_timestamp_ms(),
                };
                let payload = proto_entry.encode_to_vec();

                let mut hasher = Sha256::new();
                hasher.update(prev_hash);
                hasher.update(&payload);
                let new_hash: [u8; 32] = hasher.finalize().into();

                let buf = encode_entry(new_seq, &prev_hash, &payload);

                // Rotate segment if needed.
                if current_file.is_none()
                    || current_size + buf.len() as u64 > segment_max_bytes
                {
                    // Seal the old segment before rotation.
                    if let Some(ref mut f) = current_file {
                        if let Some(hasher) = body_hasher.take() {
                            let body_len = current_size - graphj::HEADER_SIZE as u64;
                            if let Err(e) = seal_segment(
                                f,
                                segment_first_seq,
                                segment_last_seq,
                                segment_entry_count,
                                body_len,
                                hasher,
                                current_timestamp_ms(),
                                prev_hash, // chain hash after last entry in old segment
                            ) {
                                error!("Failed to seal segment on rotation: {e}");
                            }
                        }
                    }

                    // Create new .graphj segment (read+write so seal_segment can read body).
                    let path = journal_dir.join(format!("journal-{new_seq:016}.graphj"));
                    match File::options().read(true).write(true).create(true).truncate(true).open(&path) {
                        Ok(mut f) => {
                            // Write unsealed header.
                            let header = graphj::GraphjHeader::new_unsealed(
                                new_seq,
                                current_timestamp_ms(),
                            );
                            if let Err(e) = f.write_all(&graphj::encode_header(&header)) {
                                error!("Failed to write .graphj header: {e}");
                                continue;
                            }

                            info!(segment = %path.display(), "Journal opened segment");
                            current_file = Some(f);
                            current_size = graphj::HEADER_SIZE as u64;
                            segment_first_seq = new_seq;
                            segment_last_seq = 0;
                            segment_entry_count = 0;
                            body_hasher = Some(Sha256::new());
                        }
                        Err(e) => {
                            error!("Failed to create journal segment: {e}");
                            continue;
                        }
                    }
                }

                let file = current_file.as_mut().unwrap();

                if let Err(e) = file.write_all(&buf) {
                    error!("Journal write error at seq {new_seq}: {e}");
                    // State NOT updated — next write retries with same sequence.
                    continue;
                }

                // Update body hasher with entry bytes.
                if let Some(ref mut hasher) = body_hasher {
                    hasher.update(&buf);
                }

                // Only update state after successful write.
                state.sequence.store(new_seq, Ordering::SeqCst);
                *chain = new_hash;
                drop(chain);
                current_size += buf.len() as u64;
                segment_last_seq = new_seq;
                segment_entry_count += 1;
            }
            Ok(JournalCommand::Flush(ack)) => {
                if let Some(ref mut f) = current_file {
                    if let Err(e) = f.sync_all() {
                        error!("Journal flush sync_all failed: {e}");
                    }
                }
                let _ = ack.send(());
            }
            Ok(JournalCommand::SealForUpload(ack)) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - graphj::HEADER_SIZE as u64;
                        let hash = *state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(e) = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                            hash,
                        ) {
                            error!("Failed to seal segment for upload: {e}");
                            let _ = ack.send(None);
                            continue;
                        }
                        let path = journal_dir.join(format!(
                            "journal-{:016}.graphj",
                            segment_first_seq
                        ));
                        info!(segment = %path.display(), "Journal sealed segment for upload");
                        let _ = ack.send(Some(path));
                        // Clear current file so next write opens a new segment.
                        current_file = None;
                        current_size = 0;
                        segment_first_seq = 0;
                        segment_last_seq = 0;
                        segment_entry_count = 0;
                    } else {
                        // No hasher means segment was already sealed or empty header only.
                        let _ = ack.send(None);
                    }
                } else {
                    let _ = ack.send(None);
                }
            }
            Ok(JournalCommand::Shutdown) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - graphj::HEADER_SIZE as u64;
                        let hash = *state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(e) = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                            hash,
                        ) {
                            error!("Failed to seal segment on shutdown: {e}");
                        }
                    }
                }
                break;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Some(ref mut f) = current_file {
                    if let Err(e) = f.sync_data() {
                        error!("Journal periodic sync_data failed: {e}");
                    }
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - graphj::HEADER_SIZE as u64;
                        let hash = *state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(e) = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                            hash,
                        ) {
                            error!("Failed to seal segment on disconnect: {e}");
                        }
                    }
                }
                break;
            }
        }
    }
}

// ─── Reader ───

pub struct JournalReaderEntry {
    pub sequence: u64,
    pub entry: proto::JournalEntry,
    pub chain_hash: [u8; 32],
}

pub struct JournalReader {
    segments: Vec<PathBuf>,
    seg_idx: usize,
    /// Streaming reader — either a BufReader<File> (raw/unsealed), a zstd decoder
    /// (compressed-only), or None if using cursor_reader for encrypted segments.
    reader: Option<Box<dyn Read>>,
    /// For sealed+encrypted .graphj files, we must decrypt the entire body first,
    /// then read entries from a cursor. Compressed-only uses streaming zstd instead.
    cursor_reader: Option<io::Cursor<Vec<u8>>>,
    running_hash: [u8; 32],
    start_seq: u64,
    encryption_key: Option<[u8; 32]>,
}

impl JournalReader {
    /// Open a journal reader starting from the beginning.
    pub fn open(journal_dir: &Path) -> Result<Self, String> {
        Self::from_sequence(journal_dir, 0, [0u8; 32])
    }

    /// Open a journal reader with an encryption key for sealed .graphj files.
    pub fn open_with_key(
        journal_dir: &Path,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        Self::from_sequence_with_key(journal_dir, 0, [0u8; 32], encryption_key)
    }

    /// Open a journal reader starting from a given sequence.
    /// Entries before `start_seq` are read and validated but not returned.
    pub fn from_sequence(
        journal_dir: &Path,
        start_seq: u64,
        initial_hash: [u8; 32],
    ) -> Result<Self, String> {
        Self::from_sequence_with_key(journal_dir, start_seq, initial_hash, None)
    }

    /// Open a journal reader starting from a given sequence with optional encryption key.
    pub fn from_sequence_with_key(
        journal_dir: &Path,
        start_seq: u64,
        initial_hash: [u8; 32],
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
            .map_err(|e| format!("Failed to read journal dir: {e}"))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .map_or(false, |ext| ext == "wal" || ext == "graphj")
                    && p.file_name()
                        .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
            })
            .collect();
        segments.sort();
        Ok(Self {
            segments,
            seg_idx: 0,
            reader: None,
            cursor_reader: None,
            running_hash: initial_hash,
            start_seq,
            encryption_key,
        })
    }
}

impl JournalReader {
    /// Process a decoded entry: validate chain, compute hash, decode protobuf.
    /// Returns Some(Ok(entry)) to yield, Some(Err) on error, None to skip (pre-start).
    fn process_entry(&mut self, decoded: DecodedEntry) -> Option<Result<JournalReaderEntry, String>> {
        if decoded.sequence < self.start_seq {
            let mut hasher = Sha256::new();
            hasher.update(decoded.prev_hash);
            hasher.update(&decoded.payload);
            self.running_hash = hasher.finalize().into();
            return None; // skip, keep iterating
        }

        if decoded.prev_hash != self.running_hash {
            return Some(Err(format!(
                "Chain hash mismatch at seq {}: expected {:x?}, got {:x?}",
                decoded.sequence,
                &self.running_hash[..4],
                &decoded.prev_hash[..4]
            )));
        }

        let mut hasher = Sha256::new();
        hasher.update(decoded.prev_hash);
        hasher.update(&decoded.payload);
        let new_hash: [u8; 32] = hasher.finalize().into();
        self.running_hash = new_hash;

        match proto::JournalEntry::decode(decoded.payload.as_slice()) {
            Ok(entry) => Some(Ok(JournalReaderEntry {
                sequence: decoded.sequence,
                entry,
                chain_hash: new_hash,
            })),
            Err(e) => Some(Err(format!(
                "Failed to decode journal entry at seq {}: {e}",
                decoded.sequence
            ))),
        }
    }

    /// Open a segment file, detecting format (.graphj vs legacy .wal).
    /// For sealed+compressed/encrypted .graphj, decodes body into cursor_reader.
    /// For raw .graphj or legacy .wal, sets up file reader at the right offset.
    fn open_segment(&mut self, path: &Path) -> Result<(), String> {
        let mut file = File::open(path)
            .map_err(|e| format!("Failed to open segment {}: {e}", path.display()))?;

        // Check magic bytes to detect format (only need first 6 bytes).
        let mut magic_buf = [0u8; 6];
        let is_graphj = match file.read_exact(&mut magic_buf) {
            Ok(()) => &magic_buf == graphj::MAGIC,
            Err(_) => false, // file too small, treat as legacy
        };

        // Seek back and re-read properly.
        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek: {e}"))?;

        if is_graphj {
            let hdr = graphj::read_header(&mut file)
                .map_err(|e| format!("Failed to read .graphj header: {e}"))?
                .ok_or_else(|| "Magic check inconsistency".to_string())?;

            if hdr.is_sealed() && hdr.is_encrypted() {
                // Encrypted: must decrypt entire body first, then decompress.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read sealed body: {e}"))?;
                let raw = graphj::decode_body(
                    &hdr,
                    &body,
                    self.encryption_key.as_ref(),
                )?;
                self.cursor_reader = Some(io::Cursor::new(raw));
                self.reader = None;
            } else if hdr.is_sealed() && hdr.is_compressed() {
                // Compressed-only: stream via zstd decoder (no full-body allocation).
                let take = file.take(hdr.body_len);
                let decoder = zstd::Decoder::new(take)
                    .map_err(|e| format!("Failed to create zstd decoder: {e}"))?;
                self.reader = Some(Box::new(decoder));
                self.cursor_reader = None;
            } else {
                // Unsealed or sealed-raw: reader is at offset 128 after read_header.
                self.reader = Some(Box::new(BufReader::with_capacity(1024 * 1024, file)));
                self.cursor_reader = None;
            }
        } else {
            // Legacy .wal: already at offset 0, read raw entries.
            self.reader = Some(Box::new(BufReader::with_capacity(1024 * 1024, file)));
            self.cursor_reader = None;
        }
        Ok(())
    }
}

impl Iterator for JournalReader {
    type Item = Result<JournalReaderEntry, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try reading from cursor (for decoded sealed .graphj files).
            if let Some(ref mut cursor) = self.cursor_reader {
                match read_entry_from(cursor) {
                    Ok(Some(decoded)) => {
                        match self.process_entry(decoded) {
                            Some(result) => return Some(result),
                            None => continue, // pre-start, skip
                        }
                    }
                    Ok(None) => {
                        self.cursor_reader = None;
                        self.seg_idx += 1;
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Try reading from file reader.
            if let Some(ref mut reader) = self.reader {
                match read_entry_from(reader) {
                    Ok(Some(decoded)) => {
                        match self.process_entry(decoded) {
                            Some(result) => return Some(result),
                            None => continue, // pre-start, skip
                        }
                    }
                    Ok(None) => {
                        self.reader = None;
                        self.seg_idx += 1;
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // No active reader — open next segment.
            if self.seg_idx >= self.segments.len() {
                return None;
            }
            let path = self.segments[self.seg_idx].clone();
            match self.open_segment(&path) {
                Ok(()) => {
                    // If open_segment set neither reader, skip this segment.
                    if self.reader.is_none() && self.cursor_reader.is_none() {
                        self.seg_idx += 1;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ─── Recovery ───

/// Recover the journal state (last sequence + chain hash) from sealed segments.
/// Uses the chain hash trailer in sealed .graphj files for O(1) recovery.
/// Falls back to recovery.json (snapshot bootstrap) or full O(N) scan.
/// Returns (0, [0u8; 32]) if the journal directory is empty or doesn't exist.

// ─── Recovery State (recovery.json — snapshot bootstrap only) ───

const RECOVERY_FILENAME: &str = "recovery.json";

/// Write recovery state to `{journal_dir}/recovery.json`.
/// Used ONLY by hakuzu snapshot bootstrap — sealed segments use the chain hash
/// trailer for recovery, which is atomic with the seal operation.
pub fn write_recovery_state(journal_dir: &Path, seq: u64, hash: [u8; 32]) -> Result<(), std::io::Error> {
    let path = journal_dir.join(RECOVERY_FILENAME);
    let json = format!(
        r#"{{"last_seq":{},"chain_hash":"{}"}}"#,
        seq,
        hex::encode(hash)
    );
    std::fs::write(&path, json.as_bytes())
}

/// Try to read cached recovery state from `{journal_dir}/recovery.json`.
/// Returns `None` if missing, corrupt, or unparseable.
fn read_recovery_state(journal_dir: &Path) -> Option<(u64, [u8; 32])> {
    let path = journal_dir.join(RECOVERY_FILENAME);
    let data = std::fs::read_to_string(&path).ok()?;

    let last_seq_str = data
        .split("\"last_seq\":")
        .nth(1)?
        .split([',', '}'])
        .next()?
        .trim();
    let last_seq: u64 = last_seq_str.parse().ok()?;

    let chain_hash_str = data
        .split("\"chain_hash\":\"")
        .nth(1)?
        .split('"')
        .next()?;
    let hash_bytes = hex::decode(chain_hash_str).ok()?;
    if hash_bytes.len() != 32 {
        return None;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&hash_bytes);

    Some((last_seq, hash))
}

pub fn recover_journal_state(journal_dir: &Path) -> Result<(u64, [u8; 32]), String> {
    if !journal_dir.exists() {
        return Ok((0, [0u8; 32]));
    }

    // Collect .graphj segments, sorted by name (ascending sequence).
    let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
        .map_err(|e| format!("Failed to read journal dir: {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map_or(false, |ext| ext == "wal" || ext == "graphj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();

    if segments.is_empty() {
        // No segments on disk. Check recovery.json for snapshot bootstrap case:
        // hakuzu writes recovery.json after extracting a snapshot, before any
        // journal segments exist.
        if let Some((cached_seq, cached_hash)) = read_recovery_state(journal_dir) {
            if cached_seq > 0 {
                info!(
                    "Recovery: using snapshot bootstrap state (seq={}, hash={})",
                    cached_seq,
                    hex::encode(cached_hash)
                );
                return Ok((cached_seq, cached_hash));
            }
        }
        return Ok((0, [0u8; 32]));
    }

    segments.sort();

    // Fast path: find the last sealed segment with a chain hash trailer.
    // Walk segments in reverse — the last sealed segment has the final state.
    for seg_path in segments.iter().rev() {
        if let Ok(mut f) = File::open(seg_path) {
            if let Ok(Some(hdr)) = graphj::read_header(&mut f) {
                if hdr.is_sealed() && hdr.has_chain_hash() {
                    match graphj::read_chain_hash_trailer(&mut f, &hdr) {
                        Ok(hash) => {
                            info!(
                                "Recovery: O(1) from trailer (seq={}, hash={})",
                                hdr.last_seq,
                                hex::encode(hash)
                            );
                            return Ok((hdr.last_seq, hash));
                        }
                        Err(e) => {
                            error!(
                                "Recovery: trailer read failed for {}: {e}",
                                seg_path.display()
                            );
                            // Fall through to try earlier segments or full scan.
                        }
                    }
                }
            }
        }
    }

    // Slow path: no segments have chain hash trailers (v1 segments).
    // Full scan is required to recompute the chain hash.
    info!("Recovery: no chain hash trailers found, falling back to full O(N) scan");
    recover_journal_state_full_scan(journal_dir)
}

/// Full O(N) scan of all journal segments to recover state.
fn recover_journal_state_full_scan(journal_dir: &Path) -> Result<(u64, [u8; 32]), String> {
    let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
        .map_err(|e| format!("Failed to read journal dir: {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map_or(false, |ext| ext == "wal" || ext == "graphj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();

    if segments.is_empty() {
        return Ok((0, [0u8; 32]));
    }

    segments.sort();

    // We need to compute the chain hash from the beginning because each entry's
    // hash depends on prev_hash. Read ALL segments to recover the full chain.
    let mut last_seq: u64 = 0;
    let mut running_hash = [0u8; 32];

    for segment_path in &segments {
        let mut file = File::open(segment_path)
            .map_err(|e| format!("Failed to open segment {}: {e}", segment_path.display()))?;

        // Detect .graphj format and read header.
        let hdr_opt = match graphj::read_header(&mut file) {
            Ok(hdr) => hdr, // reader is now at offset 128 if Some
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                error!(
                    "Skipping corrupted .graphj file (truncated header): {}",
                    segment_path.display()
                );
                continue;
            }
            Err(e) => return Err(format!("Failed to read {}: {e}", segment_path.display())),
        };

        // Build the appropriate reader based on segment format.
        let mut entry_reader: Box<dyn Read> = if let Some(ref hdr) = hdr_opt {
            if hdr.is_sealed() && hdr.is_encrypted() {
                // Encrypted: must decrypt entire body first, then decompress.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read encrypted body: {e}"))?;
                let raw = graphj::decode_body(hdr, &body, None)?;
                Box::new(io::Cursor::new(raw))
            } else if hdr.is_sealed() && hdr.is_compressed() {
                // Compressed-only: stream via zstd decoder.
                let take = file.take(hdr.body_len);
                let decoder = zstd::Decoder::new(take)
                    .map_err(|e| format!("Failed to create zstd decoder: {e}"))?;
                Box::new(decoder)
            } else {
                // Unsealed or sealed-raw: read directly.
                Box::new(BufReader::with_capacity(1024 * 1024, file))
            }
        } else {
            // Legacy .wal — seek back to 0.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Failed to seek: {e}"))?;
            Box::new(BufReader::with_capacity(1024 * 1024, file))
        };

        loop {
            match read_entry_from(&mut entry_reader) {
                Ok(Some(decoded)) => {
                    let mut hasher = Sha256::new();
                    hasher.update(decoded.prev_hash);
                    hasher.update(&decoded.payload);
                    running_hash = hasher.finalize().into();
                    last_seq = decoded.sequence;
                }
                Ok(None) => break, // EOF
                Err(e) => return Err(format!("Error reading journal during recovery: {e}")),
            }
        }
    }

    Ok((last_seq, running_hash))
}
