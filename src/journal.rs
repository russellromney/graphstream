//! Journal writer and reader for graphd's write-ahead journal.
//!
//! Uses hadb-changeset's `.hadbj` binary format for entries and segments.
//! Entry payload is protobuf-encoded (graphd.JournalEntry).

use crate::graphd as proto;
use crate::types::{param_values_to_map_entries, ParamValue};
use crate::current_timestamp_ms;
use hadb_changeset::journal::{
    compute_entry_hash, decode, decode_entry, decode_header, encode_entry,
    encode_header, hash_to_u64, seal, encode_compressed, JournalEntry as HjEntry,
    JournalHeader, ENTRY_HEADER_SIZE, HEADER_SIZE, HADBJ_MAGIC,
};
use prost::Message;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
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

// ─── Streaming entry reader (for unsealed segments) ───

/// Read a single hadbj entry from a streaming reader.
/// Returns None at EOF.
fn read_entry_from<R: Read>(reader: &mut R) -> Result<Option<HjEntry>, String> {
    // Read the fixed entry header (48 bytes).
    let mut hdr_buf = [0u8; ENTRY_HEADER_SIZE];
    match reader.read_exact(&mut hdr_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(format!("Failed to read entry header: {e}")),
    }

    // Peek payload_len from bytes 4..8 (BE).
    let payload_len = u32::from_be_bytes([hdr_buf[4], hdr_buf[5], hdr_buf[6], hdr_buf[7]]) as usize;

    // Read full entry: header + payload.
    let mut full = Vec::with_capacity(ENTRY_HEADER_SIZE + payload_len);
    full.extend_from_slice(&hdr_buf);
    let mut payload = vec![0u8; payload_len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| {
            let seq = u64::from_be_bytes(hdr_buf[8..16].try_into().expect("8 bytes"));
            format!("Truncated journal payload at seq {seq}: {e}")
        })?;
    full.extend_from_slice(&payload);

    let (entry, _consumed) = decode_entry(&full, 0)
        .map_err(|e| format!("decode_entry failed: {e}"))?;
    Ok(Some(entry))
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

/// Seal a .hadbj segment: read back entries, use hadb-changeset seal + encode_compressed,
/// rewrite the file atomically with fsync.
///
/// `prev_chain_hash` is the chain hash of the entry preceding this segment's first entry.
/// It is used to compute `prev_segment_checksum` for cross-segment integrity.
fn seal_segment(file: &mut File, prev_chain_hash: [u8; 32]) -> io::Result<()> {
    // Read the raw body (everything after the 128-byte header).
    let file_len = file.seek(SeekFrom::End(0))?;
    if file_len <= HEADER_SIZE as u64 {
        // Empty segment (header only), nothing to seal.
        return Ok(());
    }
    let raw_body_len = file_len - HEADER_SIZE as u64;
    file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
    let mut raw_body = vec![0u8; raw_body_len as usize];
    file.read_exact(&mut raw_body)?;

    // Decode entries from raw body.
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < raw_body.len() {
        let (entry, consumed) = decode_entry(&raw_body, offset)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode_entry during seal: {e}")))?;
        entries.push(entry);
        offset += consumed;
    }

    if entries.is_empty() {
        return Ok(());
    }

    // prev_segment_checksum: truncated hash of the chain state before this segment.
    let prev_segment_checksum = hash_to_u64(&prev_chain_hash);
    let segment = seal(entries, prev_segment_checksum);
    let encoded = encode_compressed(&segment, JOURNAL_ZSTD_LEVEL);

    // Rewrite the entire file: header + compressed body + chain hash trailer.
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&encoded)?;
    file.set_len(encoded.len() as u64)?;
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
    // Chain hash at the time this segment was opened (= chain hash of previous segment).
    // Used as prev_segment_checksum when sealing.
    let mut segment_prev_chain_hash: [u8; 32] = [0u8; 32];
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

                let new_hash = compute_entry_hash(&prev_hash, &payload);

                let hj_entry = HjEntry {
                    sequence: new_seq,
                    prev_hash,
                    payload: payload.clone(),
                };
                let buf = encode_entry(&hj_entry);

                // Rotate segment if needed.
                if current_file.is_none()
                    || current_size + buf.len() as u64 > segment_max_bytes
                {
                    // Seal the old segment before rotation.
                    if let Some(ref mut f) = current_file {
                        if let Err(e) = seal_segment(f, segment_prev_chain_hash) {
                            error!("Failed to seal segment on rotation: {e}");
                        }
                    }

                    // Create new .hadbj segment.
                    let path = journal_dir.join(format!("journal-{new_seq:016}.hadbj"));
                    match File::options().read(true).write(true).create(true).truncate(true).open(&path) {
                        Ok(mut f) => {
                            // Write unsealed header.
                            let header = JournalHeader {
                                flags: 0,
                                compression: 0,
                                first_seq: new_seq,
                                last_seq: 0,
                                entry_count: 0,
                                body_len: 0,
                                body_checksum: [0u8; 32],
                                prev_segment_checksum: 0,
                                created_ms: current_timestamp_ms(),
                            };
                            if let Err(e) = f.write_all(&encode_header(&header)) {
                                error!("Failed to write .hadbj header: {e}");
                                continue;
                            }

                            info!(segment = %path.display(), "Journal opened segment");
                            current_file = Some(f);
                            current_size = HEADER_SIZE as u64;
                            segment_first_seq = new_seq;
                            segment_prev_chain_hash = prev_hash;
                        }
                        Err(e) => {
                            error!("Failed to create journal segment: {e}");
                            continue;
                        }
                    }
                }

                let file = current_file.as_mut().expect("file must be open");

                if let Err(e) = file.write_all(&buf) {
                    error!("Journal write error at seq {new_seq}: {e}");
                    continue;
                }

                // Only update state after successful write.
                state.sequence.store(new_seq, Ordering::SeqCst);
                *chain = new_hash;
                drop(chain);
                current_size += buf.len() as u64;
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
                    if let Err(e) = seal_segment(f, segment_prev_chain_hash) {
                        error!("Failed to seal segment for upload: {e}");
                        let _ = ack.send(None);
                        continue;
                    }
                    let path = journal_dir.join(format!(
                        "journal-{:016}.hadbj",
                        segment_first_seq
                    ));
                    info!(segment = %path.display(), "Journal sealed segment for upload");
                    let _ = ack.send(Some(path));
                    // Clear current file so next write opens a new segment.
                    current_file = None;
                    current_size = 0;
                    segment_first_seq = 0;
                } else {
                    let _ = ack.send(None);
                }
            }
            Ok(JournalCommand::Shutdown) => {
                if let Some(ref mut f) = current_file {
                    if let Err(e) = seal_segment(f, segment_prev_chain_hash) {
                        error!("Failed to seal segment on shutdown: {e}");
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
                    if let Err(e) = seal_segment(f, segment_prev_chain_hash) {
                        error!("Failed to seal segment on disconnect: {e}");
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
    /// Streaming reader for unsealed segments.
    reader: Option<Box<dyn Read>>,
    /// For sealed .hadbj files, we decode all entries up front via hadb-changeset::decode.
    decoded_entries: Option<Vec<HjEntry>>,
    decoded_idx: usize,
    running_hash: [u8; 32],
    start_seq: u64,
    encryption_key: Option<[u8; 32]>,
}

impl JournalReader {
    /// Open a journal reader starting from the beginning.
    pub fn open(journal_dir: &Path) -> Result<Self, String> {
        Self::from_sequence(journal_dir, 0, [0u8; 32])
    }

    /// Open a journal reader with an encryption key for sealed segments.
    pub fn open_with_key(
        journal_dir: &Path,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        Self::from_sequence_with_key(journal_dir, 0, [0u8; 32], encryption_key)
    }

    /// Open a journal reader starting from a given sequence.
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
                    .map_or(false, |ext| ext == "hadbj")
                    && p.file_name()
                        .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
            })
            .collect();
        segments.sort();
        Ok(Self {
            segments,
            seg_idx: 0,
            reader: None,
            decoded_entries: None,
            decoded_idx: 0,
            running_hash: initial_hash,
            start_seq,
            encryption_key,
        })
    }
}

impl JournalReader {
    /// Process a decoded entry: validate chain, compute hash, decode protobuf.
    fn process_entry(&mut self, entry: HjEntry) -> Option<Result<JournalReaderEntry, String>> {
        if entry.sequence < self.start_seq {
            self.running_hash = compute_entry_hash(&entry.prev_hash, &entry.payload);
            return None; // skip, keep iterating
        }

        if entry.prev_hash != self.running_hash {
            return Some(Err(format!(
                "Chain hash mismatch at seq {}: expected {:x?}, got {:x?}",
                entry.sequence,
                &self.running_hash[..4],
                &entry.prev_hash[..4]
            )));
        }

        let new_hash = compute_entry_hash(&entry.prev_hash, &entry.payload);
        self.running_hash = new_hash;

        match proto::JournalEntry::decode(entry.payload.as_slice()) {
            Ok(proto_entry) => Some(Ok(JournalReaderEntry {
                sequence: entry.sequence,
                entry: proto_entry,
                chain_hash: new_hash,
            })),
            Err(e) => Some(Err(format!(
                "Failed to decode journal entry at seq {}: {e}",
                entry.sequence
            ))),
        }
    }

    /// Open a segment file, detecting format.
    /// For sealed .hadbj, decodes all entries via hadb-changeset::decode.
    /// For unsealed .hadbj, sets up streaming reader at offset 128.
    fn open_segment(&mut self, path: &Path) -> Result<(), String> {
        let file_bytes = std::fs::read(path)
            .map_err(|e| format!("Failed to read segment {}: {e}", path.display()))?;

        if file_bytes.len() < HEADER_SIZE {
            return Err(format!("Segment too small: {}", path.display()));
        }

        // Check for HADBJ magic.
        if file_bytes[0..5] != HADBJ_MAGIC {
            return Err(format!("Not a .hadbj file: {}", path.display()));
        }

        let header = decode_header(&file_bytes)
            .map_err(|e| format!("Failed to decode header {}: {e}", path.display()))?;

        if header.is_sealed() {
            // Encryption is layered on top: if encryption_key is set and magic
            // does NOT match HADBJ, the file is encrypted. Decrypt the entire
            // blob first, then decode. When magic matches, file is plaintext.
            // Currently encryption is not implemented in hadbj; this is a
            // forward-compatible hook for when graphstream adds encrypt-after-seal.
            let segment = decode(&file_bytes)
                .map_err(|e| format!("Failed to decode sealed segment {}: {e}", path.display()))?;

            self.decoded_entries = Some(segment.entries);
            self.decoded_idx = 0;
            self.reader = None;
        } else {
            // Unsealed: stream entries from offset 128.
            let body = file_bytes[HEADER_SIZE..].to_vec();
            self.reader = Some(Box::new(io::Cursor::new(body)));
            self.decoded_entries = None;
        }
        Ok(())
    }
}

impl Iterator for JournalReader {
    type Item = Result<JournalReaderEntry, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try reading from decoded entries (sealed segments).
            if let Some(ref entries) = self.decoded_entries {
                if self.decoded_idx < entries.len() {
                    let entry = entries[self.decoded_idx].clone();
                    self.decoded_idx += 1;
                    match self.process_entry(entry) {
                        Some(result) => return Some(result),
                        None => continue, // pre-start, skip
                    }
                } else {
                    self.decoded_entries = None;
                    self.seg_idx += 1;
                    continue;
                }
            }

            // Try reading from streaming reader (unsealed segments).
            if let Some(ref mut reader) = self.reader {
                match read_entry_from(reader) {
                    Ok(Some(entry)) => {
                        match self.process_entry(entry) {
                            Some(result) => return Some(result),
                            None => continue,
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

            // No active reader, open next segment.
            if self.seg_idx >= self.segments.len() {
                return None;
            }
            let path = self.segments[self.seg_idx].clone();
            match self.open_segment(&path) {
                Ok(()) => {
                    if self.reader.is_none() && self.decoded_entries.is_none() {
                        self.seg_idx += 1;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ─── Recovery ───

const RECOVERY_FILENAME: &str = "recovery.json";

/// Write recovery state to `{journal_dir}/recovery.json`.
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

    let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
        .map_err(|e| format!("Failed to read journal dir: {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map_or(false, |ext| ext == "hadbj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();

    if segments.is_empty() {
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
    for seg_path in segments.iter().rev() {
        match crate::format::read_header_from_path(seg_path) {
            Ok(hdr) => {
                if hdr.is_sealed() && hdr.has_chain_hash() {
                    match crate::format::read_chain_hash_from_path(seg_path, &hdr) {
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
                        }
                    }
                }
            }
            Err(e) => {
                error!("Recovery: header read failed for {}: {e}", seg_path.display());
            }
        }
    }

    // Slow path: full scan to recompute chain hash.
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
                .map_or(false, |ext| ext == "hadbj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();

    if segments.is_empty() {
        return Ok((0, [0u8; 32]));
    }

    segments.sort();

    let mut last_seq: u64 = 0;
    let mut running_hash = [0u8; 32];

    for segment_path in &segments {
        let file_bytes = std::fs::read(segment_path)
            .map_err(|e| format!("Failed to read {}: {e}", segment_path.display()))?;

        if file_bytes.len() < HEADER_SIZE {
            error!("Skipping truncated segment: {}", segment_path.display());
            continue;
        }

        if file_bytes[0..5] != HADBJ_MAGIC {
            error!("Skipping non-hadbj segment: {}", segment_path.display());
            continue;
        }

        let header = match decode_header(&file_bytes) {
            Ok(h) => h,
            Err(e) => {
                error!("Skipping corrupt header in {}: {e}", segment_path.display());
                continue;
            }
        };

        if header.is_sealed() {
            // Decode sealed segment via hadb-changeset.
            match decode(&file_bytes) {
                Ok(segment) => {
                    for entry in &segment.entries {
                        running_hash = compute_entry_hash(&entry.prev_hash, &entry.payload);
                        last_seq = entry.sequence;
                    }
                }
                Err(e) => {
                    return Err(format!("Error decoding segment {}: {e}", segment_path.display()));
                }
            }
        } else {
            // Unsealed: stream entries from body.
            let body = &file_bytes[HEADER_SIZE..];
            let mut offset = 0;
            while offset < body.len() {
                match decode_entry(body, offset) {
                    Ok((entry, consumed)) => {
                        running_hash = compute_entry_hash(&entry.prev_hash, &entry.payload);
                        last_seq = entry.sequence;
                        offset += consumed;
                    }
                    Err(_) => break, // truncated entry at end
                }
            }
        }
    }

    Ok((last_seq, running_hash))
}
