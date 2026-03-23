//! `.graphj` file format — universal journal file format for graphd.
//!
//! # Overview
//!
//! GraphJ (Graph Journal) is a container format for graphd's write-ahead journal.
//! One format is used for:
//! - Live journal segments (written entry-by-entry, fsynced)
//! - Compacted archives (multiple segments merged)
//! - S3/cold storage uploads
//! - Point-in-time restore inputs
//!
//! # Binary Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                        128-byte Fixed Header                         │
//! ├─────────┬──────┬────────────────────────────────────────────────────┤
//! │ Offset  │ Size │ Field                                               │
//! ├─────────┼──────┼────────────────────────────────────────────────────┤
//! │ 0-5     │ 6    │ magic             b"GRAPHJ"                         │
//! │ 6       │ 1    │ nul               0x00 separator                    │
//! │ 7       │ 1    │ version           0x01 (format version)             │
//! │ 8       │ 1    │ flags             SEALED | COMPRESSED | ENCRYPTED   │
//! │ 9       │ 1    │ compression       0=none, 1=zstd                    │
//! │ 10      │ 1    │ encryption        0=none, 1=xchacha20poly1305      │
//! │ 11      │ 1    │ reserved          0x00                              │
//! │ 12-15   │ 4    │ zstd_level        i32 LE (0 if uncompressed)       │
//! │ 16-23   │ 8    │ first_seq         u64 LE (first entry sequence)    │
//! │ 24-31   │ 8    │ last_seq          u64 LE (0 if unsealed)           │
//! │ 32-39   │ 8    │ entry_count       u64 LE (0 if unsealed)           │
//! │ 40-47   │ 8    │ body_len          u64 LE (0 if unsealed)           │
//! │ 48-79   │ 32   │ body_checksum     SHA-256 of body (zeros if unsealed) │
//! │ 80-103  │ 24   │ nonce             XChaCha20-Poly1305 nonce (zeros if unencrypted) │
//! │ 104-111 │ 8    │ created_ms        i64 LE (epoch milliseconds)      │
//! │ 112-127 │ 16   │ reserved          zeros (future use)                │
//! ├─────────┴──────┴────────────────────────────────────────────────────┤
//! │                        Variable-Length Body                          │
//! │    Raw journal entries (or compressed/encrypted transformation)      │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │               Optional 32-byte Chain Hash Trailer                    │
//! │    Present when FLAG_HAS_CHAIN_HASH (0x08) is set in flags.          │
//! │    Contains the running SHA-256 chain hash after the last entry.     │
//! │    Written atomically with the seal (header+body+trailer+fsync).     │
//! │    Enables O(1) recovery: read header.last_seq + trailer hash.       │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Flags (offset 8)
//!
//! - **SEALED (0x04)**: File is finalized. `last_seq`, `entry_count`, `body_len`,
//!   and `body_checksum` are valid. Unsealed files have these set to zero and
//!   readers scan to EOF.
//! - **COMPRESSED (0x01)**: Body is zstd-compressed. Only valid on sealed files.
//! - **ENCRYPTED (0x02)**: Body is XChaCha20-Poly1305 encrypted. Only valid on
//!   sealed files.
//! - **HAS_CHAIN_HASH (0x08)**: 32-byte chain hash trailer follows the body.
//!   Enables O(1) journal recovery without scanning entries.
//!
//! # Body Format
//!
//! For **unsealed** files (live segments):
//! - Raw concatenated journal entries, each with structure:
//!   `[4B CRC32C LE][4B payload_len LE][8B sequence LE][32B prev_hash SHA256][protobuf payload]`
//! - Entries are written incrementally and fsynced periodically.
//!
//! For **sealed** files:
//! - Raw: same as unsealed
//! - Compressed: `zstd_compress(raw_entries)`
//! - Encrypted: `XChaCha20Poly1305::encrypt(nonce, key, AAD, optionally_compressed_entries)`
//!   - 16-byte Poly1305 auth tag is appended to ciphertext (included in `body_len`)
//!   - AAD = header bytes [0..40) — covers stable fields only
//!
//! # Processing Order
//!
//! **Encoding:** compress → encrypt
//! **Decoding:** decrypt → decompress

use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce,
};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::current_timestamp_ms;

// ─── Constants ───

pub const MAGIC: &[u8; 6] = b"GRAPHJ";
pub const NUL: u8 = 0x00;
pub const VERSION: u8 = 0x01;
pub const HEADER_SIZE: usize = 128;
/// AAD covers header bytes [0..40) — magic, version, flags, algorithms,
/// first_seq, last_seq, entry_count. Excludes body_len and body_checksum
/// which aren't known until after encryption.
pub const AAD_LEN: usize = 40;

pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_ENCRYPTED: u8 = 0x02;
pub const FLAG_SEALED: u8 = 0x04;
pub const FLAG_HAS_CHAIN_HASH: u8 = 0x08;

pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_ZSTD: u8 = 1;

pub const ENCRYPTION_NONE: u8 = 0;
pub const ENCRYPTION_XCHACHA20POLY1305: u8 = 1;

/// Size of the chain hash trailer appended after the body in sealed segments.
pub const CHAIN_HASH_TRAILER_SIZE: usize = 32;

// ─── Header ───

#[derive(Debug, Clone)]
pub struct GraphjHeader {
    pub flags: u8,
    pub compression: u8,
    pub encryption: u8,
    pub zstd_level: i32,
    pub first_seq: u64,
    pub last_seq: u64,
    pub entry_count: u64,
    pub body_len: u64,
    pub body_checksum: [u8; 32],
    pub nonce: [u8; 24],
    pub created_ms: i64,
}

impl GraphjHeader {
    /// Create an unsealed header for a new live segment.
    pub fn new_unsealed(first_seq: u64, created_ms: i64) -> Self {
        Self {
            flags: 0,
            compression: COMPRESSION_NONE,
            encryption: ENCRYPTION_NONE,
            zstd_level: 0,
            first_seq,
            last_seq: 0,
            entry_count: 0,
            body_len: 0,
            body_checksum: [0u8; 32],
            nonce: [0u8; 24],
            created_ms,
        }
    }

    pub fn is_sealed(&self) -> bool {
        self.flags & FLAG_SEALED != 0
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }

    pub fn is_encrypted(&self) -> bool {
        self.flags & FLAG_ENCRYPTED != 0
    }

    pub fn has_chain_hash(&self) -> bool {
        self.flags & FLAG_HAS_CHAIN_HASH != 0
    }
}

/// Encode a header into a 128-byte array.
pub fn encode_header(h: &GraphjHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..6].copy_from_slice(MAGIC);
    buf[6] = NUL;
    buf[7] = VERSION;
    buf[8] = h.flags;
    buf[9] = h.compression;
    buf[10] = h.encryption;
    // buf[11] reserved
    buf[12..16].copy_from_slice(&h.zstd_level.to_le_bytes());
    buf[16..24].copy_from_slice(&h.first_seq.to_le_bytes());
    buf[24..32].copy_from_slice(&h.last_seq.to_le_bytes());
    buf[32..40].copy_from_slice(&h.entry_count.to_le_bytes());
    buf[40..48].copy_from_slice(&h.body_len.to_le_bytes());
    buf[48..80].copy_from_slice(&h.body_checksum);
    buf[80..104].copy_from_slice(&h.nonce);
    buf[104..112].copy_from_slice(&h.created_ms.to_le_bytes());
    // buf[112..128] reserved
    buf
}

/// Try to read and parse a `.graphj` header from a reader.
/// Returns `Ok(Some(header))` if magic matches, `Ok(None)` if it doesn't
/// (indicating a legacy `.wal` file). The reader is advanced by 128 bytes
/// on success, or left at an indeterminate position on `None` (caller
/// should seek back to 0).
pub fn read_header<R: Read>(r: &mut R) -> io::Result<Option<GraphjHeader>> {
    let mut buf = [0u8; HEADER_SIZE];
    r.read_exact(&mut buf)?;
    if &buf[0..6] != MAGIC {
        return Ok(None);
    }
    // version check
    let _version = buf[7];
    let flags = buf[8];
    let compression = buf[9];
    let encryption = buf[10];
    let zstd_level = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let first_seq = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let last_seq = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    let entry_count = u64::from_le_bytes(buf[32..40].try_into().unwrap());
    let body_len = u64::from_le_bytes(buf[40..48].try_into().unwrap());
    let mut body_checksum = [0u8; 32];
    body_checksum.copy_from_slice(&buf[48..80]);
    let mut nonce = [0u8; 24];
    nonce.copy_from_slice(&buf[80..104]);
    let created_ms = i64::from_le_bytes(buf[104..112].try_into().unwrap());

    Ok(Some(GraphjHeader {
        flags,
        compression,
        encryption,
        zstd_level,
        first_seq,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        nonce,
        created_ms,
    }))
}

/// Read the 32-byte chain hash trailer from a sealed `.graphj` file.
/// The file must have `FLAG_HAS_CHAIN_HASH` set and be sealed.
/// Trailer is at offset `HEADER_SIZE + body_len`.
pub fn read_chain_hash_trailer<RS: Read + Seek>(
    r: &mut RS,
    header: &GraphjHeader,
) -> io::Result<[u8; 32]> {
    if !header.has_chain_hash() || !header.is_sealed() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "No chain hash trailer (flag not set or file not sealed)",
        ));
    }
    let trailer_offset = HEADER_SIZE as u64 + header.body_len;
    r.seek(SeekFrom::Start(trailer_offset))?;
    let mut hash = [0u8; 32];
    r.read_exact(&mut hash)?;
    Ok(hash)
}

// ─── Seal ───

/// Seal a live `.graphj` file: scan entries from offset 128, compute final
/// header fields, rewrite header with `FLAG_SEALED`.
pub fn seal_file(path: &Path) -> Result<GraphjHeader, String> {
    use std::fs::OpenOptions;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("Failed to open file for sealing: {e}"))?;

    // Read existing header.
    let header = read_header(&mut file)
        .map_err(|e| format!("Failed to read header: {e}"))?
        .ok_or_else(|| "Not a .graphj file (magic mismatch)".to_string())?;

    if header.is_sealed() {
        return Ok(header);
    }

    // Scan body to compute last_seq, entry_count, body_len, body_checksum.
    let mut body_hasher = Sha256::new();
    let mut entry_count: u64 = 0;
    let mut last_seq: u64 = header.first_seq;
    let mut body_len: u64 = 0;

    loop {
        let mut entry_header = [0u8; 48]; // FIXED_HEADER from journal.rs
        match file.read_exact(&mut entry_header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("Error reading entry during seal: {e}")),
        }

        let payload_len =
            u32::from_le_bytes([entry_header[4], entry_header[5], entry_header[6], entry_header[7]])
                as usize;
        let sequence = u64::from_le_bytes(entry_header[8..16].try_into().unwrap());

        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)
            .map_err(|e| format!("Truncated entry during seal at seq {sequence}: {e}"))?;

        // Hash the raw entry bytes.
        body_hasher.update(&entry_header);
        body_hasher.update(&payload);

        last_seq = sequence;
        entry_count += 1;
        body_len += (48 + payload_len) as u64;
    }

    let body_checksum: [u8; 32] = body_hasher.finalize().into();

    let sealed = GraphjHeader {
        flags: header.flags | FLAG_SEALED,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        ..header
    };

    // Rewrite header.
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("Failed to seek for seal: {e}"))?;
    file.write_all(&encode_header(&sealed))
        .map_err(|e| format!("Failed to write sealed header: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync sealed file: {e}"))?;

    Ok(sealed)
}

// ─── Decode body ───

/// Decrypt and/or decompress a sealed `.graphj` body, returning raw entry bytes.
pub fn decode_body(
    header: &GraphjHeader,
    body: &[u8],
    encryption_key: Option<&[u8; 32]>,
) -> Result<Vec<u8>, String> {
    let mut data = body.to_vec();

    // Decrypt first (reverse of compress-then-encrypt).
    if header.is_encrypted() {
        let key = encryption_key.ok_or("Encrypted .graphj file but no encryption key provided")?;
        let cipher = XChaCha20Poly1305::new(key.into());
        let nonce = XNonce::from_slice(&header.nonce);
        let aad = &encode_header(header)[..AAD_LEN];
        data = cipher
            .decrypt(nonce, Payload { msg: &data, aad })
            .map_err(|e| format!("Decryption failed: {e}"))?;
    }

    // Decompress.
    if header.is_compressed() {
        data = zstd::decode_all(data.as_slice())
            .map_err(|e| format!("Decompression failed: {e}"))?;
    }

    Ok(data)
}

// ─── Compact ───

/// Compact N input files into a single sealed `.graphj` file.
/// Reads raw entries from all inputs (handles both `.wal` and `.graphj`),
/// optionally compresses and encrypts, writes sealed output.
pub fn compact(
    inputs: &[impl AsRef<Path>],
    output: &Path,
    compress: bool,
    zstd_level: i32,
    encryption_key: Option<&[u8; 32]>,
) -> Result<GraphjHeader, String> {
    // Collect all raw entry bytes from input files.
    let mut raw_entries = Vec::new();
    let mut first_seq: u64 = u64::MAX;
    let mut last_seq: u64 = 0;
    let mut entry_count: u64 = 0;

    for input in inputs {
        let input = input.as_ref();
        let mut file = std::fs::File::open(input)
            .map_err(|e| format!("Failed to open {}: {e}", input.display()))?;

        // Detect format.
        let mut magic_buf = [0u8; 8];
        let bytes_read = file
            .read(&mut magic_buf)
            .map_err(|e| format!("Failed to read {}: {e}", input.display()))?;

        if bytes_read >= 6 && &magic_buf[0..6] == MAGIC {
            // .graphj file — read header, then body.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Seek failed: {e}"))?;
            let hdr = read_header(&mut file)
                .map_err(|e| format!("Failed to read header from {}: {e}", input.display()))?
                .ok_or_else(|| "Magic check inconsistency".to_string())?;

            if hdr.is_sealed() && (hdr.is_compressed() || hdr.is_encrypted()) {
                // Need to decode the body first.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read sealed body: {e}"))?;
                let decoded = decode_body(&hdr, &body, encryption_key)?;
                raw_entries.extend_from_slice(&decoded);
            } else {
                // Unsealed or sealed-raw: read entries directly.
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .map_err(|e| format!("Failed to read entries: {e}"))?;
                raw_entries.extend_from_slice(&buf);
            }
        } else {
            // Legacy .wal — entire file is raw entries.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Seek failed: {e}"))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .map_err(|e| format!("Failed to read .wal: {e}"))?;
            raw_entries.extend_from_slice(&buf);
        }
    }

    // Scan raw entries to get first_seq, last_seq, entry_count.
    {
        let mut cursor = io::Cursor::new(&raw_entries);
        loop {
            let mut hdr = [0u8; 48];
            match cursor.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Error scanning entries: {e}")),
            }
            let payload_len =
                u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
            let seq = u64::from_le_bytes(hdr[8..16].try_into().unwrap());

            // Skip payload.
            let pos = cursor.position() + payload_len as u64;
            cursor.set_position(pos);

            if seq < first_seq {
                first_seq = seq;
            }
            if seq > last_seq {
                last_seq = seq;
            }
            entry_count += 1;
        }
    }

    if entry_count == 0 {
        first_seq = 0;
    }

    // Process body: compress then encrypt.
    let mut body = raw_entries;

    let compression_algo = if compress {
        body = zstd::encode_all(body.as_slice(), zstd_level)
            .map_err(|e| format!("Compression failed: {e}"))?;
        COMPRESSION_ZSTD
    } else {
        COMPRESSION_NONE
    };

    let mut flags = FLAG_SEALED;
    if compress {
        flags |= FLAG_COMPRESSED;
    }

    let mut nonce = [0u8; 24];
    let encryption_algo;

    if let Some(key) = encryption_key {
        flags |= FLAG_ENCRYPTED;
        encryption_algo = ENCRYPTION_XCHACHA20POLY1305;

        // Generate random nonce.
        use rand::RngCore;
        rand::rng().fill_bytes(&mut nonce);

        // Build AAD from stable header fields (offsets 0..40).
        let tmp_header = GraphjHeader {
            flags,
            compression: compression_algo,
            encryption: encryption_algo,
            zstd_level: if compress { zstd_level } else { 0 },
            first_seq,
            last_seq,
            entry_count,
            body_len: 0,
            body_checksum: [0u8; 32],
            nonce,
            created_ms: current_timestamp_ms(),
        };
        let header_bytes = encode_header(&tmp_header);
        let aad = &header_bytes[..AAD_LEN];

        let cipher = XChaCha20Poly1305::new(key.into());
        let nonce_ref = XNonce::from_slice(&nonce);
        body = cipher
            .encrypt(nonce_ref, Payload { msg: &body, aad })
            .map_err(|e| format!("Encryption failed: {e}"))?;
    } else {
        encryption_algo = ENCRYPTION_NONE;
    }

    let body_checksum: [u8; 32] = Sha256::digest(&body).into();
    let body_len = body.len() as u64;

    let header = GraphjHeader {
        flags,
        compression: compression_algo,
        encryption: encryption_algo,
        zstd_level: if compress { zstd_level } else { 0 },
        first_seq,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        nonce,
        created_ms: current_timestamp_ms(),
    };

    // Write output.
    let mut out = std::fs::File::create(output)
        .map_err(|e| format!("Failed to create output file: {e}"))?;
    out.write_all(&encode_header(&header))
        .map_err(|e| format!("Failed to write header: {e}"))?;
    out.write_all(&body)
        .map_err(|e| format!("Failed to write body: {e}"))?;
    out.sync_all()
        .map_err(|e| format!("Failed to sync output: {e}"))?;

    Ok(header)
}
