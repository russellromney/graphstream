//! Thin bridge to hadb-changeset's journal format.
//!
//! Provides file-level helpers that graphstream needs beyond the raw
//! encode/decode functions in hadb_changeset::journal.

use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use hadb_changeset::journal::{
    decode_header, JournalHeader, CHAIN_HASH_TRAILER_SIZE, HEADER_SIZE,
};

/// Check if a `.hadbj` file on disk is sealed by reading its header.
/// Returns `false` for missing, empty, corrupt, or unsealed files.
pub fn is_file_sealed(path: &Path) -> Result<bool, String> {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => return Err(format!("Open {}: {e}", path.display())),
    };
    let mut buf = [0u8; HEADER_SIZE];
    match f.read_exact(&mut buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(format!("Read header {}: {e}", path.display())),
    }
    match decode_header(&buf) {
        Ok(header) => Ok(header.is_sealed()),
        Err(_) => Ok(false),
    }
}

/// Read and parse a `.hadbj` header from a file path.
pub fn read_header_from_path(path: &Path) -> Result<JournalHeader, String> {
    let mut f = std::fs::File::open(path)
        .map_err(|e| format!("Open {}: {e}", path.display()))?;
    let mut buf = [0u8; HEADER_SIZE];
    f.read_exact(&mut buf)
        .map_err(|e| format!("Read header {}: {e}", path.display()))?;
    decode_header(&buf).map_err(|e| format!("Decode header {}: {e}", path.display()))
}

/// Read the 32-byte chain hash trailer from a sealed `.hadbj` file.
/// The header must have `FLAG_HAS_CHAIN_HASH` set and be sealed.
/// Trailer is at offset `HEADER_SIZE + body_len`.
pub fn read_chain_hash_from_path(
    path: &Path,
    header: &JournalHeader,
) -> Result<[u8; 32], String> {
    if !header.has_chain_hash() || !header.is_sealed() {
        return Err("No chain hash trailer (flag not set or file not sealed)".into());
    }
    let mut f = std::fs::File::open(path)
        .map_err(|e| format!("Open {}: {e}", path.display()))?;
    let trailer_offset = HEADER_SIZE as u64 + header.body_len;
    f.seek(SeekFrom::Start(trailer_offset))
        .map_err(|e| format!("Seek to trailer {}: {e}", path.display()))?;
    let mut hash = [0u8; CHAIN_HASH_TRAILER_SIZE];
    f.read_exact(&mut hash)
        .map_err(|e| format!("Read trailer {}: {e}", path.display()))?;
    Ok(hash)
}
