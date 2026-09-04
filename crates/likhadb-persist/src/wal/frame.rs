use std::io::{self, Read, Write};

use xxhash_rust::xxh64::xxh64;

pub const HEADER_BYTES: u64 = 12;

/// Write a length-prefixed, xxHash64-checksummed frame.
///
/// Format: `[payload_len: u32 LE][xxhash64: u64 LE][payload: payload_len bytes]`
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = payload.len() as u32;
    let hash = checksum(payload);
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&hash.to_le_bytes())?;
    w.write_all(payload)
}

/// Read one frame.  Returns `None` when the stream ends cleanly at a frame
/// boundary *or* when the last frame is truncated (crash at tail).
pub fn read_frame(r: &mut impl Read) -> io::Result<Option<(Vec<u8>, u64)>> {
    // Read the 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let payload_len = u32::from_le_bytes(len_buf) as usize;

    // Read the 8-byte stored xxHash64 checksum.
    let mut hash_buf = [0u8; 8];
    match r.read_exact(&mut hash_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let stored_hash = u64::from_le_bytes(hash_buf);

    // Read the payload.
    let mut payload = vec![0u8; payload_len];
    match r.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    Ok(Some((payload, stored_hash)))
}

pub fn checksum(data: &[u8]) -> u64 {
    xxh64(data, 0)
}

/// Iterator over frames in a WAL file.  Stops at the first truncated/corrupt
/// tail frame (sets `self.done = true`).  Mid-log CRC errors are surfaced as
/// `Err` items so callers can distinguish them from a clean end-of-log.
pub struct FrameIter<R> {
    reader: R,
    done: bool,
}

impl<R: Read> FrameIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            done: false,
        }
    }
}

impl<R: std::io::BufRead> FrameIter<R> {
    /// Returns `true` if there are unread bytes remaining in the stream.
    /// Used to distinguish a crash-truncated tail frame (EOF follows the
    /// corrupt frame) from genuine mid-log corruption (more data follows).
    pub fn has_remaining_bytes(&mut self) -> io::Result<bool> {
        Ok(!self.reader.fill_buf()?.is_empty())
    }
}

impl<R: Read> Iterator for FrameIter<R> {
    /// `(raw_payload_bytes, stored_xxhash64)`
    type Item = io::Result<(Vec<u8>, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match read_frame(&mut self.reader) {
            Ok(None) => {
                self.done = true;
                None
            }
            Ok(Some(frame)) => Some(Ok(frame)),
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::checksum;

    #[test]
    fn checksum_matches_xxhash64_seed_zero_test_vector() {
        assert_eq!(checksum(b""), 0xef46_db37_51d8_e999);
    }
}
