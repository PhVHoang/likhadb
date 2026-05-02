use std::io::{self, Read, Write};

use crc32fast::Hasher;

/// Write a length-prefixed, CRC32-checksummed frame.
///
/// Format: `[payload_len: u32 LE][crc32: u32 LE][payload: payload_len bytes]`
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let len = payload.len() as u32;
    let crc = checksum(payload);
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&crc.to_le_bytes())?;
    w.write_all(payload)
}

/// Read one frame.  Returns `None` when the stream ends cleanly at a frame
/// boundary *or* when the last frame is truncated / CRC-mismatched (crash at
/// tail).  Returns `Some(Err(_))` only for genuine mid-log I/O or CRC errors.
pub fn read_frame(r: &mut impl Read) -> io::Result<Option<(Vec<u8>, u32)>> {
    // Read the 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let payload_len = u32::from_le_bytes(len_buf) as usize;

    // Read the 4-byte stored CRC.
    let mut crc_buf = [0u8; 4];
    match r.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let stored_crc = u32::from_le_bytes(crc_buf);

    // Read the payload.
    let mut payload = vec![0u8; payload_len];
    match r.read_exact(&mut payload) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    Ok(Some((payload, stored_crc)))
}

pub fn checksum(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
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
    /// `(raw_payload_bytes, stored_crc)`
    type Item = io::Result<(Vec<u8>, u32)>;

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
