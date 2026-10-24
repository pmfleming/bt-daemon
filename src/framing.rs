//! Shared byte-stuffing and delimiter handling; payload validation stays vendor-specific.
use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Buffers decoded messages across reads. Corrupt frames are reported and skipped;
/// transport errors propagate, and queued messages are delivered before EOF.
/// Keeping the decoder and error policy in the caller preserves vendor semantics.
pub(crate) struct Reader<R, T, D, E> {
    reader: R,
    decode: D,
    invalid: E,
    pending: std::vec::IntoIter<Result<T>>,
}

impl<R, T, D, E> Reader<R, T, D, E>
where
    R: AsyncRead + Unpin,
    D: FnMut(&[u8]) -> Vec<Result<T>>,
    E: FnMut(anyhow::Error),
{
    pub(crate) fn new(reader: R, decode: D, invalid: E) -> Self {
        Self {
            reader,
            decode,
            invalid,
            pending: Vec::new().into_iter(),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<T>> {
        let mut bytes = [0_u8; 512];
        loop {
            if let Some(frame) = self
                .pending
                .find_map(|frame| frame.map_err(&mut self.invalid).ok())
            {
                return Ok(Some(frame));
            }
            let count = self.reader.read(&mut bytes).await?;
            if count == 0 {
                return Ok(None);
            }
            self.pending = (self.decode)(&bytes[..count]).into_iter();
        }
    }
}

// Opening delimiter plus encoded body; the closing delimiter is not counted.
const MAX_FRAME: usize = 4096;

pub(crate) fn escape(
    body: &[u8],
    markers: (u8, u8),
    escape: u8,
    encode: impl Fn(u8) -> u8,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 4);
    frame.push(markers.0);
    for &byte in body {
        if [markers.0, markers.1, escape].contains(&byte) {
            frame.extend_from_slice(&[escape, encode(byte)]);
        } else {
            frame.push(byte);
        }
    }
    frame.push(markers.1);
    frame
}

pub(crate) fn unescape(bytes: &[u8], escape: u8, decode: impl Fn(u8) -> u8) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(bytes.len());
    let mut bytes = bytes.iter().copied();
    while let Some(byte) = bytes.next() {
        body.push(if byte == escape {
            decode(bytes.next().context("frame ends inside an escape")?)
        } else {
            byte
        });
    }
    Ok(body)
}

#[derive(Default)]
pub(crate) struct DelimitedFrames {
    body: Vec<u8>,
    open: bool,
}

impl DelimitedFrames {
    pub(crate) fn push<T>(
        &mut self,
        bytes: &[u8],
        (start, end): (u8, u8),
        label: &str,
        decode: impl Fn(&[u8]) -> Result<T>,
    ) -> Vec<Result<T>> {
        let mut frames = Vec::new();
        for &byte in bytes {
            if !self.open {
                self.open = byte == start;
                continue;
            }
            if byte == end {
                // HDLC closing flags also open the next frame; consecutive flags are idle.
                if start != end || !self.body.is_empty() {
                    frames.push(decode(&self.body));
                }
                self.body.clear();
                self.open = start == end;
            } else if self.body.len() < MAX_FRAME - 1 {
                self.body.push(byte);
            } else {
                self.body.clear();
                self.open = false;
                frames.push(Err(anyhow!("{label} frame exceeds {MAX_FRAME} bytes")));
            }
        }
        frames
    }
}

#[cfg(test)]
mod tests {
    use super::{DelimitedFrames, MAX_FRAME};

    #[test]
    fn limits_and_recovery_do_not_depend_on_chunking_or_delimiter_style() {
        for markers @ (start, end) in [(0x3e, 0x3c), (0x7e, 0x7e)] {
            for length in [MAX_FRAME - 2, MAX_FRAME - 1, MAX_FRAME, MAX_FRAME + 1] {
                let wire = [vec![start], vec![0; length], vec![end]].concat();
                for chunk_size in [1, 512, wire.len()] {
                    let mut decoder = DelimitedFrames::default();
                    let mut frames = Vec::new();
                    for chunk in wire.chunks(chunk_size) {
                        frames.extend(decoder.push(chunk, markers, "test", |body| Ok(body.len())));
                        assert!(decoder.body.len() < MAX_FRAME);
                    }
                    assert_eq!(
                        frames.len(),
                        1,
                        "markers={markers:?}, length={length}, chunk={chunk_size}"
                    );
                    assert_eq!(
                        frames[0].as_ref().ok().copied(),
                        (length < MAX_FRAME).then_some(length)
                    );
                    let recovered =
                        decoder.push(&[start, 1, end], markers, "test", |body| Ok(body.to_vec()));
                    assert_eq!(recovered.len(), 1);
                    assert_eq!(recovered[0].as_ref().unwrap(), &[1]);
                }
            }
        }
    }
}
