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
pub(crate) struct DelimitedFrames(Vec<u8>);

impl DelimitedFrames {
    pub(crate) fn push<T>(
        &mut self,
        bytes: &[u8],
        (start, end): (u8, u8),
        label: &str,
        decode: impl Fn(&[u8]) -> Result<T>,
    ) -> Vec<Result<T>> {
        self.0.extend_from_slice(bytes);
        let mut frames = Vec::new();
        while let Some(begin) = self.0.iter().position(|byte| *byte == start) {
            self.0.drain(..begin);
            let Some(close) = self.0[1..].iter().position(|byte| *byte == end) else {
                if self.0.len() > MAX_FRAME {
                    self.0.clear();
                    frames.push(Err(anyhow!("{label} frame exceeds {MAX_FRAME} bytes")));
                }
                return frames;
            };
            // HDLC closing flags also open the next frame; consecutive flags are idle.
            if start != end || close > 0 {
                frames.push(decode(&self.0[1..=close]));
            }
            self.0.drain(..close + 1 + usize::from(start != end));
        }
        self.0.clear(); // Noise outside a frame does not carry over to the next read.
        frames
    }
}
