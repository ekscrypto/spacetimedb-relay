// SPDX-License-Identifier: MIT

//! Wire-framing helpers shared by the rewrite and listener paths.
//!
//! **Server → client** frames start with a 1-byte compression tag
//! (we always negotiate `?compression=None`, so tag=0 is what we
//! produce and what we expect to see). Past that byte sits the BSATN
//! body whose first byte is the `ServerMessage` sum-type discriminant.
//!
//! **Client → server** frames are raw `ClientMessage` BSATN with *no*
//! compression prefix — matching the official SpacetimeDB Rust SDK's
//! `encode_message`. Do not run [`body`] / [`message_tag`] on inbound
//! client frames; decode the full buffer as `ClientMessage` instead.

use thiserror::Error;

pub const COMPRESSION_NONE: u8 = 0;

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame too short ({0} bytes)")]
    TooShort(usize),
    #[error("unsupported compression tag {0} (expected 0 = None)")]
    UnsupportedCompression(u8),
    #[error("bsatn decode: {0}")]
    Decode(String),
    #[error("bsatn encode: {0}")]
    Encode(String),
}

/// Strip the leading compression byte. Returns the BSATN body. Errors
/// if the frame is empty or compressed.
pub fn body(frame: &[u8]) -> Result<&[u8], FrameError> {
    let first = *frame.first().ok_or(FrameError::TooShort(0))?;
    if first != COMPRESSION_NONE {
        return Err(FrameError::UnsupportedCompression(first));
    }
    Ok(&frame[1..])
}

/// First byte of the BSATN body, i.e. the sum-type discriminant.
/// `None` if the frame is empty or compressed.
pub fn message_tag(frame: &[u8]) -> Option<u8> {
    body(frame).ok().and_then(|b| b.first().copied())
}

/// Wrap a BSATN body in the compression-None envelope.
pub fn wrap_uncompressed(body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 1);
    out.push(COMPRESSION_NONE);
    out.extend_from_slice(&body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_strips_compression_byte() {
        let frame = [0u8, 0xAB, 0xCD];
        assert_eq!(body(&frame).unwrap(), &[0xAB, 0xCD]);
    }

    #[test]
    fn body_rejects_compressed_frames() {
        let frame = [1u8, 0xAB];
        assert!(matches!(
            body(&frame),
            Err(FrameError::UnsupportedCompression(1))
        ));
    }

    #[test]
    fn body_rejects_empty() {
        let frame: [u8; 0] = [];
        assert!(matches!(body(&frame), Err(FrameError::TooShort(0))));
    }

    #[test]
    fn message_tag_returns_first_body_byte() {
        let frame = [0u8, 0x04, 0x00];
        assert_eq!(message_tag(&frame), Some(0x04));
    }

    #[test]
    fn wrap_uncompressed_round_trips() {
        let body_bytes = vec![1, 2, 3];
        let frame = wrap_uncompressed(body_bytes.clone());
        assert_eq!(frame[0], COMPRESSION_NONE);
        assert_eq!(&frame[1..], &body_bytes);
    }
}
