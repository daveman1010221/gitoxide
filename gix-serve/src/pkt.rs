//! Packet lines as the two services read and write them.

use crate::{Error, Result};
use gix_packetline::blocking_io::encode;
use gix_packetline::{Channel, PacketLineRef, decode};
use std::io::Write;

/// The most data one side-band packet carries: a packet line's limit, less the band byte.
const BAND_DATA_MAX: usize = 65515;

pub(crate) fn data(bytes: &[u8], out: &mut impl Write) -> Result<()> {
    encode::data_to_write(bytes, &mut *out)?;
    Ok(())
}

pub(crate) fn text(bytes: &[u8], out: &mut impl Write) -> Result<()> {
    encode::text_to_write(bytes, &mut *out)?;
    Ok(())
}

pub(crate) fn flush(out: &mut impl Write) -> Result<()> {
    encode::flush_to_write(&mut *out)?;
    Ok(())
}

/// The delimiter between a version 2 request's capabilities and its arguments, or between a response's sections.
pub(crate) fn delimiter(out: &mut impl Write) -> Result<()> {
    encode::delim_to_write(&mut *out)?;
    Ok(())
}

/// `bytes` on `channel`, split into packets each band can carry.
pub(crate) fn band(channel: Channel, bytes: &[u8], out: &mut impl Write) -> Result<()> {
    for chunk in bytes.chunks(BAND_DATA_MAX) {
        encode::band_to_write(channel, chunk, &mut *out)?;
    }
    Ok(())
}

/// One line read from the front of a request body.
pub(crate) enum Line<'a> {
    /// A data line, its trailing newline taken off.
    Data(&'a [u8]),
    Flush,
    /// Version 2's delimiter.
    Delimiter,
}

/// Reads packet lines from `body` until it is used up, or until `stop` says so of a line, returning the lines and how many
/// bytes they took: what follows is not packet lines (a pushed pack).
pub(crate) fn lines<'a>(body: &'a [u8], mut stop: impl FnMut(&Line<'a>) -> bool) -> Result<(Vec<Line<'a>>, usize)> {
    let mut at = 0;
    let mut lines = Vec::new();
    while at < body.len() {
        let (line, consumed) = match decode::streaming(&body[at..]) {
            Ok(decode::Stream::Complete { line, bytes_consumed }) => (line, bytes_consumed),
            Ok(decode::Stream::Incomplete { .. }) => return Err(Error::new("the request ends inside a packet line")),
            Err(e) => return Err(Error::new(format!("the request is not packet lines: {e}"))),
        };
        at += consumed;
        let line = match line {
            PacketLineRef::Data(d) => Line::Data(d.strip_suffix(b"\n").unwrap_or(d)),
            PacketLineRef::Flush => Line::Flush,
            PacketLineRef::Delimiter => Line::Delimiter,
            PacketLineRef::ResponseEnd => return Err(Error::new("a response end in a request")),
        };
        let done = stop(&line);
        lines.push(line);
        if done {
            break;
        }
    }
    Ok((lines, at))
}
