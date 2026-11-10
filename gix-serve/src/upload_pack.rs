//! `upload-pack`: answers a fetch or a clone with the objects the client wants and does not have.
//!
//! A request is one round of version 0 negotiation: the `want` lines (the first carrying the client's capabilities), a flush,
//! then `have` lines, ending in a flush (another round follows) or `done` (send the pack). With `multi_ack_detailed`, each
//! round is answered with `ACK <id> common` for every `have` the repository holds and a `NAK`; after `done`, with `ACK <id>`
//! for the last object in common (or `NAK` if none) and the pack. The pack is every object reachable from the wants and not
//! from what is in common: the commits between them, and for each the objects its tree adds to its parents'.

use crate::pkt::{self, Line};
use crate::{Error, Result};
use bstr::ByteSlice;
use gix::ObjectId;
use gix_packetline::Channel;
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::atomic::AtomicBool;

/// What `upload-pack` offers. Not `no-done` (a client always says `done` before the pack), not `thin-pack`, not `shallow`.
pub const CAPABILITIES: &str = "multi_ack_detailed side-band-64k side-band ofs-delta no-progress";

/// What a client asked for in its first `want` line.
#[derive(Debug, Default, Clone)]
struct Asked {
    side_band: Option<SideBand>,
    multi_ack_detailed: bool,
}

#[derive(Debug, Clone, Copy)]
enum SideBand {
    /// `side-band`: packets of up to 1000 bytes.
    Small,
    /// `side-band-64k`.
    Large,
}

/// One round of a request.
#[derive(Debug, Default)]
struct Request {
    wants: Vec<ObjectId>,
    haves: Vec<ObjectId>,
    done: bool,
    asked: Asked,
}

fn parse(body: &[u8]) -> Result<Request> {
    let (lines, _) = pkt::lines(body, |line| matches!(line, Line::Data(b"done")))?;
    let mut request = Request::default();
    let mut in_wants = true;
    for line in lines {
        match line {
            Line::Flush => in_wants = false,
            Line::Data(b"done") => request.done = true,
            Line::Data(data) => {
                let (verb, rest) = data.split_once_str(" ").unwrap_or((data, b""));
                match verb {
                    b"want" if in_wants => {
                        let (hex, capabilities) = rest.split_once_str(" ").unwrap_or((rest, b""));
                        if request.wants.is_empty() {
                            request.asked = asked(capabilities);
                        }
                        request.wants.push(object_id(hex)?);
                    }
                    b"have" if !in_wants => request.haves.push(object_id(rest)?),
                    other => {
                        return Err(Error::new(format!(
                            "`{}` is not something this server was offered",
                            other.as_bstr()
                        )));
                    }
                }
            }
        }
    }
    if request.wants.is_empty() {
        return Err(Error::new("a request wants nothing"));
    }
    Ok(request)
}

fn asked(capabilities: &[u8]) -> Asked {
    let mut asked = Asked::default();
    for capability in capabilities.split_str(" ") {
        match capability {
            b"side-band-64k" => asked.side_band = Some(SideBand::Large),
            b"side-band" if asked.side_band.is_none() => asked.side_band = Some(SideBand::Small),
            b"multi_ack_detailed" => asked.multi_ack_detailed = true,
            _ => {}
        }
    }
    asked
}

fn object_id(hex: &[u8]) -> Result<ObjectId> {
    ObjectId::from_hex(hex).map_err(|e| Error::new(format!("`{}` is not an object id: {e}", hex.as_bstr())))
}

/// Answers one stateless-RPC round of `upload-pack` from `repo`: `body` is the request, and the answer is written to `out`.
pub fn serve(repo: &gix::Repository, body: &[u8], out: &mut impl Write) -> Result<()> {
    let request = parse(body)?;
    for want in &request.wants {
        if !repo.has_object(want) {
            return Err(Error::new(format!("{want} is not in this repository")));
        }
    }
    // What the client has that the repository holds too: the objects in common.
    let common: Vec<ObjectId> = request.haves.iter().copied().filter(|id| repo.has_object(id)).collect();

    if !request.done {
        if request.asked.multi_ack_detailed {
            for id in &common {
                pkt::data(format!("ACK {id} common\n").as_bytes(), out)?;
            }
        }
        pkt::data(b"NAK\n", out)?;
        return Ok(());
    }

    match common.last() {
        Some(last) => pkt::data(format!("ACK {last}\n").as_bytes(), out)?,
        None => pkt::data(b"NAK\n", out)?,
    }
    let pack = pack(repo, &request.wants, &common)?;
    match request.asked.side_band {
        Some(SideBand::Large) => pkt::band(Channel::Data, &pack, out)?,
        Some(SideBand::Small) => {
            for chunk in pack.chunks(995) {
                gix_packetline::blocking_io::encode::band_to_write(Channel::Data, chunk, &mut *out)?;
            }
        }
        None => {
            out.write_all(&pack)?;
            return Ok(());
        }
    }
    pkt::flush(out)?;
    Ok(())
}

/// The pack of every object reachable from `wants` and not from `common`.
fn pack(repo: &gix::Repository, wants: &[ObjectId], common: &[ObjectId]) -> Result<Vec<u8>> {
    // A want that is an annotated tag goes in as itself, and the walk starts from what it points to.
    let mut tags = BTreeSet::new();
    let mut tips = Vec::new();
    for want in wants {
        let object = repo
            .find_object(*want)
            .map_err(|e| Error::new(format!("{want} cannot be read: {e}")))?;
        if object.kind == gix::object::Kind::Tag {
            tags.insert(*want);
            let peeled = object
                .peel_tags_to_end()
                .map_err(|e| Error::new(format!("the tag {want} cannot be followed: {e}")))?;
            tips.push(peeled.id);
        } else {
            tips.push(*want);
        }
    }
    let hidden: Vec<ObjectId> = common
        .iter()
        .copied()
        .filter(|id| matches!(repo.find_object(*id).map(|o| o.kind), Ok(gix::object::Kind::Commit)))
        .collect();
    let commits: Vec<ObjectId> = repo
        .rev_walk(
            tips.iter()
                .copied()
                .filter(|id| matches!(repo.find_object(*id).map(|o| o.kind), Ok(gix::object::Kind::Commit))),
        )
        .with_hidden(hidden)
        .all()
        .map_err(|e| Error::new(format!("the history cannot be walked: {e}")))?
        .map(|info| info.map(|info| info.id))
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| Error::new(format!("the history cannot be walked: {e}")))?;
    // A want that is a tree or a blob is sent as it is, with what a tree holds.
    let others: Vec<ObjectId> = tips
        .iter()
        .copied()
        .filter(|id| !matches!(repo.find_object(*id).map(|o| o.kind), Ok(gix::object::Kind::Commit)))
        .collect();

    let interrupt = AtomicBool::new(false);
    let progress = gix::progress::Discard;
    // A handle the pack's entries can be made with on other threads.
    let mut db = gix::open(repo.path())
        .map_err(|e| Error::new(format!("the repository cannot be opened for the pack: {e}")))?
        .into_sync()
        .objects
        .into_shared_arc()
        .to_cache_arc();
    // As `gix pack create` does: a pack's entries are copied from the packs they are in, which must stay put meanwhile.
    db.prevent_pack_unload();
    db.ignore_replacements = true;
    let mut input = tags.into_iter().chain(commits).chain(others).map(Ok::<_, gix::Error>);
    let (counts, _) = gix_pack::data::output::count::objects_unthreaded(
        &db,
        &mut input,
        &progress,
        &interrupt,
        gix_pack::data::output::count::objects::ObjectExpansion::TreeAdditionsComparedToAncestor,
    )
    .map_err(|e| Error::new(format!("the objects cannot be counted: {e}")))?;
    let num_objects = counts.len() as u32;
    let entries = gix::parallel::InOrderIter::from(
        gix_pack::data::output::entry::iter_from_counts(
            counts,
            db,
            Box::new(gix::progress::Discard),
            gix_pack::data::output::entry::iter_from_counts::Options {
                thread_limit: Some(1),
                mode: gix_pack::data::output::entry::iter_from_counts::Mode::PackCopyAndBaseObjects,
                allow_thin_pack: false,
                chunk_size: 1000,
                version: Default::default(),
                compression: Default::default(),
            },
        )
        .map_err(|e| Error::new(format!("the pack cannot be made: {e}")))?,
    );
    let mut pack = Vec::new();
    let mut bytes = gix_pack::data::output::bytes::FromEntriesIter::new(
        entries,
        &mut pack,
        num_objects,
        gix_pack::data::Version::default(),
        repo.object_hash(),
    );
    for written in bytes.by_ref() {
        written.map_err(|e| Error::new(format!("the pack cannot be written: {e}")))?;
    }
    drop(bytes);
    Ok(pack)
}
