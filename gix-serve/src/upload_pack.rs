//! `upload-pack`: answers a fetch or a clone with the objects the client wants and does not have.
//!
//! Version 0 (and 1): a request is one round of negotiation, the `want` lines (the first carrying the client's capabilities),
//! a flush, then `have` lines ending in a flush (another round follows) or `done` (send the pack). Each round is answered,
//! with `multi_ack_detailed`, with `ACK <id> common` for every `have` the repository holds and a `NAK`; after `done`, with
//! `ACK <id>` for the last object in common (or `NAK` if none) and the pack.
//!
//! Version 2: a request is one command, `ls-refs` or `fetch`, with its capability lines, a delimiter and its arguments. A
//! `fetch` without `done` is answered with an `acknowledgments` section, and when what is in common already settles the pack
//! (`ready`) the pack follows in a `packfile` section; with `done`, the `packfile` section alone.
//!
//! The pack is every object reachable from the wants and not from what is in common: the commits between them, and for each
//! the objects its tree adds to its parents', with, if asked, each annotated tag that points at a commit sent.

use crate::pkt::{self, Line};
use crate::{Error, Protocol, Result};
use bstr::ByteSlice;
use gix::ObjectId;
use gix_packetline::Channel;
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};

/// What `upload-pack` offers in version 0 and 1. Not `no-done` (a client always says `done` before the pack), not
/// `thin-pack`, not `shallow`.
pub const CAPABILITIES: &str = "multi_ack_detailed side-band-64k side-band ofs-delta no-progress include-tag";

/// What it offers in version 2, one capability a line before the agent and the object format: `ls-refs` with unborn heads,
/// and `fetch` with no shallow, filter or wait-for-done.
pub(crate) const CAPABILITIES_V2: &[&str] = &["ls-refs=unborn", "fetch"];

/// The most data one packet carries with `side-band` (not `side-band-64k`), less the band byte.
const SMALL_BAND: usize = 995;

/// Answers one stateless-RPC request of `upload-pack` from `repo` in `protocol`: `body` is the request, and the answer is
/// written to `out`. Setting `should_interrupt` stops the request where it is, with an error.
pub fn serve(
    repo: &gix::Repository,
    protocol: Protocol,
    body: &[u8],
    out: &mut impl Write,
    should_interrupt: &AtomicBool,
) -> Result<()> {
    match protocol {
        Protocol::V2 => v2::serve(repo, body, out, should_interrupt),
        Protocol::V0 | Protocol::V1 => v0::serve(repo, body, out, should_interrupt),
    }
}

mod v0 {
    use super::*;

    /// What a client asked for in its first `want` line.
    #[derive(Debug, Default, Clone)]
    struct Asked {
        side_band: Option<SideBand>,
        multi_ack_detailed: bool,
        include_tag: bool,
    }

    #[derive(Debug, Clone, Copy)]
    enum SideBand {
        Small,
        Large,
    }

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
                Line::Delimiter => return Err(Error::new("a delimiter in a version 0 request")),
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
                        other => return Err(not_offered(other)),
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
                b"include-tag" => asked.include_tag = true,
                _ => {}
            }
        }
        asked
    }

    pub(super) fn serve(
        repo: &gix::Repository,
        body: &[u8],
        out: &mut impl Write,
        should_interrupt: &AtomicBool,
    ) -> Result<()> {
        let request = parse(body)?;
        check_wants(repo, &request.wants)?;
        let common = in_common(repo, &request.haves);
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
        let pack = pack(
            repo,
            &request.wants,
            &common,
            request.asked.include_tag,
            should_interrupt,
        )?;
        match request.asked.side_band {
            Some(SideBand::Large) => pkt::band(Channel::Data, &pack, out)?,
            Some(SideBand::Small) => {
                for chunk in pack.chunks(SMALL_BAND) {
                    gix_packetline::blocking_io::encode::band_to_write(Channel::Data, chunk, &mut *out)?;
                }
            }
            None => {
                out.write_all(&pack)?;
                return Ok(());
            }
        }
        pkt::flush(out)
    }
}

mod v2 {
    use super::*;

    /// One command and its arguments; the capability lines before the delimiter are not needed to answer it.
    struct Command<'a> {
        name: &'a [u8],
        arguments: Vec<&'a [u8]>,
    }

    fn parse(body: &[u8]) -> Result<Command<'_>> {
        let (lines, _) = pkt::lines(body, |line| matches!(line, Line::Flush))?;
        let mut lines = lines.into_iter();
        let name = match lines.next() {
            Some(Line::Data(line)) => line
                .strip_prefix(b"command=")
                .ok_or_else(|| Error::new("a version 2 request starts with its command"))?,
            _ => return Err(Error::new("a version 2 request names no command")),
        };
        let mut arguments = Vec::new();
        let mut after_delimiter = false;
        for line in lines {
            match line {
                Line::Delimiter => after_delimiter = true,
                Line::Flush => break,
                Line::Data(data) if after_delimiter => arguments.push(data),
                // A capability (agent, object-format, server-option): nothing here depends on it.
                Line::Data(_) => {}
            }
        }
        Ok(Command { name, arguments })
    }

    pub(super) fn serve(
        repo: &gix::Repository,
        body: &[u8],
        out: &mut impl Write,
        should_interrupt: &AtomicBool,
    ) -> Result<()> {
        let command = parse(body)?;
        match command.name {
            b"ls-refs" => ls_refs(repo, &command.arguments, out),
            b"fetch" => fetch(repo, &command.arguments, out, should_interrupt),
            other => Err(Error::new(format!(
                "`{}` is not a command this server offers",
                other.as_bstr()
            ))),
        }
    }

    /// The references, `HEAD` first, as `git ls-refs` lists them: each by its id and name, with `symref-target` for a
    /// symbolic reference if asked (`symrefs`), `peeled` for an annotated tag if asked (`peel`), only those starting with a
    /// `ref-prefix` if any is given, and an unborn `HEAD` if asked (`unborn`).
    fn ls_refs(repo: &gix::Repository, arguments: &[&[u8]], out: &mut impl Write) -> Result<()> {
        let mut symrefs = false;
        let mut peel = false;
        let mut unborn = false;
        let mut prefixes: Vec<&[u8]> = Vec::new();
        for argument in arguments {
            match *argument {
                b"symrefs" => symrefs = true,
                b"peel" => peel = true,
                b"unborn" => unborn = true,
                other => match other.strip_prefix(b"ref-prefix ") {
                    Some(prefix) => prefixes.push(prefix),
                    None => return Err(not_offered(other)),
                },
            }
        }
        let wanted = |name: &str| prefixes.is_empty() || prefixes.iter().any(|p| name.as_bytes().starts_with(p));
        let head_target = crate::head_symref(repo)?;
        let refs = crate::advertised_refs(repo)?;
        if unborn && wanted("HEAD") && refs.iter().all(|(name, _, _)| name != "HEAD") {
            if let Some(target) = &head_target {
                pkt::data(format!("unborn HEAD symref-target:{target}\n").as_bytes(), out)?;
            }
        }
        for (name, id, peeled) in &refs {
            if !wanted(name) {
                continue;
            }
            let mut line = format!("{id} {name}");
            if symrefs && name == "HEAD" {
                if let Some(target) = &head_target {
                    line.push_str(" symref-target:");
                    line.push_str(target);
                }
            }
            if let (true, Some(peeled)) = (peel, peeled) {
                line.push_str(" peeled:");
                line.push_str(&peeled.to_string());
            }
            line.push('\n');
            pkt::data(line.as_bytes(), out)?;
        }
        pkt::flush(out)
    }

    fn fetch(
        repo: &gix::Repository,
        arguments: &[&[u8]],
        out: &mut impl Write,
        should_interrupt: &AtomicBool,
    ) -> Result<()> {
        let mut wants = Vec::new();
        let mut haves = Vec::new();
        let mut done = false;
        let mut include_tag = false;
        for argument in arguments {
            let (verb, rest) = argument.split_once_str(" ").unwrap_or((argument, b""));
            match verb {
                b"want" => wants.push(object_id(rest)?),
                b"have" => haves.push(object_id(rest)?),
                b"done" => done = true,
                b"include-tag" => include_tag = true,
                // What the pack is like already: thin packs are not sent, deltas are by offset, and progress is not sent.
                b"thin-pack" | b"ofs-delta" | b"no-progress" => {}
                other => return Err(not_offered(other)),
            }
        }
        if wants.is_empty() {
            return Err(Error::new("a fetch wants nothing"));
        }
        check_wants(repo, &wants)?;
        let common = in_common(repo, &haves);
        if !done {
            pkt::data(b"acknowledgments\n", out)?;
            if common.is_empty() {
                pkt::data(b"NAK\n", out)?;
            }
            for id in &common {
                pkt::data(format!("ACK {id}\n").as_bytes(), out)?;
            }
            if !ready(repo, &wants, &common, should_interrupt)? {
                return pkt::flush(out);
            }
            pkt::data(b"ready\n", out)?;
            pkt::delimiter(out)?;
        }
        pkt::data(b"packfile\n", out)?;
        let pack = pack(repo, &wants, &common, include_tag, should_interrupt)?;
        pkt::band(Channel::Data, &pack, out)?;
        pkt::flush(out)
    }

    /// Whether what is in common settles the pack: every path from the wants back through history ends in it, so the
    /// client has every commit the walk does not reach.
    fn ready(
        repo: &gix::Repository,
        wants: &[ObjectId],
        common: &[ObjectId],
        should_interrupt: &AtomicBool,
    ) -> Result<bool> {
        if common.is_empty() {
            return Ok(false);
        }
        Ok(!walk(repo, wants, common, should_interrupt)?.reaches_a_root)
    }
}

fn object_id(hex: &[u8]) -> Result<ObjectId> {
    ObjectId::from_hex(hex).map_err(|e| Error::new(format!("`{}` is not an object id: {e}", hex.as_bstr())))
}

fn not_offered(what: &[u8]) -> Error {
    Error::new(format!("`{}` is not something this server offered", what.as_bstr()))
}

fn check_wants(repo: &gix::Repository, wants: &[ObjectId]) -> Result<()> {
    for want in wants {
        if !repo.has_object(want) {
            return Err(Error::new(format!("{want} is not in this repository")));
        }
    }
    Ok(())
}

/// The `have`s the repository holds: what the client and the repository have in common.
fn in_common(repo: &gix::Repository, haves: &[ObjectId]) -> Vec<ObjectId> {
    haves.iter().copied().filter(|id| repo.has_object(id)).collect()
}

fn kind_of(repo: &gix::Repository, id: ObjectId) -> Option<gix::object::Kind> {
    repo.find_header(id).ok().map(|h| h.kind())
}

/// The commits reachable from the tips and not from what is hidden, and whether any has no parent: then the walk did not end
/// in what is hidden.
struct Walked {
    commits: Vec<ObjectId>,
    reaches_a_root: bool,
}

fn walk(
    repo: &gix::Repository,
    tips: &[ObjectId],
    hidden: &[ObjectId],
    should_interrupt: &AtomicBool,
) -> Result<Walked> {
    let commit = |id: &ObjectId| kind_of(repo, *id) == Some(gix::object::Kind::Commit);
    let hidden: Vec<ObjectId> = hidden.iter().copied().filter(commit).collect();
    let mut walked = Walked {
        commits: Vec::new(),
        reaches_a_root: false,
    };
    for info in repo
        .rev_walk(tips.iter().copied().filter(commit))
        .with_hidden(hidden)
        .all()
        .map_err(|e| Error::new(format!("the history cannot be walked: {e}")))?
    {
        if should_interrupt.load(Ordering::Relaxed) {
            return Err(Error::new("interrupted"));
        }
        let info = info.map_err(|e| Error::new(format!("the history cannot be walked: {e}")))?;
        if info.parent_ids.is_empty() {
            walked.reaches_a_root = true;
        }
        walked.commits.push(info.id);
    }
    Ok(walked)
}

/// The pack of every object reachable from `wants` and not from `common`, and with `include_tag` each annotated tag that
/// points at a commit in it.
fn pack(
    repo: &gix::Repository,
    wants: &[ObjectId],
    common: &[ObjectId],
    include_tag: bool,
    should_interrupt: &AtomicBool,
) -> Result<Vec<u8>> {
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
    let walked = walk(repo, &tips, common, should_interrupt)?;
    let sent: BTreeSet<ObjectId> = walked.commits.iter().copied().collect();
    if include_tag {
        tags.extend(tags_pointing_into(repo, &sent, common)?);
    }
    // A want that is a tree or a blob is sent as it is, with what a tree holds.
    let others: Vec<ObjectId> = tips
        .iter()
        .copied()
        .filter(|id| kind_of(repo, *id) != Some(gix::object::Kind::Commit))
        .collect();
    // Counting by tree additions puts each commit's parents and their root trees in too. Those not walked are what the
    // client has (the walk stopped at what is in common), so they are taken out again.
    let mut already_there = gix::hashtable::HashSet::default();
    for id in &walked.commits {
        let commit = repo
            .find_commit(*id)
            .map_err(|e| Error::new(format!("the commit {id} cannot be read: {e}")))?;
        for parent in commit.parent_ids().map(gix::Id::detach) {
            if sent.contains(&parent) {
                continue;
            }
            already_there.insert(parent);
            if let Ok(tree) = repo.find_commit(parent).map(|p| p.tree_id()) {
                if let Ok(tree) = tree {
                    already_there.insert(tree.detach());
                }
            }
        }
    }

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
    let mut input = tags
        .into_iter()
        .chain(walked.commits)
        .chain(others)
        .map(Ok::<_, gix::Error>);
    let (mut counts, _) = gix_pack::data::output::count::objects_unthreaded(
        &db,
        &mut input,
        &progress,
        should_interrupt,
        gix_pack::data::output::count::objects::ObjectExpansion::TreeAdditionsComparedToAncestor,
    )
    .map_err(|e| Error::new(format!("the objects cannot be counted: {e}")))?;
    counts.retain(|count| !already_there.contains(&count.id));
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
        if should_interrupt.load(Ordering::Relaxed) {
            return Err(Error::new("interrupted"));
        }
        written.map_err(|e| Error::new(format!("the pack cannot be written: {e}")))?;
    }
    drop(bytes);
    Ok(pack)
}

/// The annotated tags under `refs/tags/` that point (through other tags) at a commit in `sent`, and that the client does not
/// have already.
fn tags_pointing_into(repo: &gix::Repository, sent: &BTreeSet<ObjectId>, common: &[ObjectId]) -> Result<Vec<ObjectId>> {
    let mut found = Vec::new();
    let platform = repo
        .references()
        .map_err(|e| Error::new(format!("the references cannot be listed: {e}")))?;
    for reference in platform
        .prefixed("refs/tags/")
        .map_err(|e| Error::new(format!("the tags cannot be listed: {e}")))?
    {
        let Ok(mut reference) = reference else { continue };
        let Some(id) = reference.try_id().map(gix::Id::detach) else {
            continue;
        };
        if common.contains(&id) || kind_of(repo, id) != Some(gix::object::Kind::Tag) {
            continue;
        }
        if let Ok(peeled) = reference.peel_to_id() {
            if sent.contains(&peeled.detach()) {
                found.push(id);
            }
        }
    }
    Ok(found)
}
