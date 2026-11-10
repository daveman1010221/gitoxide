//! `receive-pack`: takes a push, the references to update and the pack of objects they need.
//!
//! A request is the update commands as packet lines (`<old> <new> <reference>`, the first carrying the client's capabilities
//! after a NUL), a flush, and then the pack, unless every command deletes. The pack is indexed into a quarantine, a directory
//! of its own beside the repository's objects that sees them as an alternate, thin packs completed from what the repository
//! holds. The push is judged there, with the pushed objects readable and nothing else seeing them: as git does, a branch is
//! not deleted where `receive.denyDeletes` says so, nor moved other than forward where `receive.denyNonFastForwards` does,
//! and every object the new history needs must be in the pack or the repository; then the caller's own judgement. Once it is
//! allowed the pack moves into the repository, and the references are updated in one transaction, each only if it still
//! holds the `<old>` the client said, so a push that raced another is refused. A refused push leaves nothing behind. The
//! answer (`report-status`) says how the pack went and each reference's outcome, inside side band 1 if the client asked for
//! side bands.

use crate::pkt::{self, Line};
use crate::{Error, Result};
use bstr::ByteSlice;
use gix::ObjectId;
use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog};
use gix::refs::{FullName, Target};
use gix_packetline::Channel;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// What `receive-pack` offers: every update in one transaction (`atomic`), deleting references, a status report, side band 64k.
pub const CAPABILITIES: &str = "report-status delete-refs side-band-64k quiet atomic ofs-delta";

/// One update a client asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// What the reference holds now, as the client believes; the null id for a reference to create.
    pub old: ObjectId,
    /// What it should hold; the null id to delete it.
    pub new: ObjectId,
    /// The reference's full name.
    pub name: String,
}

impl Command {
    /// Whether it deletes the reference.
    pub fn deletes(&self) -> bool {
        self.new.is_null()
    }

    fn of_a_branch(&self) -> bool {
        self.name.starts_with("refs/heads/")
    }
}

/// The update commands of a `receive-pack` request, read from the front of `body`: what a server may judge before it reads
/// the pack.
pub fn commands(body: &[u8]) -> Result<Vec<Command>> {
    Ok(parse(body)?.commands)
}

struct Request {
    commands: Vec<Command>,
    side_band: bool,
    /// Where the pack starts.
    pack_at: usize,
}

fn parse(body: &[u8]) -> Result<Request> {
    let (lines, pack_at) = pkt::lines(body, |line| matches!(line, Line::Flush))?;
    let mut commands = Vec::new();
    let mut side_band = false;
    for line in lines {
        let Line::Data(data) = line else { break };
        let (command, capabilities) = data.split_once_str(b"\0").unwrap_or((data, b""));
        if commands.is_empty() {
            side_band = capabilities.split_str(" ").any(|c| c == b"side-band-64k");
        }
        commands.push(command_of(command)?);
    }
    Ok(Request {
        commands,
        side_band,
        pack_at,
    })
}

/// Answers one `receive-pack` request: `body` is the request, the answer goes to `out`, and the updates made are returned,
/// all of the request's or none. `allow` judges the commands once git's own rules have, before any object or reference is
/// taken, giving the reason for a refusal; the repository it is given reads the pushed objects as well as the repository's.
/// A refusal takes nothing. Setting `should_interrupt` stops the request where it is, taking nothing.
pub fn serve(
    repo: &gix::Repository,
    body: &[u8],
    out: &mut impl Write,
    should_interrupt: &AtomicBool,
    allow: impl FnOnce(&gix::Repository, &[Command]) -> std::result::Result<(), String>,
) -> Result<Vec<Command>> {
    let Request {
        commands,
        side_band,
        pack_at,
    } = parse(body)?;
    if commands.is_empty() {
        return Ok(Vec::new());
    }

    let mut report = Vec::new();
    let (unpacked, outcome) = if commands.iter().all(Command::deletes) {
        let outcome = git_rules(repo, &commands, should_interrupt)
            .and_then(|()| allow(repo, &commands))
            .and_then(|()| update(repo, &commands));
        (Ok(()), outcome)
    } else {
        let quarantine = Quarantine::new(repo)?;
        match quarantine.unpack(repo, &body[pack_at..], should_interrupt) {
            Err(e) => {
                let why = format!("the pack was not taken: {e}");
                (Err(e), Err(why))
            }
            Ok(()) => {
                let outcome = quarantine
                    .repository(repo)
                    .and_then(|quarantined| {
                        git_rules(&quarantined, &commands, should_interrupt)?;
                        allow(&quarantined, &commands)
                    })
                    .and_then(|()| quarantine.publish(repo))
                    .and_then(|()| {
                        // The pack is among the repository's now: read it again, so its objects are found.
                        gix::open(repo.path()).map_err(|e| format!("the repository cannot be opened again: {e}"))
                    })
                    .and_then(|repo| update(&repo, &commands));
                (Ok(()), outcome)
            }
        }
    };
    match &unpacked {
        Ok(()) => pkt::data(b"unpack ok\n", &mut report)?,
        Err(e) => pkt::data(format!("unpack {e}\n").as_bytes(), &mut report)?,
    }
    for command in &commands {
        match &outcome {
            Ok(()) => pkt::data(format!("ok {}\n", command.name).as_bytes(), &mut report)?,
            Err(why) => pkt::data(
                format!("ng {} {}\n", command.name, why.replace('\n', " ")).as_bytes(),
                &mut report,
            )?,
        }
    }
    pkt::flush(&mut report)?;
    if side_band {
        pkt::band(Channel::Data, &report, out)?;
        pkt::flush(out)?;
    } else {
        out.write_all(&report)?;
    }
    Ok(if outcome.is_ok() { commands } else { Vec::new() })
}

fn command_of(line: &[u8]) -> Result<Command> {
    let mut parts = line.splitn_str(3, " ");
    let (Some(old), Some(new), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Error::new(format!("`{}` is not an update command", line.as_bstr())));
    };
    let id = |hex: &[u8]| {
        ObjectId::from_hex(hex).map_err(|e| Error::new(format!("`{}` is not an object id: {e}", hex.as_bstr())))
    };
    let name = name
        .to_str()
        .map_err(|_| Error::new("a reference name that is not UTF-8"))?
        .to_string();
    Ok(Command {
        old: id(old)?,
        new: id(new)?,
        name,
    })
}

/// What git's own `receive-pack` holds every push to, in its words: a branch is not deleted where `receive.denyDeletes`
/// says so (`deletion prohibited`), nor moved to a commit that does not descend from the one it holds where
/// `receive.denyNonFastForwards` does (`non-fast-forward`), and the objects the new history needs, those not reachable from
/// a reference already, are all in `repo` (`missing necessary objects`).
fn git_rules(
    repo: &gix::Repository,
    commands: &[Command],
    should_interrupt: &AtomicBool,
) -> std::result::Result<(), String> {
    let config = repo.config_snapshot();
    let deny_deletes = config.boolean("receive.denyDeletes").unwrap_or(false);
    let deny_rewrites = config.boolean("receive.denyNonFastForwards").unwrap_or(false);
    for command in commands.iter().filter(|c| c.of_a_branch()) {
        if command.deletes() {
            if deny_deletes {
                return Err("deletion prohibited".into());
            }
        } else if deny_rewrites && !command.old.is_null() && !descends(repo, command.new, command.old) {
            return Err("non-fast-forward".into());
        }
    }
    complete(repo, commands, should_interrupt)
}

/// Whether `new` is `old` or a commit after it.
fn descends(repo: &gix::Repository, new: ObjectId, old: ObjectId) -> bool {
    new == old || matches!(repo.merge_base(new, old), Ok(Some(base)) if base.detach() == old)
}

/// Whether every object reachable from the commands' new values, and not from a reference, is in `repo`: each tag's
/// target, each commit with its parents, and each tree with what it holds.
fn complete(
    repo: &gix::Repository,
    commands: &[Command],
    should_interrupt: &AtomicBool,
) -> std::result::Result<(), String> {
    const MISSING: &str = "missing necessary objects";
    let missing = |_| MISSING.to_string();
    let mut tips = Vec::new();
    let mut trees = Vec::new();
    for command in commands.iter().filter(|c| !c.deletes()) {
        let mut id = command.new;
        loop {
            let object = repo.find_object(id).map_err(missing)?;
            match object.kind {
                gix::object::Kind::Tag => id = object.into_tag().target_id().map_err(missing)?.detach(),
                gix::object::Kind::Commit => break tips.push(id),
                gix::object::Kind::Tree => break trees.push(id),
                gix::object::Kind::Blob => break,
            }
        }
    }
    let mut hidden = Vec::new();
    for reference in repo
        .references()
        .map_err(|e| format!("the references cannot be listed: {e}"))?
        .all()
        .map_err(|e| format!("the references cannot be listed: {e}"))?
    {
        let Ok(mut reference) = reference else { continue };
        if let Ok(id) = reference.peel_to_id() {
            if repo
                .find_header(id)
                .is_ok_and(|h| h.kind() == gix::object::Kind::Commit)
            {
                hidden.push(id.detach());
            }
        }
    }
    for commit in repo.rev_walk(tips).with_hidden(hidden).all().map_err(missing)? {
        if should_interrupt.load(Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        let commit = commit.map_err(missing)?;
        trees.push(commit.object().map_err(missing)?.tree_id().map_err(missing)?.detach());
    }
    let mut seen = gix::hashtable::HashSet::default();
    while let Some(id) = trees.pop() {
        if !seen.insert(id) {
            continue;
        }
        if should_interrupt.load(Ordering::Relaxed) {
            return Err("interrupted".into());
        }
        let tree = repo.find_tree(id).map_err(missing)?;
        for entry in tree.decode().map_err(missing)?.entries {
            let id = entry.oid.to_owned();
            if entry.mode.is_tree() {
                trees.push(id);
            } else if entry.mode.is_commit() {
                // A submodule's commit lives in another repository.
            } else if !seen.contains(&id) {
                if !repo.has_object(id) {
                    return Err(MISSING.into());
                }
                seen.insert(id);
            }
        }
    }
    Ok(())
}

/// A pushed pack's place until the push is allowed: `objects/incoming-<pid>-<n>`, with the repository's objects as its
/// alternate, as git's own `tmp_objdir`. It is removed when dropped, whatever happened.
struct Quarantine {
    directory: PathBuf,
}

impl Quarantine {
    fn new(repo: &gix::Repository) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let objects = repo.objects.store_ref().path().to_owned();
        let objects = objects.canonicalize().unwrap_or(objects);
        let directory = objects.join(format!(
            "incoming-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let quarantine = Quarantine { directory };
        std::fs::create_dir_all(quarantine.pack_directory())?;
        std::fs::create_dir_all(quarantine.directory.join("info"))?;
        let alternate = objects
            .to_str()
            .ok_or_else(|| Error::new("the objects directory's path is not UTF-8"))?;
        std::fs::write(
            quarantine.directory.join("info").join("alternates"),
            format!("{alternate}\n"),
        )?;
        Ok(quarantine)
    }

    fn pack_directory(&self) -> PathBuf {
        self.directory.join("pack")
    }

    /// Indexes `pack` into the quarantine, completing a thin pack from the objects the repository holds.
    fn unpack(
        &self,
        repo: &gix::Repository,
        pack: &[u8],
        should_interrupt: &AtomicBool,
    ) -> std::result::Result<(), String> {
        if pack.is_empty() {
            return Err("no pack came with the commands".into());
        }
        let mut reader = std::io::BufReader::new(pack);
        gix_pack::Bundle::write_to_directory(
            &mut reader,
            Some(&self.pack_directory()),
            &mut gix::progress::Discard,
            should_interrupt,
            Some(repo.objects.clone()),
            repo.object_hash(),
            gix_pack::bundle::write::Options::default(),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    /// `repo`, reading the quarantine's objects and, through it, the repository's.
    fn repository(&self, repo: &gix::Repository) -> std::result::Result<gix::Repository, String> {
        let mut quarantined = repo.clone();
        quarantined.objects = gix::odb::at(&self.directory, repo.object_hash())
            .map_err(|e| format!("the quarantine cannot be read: {e}"))?
            .into();
        Ok(quarantined)
    }

    /// Moves the pack into the repository: each pack before its index, so a reader never finds an index without its pack.
    fn publish(&self, repo: &gix::Repository) -> std::result::Result<(), String> {
        let into = repo.objects.store_ref().path().join("pack");
        let entries: Vec<PathBuf> = std::fs::read_dir(self.pack_directory())
            .and_then(|entries| entries.map(|entry| entry.map(|e| e.path())).collect())
            .map_err(|e| format!("the quarantine cannot be listed: {e}"))?;
        for extension in ["pack", "idx"] {
            for path in entries.iter().filter(|p| p.extension().is_some_and(|e| e == extension)) {
                move_into(path, &into)?;
            }
        }
        Ok(())
    }
}

fn move_into(path: &Path, directory: &Path) -> std::result::Result<(), String> {
    let name = path
        .file_name()
        .ok_or_else(|| format!("{} has no file name", path.display()))?;
    std::fs::rename(path, directory.join(name)).map_err(|e| format!("{} cannot be moved in: {e}", path.display()))
}

impl Drop for Quarantine {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Moves every reference as `commands` say, in one transaction, each only if it still holds what the client said.
fn update(repo: &gix::Repository, commands: &[Command]) -> std::result::Result<(), String> {
    let mut edits = Vec::new();
    for command in commands {
        let name: FullName = command
            .name
            .as_str()
            .try_into()
            .map_err(|e| format!("`{}` is not a reference name: {e}", command.name))?;
        let expected = if command.old.is_null() {
            PreviousValue::MustNotExist
        } else {
            PreviousValue::MustExistAndMatch(Target::Object(command.old))
        };
        let change = if command.deletes() {
            Change::Delete {
                expected,
                log: RefLog::AndReference,
            }
        } else {
            if !repo.has_object(command.new) {
                return Err(format!("{} is not in the pack or the repository", command.new));
            }
            Change::Update {
                log: LogChange {
                    mode: RefLog::AndReference,
                    force_create_reflog: false,
                    message: "push".into(),
                },
                expected,
                new: Target::Object(command.new),
            }
        };
        edits.push(RefEdit {
            change,
            name,
            deref: false,
        });
    }
    repo.edit_references(edits).map(|_| ()).map_err(|e| e.to_string())
}
