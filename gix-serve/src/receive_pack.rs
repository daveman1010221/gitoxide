//! `receive-pack`: takes a push, the references to update and the pack of objects they need.
//!
//! A request is the update commands as packet lines (`<old> <new> <reference>`, the first carrying the client's capabilities
//! after a NUL), a flush, and then the pack, unless every command deletes. The pack is indexed into the repository, thin
//! packs completed from what it holds, and the references are updated in one transaction, each only if it still holds the
//! `<old>` the client said, so a push that raced another is refused. The answer (`report-status`) says how the pack went and
//! each reference's outcome, inside side band 1 if the client asked for side bands.

use crate::pkt::{self, Line};
use crate::{Error, Result};
use bstr::ByteSlice;
use gix::ObjectId;
use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog};
use gix::refs::{FullName, Target};
use gix_packetline::Channel;
use std::io::Write;
use std::sync::atomic::AtomicBool;

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
}

/// Answers one `receive-pack` request: `body` is the request, the answer goes to `out`. `allow` judges the commands once
/// the pack is in the repository and before any reference moves, giving the reason for a refusal; a refusal moves nothing.
pub fn serve(
    repo: &gix::Repository,
    body: &[u8],
    out: &mut impl Write,
    allow: impl FnOnce(&gix::Repository, &[Command]) -> std::result::Result<(), String>,
) -> Result<()> {
    let (lines, used) = pkt::lines(body, |line| matches!(line, Line::Flush))?;
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
    if commands.is_empty() {
        return Ok(());
    }

    let mut report = Vec::new();
    let unpacked = if commands.iter().all(Command::deletes) {
        Ok(())
    } else {
        unpack(repo, &body[used..])
    };
    let outcome = match &unpacked {
        Err(e) => Err(format!("the pack was not taken: {e}")),
        Ok(()) => {
            // The pack's objects are on disk now: read the repository again, so they are found.
            let repo = gix::open(repo.path())
                .map_err(|e| Error::new(format!("the repository cannot be opened again: {e}")))?;
            allow(&repo, &commands).and_then(|()| update(&repo, &commands))
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
    Ok(())
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

/// Indexes `pack` into the repository's pack directory, completing a thin pack from the objects the repository holds.
fn unpack(repo: &gix::Repository, pack: &[u8]) -> std::result::Result<(), String> {
    if pack.is_empty() {
        return Err("no pack came with the commands".into());
    }
    let directory = repo.objects.store_ref().path().join("pack");
    let mut reader = std::io::BufReader::new(pack);
    let interrupt = AtomicBool::new(false);
    gix_pack::Bundle::write_to_directory(
        &mut reader,
        Some(&directory),
        &mut gix::progress::Discard,
        &interrupt,
        Some(repo.objects.clone()),
        repo.object_hash(),
        gix_pack::bundle::write::Options::default(),
    )
    .map(|_| ())
    .map_err(|e| e.to_string())
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
