//! Serve git's smart protocol from a repository: `upload-pack`, which answers a fetch or a clone, and `receive-pack`, which
//! takes a push, each in its stateless-RPC form, as smart HTTP carries them.
//!
//! Smart HTTP is two requests for each service: a `GET` of `info/refs?service=git-<service>`, answered by [`advertise`],
//! and a `POST` to `git-<service>`, whose body is one round of the protocol, answered by [`upload_pack::serve`] or
//! [`receive_pack::serve`]. Nothing is kept between requests: a client sends everything a round needs each time. Which
//! protocol a request speaks is what its `Git-Protocol` header asks for ([`Protocol::from_header`]).
//!
//! `upload-pack` speaks version 2, which git asks for by default, and versions 0 and 1, which libgit2 and go-git speak.
//! `receive-pack` speaks versions 0 and 1: git has no version 2 push, and a client asking for it is answered in version 0,
//! as git's own server does. Shallow clones and the `filter` capability are not offered, so a client never asks for them.
#![deny(missing_docs, rust_2018_idioms)]
#![forbid(unsafe_code)]

use bstr::{BStr, ByteSlice};
use std::fmt::Write as _;
use std::io::Write;

mod pkt;
pub mod receive_pack;
pub mod upload_pack;

/// What went wrong while serving a request.
#[derive(Debug)]
pub struct Error(String);

impl Error {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Error(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(format!("i/o: {e}"))
    }
}

/// The result of serving.
pub type Result<T> = std::result::Result<T, Error>;

/// The protocol version a request speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// The original protocol, which every client speaks.
    V0,
    /// Version 0 with a `version 1` line in the advertisement.
    V1,
    /// One command a request, `ls-refs` or `fetch`; `upload-pack` only.
    V2,
}

impl Protocol {
    /// The version a `Git-Protocol` header asks for (colon-separated `key=value` parameters, `version=2` among them), or
    /// version 0 when there is none or it asks for one this server does not speak.
    pub fn from_header(value: Option<&str>) -> Self {
        let asked = value
            .unwrap_or("")
            .split(':')
            .filter_map(|parameter| parameter.strip_prefix("version="))
            .filter_map(|version| version.parse::<u8>().ok())
            .max();
        match asked {
            Some(2) => Protocol::V2,
            Some(1) => Protocol::V1,
            _ => Protocol::V0,
        }
    }
}

/// One of the two services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    /// Answers a fetch or a clone.
    UploadPack,
    /// Takes a push.
    ReceivePack,
}

impl Service {
    /// As smart HTTP names it: `git-upload-pack` or `git-receive-pack`.
    pub fn name(self) -> &'static str {
        match self {
            Service::UploadPack => "git-upload-pack",
            Service::ReceivePack => "git-receive-pack",
        }
    }

    /// The service a smart-HTTP name stands for.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "git-upload-pack" => Some(Service::UploadPack),
            "git-receive-pack" => Some(Service::ReceivePack),
            _ => None,
        }
    }

    fn capabilities(self) -> &'static str {
        match self {
            Service::UploadPack => upload_pack::CAPABILITIES,
            Service::ReceivePack => receive_pack::CAPABILITIES,
        }
    }
}

/// The body of the answer to a smart-HTTP `GET` of `info/refs?service=git-<service>` from `repo`, in `protocol`, as git's
/// own server gives it. In version 2 (`upload-pack` only): `version 2` and the capabilities, one a line. Otherwise: the
/// service line `# service=git-<service>` and a flush, `version 1` for version 1, and the references with the capabilities:
/// for `upload-pack`, `HEAD` first if it points anywhere, then every reference by name, each annotated tag followed by what
/// it peels to (`^{}`); for `receive-pack`, the references by name alone; the capabilities after a NUL on the first line,
/// and a flush. A repository with no references advertises its
/// capabilities on a line of its own.
pub fn advertise(repo: &gix::Repository, service: Service, protocol: Protocol, out: &mut impl Write) -> Result<()> {
    if service == Service::UploadPack && protocol == Protocol::V2 {
        pkt::text(b"version 2", out)?;
        pkt::text(format!("agent=gix-serve/{}", env!("CARGO_PKG_VERSION")).as_bytes(), out)?;
        for capability in upload_pack::CAPABILITIES_V2 {
            pkt::text(capability.as_bytes(), out)?;
        }
        pkt::text(format!("object-format={}", repo.object_hash()).as_bytes(), out)?;
        return pkt::flush(out);
    }
    pkt::text(format!("# service={}", service.name()).as_bytes(), out)?;
    pkt::flush(out)?;
    if protocol == Protocol::V1 {
        pkt::text(b"version 1", out)?;
    }
    advertise_v0(repo, service, out)
}

fn advertise_v0(repo: &gix::Repository, service: Service, out: &mut impl Write) -> Result<()> {
    let mut refs = advertised_refs(repo)?;
    if service == Service::ReceivePack {
        // A push names the references it updates: `HEAD` and what tags peel to are not among them.
        refs.retain(|(name, _, _)| name != "HEAD");
        refs.iter_mut().for_each(|(_, _, peeled)| *peeled = None);
    }
    let mut capabilities = service.capabilities().to_string();
    // As git does, only for a `HEAD` that points at something and is advertised.
    if let (Some(target), true) = (head_symref(repo)?, refs.iter().any(|(name, _, _)| name == "HEAD")) {
        let _ = write!(capabilities, " symref=HEAD:{target}");
    }
    // In git's order: `agent` after `object-format`. libgit2 (1.9) asserts that something follows `object-format=`'s value,
    // a space or a NUL, so it cannot be the last capability.
    let _ = write!(capabilities, " object-format={}", repo.object_hash());
    let _ = write!(capabilities, " agent=gix-serve/{}", env!("CARGO_PKG_VERSION"));
    if refs.is_empty() {
        let null = repo.object_hash().null();
        pkt::data(format!("{null} capabilities^{{}}\0{capabilities}\n").as_bytes(), out)?;
    }
    for (i, (name, id, peeled)) in refs.iter().enumerate() {
        if i == 0 {
            pkt::data(format!("{id} {name}\0{capabilities}\n").as_bytes(), out)?;
        } else {
            pkt::data(format!("{id} {name}\n").as_bytes(), out)?;
        }
        if let Some(peeled) = peeled {
            pkt::data(format!("{peeled} {name}^{{}}\n").as_bytes(), out)?;
        }
    }
    pkt::flush(out)?;
    Ok(())
}

/// `HEAD`'s target, when it is a symbolic reference, whether or not that exists yet.
pub(crate) fn head_symref(repo: &gix::Repository) -> Result<Option<String>> {
    let name = repo
        .head_name()
        .map_err(|e| Error::new(format!("HEAD cannot be read: {e}")))?;
    Ok(name.map(|name| name.as_bstr().to_string()))
}

pub(crate) type Advertised = (String, gix::ObjectId, Option<gix::ObjectId>);

/// `HEAD` if it resolves, then every reference with what it points to, and what an annotated tag peels to.
pub(crate) fn advertised_refs(repo: &gix::Repository) -> Result<Vec<Advertised>> {
    let mut refs = Vec::new();
    if let Ok(head) = repo.head_id() {
        refs.push(("HEAD".to_string(), head.detach(), None));
    }
    let platform = repo
        .references()
        .map_err(|e| Error::new(format!("the references cannot be listed: {e}")))?;
    let mut named: Vec<Advertised> = Vec::new();
    for reference in platform
        .all()
        .map_err(|e| Error::new(format!("the references cannot be listed: {e}")))?
    {
        let mut reference = reference.map_err(|e| Error::new(format!("a reference cannot be read: {e}")))?;
        let name: &BStr = reference.name().as_bstr();
        let Ok(name) = name.to_str().map(str::to_string) else {
            continue;
        };
        let Some(id) = reference.try_id().map(gix::Id::detach) else {
            continue;
        };
        let peeled = reference
            .peel_to_id()
            .map(gix::Id::detach)
            .ok()
            .filter(|peeled| *peeled != id);
        named.push((name, id, peeled));
    }
    named.sort_by(|a, b| a.0.cmp(&b.0));
    refs.extend(named);
    Ok(refs)
}
