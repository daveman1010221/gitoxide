//! `gix-serve` over smart HTTP against the git CLI in each protocol version (a push into an empty repository, a clone, a push
//! on top of it (a thin pack), a pull into the clone (negotiation with `have`s), a stale push refused, and `git fsck` on both
//! sides), against `git http-backend` for what `ls-refs` and the advertisement list, and by hand for what a pack holds and
//! what a refused push leaves. The HTTP server here is the least a test needs: a body by length or in chunks, gzip taken
//! off, the `Git-Protocol` header passed on.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Where git runs from, and in which protocol version it asks to be served.
struct Env {
    home: PathBuf,
    version: u8,
}

impl Env {
    fn new(tmp: &Path, version: u8) -> Self {
        let home = tmp.join("home");
        std::fs::create_dir_all(&home).unwrap();
        Env { home, version }
    }
}

fn command(dir: &Path, env: &Env, args: &[&str]) -> Command {
    let mut command = Command::new("git");
    command
        .args(["-c", &format!("protocol.version={}", env.version)])
        .args(["-c", "init.defaultBranch=main"])
        .args(["-c", "user.name=Alice", "-c", "user.email=alice@example.org"])
        // Nothing from outside the test: no hooks, no global or system configuration.
        .args(["-c", "core.hooksPath=/nonexistent"])
        .args(args)
        .current_dir(dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_PROTOCOL")
        .env("HOME", &env.home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    command
}

fn git(dir: &Path, env: &Env, args: &[&str]) -> std::process::Output {
    command(dir, env, args).output().expect("git runs")
}

fn ok(dir: &Path, home: &Env, args: &[&str]) -> String {
    let out = git(dir, home, args);
    assert!(
        out.status.success(),
        "git {args:?} failed: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Serves every repository under `root` at `http://127.0.0.1:<port>/<name>`, one request a connection, until the test ends.
fn serve(root: PathBuf) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let root = root.clone();
            std::thread::spawn(move || {
                if let Err(e) = answer(&root, stream) {
                    eprintln!("the test server failed a request: {e}");
                }
            });
        }
    });
    url
}

/// Answers the requests on one connection until the client closes it: clients keep a connection alive between requests.
fn answer(root: &Path, stream: TcpStream) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    while one(root, &mut reader, &stream)? {}
    Ok(())
}

/// Answers one request, or says the connection is closed.
fn one(root: &Path, reader: &mut BufReader<TcpStream>, stream: &TcpStream) -> std::io::Result<bool> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(false);
    }
    let mut parts = line.split_whitespace();
    let (method, target) = (
        parts.next().unwrap_or("").to_string(),
        parts.next().unwrap_or("").to_string(),
    );
    let mut length = None;
    let mut chunked = false;
    let mut gzip = false;
    let mut protocol = None;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').unwrap_or((header, ""));
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => length = value.trim().parse::<usize>().ok(),
            "transfer-encoding" => chunked = value.trim().eq_ignore_ascii_case("chunked"),
            "content-encoding" => gzip = value.trim().eq_ignore_ascii_case("gzip"),
            "git-protocol" => protocol = Some(value.trim().to_string()),
            _ => {}
        }
    }
    let mut body = Vec::new();
    if chunked {
        loop {
            let mut size = String::new();
            reader.read_line(&mut size)?;
            let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
            if size == 0 {
                let mut end = String::new();
                reader.read_line(&mut end)?;
                break;
            }
            let mut chunk = vec![0; size];
            reader.read_exact(&mut chunk)?;
            body.extend(chunk);
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf)?;
        }
    } else if let Some(length) = length {
        body.resize(length, 0);
        reader.read_exact(&mut body)?;
    }
    if gzip {
        let mut plain = Vec::new();
        flate2::read::GzDecoder::new(body.as_slice()).read_to_end(&mut plain)?;
        body = plain;
    }

    let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let path = path.trim_start_matches('/');
    let respond = |status: &str, content_type: &str, payload: &[u8]| -> std::io::Result<()> {
        // One write, as a server sends a response: a client may not cope with headers that arrive in pieces.
        let mut response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-cache\r\n\r\n",
            payload.len()
        )
        .into_bytes();
        response.extend_from_slice(payload);
        let mut stream = stream;
        stream.write_all(&response)?;
        stream.flush()
    };
    let routed = if let Some(name) = path.strip_suffix("/info/refs") {
        let service = query.strip_prefix("service=").and_then(gix_serve::Service::from_name);
        service.map(|s| (name, s, true))
    } else if let Some(name) = path.strip_suffix("/git-upload-pack") {
        Some((name, gix_serve::Service::UploadPack, false))
    } else {
        path.strip_suffix("/git-receive-pack")
            .map(|name| (name, gix_serve::Service::ReceivePack, false))
    };
    let Some((name, service, advertisement)) = routed else {
        respond("404 Not Found", "text/plain", b"not here")?;
        return Ok(true);
    };
    let repo = match gix::open(root.join(name)) {
        Ok(repo) => repo,
        Err(e) => {
            respond("404 Not Found", "text/plain", e.to_string().as_bytes())?;
            return Ok(true);
        }
    };
    let protocol = gix_serve::Protocol::from_header(protocol.as_deref());
    let mut out = Vec::new();
    let served = match (method.as_str(), advertisement, service) {
        ("GET", true, service) => gix_serve::advertise(&repo, service, protocol, &mut out),
        ("POST", false, gix_serve::Service::UploadPack) => {
            gix_serve::upload_pack::serve(&repo, protocol, &body, &mut out)
        }
        ("POST", false, gix_serve::Service::ReceivePack) => {
            gix_serve::receive_pack::serve(&repo, &body, &mut out, |_, _| Ok(()))
        }
        _ => {
            respond("405 Method Not Allowed", "text/plain", b"")?;
            return Ok(true);
        }
    };
    match served {
        Ok(()) => {
            let kind = if advertisement { "advertisement" } else { "result" };
            respond("200 OK", &format!("application/x-{}-{kind}", service.name()), &out)
        }
        Err(e) => respond("500 Internal Server Error", "text/plain", e.to_string().as_bytes()),
    }?;
    Ok(true)
}

fn write(dir: &Path, file: &str, text: &str) {
    let path = dir.join(file);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

#[test]
fn the_git_cli_in_version_0() {
    push_clone_push_again_pull_and_refuse_a_stale_update(0);
}

#[test]
fn the_git_cli_in_version_1() {
    push_clone_push_again_pull_and_refuse_a_stale_update(1);
}

#[test]
fn the_git_cli_in_version_2() {
    push_clone_push_again_pull_and_refuse_a_stale_update(2);
}

fn push_clone_push_again_pull_and_refuse_a_stale_update(version: u8) {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), version);
    let served = tmp.path().join("served");
    std::fs::create_dir_all(&served).unwrap();
    ok(&served, &home, &["init", "--quiet", "--bare", "repo.git"]);
    let url = format!("{}/repo.git", serve(served.clone()));
    let server = served.join("repo.git");

    // A first push into an empty repository: every object, no thin pack possible.
    let author = tmp.path().join("author");
    std::fs::create_dir_all(&author).unwrap();
    ok(&author, &home, &["init", "--quiet"]);
    write(&author, "README", "hello\n");
    write(&author, "src/lib.rs", "pub fn one() -> u8 { 1 }\n");
    ok(&author, &home, &["add", "--all"]);
    ok(&author, &home, &["commit", "--quiet", "-m", "first"]);
    ok(&author, &home, &["push", "--quiet", &url, "main"]);
    let first = ok(&author, &home, &["rev-parse", "HEAD"]);
    assert_eq!(ok(&server, &home, &["rev-parse", "main"]), first, "the push moved main");

    // A clone: the advertisement, then one round with `done`.
    let clone = tmp.path().join("clone");
    ok(tmp.path(), &home, &["clone", "--quiet", &url, clone.to_str().unwrap()]);
    assert_eq!(ok(&clone, &home, &["rev-parse", "HEAD"]), first);
    assert_eq!(
        std::fs::read_to_string(clone.join("src/lib.rs")).unwrap(),
        "pub fn one() -> u8 { 1 }\n"
    );
    // A push on top: the client sends a thin pack, whose bases the server holds.
    write(
        &author,
        "src/lib.rs",
        "pub fn one() -> u8 { 1 }\npub fn two() -> u8 { 2 }\n",
    );
    ok(&author, &home, &["commit", "--quiet", "-am", "second"]);
    write(&author, "docs/notes.md", "# Notes\n");
    ok(&author, &home, &["add", "--all"]);
    ok(&author, &home, &["commit", "--quiet", "-m", "third"]);
    ok(&author, &home, &["push", "--quiet", &url, "main"]);
    let third = ok(&author, &home, &["rev-parse", "HEAD"]);
    assert_eq!(ok(&server, &home, &["rev-parse", "main"]), third);

    // A pull into the clone: it says what it has, and is sent only what it lacks.
    let mut pull = command(&clone, &home, &["pull", "--quiet", "--ff-only"]);
    let pulled = pull.env("GIT_TRACE_PACKET", "1").output().unwrap();
    assert!(pulled.status.success(), "{}", String::from_utf8_lossy(&pulled.stderr));
    let trace = String::from_utf8_lossy(&pulled.stderr);
    assert_eq!(
        trace.contains("command=fetch"),
        version == 2,
        "the pull spoke version {version}:\n{trace}"
    );
    assert_eq!(ok(&clone, &home, &["rev-parse", "HEAD"]), third);
    assert_eq!(
        std::fs::read_to_string(clone.join("docs/notes.md")).unwrap(),
        "# Notes\n"
    );

    // A command that believes main is still `first` is refused as a whole, and nothing moves; one that holds is taken. The git
    // CLI reads the advertisement just before it pushes, so a race is written here by hand.
    ok(&server, &home, &["branch", "scratch", &first]);
    let repo = gix::open(&server).unwrap();
    let request = |commands: &[String]| {
        let mut body = Vec::new();
        for (i, command) in commands.iter().enumerate() {
            let line = if i == 0 {
                format!("{command}\0report-status atomic\n")
            } else {
                format!("{command}\n")
            };
            body.extend(format!("{:04x}", line.len() + 4).as_bytes());
            body.extend(line.as_bytes());
        }
        body.extend(b"0000");
        let mut out = Vec::new();
        gix_serve::receive_pack::serve(&repo, &body, &mut out, |_, _| Ok(())).unwrap();
        String::from_utf8_lossy(&out).to_string()
    };
    let null = "0".repeat(40);
    let answer = request(&[
        format!("{first} {null} refs/heads/main"),
        format!("{first} {null} refs/heads/scratch"),
    ]);
    assert!(
        answer.contains("ng refs/heads/main") && answer.contains("ng refs/heads/scratch"),
        "{answer}"
    );
    assert_eq!(
        ok(&server, &home, &["rev-parse", "main"]),
        third,
        "a stale command moves nothing"
    );
    assert_eq!(
        ok(&server, &home, &["rev-parse", "scratch"]),
        first,
        "nor the other in its transaction"
    );
    let answer = request(&[format!("{first} {null} refs/heads/scratch")]);
    assert!(answer.contains("ok refs/heads/scratch"), "{answer}");
    assert!(
        !git(
            &server,
            &home,
            &["rev-parse", "--verify", "--quiet", "refs/heads/scratch"]
        )
        .status
        .success()
    );

    ok(&server, &home, &["fsck", "--strict"]);
    ok(&clone, &home, &["fsck", "--strict"]);
}

/// libgit2 (1.9) asserts that a space or a NUL follows the value of `object-format=`, so it is never the last capability, and
/// `agent` comes after it as in git's own advertisement.
#[test]
fn object_format_is_never_the_last_capability() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), 0);
    ok(tmp.path(), &home, &["init", "--quiet", "--bare", "repo.git"]);
    let repo = gix::open(tmp.path().join("repo.git")).unwrap();
    for service in [gix_serve::Service::UploadPack, gix_serve::Service::ReceivePack] {
        let mut out = Vec::new();
        gix_serve::advertise(&repo, service, gix_serve::Protocol::V0, &mut out).unwrap();
        let text = String::from_utf8_lossy(&out);
        let capabilities = text.split('\0').nth(1).expect("capabilities after a NUL");
        let capabilities = capabilities.split('\n').next().unwrap();
        assert!(capabilities.contains("object-format=sha1 "), "{capabilities}");
        assert!(
            capabilities
                .trim_end()
                .split(' ')
                .next_back()
                .unwrap()
                .starts_with("agent="),
            "{capabilities}"
        );
    }
}

fn pkt(line: &str) -> Vec<u8> {
    let mut out = format!("{:04x}", line.len() + 4).into_bytes();
    out.extend(line.as_bytes());
    out
}

/// The packet lines of `bytes`: `Some` data, `None` for a flush or delimiter.
fn packets(mut bytes: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut packets = Vec::new();
    while bytes.len() >= 4 {
        let length = usize::from_str_radix(std::str::from_utf8(&bytes[..4]).unwrap(), 16).unwrap();
        if length < 4 {
            packets.push(None);
            bytes = &bytes[4..];
        } else {
            packets.push(Some(bytes[4..length].to_vec()));
            bytes = &bytes[length..];
        }
    }
    assert!(bytes.is_empty(), "the answer ends inside a packet line");
    packets
}

/// What a `git http-backend` run as CGI answers, without its headers.
fn http_backend(root: &Path, home: &Env, path: &str, query: &str, protocol: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut backend = command(root, home, &["-c", "http.receivepack=true", "http-backend"]);
    backend
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("PATH_INFO", path)
        .env("QUERY_STRING", query)
        .env("REQUEST_METHOD", if body.is_empty() { "GET" } else { "POST" })
        .env("CONTENT_TYPE", "application/x-git-upload-pack-request")
        .env("CONTENT_LENGTH", body.len().to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    if let Some(protocol) = protocol {
        backend.env("GIT_PROTOCOL", protocol);
    }
    let mut child = backend.spawn().unwrap();
    child.stdin.take().unwrap().write_all(body).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let at = out
        .stdout
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("CGI headers")
        + 4;
    out.stdout[at..].to_vec()
}

/// A repository with two branches, an annotated tag and a lightweight one, `HEAD` at `main`, and an empty one beside it.
fn refs_to_list(tmp: &Path, home: &Env) -> PathBuf {
    let root = tmp.join("served");
    let work = tmp.join("work");
    std::fs::create_dir_all(&work).unwrap();
    ok(&work, home, &["init", "--quiet"]);
    write(&work, "README", "hello\n");
    ok(&work, home, &["add", "--all"]);
    ok(&work, home, &["commit", "--quiet", "-m", "first"]);
    ok(&work, home, &["tag", "-a", "-m", "one", "v1"]);
    ok(&work, home, &["tag", "light"]);
    ok(&work, home, &["checkout", "--quiet", "-b", "topic"]);
    write(&work, "README", "hello again\n");
    ok(&work, home, &["commit", "--quiet", "-am", "second"]);
    ok(&work, home, &["checkout", "--quiet", "main"]);
    std::fs::create_dir_all(&root).unwrap();
    ok(
        &root,
        home,
        &["clone", "--quiet", "--bare", work.to_str().unwrap(), "repo.git"],
    );
    ok(
        &root.join("repo.git"),
        home,
        &["fetch", "--quiet", "origin", "topic:topic"],
    );
    ok(&root, home, &["init", "--quiet", "--bare", "empty.git"]);
    root
}

#[test]
fn ls_refs_lists_what_git_http_backend_lists() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), 2);
    let root = refs_to_list(tmp.path(), &home);
    let cases: &[(&str, &[&str])] = &[
        ("repo.git", &[]),
        ("repo.git", &["symrefs"]),
        ("repo.git", &["peel"]),
        ("repo.git", &["symrefs", "peel", "unborn"]),
        ("repo.git", &["peel", "ref-prefix refs/tags/"]),
        ("repo.git", &["symrefs", "ref-prefix refs/heads/m", "ref-prefix HEAD"]),
        ("empty.git", &[]),
        ("empty.git", &["symrefs"]),
        ("empty.git", &["symrefs", "unborn"]),
    ];
    for (name, arguments) in cases {
        let mut body = pkt("command=ls-refs\n");
        body.extend(pkt("object-format=sha1\n"));
        body.extend(b"0001");
        for argument in *arguments {
            body.extend(pkt(&format!("{argument}\n")));
        }
        body.extend(b"0000");
        let expected = http_backend(
            &root,
            &home,
            &format!("/{name}/git-upload-pack"),
            "",
            Some("version=2"),
            &body,
        );
        let repo = gix::open(root.join(name)).unwrap();
        let mut ours = Vec::new();
        gix_serve::upload_pack::serve(&repo, gix_serve::Protocol::V2, &body, &mut ours).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&ours),
            String::from_utf8_lossy(&expected),
            "{name} {arguments:?}"
        );
    }
}

/// The references a version 0 or 1 advertisement lists, capabilities aside, and the lines before them.
#[test]
fn the_advertisement_lists_what_git_http_backend_lists() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), 0);
    let root = refs_to_list(tmp.path(), &home);
    let without_capabilities = |answer: &[u8]| -> Vec<Option<Vec<u8>>> {
        packets(answer)
            .into_iter()
            .map(|p| {
                p.map(|line| match line.iter().position(|b| *b == 0) {
                    Some(nul) => [&line[..nul], b"\n"].concat(),
                    None => line,
                })
            })
            .collect()
    };
    for name in ["repo.git", "empty.git"] {
        for (service, protocol, header) in [
            (gix_serve::Service::UploadPack, gix_serve::Protocol::V0, None),
            (
                gix_serve::Service::UploadPack,
                gix_serve::Protocol::V1,
                Some("version=1"),
            ),
            (gix_serve::Service::ReceivePack, gix_serve::Protocol::V0, None),
            (
                gix_serve::Service::ReceivePack,
                gix_serve::Protocol::V1,
                Some("version=1"),
            ),
            // There is no version 2 push: it is answered in version 0. git's own server leaves out the service line then,
            // which a client takes for a server that is not smart; this one keeps it, so only what follows is compared.
            (
                gix_serve::Service::ReceivePack,
                gix_serve::Protocol::V2,
                Some("version=2"),
            ),
        ] {
            let expected = http_backend(
                &root,
                &home,
                &format!("/{name}/info/refs"),
                &format!("service={}", service.name()),
                header,
                b"",
            );
            let repo = gix::open(root.join(name)).unwrap();
            let mut ours = Vec::new();
            gix_serve::advertise(&repo, service, protocol, &mut ours).unwrap();
            let mut ours = without_capabilities(&ours);
            if protocol == gix_serve::Protocol::V2 {
                assert_eq!(ours[0], Some(b"# service=git-receive-pack\n".to_vec()));
                ours.drain(..2);
            }
            assert_eq!(ours, without_capabilities(&expected), "{name} {service:?} {protocol:?}");
        }
    }
    // Version 2's capabilities, though not git's own, are announced in its shape: `version 2` first, then one a line.
    let repo = gix::open(root.join("repo.git")).unwrap();
    let mut ours = Vec::new();
    gix_serve::advertise(
        &repo,
        gix_serve::Service::UploadPack,
        gix_serve::Protocol::V2,
        &mut ours,
    )
    .unwrap();
    let lines = packets(&ours);
    assert_eq!(lines.first(), Some(&Some(b"version 2\n".to_vec())));
    assert_eq!(lines.last(), Some(&None));
}

/// The pack in an answer: side band 1's data, after whatever lines came before it.
fn pack_in(answer: &[u8]) -> Vec<u8> {
    packets(answer)
        .into_iter()
        .flatten()
        .filter(|line| line.first() == Some(&1))
        .flat_map(|line| line[1..].to_vec())
        .collect()
}

fn objects_in(pack: &[u8]) -> u32 {
    assert_eq!(&pack[..4], b"PACK", "a pack");
    u32::from_be_bytes(pack[8..12].try_into().unwrap())
}

/// A client that has the first commit and wants the second, which changes one file in a directory, is sent four objects: the
/// commit, the root tree, the directory's tree and the file. The tag on the second commit comes too when asked for.
#[test]
fn a_fetch_is_sent_only_what_the_client_lacks() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), 0);
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    ok(&work, &home, &["init", "--quiet"]);
    write(&work, "README", "hello\n");
    write(&work, "src/lib.rs", "pub fn one() -> u8 { 1 }\n");
    write(&work, "src/main.rs", "fn main() {}\n");
    write(&work, "docs/notes.md", "# Notes\n");
    ok(&work, &home, &["add", "--all"]);
    ok(&work, &home, &["commit", "--quiet", "-m", "first"]);
    let first = ok(&work, &home, &["rev-parse", "HEAD"]);
    write(
        &work,
        "src/lib.rs",
        "pub fn one() -> u8 { 1 }\npub fn two() -> u8 { 2 }\n",
    );
    ok(&work, &home, &["commit", "--quiet", "-am", "second"]);
    let second = ok(&work, &home, &["rev-parse", "HEAD"]);
    ok(&work, &home, &["tag", "-a", "-m", "two", "v2"]);
    let repo = gix::open(&work).unwrap();

    let v2 = |arguments: &[String]| {
        let mut body = pkt("command=fetch\n");
        body.extend(b"0001");
        for argument in arguments {
            body.extend(pkt(&format!("{argument}\n")));
        }
        body.extend(b"0000");
        let mut out = Vec::new();
        gix_serve::upload_pack::serve(&repo, gix_serve::Protocol::V2, &body, &mut out).unwrap();
        out
    };
    let want = format!("want {second}");
    let have = format!("have {first}");
    let done = "done".to_string();
    assert_eq!(
        objects_in(&pack_in(&v2(&[want.clone(), have.clone(), done.clone()]))),
        4
    );
    assert_eq!(
        objects_in(&pack_in(&v2(&[
            want.clone(),
            have.clone(),
            done.clone(),
            "include-tag".into()
        ]))),
        5
    );
    // Without `done`: what is in common settles the pack, so it is ready and the pack follows.
    let answer = v2(&[want.clone(), have.clone()]);
    let lines = packets(&answer);
    assert!(
        lines.contains(&Some(format!("ACK {first}\n").into_bytes())),
        "{lines:?}"
    );
    assert!(lines.contains(&Some(b"ready\n".to_vec())), "{lines:?}");
    assert_eq!(objects_in(&pack_in(&answer)), 4);
    // With nothing in common it is not ready, and no pack is sent.
    let lines = packets(&v2(&[want.clone(), format!("have {}", "1".repeat(40))]));
    assert_eq!(
        lines,
        vec![Some(b"acknowledgments\n".to_vec()), Some(b"NAK\n".to_vec()), None]
    );
    // A clone is sent everything: two commits, three trees and two changed ones, four blobs and the changed one.
    assert_eq!(objects_in(&pack_in(&v2(&[want.clone(), done.clone()]))), 2 + 5 + 5);

    let mut body = pkt(&format!("want {second} multi_ack_detailed side-band-64k ofs-delta\n"));
    body.extend(b"0000");
    body.extend(pkt(&format!("have {first}\n")));
    body.extend(pkt("done\n"));
    let mut out = Vec::new();
    gix_serve::upload_pack::serve(&repo, gix_serve::Protocol::V0, &body, &mut out).unwrap();
    assert_eq!(packets(&out)[0], Some(format!("ACK {first}\n").into_bytes()));
    assert_eq!(objects_in(&pack_in(&out)), 4);
}

/// A push that is refused leaves no object and no quarantine behind, though what judged it could read the pushed objects; the
/// same push allowed is taken whole.
#[test]
fn a_refused_push_leaves_nothing_behind() {
    let tmp = tempfile::tempdir().unwrap();
    let home = Env::new(tmp.path(), 0);
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    ok(&work, &home, &["init", "--quiet"]);
    write(&work, "README", "hello\n");
    ok(&work, &home, &["add", "--all"]);
    ok(&work, &home, &["commit", "--quiet", "-m", "first"]);
    let first = ok(&work, &home, &["rev-parse", "HEAD"]);
    let server = tmp.path().join("server.git");
    ok(
        tmp.path(),
        &home,
        &["clone", "--quiet", "--bare", work.to_str().unwrap(), "server.git"],
    );
    write(&work, "README", "hello again\n");
    ok(&work, &home, &["commit", "--quiet", "-am", "second"]);
    let second = ok(&work, &home, &["rev-parse", "HEAD"]);

    let mut packer = command(
        &work,
        &home,
        &["pack-objects", "--stdout", "--revs", "--thin", "--quiet"],
    );
    let mut packer = packer
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    packer
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{second}\n^{first}\n").as_bytes())
        .unwrap();
    let pack = packer.wait_with_output().unwrap().stdout;
    let mut body = pkt(&format!("{first} {second} refs/heads/main\0report-status\n"));
    body.extend(b"0000");
    body.extend(&pack);

    let listing = |dir: &Path| {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    let objects_before = listing(&server.join("objects"));
    let packs_before = listing(&server.join("objects/pack"));
    let second_id = gix::ObjectId::from_hex(second.as_bytes()).unwrap();
    let repo = gix::open(&server).unwrap();
    let mut out = Vec::new();
    gix_serve::receive_pack::serve(&repo, &body, &mut out, |quarantined, commands| {
        assert_eq!(commands.len(), 1);
        let commit = quarantined
            .find_commit(second_id)
            .expect("the pushed commit is readable");
        assert_eq!(commit.message_raw_sloppy(), "second\n");
        assert!(
            !gix::open(&server).unwrap().has_object(second_id),
            "nothing outside sees it"
        );
        Err("not this one".into())
    })
    .unwrap();
    let answer = String::from_utf8_lossy(&out);
    assert!(
        answer.contains("unpack ok") && answer.contains("ng refs/heads/main not this one"),
        "{answer}"
    );
    assert_eq!(ok(&server, &home, &["rev-parse", "main"]), first);
    assert!(!git(&server, &home, &["cat-file", "-e", &second]).status.success());
    assert_eq!(
        listing(&server.join("objects")),
        objects_before,
        "no quarantine is left"
    );
    assert_eq!(listing(&server.join("objects/pack")), packs_before, "no pack is taken");

    let mut out = Vec::new();
    gix_serve::receive_pack::serve(&repo, &body, &mut out, |_, _| Ok(())).unwrap();
    let answer = String::from_utf8_lossy(&out);
    assert!(answer.contains("ok refs/heads/main"), "{answer}");
    assert_eq!(ok(&server, &home, &["rev-parse", "main"]), second);
    assert_eq!(
        listing(&server.join("objects")),
        objects_before,
        "no quarantine is left"
    );
    ok(&server, &home, &["fsck", "--strict"]);
}
