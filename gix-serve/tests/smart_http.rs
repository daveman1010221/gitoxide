//! The git CLI against `gix-serve` over smart HTTP, protocol version 0: a push into an empty repository, a clone, a push on
//! top of it (a thin pack), a pull into the clone (negotiation with `have`s), a stale push refused, and `git fsck` on both
//! sides. The HTTP server here is the least a test needs: one request a connection, a body by length or in chunks, gzip
//! taken off.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;

fn git(dir: &Path, home: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
        .args(["-c", "protocol.version=0", "-c", "init.defaultBranch=main"])
        .args(["-c", "user.name=Alice", "-c", "user.email=alice@example.org"])
        // Nothing from outside the test: no hooks, no global or system configuration.
        .args(["-c", "core.hooksPath=/nonexistent"])
        .args(args)
        .current_dir(dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env("HOME", home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("git runs")
}

fn ok(dir: &Path, home: &Path, args: &[&str]) -> String {
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
    let mut out = Vec::new();
    let served = match (method.as_str(), advertisement, service) {
        ("GET", true, service) => {
            gix_serve::service_header(service, &mut out).and_then(|()| gix_serve::advertise(&repo, service, &mut out))
        }
        ("POST", false, gix_serve::Service::UploadPack) => gix_serve::upload_pack::serve(&repo, &body, &mut out),
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
fn the_git_cli_pushes_clones_pushes_again_and_pulls_and_a_stale_update_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let served = tmp.path().join("served");
    std::fs::create_dir_all(&home).unwrap();
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
    ok(&clone, &home, &["pull", "--quiet", "--ff-only"]);
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
    let home = tmp.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    ok(tmp.path(), &home, &["init", "--quiet", "--bare", "repo.git"]);
    let repo = gix::open(tmp.path().join("repo.git")).unwrap();
    for service in [gix_serve::Service::UploadPack, gix_serve::Service::ReceivePack] {
        let mut out = Vec::new();
        gix_serve::advertise(&repo, service, &mut out).unwrap();
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
