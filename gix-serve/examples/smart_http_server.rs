//! Serves every repository under a directory over smart HTTP, protocol version 0, for trying clients by hand:
//! `cargo run -p gix-serve --example smart_http_server -- <dir> [port]`, then `git clone http://127.0.0.1:<port>/<name>`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};

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

    if std::env::var_os("GIX_SERVE_TRACE").is_some() {
        eprintln!(
            ">>> {method} {target} ({} bytes)\n{}",
            body.len(),
            String::from_utf8_lossy(&body[..body.len().min(400)])
        );
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
    if std::env::var_os("GIX_SERVE_TRACE").is_some() {
        eprintln!(
            "<<< {} bytes\n{}",
            out.len(),
            String::from_utf8_lossy(&out[..out.len().min(300)])
        );
    }
    match served {
        Ok(()) => {
            let kind = if advertisement { "advertisement" } else { "result" };
            respond("200 OK", &format!("application/x-{}-{kind}", service.name()), &out)
        }
        Err(e) => respond("500 Internal Server Error", "text/plain", e.to_string().as_bytes()),
    }?;
    Ok(true)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().expect("the directory of repositories to serve"));
    let port: u16 = args.next().map_or(8973, |p| p.parse().expect("a port"));
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("the port is free");
    eprintln!("serving {} at http://127.0.0.1:{port}/", root.display());
    for stream in listener.incoming().flatten() {
        let root = root.clone();
        std::thread::spawn(move || {
            if let Err(e) = answer(&root, stream) {
                eprintln!("a request failed: {e}");
            }
        });
    }
}
