//! Tests for opt-in request body replay on 307/308 redirects.
//!
//! Uses a real TCP server (one request per connection) so the exact request
//! bytes received on every redirect hop can be asserted. The `_test` feature
//! intercepts all connections with an in-memory transport, so these tests
//! only run without it (same pattern as `tcp_eintr.rs`).
#![cfg(not(feature = "_test"))]

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use ureq::http;

#[derive(Debug, Clone)]
struct RecordedRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RecordedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn respond(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Response {
    Response {
        status,
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        body: body.to_vec(),
    }
}

/// A test server handling one request per connection. The handler is called
/// with the 0-based hop index and the received request, and produces the
/// response to send back.
struct Server {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Server {
    fn start(
        handler: impl Fn(usize, &RecordedRequest) -> Response + Send + Sync + 'static,
    ) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let handle = {
            let requests = requests.clone();
            let stop = stop.clone();
            let handler = Arc::new(handler);
            thread::spawn(move || {
                let mut hop = 0;
                while !stop.load(Ordering::Relaxed) {
                    let (mut socket, _) = match listener.accept() {
                        Ok(v) => v,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                        Err(e) => panic!("accept: {e}"),
                    };
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();

                    let req = read_request(&mut socket);
                    requests.lock().unwrap().push(req.clone());
                    let response = handler(hop, &req);
                    write_response(&mut socket, &response);
                    hop += 1;
                }
            })
        };

        Server {
            addr,
            requests,
            stop,
            handle: Some(handle),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    fn recorded(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read the request head (up to and including \r\n\r\n). Returns the head
/// text and any body bytes already read.
fn read_head(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut buf = Vec::new();
    loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            let head = String::from_utf8(buf[..pos].to_vec()).expect("utf8 head");
            let rest = buf[pos + 4..].to_vec();
            return (head, rest);
        }
        let mut tmp = [0u8; 8192];
        let n = socket.read(&mut tmp).expect("read head");
        assert!(n > 0, "connection closed before request head");
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn parse_head(head: &str) -> (String, String, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap().to_string();
    let target = parts.next().unwrap().to_string();
    let headers = lines
        .map(|l| {
            let (k, v) = l.split_once(':').expect("header colon");
            (k.trim().to_string(), v.trim().to_string())
        })
        .collect();
    (method, target, headers)
}

fn read_request(socket: &mut TcpStream) -> RecordedRequest {
    let (head, mut body) = read_head(socket);
    let (method, target, headers) = parse_head(&head);

    // If the client is waiting for a 100-continue, unblock it so it sends
    // the body.
    let expects_100 = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("expect") && v.eq_ignore_ascii_case("100-continue"));
    if expects_100 {
        socket
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .expect("write 100 continue");
    }

    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok());
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });

    let body = if chunked {
        read_chunked(socket, &mut body)
    } else if let Some(len) = content_length {
        while body.len() < len {
            let mut tmp = [0u8; 8192];
            let n = socket.read(&mut tmp).expect("read body");
            assert!(n > 0, "connection closed before full body");
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(len);
        body
    } else {
        Vec::new()
    };

    RecordedRequest {
        method,
        target,
        headers,
        body,
    }
}

fn read_chunked(socket: &mut TcpStream, buf: &mut Vec<u8>) -> Vec<u8> {
    loop {
        if let Some(decoded) = try_decode_chunked(buf) {
            return decoded;
        }
        let mut tmp = [0u8; 8192];
        let n = socket.read(&mut tmp).expect("read chunked body");
        assert!(n > 0, "connection closed during chunked body");
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Decode a complete chunked body (including the 0-size terminating chunk),
/// or return None if more bytes are needed.
fn try_decode_chunked(buf: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    let mut out = Vec::new();
    loop {
        let line_end = find(&buf[pos..], b"\r\n").map(|i| pos + i)?;
        let size_str = std::str::from_utf8(&buf[pos..line_end]).ok()?;
        let size = usize::from_str_radix(size_str.trim(), 16).ok()?;
        pos = line_end + 2;
        if buf.len() < pos + size + 2 {
            return None;
        }
        out.extend_from_slice(&buf[pos..pos + size]);
        pos += size;
        assert_eq!(&buf[pos..pos + 2], b"\r\n", "malformed chunk");
        pos += 2;
        if size == 0 {
            return Some(out);
        }
    }
}

fn write_response(socket: &mut TcpStream, response: &Response) {
    let reason = match response.status {
        200 => "OK",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        _ => "Status",
    };

    let mut head = format!("HTTP/1.1 {} {}\r\n", response.status, reason);

    let mut has_content_length = false;
    let mut has_connection = false;
    for (k, v) in &response.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
        has_content_length |= k.eq_ignore_ascii_case("content-length");
        has_connection |= k.eq_ignore_ascii_case("connection");
    }
    if !has_content_length {
        head.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
    }
    if !has_connection {
        head.push_str("Connection: close\r\n");
    }
    head.push_str("\r\n");

    socket.write_all(head.as_bytes()).expect("write head");
    socket.write_all(&response.body).expect("write body");
    socket.flush().expect("flush");
}

fn agent(replay: bool) -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(replay)
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent()
}

/// Server responding 307 to /source (redirecting to /target) and 200 to /target.
fn server_307() -> Server {
    Server::start(|hop, _| match hop {
        0 => respond(307, &[("Location", "/target")], b""),
        1 => respond(200, &[], b"done"),
        _ => panic!("unexpected hop {hop}"),
    })
}

#[test]
fn default_refuses_307_with_body() {
    let server = server_307();

    let err = agent(false)
        .post(server.url("/source"))
        .send("hello")
        .unwrap_err();

    assert!(matches!(err, ureq::Error::RedirectFailed));

    // The redirect target must not receive anything.
    let rec = server.recorded();
    assert_eq!(rec.len(), 1);
    assert_eq!(rec[0].target, "/source");
    assert_eq!(rec[0].body, b"hello");
}

#[test]
fn replay_enabled_on_agent() {
    let server = server_307();

    let mut res = agent(true)
        .post(server.url("/source"))
        .content_type("text/plain")
        .send("hello replay")
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.body_mut().read_to_string().unwrap(), "done");

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);

    for r in &rec {
        // Method, full body, content type and (automatic) content length
        // are the same on every hop.
        assert_eq!(r.method, "POST");
        assert_eq!(r.body, b"hello replay");
        assert_eq!(r.header("content-type"), Some("text/plain"));
        assert_eq!(r.header("content-length"), Some("12"));
    }
    assert_eq!(rec[0].target, "/source");
    assert_eq!(rec[1].target, "/target");
}

#[test]
fn replay_enabled_per_request() {
    let server = server_307();

    // Agent has the default (off), the request turns it on.
    let res = agent(false)
        .post(server.url("/source"))
        .config()
        .redirect_body_replay(true)
        .build()
        .send("per request")
        .unwrap();

    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"per request");
}

#[test]
fn request_level_off_overrides_agent() {
    let server = server_307();

    // Agent has it on, the request turns it off. Other requests using the
    // same agent are unaffected (covered by replay_enabled_on_agent).
    let err = agent(true)
        .post(server.url("/source"))
        .config()
        .redirect_body_replay(false)
        .build()
        .send("hello")
        .unwrap_err();

    assert!(matches!(err, ureq::Error::RedirectFailed));
    assert_eq!(server.recorded().len(), 1);
}

#[test]
fn http_crate_request_agent_level() {
    let server = server_307();

    let request = http::Request::post(server.url("/source"))
        .body("http crate")
        .unwrap();

    let res = agent(true).run(request).unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"http crate");
}

#[test]
fn http_crate_request_request_level() {
    let server = server_307();

    let agent = agent(false);

    let request = http::Request::post(server.url("/source"))
        .body("http crate")
        .unwrap();

    let request = agent
        .configure_request(request)
        .redirect_body_replay(true)
        .build();

    let res = agent.run(request).unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"http crate");
}

#[test]
fn http_crate_request_request_ext() {
    use ureq::RequestExt;

    let server = server_307();

    let res = http::Request::post(server.url("/source"))
        .body("http crate")
        .unwrap()
        .with_agent(&agent(false))
        .configure()
        .redirect_body_replay(true)
        .run()
        .unwrap();

    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"http crate");
}

#[test]
fn http_crate_request_default_refused() {
    let server = server_307();

    let request = http::Request::post(server.url("/source"))
        .body("http crate")
        .unwrap();

    let err = agent(false).run(request).unwrap_err();
    assert!(matches!(err, ureq::Error::RedirectFailed));
    assert_eq!(server.recorded().len(), 1);
}

#[test]
fn put_and_patch_replayed() {
    let server = Server::start(|hop, _| match hop {
        0 => respond(308, &[("Location", "/t2")], b""),
        1 => respond(307, &[("Location", "/t3")], b""),
        2 => respond(200, &[], b"ok"),
        _ => panic!("unexpected hop {hop}"),
    });

    // PUT with an owned Vec<u8> followed over 308 and 307.
    let res = agent(true)
        .put(server.url("/source"))
        .send(vec![1u8, 2, 3, 4])
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 3);
    for r in &rec {
        assert_eq!(r.method, "PUT");
        assert_eq!(r.body, &[1, 2, 3, 4]);
        assert_eq!(r.header("content-length"), Some("4"));
    }

    // PATCH with a byte array.
    let server = server_307();
    let res = agent(true)
        .patch(server.url("/source"))
        .send(&[9u8; 7])
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    for r in &rec {
        assert_eq!(r.method, "PATCH");
        assert_eq!(r.body, &[9u8; 7]);
    }
}

#[test]
fn borrowed_and_owned_strings_replayed() {
    // &String / String / &Vec<u8> go through the same in-memory path.
    let server = server_307();
    let owned = String::from("owned string");
    let res = agent(true).post(server.url("/source")).send(owned).unwrap();
    assert_eq!(res.status(), 200);
    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"owned string");

    let server = server_307();
    let borrowed = String::from("borrowed string");
    let res = agent(true)
        .post(server.url("/source"))
        .send(&borrowed)
        .unwrap();
    assert_eq!(res.status(), 200);
    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].body, b"borrowed string");
}

#[test]
fn empty_body_replayed() {
    let server = server_307();

    let res = agent(true)
        .post(server.url("/source"))
        .send_empty()
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    for r in &rec {
        assert_eq!(r.method, "POST");
        assert_eq!(r.body, b"");
        assert_eq!(r.header("content-length"), Some("0"));
    }
}

#[cfg(feature = "json")]
#[test]
fn json_body_replayed_identically() {
    use std::sync::atomic::AtomicUsize;

    use serde::ser::{Serialize, SerializeMap, Serializer};

    struct Counting(Arc<AtomicUsize>);

    impl Serialize for Counting {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.0.fetch_add(1, Ordering::Relaxed);
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("key", "value")?;
            map.end()
        }
    }

    let server = server_307();

    let count = Arc::new(AtomicUsize::new(0));
    let res = agent(true)
        .post(server.url("/source"))
        .send_json(Counting(count.clone()))
        .unwrap();
    assert_eq!(res.status(), 200);

    // The user's serialization logic must run exactly once.
    assert_eq!(count.load(Ordering::Relaxed), 1);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);

    // Every hop receives byte-identical bodies.
    assert_eq!(rec[0].body, rec[1].body);
    let expected = serde_json::to_vec_pretty(&serde_json::json!({"key": "value"})).unwrap();
    assert_eq!(rec[1].body, expected);
    assert_eq!(
        rec[1].header("content-type"),
        Some("application/json; charset=utf-8")
    );
}

#[test]
fn form_body_replayed() {
    let server = server_307();

    let res = agent(true)
        .post(server.url("/source"))
        .send_form([("name", "martin"), ("bird", "blue-footed booby")])
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    for r in &rec {
        assert_eq!(r.body, b"name=martin&bird=blue-footed+booby");
        assert_eq!(
            r.header("content-type"),
            Some("application/x-www-form-urlencoded")
        );
    }
}

#[test]
fn chunked_body_replayed() {
    let server = server_307();

    // Caller chooses chunked transfer encoding.
    let res = agent(true)
        .post(server.url("/source"))
        .header("transfer-encoding", "chunked")
        .send("chunked body data")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    for r in &rec {
        assert_eq!(r.header("transfer-encoding"), Some("chunked"));
        assert_eq!(r.header("content-length"), None);
        // The full body is received on every hop. Successful decoding
        // implies the terminating 0-chunk was present.
        assert_eq!(r.body, b"chunked body data");
    }
}

#[test]
fn caller_content_length_preserved() {
    let server = server_307();

    let res = agent(true)
        .post(server.url("/source"))
        .header("content-length", "5")
        .send("hello")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    for r in &rec {
        assert_eq!(r.header("content-length"), Some("5"));
        assert_eq!(r.body, b"hello");
    }
}

#[test]
fn reader_body_not_replayed() {
    // Shared reader.
    let server = server_307();
    let mut data: &[u8] = b"streamed body";
    let err = agent(true)
        .post(server.url("/source"))
        .send(ureq::SendBody::from_reader(&mut data))
        .unwrap_err();
    assert!(matches!(err, ureq::Error::RedirectFailed));
    assert_eq!(server.recorded().len(), 1);

    // Owned reader.
    let server = server_307();
    let err = agent(true)
        .post(server.url("/source"))
        .send(ureq::SendBody::from_owned_reader(io::Cursor::new(
            b"owned".to_vec(),
        )))
        .unwrap_err();
    assert!(matches!(err, ureq::Error::RedirectFailed));
    assert_eq!(server.recorded().len(), 1);
}

#[test]
fn response_body_not_replayed() {
    let server = Server::start(|hop, req| match (hop, req.target.as_str()) {
        (0, "/get") => respond(200, &[], b"response body"),
        (1, "/source") => respond(307, &[("Location", "/target")], b""),
        _ => panic!("unexpected hop {hop}"),
    });

    let agent = agent(true);
    let get = agent.get(server.url("/get")).call().unwrap();
    assert_eq!(get.status(), 200);

    // Using a previous response body as request body cannot be replayed.
    let err = agent.post(server.url("/source")).send(get).unwrap_err();
    assert!(matches!(err, ureq::Error::RedirectFailed));

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].target, "/source");
}

#[test]
fn chained_307_308_relative_locations() {
    let server = Server::start(|hop, _| match hop {
        // Relative location resolved against the current path.
        0 => respond(307, &[("Location", "y")], b""),
        // Root-relative location.
        1 => respond(308, &[("Location", "/z")], b""),
        2 => respond(200, &[], b"ok"),
        _ => panic!("unexpected hop {hop}"),
    });

    let res = agent(true)
        .post(server.url("/a/x"))
        .send("chain body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 3);
    assert_eq!(rec[0].target, "/a/x");
    assert_eq!(rec[1].target, "/a/y");
    assert_eq!(rec[2].target, "/z");
    for r in &rec {
        assert_eq!(r.method, "POST");
        assert_eq!(r.body, b"chain body");
    }
}

#[test]
fn absolute_location_replayed() {
    // Absolute Location pointing back at the same server.
    let target = Arc::new(Mutex::new(String::new()));
    let server = Server::start({
        let target = target.clone();
        move |hop, _| match hop {
            0 => {
                let location = target.lock().unwrap().clone();
                respond(307, &[("Location", &location)], b"")
            }
            1 => respond(200, &[], b"ok"),
            _ => panic!("unexpected hop {hop}"),
        }
    });
    *target.lock().unwrap() = server.url("/target");

    let res = agent(true)
        .post(server.url("/source"))
        .send("absolute")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[0].target, "/source");
    assert_eq!(rec[1].target, "/target");
    assert_eq!(rec[1].body, b"absolute");
}

#[test]
fn mixed_301_then_307_becomes_bodiless_get() {
    let server = Server::start(|hop, _| match hop {
        // POST redirects with 301: method becomes GET and the body is dropped.
        0 => respond(301, &[("Location", "/b")], b""),
        // A later 307 cannot bring the body back.
        1 => respond(307, &[("Location", "/c")], b""),
        2 => respond(200, &[], b"ok"),
        _ => panic!("unexpected hop {hop}"),
    });

    let res = agent(true)
        .post(server.url("/a"))
        .send("lost body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 3);

    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[0].body, b"lost body");

    for r in &rec[1..] {
        assert_eq!(r.method, "GET");
        assert_eq!(r.body, b"");
        assert_eq!(r.header("content-length"), None);
        assert_eq!(r.header("transfer-encoding"), None);
    }
}

#[test]
fn mixed_307_then_301() {
    let server = Server::start(|hop, _| match hop {
        // First a 307: the body is replayed with the same method.
        0 => respond(307, &[("Location", "/b")], b""),
        // Then a 301: method becomes GET and the body is dropped.
        1 => respond(301, &[("Location", "/c")], b""),
        2 => respond(200, &[], b"ok"),
        _ => panic!("unexpected hop {hop}"),
    });

    let res = agent(true).post(server.url("/a")).send("two hops").unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 3);

    assert_eq!(rec[0].method, "POST");
    assert_eq!(rec[1].method, "POST");
    assert_eq!(rec[1].body, b"two hops");

    assert_eq!(rec[2].method, "GET");
    assert_eq!(rec[2].body, b"");
    assert_eq!(rec[2].header("content-length"), None);
    assert_eq!(rec[2].header("transfer-encoding"), None);
}

#[test]
fn max_redirects_zero_returns_response() {
    let server = server_307();

    let agent = ureq::Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .max_redirects(0)
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent();

    let res = agent.post(server.url("/source")).send("hello").unwrap();
    assert_eq!(res.status(), 307);

    assert_eq!(server.recorded().len(), 1);
}

#[test]
fn max_redirects_limit_still_applies() {
    let server = Server::start(|_, _| respond(307, &[("Location", "/loop")], b""));

    let agent = ureq::Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .max_redirects(1)
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent();

    let err = agent.post(server.url("/loop")).send("hello").unwrap_err();
    assert!(matches!(err, ureq::Error::TooManyRedirects));

    // One request plus one followed redirect.
    assert_eq!(server.recorded().len(), 2);
}

#[test]
fn authorization_removed_on_replay_by_default() {
    let server = server_307();

    let res = agent(true)
        .post(server.url("/source"))
        .header("authorization", "Bearer secret")
        .send("hello")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[0].header("authorization"), Some("Bearer secret"));
    assert_eq!(rec[1].header("authorization"), None);
}

#[test]
fn authorization_kept_with_same_host_config() {
    let server = server_307();

    let agent = ureq::Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .redirect_auth_headers(ureq::config::RedirectAuthHeaders::SameHost)
        .timeout_global(Some(Duration::from_secs(10)))
        .build()
        .new_agent();

    let res = agent
        .post(server.url("/source"))
        .header("authorization", "Bearer secret")
        .send("hello")
        .unwrap();
    assert_eq!(res.status(), 200);

    let rec = server.recorded();
    assert_eq!(rec.len(), 2);
    assert_eq!(rec[1].header("authorization"), Some("Bearer secret"));
}

#[test]
fn redirect_during_100_continue_replays_body() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = thread::spawn(move || {
        // Hop 1: answer the Expect: 100-continue with a 307 before any
        // body bytes are read.
        let (mut socket, _) = listener.accept().unwrap();
        let (head, _) = read_head(&mut socket);
        let (_, _, headers) = parse_head(&head);
        assert!(headers.iter().any(
            |(k, v)| k.eq_ignore_ascii_case("expect") && v.eq_ignore_ascii_case("100-continue")
        ));
        socket
            .write_all(
                b"HTTP/1.1 307 Temporary Redirect\r\n\
                  Location: /target\r\n\
                  Content-Length: 0\r\n\
                  Connection: close\r\n\
                  \r\n",
            )
            .unwrap();

        // Hop 2: send 100 Continue, then read the full body.
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (head, mut body) = read_head(&mut socket);
        let (_, target, headers) = parse_head(&head);
        assert_eq!(target, "/target");
        socket.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").unwrap();

        let len: usize = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.parse().ok())
            .expect("content-length");
        while body.len() < len {
            let mut tmp = [0u8; 8192];
            let n = socket.read(&mut tmp).unwrap();
            assert!(n > 0);
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(len);
        assert_eq!(body, b"expect me");

        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
    });

    let res = agent(true)
        .post(format!("http://{addr}/source"))
        .header("expect", "100-continue")
        .send("expect me")
        .unwrap();
    assert_eq!(res.status(), 200);

    handle.join().unwrap();
}

#[test]
fn redirect_during_100_continue_reader_body_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let connections = Arc::new(Mutex::new(0usize));
    let stop = Arc::new(AtomicBool::new(false));

    let handle = {
        let connections = connections.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        *connections.lock().unwrap() += 1;
                        socket.set_nonblocking(false).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let _ = read_head(&mut socket);
                        // Redirect before the body was read. Even though the
                        // body is untouched, a reader cannot be replayed.
                        socket
                            .write_all(
                                b"HTTP/1.1 307 Temporary Redirect\r\n\
                                  Location: /target\r\n\
                                  Content-Length: 0\r\n\
                                  Connection: close\r\n\
                                  \r\n",
                            )
                            .unwrap();
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => panic!("accept: {e}"),
                }
            }
        })
    };

    let mut data: &[u8] = b"unread body";
    let err = agent(true)
        .post(format!("http://{addr}/source"))
        .header("expect", "100-continue")
        .send(ureq::SendBody::from_reader(&mut data))
        .unwrap_err();
    assert!(matches!(err, ureq::Error::RedirectFailed));

    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();

    // The redirect target was never contacted.
    assert_eq!(*connections.lock().unwrap(), 1);
}
