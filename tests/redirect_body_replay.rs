// Tests for replaying request bodies on 307/308 redirects.
//
// These tests use a real local TCP server, because the `_test` transport
// cannot inspect request bodies.
#![cfg(not(feature = "_test"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ureq::{Agent, Error, SendBody};

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn new(status: u16) -> Self {
        Response {
            status,
            headers: vec![],
            body: vec![],
        }
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    fn body(mut self, body: &[u8]) -> Self {
        self.body = body.to_vec();
        self
    }
}

enum Reply {
    /// Respond without reading the request body. For use with
    /// `Expect: 100-continue` requests, where the client waits
    /// for the server before sending the body.
    Now(Response),
    /// Read the request body (answering 100-continue if asked),
    /// record it, then respond.
    WithBody(Response),
}

struct Server {
    addr: SocketAddr,
    received: Arc<Mutex<Vec<Recorded>>>,
}

impl Server {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    fn received(&self) -> Vec<Recorded> {
        self.received.lock().unwrap().clone()
    }
}

/// Start a server that handles one request per connection and records it.
fn serve<F>(handler: F) -> Server
where
    F: Fn(SocketAddr, &Recorded) -> Reply + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let received = Arc::new(Mutex::new(vec![]));
    let received2 = received.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            handle_conn(stream, &handler, &received2, addr);
        }
    });

    Server { addr, received }
}

fn handle_conn<F>(
    stream: TcpStream,
    handler: &F,
    received: &Arc<Mutex<Vec<Recorded>>>,
    addr: SocketAddr,
) where
    F: Fn(SocketAddr, &Recorded) -> Reply,
{
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();

    let mut reader = BufReader::new(&stream);

    let mut req = match read_head(&mut reader) {
        Ok(r) => r,
        Err(_) => return,
    };

    let reply = handler(addr, &req);

    let response = match reply {
        Reply::Now(r) => r,
        Reply::WithBody(r) => {
            let wants_100 = req
                .header("expect")
                .map(|v| v.eq_ignore_ascii_case("100-continue"))
                .unwrap_or(false);
            if wants_100
                && (&stream)
                    .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                    .is_err()
            {
                return;
            }
            match read_body(&mut reader, &req) {
                Ok(b) => req.body = b,
                Err(_) => return,
            }
            r
        }
    };

    received.lock().unwrap().push(req);

    let mut out = format!("HTTP/1.1 {} OK\r\n", response.status);
    for (k, v) in &response.headers {
        out.push_str(&format!("{}: {}\r\n", k, v));
    }
    out.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        response.body.len()
    ));

    if (&stream).write_all(out.as_bytes()).is_err() {
        return;
    }
    if response.body.is_empty() {
        return;
    }
    let _ = (&stream).write_all(&response.body);
}

fn read_head(reader: &mut BufReader<&TcpStream>) -> std::io::Result<Recorded> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.trim().split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();

    let mut headers = vec![];
    loop {
        let mut l = String::new();
        reader.read_line(&mut l)?;
        let t = l.trim();
        if t.is_empty() {
            break;
        }
        if let Some((k, v)) = t.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    Ok(Recorded {
        method,
        target,
        headers,
        body: vec![],
    })
}

fn read_body(reader: &mut BufReader<&TcpStream>, req: &Recorded) -> std::io::Result<Vec<u8>> {
    if let Some(te) = req.header("transfer-encoding") {
        if te.to_ascii_lowercase().contains("chunked") {
            let mut body = vec![];
            loop {
                let mut size_line = String::new();
                reader.read_line(&mut size_line)?;
                let size_str = size_line.trim().split(';').next().unwrap();
                let size = usize::from_str_radix(size_str, 16)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                if size == 0 {
                    // Trailers (if any) up to the empty line.
                    loop {
                        let mut l = String::new();
                        reader.read_line(&mut l)?;
                        if l.trim().is_empty() {
                            break;
                        }
                    }
                    break;
                }
                let mut chunk = vec![0u8; size];
                reader.read_exact(&mut chunk)?;
                body.extend_from_slice(&chunk);
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf)?;
            }
            return Ok(body);
        }
    }

    if let Some(cl) = req.header("content-length") {
        let n: usize = cl.parse().unwrap();
        let mut body = vec![0u8; n];
        reader.read_exact(&mut body)?;
        return Ok(body);
    }

    Ok(vec![])
}

fn agent(replay: bool) -> Agent {
    Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(replay)
        .build()
        .into()
}

/// Server answering 307 for /a and 200 for /b.
fn redirect_307_server() -> Server {
    serve(|addr, req| {
        if req.target == "/a" {
            Reply::WithBody(Response::new(307).header("Location", &format!("http://{}/b", addr)))
        } else {
            Reply::WithBody(Response::new(200).body(b"made it"))
        }
    })
}

#[test]
fn post_307_replays_body_when_enabled() {
    let server = redirect_307_server();

    let mut res = agent(true)
        .post(&server.url("/a"))
        .send("hello body")
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.body_mut().read_to_string().unwrap(), "made it");

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].method, "POST");
    assert_eq!(received[1].method, "POST");
    assert_eq!(received[1].target, "/b");
    assert_eq!(received[0].body, b"hello body");
    assert_eq!(received[1].body, b"hello body");
    // Auto-generated Content-Length covers the full body on every hop.
    assert_eq!(received[0].header("content-length"), Some("10"));
    assert_eq!(received[1].header("content-length"), Some("10"));
}

#[test]
fn post_307_fails_by_default() {
    let server = redirect_307_server();

    let err = agent(false)
        .post(&server.url("/a"))
        .send("hello body")
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));

    // The redirect target must not receive anything.
    let received = server.received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].target, "/a");
}

#[test]
fn request_level_config_overrides_agent() {
    let server = redirect_307_server();

    // Agent level off, request level on.
    let res = agent(false)
        .post(&server.url("/a"))
        .config()
        .redirect_body_replay(true)
        .build()
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(server.received().len(), 2);

    // Agent level on, request level off.
    let err = agent(true)
        .post(&server.url("/a"))
        .config()
        .redirect_body_replay(false)
        .build()
        .send("hello body")
        .unwrap_err();
    assert!(matches!(err, Error::RedirectFailed));

    // The request level choice does not affect other requests on the agent.
    let agent = agent(true);
    let res = agent.post(&server.url("/a")).send("hello body").unwrap();
    assert_eq!(res.status(), 200);
}

#[test]
fn http_crate_request_api_replays() {
    let server = redirect_307_server();

    let agent = agent(false);

    let request = ureq::http::Request::post(server.url("/a"))
        .body("hello body")
        .unwrap();

    let request = agent
        .configure_request(request)
        .redirect_body_replay(true)
        .build();

    let res = agent.run(request).unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].method, "POST");
    assert_eq!(received[1].body, b"hello body");
}

#[test]
fn put_308_replays_owned_vec() {
    let server = serve(|addr, req| {
        if req.target == "/a" {
            Reply::WithBody(Response::new(308).header("Location", &format!("http://{}/b", addr)))
        } else {
            Reply::WithBody(Response::new(200))
        }
    });

    let data = vec![7u8; 1000];
    let res = agent(true)
        .put(&server.url("/a"))
        .send(data.clone())
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].method, "PUT");
    assert_eq!(received[1].method, "PUT");
    assert_eq!(received[0].body, data);
    assert_eq!(received[1].body, data);
}

#[test]
fn patch_307_relative_location() {
    let server = serve(|_addr, req| {
        if req.target == "/dir/a" {
            // Relative location resolved against the base path.
            Reply::WithBody(Response::new(307).header("Location", "b"))
        } else {
            Reply::WithBody(Response::new(200))
        }
    });

    let res = agent(true)
        .patch(&server.url("/dir/a"))
        .send("patch data")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].method, "PATCH");
    assert_eq!(received[1].target, "/dir/b");
    assert_eq!(received[1].body, b"patch data");
}

#[test]
fn consecutive_redirects_replay_each_hop() {
    let server = serve(|addr, req| match req.target.as_str() {
        "/a" => {
            Reply::WithBody(Response::new(307).header("Location", &format!("http://{}/b", addr)))
        }
        "/b" => {
            Reply::WithBody(Response::new(308).header("Location", &format!("http://{}/c", addr)))
        }
        _ => Reply::WithBody(Response::new(200)),
    });

    let res = agent(true).post(&server.url("/a")).send("again").unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 3);
    for req in &received {
        assert_eq!(req.method, "POST");
        assert_eq!(req.body, b"again");
    }
}

#[test]
#[cfg(feature = "json")]
fn json_body_replayed_identically() {
    let server = redirect_307_server();

    // A Serialize impl that panics if invoked more than once. Redirects
    // must resend the already-serialized bytes, not re-serialize.
    struct Once(std::sync::atomic::AtomicUsize);
    impl serde::Serialize for Once {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            let n = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(n == 0, "value serialized more than once");
            s.serialize_str("the json value")
        }
    }

    let res = agent(true)
        .post(&server.url("/a"))
        .send_json(Once(0.into()))
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert!(!received[0].body.is_empty());
    assert_eq!(received[0].body, received[1].body);
    assert_eq!(
        received[1].header("content-type"),
        Some("application/json; charset=utf-8")
    );
}

#[test]
fn form_body_replayed_identically() {
    let server = redirect_307_server();

    let form = [("key 1", "value 1"), ("key2", "välue2")];
    let res = agent(true).post(&server.url("/a")).send_form(form).unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert!(!received[0].body.is_empty());
    assert_eq!(received[0].body, received[1].body);
    assert_eq!(
        received[1].header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
}

#[test]
fn empty_body_replays() {
    let server = redirect_307_server();

    let res = agent(true).post(&server.url("/a")).send_empty().unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].method, "POST");
    assert_eq!(received[1].body, b"");
}

#[test]
fn chunked_body_replays_complete() {
    let server = redirect_307_server();

    let body = "chunked body content";
    let res = agent(true)
        .post(&server.url("/a"))
        .header("transfer-encoding", "chunked")
        .send(body)
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    for req in &received {
        // The server only finishes reading the body if it is complete,
        // including the terminating chunk.
        assert_eq!(req.body, body.as_bytes());
        assert!(req.header("content-length").is_none());
    }
}

#[test]
fn caller_content_length_is_preserved() {
    let server = redirect_307_server();

    let res = agent(true)
        .post(&server.url("/a"))
        .header("content-length", "10")
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].header("content-length"), Some("10"));
    assert_eq!(received[1].header("content-length"), Some("10"));
    assert_eq!(received[1].body, b"hello body");
}

#[test]
fn content_type_is_preserved() {
    let server = redirect_307_server();

    let res = agent(true)
        .post(&server.url("/a"))
        .content_type("text/plain; charset=utf-8")
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(
        received[1].header("content-type"),
        Some("text/plain; charset=utf-8")
    );
}

#[test]
fn reader_body_is_not_replayed() {
    let server = redirect_307_server();

    let mut reader = std::io::Cursor::new(b"reader body".to_vec());
    let err = agent(true)
        .post(&server.url("/a"))
        .send(SendBody::from_reader(&mut reader))
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));

    let received = server.received();
    assert_eq!(received.len(), 1);
}

#[test]
fn reader_body_with_declared_length_is_not_replayed() {
    let server = redirect_307_server();

    let mut reader = std::io::Cursor::new(b"reader body".to_vec());
    let err = agent(true)
        .post(&server.url("/a"))
        // Even with a declared length, a reader cannot be replayed.
        .header("content-length", "11")
        .send(SendBody::from_reader(&mut reader))
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));

    let received = server.received();
    assert_eq!(received.len(), 1);
}

#[test]
fn response_body_is_not_replayed() {
    let server = redirect_307_server();

    let agent = agent(true);

    // Get a body to use as request body.
    let source = serve(|_addr, _req| Reply::WithBody(Response::new(200).body(b"source body")));
    let res = agent.get(&source.url("/source")).call().unwrap();
    let (parts, body) = res.into_parts();
    let _ = parts;

    let err = agent.post(&server.url("/a")).send(body).unwrap_err();
    assert!(matches!(err, Error::RedirectFailed));

    assert_eq!(server.received().len(), 1);
}

#[test]
fn expect_100_redirect_replays_replayable_body() {
    let server = serve(|addr, req| {
        if req.target == "/a" {
            // Redirect during the 100-continue phase, before the body is sent.
            Reply::Now(Response::new(307).header("Location", &format!("http://{}/b", addr)))
        } else {
            Reply::WithBody(Response::new(200))
        }
    });

    let res = agent(true)
        .post(&server.url("/a"))
        .header("expect", "100-continue")
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    // The first hop never received a body.
    assert_eq!(received[0].body, b"");
    // The full body is sent to the redirect target.
    assert_eq!(received[1].body, b"hello body");
}

#[test]
fn expect_100_redirect_does_not_replay_reader() {
    let server = serve(|addr, req| {
        if req.target == "/a" {
            // Redirect during the 100-continue phase, before the body is sent.
            Reply::Now(Response::new(307).header("Location", &format!("http://{}/b", addr)))
        } else {
            Reply::WithBody(Response::new(200))
        }
    });

    // Even though the body was never read for the first request, a reader
    // cannot be replayed.
    let mut reader = std::io::Cursor::new(b"reader body".to_vec());
    let err = agent(true)
        .post(&server.url("/a"))
        .header("expect", "100-continue")
        .header("content-length", "11")
        .send(SendBody::from_reader(&mut reader))
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));
    assert_eq!(server.received().len(), 1);
}

#[test]
fn reader_error_surfaces_on_first_send() {
    struct FailingReader;
    impl Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "nope"))
        }
    }

    let server = redirect_307_server();

    let err = agent(true)
        .post(&server.url("/a"))
        .send(SendBody::from_owned_reader(FailingReader))
        .unwrap_err();

    assert!(matches!(err, Error::Io(_)), "unexpected: {:?}", err);
}

#[test]
fn mixed_redirect_chain_302_then_307() {
    let server = serve(|addr, req| match req.target.as_str() {
        "/a" => {
            Reply::WithBody(Response::new(302).header("Location", &format!("http://{}/b", addr)))
        }
        "/b" => {
            Reply::WithBody(Response::new(307).header("Location", &format!("http://{}/c", addr)))
        }
        _ => Reply::WithBody(Response::new(200)),
    });

    let res = agent(true)
        .post(&server.url("/a"))
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 3);
    assert_eq!(received[0].method, "POST");
    // The 302 turned the request into a GET without a body. The later 307
    // cannot bring the original body back.
    assert_eq!(received[1].method, "GET");
    assert_eq!(received[1].body, b"");
    assert!(received[1].header("content-length").is_none());
    assert!(received[1].header("transfer-encoding").is_none());
    assert_eq!(received[2].method, "GET");
    assert_eq!(received[2].body, b"");
    assert!(received[2].header("content-length").is_none());
    assert!(received[2].header("transfer-encoding").is_none());
}

#[test]
fn max_redirects_still_apply_with_replay() {
    let server = serve(|addr, _req| {
        Reply::WithBody(Response::new(307).header("Location", &format!("http://{}/a", addr)))
    });

    let agent: Agent = Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .max_redirects(2)
        .build()
        .into();

    let err = agent
        .post(&server.url("/a"))
        .send("hello body")
        .unwrap_err();
    assert!(matches!(err, Error::TooManyRedirects));

    // With max_redirects_will_error(false), the last response is returned.
    let agent: Agent = Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .max_redirects(2)
        .max_redirects_will_error(false)
        .build()
        .into();

    let res = agent.post(&server.url("/a")).send("hello body").unwrap();
    assert_eq!(res.status(), 307);
}

#[test]
fn authorization_header_removed_on_replay() {
    let server = redirect_307_server();

    let res = agent(true)
        .post(&server.url("/a"))
        .header("authorization", "Bearer secret")
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].header("authorization"), Some("Bearer secret"));
    // Default RedirectAuthHeaders::Never drops the header on redirect.
    assert_eq!(received[1].header("authorization"), None);
}

#[test]
fn authorization_header_kept_when_same_host() {
    let server = redirect_307_server();

    let agent: Agent = Agent::config_builder()
        .proxy(None)
        .redirect_body_replay(true)
        .redirect_auth_headers(ureq::config::RedirectAuthHeaders::SameHost)
        .build()
        .into();

    let res = agent
        .post(&server.url("/a"))
        .header("authorization", "Bearer secret")
        .send("hello body")
        .unwrap();
    assert_eq!(res.status(), 200);

    let received = server.received();
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].header("authorization"), Some("Bearer secret"));
}
