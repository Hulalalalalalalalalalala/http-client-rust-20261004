// Tests for resending a request once when a pooled connection turns out
// to be dropped by the server (retry_on_disconnect).
//
// These tests use a real local TCP server, because the `_test` transport
// cannot drop connections mid-conversation.
#![cfg(not(feature = "_test"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ureq::{Agent, Error, SendBody};

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
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

struct Server {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Recorded>>>,
}

impl Server {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn received(&self) -> Vec<Recorded> {
        self.received.lock().unwrap().clone()
    }
}

/// Read the request head (request line + headers). Does not read the body.
fn read_head(reader: &mut BufReader<&TcpStream>) -> std::io::Result<Recorded> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.is_empty() {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let mut parts = line.trim().split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let _target = parts.next().unwrap_or("").to_string();

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
        headers,
        body: vec![],
    })
}

fn read_body(reader: &mut BufReader<&TcpStream>, req: &Recorded) -> std::io::Result<Vec<u8>> {
    if let Some(cl) = req.header("content-length") {
        let n: usize = cl.parse().unwrap();
        let mut body = vec![0u8; n];
        reader.read_exact(&mut body)?;
        return Ok(body);
    }

    Ok(vec![])
}

/// Write a 200 response that keeps the connection open for reuse.
fn respond(stream: &TcpStream, body: &str) {
    let out = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let mut stream = stream;
    stream.write_all(out.as_bytes()).unwrap();
}

fn agent(retry: bool) -> Agent {
    Agent::config_builder()
        .proxy(None)
        .retry_on_disconnect(retry)
        .build()
        .into()
}

/// Server where the first connection answers one request and then drops
/// the connection when the next request arrives (simulating a server that
/// reaped the idle connection). Later connections answer normally.
fn drop_on_second_request_server() -> Server {
    let received: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(vec![]));
    let received2 = received.clone();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let connections2 = connections.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let index = connections2.fetch_add(1, Ordering::SeqCst);
            let received = received2.clone();
            thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                match index {
                    0 => {
                        let mut req = read_head(&mut reader).expect("first request");
                        req.body = read_body(&mut reader, &req).expect("first body");
                        received.lock().unwrap().push(req);
                        respond(&stream, "first");
                        // Read the reused request, then drop the connection
                        // without answering.
                        let mut req = read_head(&mut reader).expect("reused request");
                        req.body = read_body(&mut reader, &req).unwrap_or_default();
                        received.lock().unwrap().push(req);
                    }
                    _ => {
                        let mut req = read_head(&mut reader).expect("resent request");
                        req.body = read_body(&mut reader, &req).expect("resent body");
                        received.lock().unwrap().push(req);
                        respond(&stream, "resent");
                    }
                }
            });
        }
    });

    Server {
        addr,
        connections,
        received,
    }
}

#[test]
fn get_resent_on_dropped_pooled_connection() {
    let server = drop_on_second_request_server();
    let agent = agent(true);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    // The pooled connection is dropped by the server mid-request. The
    // request must be resent on a fresh connection.
    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "resent");

    assert_eq!(server.connection_count(), 2);
    let received = server.received();
    assert_eq!(received.len(), 3);
    assert!(received.iter().all(|r| r.method == "GET"));
}

#[test]
fn disabled_by_default_keeps_error() {
    let server = drop_on_second_request_server();
    let agent = agent(false);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let err = agent.get(&server.url("/")).call().unwrap_err();
    assert!(
        matches!(&err, Error::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof),
        "unexpected: {:?}",
        err
    );

    // No new connection was established.
    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.received().len(), 2);
}

#[test]
fn request_level_config_overrides_agent() {
    // Agent level off, request level on.
    let server = drop_on_second_request_server();
    let agent_off = agent(false);

    let mut res = agent_off.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let mut res = agent_off
        .get(&server.url("/"))
        .config()
        .retry_on_disconnect(true)
        .build()
        .call()
        .unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "resent");
    assert_eq!(server.connection_count(), 2);

    // Agent level on, request level off.
    let server = drop_on_second_request_server();
    let agent_on = agent(true);

    let mut res = agent_on.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let err = agent_on
        .get(&server.url("/"))
        .config()
        .retry_on_disconnect(false)
        .build()
        .call()
        .unwrap_err();
    assert!(matches!(&err, Error::Io(_)), "unexpected: {:?}", err);
    assert_eq!(server.connection_count(), 1);
}

#[test]
fn http_crate_request_api_supports_flag() {
    let server = drop_on_second_request_server();
    let agent = agent(false);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let request = ureq::http::Request::get(server.url("/")).body(()).unwrap();
    let request = agent
        .configure_request(request)
        .retry_on_disconnect(true)
        .build();

    let mut res = agent.run(request).unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "resent");
    assert_eq!(server.connection_count(), 2);
}

#[test]
fn post_is_not_resent() {
    let server = drop_on_second_request_server();
    let agent = agent(true);

    // Prime the pool.
    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    // POST is never resent, even with an in-memory body.
    let err = agent.post(&server.url("/")).send("hello").unwrap_err();
    assert!(matches!(&err, Error::Io(_)), "unexpected: {:?}", err);

    assert_eq!(server.connection_count(), 1);
}

#[test]
fn put_with_in_memory_body_is_resent_complete() {
    let server = drop_on_second_request_server();
    let agent = agent(true);

    let mut res = agent.put(&server.url("/")).send("hello body").unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let mut res = agent.put(&server.url("/")).send("hello body").unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "resent");

    assert_eq!(server.connection_count(), 2);
    let received = server.received();
    assert_eq!(received.len(), 3);
    // The resent request carries the full body from the start, with the
    // automatically generated length covering all of it.
    let resent = &received[2];
    assert_eq!(resent.method, "PUT");
    assert_eq!(resent.body, b"hello body");
    assert_eq!(resent.header("content-length"), Some("10"));
}

#[test]
fn reader_body_is_not_resent() {
    let server = drop_on_second_request_server();
    let agent = agent(true);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let mut reader = std::io::Cursor::new(b"reader body".to_vec());
    let err = agent
        .put(&server.url("/"))
        .send(SendBody::from_reader(&mut reader))
        .unwrap_err();
    assert!(matches!(&err, Error::Io(_)), "unexpected: {:?}", err);

    assert_eq!(server.connection_count(), 1);
}

#[test]
fn partial_response_is_not_resent() {
    let received: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(vec![]));
    let received2 = received.clone();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let connections2 = connections.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let index = connections2.fetch_add(1, Ordering::SeqCst);
            let received = received2.clone();
            thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                let req = read_head(&mut reader).expect("request");
                received.lock().unwrap().push(req);
                if index == 0 {
                    respond(&stream, "first");
                    let req = read_head(&mut reader).expect("reused request");
                    received.lock().unwrap().push(req);
                    // Send a partial response header, then drop.
                    let mut s = &stream;
                    s.write_all(b"HTTP/1.1 200 OK\r\nContent-Len").unwrap();
                }
            });
        }
    });

    let server = Server {
        addr,
        connections,
        received,
    };
    let agent = agent(true);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    // The server started answering; the failure must surface as-is.
    let err = agent.get(&server.url("/")).call().unwrap_err();
    assert!(matches!(&err, Error::Io(_)), "unexpected: {:?}", err);
    assert_eq!(server.connection_count(), 1);
}

#[test]
fn resent_at_most_once() {
    // Every connection drops the request without answering (after the
    // first, which primes the pool).
    let received: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(vec![]));
    let received2 = received.clone();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let connections2 = connections.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let index = connections2.fetch_add(1, Ordering::SeqCst);
            let received = received2.clone();
            thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                let req = read_head(&mut reader).expect("request");
                received.lock().unwrap().push(req);
                if index == 0 {
                    respond(&stream, "first");
                    let req = read_head(&mut reader).expect("reused request");
                    received.lock().unwrap().push(req);
                }
                // All other connections: drop without answering.
            });
        }
    });

    let server = Server {
        addr,
        connections,
        received,
    };
    let agent = agent(true);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    // The resent request also fails; the second error is returned.
    let err = agent.get(&server.url("/")).call().unwrap_err();
    assert!(matches!(&err, Error::Io(_)), "unexpected: {:?}", err);

    // Pooled connection + one fresh resend connection, no more.
    assert_eq!(server.connection_count(), 2);
}

#[test]
fn resent_connection_is_reused_after_success() {
    let received: Arc<Mutex<Vec<Recorded>>> = Arc::new(Mutex::new(vec![]));
    let received2 = received.clone();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let connections2 = connections.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let index = connections2.fetch_add(1, Ordering::SeqCst);
            let received = received2.clone();
            thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(&stream);
                match index {
                    0 => {
                        let req = read_head(&mut reader).expect("first request");
                        received.lock().unwrap().push(req);
                        respond(&stream, "first");
                        let _req = read_head(&mut reader).expect("reused request");
                        // Drop the reused connection.
                    }
                    _ => {
                        let req = read_head(&mut reader).expect("resent request");
                        received.lock().unwrap().push(req);
                        respond(&stream, "second");
                        // The fresh connection must stay usable.
                        let req = read_head(&mut reader).expect("third request");
                        received.lock().unwrap().push(req);
                        respond(&stream, "third");
                    }
                }
            });
        }
    });

    let server = Server {
        addr,
        connections,
        received,
    };
    let agent = agent(true);

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "first");

    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "second");

    // The connection used for the resend went back to the pool.
    let mut res = agent.get(&server.url("/")).call().unwrap();
    assert_eq!(res.body_mut().read_to_string().unwrap(), "third");

    assert_eq!(server.connection_count(), 2);
}
