// Tests for resending a request once when a reused pooled connection
// turns out to be disconnected (`retry_disconnected` config).
//
// These tests use a real local TCP server, because the `_test` transport
// cannot drop connections mid-request.
#![cfg(not(feature = "_test"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ureq::{Agent, Error, SendBody};

/// What to do with a request that is not answered normally.
#[derive(Clone, Copy)]
enum Misbehave {
    /// Close the connection without any response.
    Close,
    /// Send a partial status line, then close.
    PartialThenClose,
    /// Send a 100 Continue, then close.
    HundredThenClose,
}

struct Server {
    addr: SocketAddr,
    connections: Arc<AtomicUsize>,
    seen: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

#[derive(Debug)]
struct Recorded {
    request_line: String,
    body: Vec<u8>,
}

impl Server {
    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    /// How many TCP connections the server accepted.
    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// How many requests arrived on the wire (including ones the
    /// server dropped without responding).
    fn seen_count(&self) -> usize {
        self.seen.load(Ordering::SeqCst)
    }

    /// The requests the server answered.
    fn answered(&self) -> Vec<(String, Vec<u8>)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| (r.request_line.clone(), r.body.clone()))
            .collect()
    }
}

/// Start a server that answers the first request on each connection and
/// keeps the connection open. A request that is not answered normally is
/// one where `misbehave_on_reuse` applies (second and later requests on
/// the same connection) or whose 1-based arrival order is in
/// `misbehave_ordinals`.
fn serve(misbehave_on_reuse: Option<Misbehave>, misbehave_ordinals: &'static [usize]) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = Server {
        addr,
        connections: Arc::new(AtomicUsize::new(0)),
        seen: Arc::new(AtomicUsize::new(0)),
        requests: Arc::new(Mutex::new(vec![])),
    };

    let connections = server.connections.clone();
    let seen = server.seen.clone();
    let requests = server.requests.clone();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            connections.fetch_add(1, Ordering::SeqCst);
            let seen = seen.clone();
            let requests = requests.clone();
            thread::spawn(move || {
                handle_conn(
                    stream,
                    misbehave_on_reuse,
                    misbehave_ordinals,
                    seen,
                    requests,
                )
            });
        }
    });

    server
}

fn handle_conn(
    stream: TcpStream,
    misbehave_on_reuse: Option<Misbehave>,
    misbehave_ordinals: &[usize],
    seen: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Recorded>>>,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);

    // Number of requests seen on this connection.
    let mut on_this_conn = 0;

    loop {
        on_this_conn += 1;

        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return; // client went away
        }
        let request_line = line.trim().to_string();

        let mut content_length = 0usize;
        let mut expect_100 = false;
        loop {
            let mut h = String::new();
            if reader.read_line(&mut h).unwrap_or(0) == 0 {
                return;
            }
            let h = h.trim();
            if h.is_empty() {
                break;
            }
            if let Some((name, value)) = h.split_once(':') {
                let value = value.trim();
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.parse().unwrap_or(0);
                } else if name.eq_ignore_ascii_case("expect")
                    && value.eq_ignore_ascii_case("100-continue")
                {
                    expect_100 = true;
                }
            }
        }

        let ordinal = seen.fetch_add(1, Ordering::SeqCst) + 1;

        let misbehave = if misbehave_ordinals.contains(&ordinal) {
            Some(Misbehave::Close)
        } else if on_this_conn > 1 {
            misbehave_on_reuse
        } else {
            None
        };

        if let Some(misbehave) = misbehave {
            match misbehave {
                Misbehave::Close => {}
                Misbehave::PartialThenClose => {
                    let _ = writer.write_all(b"HTTP/1.1 200 OK\r\nContent-");
                    let _ = writer.flush();
                }
                Misbehave::HundredThenClose => {
                    let _ = writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
                    let _ = writer.flush();
                }
            }
            // Drop the connection.
            return;
        }

        if expect_100 {
            writer
                .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                .unwrap();
            writer.flush().unwrap();
        }

        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body).unwrap();

        requests.lock().unwrap().push(Recorded { request_line, body });

        writer
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        writer.flush().unwrap();
    }
}

fn retry_agent() -> Agent {
    Agent::config_builder()
        .retry_disconnected(true)
        .build()
        .into()
}

/// Run a request and consume the response body, so the connection is
/// returned to the pool.
fn run_and_pool(response: Result<ureq::http::Response<ureq::Body>, Error>) {
    let mut response = response.unwrap();
    assert_eq!(response.status(), 200);
    response.body_mut().read_to_string().unwrap();
}

#[test]
fn get_is_resent_on_fresh_connection() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    // Seed the pool with an idle connection.
    run_and_pool(agent.get(&server.url("/a")).call());

    // The pooled connection is dropped by the server. The request must
    // be resent on a fresh connection.
    run_and_pool(agent.get(&server.url("/b")).call());

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
    let answered = server.answered();
    assert_eq!(answered.len(), 2);
    assert_eq!(answered[1].0, "GET /b HTTP/1.1");
}

#[test]
fn allowed_methods_are_resent() {
    for method in ["GET", "HEAD", "OPTIONS", "DELETE", "TRACE"] {
        let server = serve(Some(Misbehave::Close), &[]);
        let agent = retry_agent();

        run_and_pool(agent.get(&server.url("/a")).call());

        let result = match method {
            "GET" => agent.get(&server.url("/b")).call(),
            "HEAD" => agent.head(&server.url("/b")).call(),
            "OPTIONS" => agent.options(&server.url("/b")).call(),
            "DELETE" => agent.delete(&server.url("/b")).call(),
            "TRACE" => agent.trace(&server.url("/b")).call(),
            _ => unreachable!(),
        };
        run_and_pool(result);

        assert_eq!(server.connection_count(), 2, "{method}");
        assert_eq!(server.seen_count(), 3, "{method}");
    }
}

#[test]
fn put_with_in_memory_body_is_resent_complete() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    run_and_pool(agent.put(&server.url("/b")).send("hello"));

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
    let answered = server.answered();
    assert_eq!(answered.len(), 2);
    // The body is resent from the start, in full.
    assert_eq!(answered[1].0, "PUT /b HTTP/1.1");
    assert_eq!(answered[1].1, b"hello");
}

#[test]
fn not_resent_by_default() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = Agent::new_with_defaults();

    run_and_pool(agent.get(&server.url("/a")).call());

    let err = agent.get(&server.url("/b")).call().unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    // No retry happened.
    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 2);
}

#[test]
fn request_level_config_enables_retry() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = Agent::new_with_defaults();

    run_and_pool(agent.get(&server.url("/a")).call());

    // The agent level default is off; the request level turns it on.
    // The request level setting must not prevent connection sharing.
    run_and_pool(
        agent
            .get(&server.url("/b"))
            .config()
            .retry_disconnected(true)
            .build()
            .call(),
    );

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
}

#[test]
fn request_level_config_via_http_crate() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = Agent::new_with_defaults();

    run_and_pool(agent.get(&server.url("/a")).call());

    let request = ureq::http::Request::get(server.url("/b")).body(()).unwrap();
    let request = agent
        .configure_request(request)
        .retry_disconnected(true)
        .build();

    run_and_pool(agent.run(request));

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
}

#[test]
fn post_is_not_resent() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // POST is never resent, even with an in-memory body.
    let err = agent.post(&server.url("/b")).send("hello").unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 2);
}

#[test]
fn reader_body_is_not_resent() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // A Read-backed body is never resent, even with a declared length.
    let mut data: &[u8] = b"hello";
    let err = agent
        .put(&server.url("/b"))
        .header("content-length", "5")
        .send(SendBody::from_reader(&mut data))
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 2);
}

#[test]
fn partial_response_is_not_resent() {
    let server = serve(Some(Misbehave::PartialThenClose), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // The server sent a partial status line before dropping the
    // connection. The request must not be resent.
    let err = agent.get(&server.url("/b")).call().unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 2);
}

#[test]
fn disconnect_after_100_continue_is_not_resent() {
    let server = serve(Some(Misbehave::HundredThenClose), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // The server answers 100 Continue and then drops the connection.
    let err = agent
        .put(&server.url("/b"))
        .header("expect", "100-continue")
        .send("hello")
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 2);
}

#[test]
fn retry_bypasses_other_idle_pool_connections() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    // Seed the pool with two idle connections.
    let mut handles = Vec::new();
    for path in ["/a", "/b"] {
        let agent = agent.clone();
        let url = server.url(path);
        handles.push(thread::spawn(move || {
            run_and_pool(agent.get(&url).call());
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(server.connection_count(), 2);

    // The reused connection is dropped. The resend must not pick up the
    // other idle pooled connection (which the server would also drop),
    // but establish a fresh one.
    run_and_pool(agent.get(&server.url("/c")).call());

    assert_eq!(server.connection_count(), 3);
    assert_eq!(server.seen_count(), 4);
}

#[test]
fn resent_at_most_once() {
    // Both the reused connection and the fresh resend connection are
    // dropped by the server.
    let server = serve(None, &[2, 3]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // The second error is returned.
    let err = agent.get(&server.url("/b")).call().unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
}

#[test]
fn fresh_connection_failure_is_not_resent() {
    // The very first request fails on a freshly established connection.
    let server = serve(None, &[1]);
    let agent = retry_agent();

    let err = agent.get(&server.url("/a")).call().unwrap_err();
    assert!(matches!(err, Error::Io(_)), "unexpected: {err:?}");

    // No retry, since the connection did not come from the pool.
    assert_eq!(server.connection_count(), 1);
    assert_eq!(server.seen_count(), 1);
}

#[test]
fn resent_connection_is_reused_after_success() {
    // Only the second request on the wire is dropped.
    let server = serve(None, &[2]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    // The pooled connection is dropped; the resend succeeds on a
    // fresh connection.
    run_and_pool(agent.get(&server.url("/b")).call());
    assert_eq!(server.connection_count(), 2);

    // The connection established for the resend is pooled and reused
    // for the next request.
    run_and_pool(agent.get(&server.url("/c")).call());
    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 4);
}

#[cfg(feature = "json")]
#[test]
fn json_body_is_resent_without_reserializing() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    run_and_pool(agent.put(&server.url("/b")).send_json(serde_json::json!({"a": 1})));

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
    let answered = server.answered();
    assert_eq!(answered.len(), 2);
    // The already serialized JSON is resent in full.
    let expected = serde_json::to_vec_pretty(&serde_json::json!({"a": 1})).unwrap();
    assert_eq!(answered[1].1, expected);
}

#[cfg(feature = "json")]
#[test]
fn form_body_is_resent() {
    let server = serve(Some(Misbehave::Close), &[]);
    let agent = retry_agent();

    run_and_pool(agent.get(&server.url("/a")).call());

    run_and_pool(agent.put(&server.url("/b")).send_form([("key", "value")]));

    assert_eq!(server.connection_count(), 2);
    assert_eq!(server.seen_count(), 3);
    let answered = server.answered();
    assert_eq!(answered.len(), 2);
    assert_eq!(answered[1].1, b"key=value");
}
