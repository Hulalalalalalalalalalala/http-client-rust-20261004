//! Tests for opt-in request body replay on 307/308 redirects, using the
//! `_test` feature in-memory transport.
//!
//! The test transport only exposes the request head to handlers, so these
//! tests assert on method and headers per redirect hop. Byte-exact body
//! verification lives in `tests/redirect_body_replay.rs`.

use crate::config::Config;
use crate::http::{self, Method};
use crate::test::init_test_log;
use crate::transport::{set_handler, set_handler_cb};
use crate::{Agent, Error, SendBody};

#[test]
fn replay_307_followed_when_enabled() {
    init_test_log();

    set_handler(
        "/replay-src",
        307,
        &[
            ("Location", "http://example.com/replay-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/replay-dst", 200, &[], b"ok", |req| {
        // Method, content type and the full content length are kept.
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.headers().get("content-type").unwrap(), "text/plain");
        assert_eq!(req.headers().get("content-length").unwrap(), "9");
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let mut res = agent
        .post("http://example.org/replay-src")
        .content_type("text/plain")
        .send("123456789")
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.body_mut().read_to_string().unwrap(), "ok");
}

#[test]
fn replay_307_refused_by_default() {
    init_test_log();

    set_handler(
        "/noreplay-src",
        307,
        // No handler for this location; following would fail the test server.
        &[
            ("Location", "http://example.edu/noreplay-dst"),
            ("Connection", "close"),
        ],
        &[],
    );

    let err = crate::post("http://example.org/noreplay-src")
        .send("some body")
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));
}

#[test]
fn replay_308_per_request_config() {
    init_test_log();

    set_handler(
        "/replay308-src",
        308,
        &[
            ("Location", "http://example.com/replay308-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/replay308-dst", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::PUT);
        assert_eq!(req.headers().get("content-length").unwrap(), "3");
    });

    // Agent has the default (off), the request turns it on.
    let res = crate::put("http://example.org/replay308-src")
        .config()
        .redirect_body_replay(true)
        .build()
        .send("abc")
        .unwrap();

    assert_eq!(res.status(), 200);
}

#[test]
fn replay_request_level_off_overrides_agent() {
    init_test_log();

    set_handler(
        "/replayoff-src",
        307,
        &[
            ("Location", "http://example.edu/replayoff-dst"),
            ("Connection", "close"),
        ],
        &[],
    );

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let err = agent
        .post("http://example.org/replayoff-src")
        .config()
        .redirect_body_replay(false)
        .build()
        .send("some body")
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));
}

#[test]
fn replay_http_crate_request() {
    init_test_log();

    set_handler(
        "/replayhttp-src",
        307,
        &[
            ("Location", "http://example.com/replayhttp-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/replayhttp-dst", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.headers().get("content-length").unwrap(), "4");
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let request = http::Request::post("http://example.org/replayhttp-src")
        .body("http")
        .unwrap();

    let res = agent.run(request).unwrap();
    assert_eq!(res.status(), 200);
}

#[test]
fn no_replay_for_reader_body() {
    init_test_log();

    set_handler(
        "/reader-src",
        307,
        &[
            ("Location", "http://example.edu/reader-dst"),
            ("Connection", "close"),
        ],
        &[],
    );

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    // A generic Read body cannot be replayed, even though the feature is on.
    let mut data: &[u8] = b"streamed";
    let err = agent
        .post("http://example.org/reader-src")
        .send(SendBody::from_reader(&mut data))
        .unwrap_err();

    assert!(matches!(err, Error::RedirectFailed));
}

#[test]
fn replay_307_chunked_keeps_transfer_encoding() {
    init_test_log();

    set_handler(
        "/replaychunked-src",
        307,
        &[
            ("Location", "http://example.com/replaychunked-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/replaychunked-dst", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::POST);
        // Caller-provided transfer encoding is kept on the redirect.
        assert_eq!(req.headers().get("transfer-encoding").unwrap(), "chunked");
        assert!(req.headers().get("content-length").is_none());
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let res = agent
        .post("http://example.org/replaychunked-src")
        .header("transfer-encoding", "chunked")
        .send("chunked body")
        .unwrap();

    assert_eq!(res.status(), 200);
}

#[test]
fn replay_307_empty_body() {
    init_test_log();

    set_handler(
        "/replayempty-src",
        307,
        &[
            ("Location", "http://example.com/replayempty-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/replayempty-dst", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.headers().get("content-length").unwrap(), "0");
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let res = agent
        .post("http://example.org/replayempty-src")
        .send_empty()
        .unwrap();

    assert_eq!(res.status(), 200);
}

#[test]
fn replay_chained_relative_locations() {
    init_test_log();

    set_handler(
        "/chain/a",
        307,
        // Root-relative location.
        &[("Location", "/chain/b"), ("Connection", "close")],
        &[],
    );
    set_handler(
        "/chain/b",
        308,
        &[
            ("Location", "http://example.com/chain/c"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/chain/c", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.headers().get("content-length").unwrap(), "5");
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let res = agent
        .post("http://example.org/chain/a")
        .send("chain")
        .unwrap();

    assert_eq!(res.status(), 200);
}

#[test]
fn replay_still_requires_https_only() {
    init_test_log();

    set_handler(
        "/httpsonly-src",
        307,
        &[
            ("Location", "http://example.com/httpsonly-dst"),
            ("Connection", "close"),
        ],
        &[],
    );

    let agent: Agent = Config::builder()
        .https_only(true)
        .redirect_body_replay(true)
        .build()
        .into();

    // The first request is https (the test transport pretends to be TLS),
    // but the redirect target is http and must be refused.
    let err = agent
        .post("https://example.org/httpsonly-src")
        .send("body")
        .unwrap_err();

    assert!(matches!(err, Error::RequireHttpsOnly(_)));
}

#[test]
fn replay_still_limited_by_max_redirects() {
    init_test_log();

    set_handler(
        "/replayloop",
        307,
        &[("Location", "/replayloop"), ("Connection", "close")],
        &[],
    );

    let agent: Agent = Config::builder()
        .redirect_body_replay(true)
        .max_redirects(2)
        .build()
        .into();

    let err = agent
        .post("http://example.org/replayloop")
        .send("loop")
        .unwrap_err();

    assert!(matches!(err, Error::TooManyRedirects));
}

#[test]
fn no_replay_after_method_becomes_get() {
    init_test_log();

    // POST redirects with 301: method becomes GET and the body is dropped.
    set_handler(
        "/getconv-src",
        301,
        &[
            ("Location", "http://example.com/getconv-mid"),
            ("Connection", "close"),
        ],
        &[],
    );
    // A later 307 follows the (bodiless) GET.
    set_handler(
        "/getconv-mid",
        307,
        &[
            ("Location", "http://example.com/getconv-dst"),
            ("Connection", "close"),
        ],
        &[],
    );
    set_handler_cb("/getconv-dst", 200, &[], b"ok", |req| {
        assert_eq!(req.method(), Method::GET);
        assert!(req.headers().get("content-length").is_none());
        assert!(req.headers().get("transfer-encoding").is_none());
    });

    let agent: Agent = Config::builder().redirect_body_replay(true).build().into();

    let res = agent
        .post("http://example.org/getconv-src")
        .send("dropped body")
        .unwrap();

    assert_eq!(res.status(), 200);
}
