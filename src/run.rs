use std::sync::{Arc, LazyLock};
use std::{io, mem};

use http::uri::Scheme;
use http::{HeaderValue, Method, Request, Response, StatusCode, Uri, header};
use ureq_proto::BodyMode;
use ureq_proto::client::state::{Await100, RecvBody, RecvResponse, Redirect, SendRequest};
use ureq_proto::client::state::{Prepare, SendBody as SendBodyState};
use ureq_proto::client::{Await100Result, RecvBodyResult};
use ureq_proto::client::{RecvResponseResult, SendRequestResult};

use crate::body::ResponseInfo;
use crate::config::{Config, DEFAULT_USER_AGENT, RedirectAuthHeaders, RequestLevelConfig};
use crate::http;
use crate::pool::Connection;
use crate::request::ForceSendBody;
use crate::response::{RedirectHistory, ResponseUri};
use crate::timings::{CallTimings, CurrentTime};
use crate::transport::ConnectionDetails;
use crate::transport::time::{Duration, Instant};
use crate::util::{
    DebugRequest, DebugResponse, DebugUri, HeaderMapExt, UriExt, new_uri_from_location,
};
use crate::{Agent, Body, Error, SendBody, Timeout};

type Call<T> = ureq_proto::client::Call<T>;

/// Run a request.
///
/// This is the "main loop" of entire ureq.
pub(crate) fn run(
    agent: &Agent,
    mut request: Request<()>,
    mut body: SendBody,
) -> Result<Response<Body>, Error> {
    let mut redirect_count = 0;

    // Configuration on the request level overrides the agent level.
    let (config, request_level) = request
        .extensions_mut()
        .remove::<RequestLevelConfig>()
        .map(|rl| (Arc::new(rl.0), true))
        .unwrap_or_else(|| (agent.config.clone(), false));

    let force_send_body = request.extensions_mut().remove::<ForceSendBody>().is_some();

    let mut redirect_history: Option<Vec<Uri>> =
        config.save_redirect_history().then_some(Vec::new());

    let timeouts = config.timeouts();

    let mut timings = CallTimings::new(timeouts, CurrentTime::default()).with_io_timeouts(&config);

    // A template of the request currently being sent. This is used to
    // rebuild the request when a 307/308 redirect requires the body to
    // be replayed. The template only ever holds headers from the original
    // request; headers ureq adds automatically (such as Content-Length)
    // are amended on the call and recomputed for each redirect hop.
    let mut template = request_template(&request);

    let mut call = prepare_call(request, force_send_body, &config)?;

    // Whether we already resent the request once. The entire run (including
    // any redirects) is allowed at most one resend on a fresh connection.
    let mut retry_used = false;

    // Set when the next attempt must use a newly established connection,
    // bypassing any idle connections in the pool.
    let mut fresh_connection = false;

    let (response, handler) = loop {
        let timeout = timings.next_timeout(Timeout::Global);
        let timed_out = match timeout.after {
            Duration::Exact(v) => v.is_zero(),
            Duration::NotHappening => false,
        };
        if timed_out {
            return Err(Error::Timeout(Timeout::Global));
        }

        let mut attempt = Attempt::default();
        let fresh = mem::take(&mut fresh_connection);

        let result = call_run(
            agent,
            &config,
            request_level,
            call,
            &mut body,
            redirect_count,
            &mut redirect_history,
            &mut timings,
            fresh,
            &mut attempt,
        );

        let result = match result {
            Ok(result) => result,
            Err(err) => {
                let can_retry = !retry_used
                    && config.retry_on_disconnect()
                    && attempt.can_retry()
                    && method_allows_retry(template.method())
                    && body.is_replayable()
                    && is_disconnect(&err);

                if !can_retry {
                    return Err(err);
                }

                // The failed attempt and the resend share the Global and
                // PerCall budgets. If the budget is already spent, do not
                // connect again; report the earliest expired timeout.
                let timeout = timings.next_timeout(Timeout::Resolve);
                if timeout.after.is_zero() {
                    return Err(Error::Timeout(timeout.reason));
                }

                debug!("Resending request on a fresh connection");

                retry_used = true;
                fresh_connection = true;
                body.rewind();
                call = prepare_call(request_template(&template), force_send_body, &config)?;
                timings = timings.new_attempt();
                continue;
            }
        };

        match result {
            // Follow redirect
            CallResult::Redirect(rcall, rtimings, location) => {
                redirect_count += 1;

                let status = rcall.status();
                let is_retain_status = matches!(
                    status,
                    StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
                );
                let method_needs_body = matches!(
                    template.method(),
                    &Method::POST | &Method::PUT | &Method::PATCH
                );

                call = if is_retain_status && method_needs_body {
                    // A 307/308 redirect on POST/PUT/PATCH must resend the
                    // request body. This is only possible for bodies held
                    // in memory, and only when the caller opted in.
                    if !config.redirect_body_replay() || !body.is_replayable() {
                        return Err(Error::RedirectFailed);
                    }

                    body.rewind();

                    let request = redirect_replay_request(&template, location.as_ref(), &config)?;

                    debug!(
                        "Redirect ({}): {} {:?}",
                        status,
                        request.method(),
                        DebugUri(request.uri())
                    );

                    Call::new(request)?
                } else {
                    let call = handle_redirect(rcall, &config)?;

                    // If the new method doesn't need a body, clear it.
                    // This prevents Content-Length/Transfer-Encoding headers from being added
                    // on methods like GET that don't send bodies (e.g., POST->GET redirects).
                    let method = call.method();
                    if !matches!(method, &Method::POST | &Method::PUT | &Method::PATCH) {
                        body.remove();
                    }

                    call
                };

                template = request_template_from_call(&call);

                timings = rtimings.new_call();
            }

            // Return response
            CallResult::Response(response, handler) => break (response, handler),
        }
    };

    let (mut parts, _) = response.into_parts();

    let recv_body_mode = handler
        .call
        .as_ref()
        .map(|f| f.body_mode())
        .unwrap_or(BodyMode::NoBody);

    let info = ResponseInfo::new(&parts.headers, recv_body_mode);

    // If the body will be decompressed, strip Content-Encoding and Content-Length
    // from the response headers. The Content-Length no longer matches the
    // decompressed body size, and Content-Encoding no longer applies since
    // the body is delivered to the caller already decompressed (RFC 9110 §8.7).
    if info.is_decompressing() {
        parts.headers.remove(http::header::CONTENT_ENCODING);
        parts.headers.remove(http::header::CONTENT_LENGTH);
    }

    let body = Body::new(handler, info);

    let response = Response::from_parts(parts, body);

    let status = response.status();
    let is_err = status.is_client_error() || status.is_server_error();

    if config.http_status_as_error() && is_err {
        return Err(Error::StatusCode(status.as_u16()));
    }

    Ok(response)
}

#[allow(clippy::too_many_arguments)]
fn call_run(
    agent: &Agent,
    config: &Config,
    request_level: bool,
    mut call: Call<Prepare>,
    body: &mut SendBody,
    redirect_count: u32,
    redirect_history: &mut Option<Vec<Uri>>,
    timings: &mut CallTimings,
    fresh_connection: bool,
    attempt: &mut Attempt,
) -> Result<CallResult, Error> {
    let uri = call.uri().clone();
    debug!("{} {:?}", call.method(), DebugUri(call.uri()));

    if config.https_only() && uri.scheme() != Some(&Scheme::HTTPS) {
        return Err(Error::RequireHttpsOnly(uri.to_string()));
    }

    add_headers(&mut call, agent, config, body, &uri)?;

    let mut connection = connect(agent, config, request_level, &uri, timings, fresh_connection)?;
    attempt.from_pool = connection.is_from_pool();

    let mut call = call.proceed();

    if log_enabled!(log::Level::Debug) {
        let headers = call.headers_map()?;

        let r = DebugRequest {
            method: call.method(),
            uri: call.uri(),
            version: call.version(),
            headers,
        };
        debug!("{:?}", r);
    }

    let call = match send_request(call, &mut connection, timings)? {
        SendRequestResult::Await100(call) => {
            match await_100(call, &mut connection, timings, attempt)? {
                Await100Result::SendBody(call) => send_body(call, body, &mut connection, timings)?,
                Await100Result::RecvResponse(call) => call,
            }
        }
        SendRequestResult::SendBody(call) => send_body(call, body, &mut connection, timings)?,
        SendRequestResult::RecvResponse(call) => call,
    };

    let is_following_redirects = redirect_count < config.max_redirects();

    let (mut response, response_result) = recv_response(
        call,
        &mut connection,
        config,
        timings,
        is_following_redirects,
        attempt,
    )?;

    debug!("{:?}", DebugResponse(&response));

    #[cfg(feature = "cookies")]
    {
        let mut jar = agent.cookie_jar_lock();

        let iter = response
            .headers()
            .get_all(http::header::SET_COOKIE)
            .iter()
            .filter_map(|h| h.to_str().ok())
            .filter_map(|s| crate::Cookie::parse(s, &uri).ok());

        jar.store_response_cookies(iter, &uri);
    }

    if let Some(history) = redirect_history.as_mut() {
        history.push(uri.clone());
        response
            .extensions_mut()
            .insert(RedirectHistory(history.clone()));
    }
    response.extensions_mut().insert(ResponseUri(uri));

    let ret = match response_result {
        RecvResponseResult::RecvBody(call) => {
            let timings = mem::take(timings);
            let mut handler = BodyHandler {
                call: Some(call),
                connection: Some(connection),
                timings,
                ..Default::default()
            };

            if response.status().is_redirection() {
                if is_following_redirects {
                    let location = response.headers().get(header::LOCATION).cloned();
                    let call = handler.consume_redirect_body()?;

                    CallResult::Redirect(call, handler.timings, location)
                } else if config.max_redirects_do_error() {
                    return Err(Error::TooManyRedirects);
                } else {
                    CallResult::Response(response, handler)
                }
            } else {
                CallResult::Response(response, handler)
            }
        }
        RecvResponseResult::Redirect(call) => {
            cleanup(connection, call.must_close_connection(), timings.now());

            if is_following_redirects {
                let location = response.headers().get(header::LOCATION).cloned();
                CallResult::Redirect(call, mem::take(timings), location)
            } else if config.max_redirects_do_error() {
                return Err(Error::TooManyRedirects);
            } else {
                CallResult::Response(response, BodyHandler::default())
            }
        }
        RecvResponseResult::Cleanup(call) => {
            cleanup(connection, call.must_close_connection(), timings.now());
            CallResult::Response(response, BodyHandler::default())
        }
    };

    Ok(ret)
}

/// Return type of [`call_run`].
#[allow(clippy::large_enum_variant)]
enum CallResult {
    /// Call resulted in a redirect.
    Redirect(Call<Redirect>, CallTimings, Option<HeaderValue>),

    /// Call resulted in a response.
    Response(Response<()>, BodyHandler),
}

/// Build the call for one attempt of sending the request.
fn prepare_call(
    request: Request<()>,
    force_send_body: bool,
    config: &Config,
) -> Result<Call<Prepare>, Error> {
    let mut call = Call::new(request)?;

    if force_send_body {
        call.force_send_body();
    }

    call.allow_non_standard_methods(config.allow_non_standard_methods());

    Ok(call)
}

/// State tracked while attempting to send a request once.
///
/// Used to decide whether a failed attempt may be resent on a fresh
/// connection, see [`Config::retry_on_disconnect`].
#[derive(Default)]
struct Attempt {
    /// The connection used for this attempt came out of the pool.
    from_pool: bool,

    /// Any response bytes (status line, headers, 100-continue) arrived.
    response_started: bool,
}

impl Attempt {
    /// A resend is only possible for a pooled connection that failed
    /// before the server sent any response bytes.
    fn can_retry(&self) -> bool {
        self.from_pool && !self.response_started
    }
}

/// Methods that are safe to resend in full.
///
/// POST, PATCH and any other method are never resent, even without a body.
fn method_allows_retry(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

/// Whether the error indicates the connection was dropped by the remote.
fn is_disconnect(err: &Error) -> bool {
    let Error::Io(e) = err else {
        return false;
    };

    matches!(
        e.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::UnexpectedEof
    )
}

fn add_headers(
    call: &mut Call<Prepare>,
    agent: &Agent,
    config: &Config,
    body: &mut SendBody,
    uri: &Uri,
) -> Result<(), Error> {
    let headers = call.headers();

    let send_body_mode = if headers.has_send_body_mode() {
        None
    } else {
        Some(body.body_mode()?)
    };
    let has_header_accept_enc = headers.has_accept_encoding();
    let has_header_ua = headers.has_user_agent();
    let has_header_accept = headers.has_accept();

    #[cfg(not(feature = "cookies"))]
    {
        let _ = agent;
        let _ = uri;
    }
    #[cfg(feature = "cookies")]
    {
        let value = agent.jar.get_request_cookies(uri);
        if !value.is_empty() {
            let value = HeaderValue::from_str(&value)
                .map_err(|_| Error::CookieValue("Cookie value is an invalid http-header"))?;
            call.header(header::COOKIE, value)?;
        }
    }

    {
        static ACCEPTS: LazyLock<String> = LazyLock::new(|| {
            #[allow(unused_mut)]
            let mut value = String::with_capacity(10);
            #[cfg(feature = "gzip")]
            value.push_str("gzip");
            #[cfg(all(feature = "gzip", feature = "brotli"))]
            value.push_str(", ");
            #[cfg(feature = "brotli")]
            value.push_str("br");
            value
        });

        let accepts = &*ACCEPTS;

        if !has_header_accept_enc {
            if let Some(v) = config.accept_encoding().as_str(accepts) {
                // unwrap is ok because above ACCEPTS will produce a valid value,
                // or the value is user provided in which case it must be valid.
                let value = HeaderValue::from_str(v).unwrap();
                call.header(header::ACCEPT_ENCODING, value)?;
            }
        }
    }

    if let Some(send_body_mode) = send_body_mode {
        match send_body_mode {
            BodyMode::LengthDelimited(v) => {
                let value = HeaderValue::from(v);
                call.header(header::CONTENT_LENGTH, value)?;
            }
            BodyMode::Chunked => {
                let value = HeaderValue::from_static("chunked");
                call.header(header::TRANSFER_ENCODING, value)?;
            }
            _ => {}
        }
    }

    if !has_header_ua {
        // unwrap is ok because a user might override the agent, and if they
        // set bad values, it's not really ureq's problem.
        if let Some(v) = config.user_agent().as_str(DEFAULT_USER_AGENT) {
            let value = HeaderValue::from_str(v).unwrap();
            call.header(header::USER_AGENT, value)?;
        }
    }

    if !has_header_accept {
        // unwrap is ok because a user might override accepts header, and if they
        // set bad values, it's not really ureq's problem.
        if let Some(v) = config.accept().as_str("*/*") {
            let value = HeaderValue::from_str(v).unwrap();
            call.header(header::ACCEPT, value)?;
        }
    }

    Ok(())
}

fn connect(
    agent: &Agent,
    config: &Config,
    request_level: bool,
    uri: &Uri,
    timings: &mut CallTimings,
    fresh_connection: bool,
) -> Result<Connection, Error> {
    // Before resolving the URI we need to ensure it is a full URI. We
    // cannot make requests with partial uri like "/path".
    uri.ensure_valid_url()?;

    let is_proxy = config.proxy().is_some();

    // For most proxy configs, the proxy itself should resolve the host name we are connecting to.
    // However for SOCKS4, we must do it and pass the resolved IP to the proxy.
    let is_proxy_local_resolve = config.proxy().map(|p| p.resolve_target()).unwrap_or(false);

    // Tells if this host matches NO_PROXY
    let is_no_proxy = config.proxy().map(|p| p.is_no_proxy(uri)).unwrap_or(false);

    let addrs = if is_no_proxy || !is_proxy || is_proxy_local_resolve {
        agent
            .resolver
            .resolve(uri, config, timings.next_timeout(Timeout::Resolve))?
    } else {
        agent.resolver.empty()
    };

    timings.record_time(Timeout::Resolve);

    let details = ConnectionDetails {
        uri,
        addrs,
        resolver: &*agent.resolver,
        config,
        request_level,
        now: timings.now(),
        timeout: timings.next_timeout(Timeout::Connect),
        current_time: timings.current_time(),
        run_connector: agent.run_connector.clone(),
    };

    let use_pool = config.can_share_pool_with(&agent.config);
    let max_idle_age = config.max_idle_age().into();
    let connection = agent
        .pool
        .connect(&details, max_idle_age, use_pool, fresh_connection)?;

    if details.needs_tls() && !connection.is_tls() {
        return Err(Error::TlsRequired);
    }

    timings.record_time(Timeout::Connect);

    Ok(connection)
}

fn send_request(
    mut call: Call<SendRequest>,
    connection: &mut Connection,
    timings: &mut CallTimings,
) -> Result<SendRequestResult, Error> {
    loop {
        if call.can_proceed() {
            break;
        }

        let buffers = connection.buffers();
        let amount = call.write(buffers.output())?;
        let timeout = timings.next_timeout(Timeout::SendRequest);
        connection.transmit_output(amount, timeout)?;
    }

    timings.record_time(Timeout::SendRequest);

    // The request might be misconfigured.
    let call = call.proceed()?;

    // We checked can_proceed() above, this unwrap is fine.
    Ok(call.unwrap())
}

fn await_100(
    mut call: Call<Await100>,
    connection: &mut Connection,
    timings: &mut CallTimings,
    attempt: &mut Attempt,
) -> Result<Await100Result, Error> {
    while call.can_keep_await_100() {
        let timeout = timings.next_timeout(Timeout::Await100);

        if timeout.after.is_zero() {
            if timeout.reason == Timeout::Await100 {
                // Stop waiting for 100-continue.
                break;
            }
            return Err(Error::Timeout(timeout.reason));
        }

        match connection.maybe_await_input(timeout) {
            Ok(_) => {
                let input = connection.buffers().input();
                if input.is_empty() {
                    return Err(Error::disconnected("await_100 empty input"));
                }

                // Any response bytes (such as a 100-continue) mean the
                // request reached the server and must not be resent.
                attempt.response_started = true;

                let amount = call.try_read_100(input)?;
                if amount > 0 {
                    connection.consume_input(amount);
                    break;
                }
            }
            Err(Error::Timeout(Timeout::Await100)) => {
                // If we get a timeout while waiting for input, that is not an error,
                // we progress to send the request body.
                break;
            }
            Err(e) => return Err(e),
        }
    }

    timings.record_time(Timeout::Await100);

    // A misconfigured request might surface here.
    let call = call.proceed()?;

    Ok(call)
}

fn send_body(
    mut call: Call<SendBodyState>,
    body: &mut SendBody,
    connection: &mut Connection,
    timings: &mut CallTimings,
) -> Result<Call<RecvResponse>, Error> {
    loop {
        if call.can_proceed() {
            break;
        }

        let buffers = connection.buffers();

        let (tmp, output) = buffers.tmp_and_output();

        let input_len = tmp.len();

        let input_fitting_in_output = call.calculate_max_input(output.len());
        let max_input = input_len.min(input_fitting_in_output);

        let output_used = if !call.is_chunked() {
            // For non-chunked, The body can be written directly to the output.
            // This optimizes away a memcopy if we were to go via call.write().
            let output_used = body.read(output)?;

            // Size checking is still in the call.
            call.consume_direct_write(output_used)?;

            output_used
        } else {
            let tmp = &mut tmp[..max_input];
            let n = body.read(tmp)?;

            let (input_used, output_used) = call.write(&tmp[..n], output)?;

            // Since output is "a bit" larger than the input (compensate for chunk ovexrhead),
            // the entire input we read from the body should also be shipped to the output.
            assert!(input_used == n);

            output_used
        };

        let timeout = timings.next_timeout(Timeout::SendBody);
        connection.transmit_output(output_used, timeout)?;
    }

    timings.record_time(Timeout::SendBody);
    Ok(call.proceed().unwrap())
}

fn recv_response(
    mut call: Call<RecvResponse>,
    connection: &mut Connection,
    config: &Config,
    timings: &mut CallTimings,
    is_following_redirects: bool,
    attempt: &mut Attempt,
) -> Result<(Response<()>, RecvResponseResult), Error> {
    let response = loop {
        let timeout = timings.next_timeout(Timeout::RecvResponse);
        let made_progress = connection.maybe_await_input(timeout)?;

        let input = connection.buffers().input();

        if made_progress || !input.is_empty() {
            // Once any response bytes arrive (even a partial status line),
            // the request must not be resent.
            attempt.response_started = true;
        }

        // If cookies are disabled, we can allow ourselves to follow
        // the redirect as soon as we discover the `Location` header.
        // There are plenty of broken servers out there that don't send
        // the finishing \r\n on redirects. With cookies off, we can
        // handle that situation.
        //
        // If cookies are enabled, we risk mising a `Set-Cookie` header
        // with such a strategy.
        let cookies_enabled = cfg!(feature = "cookies");

        // If we are not following redirects, do not allow partial returned
        // 302 responses.
        let allow_partial_redirect = !cookies_enabled && is_following_redirects;

        let (amount, maybe_response) = call.try_response(input, allow_partial_redirect)?;

        let check_size = if maybe_response.is_some() {
            // We got a parsed response, ensure the size is within
            // configured parameters.
            amount
        } else {
            // We did not parse a response, if input is too large,
            // we stop trying to get more data.
            input.len()
        };

        if check_size > config.max_response_header_size() {
            return Err(Error::LargeResponseHeader(
                input.len(),
                config.max_response_header_size(),
            ));
        }

        connection.consume_input(amount);

        if let Some(response) = maybe_response {
            assert!(call.can_proceed());
            break response;
        } else if !made_progress {
            return Err(Error::disconnected("recv_respone made no progress"));
        }
    };

    timings.record_time(Timeout::RecvResponse);
    Ok((response, call.proceed().unwrap()))
}

fn handle_redirect(mut call: Call<Redirect>, config: &Config) -> Result<Call<Prepare>, Error> {
    let maybe_new_call = call.as_new_call(config.redirect_auth_headers())?;
    let status = call.status();

    if let Some(call) = maybe_new_call {
        debug!(
            "Redirect ({}): {} {:?}",
            status,
            call.method(),
            DebugUri(call.uri())
        );

        Ok(call)
    } else {
        Err(Error::RedirectFailed)
    }
}

/// A snapshot of a request, used to rebuild it when a 307/308 redirect
/// requires the body to be replayed.
///
/// Extensions are not carried over. They are either consumed when setting
/// up the call (such as [`RequestLevelConfig`]) or not relevant for a
/// redirected request.
fn request_template(request: &Request<()>) -> Request<()> {
    let mut template = Request::new(());
    *template.method_mut() = request.method().clone();
    *template.uri_mut() = request.uri().clone();
    *template.version_mut() = request.version();
    *template.headers_mut() = request.headers().clone();
    template
}

fn request_template_from_call(call: &Call<Prepare>) -> Request<()> {
    let mut template = Request::new(());
    *template.method_mut() = call.method().clone();
    *template.uri_mut() = call.uri().clone();
    *template.version_mut() = call.version();
    *template.headers_mut() = call.headers().clone();
    template
}

/// Build the request for a 307/308 redirect that replays the request body.
///
/// The method and headers are retained from the previous request. This
/// mirrors the header handling in ureq-proto's redirect logic, except that
/// a caller-provided `Content-Length`/`Transfer-Encoding` is kept, since the
/// replayed body is byte-identical to what was first sent.
fn redirect_replay_request(
    template: &Request<()>,
    location: Option<&HeaderValue>,
    config: &Config,
) -> Result<Request<()>, Error> {
    let header = location.ok_or(ureq_proto::Error::NoLocationHeader)?;

    let location = header.to_str().map_err(|_| {
        ureq_proto::Error::BadLocationHeader(String::from_utf8_lossy(header.as_bytes()).to_string())
    })?;

    let uri = new_uri_from_location(template.uri(), location)?;

    let keep_auth_header = match config.redirect_auth_headers() {
        RedirectAuthHeaders::SameHost => can_redirect_auth_header(template.uri(), &uri),
        _ => false,
    };

    let mut request = request_template(template);
    *request.uri_mut() = uri;

    // Mutate the request to remove headers we cannot keep in the redirect.
    let headers = request.headers_mut();
    if !keep_auth_header {
        headers.remove(header::AUTHORIZATION);
    }
    headers.remove(header::COOKIE);

    Ok(request)
}

fn can_redirect_auth_header(prev: &Uri, next: &Uri) -> bool {
    let host_prev = prev.authority().map(|a| a.host());
    let host_next = next.authority().map(|a| a.host());
    let scheme_prev = prev.scheme();
    let scheme_next = next.scheme();
    host_prev == host_next && (scheme_prev == scheme_next || scheme_next == Some(&Scheme::HTTPS))
}

fn cleanup(connection: Connection, must_close: bool, now: Instant) {
    if must_close {
        connection.close();
    } else {
        connection.reuse(now)
    }
}

#[derive(Default)]
pub(crate) struct BodyHandler {
    call: Option<Call<RecvBody>>,
    connection: Option<Connection>,
    timings: CallTimings,
    remote_closed: bool,
    redirect: Option<Box<Call<Redirect>>>,
}

impl BodyHandler {
    fn do_read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let (Some(call), Some(connection), timings) =
            (&mut self.call, &mut self.connection, &mut self.timings)
        else {
            return Ok(0);
        };

        loop {
            let body_fulfilled = match call.body_mode() {
                BodyMode::NoBody => unreachable!("must be a BodyMode for BodyHandler"),
                // These modes are fulfilled by either reaching the content-length or
                // receiving an end chunk delimiter.
                BodyMode::LengthDelimited(_) | BodyMode::Chunked => call.can_proceed(),
                // This mode can only end once remote closes
                BodyMode::CloseDelimited => false,
            };

            if body_fulfilled {
                self.ended()?;
                return Ok(0);
            }

            let has_buffered_input = connection.buffers().can_use_input();

            // First try to use input already buffered
            if has_buffered_input {
                let input = connection.buffers().input();
                let (input_used, output_used) = call.read(input, buf)?;
                connection.consume_input(input_used);

                if output_used > 0 {
                    return Ok(output_used);
                }

                if input_used > 0 {
                    // Body might be fulfilled now.
                    continue;
                }
            }

            if self.remote_closed {
                // we should not try to await_input again.
                self.ended()?;
                return Ok(0);
            }

            let timeout = timings.next_timeout(Timeout::RecvBody);

            let made_progress = match connection.maybe_await_input(timeout) {
                Ok(v) => v,
                Err(Error::Io(e)) => match e.kind() {
                    io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset => {
                        self.remote_closed = true;
                        true
                    }
                    _ => return Err(Error::Io(e)),
                },
                Err(e) => return Err(e),
            };

            let input = connection.buffers().input();
            let input_ended = input.is_empty();

            let (input_used, output_used) = call.read(input, buf)?;
            connection.consume_input(input_used);

            if output_used > 0 {
                return Ok(output_used);
            } else if input_ended {
                self.ended()?;
                return Ok(0);
            } else if made_progress {
                // The maybe_await_input() made progress, but handled amount is 0. This
                // can for instance happen if we read some data, but not enough for
                // decoding any gzip.
                continue;
            } else {
                // This is an error case we don't want to see.
                return Err(Error::BodyStalled);
            }
        }
    }

    fn ended(&mut self) -> Result<(), Error> {
        self.timings.record_time(Timeout::RecvBody);

        let call = self.call.take().expect("ended() called with body");

        // In some cases, when reading chunked, the server send 0\r\n to indicate
        // the end of the body, and then abruptly does a FIN. In these cases we have
        // received the entire body, but must clean up the connection.
        let is_ended_chunked = call.is_ended_chunked();

        let mut force_close = false;

        if !call.can_proceed() {
            if is_ended_chunked {
                // This case means we got 0\r\n, but can_proceed() is false because
                // it only goes true on fully received chunked bodies.
                debug!("Server ended connection after sending chunked 0\\r\\n");
                force_close = true;
            } else {
                return Err(Error::disconnected("ended call cannot proceed"));
            }
        }

        let must_close_connection = force_close
            || match call.proceed().unwrap() {
                RecvBodyResult::Redirect(call) => {
                    let c = call.must_close_connection();
                    self.redirect = Some(Box::new(call));
                    c
                }
                RecvBodyResult::Cleanup(v) => v.must_close_connection(),
            };

        let connection = self.connection.take().expect("ended() called with body");
        cleanup(connection, must_close_connection, self.timings.now());

        Ok(())
    }

    fn consume_redirect_body(&mut self) -> Result<Call<Redirect>, Error> {
        let mut buf = vec![0; 1024];
        loop {
            let amount = self.do_read(&mut buf)?;
            if amount == 0 {
                break;
            }
        }

        // Unwrap is OK, because we are only consuming the redirect body if
        // such a body was signalled by the remote.
        let redirect = self.redirect.take().map(|b| *b);
        Ok(redirect.expect("remote to have signaled redirect"))
    }
}

impl io::Read for BodyHandler {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.do_read(buf).map_err(|e| e.into_io())
    }
}
