use std::fmt;
use std::io;
use std::mem;
use std::sync::{Arc, Mutex};

pub use build::BodyBuilder;
use ureq_proto::BodyMode;
use ureq_proto::http::header;

use crate::Error;
use crate::http;
use crate::pool::Connection;
use crate::run::BodyHandler;
use crate::transport::time::Instant;

use self::limit::LimitReader;
use self::lossy::LossyUtf8Reader;

mod build;
mod limit;
mod lossy;

#[cfg(feature = "charset")]
mod charset;

#[cfg(feature = "gzip")]
mod gzip;

#[cfg(feature = "brotli")]
mod brotli;

/// Default max body size for read_to_string() and read_to_vec().
const MAX_BODY_SIZE: u64 = 10 * 1024 * 1024;

/// Fraction of the read limit under which `read_json()` buffers the body into memory
/// and parses it from a slice (faster) rather than streaming it through serde_json's
/// reader. The cutoff leaves some headroom for the parsed value.
#[cfg(feature = "json")]
const JSON_BUFFER_DIVISOR: u64 = 3;

/// A response body returned as [`http::Response<Body>`].
///
/// # Default size limit
///
/// Methods like `read_to_string()`, `read_to_vec()`, and `read_json()` have a **default 10MB limit**
/// to prevent memory exhaustion. To download larger files, use `with_config().limit(new_size)`:
///
/// ```
/// // Download a 20MB file
/// let bytes = ureq::get("http://httpbin.org/bytes/200000000")
///     .call()?
///     .body_mut()
///     .with_config()
///     .limit(20 * 1024 * 1024) // 20MB
///     .read_to_vec()?;
/// # Ok::<_, ureq::Error>(())
/// ```
///
/// # Body lengths
///
/// HTTP/1.1 has two major modes of transfering body data. Either a `Content-Length`
/// header defines exactly how many bytes to transfer, or `Transfer-Encoding: chunked`
/// facilitates a streaming style when the size is not known up front.
///
/// To protect against a problem called [request smuggling], ureq has heuristics for
/// how to interpret a server sending both `Transfer-Encoding` and `Content-Length` headers.
///
/// 1. `chunked` takes precedence if there both headers are present (not for HTTP/1.0)
/// 2. `content-length` is used if there is no chunked
/// 3. If there are no headers, fall back on "close delimited" meaning the socket
///    must close to end the body
///
/// When a `Content-Length` header is used, ureq will ensure the received body is _EXACTLY_
/// as many bytes as declared (it cannot be less). This mechanic is in `ureq-proto`
/// and is different to the [`BodyWithConfig::limit()`] below.
///
/// # Pool reuse
///
/// To return a connection (aka [`Transport`][crate::unversioned::transport::Transport])
/// to the Agent's pool, the body must be read to end. If [`BodyWithConfig::limit()`] is set
/// shorter size than the actual response body, the connection will not be reused.
///
/// # Example
///
/// ```
/// use std::io::Read;
/// let mut res = ureq::get("http://httpbin.org/bytes/100")
///     .call()?;
///
/// assert!(res.headers().contains_key("Content-Length"));
/// let len: usize = res.headers().get("Content-Length")
///     .unwrap().to_str().unwrap().parse().unwrap();
///
/// let mut bytes: Vec<u8> = Vec::with_capacity(len);
/// res.body_mut().as_reader()
///     .read_to_end(&mut bytes)?;
///
/// assert_eq!(bytes.len(), len);
/// # Ok::<_, ureq::Error>(())
/// ```
///
/// [request smuggling]: https://en.wikipedia.org/wiki/HTTP_request_smuggling
pub struct Body {
    source: BodyDataSource,
    info: Arc<ResponseInfo>,
}

enum BodyDataSource {
    /// The body as received, not yet wrapped in decoders.
    Raw(RawSource),
    /// The decoding chain, built on first use and shared by all readers
    /// of this body.
    Decoded(Box<DecodedBody>),
}

/// The body exactly as received (or as set via [`Body::builder`]).
enum RawSource {
    Handler(Box<BodyHandler>),
    Reader(Box<dyn io::Read + Send + Sync>),
}

impl io::Read for RawSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            RawSource::Handler(v) => v.read(buf),
            RawSource::Reader(v) => v.read(buf),
        }
    }
}

/// The decoded body: the raw source wrapped in content decoders (gzip,
/// brotli) and charset conversion, plus state shared by all readers of
/// the body.
///
/// The chain is built lazily on the first reader and then lives in the
/// [`Body`] rather than in any individual reader. This is what allows a
/// caller to read a prefix with one reader and continue with another:
/// decompression state and any received-but-undelivered output stay
/// here instead of being lost when a reader is dropped.
pub(crate) struct DecodedBody {
    chain: CharsetDecoder<ContentDecoder>,
    state: Arc<Mutex<SharedState>>,
}

impl DecodedBody {
    fn new(raw: RawSource, info: &ResponseInfo) -> Self {
        // A handler-backed body shares its state with the handler, so the
        // connection can be held in it once the raw body is fulfilled
        // (see BodyHandler).
        let state = match &raw {
            RawSource::Handler(v) => v.shared(),
            RawSource::Reader(_) => Arc::new(Mutex::new(SharedState::default())),
        };

        let chain = content_decoders(raw, info);
        let chain = charset_decoder(chain, info);

        DecodedBody { chain, state }
    }
}

/// State shared between all readers of one body.
#[derive(Default)]
pub(crate) struct SharedState {
    /// A read confirmed the body exceeds this limit. Sticky: subsequent
    /// readers must keep reporting it rather than deliver more data.
    pub(crate) exceeded_limit: Option<u64>,

    /// A decompression failure. Terminal for the body: subsequent
    /// readers must keep reporting it rather than see a normal end.
    #[cfg(any(feature = "gzip", feature = "brotli"))]
    pub(crate) decompress_failed: Option<&'static str>,

    /// Connection held back from the pool until the decoded body is
    /// fully delivered to the caller. Dropping it closes the connection.
    pub(crate) held_connection: Option<HeldConnection>,
}

/// A connection whose raw body has been fully received, kept out of the
/// pool until the decoded output is fully delivered to the caller.
pub(crate) struct HeldConnection {
    connection: Connection,
    now: Instant,
}

impl HeldConnection {
    pub(crate) fn new(connection: Connection, now: Instant) -> Self {
        HeldConnection { connection, now }
    }

    /// Return the connection to the pool.
    pub(crate) fn release(self) {
        self.connection.reuse(self.now);
    }
}

#[derive(Clone)]
pub(crate) struct ResponseInfo {
    content_encodings: Vec<ContentEncoding>,
    mime_type: Option<String>,
    charset: Option<String>,
    body_mode: BodyMode,
}

impl Body {
    /// Builder for creating a body
    ///
    /// This is useful for testing, or for [`Middleware`][crate::middleware::Middleware] that
    /// returns another body than the requested one.
    pub fn builder() -> BodyBuilder {
        BodyBuilder::new()
    }

    pub(crate) fn new(handler: BodyHandler, info: ResponseInfo) -> Self {
        Body {
            source: BodyDataSource::Raw(RawSource::Handler(Box::new(handler))),
            info: Arc::new(info),
        }
    }

    /// Wrap the raw source in the decoding chain, if not done already.
    ///
    /// The chain is built once and then shared by every reader created
    /// from this body, so reading can continue where a previous reader
    /// stopped.
    fn ensure_decoded(&mut self) {
        if matches!(self.source, BodyDataSource::Decoded(_)) {
            return;
        }

        let placeholder = BodyDataSource::Raw(RawSource::Reader(Box::new(io::empty())));
        let source = mem::replace(&mut self.source, placeholder);
        let BodyDataSource::Raw(raw) = source else {
            unreachable!("checked to be raw above")
        };

        self.source = BodyDataSource::Decoded(Box::new(DecodedBody::new(raw, &self.info)));
    }

    /// The mime-type of the `content-type` header.
    ///
    /// For the below header, we would get `Some("text/plain")`:
    ///
    /// ```text
    ///     Content-Type: text/plain; charset=iso-8859-1
    /// ```
    ///
    /// *Caution:* A bad server might set `Content-Type` to one thing and send
    /// something else. There is no way ureq can verify this.
    ///
    /// # Example
    ///
    /// ```
    /// let res = ureq::get("https://www.google.com/")
    ///     .call()?;
    ///
    /// assert_eq!(res.body().mime_type(), Some("text/html"));
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn mime_type(&self) -> Option<&str> {
        self.info.mime_type.as_deref()
    }

    /// The charset of the `content-type` header.
    ///
    /// For the below header, we would get `Some("iso-8859-1")`:
    ///
    /// ```text
    ///     Content-Type: text/plain; charset=iso-8859-1
    /// ```
    ///
    /// *Caution:* A bad server might set `Content-Type` to one thing and send
    /// something else. There is no way ureq can verify this.
    ///
    /// # Example
    ///
    /// ```
    /// let res = ureq::get("https://www.google.com/")
    ///     .call()?;
    ///
    /// assert_eq!(res.body().charset(), Some("ISO-8859-1"));
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn charset(&self) -> Option<&str> {
        self.info.charset.as_deref()
    }

    /// The content length of the body.
    ///
    /// This is the value of the `Content-Length` header, if there is one. For chunked
    /// responses (`Transfer-Encoding: chunked`) , this will be `None`. Similarly for
    /// HTTP/1.0 without a `Content-Length` header, the response is close delimited,
    /// which means the length is unknown.
    ///
    /// A bad server might set `Content-Length` to one thing and send something else.
    /// ureq will double check this, see section on body length heuristics.
    ///
    /// # Example
    ///
    /// ```
    /// let res = ureq::get("https://httpbin.org/bytes/100")
    ///     .call()?;
    ///
    /// assert_eq!(res.body().content_length(), Some(100));
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn content_length(&self) -> Option<u64> {
        self.info.content_length()
    }

    /// Handle this body as a shared `impl Read` of the body.
    ///
    /// This is the regular API which goes via [`http::Response::body_mut()`] to get a
    /// mut reference to the `Body`, and then use `as_reader()`. It is also possible to
    /// get a non-shared, owned reader via [`Body::into_reader()`].
    ///
    /// * Reader is not limited by default. That means a malicious server could
    ///   exhaust all avaliable memory on your client machine.
    ///   To set a limit use [`Body::into_with_config()`].
    /// * Reader will error if `Content-Length` is set, but the connection is closed
    ///   before all bytes are received.
    ///
    /// # Example
    ///
    /// ```
    /// use std::io::Read;
    ///
    /// let mut res = ureq::get("http://httpbin.org/bytes/100")
    ///     .call()?;
    ///
    /// let mut bytes: Vec<u8> = Vec::with_capacity(1000);
    /// res.body_mut().as_reader()
    ///     .read_to_end(&mut bytes)?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn as_reader(&mut self) -> BodyReader {
        self.with_config().reader()
    }

    /// Turn this response into an owned `impl Read` of the body.
    ///
    /// Sometimes it might be useful to disconnect the body reader from the body.
    /// The reader returned by [`Body::as_reader()`] borrows the body, while this
    /// variant consumes the body and turns it into a reader with lifetime `'static`.
    /// The reader can for instance be sent to another thread.
    ///
    /// * Reader is not limited by default. That means a malicious server could
    ///   exhaust all avaliable memory on your client machine.
    ///   To set a limit use [`Body::into_with_config()`].
    /// * Reader will error if `Content-Length` is set, but the connection is closed
    ///   before all bytes are received.
    ///
    /// ```
    /// use std::io::Read;
    ///
    /// let res = ureq::get("http://httpbin.org/bytes/100")
    ///     .call()?;
    ///
    /// let (_, body) = res.into_parts();
    ///
    /// let mut bytes: Vec<u8> = Vec::with_capacity(1000);
    /// body.into_reader()
    ///     .read_to_end(&mut bytes)?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn into_reader(self) -> BodyReader<'static> {
        self.into_with_config().reader()
    }

    /// Read the response as a string.
    ///
    /// * Response is limited to 10MB
    /// * Replaces incorrect utf-8 chars to `?`
    ///
    /// To change these defaults use [`Body::with_config()`].
    ///
    /// ```
    /// let mut res = ureq::get("http://httpbin.org/robots.txt")
    ///     .call()?;
    ///
    /// let s = res.body_mut().read_to_string()?;
    /// assert_eq!(s, "User-agent: *\nDisallow: /deny\n");
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// For larger text files, you must explicitly increase the limit:
    ///
    /// ```
    /// // Read a large text file (25MB)
    /// let text = ureq::get("http://httpbin.org/get")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     .limit(25 * 1024 * 1024) // 25MB
    ///     .read_to_string()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn read_to_string(&mut self) -> Result<String, Error> {
        self.with_config()
            .limit(MAX_BODY_SIZE)
            .lossy_utf8(true)
            .read_to_string()
    }

    /// Read the response to a vec.
    ///
    /// * Response is limited to 10MB.
    ///
    /// To change this default use [`Body::with_config()`].
    /// ```
    /// let mut res = ureq::get("http://httpbin.org/bytes/100")
    ///     .call()?;
    ///
    /// let bytes = res.body_mut().read_to_vec()?;
    /// assert_eq!(bytes.len(), 100);
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// For larger files, you must explicitly increase the limit:
    ///
    /// ```
    /// // Download a larger file (50MB)
    /// let bytes = ureq::get("http://httpbin.org/bytes/200000000")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     .limit(50 * 1024 * 1024) // 50MB
    ///     .read_to_vec()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn read_to_vec(&mut self) -> Result<Vec<u8>, Error> {
        self.with_config()
            //
            .limit(MAX_BODY_SIZE)
            .read_to_vec()
    }

    /// Read the response from JSON.
    ///
    /// * Response is limited to 10MB.
    ///
    /// To change this default use [`Body::with_config()`].
    ///
    /// The returned value is something that derives [`Deserialize`](serde::Deserialize).
    /// You might need to be explicit with which type you want. See example below.
    ///
    /// ```
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct BodyType {
    ///   slideshow: BodyTypeInner,
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct BodyTypeInner {
    ///   author: String,
    /// }
    ///
    /// let body = ureq::get("https://httpbin.org/json")
    ///     .call()?
    ///     .body_mut()
    ///     .read_json::<BodyType>()?;
    ///
    /// assert_eq!(body.slideshow.author, "Yours Truly");
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// For larger JSON files, you must explicitly increase the limit:
    ///
    /// ```
    /// use serde_json::Value;
    ///
    /// // Parse a large JSON file (30MB)
    /// let json: Value = ureq::get("https://httpbin.org/json")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     .limit(30 * 1024 * 1024) // 30MB
    ///     .read_json()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// # Performance
    ///
    /// When the response has a `Content-Length` that is comfortably below the limit,
    /// the body is buffered into memory and parsed from the slice, which is faster
    /// than streaming it through the reader. For chunked responses, or bodies at least
    /// a third of the limit, parsing falls back to reading directly from the stream.
    #[cfg(feature = "json")]
    pub fn read_json<T: serde::de::DeserializeOwned>(&mut self) -> Result<T, Error> {
        // serde_json's from_reader parses one byte at a time from the underlying
        // reader, which is significantly slower than parsing an in-memory slice. When
        // the Content-Length tells us the body comfortably fits within the limit, read
        // it into a Vec first and parse the slice. The 1/3 cutoff leaves some headroom
        // for the deserialized value. See
        // https://github.com/algesten/ureq/issues/1151
        if let Some(len) = self.content_length() {
            if len <= MAX_BODY_SIZE / JSON_BUFFER_DIVISOR {
                let vec = self.with_config().limit(MAX_BODY_SIZE).read_to_vec()?;
                let value: T = serde_json::from_slice(&vec)?;
                return Ok(value);
            }
        }
        let reader = self.with_config().limit(MAX_BODY_SIZE).reader();
        let value: T = serde_json::from_reader(reader).map_err(json_error)?;
        Ok(value)
    }

    /// Read the body data with configuration.
    ///
    /// This borrows the body which gives easier use with [`http::Response::body_mut()`].
    /// To get a non-borrowed reader use [`Body::into_with_config()`].
    ///
    /// # Example
    ///
    /// ```
    /// let reader = ureq::get("http://httpbin.org/bytes/100")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     // Reader will only read 50 bytes
    ///     .limit(50)
    ///     .reader();
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn with_config(&mut self) -> BodyWithConfig {
        self.ensure_decoded();
        let BodyDataSource::Decoded(decoded) = &mut self.source else {
            unreachable!("ensure_decoded makes the source decoded")
        };
        let handler = BodySourceRef::Shared(&mut decoded.chain, decoded.state.clone());
        BodyWithConfig::new(handler, self.info.clone())
    }

    /// Consume self and read the body with configuration.
    ///
    /// This consumes self and returns a reader with `'static` lifetime.
    ///
    /// # Example
    ///
    /// ```
    /// // Get the body out of http::Response
    /// let (_, body) = ureq::get("http://httpbin.org/bytes/100")
    ///     .call()?
    ///     .into_parts();
    ///
    /// let reader = body
    ///     .into_with_config()
    ///     // Reader will only read 50 bytes
    ///     .limit(50)
    ///     .reader();
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// This limit behavior can be used to prevent a malicious server from exhausting
    /// memory on the client machine. For example, if the machine running
    /// ureq has 1GB of RAM, you could protect the machine by setting a smaller
    /// limit such as 128MB. The exact number will vary by your client's download
    /// needs, available system resources, and system utilization.
    pub fn into_with_config(self) -> BodyWithConfig<'static> {
        let Body { source, info } = self;
        let decoded = match source {
            BodyDataSource::Decoded(v) => v,
            BodyDataSource::Raw(raw) => Box::new(DecodedBody::new(raw, &info)),
        };
        let handler = BodySourceRef::Owned(decoded.chain, decoded.state);
        BodyWithConfig::new(handler, info)
    }
}

/// Configuration of how to read the body.
///
/// Obtained via one of:
///
/// * [Body::with_config()]
/// * [Body::into_with_config()]
///
/// # Handling large responses
///
/// The `BodyWithConfig` is the primary way to increase the default 10MB size limit
/// when downloading large files to memory:
///
/// ```
/// // Download a 50MB file
/// let large_data = ureq::get("http://httpbin.org/bytes/200000000")
///     .call()?
///     .body_mut()
///     .with_config()
///     .limit(50 * 1024 * 1024) // 50MB
///     .read_to_vec()?;
/// # Ok::<_, ureq::Error>(())
/// ```
pub struct BodyWithConfig<'a> {
    handler: BodySourceRef<'a>,
    info: Arc<ResponseInfo>,
    limit: u64,
    lossy_utf8: bool,
}

impl<'a> BodyWithConfig<'a> {
    fn new(handler: BodySourceRef<'a>, info: Arc<ResponseInfo>) -> Self {
        BodyWithConfig {
            handler,
            info,
            limit: u64::MAX,
            lossy_utf8: false,
        }
    }

    /// Limit the response body.
    ///
    /// Controls how many bytes we should read before throwing an error. This is used
    /// to ensure RAM isn't exhausted by a server sending a very large response body.
    ///
    /// The limit counts the bytes delivered to the caller, i.e. the output after
    /// automatic decompression (gzip/brotli), charset conversion to UTF-8 and
    /// lossy utf-8 replacement — not the compressed transfer size. A body that
    /// is exactly `value` bytes long and then ends normally is not an error;
    /// only a body with at least one more output byte fails with
    /// [`Error::BodyExceedsLimit`].
    ///
    /// The default limit is `u64::MAX` (unlimited).
    pub fn limit(mut self, value: u64) -> Self {
        self.limit = value;
        self
    }

    /// Replace invalid utf-8 chars.
    ///
    /// `true` means that broken utf-8 characters are replaced by a question mark `?`
    /// (not utf-8 replacement char). This happens after charset conversion regardless of
    /// whether the **charset** feature is enabled or not.
    ///
    /// The default is `false`.
    pub fn lossy_utf8(mut self, value: bool) -> Self {
        self.lossy_utf8 = value;
        self
    }

    fn do_build(self) -> BodyReader<'a> {
        BodyReader::new(self.handler, self.limit, &self.info, self.lossy_utf8)
    }

    /// Creates a reader.
    ///
    /// The reader is either shared or owned, depending on `with_config` or `into_with_config`.
    ///
    /// # Example of owned vs shared
    ///
    /// ```
    /// // Creates an owned reader.
    /// let reader = ureq::get("https://httpbin.org/get")
    ///     .call()?
    ///     .into_body()
    ///     // takes ownership of Body
    ///     .into_with_config()
    ///     .limit(10)
    ///     .reader();
    /// # Ok::<_, ureq::Error>(())
    /// ```
    ///
    /// ```
    /// // Creates a shared reader.
    /// let reader = ureq::get("https://httpbin.org/get")
    ///     .call()?
    ///     .body_mut()
    ///     // borrows Body
    ///     .with_config()
    ///     .limit(10)
    ///     .reader();
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn reader(self) -> BodyReader<'a> {
        self.do_build()
    }

    /// Read into string.
    ///
    /// *Caution:* without a preceeding [`limit()`][BodyWithConfig::limit], this
    /// becomes an unbounded sized `String`. A bad server could exhaust your memory.
    ///
    /// # Example
    ///
    /// ```
    /// // Reads max 10k to a String.
    /// let string = ureq::get("https://httpbin.org/get")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     // Important. Limits body to 10k
    ///     .limit(10_000)
    ///     .read_to_string()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn read_to_string(self) -> Result<String, Error> {
        use std::io::Read;
        let mut reader = self.do_build();
        let mut buf = String::new();
        reader.read_to_string(&mut buf)?;
        Ok(buf)
    }

    /// Read into vector.
    ///
    /// *Caution:* without a preceeding [`limit()`][BodyWithConfig::limit], this
    /// becomes an unbounded sized `Vec`. A bad server could exhaust your memory.
    ///
    /// # Example
    ///
    /// ```
    /// // Reads max 10k to a Vec.
    /// let myvec = ureq::get("https://httpbin.org/get")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     // Important. Limits body to 10k
    ///     .limit(10_000)
    ///     .read_to_vec()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    pub fn read_to_vec(self) -> Result<Vec<u8>, Error> {
        use std::io::Read;
        let mut reader = self.do_build();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Read JSON body.
    ///
    /// *Caution:* without a preceeding [`limit()`][BodyWithConfig::limit], this
    /// becomes an unbounded sized `String`. A bad server could exhaust your memory.
    ///
    /// # Example
    ///
    /// ```
    /// use serde_json::Value;
    ///
    /// // Reads max 10k as a JSON value.
    /// let json: Value  = ureq::get("https://httpbin.org/get")
    ///     .call()?
    ///     .body_mut()
    ///     .with_config()
    ///     // Important. Limits body to 10k
    ///     .limit(10_000)
    ///     .read_json()?;
    /// # Ok::<_, ureq::Error>(())
    /// ```
    #[cfg(feature = "json")]
    pub fn read_json<T: serde::de::DeserializeOwned>(self) -> Result<T, Error> {
        // serde_json's from_reader parses one byte at a time from the underlying
        // reader, which is significantly slower than parsing an in-memory slice. When
        // the Content-Length tells us the body comfortably fits within the limit, read
        // it into a Vec first and parse the slice. The 1/3 cutoff leaves some headroom
        // for the deserialized value. See
        // https://github.com/algesten/ureq/issues/1151
        if let Some(len) = self.info.content_length() {
            if len <= self.limit / JSON_BUFFER_DIVISOR {
                let vec = self.read_to_vec()?;
                let value: T = serde_json::from_slice(&vec)?;
                return Ok(value);
            }
        }
        let reader = self.do_build();
        let value: T = serde_json::from_reader(reader).map_err(json_error)?;
        Ok(value)
    }
}

/// Translate a serde_json streaming error back to the error it wraps.
///
/// When `read_json()` parses directly from the body reader, a read failure
/// (such as [`Error::BodyExceedsLimit`], a timeout or a decompression
/// failure) travels through serde_json as an io error. Unwrap it so the
/// caller sees the original error rather than a JSON error. Genuine JSON
/// syntax and type errors are returned as [`Error::Json`] as before.
#[cfg(feature = "json")]
fn json_error(e: serde_json::Error) -> Error {
    if e.is_io() {
        // From<io::Error> for Error unwraps any wrapped ureq error
        // (BodyExceedsLimit, Timeout, Decompress, ...) and falls back
        // to Error::Io for other io errors.
        return Error::from(io::Error::from(e));
    }
    Error::Json(e)
}

#[derive(Debug, Clone, Copy)]
enum ContentEncoding {
    Gzip,
    Brotli,
    Unknown,
}

impl ContentEncoding {
    /// Parse a single content-coding token.
    ///
    /// Coding names are case-insensitive and may be surrounded by
    /// spaces and tabs. Anything else — including an empty token — is
    /// an unknown coding.
    fn parse(token: &str) -> Self {
        let token = token.trim_matches(|c| c == ' ' || c == '\t');
        if token.eq_ignore_ascii_case("gzip") {
            ContentEncoding::Gzip
        } else if token.eq_ignore_ascii_case("br") {
            ContentEncoding::Brotli
        } else {
            debug!("Unknown content-encoding: {}", token);
            ContentEncoding::Unknown
        }
    }

    /// Whether the coding can be decompressed with the enabled features.
    fn is_supported(&self) -> bool {
        match self {
            #[cfg(feature = "gzip")]
            ContentEncoding::Gzip => true,
            #[cfg(feature = "brotli")]
            ContentEncoding::Brotli => true,
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }
}

impl ResponseInfo {
    pub fn new(headers: &http::HeaderMap, body_mode: BodyMode) -> Self {
        // Content codings are listed in the order the server applied them,
        // either comma-separated in a single Content-Encoding header or
        // spread over multiple headers. Both forms combine into one
        // sequence in the order the values were received.
        let mut content_encodings = Vec::new();
        for value in headers.get_all(header::CONTENT_ENCODING) {
            match value.to_str() {
                Ok(s) => content_encodings.extend(s.split(',').map(ContentEncoding::parse)),
                // A non-ASCII value is not a valid coding name.
                Err(_) => content_encodings.push(ContentEncoding::Unknown),
            }
        }

        let (mime_type, charset) = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(split_content_type)
            .unwrap_or((None, None));

        ResponseInfo {
            content_encodings,
            mime_type,
            charset,
            body_mode,
        }
    }

    /// Returns true if the body will be decompressed.
    ///
    /// This is only the case when there is at least one content-coding and
    /// every coding in the sequence is supported by the enabled features
    /// (**gzip** and **brotli**). A single unknown or unsupported coding
    /// means the entire sequence is left as-is.
    pub(crate) fn is_decompressing(&self) -> bool {
        !self.content_encodings.is_empty()
            && self.content_encodings.iter().all(|e| e.is_supported())
    }

    /// The known length of the body, if any.
    ///
    /// This is the value of the `Content-Length` header. For chunked or close-delimited
    /// responses, and for bodies that are transparently decompressed (where the original
    /// `Content-Length` no longer reflects the decoded size), this is `None`.
    pub(crate) fn content_length(&self) -> Option<u64> {
        // After transparent decompression, the original Content-Length no longer
        // reflects the actual body size, so we return None.
        if self.is_decompressing() {
            return None;
        }
        match self.body_mode {
            BodyMode::NoBody => None,
            BodyMode::LengthDelimited(v) => Some(v),
            BodyMode::Chunked => None,
            BodyMode::CloseDelimited => None,
        }
    }

    /// Whether the mime type indicats text.
    fn is_text(&self) -> bool {
        self.mime_type
            .as_deref()
            .map(|s| s.starts_with("text/"))
            .unwrap_or(false)
    }

    /// The body mode to declare when a reader of this body is reused as
    /// a request send body.
    ///
    /// Decompression and charset conversion change the number of bytes
    /// in unknown ways, so a transformed body must be sent chunked.
    fn outgoing_body_mode(&self) -> BodyMode {
        let mut mode = self.body_mode;

        if self.is_decompressing() {
            // The decompressed size is not known up front.
            mode = BodyMode::Chunked;
        }

        #[cfg(feature = "charset")]
        if charset_encoding(self).is_some() {
            // Charset conversion changes the size.
            mode = BodyMode::Chunked;
        }

        mode
    }
}

fn split_content_type(content_type: &str) -> (Option<String>, Option<String>) {
    // Content-Type: text/plain; charset=iso-8859-1
    let mut split = content_type.split(';');

    let Some(mime_type) = split.next() else {
        return (None, None);
    };

    let mut charset = None;

    for maybe_charset in split {
        let maybe_charset = maybe_charset.trim();
        if let Some(s) = maybe_charset.strip_prefix("charset=") {
            charset = Some(s.to_string());
        }
    }

    (Some(mime_type.to_string()), charset)
}

/// A reader of the response data.
///
/// 1. If `Transfer-Encoding: chunked`, the returned reader will unchunk it
///    and any `Content-Length` header is ignored.
/// 2. If `Content-Encoding: gzip` (or `br`) and the corresponding feature
///    flag is enabled (**gzip** and **brotli**), decompresses the body data.
///    Multiple codings (`Content-Encoding: gzip, br`, or repeated headers)
///    are decoded in reverse order of how the server applied them, but only
///    if every coding in the sequence is supported.
/// 3. Given a header like `Content-Type: text/plain; charset=ISO-8859-1`
///    and the **charset** feature enabled, will translate the body to utf-8.
///    This mechanic need two components a mime-type starting `text/` and
///    a non-utf8 charset indication.
/// 4. If `Content-Length` is set, the returned reader is limited to this byte
///    length regardless of how many bytes the server sends.
/// 5. If no length header, the reader is until server stream end.
/// 6. The limit in the body method used to obtain the reader. The limit counts
///    the output bytes after the decoding steps above (decompression, charset
///    conversion, lossy utf-8 replacement). A body ending exactly at the limit
///    is read successfully; one more output byte causes
///    [`Error::BodyExceedsLimit`].
///
/// Note: The reader is also limited by the [`Body::as_reader`] and
/// [`Body::into_reader`] calls. If that limit is set very high, a malicious
/// server might return enough bytes to exhaust available memory. If you're
/// making requests to untrusted servers, you should use set that
/// limit accordingly.
///
/// # Example
///
/// ```
/// use std::io::Read;
/// let mut res = ureq::get("http://httpbin.org/bytes/100")
///     .call()?;
///
/// assert!(res.headers().contains_key("Content-Length"));
/// let len: usize = res.headers().get("Content-Length")
///     .unwrap().to_str().unwrap().parse().unwrap();
///
/// let mut bytes: Vec<u8> = Vec::with_capacity(len);
/// res.body_mut().as_reader()
///     .read_to_end(&mut bytes)?;
///
/// assert_eq!(bytes.len(), len);
/// # Ok::<_, ureq::Error>(())
/// ```
pub struct BodyReader<'a> {
    // The limit is outermost, so it counts the final output bytes: after
    // decompression, charset conversion and lossy utf-8 replacement.
    reader: LimitReader<MaybeLossyDecoder<DecodedRef<'a>>>,
    // If this reader is used as SendBody for another request, this
    // body mode can indiciate the content-length. Gzip, charset etc
    // would mean input is not same as output.
    outgoing_body_mode: BodyMode,
}

/// The content-decoding stage of the decoded body chain.
///
/// A response can carry a sequence of content-codings (`Content-Encoding:
/// gzip, br`), so the decoders nest recursively: each layer decodes the
/// output of the previous one.
enum ContentDecoder {
    Source(RawSource),
    #[cfg(feature = "gzip")]
    Gzip(Box<gzip::GzipDecoder<ContentDecoder>>),
    #[cfg(feature = "brotli")]
    Brotli(Box<brotli::BrotliDecoder<ContentDecoder>>),
}

impl io::Read for ContentDecoder {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ContentDecoder::Source(v) => v.read(buf),
            #[cfg(feature = "gzip")]
            ContentDecoder::Gzip(v) => v.read(buf),
            #[cfg(feature = "brotli")]
            ContentDecoder::Brotli(v) => v.read(buf),
        }
    }
}

impl<'a> BodyReader<'a> {
    fn new(
        source: BodySourceRef<'a>,
        limit: u64,
        info: &ResponseInfo,
        lossy_utf8: bool,
    ) -> BodyReader<'a> {
        // This is outgoing body_mode in case we are using the BodyReader as a send body
        // in a proxy situation.
        let outgoing_body_mode = info.outgoing_body_mode();

        let (chain, state) = match source {
            BodySourceRef::Shared(chain, state) => (ChainRef::Shared(chain), state),
            BodySourceRef::Owned(chain, state) => (ChainRef::Owned(chain), state),
        };

        let decoded = DecodedRef {
            chain,
            state: state.clone(),
        };

        let reader = if info.is_text() && lossy_utf8 {
            MaybeLossyDecoder::Lossy(LossyUtf8Reader::new(decoded))
        } else {
            MaybeLossyDecoder::PassThrough(decoded)
        };

        // The size limit applies to the output bytes the caller receives,
        // i.e. after decompression, charset conversion and lossy utf-8
        // replacement. A body that ends exactly at the limit is not an error.
        let reader = LimitReader::new(reader, limit, state);

        BodyReader {
            outgoing_body_mode,
            reader,
        }
    }

    pub(crate) fn body_mode(&self) -> BodyMode {
        self.outgoing_body_mode
    }
}

/// Wrap the raw source in one decoder per content-coding.
///
/// The server applied the codings in the order they are listed, so
/// decoding happens in reverse: the last listed coding is the outermost
/// layer on the wire and is decoded first.
///
/// The chain is only built when [`ResponseInfo::is_decompressing`], i.e.
/// every coding in the sequence is supported by the enabled features.
/// Otherwise the body passes through untouched.
fn content_decoders(raw: RawSource, info: &ResponseInfo) -> ContentDecoder {
    let mut reader = ContentDecoder::Source(raw);

    if !info.is_decompressing() {
        return reader;
    }

    for encoding in info.content_encodings.iter().rev() {
        reader = match encoding {
            #[cfg(feature = "gzip")]
            ContentEncoding::Gzip => {
                debug!("Decoding gzip");
                ContentDecoder::Gzip(Box::new(gzip::GzipDecoder::new(reader)))
            }
            #[cfg(feature = "brotli")]
            ContentEncoding::Brotli => {
                debug!("Decoding brotli");
                ContentDecoder::Brotli(Box::new(brotli::BrotliDecoder::new(reader)))
            }
            // is_decompressing() guarantees every coding in the sequence
            // is supported by the enabled features, so this never happens.
            #[allow(unreachable_patterns)]
            _ => reader,
        };
    }

    reader
}

/// The charset the body is converted from, if charset decoding applies.
///
/// This needs two components: a mime-type starting `text/` and a
/// non-utf8 charset indication.
#[cfg(feature = "charset")]
fn charset_encoding(info: &ResponseInfo) -> Option<&'static encoding_rs::Encoding> {
    use encoding_rs::{Encoding, UTF_8};

    if !info.is_text() {
        return None;
    }

    let from = info
        .charset
        .as_deref()
        .and_then(|c| Encoding::for_label(c.as_bytes()))
        .unwrap_or(UTF_8);

    (from != UTF_8).then_some(from)
}

#[allow(unused)]
fn charset_decoder(reader: ContentDecoder, info: &ResponseInfo) -> CharsetDecoder<ContentDecoder> {
    #[cfg(feature = "charset")]
    if let Some(from) = charset_encoding(info) {
        debug!("Decoding charset {}", from.name());
        return CharsetDecoder::Decoder(self::charset::CharCodec::new(
            reader,
            from,
            encoding_rs::UTF_8,
        ));
    }

    CharsetDecoder::PassThrough(reader)
}

enum MaybeLossyDecoder<R> {
    Lossy(LossyUtf8Reader<R>),
    PassThrough(R),
}

impl<R: io::Read> io::Read for MaybeLossyDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            MaybeLossyDecoder::Lossy(r) => r.read(buf),
            MaybeLossyDecoder::PassThrough(r) => r.read(buf),
        }
    }
}

impl<'a> io::Read for BodyReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

enum CharsetDecoder<R> {
    #[cfg(feature = "charset")]
    Decoder(charset::CharCodec<R>),
    PassThrough(R),
}

impl<R: io::Read> io::Read for CharsetDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(feature = "charset")]
            CharsetDecoder::Decoder(v) => v.read(buf),
            CharsetDecoder::PassThrough(v) => v.read(buf),
        }
    }
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body").finish()
    }
}

/// A reference to the decoded body chain, either borrowed (via
/// [`Body::with_config`]) or owned (via [`Body::into_with_config`]).
enum BodySourceRef<'a> {
    Shared(
        &'a mut CharsetDecoder<ContentDecoder>,
        Arc<Mutex<SharedState>>,
    ),
    Owned(CharsetDecoder<ContentDecoder>, Arc<Mutex<SharedState>>),
}

/// Per-reader handle on the shared decoded body chain.
///
/// All readers of one body read from the same chain, so reading
/// continues where the previous reader stopped. Terminal conditions
/// (decompression failure, end of body) are handled here so they behave
/// the same no matter which reader observes them first.
struct DecodedRef<'a> {
    chain: ChainRef<'a>,
    state: Arc<Mutex<SharedState>>,
}

enum ChainRef<'a> {
    Shared(&'a mut CharsetDecoder<ContentDecoder>),
    Owned(CharsetDecoder<ContentDecoder>),
}

impl<'a> DecodedRef<'a> {
    /// Return a held connection to the pool, if there is one.
    fn release_connection(&self) {
        let held = self.state.lock().unwrap().held_connection.take();
        if let Some(held) = held {
            held.release();
        }
    }
}

impl<'a> io::Read for DecodedRef<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Reading into an empty buffer is always a no-op. It must not
        // touch the chain or be mistaken for end-of-body.
        if buf.is_empty() {
            return Ok(0);
        }

        // A decompression failure is terminal for the whole body. Every
        // subsequent reader must see the same failure, not a normal end.
        #[cfg(any(feature = "gzip", feature = "brotli"))]
        if let Some(encoding) = self.state.lock().unwrap().decompress_failed {
            return Err(
                Error::Decompress(encoding, io::Error::other("decompression failed")).into_io(),
            );
        }

        let chain = match &mut self.chain {
            ChainRef::Shared(v) => &mut **v,
            ChainRef::Owned(v) => v,
        };

        match chain.read(buf) {
            Ok(0) => {
                // The decoded body is fully delivered. A connection held
                // back from the pool can be reused now.
                self.release_connection();
                Ok(0)
            }
            Err(e) => {
                // A decompression failure stays in effect for the rest
                // of this body.
                #[cfg(any(feature = "gzip", feature = "brotli"))]
                if let Some(encoding) = decompress_encoding(&e) {
                    self.state.lock().unwrap().decompress_failed = Some(encoding);
                }
                Err(e)
            }
            ok => ok,
        }
    }
}

/// The content-coding of a decompression failure, if the error is one.
#[cfg(any(feature = "gzip", feature = "brotli"))]
fn decompress_encoding(e: &io::Error) -> Option<&'static str> {
    match e.get_ref()?.downcast_ref::<Error>() {
        Some(Error::Decompress(encoding, _)) => Some(*encoding),
        _ => None,
    }
}

#[cfg(all(test, feature = "_test"))]
mod test {
    use crate::Error;
    use crate::test::init_test_log;
    use crate::transport::set_handler;

    #[test]
    fn content_type_without_charset() {
        init_test_log();
        set_handler("/get", 200, &[("content-type", "application/json")], b"{}");

        let res = crate::get("https://my.test/get").call().unwrap();
        assert_eq!(res.body().mime_type(), Some("application/json"));
        assert!(res.body().charset().is_none());
    }

    #[test]
    fn content_type_with_charset() {
        init_test_log();
        set_handler(
            "/get",
            200,
            &[("content-type", "application/json; charset=iso-8859-4")],
            b"{}",
        );

        let res = crate::get("https://my.test/get").call().unwrap();
        assert_eq!(res.body().mime_type(), Some("application/json"));
        assert_eq!(res.body().charset(), Some("iso-8859-4"));
    }

    #[test]
    fn chunked_transfer() {
        init_test_log();

        let s = "3\r\n\
            hel\r\n\
            b\r\n\
            lo world!!!\r\n\
            0\r\n\
            \r\n";

        set_handler(
            "/get",
            200,
            &[("transfer-encoding", "chunked")],
            s.as_bytes(),
        );

        let mut res = crate::get("https://my.test/get").call().unwrap();
        let b = res.body_mut().read_to_string().unwrap();
        assert_eq!(b, "hello world!!!");
    }

    #[test]
    #[cfg(feature = "json")]
    fn read_json_buffered() {
        use serde_json::Value;

        let json = br#"{"hello":"world","nums":[1,2,3]}"#;

        let mut body = crate::Body::builder().data(json.to_vec());
        let value: Value = body.read_json().unwrap();
        assert_eq!(value["hello"], "world");
        assert_eq!(value["nums"][2], 3);
    }

    #[test]
    #[cfg(feature = "json")]
    fn read_json_streamed() {
        use serde_json::Value;

        let json = br#"{"hello":"world","nums":[1,2,3]}"#;

        let mut body = crate::Body::builder().data(json.to_vec());
        let value: Value = body.with_config().limit(40).read_json().unwrap();
        assert_eq!(value["hello"], "world");
        assert_eq!(value["nums"][2], 3);
    }

    #[test]
    #[cfg(feature = "json")]
    fn read_json_exact_limit() {
        use serde_json::Value;

        // A JSON body of exactly the limit size parses successfully.
        let json = br#"{"hello":"world"}"#;
        let len = json.len().to_string();
        set_handler(
            "/json",
            200,
            &[
                ("content-length", &len),
                ("content-type", "application/json"),
            ],
            json,
        );

        let mut res = crate::get("https://my.test/json").call().unwrap();
        let value: Value = res
            .body_mut()
            .with_config()
            .limit(json.len() as u64)
            .read_json()
            .unwrap();
        assert_eq!(value["hello"], "world");
    }

    #[test]
    #[cfg(feature = "json")]
    fn read_json_limit_exceeded_is_not_json_error() {
        use serde_json::Value;

        let json = br#"{"hello":"world","nums":[1,2,3]}"#;
        let len = json.len().to_string();

        // Known content-length (streamed parse, since the length is more
        // than a third of the limit).
        set_handler(
            "/json_len",
            200,
            &[
                ("content-length", &len),
                ("content-type", "application/json"),
            ],
            json,
        );
        let mut res = crate::get("https://my.test/json_len").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_json::<Value>()
            .unwrap_err();
        assert!(
            matches!(err, Error::BodyExceedsLimit(5)),
            "expected BodyExceedsLimit, got: {:?}",
            err
        );

        // Chunked (unknown length).
        let mut chunked = format!("{:x}\r\n", json.len());
        chunked.push_str(std::str::from_utf8(json).unwrap());
        chunked.push_str("\r\n0\r\n\r\n");
        set_handler(
            "/json_chunked",
            200,
            &[
                ("transfer-encoding", "chunked"),
                ("content-type", "application/json"),
            ],
            chunked.as_bytes(),
        );
        let mut res = crate::get("https://my.test/json_chunked").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_json::<Value>()
            .unwrap_err();
        assert!(
            matches!(err, Error::BodyExceedsLimit(5)),
            "expected BodyExceedsLimit, got: {:?}",
            err
        );
    }

    #[test]
    #[cfg(all(feature = "json", feature = "gzip"))]
    fn read_json_gzip_limit_exceeded_is_not_json_error() {
        use serde_json::Value;

        // A JSON body that compresses below the limit still exceeds it once
        // decompressed, and the error must be BodyExceedsLimit, not Json.
        let json = br#"{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":1}"#;
        let compressed = {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(json).unwrap();
            encoder.finish().unwrap()
        };
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/json_gz",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
                ("content-type", "application/json"),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/json_gz").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(10)
            .read_json::<Value>()
            .unwrap_err();
        assert!(
            matches!(err, Error::BodyExceedsLimit(10)),
            "expected BodyExceedsLimit, got: {:?}",
            err
        );
    }

    #[test]
    #[cfg(feature = "charset")]
    fn limit_counts_utf8_output_bytes() {
        init_test_log();

        // ISO-8859-1 encoded "é" is 1 byte, but 2 bytes as UTF-8. The limit
        // counts the converted output.
        set_handler(
            "/get",
            200,
            &[
                ("content-type", "text/plain; charset=iso-8859-1"),
                ("content-length", "1"),
            ],
            &[0xE9],
        );

        let mut res = crate::get("https://my.test/get").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(1)
            .read_to_string()
            .unwrap_err();
        assert!(matches!(err, Error::BodyExceedsLimit(1)));

        set_handler(
            "/get",
            200,
            &[
                ("content-type", "text/plain; charset=iso-8859-1"),
                ("content-length", "1"),
            ],
            &[0xE9],
        );
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let s = res
            .body_mut()
            .with_config()
            .limit(2)
            .read_to_string()
            .unwrap();
        assert_eq!(s, "é");
    }

    #[test]
    fn large_response_header() {
        init_test_log();
        set_handler(
            "/get",
            200,
            &[("content-type", &"b".repeat(64 * 1024))],
            b"{}",
        );

        let err = crate::get("https://my.test/get").call().unwrap_err();
        assert!(matches!(err, Error::LargeResponseHeader(_, _)));
    }
}
