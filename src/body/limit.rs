use std::io;

use crate::Error;

/// Limits the number of output bytes delivered to the caller.
///
/// The limit counts bytes that the caller actually receives, i.e. bytes after
/// decompression (gzip/br), charset conversion to utf-8 and any lossy utf-8
/// replacement. It does not count compressed wire bytes or characters.
///
/// Boundary rules:
///
/// * A body of exactly `limit` bytes that ends cleanly reads in full.
/// * [`Error::BodyExceedsLimit`] is returned only once an `limit + 1`th output
///   byte is known to exist. At most `limit` bytes are ever delivered, and the
///   error is returned on the read *after* the final permitted byte, so a read
///   that fills the boundary still returns its data successfully.
/// * Whether the caller uses one large buffer or many small buffers makes no
///   difference; byte counting is cumulative.
/// * A read into an empty buffer always returns `Ok(0)` without consuming body
///   data, triggering the boundary check, or affecting later reads.
pub(crate) struct LimitReader<R> {
    reader: R,
    limit: u64,
    /// Output bytes already delivered to the caller. Always `<= limit`.
    read: u64,
    /// A previous boundary probe found an additional byte. All further reads
    /// (except empty-buffer reads) fail.
    exceeded: bool,
    /// A previous boundary probe found a clean end of body.
    ended: bool,
}

impl<R: io::Read> LimitReader<R> {
    pub fn new(reader: R, limit: u64) -> Self {
        LimitReader {
            reader,
            limit,
            read: 0,
            exceeded: false,
            ended: false,
        }
    }

    /// Decide whether the body ends right at the boundary or continues.
    ///
    /// Called only once exactly `limit` bytes have been delivered. We ask the
    /// inner reader for a single additional output byte: if there is none, the
    /// body is exactly `limit` bytes long; if there is one (or finishing the
    /// stream raises an error, such as a gzip checksum failure, truncation or
    /// timeout), that outcome is propagated as-is rather than masking it with
    /// the limit error.
    fn settle_boundary(&mut self) -> io::Result<usize> {
        let mut extra = [0u8; 1];
        match self.reader.read(&mut extra) {
            Ok(0) => {
                self.ended = true;
                Ok(0)
            }
            Ok(_) => {
                self.exceeded = true;
                Err(Error::BodyExceedsLimit(self.limit).into_io())
            }
            Err(e) => Err(e),
        }
    }
}

impl<R: io::Read> io::Read for LimitReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // An empty buffer must pass through without consuming anything and
        // without triggering the boundary check.
        if buf.is_empty() {
            return Ok(0);
        }

        if self.exceeded {
            return Err(Error::BodyExceedsLimit(self.limit).into_io());
        }

        if self.ended {
            return Ok(0);
        }

        // Exactly `limit` bytes have been delivered in earlier calls. Settle
        // whether the body ends here (Ok(0)) or has an limit+1th byte (error).
        if self.read == self.limit {
            return self.settle_boundary();
        }

        // The max buffer size is usize, which may be 32 bit.
        let remaining = self.limit - self.read;
        let max = (remaining.min(usize::MAX as u64) as usize).min(buf.len());

        let n = self.reader.read(&mut buf[..max])?;
        self.read += n as u64;

        // n == 0 means the inner reader reached a clean end of body. If the
        // body was truncated, the error comes via the Err branch instead.
        // Deliberately do not probe here: the next read settles the boundary,
        // which leaves trailer verification / close detection to the inner
        // reader until the caller asks for more data.
        Ok(n)
    }
}

#[cfg(all(test, feature = "_test"))]
mod test {
    use std::io;

    use crate::Error;
    use crate::test::init_test_log;
    use crate::transport::set_handler;

    #[test]
    fn short_read() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "10")], b"hello");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let err = res.body_mut().read_to_string().unwrap_err();
        let ioe = err.into_io();
        assert_eq!(ioe.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn limit_below_size() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "5")], b"hello");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(3)
            .read_to_string()
            .unwrap_err();
        println!("{:?}", err);
        assert!(matches!(err, Error::BodyExceedsLimit(3)));
    }

    #[test]
    fn limit_exact_size() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "5")], b"hello");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let s = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_string()
            .unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn limit_zero_empty_body() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "0")], b"");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let v = res.body_mut().with_config().limit(0).read_to_vec().unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn limit_zero_nonempty_body() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "1")], b"x");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(0)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, Error::BodyExceedsLimit(0)));
    }

    #[test]
    fn limit_streaming_delivery_capped() {
        use std::io::Read;

        init_test_log();
        set_handler("/get", 200, &[("content-length", "10")], b"0123456789");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(4).reader();

        // No more than `limit` bytes are ever delivered, regardless of how the
        // caller sizes their buffers, and no extra body follows the error.
        let mut delivered = Vec::new();
        let mut buf = [0u8; 2];
        let mut err = None;
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    assert!(delivered.len() + n <= 4, "delivered beyond limit");
                    delivered.extend_from_slice(&buf[..n]);
                }
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert_eq!(delivered, b"0123");
        let ureq = Error::from(err.unwrap());
        assert!(matches!(ureq, Error::BodyExceedsLimit(4)));

        // After the error the reader keeps failing rather than ending cleanly.
        let again = reader.read(&mut buf).unwrap_err();
        assert!(matches!(Error::from(again), Error::BodyExceedsLimit(4)));
    }

    #[test]
    fn limit_independent_of_buffer_size() {
        use std::io::Read;

        init_test_log();
        set_handler("/get", 200, &[("content-length", "7")], b"0123456");

        for buf_size in [1usize, 2, 3, 5, 16, 128] {
            let mut res = crate::get("https://my.test/get").call().unwrap();
            let mut reader = res.body_mut().with_config().limit(7).reader();
            let mut got = Vec::new();
            let mut buf = vec![0u8; buf_size];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => got.extend_from_slice(&buf[..n]),
                    Err(e) => panic!("unexpected error with buf {buf_size}: {e}"),
                }
            }
            assert_eq!(got, b"0123456", "buf_size {buf_size}");
        }
    }

    #[test]
    fn limit_empty_buffer_read_is_noop() {
        use std::io::Read;

        init_test_log();
        set_handler("/get", 200, &[("content-length", "3")], b"abc");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(3).reader();

        let mut empty = [];
        for _ in 0..3 {
            assert_eq!(reader.read(&mut empty).unwrap(), 0);
        }

        let mut buf = [0u8; 3];
        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, b"abc");
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    // After delivering exactly the limit, a truncated transport (connection
    // closed while Content-Length expects more) must surface as a transport
    // error rather than as BodyExceedsLimit or a clean end.
    #[test]
    fn limit_boundary_truncation_is_transport_error() {
        use std::io::Read;

        init_test_log();
        crate::transport::set_handler_raw("/truncated_at_limit", |w| {
            use std::io::Write;
            write!(
                w,
                "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n12345678<hangup>"
            )
        });

        let mut res = crate::get("https://my.test/truncated_at_limit")
            .call()
            .unwrap();
        let mut reader = res.body_mut().with_config().limit(8).reader();
        let mut got = Vec::new();
        let mut buf = [0u8; 4];
        let err = loop {
            match reader.read(&mut buf) {
                Ok(0) => break None,
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(e) => break Some(e),
            }
        }
        .expect("expected an error, got clean EOF");

        assert_eq!(got, b"12345678");
        let ureq = Error::from(err);
        assert!(
            matches!(ureq, Error::Io(_)),
            "expected a transport (io) error, got {ureq:?}"
        );
    }
}
