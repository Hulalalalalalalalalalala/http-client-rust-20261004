use std::io;

use crate::Error;

/// Reader that limits how many bytes the caller can receive.
///
/// This sits outermost in the body reader chain, so the limit counts the
/// final output bytes: after automatic decompression, charset conversion to
/// UTF-8 and lossy utf-8 replacement (whichever are enabled).
///
/// A body that is exactly `limit` bytes long and then ends normally is a
/// successful read. Only when there is at least one more output byte does
/// the reader fail with [`Error::BodyExceedsLimit`].
pub(crate) struct LimitReader<R> {
    reader: R,
    limit: u64,
    left: u64,
    exceeded: bool,
}

impl<R> LimitReader<R> {
    pub fn new(reader: R, limit: u64) -> Self {
        LimitReader {
            reader,
            limit,
            left: limit,
            exceeded: false,
        }
    }
}

impl<R: io::Read> io::Read for LimitReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Reading into an empty buffer is always a no-op. It must not consume
        // body bytes, trigger the limit check, or change the outcome of
        // subsequent reads.
        if buf.is_empty() {
            return Ok(0);
        }

        // Once the limit is exceeded, keep reporting the error. The caller
        // must not receive further body bytes, nor see the exceeded body
        // as a normal end-of-body.
        if self.exceeded {
            return Err(Error::BodyExceedsLimit(self.limit).into_io());
        }

        if self.left == 0 {
            // The limit is reached. What remains is only to determine whether
            // the body ends exactly here. Probe the wrapped reader for one
            // more output byte.
            //
            // Any error from the wrapped reader (decompression, transport,
            // timeout) must surface as-is, not be masked by the limit error,
            // and we must not report success before the wrapped reader
            // confirms the end of the body.
            let mut probe = [0u8; 1];
            let n = self.reader.read(&mut probe)?;

            if n == 0 {
                // The body ended exactly at the limit. Normal EOF.
                return Ok(0);
            }

            // There was at least one more output byte, the body is too big.
            self.exceeded = true;
            return Err(Error::BodyExceedsLimit(self.limit).into_io());
        }

        // The max buffer size is usize, which may be 32 bit.
        let max = (self.left.min(usize::MAX as u64) as usize).min(buf.len());

        let n = self.reader.read(&mut buf[..max])?;

        self.left -= n as u64;

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
        // A body of exactly the limit size must read successfully.
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
    fn limit_exact_size_plus_one() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "6")], b"hello!");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_string()
            .unwrap_err();
        assert!(matches!(err, Error::BodyExceedsLimit(5)));
    }

    #[test]
    fn limit_zero() {
        init_test_log();
        // Empty body with limit 0 is fine.
        set_handler("/empty", 200, &[("content-length", "0")], b"");
        let mut res = crate::get("https://my.test/empty").call().unwrap();
        let s = res
            .body_mut()
            .with_config()
            .limit(0)
            .read_to_string()
            .unwrap();
        assert_eq!(s, "");

        // Non-empty body with limit 0 exceeds.
        set_handler("/nonempty", 200, &[("content-length", "1")], b"x");
        let mut res = crate::get("https://my.test/nonempty").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(0)
            .read_to_string()
            .unwrap_err();
        assert!(matches!(err, Error::BodyExceedsLimit(0)));
    }

    #[test]
    fn limit_empty_buffer_read_is_noop() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "5")], b"hello");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(5).reader();

        use io::Read;
        // Empty buffer reads return 0 without consuming the body or
        // triggering the limit check.
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(reader.read(&mut []).unwrap(), 0);

        // The body is still fully readable afterwards.
        let mut s = String::new();
        reader.read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn limit_streaming_mixed_buffer_sizes() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "10")], b"0123456789");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(10).reader();

        use io::Read;
        // Read with different buffer sizes; the result must be the same as
        // reading with any single buffer size.
        let mut out = Vec::new();
        for size in [1, 4, 3, 2, 8] {
            let mut buf = vec![0u8; size];
            let n = reader.read(&mut buf).unwrap();
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, b"0123456789");
        // Exactly at the limit, the stream ends normally.
        let mut buf = [0u8; 4];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn limit_streaming_never_delivers_more_than_limit() {
        init_test_log();
        set_handler("/get", 200, &[("content-length", "10")], b"0123456789");
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(4).reader();

        use io::Read;
        let mut buf = [0u8; 3];
        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(reader.read(&mut buf).unwrap(), 1);
        // The 5th output byte exists, so the limit is exceeded...
        let err = reader.read(&mut buf).unwrap_err();
        assert!(matches!(Error::from(err), Error::BodyExceedsLimit(4)));
        // ...and it keeps being reported rather than turning into EOF.
        let err = reader.read(&mut buf).unwrap_err();
        assert!(matches!(Error::from(err), Error::BodyExceedsLimit(4)));
    }

    #[test]
    fn limit_chunked_exact_size() {
        init_test_log();
        let s = "5\r\n\
            hello\r\n\
            0\r\n\
            \r\n";
        set_handler(
            "/get",
            200,
            &[("transfer-encoding", "chunked")],
            s.as_bytes(),
        );
        let mut res = crate::get("https://my.test/get").call().unwrap();
        let body = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_string()
            .unwrap();
        assert_eq!(body, "hello");
    }
}
