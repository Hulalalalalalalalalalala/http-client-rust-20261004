use std::io;

use brotli_decompressor::Decompressor;

use crate::Error;
use crate::error::is_wrapped_ureq_error;

pub(crate) struct BrotliDecoder<R: io::Read>(Decompressor<R>);

impl<R: io::Read> BrotliDecoder<R> {
    pub fn new(reader: R) -> Self {
        BrotliDecoder(Decompressor::new(reader, 4096))
    }
}

impl<R: io::Read> io::Read for BrotliDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf).map_err(|e| {
            if is_wrapped_ureq_error(&e) {
                // If this already is a ureq::Error, like Timeout, pass it along.
                e
            } else {
                Error::Decompress("brotli", e).into_io()
            }
        })
    }
}

#[cfg(all(test, feature = "_test"))]
mod test {
    // A valid brotli stream (5018 decompressed bytes of ASCII text).
    // Taken from the MIT/BSD-licensed brotli-decompressor crate test fixtures.
    const BROTLI_FIXTURE: &[u8] = include_bytes!("testdata/ipsum.brotli");
    const FIXTURE_LEN: u64 = 5018;

    // The limit counts decompressed output bytes, not the compressed wire size.
    #[test]
    fn br_limit_counts_decompressed_bytes() {
        use crate::test::init_test_log;
        use crate::transport::set_handler;

        init_test_log();
        let len = BROTLI_FIXTURE.len().to_string();
        set_handler(
            "/br_limit",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            BROTLI_FIXTURE,
        );

        // Limit below the decompressed size but well above the compressed size.
        let mut res = crate::get("https://my.test/br_limit").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(FIXTURE_LEN - 1)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(n) if n == FIXTURE_LEN - 1));
    }

    // Exactly N decompressed bytes reads in full.
    #[test]
    fn br_limit_exact_size() {
        use crate::test::init_test_log;
        use crate::transport::set_handler;

        init_test_log();
        let len = BROTLI_FIXTURE.len().to_string();
        set_handler(
            "/br_exact",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            BROTLI_FIXTURE,
        );

        let mut res = crate::get("https://my.test/br_exact").call().unwrap();
        let body = res
            .body_mut()
            .with_config()
            .limit(FIXTURE_LEN)
            .read_to_vec()
            .unwrap();
        assert_eq!(body.len() as u64, FIXTURE_LEN);
    }

    // Streaming delivers at most the limit and then errors, regardless of
    // caller buffer size.
    #[test]
    fn br_limit_streaming_delivery_capped() {
        use std::io::Read;

        use crate::test::init_test_log;
        use crate::transport::set_handler;

        init_test_log();
        let len = BROTLI_FIXTURE.len().to_string();
        set_handler(
            "/br_stream",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            BROTLI_FIXTURE,
        );

        let limit = 2000u64;
        let mut res = crate::get("https://my.test/br_stream").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(limit).reader();

        let mut total = 0u64;
        let mut buf = [0u8; 13];
        let err = loop {
            match reader.read(&mut buf) {
                Ok(0) => panic!("unexpected clean end for body over the limit"),
                Ok(n) => {
                    total += n as u64;
                    assert!(total <= limit, "delivered {total}, limit {limit}");
                }
                Err(e) => break crate::Error::from(e),
            }
        };
        assert_eq!(total, limit);
        assert!(matches!(err, crate::Error::BodyExceedsLimit(n) if n == limit));
    }
}
