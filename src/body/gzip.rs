use std::io;

use flate2::read::MultiGzDecoder;

use crate::Error;
use crate::error::is_wrapped_ureq_error;

pub(crate) struct GzipDecoder<R>(MultiGzDecoder<R>);

impl<R: io::Read> GzipDecoder<R> {
    pub fn new(reader: R) -> Self {
        GzipDecoder(MultiGzDecoder::new(reader))
    }
}

impl<R: io::Read> io::Read for GzipDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf).map_err(|e| {
            if is_wrapped_ureq_error(&e) {
                // If this already is a ureq::Error, like Timeout, pass it along.
                e
            } else {
                Error::Decompress("gzip", e).into_io()
            }
        })
    }
}

#[cfg(all(test, feature = "_test"))]
mod test {
    use crate::Agent;
    use crate::test::init_test_log;
    use crate::transport::set_handler;

    // Test that a stream gets returned to the pool if it is gzip encoded and the gzip
    // decoder reads the exact amount from a chunked stream, not past the 0. This
    // happens because gzip has built-in knowledge of the length to read.
    #[test]
    fn gz_internal_length() {
        init_test_log();

        let gz_body = vec![
            b'E', b'\r', b'\n', // 14 first chunk
            0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0xCB, 0x48, 0xCD, 0xC9,
            b'\r', b'\n', //
            b'E', b'\r', b'\n', // 14 second chunk
            0xC9, 0x57, 0x28, 0xCF, 0x2F, 0xCA, 0x49, 0x51, 0xC8, 0x18, 0xBC, 0x6C, 0x00, 0xA5,
            b'\r', b'\n', //
            b'7', b'\r', b'\n', // 7 third chunk
            0x5C, 0x7C, 0xEF, 0xA7, 0x00, 0x00, 0x00, //
            b'\r', b'\n', //
            // end
            b'0', b'\r', b'\n', //
            b'\r', b'\n', //
        ];

        let agent = Agent::new_with_defaults();
        assert_eq!(agent.pool_count(), 0);

        set_handler(
            "/gz_body",
            200,
            &[
                ("transfer-encoding", "chunked"),
                ("content-encoding", "gzip"),
            ],
            &gz_body,
        );

        let mut res = agent.get("https://example.test/gz_body").call().unwrap();
        res.body_mut().read_to_string().unwrap();

        assert_eq!(agent.pool_count(), 1);
    }

    /// Gzip-compress a byte slice using flate2.
    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    // When ureq transparently decompresses a gzip response, Content-Encoding and
    // Content-Length headers must be stripped from the response per RFC 9110 §8.7.
    // Content-Length no longer matches the decompressed body size, and
    // Content-Encoding no longer applies since the caller receives plaintext.
    #[test]
    fn gz_strips_content_encoding_and_content_length() {
        init_test_log();

        let original = b"{\"hello\":\"world\"}";
        let compressed = gzip_compress(original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_strip",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
                ("content-type", "application/json"),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_strip").call().unwrap();

        // Content-Encoding must be removed after transparent decompression
        assert!(
            res.headers().get("content-encoding").is_none(),
            "Content-Encoding should be stripped after gzip decompression, got: {:?}",
            res.headers().get("content-encoding"),
        );

        // Content-Length header must be removed (it referred to compressed size)
        assert!(
            res.headers().get("content-length").is_none(),
            "Content-Length header should be stripped after gzip decompression, got: {:?}",
            res.headers().get("content-length"),
        );

        // Body::content_length() must also return None after decompression
        assert!(
            res.body().content_length().is_none(),
            "Body::content_length() should return None after gzip decompression, got: {:?}",
            res.body().content_length(),
        );

        // Other headers must be preserved
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "application/json",
        );

        // Body must be the decompressed original
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "{\"hello\":\"world\"}");
    }

    // The limit counts decompressed output bytes, not the compressed wire size.
    #[test]
    fn gz_limit_counts_decompressed_bytes() {
        init_test_log();

        let original = vec![b'a'; 5000];
        let compressed = gzip_compress(&original);
        assert!(compressed.len() < original.len());
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_limit",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        // A limit larger than the compressed body but smaller than the
        // decompressed body must still be exceeded.
        let mut res = crate::get("https://my.test/gz_limit").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(1000)));
    }

    // Exactly N decompressed bytes ending cleanly reads in full; N+1 fails.
    #[test]
    fn gz_limit_exact_size_and_plus_one() {
        init_test_log();

        let original = vec![b'b'; 1000];
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_exact",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_exact").call().unwrap();
        let body = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap();
        assert_eq!(body.len(), 1000);

        // Re-register the handler for the second request.
        set_handler(
            "/gz_exact",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_exact").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(999)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(999)));
    }

    // Streaming a gzip body delivers at most N bytes and reports the limit
    // without buffering the whole (potentially huge) decompressed body.
    #[test]
    fn gz_limit_streaming_delivery_capped() {
        use std::io::Read;

        init_test_log();

        let original = vec![b'c'; 10_000];
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_stream",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_stream").call().unwrap();
        let mut reader = res.body_mut().with_config().limit(137).reader();

        let mut total = 0usize;
        let mut buf = [0u8; 16];
        let err = loop {
            match reader.read(&mut buf) {
                Ok(0) => panic!("unexpected clean end for body over the limit"),
                Ok(n) => {
                    total += n;
                    assert!(total <= 137, "delivered {total} bytes, limit is 137");
                }
                Err(e) => break crate::Error::from(e),
            }
        };
        assert_eq!(total, 137);
        assert!(matches!(err, crate::Error::BodyExceedsLimit(137)));
    }

    // After exactly N decompressed bytes, the remaining compressed data only
    // confirms the end of the body. If the gzip checksum is corrupt there, the
    // decompression error must surface rather than being masked by a limit
    // error (or reported as success).
    #[test]
    fn gz_limit_corrupt_trailer_after_exact_limit_is_decompress_error() {
        init_test_log();

        let original = vec![b'd'; 1000];
        let mut compressed = gzip_compress(&original);

        // Corrupt the CRC32 field (4 bytes immediately before the 4-byte ISIZE).
        let crc_start = compressed.len() - 8;
        for b in &mut compressed[crc_start..crc_start + 4] {
            *b ^= 0xFF;
        }
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_corrupt_trailer",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_corrupt_trailer")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip Decompress error, got {err:?}"
        );
    }

    // A truncated gzip stream right at the output limit is a decompression
    // error, not BodyExceedsLimit and not success.
    #[test]
    fn gz_limit_truncated_stream_after_exact_limit_is_error() {
        init_test_log();

        let original = vec![b'e'; 1000];
        let compressed = gzip_compress(&original);
        // Drop the trailer and part of the deflate tail.
        let truncated = compressed[..compressed.len() - 12].to_vec();
        let truncated_len = truncated.len().to_string();

        set_handler(
            "/gz_truncated",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &truncated_len),
            ],
            &truncated,
        );

        let mut res = crate::get("https://my.test/gz_truncated").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip Decompress error, got {err:?}"
        );
    }
}
