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

    // The body size limit must count decompressed output bytes, not the
    // compressed transfer size.
    #[test]
    fn gz_limit_counts_decompressed_bytes() {
        init_test_log();

        let original = vec![b'a'; 1000];
        let compressed = gzip_compress(&original);
        // Ensure the test premise: the body compresses well.
        assert!(compressed.len() < 100);
        let compressed_len = compressed.len().to_string();

        // A limit between the compressed and decompressed sizes must succeed,
        // since the limit applies to the decompressed output.
        set_handler(
            "/gz_limit_ok",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_limit_ok").call().unwrap();
        let v = res
            .body_mut()
            .with_config()
            .limit(1500)
            .read_to_vec()
            .unwrap();
        assert_eq!(v, original);

        // A decompressed body of exactly the limit size ends normally.
        set_handler(
            "/gz_limit_exact",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_limit_exact").call().unwrap();
        let v = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap();
        assert_eq!(v.len(), 1000);

        // One decompressed byte less than the body size exceeds the limit,
        // even though the limit is way above the compressed size.
        set_handler(
            "/gz_limit_exceeded",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_limit_exceeded").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(999)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(999)));
    }

    // When the decompressed output ends exactly at the limit, the reader still
    // has to confirm the body ends. If the gzip trailer is corrupt, that
    // decompression error must surface, not a limit error and not a success.
    #[test]
    fn gz_corrupt_trailer_at_limit() {
        init_test_log();

        let original = b"hello";
        let mut compressed = gzip_compress(original);
        // Corrupt the CRC32 in the gzip trailer (last 8 bytes: CRC32 + ISIZE).
        let n = compressed.len();
        compressed[n - 8] ^= 0xFF;
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_corrupt",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_corrupt").call().unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected decompression error, got: {:?}",
            err
        );
    }

    /// Gzip-compress a byte slice twice.
    fn gzip_compress_twice(data: &[u8]) -> Vec<u8> {
        gzip_compress(&gzip_compress(data))
    }

    // A sequence containing a coding whose feature flag is not enabled is
    // not decompressed at all, even though the other codings are supported.
    #[test]
    #[cfg(not(feature = "brotli"))]
    fn gz_unsupported_coding_not_decompressed() {
        init_test_log();

        let original = b"hello";
        let compressed = gzip_compress(original);
        let compressed_len = compressed.len().to_string();

        // Comma-separated in one header.
        set_handler(
            "/gz_unsupported",
            200,
            &[
                ("content-encoding", "gzip, br"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_unsupported").call().unwrap();
        assert_eq!(
            res.headers()
                .get("content-encoding")
                .unwrap()
                .to_str()
                .unwrap(),
            "gzip, br"
        );
        assert_eq!(res.body().content_length(), Some(compressed.len() as u64));
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, compressed);

        // Split over two headers.
        set_handler(
            "/gz_unsupported_split",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-encoding", "br"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_unsupported_split")
            .call()
            .unwrap();
        assert_eq!(res.body().content_length(), Some(compressed.len() as u64));
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, compressed);
    }

    // A body compressed twice is listed as `gzip, gzip` and must be decoded
    // twice, delivering the original content rather than the intermediate
    // compressed bytes.
    #[test]
    fn gz_double_gzip_single_header() {
        init_test_log();

        let original = b"hello hello hello, multi-layer world!";
        let compressed = gzip_compress_twice(original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
                ("content-type", "text/plain"),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double").call().unwrap();

        // Headers are stripped like for single-layer decompression.
        assert!(res.headers().get("content-encoding").is_none());
        assert!(res.headers().get("content-length").is_none());
        assert!(res.body().content_length().is_none());
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/plain",
        );

        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello, multi-layer world!");
    }

    // The same sequence spread over multiple `Content-Encoding` headers is
    // one sequence in the order the values were received.
    #[test]
    fn gz_double_gzip_multiple_headers() {
        init_test_log();

        let original = b"hello hello hello, multi-layer world!";
        let compressed = gzip_compress_twice(original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_multi",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_multi").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello, multi-layer world!");
    }

    // Coding names are case-insensitive and may be surrounded by spaces
    // and tabs.
    #[test]
    fn gz_coding_names_case_and_whitespace() {
        init_test_log();

        let original = b"hello hello hello, multi-layer world!";
        let compressed = gzip_compress_twice(original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_case",
            200,
            &[
                ("content-encoding", "GZip ,\tgzip\t"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_case").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello, multi-layer world!");
    }

    // A sequence containing an unknown coding is not decompressed at all.
    // All Content-Encoding values and the Content-Length are preserved.
    #[test]
    fn gz_unknown_coding_not_decompressed() {
        init_test_log();

        let original = b"hello";
        let compressed = gzip_compress(original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_unknown",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-encoding", "deflate"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_unknown").call().unwrap();

        // Both Content-Encoding values and the Content-Length are preserved.
        let mut encodings = res.headers().get_all("content-encoding").iter();
        assert_eq!(encodings.next().unwrap().to_str().unwrap(), "gzip");
        assert_eq!(encodings.next().unwrap().to_str().unwrap(), "deflate");
        assert!(encodings.next().is_none());
        assert_eq!(
            res.headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap(),
            compressed_len
        );
        assert_eq!(res.body().content_length(), Some(compressed.len() as u64));

        // The body is the untouched compressed bytes.
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, compressed);
    }

    // An empty item in the sequence opts the entire sequence out of
    // decompression.
    #[test]
    fn gz_empty_coding_not_decompressed() {
        init_test_log();

        let original = b"hello";
        let compressed = gzip_compress(original);
        let compressed_len = compressed.len().to_string();

        for (i, encoding) in ["gzip,,gzip", "gzip,"].iter().enumerate() {
            let path = format!("/gz_empty_{}", i);
            // Leak to get a &'static str for the test handler.
            let path: &'static str = Box::leak(path.into_boxed_str());

            set_handler(
                path,
                200,
                &[
                    ("content-encoding", encoding),
                    ("content-length", &compressed_len),
                ],
                &compressed,
            );

            let mut res = crate::get(&format!("https://my.test{}", path))
                .call()
                .unwrap();

            assert_eq!(
                res.headers()
                    .get("content-encoding")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                *encoding
            );
            assert_eq!(res.body().content_length(), Some(compressed.len() as u64));

            let body = res.body_mut().read_to_vec().unwrap();
            assert_eq!(body, compressed);
        }
    }

    // The limit counts the final output bytes, not any intermediate or
    // transfer size, also for multi-layer bodies.
    #[test]
    fn gz_double_gzip_limit() {
        init_test_log();

        let original = vec![b'a'; 1000];
        let compressed = gzip_compress_twice(&original);
        let compressed_len = compressed.len().to_string();

        // A decompressed body of exactly the limit size ends normally.
        set_handler(
            "/gz_double_limit_exact",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_double_limit_exact")
            .call()
            .unwrap();
        let v = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap();
        assert_eq!(v, original);

        // One byte less than the decompressed size exceeds the limit.
        set_handler(
            "/gz_double_limit_exceeded",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_double_limit_exceeded")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(999)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(999)));
    }

    // A corrupt inner gzip layer (the encoding the server applied first)
    // surfaces as a gzip decompression error. It must not be rewritten by
    // the outer layer, and it must surface even when the output limit is
    // already reached.
    #[test]
    fn gz_double_gzip_inner_corrupt() {
        init_test_log();

        let original = b"hello";
        let mut inner = gzip_compress(original);
        // Corrupt the CRC32 of the inner gzip trailer.
        let n = inner.len();
        inner[n - 8] ^= 0xFF;
        let compressed = gzip_compress(&inner);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_inner_corrupt",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_inner_corrupt")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip decompression error, got: {:?}",
            err
        );
    }

    // A corrupt outer gzip layer surfaces as a gzip decompression error.
    #[test]
    fn gz_double_gzip_outer_corrupt() {
        init_test_log();

        let mut compressed = gzip_compress_twice(b"hello");
        // Corrupt the CRC32 of the outer gzip trailer.
        let n = compressed.len();
        compressed[n - 8] ^= 0xFF;
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_outer_corrupt",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_outer_corrupt")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip decompression error, got: {:?}",
            err
        );
    }

    // The HTTP body is received in full (Content-Length is satisfied), but
    // the compressed data itself is truncated. That is a decompression
    // error, not a clean end of body.
    #[test]
    fn gz_truncated_stream() {
        init_test_log();

        let full = gzip_compress(b"hello");
        let compressed = &full[..full.len() - 4];
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_truncated",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            compressed,
        );

        let mut res = crate::get("https://my.test/gz_truncated").call().unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip decompression error, got: {:?}",
            err
        );
    }

    // Same, but the outer layer of a double-compressed body is truncated.
    #[test]
    fn gz_double_gzip_truncated_stream() {
        init_test_log();

        let full = gzip_compress_twice(b"hello");
        let compressed = &full[..full.len() - 4];
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_truncated",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_truncated")
            .call()
            .unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected gzip decompression error, got: {:?}",
            err
        );
    }

    // Reading a multi-layer body in small chunks yields the same content
    // as reading it to the end at once.
    #[test]
    fn gz_double_gzip_chunked_reads() {
        init_test_log();

        let original: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress_twice(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_chunks",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_chunks").call().unwrap();
        let mut reader = res.body_mut().as_reader();

        use std::io::Read;
        let mut out = Vec::new();
        // Varying small buffer sizes, including 1-byte reads.
        for size in [1, 7, 3, 64, 2, 100] {
            let mut buf = vec![0u8; size];
            loop {
                let n = reader.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&buf[..n]);
            }
        }
        assert_eq!(out, original);
    }

    // Multi-layer decompression works the same for chunked transfer
    // encoding: headers are stripped and content_length() is None.
    #[test]
    fn gz_double_gzip_chunked_transfer() {
        init_test_log();

        let original = b"hello hello hello, multi-layer world!";
        let compressed = gzip_compress_twice(original);

        let mut body = format!("{:x}\r\n", compressed.len()).into_bytes();
        body.extend_from_slice(&compressed);
        body.extend_from_slice(b"\r\n0\r\n\r\n");

        set_handler(
            "/gz_double_chunked",
            200,
            &[
                ("transfer-encoding", "chunked"),
                ("content-encoding", "gzip, gzip"),
            ],
            &body,
        );

        let mut res = crate::get("https://my.test/gz_double_chunked")
            .call()
            .unwrap();

        assert!(res.headers().get("content-encoding").is_none());
        assert!(res.body().content_length().is_none());

        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello, multi-layer world!");
    }

    // A decompressed multi-layer body used as a subsequent request body
    // sends the full decompressed content. The compressed Content-Length
    // must not truncate the output.
    #[test]
    fn gz_double_gzip_as_send_body() {
        init_test_log();

        let original: Vec<u8> = (0..1000).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress_twice(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_send",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_send").call().unwrap();

        use crate::AsSendBody;
        use ureq_proto::BodyMode;
        let mut send_body = res.body_mut().as_body();

        // The outgoing body cannot reuse the compressed length.
        assert_eq!(send_body.body_mode().unwrap(), BodyMode::Chunked);

        let mut out = Vec::new();
        let mut buf = [0u8; 128];
        loop {
            let n = send_body.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, original);
    }
}
