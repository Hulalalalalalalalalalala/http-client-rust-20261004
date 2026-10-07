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
        let mut res = crate::get("https://my.test/gz_limit_exceeded")
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

    // A comma-separated sequence in a single Content-Encoding header means
    // the server applied the codings in that order. "gzip, gzip" is the
    // content compressed twice, and reading must produce the original
    // content, not stop at the intermediate compressed bytes.
    #[test]
    fn gz_double_gzip_single_header() {
        init_test_log();

        let original = b"hello hello hello world";
        let compressed = gzip_compress(&gzip_compress(original));
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

        // Both headers are stripped, like for a single coding.
        assert!(res.headers().get("content-encoding").is_none());
        assert!(res.headers().get("content-length").is_none());
        assert!(res.body().content_length().is_none());
        // Other headers are preserved.
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/plain",
        );

        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello world");
    }

    // The same sequence spread over multiple Content-Encoding headers must
    // be understood as the same sequence, in the order received.
    #[test]
    fn gz_double_gzip_multiple_headers() {
        init_test_log();

        let original = b"hello hello hello world";
        let compressed = gzip_compress(&gzip_compress(original));
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

        let mut res = crate::get("https://my.test/gz_double_multi")
            .call()
            .unwrap();
        assert!(res.headers().get("content-encoding").is_none());
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello world");
    }

    // Coding names are case-insensitive and may be surrounded by
    // spaces and tabs.
    #[test]
    fn gz_case_insensitive_and_whitespace() {
        init_test_log();

        let original = b"hello hello hello world";
        let compressed = gzip_compress(&gzip_compress(original));
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
        assert_eq!(body, "hello hello hello world");
    }

    // If any coding in the sequence is unknown, the entire sequence is
    // left compressed: all Content-Encoding values and Content-Length are
    // preserved and the body is delivered as received.
    #[test]
    fn gz_unknown_coding_in_sequence_not_decompressed() {
        init_test_log();

        let original = b"hello hello hello world";
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

        // All Content-Encoding values and the Content-Length are kept.
        let mut encodings = res.headers().get_all("content-encoding").iter();
        assert_eq!(encodings.next().unwrap(), "gzip");
        assert_eq!(encodings.next().unwrap(), "deflate");
        assert!(encodings.next().is_none());
        assert_eq!(
            res.headers().get("content-length").unwrap(),
            &compressed_len
        );
        assert_eq!(res.body().content_length(), Some(compressed.len() as u64));

        // The body is the untouched compressed bytes.
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, compressed);
    }

    // An empty item in the sequence is not a valid coding and disables
    // decompression of the entire sequence.
    #[test]
    fn gz_empty_item_in_sequence_not_decompressed() {
        init_test_log();

        let original = b"hello hello hello world";
        let compressed = gzip_compress(&gzip_compress(original));
        let compressed_len = compressed.len().to_string();

        for (path, header) in [
            ("/gz_empty_1", "gzip,,gzip"),
            ("/gz_empty_2", "gzip, "),
            ("/gz_empty_3", ",gzip"),
        ] {
            set_handler(
                path,
                200,
                &[
                    ("content-encoding", header),
                    ("content-length", &compressed_len),
                ],
                &compressed,
            );

            let mut res = crate::get(&format!("https://my.test{}", path))
                .call()
                .unwrap();
            assert!(
                res.headers().get("content-encoding").is_some(),
                "Content-Encoding must be kept for {:?}",
                header,
            );
            let body = res.body_mut().read_to_vec().unwrap();
            assert_eq!(body, compressed, "body must stay compressed");
        }
    }

    // A coding whose feature is not enabled disables decompression of the
    // entire sequence, even if the other codings are supported.
    #[test]
    #[cfg(not(feature = "brotli"))]
    fn gz_unsupported_coding_in_sequence_not_decompressed() {
        init_test_log();

        let original = b"hello hello hello world";
        let compressed = gzip_compress(original);
        let compressed_len = compressed.len().to_string();

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
        assert_eq!(res.headers().get("content-encoding").unwrap(), "gzip, br");
        assert_eq!(
            res.headers().get("content-length").unwrap(),
            &compressed_len
        );
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, compressed);
    }

    // The body size limit counts the final output bytes, not the compressed
    // transfer size or any intermediate layer, also for multi-layer coding.
    #[test]
    fn gz_double_limit_counts_output_bytes() {
        init_test_log();

        let original = vec![b'a'; 1000];
        let compressed = gzip_compress(&gzip_compress(&original));
        let compressed_len = compressed.len().to_string();

        // A body of exactly the limit size ends normally.
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

        // One output byte more than the limit exceeds it.
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

    // With multiple layers, a corrupt inner gzip trailer must surface as a
    // decompression error even when the output has reached the limit.
    #[test]
    fn gz_double_corrupt_inner_trailer_at_limit() {
        init_test_log();

        let original = b"hello";
        let mut inner = gzip_compress(original);
        // Corrupt the CRC32 in the inner gzip trailer.
        let n = inner.len();
        inner[n - 8] ^= 0xFF;
        let compressed = gzip_compress(&inner);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_corrupt",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_corrupt")
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
            "expected decompression error, got: {:?}",
            err
        );
    }

    // The HTTP body is received in full (Content-Length satisfied), but the
    // compressed data is truncated. That is a decompression error.
    #[test]
    fn gz_double_truncated_stream() {
        init_test_log();

        let original = b"hello world, hello world";
        let compressed = gzip_compress(&gzip_compress(original));
        let truncated = &compressed[..compressed.len() - 4];
        let truncated_len = truncated.len().to_string();

        set_handler(
            "/gz_double_truncated",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &truncated_len),
            ],
            truncated,
        );

        let mut res = crate::get("https://my.test/gz_double_truncated")
            .call()
            .unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected decompression error, got: {:?}",
            err
        );
    }

    // Reading a multi-layer body in small pieces delivers the same content
    // and the same final outcome as reading it all at once.
    #[test]
    fn gz_double_streaming_reads() {
        init_test_log();

        let original: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&gzip_compress(&original));
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_stream",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_stream")
            .call()
            .unwrap();
        let mut reader = res.body_mut().as_reader();

        use std::io::Read;
        let mut out = Vec::new();
        // Read in odd-sized pieces; the stream must deliver output before
        // the entire body has been received.
        for size in [1usize, 7, 100, 3, 4096, 2] {
            let mut buf = vec![0u8; size];
            loop {
                let n = reader.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                out.extend_from_slice(&buf[..n]);
                if out.len() >= original.len() {
                    break;
                }
            }
        }
        // Drain the rest.
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }

    // A multi-layer decompressed response body used as a subsequent request
    // body sends the full decompressed content. The compressed
    // Content-Length must not be reused to truncate the output.
    #[test]
    fn gz_double_body_sent_decompressed() {
        init_test_log();

        let original = b"hello world, hello world, hello world".to_vec();
        let compressed = gzip_compress(&gzip_compress(&original));
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_src",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_src").call().unwrap();

        use crate::send_body::AsSendBody;
        let mut send_body = res.as_body();

        // The compressed Content-Length does not apply to the decompressed
        // output, so the body must be sent chunked.
        assert_eq!(
            send_body.body_mode().unwrap(),
            ureq_proto::BodyMode::Chunked
        );

        // The full decompressed content is sent, not truncated at the
        // compressed length.
        let mut out = Vec::new();
        let mut buf = [0u8; 16];
        loop {
            let n = send_body.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, original);
    }

    // Reading a prefix with one reader and continuing with another must
    // deliver exactly the same bytes as reading the body in one go. This
    // exercises as_reader(), with_config().reader() and read_to_vec()
    // one after another on the same body.
    #[test]
    fn gz_continue_across_readers_content_length() {
        init_test_log();

        let original: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_cont_cl",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_cont_cl").call().unwrap();
        let mut out = Vec::new();

        use std::io::Read;

        // 1. Read a small prefix with as_reader().
        {
            let mut reader = res.body_mut().as_reader();
            let mut buf = [0u8; 7];
            reader.read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }

        // 2. Continue with a fresh as_reader().
        {
            let mut reader = res.body_mut().as_reader();
            let mut buf = [0u8; 100];
            let n = reader.read(&mut buf).unwrap();
            out.extend_from_slice(&buf[..n]);
        }

        // 3. Continue with with_config().reader().
        {
            let mut reader = res.body_mut().with_config().reader();
            let mut buf = [0u8; 13];
            reader.read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }

        // 4. Finish with read_to_vec().
        let rest = res.body_mut().read_to_vec().unwrap();
        out.extend_from_slice(&rest);

        assert_eq!(out, original);

        // After a normal end, new readers see an empty body.
        let after = res.body_mut().read_to_vec().unwrap();
        assert!(after.is_empty());
    }

    // The same continuation must work for chunked transfer-encoding.
    #[test]
    fn gz_continue_across_readers_chunked() {
        init_test_log();

        let original: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&original);

        // Chunk-encode the compressed body in odd-sized chunks.
        let mut wire = Vec::new();
        for chunk in compressed.chunks(11) {
            wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            wire.extend_from_slice(chunk);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");

        set_handler(
            "/gz_cont_chunked",
            200,
            &[
                ("content-encoding", "gzip"),
                ("transfer-encoding", "chunked"),
            ],
            &wire,
        );

        let mut res = crate::get("https://my.test/gz_cont_chunked")
            .call()
            .unwrap();
        let mut out = Vec::new();

        use std::io::Read;
        {
            let mut buf = [0u8; 9];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }
        let rest = res.body_mut().read_to_vec().unwrap();
        out.extend_from_slice(&rest);

        assert_eq!(out, original);
    }

    // The same continuation must work when the content is gzipped twice.
    #[test]
    fn gz_double_continue_across_readers() {
        init_test_log();

        let original: Vec<u8> = (0..1500u32).map(|i| (i % 241) as u8).collect();
        let compressed = gzip_compress(&gzip_compress(&original));
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_double_cont",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_double_cont").call().unwrap();
        let mut out = Vec::new();

        use std::io::Read;
        {
            let mut buf = [0u8; 9];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }
        let rest = res.body_mut().read_to_vec().unwrap();
        out.extend_from_slice(&rest);

        assert_eq!(out, original);
    }

    // The same continuation must work when the content is gzipped twice
    // and transferred chunked.
    #[test]
    fn gz_double_continue_across_readers_chunked() {
        init_test_log();

        let original: Vec<u8> = (0..1500u32).map(|i| (i % 241) as u8).collect();
        let compressed = gzip_compress(&gzip_compress(&original));

        // Chunk-encode the compressed body in odd-sized chunks.
        let mut wire = Vec::new();
        for chunk in compressed.chunks(13) {
            wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            wire.extend_from_slice(chunk);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");

        set_handler(
            "/gz_double_cont_chunked",
            200,
            &[
                ("content-encoding", "gzip, gzip"),
                ("transfer-encoding", "chunked"),
            ],
            &wire,
        );

        let mut res = crate::get("https://my.test/gz_double_cont_chunked")
            .call()
            .unwrap();
        let mut out = Vec::new();

        use std::io::Read;
        {
            let mut buf = [0u8; 9];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }
        let rest = res.body_mut().read_to_vec().unwrap();
        out.extend_from_slice(&rest);

        assert_eq!(out, original);
    }

    // The compressed data arriving in many small pieces must not change
    // the result of continuing with another reader.
    #[test]
    fn gz_continue_across_readers_split_delivery() {
        init_test_log();

        let original: Vec<u8> = (0..2000u32).map(|i| (i % 253) as u8).collect();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len();

        crate::transport::set_handler_raw("/gz_cont_split", move |w| {
            use std::io::Write;
            write!(
                w,
                "HTTP/1.1 200 OK\r\n\
                content-encoding: gzip\r\n\
                content-length: {}\r\n\
                \r\n",
                compressed_len
            )?;
            // Deliver the compressed body in small pieces.
            for chunk in compressed.chunks(9) {
                w.write_all(chunk)?;
                w.flush()?;
            }
            Ok(())
        });

        let mut res = crate::get("https://my.test/gz_cont_split").call().unwrap();
        let mut out = Vec::new();

        use std::io::Read;
        {
            let mut buf = [0u8; 5];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            out.extend_from_slice(&buf);
        }
        let rest = res.body_mut().read_to_vec().unwrap();
        out.extend_from_slice(&rest);

        assert_eq!(out, original);
    }

    // After reading a prefix with a borrowed reader, into_reader() must
    // continue at the same position. The owned reader can be sent to
    // another thread.
    #[test]
    fn gz_continue_into_reader_on_another_thread() {
        init_test_log();

        let original: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_cont_owned",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_cont_owned").call().unwrap();

        use std::io::Read;
        {
            let mut buf = [0u8; 3];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            assert_eq!(&buf, &original[..3]);
        }

        let (_, body) = res.into_parts();
        let mut reader = body.into_reader();

        let rest = std::thread::spawn(move || {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).unwrap();
            out
        })
        .join()
        .unwrap();

        assert_eq!(rest, &original[3..]);
    }

    // A reader that only takes a small prefix must return as soon as the
    // needed data is there, not wait for the entire response to arrive.
    #[test]
    fn gz_prefix_read_does_not_wait_for_full_body() {
        init_test_log();

        let original: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len();
        let split = compressed_len / 2;
        let first = compressed[..split].to_vec();
        let second = compressed[split..].to_vec();

        crate::transport::set_handler_raw("/gz_prefix", move |w| {
            use std::io::Write;
            write!(
                w,
                "HTTP/1.1 200 OK\r\n\
                content-encoding: gzip\r\n\
                content-length: {}\r\n\
                \r\n",
                compressed_len
            )?;
            w.write_all(&first)?;
            w.flush()?;
            // The rest of the body arrives much later than the client's
            // recv-body timeout. A prefix read must not wait for it.
            std::thread::sleep(std::time::Duration::from_millis(500));
            // The client is gone by now; ignore write failures.
            let _ = w.write_all(&second);
            let _ = w.flush();
            Ok(())
        });

        let mut res = crate::get("https://my.test/gz_prefix")
            .config()
            .timeout_recv_body(Some(std::time::Duration::from_millis(200)))
            .build()
            .call()
            .unwrap();

        use std::io::Read;
        let mut buf = [0u8; 10];
        res.body_mut().as_reader().read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &original[..10]);
    }

    // The limit of each with_config() counts only the bytes delivered in
    // that read, not the bytes taken by earlier readers.
    #[test]
    fn gz_limit_counts_only_current_read() {
        init_test_log();

        use std::io::Read;

        // 8-byte body: take 3, the remaining 5 fit a limit of 5.
        let compressed = gzip_compress(b"hello!!!");
        let compressed_len = compressed.len().to_string();
        set_handler(
            "/gz_lim8",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_lim8").call().unwrap();
        {
            let mut buf = [0u8; 3];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"hel");
        }
        let rest = res.body_mut().with_config().limit(5).read_to_vec().unwrap();
        assert_eq!(rest, b"lo!!!");

        // 9-byte body: take 3, the remaining 6 exceed a limit of 5.
        let compressed = gzip_compress(b"hello!!!!");
        let compressed_len = compressed.len().to_string();
        set_handler(
            "/gz_lim9",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );
        let mut res = crate::get("https://my.test/gz_lim9").call().unwrap();
        {
            let mut buf = [0u8; 3];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
        }
        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(5)));
    }

    // Taking only part of what the limit allows and ending the borrow is
    // not an error; the next reader continues normally.
    #[test]
    fn gz_limit_partial_read_within_limit_can_continue() {
        init_test_log();

        let compressed = gzip_compress(b"hello world");
        let compressed_len = compressed.len().to_string();
        set_handler(
            "/gz_lim_partial",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_lim_partial").call().unwrap();

        use std::io::Read;
        {
            // Take 3 bytes with a limit of 10: no exceedance happens.
            let mut reader = res.body_mut().with_config().limit(10).reader();
            let mut buf = [0u8; 3];
            reader.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"hel");
        }

        let rest = res.body_mut().read_to_vec().unwrap();
        assert_eq!(rest, b"lo world");
    }

    // Once a read confirms the body exceeds a limit, every subsequent
    // reader of the same body keeps reporting that same limit.
    #[test]
    fn gz_limit_exceeded_is_sticky_across_readers() {
        init_test_log();

        let compressed = gzip_compress(b"hello world");
        let compressed_len = compressed.len().to_string();
        set_handler(
            "/gz_lim_sticky",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_lim_sticky").call().unwrap();

        let err = res
            .body_mut()
            .with_config()
            .limit(5)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(5)));

        // A new reader, even with a larger limit, reports the first limit.
        let err = res
            .body_mut()
            .with_config()
            .limit(1000)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(err, crate::Error::BodyExceedsLimit(5)));

        use std::io::Read;
        // So does a plain as_reader().
        let mut buf = [0u8; 4];
        let err = res.body_mut().as_reader().read(&mut buf).unwrap_err();
        assert!(matches!(
            crate::Error::from(err),
            crate::Error::BodyExceedsLimit(5)
        ));

        // Empty buffer reads are still a no-op returning 0.
        assert_eq!(res.body_mut().as_reader().read(&mut []).unwrap(), 0);

        // An owned reader reports the same limit too.
        let (_, body) = res.into_parts();
        let mut reader = body.into_reader();
        let err = reader.read(&mut buf).unwrap_err();
        assert!(matches!(
            crate::Error::from(err),
            crate::Error::BodyExceedsLimit(5)
        ));
    }

    // A corrupt gzip trailer is a decompression failure that stays in
    // effect for subsequent readers of the same body.
    #[test]
    fn gz_corrupt_trailer_is_sticky_across_readers() {
        init_test_log();

        let mut compressed = gzip_compress(b"hello");
        // Corrupt the CRC32 in the gzip trailer.
        let n = compressed.len();
        compressed[n - 8] ^= 0xFF;
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_corrupt_sticky",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_corrupt_sticky")
            .call()
            .unwrap();

        use std::io::Read;
        let mut out = Vec::new();
        let err = res
            .body_mut()
            .as_reader()
            .read_to_end(&mut out)
            .unwrap_err();
        assert!(matches!(
            crate::Error::from(err),
            crate::Error::Decompress("gzip", _)
        ));

        // A new reader must see the same failure, not a normal end.
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(matches!(err, crate::Error::Decompress("gzip", _)));
    }

    // A timeout while reading keeps its category for the next reader;
    // switching readers must not turn it into a normal end of body.
    #[test]
    fn gz_timeout_keeps_category_across_readers() {
        init_test_log();

        let original: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len();
        let split = compressed_len / 2;
        let first = compressed[..split].to_vec();
        let second = compressed[split..].to_vec();

        crate::transport::set_handler_raw("/gz_timeout", move |w| {
            use std::io::Write;
            write!(
                w,
                "HTTP/1.1 200 OK\r\n\
                content-encoding: gzip\r\n\
                content-length: {}\r\n\
                \r\n",
                compressed_len
            )?;
            w.write_all(&first)?;
            w.flush()?;
            // The rest arrives long after the client's recv-body timeout.
            std::thread::sleep(std::time::Duration::from_millis(600));
            let _ = w.write_all(&second);
            Ok(())
        });

        let mut res = crate::get("https://my.test/gz_timeout")
            .config()
            .timeout_recv_body(Some(std::time::Duration::from_millis(100)))
            .build()
            .call()
            .unwrap();

        use std::io::Read;
        // Read until the body stalls: the read times out.
        let mut buf = [0u8; 4096];
        let err = loop {
            match res.body_mut().as_reader().read(&mut buf) {
                Ok(0) => panic!("body must not end normally"),
                Ok(_) => continue,
                Err(e) => break e,
            }
        };
        assert!(
            matches!(crate::Error::from(err), crate::Error::Timeout(_)),
            "expected a timeout"
        );

        // A new reader on the same body hits the same timeout, not EOF.
        let err = res.body_mut().as_reader().read(&mut buf).unwrap_err();
        assert!(
            matches!(crate::Error::from(err), crate::Error::Timeout(_)),
            "expected a timeout"
        );
    }

    // Empty buffer reads return zero and do not advance the body.
    #[test]
    fn gz_empty_buffer_read_is_noop() {
        init_test_log();

        let compressed = gzip_compress(b"hello");
        let compressed_len = compressed.len().to_string();
        set_handler(
            "/gz_empty_buf",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let mut res = crate::get("https://my.test/gz_empty_buf").call().unwrap();

        use std::io::Read;
        {
            let mut reader = res.body_mut().as_reader();
            assert_eq!(reader.read(&mut []).unwrap(), 0);
            assert_eq!(reader.read(&mut []).unwrap(), 0);
        }

        // The body is still fully readable afterwards.
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, b"hello");
    }

    // Even when all compressed bytes have been received, the connection
    // must stay out of the pool while decompressed content remains
    // undelivered. Once the body is read to end, it can be reused.
    #[test]
    fn gz_pool_held_until_body_delivered() {
        init_test_log();

        let original = b"hello world, hello world".to_vec();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_pool_hold",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let agent = Agent::new_with_defaults();
        assert_eq!(agent.pool_count(), 0);

        let mut res = agent
            .get("https://example.test/gz_pool_hold")
            .call()
            .unwrap();

        use std::io::Read;
        {
            // Read only a prefix. The whole compressed body fits in one
            // read, so the compressed data is fully received by now, but
            // decompressed content remains undelivered.
            let mut buf = [0u8; 5];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"hello");
        }

        // The connection must not be reusable yet.
        assert_eq!(agent.pool_count(), 0);

        // Reading the body to end releases the connection to the pool.
        let rest = res.body_mut().read_to_vec().unwrap();
        assert_eq!(rest, b" world, hello world");
        assert_eq!(agent.pool_count(), 1);
    }

    // Dropping a body that was not read to end closes the connection
    // instead of returning it to the pool.
    #[test]
    fn gz_pool_not_reused_when_body_dropped_unfinished() {
        init_test_log();

        let original = b"hello world, hello world".to_vec();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_pool_drop",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let agent = Agent::new_with_defaults();

        {
            let mut res = agent
                .get("https://example.test/gz_pool_drop")
                .call()
                .unwrap();
            use std::io::Read;
            let mut buf = [0u8; 5];
            res.body_mut().as_reader().read_exact(&mut buf).unwrap();
            // Drop the response with the body unfinished.
        }

        assert_eq!(agent.pool_count(), 0);
    }

    // A gzip body that is fully read to a normal end leaves the
    // connection reusable for the next request of the same agent.
    #[test]
    fn gz_pool_reuse_after_full_read() {
        init_test_log();

        let original = b"hello world, hello world".to_vec();
        let compressed = gzip_compress(&original);
        let compressed_len = compressed.len().to_string();

        set_handler(
            "/gz_pool_reuse",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-length", &compressed_len),
            ],
            &compressed,
        );

        let agent = Agent::new_with_defaults();

        let mut res = agent
            .get("https://example.test/gz_pool_reuse")
            .call()
            .unwrap();
        let body = res.body_mut().read_to_vec().unwrap();
        assert_eq!(body, original);

        // The connection is back in the pool, available for the next request.
        assert_eq!(agent.pool_count(), 1);
    }
}
