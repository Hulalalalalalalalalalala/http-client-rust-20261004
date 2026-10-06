use std::io;

use brotli_decompressor::{BrotliDecompressStream, BrotliResult, BrotliState, StandardAlloc};

use crate::Error;

/// Size of the buffer holding compressed input.
const INPUT_BUFFER_SIZE: usize = 4096;

/// Streaming brotli decoder.
///
/// This drives [`BrotliDecompressStream`] directly instead of using
/// `brotli_decompressor::Decompressor`, because the latter silently
/// reports end-of-body when the input ends in the middle of the brotli
/// stream. Here a truncated stream is a decompression error.
pub(crate) struct BrotliDecoder<R: io::Read> {
    reader: R,
    state: BrotliState<StandardAlloc, StandardAlloc, StandardAlloc>,
    input: [u8; INPUT_BUFFER_SIZE],
    input_offset: usize,
    input_len: usize,
    total_out: usize,
    /// The brotli stream reached its proper end.
    done: bool,
}

impl<R: io::Read> BrotliDecoder<R> {
    pub fn new(reader: R) -> Self {
        BrotliDecoder {
            reader,
            state: BrotliState::new(
                StandardAlloc::default(),
                StandardAlloc::default(),
                StandardAlloc::default(),
            ),
            input: [0; INPUT_BUFFER_SIZE],
            input_offset: 0,
            input_len: 0,
            total_out: 0,
            done: false,
        }
    }
}

impl<R: io::Read> io::Read for BrotliDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.done {
            return Ok(0);
        }

        let mut output_offset = 0;

        loop {
            let mut avail_in = self.input_len - self.input_offset;
            let mut avail_out = buf.len() - output_offset;

            let result = BrotliDecompressStream(
                &mut avail_in,
                &mut self.input_offset,
                &self.input[..self.input_len],
                &mut avail_out,
                &mut output_offset,
                buf,
                &mut self.total_out,
                &mut self.state,
            );

            match result {
                BrotliResult::NeedsMoreInput => {
                    if output_offset > 0 {
                        // Deliver the output produced so far. More input is
                        // pulled from the underlying reader on the next read.
                        return Ok(output_offset);
                    }

                    // Compact the buffer, keeping unconsumed input.
                    self.input.copy_within(self.input_offset..self.input_len, 0);
                    self.input_len -= self.input_offset;
                    self.input_offset = 0;

                    if self.input_len == self.input.len() {
                        // The stream makes progress with far less than a full
                        // buffer of input; this would loop forever.
                        return Err(decompress_error("brotli stream made no progress"));
                    }

                    // Errors from the underlying reader (transport errors,
                    // timeouts, errors of an inner decoder) are passed
                    // through unchanged.
                    let n = self.reader.read(&mut self.input[self.input_len..])?;

                    if n == 0 {
                        // The HTTP body ended in the middle of the brotli
                        // stream.
                        return Err(decompress_error("truncated brotli stream"));
                    }

                    self.input_len += n;
                }
                BrotliResult::NeedsMoreOutput => {
                    // The output buffer is full.
                    return Ok(output_offset);
                }
                BrotliResult::ResultSuccess => {
                    self.done = true;

                    if self.input_offset != self.input_len {
                        // Garbage after the end of the brotli stream.
                        return Err(decompress_error("trailing data after brotli stream"));
                    }

                    return Ok(output_offset);
                }
                BrotliResult::ResultFailure => {
                    return Err(decompress_error("corrupt brotli stream"));
                }
            }
        }
    }
}

fn decompress_error(message: &'static str) -> io::Error {
    Error::Decompress("brotli", io::Error::new(io::ErrorKind::InvalidData, message)).into_io()
}

#[cfg(all(test, feature = "_test"))]
mod test {
    use crate::test::init_test_log;
    use crate::transport::set_handler;

    const DATA: &[u8] = b"hello hello hello, multi-layer world!";

    /// brotli(DATA)
    const BR: [u8; 41] = [
        11, 18, 128, 104, 101, 108, 108, 111, 32, 104, 101, 108, 108, 111, 32, 104, 101, 108, 108,
        111, 44, 32, 109, 117, 108, 116, 105, 45, 108, 97, 121, 101, 114, 32, 119, 111, 114, 108,
        100, 33, 3,
    ];

    /// brotli(gzip(DATA)) — any valid gzip of DATA works, since the gzip
    /// layer is decoded by flate2.
    const BR_OF_GZ: [u8; 52] = [
        139, 23, 128, 31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 203, 72, 205, 201, 201, 87, 200, 64, 144,
        58, 10, 185, 165, 57, 37, 153, 186, 57, 137, 149, 169, 69, 10, 229, 249, 69, 57, 41, 138,
        0, 26, 20, 217, 187, 37, 0, 0, 0, 3,
    ];

    /// brotli(brotli(DATA))
    const BR2: [u8; 45] = [
        11, 20, 128, 11, 18, 128, 104, 101, 108, 108, 111, 32, 104, 101, 108, 108, 111, 32, 104,
        101, 108, 108, 111, 44, 32, 109, 117, 108, 116, 105, 45, 108, 97, 121, 101, 114, 32, 119,
        111, 114, 108, 100, 33, 3, 3,
    ];

    #[cfg(feature = "gzip")]
    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    // A plain single-layer brotli body decodes to the original content.
    #[test]
    fn br_single_layer() {
        init_test_log();

        let len = BR.len().to_string();
        set_handler(
            "/br_single",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            &BR,
        );

        let mut res = crate::get("https://my.test/br_single").call().unwrap();
        assert!(res.headers().get("content-encoding").is_none());
        assert!(res.body().content_length().is_none());

        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body.as_bytes(), DATA);
    }

    // `br, br` decodes two brotli layers.
    #[test]
    fn br_double_brotli() {
        init_test_log();

        let len = BR2.len().to_string();
        set_handler(
            "/br_double",
            200,
            &[("content-encoding", "br, br"), ("content-length", &len)],
            &BR2,
        );

        let mut res = crate::get("https://my.test/br_double").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body.as_bytes(), DATA);
    }

    // The HTTP body is received in full, but the brotli stream is
    // truncated. That is a decompression error, not a clean end of body.
    #[test]
    fn br_truncated_stream() {
        init_test_log();

        let truncated = &BR[..BR.len() - 3];
        let len = truncated.len().to_string();
        set_handler(
            "/br_truncated",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            truncated,
        );

        let mut res = crate::get("https://my.test/br_truncated").call().unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("brotli", _)),
            "expected brotli decompression error, got: {:?}",
            err
        );
    }

    // The truncation is reported even when the output limit is already
    // reached: the reader must not stop before checking the stream ends
    // properly. Only the final stream-end marker is missing, so the entire
    // output is produced before the truncation shows.
    #[test]
    fn br_truncated_stream_at_limit() {
        init_test_log();

        let truncated = &BR[..BR.len() - 1];
        let len = truncated.len().to_string();
        set_handler(
            "/br_truncated_limit",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            truncated,
        );

        let mut res = crate::get("https://my.test/br_truncated_limit")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(DATA.len() as u64)
            .read_to_vec()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("brotli", _)),
            "expected brotli decompression error, got: {:?}",
            err
        );
    }

    // Corrupt brotli data is a decompression error.
    #[test]
    fn br_corrupt_stream() {
        init_test_log();

        let mut corrupted = BR.to_vec();
        corrupted[1] ^= 0xFF;
        let len = corrupted.len().to_string();
        set_handler(
            "/br_corrupt",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            &corrupted,
        );

        let mut res = crate::get("https://my.test/br_corrupt").call().unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("brotli", _)),
            "expected brotli decompression error, got: {:?}",
            err
        );
    }

    // The limit counts the decompressed output bytes.
    #[test]
    fn br_limit_counts_output() {
        init_test_log();

        let len = BR.len().to_string();

        // Exactly the output size ends normally.
        set_handler(
            "/br_limit_exact",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            &BR,
        );
        let mut res = crate::get("https://my.test/br_limit_exact").call().unwrap();
        let v = res
            .body_mut()
            .with_config()
            .limit(DATA.len() as u64)
            .read_to_vec()
            .unwrap();
        assert_eq!(v, DATA);

        // One byte less exceeds the limit.
        set_handler(
            "/br_limit_exceeded",
            200,
            &[("content-encoding", "br"), ("content-length", &len)],
            &BR,
        );
        let mut res = crate::get("https://my.test/br_limit_exceeded")
            .call()
            .unwrap();
        let err = res
            .body_mut()
            .with_config()
            .limit(DATA.len() as u64 - 1)
            .read_to_vec()
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::BodyExceedsLimit(v) if v == DATA.len() as u64 - 1
        ));
    }

    // `gzip, br`: the server applied gzip first, then brotli. Decoding
    // reverses the order: brotli first, then gzip.
    #[test]
    #[cfg(feature = "gzip")]
    fn br_gzip_then_br() {
        init_test_log();

        let len = BR_OF_GZ.len().to_string();
        set_handler(
            "/br_gzip_then_br",
            200,
            &[
                ("content-encoding", "gzip, br"),
                ("content-length", &len),
            ],
            &BR_OF_GZ,
        );

        let mut res = crate::get("https://my.test/br_gzip_then_br").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body.as_bytes(), DATA);
    }

    // `br, gzip`: the server applied brotli first, then gzip. Decoding
    // reverses the order: gzip first, then brotli.
    #[test]
    #[cfg(feature = "gzip")]
    fn br_br_then_gzip() {
        init_test_log();

        let wire = gzip_compress(&BR);
        let len = wire.len().to_string();
        set_handler(
            "/br_br_then_gzip",
            200,
            &[
                ("content-encoding", "br, gzip"),
                ("content-length", &len),
            ],
            &wire,
        );

        let mut res = crate::get("https://my.test/br_br_then_gzip").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body.as_bytes(), DATA);
    }

    // The gzip and brotli codings split over two headers form the same
    // sequence as the comma-separated form.
    #[test]
    #[cfg(feature = "gzip")]
    fn br_gzip_split_headers() {
        init_test_log();

        let len = BR_OF_GZ.len().to_string();
        set_handler(
            "/br_gzip_split",
            200,
            &[
                ("content-encoding", "gzip"),
                ("content-encoding", "br"),
                ("content-length", &len),
            ],
            &BR_OF_GZ,
        );

        let mut res = crate::get("https://my.test/br_gzip_split").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body.as_bytes(), DATA);
    }

    // A corrupt inner brotli layer surfaces as a brotli decompression
    // error, not rewritten as a gzip error by the outer layer.
    #[test]
    #[cfg(feature = "gzip")]
    fn br_inner_corrupt_not_rewritten() {
        init_test_log();

        let mut inner = BR.to_vec();
        inner[1] ^= 0xFF;
        let wire = gzip_compress(&inner);
        let len = wire.len().to_string();
        set_handler(
            "/br_inner_corrupt",
            200,
            &[
                ("content-encoding", "br, gzip"),
                ("content-length", &len),
            ],
            &wire,
        );

        let mut res = crate::get("https://my.test/br_inner_corrupt").call().unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("brotli", _)),
            "expected brotli decompression error, got: {:?}",
            err
        );
    }
}
