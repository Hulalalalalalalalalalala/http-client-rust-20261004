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

#[cfg(all(test, feature = "_test", feature = "gzip"))]
mod test {
    use crate::test::init_test_log;
    use crate::transport::set_handler;

    // brotli(gzip("hello hello hello world"))
    const BR_OF_GZIP: &[u8] = &[
        0x0B, 0x10, 0x80, 0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xFF, 0xCB, 0x48,
        0xCD, 0xC9, 0xC9, 0x57, 0xC8, 0x40, 0x22, 0xCB, 0xF3, 0x8B, 0x72, 0x52, 0x00, 0x26, 0xE6,
        0x5A, 0x81, 0x17, 0x00, 0x00, 0x00, 0x03,
    ];

    // gzip(brotli("hello hello hello world"))
    const GZIP_OF_BR: &[u8] = &[
        0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xFF, 0x93, 0x16, 0x63, 0xF8, 0xD1,
        0x3B, 0x25, 0xEF, 0x9E, 0x4B, 0x68, 0xDB, 0xB4, 0x1C, 0x85, 0x7C, 0x53, 0x0F, 0x9B, 0x52,
        0x07, 0xCE, 0x4E, 0x65, 0x00, 0x09, 0x8C, 0x76, 0x95, 0x17, 0x00, 0x00, 0x00,
    ];

    // brotli(gzip("hello") with a corrupt CRC32 in the gzip trailer)
    const BR_OF_CORRUPT_GZIP: &[u8] = &[
        0x0B, 0x0C, 0x80, 0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xFF, 0xCB, 0x48,
        0xCD, 0xC9, 0xC9, 0x07, 0x00, 0x79, 0xA6, 0x10, 0x36, 0x05, 0x00, 0x00, 0x00, 0x03,
    ];

    // "gzip, br" means the server first gzipped the content and then
    // brotli-compressed the result. Decoding happens in reverse: brotli
    // first, then gzip.
    #[test]
    fn br_gzip_combo() {
        init_test_log();

        let len = BR_OF_GZIP.len().to_string();
        set_handler(
            "/br_gzip",
            200,
            &[
                ("content-encoding", "gzip, br"),
                ("content-length", &len),
            ],
            BR_OF_GZIP,
        );

        let mut res = crate::get("https://my.test/br_gzip").call().unwrap();
        assert!(res.headers().get("content-encoding").is_none());
        assert!(res.headers().get("content-length").is_none());
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello world");
    }

    // The reverse order: "br, gzip" means brotli was applied first and
    // gzip second, so gzip is decoded first. The sequence can also be
    // split over multiple headers.
    #[test]
    fn br_gzip_combo_reverse_order_multiple_headers() {
        init_test_log();

        let len = GZIP_OF_BR.len().to_string();
        set_handler(
            "/br_gzip_rev",
            200,
            &[
                ("content-encoding", "br"),
                ("content-encoding", "gzip"),
                ("content-length", &len),
            ],
            GZIP_OF_BR,
        );

        let mut res = crate::get("https://my.test/br_gzip_rev").call().unwrap();
        let body = res.body_mut().read_to_string().unwrap();
        assert_eq!(body, "hello hello hello world");
    }

    // When the inner gzip layer is corrupt, the error must stay a gzip
    // decompression error and not be rewritten as a brotli error by the
    // outer layer.
    #[test]
    fn br_inner_gzip_error_not_rewritten() {
        init_test_log();

        let len = BR_OF_CORRUPT_GZIP.len().to_string();
        set_handler(
            "/br_corrupt_inner",
            200,
            &[
                ("content-encoding", "gzip, br"),
                ("content-length", &len),
            ],
            BR_OF_CORRUPT_GZIP,
        );

        let mut res = crate::get("https://my.test/br_corrupt_inner")
            .call()
            .unwrap();
        let err = res.body_mut().read_to_vec().unwrap_err();
        assert!(
            matches!(err, crate::Error::Decompress("gzip", _)),
            "expected inner gzip decompression error, got: {:?}",
            err
        );
    }
}
