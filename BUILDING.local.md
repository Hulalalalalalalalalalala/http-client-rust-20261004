# Reproducible Ubuntu build

Use the pinned Rust toolchain. Run `cargo fetch --locked` once to populate the dependency cache before offline tests. Keep CARGO_BUILD_JOBS=2 when building alongside other workspaces.

```sh
cargo test --locked --offline --lib io_timeout_tests --features json,cookies
```

The network client tests use local or scripted transports; clear HTTP_PROXY, HTTPS_PROXY, ALL_PROXY and their lowercase equivalents in the test process to prevent proxy routing of local fixtures. The smoke command selects deterministic timeout tests; it does not claim coverage of every transport.
