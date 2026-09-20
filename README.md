# wreq-proto

[![CI](https://github.com/0x676e67/wreq-proto/actions/workflows/ci.yml/badge.svg)](https://github.com/0x676e67/wreq-proto/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/wreq-proto.svg)][license]
[![Crates.io](https://img.shields.io/crates/v/wreq-proto.svg)](https://crates.io/crates/wreq-proto)

A low-level, asynchronous HTTP client protocol implementation for [wreq].

## Features

- [HTTP/1](https://www.rfc-editor.org/rfc/rfc9112.html) and [HTTP/2](https://www.rfc-editor.org/rfc/rfc9113.html) implementations.
- Optional [HTTP/3](https://www.rfc-editor.org/rfc/rfc9114.html) over [QUIC](https://www.rfc-editor.org/rfc/rfc9000.html) implementations. 
- Streaming bodies and trailers with backpressure.
- [HTTP Upgrade](https://www.rfc-editor.org/rfc/rfc9110.html#name-upgrade) and [CONNECT](https://www.rfc-editor.org/rfc/rfc9110.html#name-connect) tunnels, including [HTTP/2 Extended CONNECT](https://www.rfc-editor.org/rfc/rfc8441.html).
- Pluggable executor, timer and transport interfaces implemented by the caller.
- Optional tracing with no default Cargo features.
- Tested against [Hyper] servers.

## Usage

Add the protocol crate to `Cargo.toml`:

```toml
[dependencies]
wreq-proto = "0.2"
```

The client APIs are organized by protocol:

```rust
use wreq_proto::conn::{http1, http2};

fn main() {
    // ...
}
```

## Documentation

- [Protocol API][protocol-api]
- [Runtime contracts](https://docs.rs/wreq-proto/latest/wreq_proto/rt/)

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE][license] or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the [Apache-2.0][license] license, shall be licensed as above, without any additional terms or conditions.

## Accolades

A hard fork of [Hyper].

[wreq]: https://github.com/0x676e67/wreq
[Hyper]: https://github.com/hyperium/hyper
[protocol-api]: https://docs.rs/wreq-proto
[license]: ./LICENSE
