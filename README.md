# librqbit-utp

A uTP library in Rust inspired by (and much copy-pasted from!) awesome [smoltcp](https://github.com/smoltcp-rs/smoltcp)

Intended for use in [rqbit](https://github.com/ikatson/rqbit) torrent client.

See example usage in [examples](https://github.com/ikatson/librqbit-utp/blob/main/examples/echo_minimal.rs).

## Run example

This will run the example server and client talking to each other

```
RUST_LOG=info cargo run --example echo
```
