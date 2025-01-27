# **WORK IN PROGRESS** librqbit-utp

An exploratory attempt to build a uTP library in Rust modeled (and much copy-pasted from!) after awesome [smoltcp](https://github.com/smoltcp-rs/smoltcp)

See example usage in [examples](https://github.com/ikatson/librqbit-utp/blob/main/examples/echo.rs).

## Run example

This will run the example server and client talking to each other

```
RUST_LOG=info cargo run --example echo
```
