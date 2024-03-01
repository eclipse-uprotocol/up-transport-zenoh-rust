# up-client-zenoh-rust

Rust UPClient implementation for the Zenoh transport

# Build

```shell
# Check clippy
cargo clippy --all-targets
# Build
cargo build
# Run test
cargo test
```

# Examples

```shell
# Publisher
cargo run --example publisher
# Subscriber
cargo run --example subscriber
# RPC Server
cargo run --example rpc_server
# RPC Client
cargo run --example rpc_client
```

# Note

The implementation follows the spec defined in [up-l1/zenoh](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l1/zenoh.adoc).
