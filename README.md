# up-client-zenoh-rust

Rust UPClient implementation for the Zenoh transport

# Build

```shell
# Check clippy
cargo clippy
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
The project is based on [uprotocol-rust](https://github.com/eclipse-uprotocol/uprotocol-rust/tree/uprotocol_1.5)
