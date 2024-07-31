# up-transport-zenoh-rust

uProtocol transport implementation for Zenoh in Rust

## Build

```shell
# Check clippy
cargo clippy --all-targets
# Build
cargo build
# Run test
cargo test
# Test coverage
cargo tarpaulin -o lcov -o html --output-dir target/tarpaulin
```

## Examples

The examples of up-transport-zenoh-rust can be found under examples folder.

```shell
# Publisher
cargo run --example publisher
# Subscriber
cargo run --example subscriber
# Notifier
cargo run --example notifier
# Notification Receiver
cargo run --example notification_receiver
# RPC Server
cargo run --example rpc_server
# RPC Client
cargo run --example rpc_client
```

## Note

The implementation follows the spec defined in [up-l1/zenoh](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l1/zenoh.adoc).
