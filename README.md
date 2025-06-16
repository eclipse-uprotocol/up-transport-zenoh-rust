# up-transport-zenoh-rust

uProtocol transport implementation for Zenoh in Rust

## Build

```shell
# Check clippy
cargo clippy --all-targets
# Build
cargo build
# Optional: Build with feature `zenoh-unstable` in uStreamer use case
cargo build --features zenoh-unstable
# Run test
cargo test
# Test coverage
cargo tarpaulin -o lcov -o html --output-dir target/tarpaulin
```

## Examples

The examples of up-transport-zenoh-rust can be found under examples folder.
Assume you're using debug build.[^1]

```shell
# Publisher
./target/debug/examples/publisher
# Subscriber
./target/debug/examples/subscriber
# Notifier
./target/debug/examples/notifier
# Notification Receiver
./target/debug/examples/notification_receiver
# RPC Server
./target/debug/examples/rpc_server
# RPC Client
./target/debug/examples/rpc_client
# L2 RPC Client
./target/debug/examples/l2_rpc_client
```

For the advanced Zenoh configuration, you can either use `-h` to see more details or pass the configuration file with `-c`.
The configuration file example is under the folder `config`.

## Note

This crate implements the Zenoh transport as specified in [uProtocol v1.6.0-alpha.5](https://github.com/eclipse-uprotocol/up-spec/tree/v1.6.0-alpha.5).

## Change Log

Please refer to the [Releases on GitHub](https://github.com/eclipse-uprotocol/up-transport-zenoh-rust/releases) for the change log.

[^1]: Some PC configurations cannot connect locally. Add multicast to `lo` interface using
  ` $ sudo ip link set dev lo multicast on `
