# Rust based Eclipse Zenoh&trade; Transport Library for Eclipse uProtocol&trade;

This crate implements the Zenoh transport as specified in [uProtocol v1.6.0-alpha.5](https://github.com/eclipse-uprotocol/up-spec/tree/v1.6.0-alpha.5).

## Getting started

### Building the Library

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

### Running the Examples

The [examples](examples) folder contains sample code illustrating how the crate can be used for the different message exchange patterns supported by uProtocol.
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
The example configuration file is located in the [config folder](config).

## Using the Library

Most developers will want to create an instance of the *UPTransportZenoh* struct and use it with the Communication Level API and its default implementation which are provided by the *up-rust* library.

The libraries need to be added to the `[dependencies]` section of the `Cargo.toml` file:

```toml
[dependencies]
up-rust = { version = "0.6" }
up-transport-zenoh = { version = "0.7" }
```

Please refer to the [publisher](/examples/publisher.rs) and [subscriber](/examples/subscriber.rs) examples to see how to initialize and use the transport.

## Change Log

Please refer to the [Releases on GitHub](https://github.com/eclipse-uprotocol/up-transport-zenoh-rust/releases) for the change log.

[^1]: Some PC configurations cannot connect locally. Add multicast to `lo` interface using
  ` $ sudo ip link set dev lo multicast on `
