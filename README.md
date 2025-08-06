# Rust based Eclipse Zenoh&trade; Transport Library for Eclipse uProtocol&trade;

This crate implements the Zenoh transport as specified in [uProtocol v1.6.0-alpha.6](https://github.com/eclipse-uprotocol/up-spec/blob/v1.6.0-alpha.6).

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
up-rust = { version = "0.7" }
up-transport-zenoh = { version = "0.8" }
```

Please refer to the [publisher](/examples/publisher.rs) and [subscriber](/examples/subscriber.rs) examples to see how to initialize and use the transport.

### Supported Service Classes
`uman~supported-service-classes~1`

The Zenoh transport supports all service classes defined by uProtocol and maps them to corresponding Zenoh message priority levels.

Covers:
- `req~utransport-send-qos-mapping~1`

### Supported Message Delivery Methods
`uman~supported-message-delivery-methods~1`

The transport provided by this crate supports the [push delivery method](https://github.com/eclipse-uprotocol/up-spec/blob/v1.6.0-alpha.6/up-l1/README.adoc#5-message-delivery) only.
The `UPTransportZenoh::receive` function therefore always returns `UCode::UNIMPLEMENTED`.

Covers:
- `req~utransport-delivery-methods~1`

### Authentication & Authorization
`uman~auth-configuration~1`

The transport provided by this crate can be configured with credentials that the transport will provide to the Zenoh router during connection establishment. A [_username_ and _password_](https://zenoh.io/docs/manual/user-password/) can be specified in the Zenoh config file that is passed into the `UPTransportZenohBuilder::with_config_file` function.

Access to resources can be configured in the Zenoh (peer's or router's) config file by means of [Access Control Lists](https://zenoh.io/docs/manual/access-control/).
The [authorization integration tests](./tests/authorization.rs) illustrate, how ACLs can be used to restrict a client's authority to put and subscribe to messages using corresponding rule sets.

Covers:
- `req~utransport-send-prevent-address-spoofing~1`
- `req~utransport-registerlistener-prevent-unauthorized-access~1`

### Maximum number of listeners
`uman~max-listeners-configuration~1`

The transport provided by this crate supports setting an upper limit to the number of (filter pattern, listener) tuples that can be registered by means of the `UPTransportZenohBuilder::with_max_listeners` function. Please refer to the [API Documentation](https://docs.rs/up-transport-zenoh/) for details.

Covers:
- `req~utransport-registerlistener-max-listeners~1`

## Design

### Message Delivery
`dsn~supported-message-delivery-methods~1`

All messages are being received by means of registering callbacks for relevant Zenoh key patterns and delivering the messages to listeners that have been registered via `UPTransportZenoh::register_listener`.
The callbacks dispatch all incoming messages to the registered listeners on the _same_ thread that the Zenoh runtime runs on.

Rationale:
The Zenoh protocol does not provide means to poll other nodes for messages but only supports the push model by means of clients subscribing to key patterns.

The transport requires a tokio runtime to execute but does not make any implicit assumption regarding the availability and size of thread pools. This provides for flexibility regarding the environment that the transport can be deployed to but also requires application developers to take care when implementing message listeners, making sure to not block the transport's message callback when processing a dispatched message.

Covers:
- `req~utransport-delivery-methods~1`

Needs: impl, itest

### Authorization
`dsn~utransport-authorization~1`

In general, uProtocol entities are only allowed to send messages on their own behalf. Certain specific uEntities acting as a uProtocol Streamer also need to send messages _on behalf of_ other uEntities in order to fulfill their original purpose of routing messages hence and forth between different transports. Making these authoritzation decisions requires the (proven) establishment of an _identity_ and its _authorities_.

The Zenoh transport delegates all authorization decisions to the Zenoh router (or peer) that the transport is configured to connect to. For this purpose, the transport supports configuration of credentials which are being used during connection establishment. The Zenoh router uses the provided credentials to establish the client's identity and its associated authorities. Whenever the uEntity sends a message via the router or registers a subscriber for a key pattern, the router verifies that the client is authorized to publish using the key or receive messages matching the key pattern.

Rationale:
The Zenoh transport is implemented as a library that is linked to the (custom) code that implements a uEntity's functionality. It is therefore not feasible to perform the authentication and authoritzation within the transport library code, because uEntities can not be forced to actually utilize one of uProtocol's transport libraries but may instead chooose to implement the binding to the transport protocol themselves.

Covers:
- `req~utransport-send-prevent-address-spoofing~1`
- `req~utransport-registerlistener-prevent-unauthorized-access~1`

Needs: itest

## Change Log

Please refer to the [Releases on GitHub](https://github.com/eclipse-uprotocol/up-transport-zenoh-rust/releases) for the change log.

[^1]: Some PC configurations cannot connect locally. Add multicast to `lo` interface using
  ` $ sudo ip link set dev lo multicast on `
