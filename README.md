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

The examples of using up-transport-zenoh-rust can be found in [up-zenoh-example-rust](https://github.com/eclipse-uprotocol/up-zenoh-example-rust).

## Note

The implementation follows the spec defined in [up-l1/zenoh](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l1/zenoh.adoc).
