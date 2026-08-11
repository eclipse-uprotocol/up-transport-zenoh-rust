/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

/*!
This crate provides an implementation of the Eclipse Zenoh &trade; uProtocol Transport.
The transport uses Zenoh's publish-subscribe mechanism to exchange messages. It is
designed to be used in conjunction with the [up-rust](https://crates.io/crates/up_rust)
crate, which provides the uProtocol message types and utilities.

The transport is designed to run in the context of a [tokio `Runtime`] which
needs to be configured outside of the transport according to the
processing requirements of the use case at hand. The transport does
not make any implicit assumptions about the number of threads available
and does not spawn any threads itself.

[tokio `Runtime`]: https://docs.rs/tokio/latest/tokio/runtime/index.html
*/

mod listener_registry;
pub(crate) mod utransport;

use std::sync::Arc;

use listener_registry::ListenerRegistry;
use tracing::{error, info, warn};
use up_rust::{UCode, UStatus, UUri};
use zenoh::{Config, Session};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;

const UPROTOCOL_MAJOR_VERSION: u8 = 1;
const DEFAULT_MAX_LISTENERS: usize = 100;
const EXISTING_SESSION_USAGE_MESSAGE_PREFIX: &str =
    "Using an existing Zenoh session for the transport.";
const COMPRESSION_ENABLED_FOR_UNICAST_MESSAGE_PREFIX: &str =
    "Compression for the unicast transport is enabled in the Zenoh configuration";
const COMPRESSION_ENABLED_FOR_MULTICAST_MESSAGE_PREFIX: &str =
    "Compression for the multicast transport is enabled in the Zenoh configuration";

/// An Eclipse Zenoh &trade; based uProtocol transport implementation.
///
/// The transport registers callbacks on the Zenoh runtime for listeners that
/// are being registered using `up_rust::UTransport::register_listener`.
///
/// <div class="warning">
///
/// The registered listeners are being invoked sequentially on the **same thread**
/// that the callback is being executed on. Implementers of listeners are therefore
/// **strongly advised** to move non-trivial processing logic to **another/dedicated
/// thread**, if necessary. Please refer to `subscriber` and `notification_receiver`
/// in the examples directory for how this can be done.
///
/// </div>
pub struct UPTransportZenoh {
    session: Arc<Session>,
    subscribers: ListenerRegistry,
    local_authority: String,
}

impl UPTransportZenoh {
    /// Gets a builder for creating a new Zenoh transport.
    ///
    /// # Arguments
    ///
    /// * `local_uri` - The URI identifying the (local) uEntity that the transport runs on.
    ///
    /// # Errors
    ///
    /// Returns an error if the URI contains an empty or wildcard authority name
    /// or has a non-zero resource ID.
    pub fn builder<U: Into<String>>(
        local_authority: U,
    ) -> Result<UPTransportZenohBuilder<InitialBuilderState>, UStatus> {
        let authority_name = local_authority.into();
        if authority_name.is_empty() || &authority_name == "*" {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Authority name must be non-empty and must not be the wildcard authority name",
            ));
        }

        UUri::verify_authority(&authority_name).map_err(|err| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("Invalid authority name: {err}"),
            )
        })?;

        Ok(UPTransportZenohBuilder {
            common: Box::new(CommonProperties {
                local_authority: authority_name,
                max_listeners: DEFAULT_MAX_LISTENERS,
            }),
            extra: InitialBuilderState,
        })
    }

    fn read_bool_config(config: &Config, key: &str) -> Result<bool, UStatus> {
        let Ok(value) = config.get_json(key) else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to read Zenoh config value for {key}"),
            ));
        };
        let Ok(bool_value) = value.parse::<bool>() else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Failed to parse Zenoh config value for {key} into a boolean"),
            ));
        };
        Ok(bool_value)
    }

    async fn init_with_config(
        config: Config,
        local_authority: String,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus> {
        if Self::read_bool_config(&config, "/transport/unicast/compression/enabled")?
        {
            warn!(
                "{} (\"/transport/unicast/compression/enabled\"). Note that Zenoh uses the lz4_flex crate for compression in a version that is affected by RUSTSEC-2026-0041, which may result in data leakage.",
                COMPRESSION_ENABLED_FOR_UNICAST_MESSAGE_PREFIX
            );
        }

        if Self::read_bool_config(&config, "/transport/multicast/compression/enabled")? {
            warn!(
                "{} (\"/transport/multicast/compression/enabled\"). Note that Zenoh uses the lz4_flex crate for compression in a version that is affected by RUSTSEC-2026-0041, which may result in data leakage.",
                COMPRESSION_ENABLED_FOR_MULTICAST_MESSAGE_PREFIX
            );
        }

        let session = zenoh::open(config).await.map_err(|err| {
            let msg = "Failed to open Zenoh session";
            error!("{msg}: {err}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;
        Self::init_with_session(
            session,
            local_authority,
            max_listeners,
            // no need to warn about compression here, since we already did that above
            false,
        )
    }

    fn init_with_session(
        session: Session,
        local_authority: String,
        max_listeners: usize,
        warn_on_compression_enabled: bool,
    ) -> Result<UPTransportZenoh, UStatus> {
        if warn_on_compression_enabled {
            info!(
                "{} Please be aware that Zenoh uses the lz4_flex crate in a version that is affected by RUST-SEC-2026-0041, which may result in data leakage when compression is enabled for Zenoh. It is therefore strongly recommended to disable compression in the Zenoh configuration when using this transport.",
                EXISTING_SESSION_USAGE_MESSAGE_PREFIX
            );
        }
        let session_to_use = Arc::new(session);
        Ok(UPTransportZenoh {
            session: session_to_use.clone(),
            subscribers: ListenerRegistry::new(session_to_use, max_listeners),
            local_authority,
        })
    }

    /// Enables a tracing formatter subscriber that is initialized from the `RUST_LOG` environment variable.
    pub fn try_init_log_from_env() {
        zenoh::init_log_from_env_or("");
    }
}

struct CommonProperties {
    local_authority: String,
    max_listeners: usize,
}

pub struct InitialBuilderState;
pub struct ConfigBuilderState {
    config: zenoh_config::Config,
}
pub struct ConfigPathBuilderState {
    config_path: String,
}

pub struct SessionBuilderState {
    zenoh_session: Session,
}

pub trait BuilderState {}
impl BuilderState for InitialBuilderState {}
impl BuilderState for ConfigBuilderState {}
impl BuilderState for ConfigPathBuilderState {}
impl BuilderState for SessionBuilderState {}

pub struct UPTransportZenohBuilder<S: BuilderState> {
    common: Box<CommonProperties>,
    extra: S,
}

impl UPTransportZenohBuilder<InitialBuilderState> {
    /// Sets the Zenoh configuration to use for the transport.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
    ///
    /// **Note**: Zenoh uses the lz4_flex crate in a version that is affected by
    /// [RUSTSEC-2026-0041](https://rustsec.org/advisories/RUSTSEC-2026-0041),
    /// which may result in data leakage when compression is enabled for Zenoh.
    /// It is therefore strongly recommended to disable compression in the Zenoh configuration when
    /// using this transport.
    #[must_use]
    pub fn with_config(
        self,
        config: zenoh_config::Config,
    ) -> UPTransportZenohBuilder<ConfigBuilderState> {
        UPTransportZenohBuilder {
            common: self.common,
            extra: ConfigBuilderState { config },
        }
    }

    /// Sets the path to a Zenoh configuration file to use for the transport.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
    ///
    /// **Note**: Zenoh uses the lz4_flex crate in a version that is affected by
    /// [RUSTSEC-2026-0041](https://rustsec.org/advisories/RUSTSEC-2026-0041),
    /// which may result in data leakage when compression is enabled for Zenoh.
    /// It is therefore strongly recommended to disable compression in the Zenoh configuration when
    /// using this transport.
    #[must_use]
    pub fn with_config_path(
        self,
        config_path: String,
    ) -> UPTransportZenohBuilder<ConfigPathBuilderState> {
        UPTransportZenohBuilder {
            common: self.common,
            extra: ConfigPathBuilderState { config_path },
        }
    }

    /// Sets an existing Zenoh session to use for the transport.
    ///
    /// **Note**: Zenoh uses the lz4_flex crate in a version that is affected by
    /// [RUSTSEC-2026-0041](https://rustsec.org/advisories/RUSTSEC-2026-0041),
    /// which may result in data leakage when compression is enabled for Zenoh.
    /// It is therefore strongly recommended to disable compression in the Zenoh configuration when
    /// using this transport.
    #[must_use]
    pub fn with_session(
        self,
        zenoh_session: Session,
    ) -> UPTransportZenohBuilder<SessionBuilderState> {
        UPTransportZenohBuilder {
            common: self.common,
            extra: SessionBuilderState { zenoh_session },
        }
    }
}

impl UPTransportZenohBuilder<ConfigBuilderState> {
    /// Creates the transport based on the provided configuration properties.
    ///
    /// # Returns
    ///
    /// The newly created transport instance. Note that the builder consumes itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be created.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::{zenoh_config, UPTransportZenoh};
    ///
    /// assert!(UPTransportZenoh::builder("local_authority")
    ///    .expect("Invalid authority name")
    ///    .with_config(zenoh_config::Config::default())
    ///    .with_max_listeners(10)
    ///    .build()
    ///    .await
    ///    .is_ok());
    /// # }
    /// ```
    pub async fn build(self) -> Result<UPTransportZenoh, UStatus> {
        UPTransportZenoh::init_with_config(
            self.extra.config,
            self.common.local_authority,
            self.common.max_listeners,
        )
        .await
    }
}

impl UPTransportZenohBuilder<ConfigPathBuilderState> {
    /// Creates the transport based on the provided configuration file.
    ///
    /// # Returns
    ///
    /// The newly created transport instance. Note that the builder consumes itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be created, e.g. because the configuration
    /// file cannot be read or is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::UPTransportZenoh;
    ///
    /// assert!(UPTransportZenoh::builder("local_authority")
    ///    .expect("Invalid authority name")
    ///    .with_config_path("non-existing-config.json5".to_string())
    ///    .build()
    ///    .await
    ///    .is_err_and(|e| e.get_code() == up_rust::UCode::INVALID_ARGUMENT));
    /// # }
    /// ```
    pub async fn build(self) -> Result<UPTransportZenoh, UStatus> {
        let config = zenoh_config::Config::from_file(self.extra.config_path).map_err(|e| {
            error!("Failed to load Zenoh config from file: {e}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string())
        })?;
        UPTransportZenoh::init_with_config(
            config,
            self.common.local_authority,
            self.common.max_listeners,
        )
        .await
    }
}

impl UPTransportZenohBuilder<SessionBuilderState> {
    /// Creates the transport based on the provided configuration file.
    ///
    /// # Returns
    ///
    /// The newly created transport instance. Note that the builder consumes itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be created.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::UPTransportZenoh;
    /// use zenoh::{Config, Session};
    ///
    /// let zenoh_session = zenoh::open(Config::default()).await.expect("Failed to open Zenoh session");
    /// assert!(UPTransportZenoh::builder("local_authority")
    ///    .expect("Invalid authority name")
    ///    .with_session(zenoh_session)
    ///    .with_max_listeners(10)
    ///    .build()
    ///    .is_ok());
    /// # }
    /// ```
    pub fn build(self) -> Result<UPTransportZenoh, UStatus> {
        UPTransportZenoh::init_with_session(
            self.extra.zenoh_session,
            self.common.local_authority,
            self.common.max_listeners,
            true, // warn about compression enabled in existing session
        )
    }
}

impl<S: BuilderState> UPTransportZenohBuilder<S> {
    /// Sets the maximum number of listeners that can be registered with this transport.
    /// If not set explicitly, the default value is 100.
    #[must_use]
    pub fn with_max_listeners(mut self, max_listeners: usize) -> Self {
        self.common.max_listeners = max_listeners;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};
    use test_case::test_case;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedLogBuffer {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedLogBuffer {
        fn contents(&self) -> String {
            String::from_utf8(self.bytes.lock().expect("log buffer poisoned").clone())
                .expect("log output should be valid UTF-8")
        }
    }

    struct SharedLogWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for SharedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes
                .lock()
                .expect("log buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogBuffer {
        type Writer = SharedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedLogWriter {
                bytes: Arc::clone(&self.bytes),
            }
        }
    }

    #[test_case(
        "/transport/unicast/compression/enabled",
        COMPRESSION_ENABLED_FOR_UNICAST_MESSAGE_PREFIX;
        "emits warning for unicast compression"
    )]
    #[test_case(
        "/transport/multicast/compression/enabled",
        COMPRESSION_ENABLED_FOR_MULTICAST_MESSAGE_PREFIX;
        "emits warning for multicast compression"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn test_builder_emits_warning_for_config_with_compression_enabled(
        compression_key_expr: &str,
        expected_message: &str,
    ) {
        let logs = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .finish();

        let _guard = tracing::subscriber::set_default(subscriber);

        let mut config = zenoh_config::Config::default();
        config
            .insert_json5(compression_key_expr, "true")
            .expect("Failed to set compression enabled in config");

        let result = UPTransportZenoh::builder("local_authority")
            .expect("failed to create builder")
            .with_config(config)
            .build()
            .await;

        assert!(result.is_ok(), "builder should succeed and emit a warning");

        let output = logs.contents();
        assert!(
            output.contains(expected_message),
            "captured logs:\n{output}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn test_builder_emits_info_when_using_existing_session() {
        let logs = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .without_time()
            .finish();

        let _guard = tracing::subscriber::set_default(subscriber);

        let session = zenoh::open(zenoh_config::Config::default())
            .await
            .expect("Failed to open Zenoh session");

        let result = UPTransportZenoh::builder("local_authority")
            .expect("failed to create builder")
            .with_session(session)
            .build();

        assert!(
            result.is_ok(),
            "builder should succeed and emit an info message"
        );

        let output = logs.contents();
        assert!(
            output.contains(EXISTING_SESSION_USAGE_MESSAGE_PREFIX),
            "captured logs:\n{output}"
        );
    }

    #[test_case("vehicle1" => true; "succeeds for valid authority name")]
    #[test_case("This is not an authority name" => false; "fails for invalid authority name")]
    #[test_case("" => false; "fails for empty authority name")]
    #[test_case("*" => false; "fails for wildcard authority name")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_getting_a_builder<S: Into<String>>(local_authority: S) -> bool {
        if let Ok(builder) = UPTransportZenoh::builder(local_authority) {
            builder
                .with_config(zenoh_config::Config::default())
                .build()
                .await
                .is_ok()
        } else {
            false
        }
    }
}
