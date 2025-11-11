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
use tracing::error;
use up_rust::{UCode, UStatus, UUri};
use zenoh::{Config, Session};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;

const UPROTOCOL_MAJOR_VERSION: u8 = 1;
const DEFAULT_MAX_LISTENERS: usize = 100;

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
    ) -> Result<UPTransportZenohBuilder, UStatus> {
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
            extra: BuilderState::Initial,
        })
    }

    async fn init_with_config(
        config: Config,
        local_authority: String,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus> {
        let session = zenoh::open(config).await.map_err(|err| {
            let msg = "Failed to open Zenoh session";
            error!("{msg}: {err}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;
        Ok(Self::init_with_session(
            session,
            local_authority,
            max_listeners,
        ))
    }

    fn init_with_session(
        session: Session,
        local_authority: String,
        max_listeners: usize,
    ) -> UPTransportZenoh {
        let session_to_use = Arc::new(session);
        UPTransportZenoh {
            session: session_to_use.clone(),
            subscribers: ListenerRegistry::new(session_to_use, max_listeners),
            local_authority,
        }
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

pub enum BuilderState {
    Initial,
    Config(Box<zenoh_config::Config>),
    ConfigPath(String),
    Session(Session),
}

pub struct UPTransportZenohBuilder {
    common: Box<CommonProperties>,
    extra: BuilderState,
}

impl UPTransportZenohBuilder {
    /// Sets the Zenoh configuration to use for the transport.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
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
    #[must_use]
    pub fn with_config(self, config: zenoh_config::Config) -> UPTransportZenohBuilder {
        UPTransportZenohBuilder {
            common: self.common,
            extra: BuilderState::Config(Box::new(config)),
        }
    }

    /// Sets the path to a Zenoh configuration file to use for the transport.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
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
    #[must_use]
    pub fn with_config_path(self, config_path: String) -> UPTransportZenohBuilder {
        UPTransportZenohBuilder {
            common: self.common,
            extra: BuilderState::ConfigPath(config_path),
        }
    }

    /// Sets an existing Zenoh session to use for the transport.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::UPTransportZenoh;
    /// use zenoh::config::Config;
    ///
    /// let zenoh_session = zenoh::open(Config::default()).await.expect("Failed to open Zenoh session");
    /// assert!(UPTransportZenoh::builder("local_authority")
    ///    .expect("Invalid authority name")
    ///    .with_session(zenoh_session)
    ///    .with_max_listeners(10)
    ///    .build()
    ///    .await
    ///    .is_ok());
    /// # }
    /// ```
    #[must_use]
    pub fn with_session(self, zenoh_session: Session) -> UPTransportZenohBuilder {
        UPTransportZenohBuilder {
            common: self.common,
            extra: BuilderState::Session(zenoh_session),
        }
    }

    /// Creates the Zenoh transport.
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
    ///    .with_max_listeners(10)
    ///    .build()
    ///    .await
    ///    .is_ok());
    /// # }
    /// ```
    pub async fn build(self) -> Result<UPTransportZenoh, UStatus> {
        match self.extra {
            BuilderState::Initial => {
                UPTransportZenoh::init_with_config(
                    zenoh_config::Config::default(),
                    self.common.local_authority,
                    self.common.max_listeners,
                )
                .await
            }
            BuilderState::Config(config) => {
                UPTransportZenoh::init_with_config(
                    *config,
                    self.common.local_authority,
                    self.common.max_listeners,
                )
                .await
            }
            BuilderState::ConfigPath(config_path) => {
                let config = zenoh_config::Config::from_file(config_path).map_err(|e| {
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
            BuilderState::Session(zenoh_session) => Ok(UPTransportZenoh::init_with_session(
                zenoh_session,
                self.common.local_authority,
                self.common.max_listeners,
            )),
        }
    }

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
    use test_case::test_case;

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
