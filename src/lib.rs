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
mod listener_registry;
pub mod uri_provider;
pub mod utransport;

use std::{fmt::Display, sync::Arc};

use listener_registry::ListenerRegistry;
use tracing::error;
use up_rust::{UCode, UStatus, UUri};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;
#[cfg(feature = "zenoh-unstable")]
use zenoh::internal::runtime::Runtime as ZRuntime;
use zenoh::Session;

const UPROTOCOL_MAJOR_VERSION: u8 = 1;
const DEFAULT_MAX_LISTENERS: usize = 100;

pub struct UPTransportZenoh {
    session: Arc<Session>,
    subscribers: ListenerRegistry,
    local_uri: UUri,
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
    pub fn builder<U>(local_uri: U) -> Result<UPTransportZenohBuilder, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        UPTransportZenohBuilder::new(local_uri)
    }

    fn init_with_session(
        session: Session,
        local_uri: UUri,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus> {
        // Need to make sure the authority is always non-empty
        if local_uri.has_empty_authority() {
            let msg = "Empty authority is not allowed".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        // Make sure the resource ID is always 0
        if local_uri.resource_id != 0 {
            let msg = "Resource ID must be 0".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        let session_to_use = Arc::new(session);
        // Return UPTransportZenoh
        Ok(UPTransportZenoh {
            session: session_to_use.clone(),
            subscribers: ListenerRegistry::new(session_to_use, max_listeners),
            local_uri,
        })
    }

    /// Enables a tracing formatter subscriber that is initialized from the `RUST_LOG` environment variable.
    pub fn try_init_log_from_env() {
        zenoh::init_log_from_env_or("");
    }
}

#[must_use]
pub struct UPTransportZenohBuilder {
    config: Option<zenoh_config::Config>,
    config_path: Option<String>,
    uri: UUri,
    max_listeners: usize,
    #[cfg(feature = "zenoh-unstable")]
    runtime: Option<ZRuntime>,
}

impl UPTransportZenohBuilder {
    /// Creates a new builder with the given local URI.
    fn new<U>(local_uri: U) -> Result<Self, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        let uri = local_uri.try_into().map_err(|err| {
            let msg = format!("Invalid URI: {err}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        if uri.has_empty_authority() || uri.has_wildcard_authority() {
            let msg = "URI must have non-empty, non-wildcard authority name";
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        if uri.resource_id != 0 {
            let msg = "URI must have resource ID 0".to_string();
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        Ok(UPTransportZenohBuilder {
            config: None,
            config_path: None,
            uri,
            max_listeners: DEFAULT_MAX_LISTENERS,
            #[cfg(feature = "zenoh-unstable")]
            runtime: None,
        })
    }

    /// Sets the Zenoh configuration to use for the transport.
    ///
    /// Setting the configuration using this function is mutually exclusive with `with_config_path`.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
    pub fn with_config(mut self, config: zenoh_config::Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the path to a Zenoh configuration file to use for the transport.
    ///
    /// Setting the configuration using this function is mutually exclusive with `with_config`.
    ///
    /// Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
    pub fn with_config_path(mut self, config_path: String) -> Self {
        self.config_path = Some(config_path);
        self
    }

    /// Sets the maximum number of listeners that can be registered with this transport.
    /// If not set explicitly, the default value is 100.
    pub fn with_max_listeners(mut self, max_listeners: usize) -> Self {
        self.max_listeners = max_listeners;
        self
    }

    /// Sets the Zenoh Runtime to use for the transport.
    ///
    /// Setting the runtime using this function is mutually exclusive
    /// with `with_config` and `with_config_path`.
    #[cfg(feature = "zenoh-unstable")]
    pub fn with_runtime(&mut self, runtime: ZRuntime) -> &mut Self {
        self.runtime = Some(runtime);
        self
    }

    /// Creates the transport based on the provided configuration properties.
    ///
    /// If neither `with_config` nor `with_config_path` has been invoked,
    /// the default Zenoh configuration will be used.
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
    /// assert!(UPTransportZenoh::builder("//MyAuthName/ABCD/1/0")
    ///    .expect("Invalid URI")
    ///    .with_config(zenoh_config::Config::default())
    ///    .with_max_listeners(10)
    ///    .build()
    ///    .await
    ///    .is_ok());
    /// # }
    /// ```
    pub async fn build(self) -> Result<UPTransportZenoh, UStatus> {
        #[cfg(feature = "zenoh-unstable")]
        if let Some(runtime) = self.runtime {
            if self.config.is_some() {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Zenoh Runtime is used, but Zenoh config is also provided",
                ));
            }
            if self.config_path.is_some() {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Zenoh Runtime is used, but Zenoh config path is also provided",
                ));
            }
            let session = zenoh::session::init(runtime).await.map_err(|err| {
                let msg = "Unable to open Zenoh session";
                error!("{msg}: {err}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?;

            return UPTransportZenoh::init_with_session(session, self.uri, self.max_listeners);
        }

        let config = match (self.config_path.as_ref(), self.config.as_ref()) {
            (None, None) => zenoh_config::Config::default(),
            (Some(_), Some(_)) => {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "Either Zenoh config or config file path may be provided, not both",
                ));
            }
            (Some(config_path), None) => {
                let config = zenoh_config::Config::from_file(config_path).map_err(|e| {
                    error!("Failed to load Zenoh config from file: {e}");
                    UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string())
                })?;
                config
            }
            (None, Some(config)) => config.clone(),
        };
        let session = zenoh::open(config).await.map_err(|err| {
            let msg = "Failed to open Zenoh session";
            error!("{msg}: {err}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;

        UPTransportZenoh::init_with_session(session, self.uri, self.max_listeners)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case("//vehicle1/AABB/7/0" => true; "succeeds for valid URI")]
    #[test_case(UUri::try_from_parts("vehicle1", 0xAABB, 0x07, 0x00).unwrap() => true; "succeeds for valid UUri")]
    #[test_case("This is not a UUri" => false; "fails for invalid URI")]
    #[test_case("/AABB/7/0" => false; "fails for empty UAuthority")]
    #[test_case("//vehicle1/AABB/7/1" => false; "fails for non-zero resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_up_transport_zenoh<U>(uri: U) -> bool
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        if let Ok(builder) = UPTransportZenoh::builder(uri) {
            builder.build().await.is_ok()
        } else {
            false
        }
    }
}
