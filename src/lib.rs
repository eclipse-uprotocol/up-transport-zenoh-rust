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

pub struct UPTransportZenoh {
    session: Arc<Session>,
    subscribers: ListenerRegistry,
    local_uri: UUri,
}

impl UPTransportZenoh {
    /// Create `UPTransportZenoh` by applying the Zenoh configuration, local `UUri`.
    ///
    /// # Arguments
    ///
    /// * `config` - The Zenoh configuration to use.
    ///   Please refer to the [Zenoh documentation](https://zenoh.io/docs/manual/configuration/) for details.
    /// * `uri` - The uProtocol URI identifying the local uEntity. Its authority MUST be non-empty and the
    ///   resource ID must be 0.
    /// * `max_listeners` - The maximum number of listeners that can be registered with this transport.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport cannot be created. Returns an error with `UCode::ILLEGAL_ARGUMENT`,
    /// if the given URI has an empty authority or a non-zero resource ID.
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::{zenoh_config, UPTransportZenoh};
    ///
    /// assert!(
    ///     UPTransportZenoh::new(zenoh_config::Config::default(), "//MyAuthName/ABCD/1/0", 10)
    ///         .await
    ///         .is_ok());
    /// # }
    /// ```
    pub async fn new<U>(
        config: zenoh_config::Config,
        uri: U,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        // Create Zenoh session
        let Ok(session) = zenoh::open(config).await else {
            let msg = "Unable to open Zenoh session".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        UPTransportZenoh::init_with_session(session, uri, max_listeners)
    }

    /// Create `UPTransportZenoh` by applying the Zenoh Runtime and local `UUri`. This can be used by uStreamer.
    /// You need to enable feature `zenoh-unstable` to support this function.
    ///
    /// # Arguments
    ///
    /// * `runtime` - Zenoh Runtime.
    /// * `uri` - The uProtocol URI identifying the local uEntity. Its authority MUST be non-empty and the
    ///   resource ID must be 0.
    /// * `max_listeners` - The maximum number of listeners that can be registered with this transport.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPTransportZenoh`
    #[cfg(feature = "zenoh-unstable")]
    pub async fn new_with_runtime<U>(
        runtime: ZRuntime,
        uri: U,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        let Ok(session) = zenoh::session::init(runtime).await else {
            let msg = "Unable to open Zenoh session".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        UPTransportZenoh::init_with_session(session, uri, max_listeners)
    }

    fn init_with_session<U>(
        session: Session,
        uri: U,
        max_listeners: usize,
    ) -> Result<UPTransportZenoh, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        // From String to UUri
        let local_uri: UUri = uri.try_into().map_err(|e| {
            let msg = e.to_string();
            error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case("//vehicle1/AABB/7/0" => true; "succeeds for valid URI")]
    #[test_case(UUri::try_from_parts("vehicle1", 0xAABB, 0x07, 0x00).unwrap() => true; "succeeds for valid UUri")]
    #[test_case("This is not UUri" => false; "fails for invalid URI")]
    #[test_case("/AABB/7/0" => false; "fails for empty UAuthority")]
    #[test_case("//vehicle1/AABB/7/1" => false; "fails for non-zero resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_up_transport_zenoh<U>(uri: U) -> bool
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        UPTransportZenoh::new(zenoh_config::Config::default(), uri, 10)
            .await
            .is_ok()
    }
}
