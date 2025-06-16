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
pub mod uri_provider;
pub mod utransport;

use protobuf::Message;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, LazyLock, Mutex},
};
use tokio::runtime::Runtime;
use tracing::error;
use up_rust::{ComparableListener, LocalUriProvider, UAttributes, UCode, UPriority, UStatus, UUri};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;
#[cfg(feature = "zenoh-unstable")]
use zenoh::internal::runtime::Runtime as ZRuntime;
use zenoh::{bytes::ZBytes, pubsub::Subscriber, qos::Priority, Session};

const UPROTOCOL_MAJOR_VERSION: u8 = 1;
const THREAD_NUM: usize = 10;

// Create a separate tokio Runtime for running the callback
static CB_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(THREAD_NUM)
        .enable_all()
        .build()
        .expect("Unable to create callback runtime")
});

type SubscriberMap = Arc<Mutex<HashMap<(String, ComparableListener), Subscriber<()>>>>;
pub struct UPTransportZenoh {
    session: Arc<Session>,
    // Able to unregister Subscriber
    subscriber_map: SubscriberMap,
    // URI
    local_uri: UUri,
}

impl UPTransportZenoh {
    /// Create `UPTransportZenoh` by applying the Zenoh configuration, local `UUri`.
    ///
    /// # Arguments
    ///
    /// * `config` - Zenoh configuration. You can refer to [here](https://github.com/eclipse-zenoh/zenoh/blob/0.11.0/DEFAULT_CONFIG.json5) for more configuration details.
    /// * `uri` - Local `UUri`. Note that the Authority of the `UUri` MUST be non-empty and the resource ID should be non-zero.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPTransportZenoh`
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::{zenoh_config, UPTransportZenoh};
    /// let uptransport =
    ///     UPTransportZenoh::new(zenoh_config::Config::default(), "//MyAuthName/ABCD/1/0")
    ///         .await
    ///         .unwrap();
    /// # }
    /// ```
    pub async fn new<U>(config: zenoh_config::Config, uri: U) -> Result<UPTransportZenoh, UStatus>
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
        UPTransportZenoh::init_with_session(session, uri)
    }

    /// Create `UPTransportZenoh` by applying the Zenoh Runtime and local `UUri`. This can be used by uStreamer.
    /// You need to enable feature `zenoh-unstable` to support this function.
    ///
    /// # Arguments
    ///
    /// * `runtime` - Zenoh Runtime.
    /// * `uri` - Local `UUri`. Note that the Authority of the `UUri` MUST be non-empty and the resource ID should be non-zero.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPTransportZenoh`
    #[cfg(feature = "zenoh-unstable")]
    pub async fn new_with_runtime<U>(runtime: ZRuntime, uri: U) -> Result<UPTransportZenoh, UStatus>
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        let Ok(session) = zenoh::session::init(runtime).await else {
            let msg = "Unable to open Zenoh session".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        UPTransportZenoh::init_with_session(session, uri)
    }

    fn init_with_session<U>(session: Session, uri: U) -> Result<UPTransportZenoh, UStatus>
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
            let msg = "Resource ID should always be 0".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        // Return UPTransportZenoh
        Ok(UPTransportZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            local_uri,
        })
    }

    /// Enables a tracing formatter subscriber that is initialized from the `RUST_LOG` environment variable.
    pub fn try_init_log_from_env() {
        zenoh::init_log_from_env_or("");
    }

    // [impl->dsn~up-transport-zenoh-key-expr~1]
    fn uri_to_zenoh_key(&self, uri: &UUri) -> String {
        // authority_name
        let authority = if uri.authority_name.is_empty() {
            self.get_authority()
        } else {
            uri.authority_name()
        };
        // ue_type
        let ue_type = if uri.has_wildcard_entity_type() {
            "*".to_string()
        } else {
            format!("{:X}", uri.uentity_type_id())
        };
        // ue_instance
        let ue_instance = if uri.has_wildcard_entity_instance() {
            "*".to_string()
        } else {
            format!("{:X}", uri.uentity_instance_id())
        };
        // ue_version_major
        let ue_version_major = if uri.has_wildcard_version() {
            "*".to_string()
        } else {
            format!("{:X}", uri.uentity_major_version())
        };
        // resource_id
        let resource_id = if uri.has_wildcard_resource_id() {
            "*".to_string()
        } else {
            format!("{:X}", uri.resource_id())
        };
        format!("{authority}/{ue_type}/{ue_instance}/{ue_version_major}/{resource_id}")
    }

    // The format of Zenoh key should be
    // up/[src.authority]/[src.ue_type]/[src.ue_instance]/[src.ue_version_major]/[src.resource_id]/[sink.authority]/[sink.ue_type]/[sink.ue_instance]/[sink.ue_version_major]/[sink.resource_id]
    // [impl->dsn~up-transport-zenoh-key-expr~1]
    fn to_zenoh_key_string(&self, src_uri: &UUri, dst_uri: Option<&UUri>) -> String {
        let src = self.uri_to_zenoh_key(src_uri);
        let dst = if let Some(dst) = dst_uri {
            self.uri_to_zenoh_key(dst)
        } else {
            "{}/{}/{}/{}/{}".to_string()
        };
        format!("up/{src}/{dst}")
    }

    // [impl->dsn~up-transport-zenoh-message-priority-mapping~1]
    #[allow(clippy::match_same_arms)]
    fn map_zenoh_priority(upriority: UPriority) -> Priority {
        match upriority {
            UPriority::UPRIORITY_CS0 => Priority::Background,
            UPriority::UPRIORITY_CS1 => Priority::DataLow,
            UPriority::UPRIORITY_CS2 => Priority::Data,
            UPriority::UPRIORITY_CS3 => Priority::DataHigh,
            UPriority::UPRIORITY_CS4 => Priority::InteractiveLow,
            UPriority::UPRIORITY_CS5 => Priority::InteractiveHigh,
            UPriority::UPRIORITY_CS6 => Priority::RealTime,
            // If uProtocol prioritiy isn't specified, use CS1(DataLow) by default.
            // https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/basics/qos.adoc
            UPriority::UPRIORITY_UNSPECIFIED => Priority::DataLow,
        }
    }

    // [impl->dsn~up-transport-zenoh-attributes-mapping~1]
    fn uattributes_to_attachment(uattributes: &UAttributes) -> anyhow::Result<ZBytes> {
        let mut writer = ZBytes::writer();
        writer.append(ZBytes::from(UPROTOCOL_MAJOR_VERSION.to_le_bytes().to_vec()));
        writer.append(ZBytes::from(uattributes.write_to_bytes()?));
        let zbytes = writer.finish();
        Ok(zbytes)
    }

    fn attachment_to_uattributes(attachment: &ZBytes) -> anyhow::Result<UAttributes> {
        if attachment.len() < 2 {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "message has no/invalid attachment",
            )
            .into());
        }

        let attachment_bytes = attachment.to_bytes();
        let ver = attachment_bytes[0];
        if ver != UPROTOCOL_MAJOR_VERSION {
            let msg = format!(
                "Expected UAttributes version {UPROTOCOL_MAJOR_VERSION} but found version {ver}"
            );
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        }
        // Get the attributes
        let uattributes = UAttributes::parse_from_bytes(&attachment_bytes[1..])?;
        Ok(uattributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case("//vehicle1/AABB/7/0", true; "succeeds for valid URI")]
    #[test_case(UUri::try_from_parts("vehicle1", 0xAABB, 0x07, 0x00).unwrap(), true; "succeeds for valid UUri")]
    #[test_case("This is not UUri", false; "fails for invalid URI")]
    #[test_case("/AABB/7/0", false; "fails for empty UAuthority")]
    #[test_case("//vehicle1/AABB/7/1", false; "fails for non-zero resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_up_transport_zenoh<U>(uri: U, expected_result: bool)
    where
        U: TryInto<UUri>,
        U::Error: Display,
    {
        let up_transport_zenoh = UPTransportZenoh::new(zenoh_config::Config::default(), uri).await;
        assert_eq!(up_transport_zenoh.is_ok(), expected_result);
    }

    #[test_case("//1.2.3.4/1234/2/8001", "1.2.3.4/1234/0/2/8001"; "Standard")]
    #[test_case("/1234/2/8001", "local_authority/1234/0/2/8001"; "Standard without authority")]
    #[test_case("//1.2.3.4/12345678/2/8001", "1.2.3.4/5678/1234/2/8001"; "Standard with entity instance")]
    #[test_case("//1.2.3.4/FFFF5678/2/8001", "1.2.3.4/5678/*/2/8001"; "Standard with wildcard entity instance")]
    #[test_case("//*/FFFFFFFF/FF/FFFF", "*/*/*/*/*"; "All wildcard")]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~up-transport-zenoh-key-expr~1]
    async fn uri_to_zenoh_key(src_uri: &str, zenoh_key: &str) {
        let up_transport_zenoh = UPTransportZenoh::new(
            zenoh_config::Config::default(),
            "//local_authority/1234/5/0",
        )
        .await
        .unwrap();
        let src = UUri::from_str(src_uri).unwrap();
        let zenoh_key_string = up_transport_zenoh.uri_to_zenoh_key(&src);
        assert_eq!(zenoh_key_string, zenoh_key);
    }

    // Mapping with the examples in Zenoh spec
    #[test_case("/10AB/3/80CD", None, "up/device1/10AB/0/3/80CD/{}/{}/{}/{}/{}"; "Send Publish")]
    #[test_case("up://device1/10AB/3/80CD", Some("//device2/300EF/4/0"), "up/device1/10AB/0/3/80CD/device2/EF/3/4/0"; "Send Notification")]
    #[test_case("/403AB/3/0", Some("//device2/CD/4/B"), "up/device1/3AB/4/3/0/device2/CD/0/4/B"; "Send RPC Request")]
    #[test_case("up://device2/10AB/3/80CD", None, "up/device2/10AB/0/3/80CD/{}/{}/{}/{}/{}"; "Subscribe messages")]
    #[test_case("//*/FFFFFFFF/FF/FFFF", Some("/300EF/4/0"), "up/*/*/*/*/*/device1/EF/3/4/0"; "Receive all Notifications")]
    #[test_case("up://*/FFFF05A1/2/FFFF", Some("up://device1/300EF/4/B18"), "up/*/5A1/*/2/*/device1/EF/3/4/B18"; "Receive all RPC Requests from a specific entity type")]
    #[test_case("//*/FFFFFFFF/FF/FFFF", Some("//device1/FFFFFFFF/FF/FFFF"), "up/*/*/*/*/*/device1/*/*/*/*"; "Receive all messages to the local authority")]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~up-transport-zenoh-key-expr~1]
    async fn test_to_zenoh_key_string(src_uri: &str, sink_uri: Option<&str>, zenoh_key: &str) {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh_config::Config::default(), "//device1/10AB/3/0")
                .await
                .unwrap();
        let src = UUri::from_str(src_uri).unwrap();
        if let Some(sink) = sink_uri {
            let sink = UUri::from_str(sink).unwrap();
            assert_eq!(
                up_transport_zenoh.to_zenoh_key_string(&src, Some(&sink)),
                zenoh_key.to_string()
            );
        } else {
            assert_eq!(
                up_transport_zenoh.to_zenoh_key_string(&src, None),
                zenoh_key.to_string()
            );
        }
    }
}
