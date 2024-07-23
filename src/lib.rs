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
pub mod rpc;
pub mod uri_provider;
pub mod utransport;

pub use rpc::ZenohRpcClient;

use bitmask_enum::bitmask;
use protobuf::Message;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;
use tracing::error;
use up_rust::{
    ComparableListener, LocalUriProvider, UAttributes, UCode, UListener, UPriority, UStatus, UUri,
};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;
use zenoh::{
    prelude::r#async::*,
    queryable::{Query, Queryable},
    runtime::Runtime as ZRuntime,
    sample::{Attachment, AttachmentBuilder},
    subscriber::Subscriber,
};

const UATTRIBUTE_VERSION: u8 = 1;
const THREAD_NUM: usize = 10;

// Create a separate tokio Runtime for running the callback
lazy_static::lazy_static! {
    static ref CB_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
               .worker_threads(THREAD_NUM)
               .enable_all()
               .build()
               .expect("Unable to create callback runtime");
}

#[bitmask(u8)]
enum MessageFlag {
    Publish,
    Notification,
    Request,
    Response,
}

type SubscriberMap = Arc<Mutex<HashMap<(String, ComparableListener), Subscriber<'static, ()>>>>;
type QueryableMap = Arc<Mutex<HashMap<(String, ComparableListener), Queryable<'static, ()>>>>;
type QueryMap = Arc<Mutex<HashMap<String, Query>>>;
type RpcCallbackMap = Arc<Mutex<HashMap<OwnedKeyExpr, Arc<dyn UListener>>>>;
pub struct UPTransportZenoh {
    session: Arc<Session>,
    // Able to unregister Subscriber
    subscriber_map: SubscriberMap,
    // Able to unregister Queryable
    queryable_map: QueryableMap,
    // Save the reqid to be able to send back response
    query_map: QueryMap,
    // Save the callback for RPC response
    rpc_callback_map: RpcCallbackMap,
    // URI
    uri: UUri,
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
    pub async fn new(
        config: zenoh_config::Config,
        uri: impl Into<String>,
    ) -> Result<UPTransportZenoh, UStatus> {
        // Create Zenoh session
        let Ok(session) = zenoh::open(config).res().await else {
            let msg = "Unable to open Zenoh session".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        UPTransportZenoh::init_with_session(session, uri)
    }

    /// Create `UPTransportZenoh` by applying the Zenoh Runtime and local `UUri`. This can be used by uStreamer.
    ///
    /// # Arguments
    ///
    /// * `runtime` - Zenoh Runtime.
    /// * `uri` - Local `UUri`. Note that the Authority of the `UUri` MUST be non-empty and the resource ID should be non-zero.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPTransportZenoh`
    pub async fn new_with_runtime(
        runtime: ZRuntime,
        uri: impl Into<String>,
    ) -> Result<UPTransportZenoh, UStatus> {
        let Ok(session) = zenoh::init(runtime).res().await else {
            let msg = "Unable to open Zenoh session".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        UPTransportZenoh::init_with_session(session, uri)
    }

    fn init_with_session(
        session: Session,
        uri: impl Into<String>,
    ) -> Result<UPTransportZenoh, UStatus> {
        // From String to UUri
        let uri = UUri::from_str(&uri.into()).map_err(|_| {
            let msg = "Unable to transform the uri to UUri".to_string();
            error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        // Need to make sure the authority is always non-empty
        if uri.has_empty_authority() {
            let msg = "Empty authority is not allowed".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        // Make sure the resource ID is always 0
        if uri.resource_id != 0 {
            let msg = "Resource ID should always be 0".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        }
        // Return UPTransportZenoh
        Ok(UPTransportZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            rpc_callback_map: Arc::new(Mutex::new(HashMap::new())),
            uri,
        })
    }

    /// The function to enable tracing subscriber from the environment variables `RUST_LOG`.
    pub fn try_init_log_from_env() {
        if let Ok(env_filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_thread_ids(true)
                .with_thread_names(true)
                .with_level(true)
                .with_target(true);

            let subscriber = subscriber.finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        }
    }

    fn uri_to_zenoh_key(&self, uri: &UUri) -> String {
        // authority_name
        let authority = if uri.authority_name.is_empty() {
            &self.get_authority()
        } else {
            &uri.authority_name
        };
        // ue_id
        let ue_id = if uri.has_wildcard_entity_id() {
            "*".to_string()
        } else {
            format!("{:X}", uri.ue_id)
        };
        // ue_version_major
        let ue_version_major = if uri.has_wildcard_version() {
            "*".to_string()
        } else {
            format!("{:X}", uri.ue_version_major)
        };
        // resource_id
        let resource_id = if uri.has_wildcard_resource_id() {
            "*".to_string()
        } else {
            format!("{:X}", uri.resource_id)
        };
        format!("{authority}/{ue_id}/{ue_version_major}/{resource_id}")
    }

    // The format of Zenoh key should be
    // up/[src.authority]/[src.ue_id]/[src.ue_version_major]/[src.resource_id]/[sink.authority]/[sink.ue_id]/[sink.ue_version_major]/[sink.resource_id]
    fn to_zenoh_key_string(&self, src_uri: &UUri, dst_uri: Option<&UUri>) -> String {
        let src = self.uri_to_zenoh_key(src_uri);
        let dst = if let Some(dst) = dst_uri {
            self.uri_to_zenoh_key(dst)
        } else {
            "{}/{}/{}/{}".to_string()
        };
        format!("up/{src}/{dst}")
    }

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

    fn uattributes_to_attachment(uattributes: &UAttributes) -> anyhow::Result<AttachmentBuilder> {
        let mut attachment = AttachmentBuilder::new();
        attachment.insert("", &UATTRIBUTE_VERSION.to_le_bytes());
        attachment.insert("", &uattributes.write_to_bytes()?);
        Ok(attachment)
    }

    fn attachment_to_uattributes(attachment: &Attachment) -> anyhow::Result<UAttributes> {
        let mut attachment_iter = attachment.iter();
        if let Some((_, value)) = attachment_iter.next() {
            let version = *value.as_slice().first().ok_or_else(|| {
                let msg = format!("UAttributes version is empty (should be {UATTRIBUTE_VERSION})");
                error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;
            if version != UATTRIBUTE_VERSION {
                let msg =
                    format!("UAttributes version is {version} (should be {UATTRIBUTE_VERSION})");
                error!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
            }
        } else {
            let msg = "Unable to get the UAttributes version".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        }
        let uattributes = if let Some((_, value)) = attachment_iter.next() {
            UAttributes::parse_from_bytes(value.as_slice())?
        } else {
            let msg = "Unable to get the UAttributes".to_string();
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        };
        Ok(uattributes)
    }

    // You can take a look at the table in up-spec for more detail
    // https://github.com/eclipse-uprotocol/up-spec/blob/ca8172a8cf17d70e4f095e6c0d57fe2ebc68c58d/up-l1/README.adoc#23-registerlistener
    #[allow(clippy::nonminimal_bool)] // Don't simplify the boolean expression for better understanding
    fn get_listener_message_type(
        source_uuri: &UUri,
        sink_uuri: Option<&UUri>,
    ) -> Result<MessageFlag, UStatus> {
        let mut flag = MessageFlag::none();
        let rpc_range = 1..0x7FFF_u32;
        let nonrpc_range = 0x8000..0xFFFE_u32;

        let src_resource = source_uuri.resource_id;
        // Notification / Request / Response
        if let Some(dst_uuri) = sink_uuri {
            let dst_resource = dst_uuri.resource_id;

            if (nonrpc_range.contains(&src_resource) && dst_resource == 0)
                || (src_resource == 0xFFFF && dst_resource == 0)
                || (src_resource == 0xFFFF && dst_resource == 0xFFFF)
            {
                flag |= MessageFlag::Notification;
            }
            if (src_resource == 0 && rpc_range.contains(&dst_resource))
                || (src_resource == 0xFFFF && dst_resource == 0xFFFF)
            {
                flag |= MessageFlag::Request;
            }
            if (rpc_range.contains(&src_resource) && dst_resource == 0)
                || (src_resource == 0xFFFF && dst_resource == 0)
                || (src_resource == 0xFFFF && dst_resource == 0xFFFF)
            {
                flag |= MessageFlag::Response;
            }
        } else if nonrpc_range.contains(&src_resource) || src_resource == 0xFFFF {
            flag |= MessageFlag::Publish;
        }
        if flag.is_none() {
            Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Wrong combination of source UUri and sink UUri",
            ))
        } else {
            Ok(flag)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case("//vehicle1/AABB/7/0", true; "succeeds with valid UUri")]
    #[test_case("This is not UUri", false; "fails with invalid UUri")]
    #[test_case("/AABB/7/0", false; "fails with empty UAuthority")]
    #[test_case("//vehicle1/AABB/7/1", false; "fails with non-zero resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_up_transport_zenoh(uri: &str, expected_result: bool) {
        let up_transport_zenoh = UPTransportZenoh::new(zenoh_config::Config::default(), uri).await;
        assert_eq!(up_transport_zenoh.is_ok(), expected_result);
    }

    // Mapping with the examples in Zenoh spec
    #[test_case("/10AB/3/80CD", None, "up/192.168.1.100/10AB/3/80CD/{}/{}/{}/{}"; "Send Publish")]
    #[test_case("//192.168.1.100/10AB/3/80CD", None, "up/192.168.1.100/10AB/3/80CD/{}/{}/{}/{}"; "Subscribe messages")]
    #[test_case("//192.168.1.100/10AB/3/80CD", Some("//192.168.1.101/20EF/4/0"), "up/192.168.1.100/10AB/3/80CD/192.168.1.101/20EF/4/0"; "Send Notification")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//192.168.1.101/20EF/4/0"), "up/*/*/*/*/192.168.1.101/20EF/4/0"; "Receive all Notifications")]
    #[test_case("//my-host1/10AB/3/0", Some("//my-host2/20EF/4/B"), "up/my-host1/10AB/3/0/my-host2/20EF/4/B"; "Send Request")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//my-host2/20EF/4/B"), "up/*/*/*/*/my-host2/20EF/4/B"; "Receive all Requests")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//[::1]/FFFF/FF/FFFF"), "up/*/*/*/*/[::1]/*/*/*"; "Receive all messages to a device")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_to_zenoh_key_string(src_uri: &str, sink_uri: Option<&str>, zenoh_key: &str) {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh_config::Config::default(), "//192.168.1.100/10AB/3/0")
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

    #[test_case("//192.168.1.100/10AB/3/80CD", None, Ok(MessageFlag::Publish); "Publish Message")]
    #[test_case("//192.168.1.100/10AB/3/80CD", Some("//192.168.1.101/20EF/4/0"), Ok(MessageFlag::Notification); "Notification Message")]
    #[test_case("//192.168.1.100/10AB/3/0", Some("//192.168.1.101/20EF/4/B"), Ok(MessageFlag::Request); "Request Message")]
    #[test_case("//192.168.1.101/20EF/4/B", Some("//192.168.1.100/10AB/3/0"), Ok(MessageFlag::Response); "Response Message")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//192.168.1.100/10AB/3/0"), Ok(MessageFlag::Notification | MessageFlag::Response); "Listen to Notification and Response Message")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//192.168.1.101/20EF/4/B"), Err(UStatus::fail_with_code(UCode::INTERNAL, "Wrong combination of source UUri and sink UUri")); "Impossible scenario 1")]
    #[test_case("//192.168.1.100/10AB/3/0", Some("//*/FFFF/FF/FFFF"), Err(UStatus::fail_with_code(UCode::INTERNAL, "Wrong combination of source UUri and sink UUri")); "Impossible scenario 2")]
    #[test_case("//192.168.1.101/20EF/4/B", Some("//*/FFFF/FF/FFFF"), Err(UStatus::fail_with_code(UCode::INTERNAL, "Wrong combination of source UUri and sink UUri")); "Impossible scenario 3")]
    #[test_case("//192.168.1.100/10AB/3/80CD", Some("//*/FFFF/FF/FFFF"), Err(UStatus::fail_with_code(UCode::INTERNAL, "Wrong combination of source UUri and sink UUri")); "Impossible scenario 4")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//[::1]/FFFF/FF/FFFF"), Ok(MessageFlag::Notification | MessageFlag::Request | MessageFlag::Response); "All messages to a device")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_listener_message_type(
        src_uri: &str,
        sink_uri: Option<&str>,
        result: Result<MessageFlag, UStatus>,
    ) {
        let src = UUri::from_str(src_uri).unwrap();
        if let Some(uri) = sink_uri {
            let dst = UUri::from_str(uri).unwrap();
            assert_eq!(
                UPTransportZenoh::get_listener_message_type(&src, Some(&dst)),
                result
            );
        } else {
            assert_eq!(
                UPTransportZenoh::get_listener_message_type(&src, None),
                result
            );
        }
    }
}
