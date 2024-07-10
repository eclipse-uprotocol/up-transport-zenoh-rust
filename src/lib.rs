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
pub mod utransport;

use bitmask_enum::bitmask;
use protobuf::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;
use up_rust::{ComparableListener, UAttributes, UCode, UListener, UPriority, UStatus, UUri};
// Re-export Zenoh config
pub use zenoh::config as zenoh_config;
use zenoh::{
    prelude::r#async::*,
    queryable::{Query, Queryable},
    runtime::Runtime as ZRuntime,
    sample::{Attachment, AttachmentBuilder},
    subscriber::Subscriber,
};

// CY_TODO: Whether to expose from up_rust or not
const _WILDCARD_AUTHORITY: &str = "*";
const WILDCARD_ENTITY_ID: u32 = 0x0000_FFFF;
const WILDCARD_ENTITY_VERSION: u32 = 0x0000_00FF;
const WILDCARD_RESOURCE_ID: u32 = 0x0000_FFFF;

const _RESOURCE_ID_RESPONSE: u32 = 0;
const _RESOURCE_ID_MIN_EVENT: u32 = 0x8000;

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
pub struct UPClientZenoh {
    session: Arc<Session>,
    // Able to unregister Subscriber
    subscriber_map: SubscriberMap,
    // Able to unregister Queryable
    queryable_map: QueryableMap,
    // Save the reqid to be able to send back response
    query_map: QueryMap,
    // Save the callback for RPC response
    rpc_callback_map: RpcCallbackMap,
    // My authority
    authority_name: String,
}

impl UPClientZenoh {
    /// Create `UPClientZenoh` by applying the Zenoh configuration, `UAuthority`.
    ///
    /// # Arguments
    ///
    /// * `config` - Zenoh configuration. You can refer to [here](https://github.com/eclipse-zenoh/zenoh/blob/0.11.0-rc.3/DEFAULT_CONFIG.json5) for more configuration details.
    /// * `authority_name` - The authority name. We need it to generate Zenoh key since authority might be omitted in `UUri`.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPClientZenoh`
    ///
    /// # Examples
    ///
    /// ```
    /// #[tokio::main]
    /// # async fn main() {
    /// use up_transport_zenoh::{zenoh_config, UPClientZenoh};
    /// let upclient = UPClientZenoh::new(zenoh_config::Config::default(), String::from("MyAuthName"))
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub async fn new(
        config: zenoh_config::Config,
        authority_name: String,
    ) -> Result<UPClientZenoh, UStatus> {
        // Create Zenoh session
        let Ok(session) = zenoh::open(config).res().await else {
            let msg = "Unable to open Zenoh session".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        // Return UPClientZenoh
        Ok(UPClientZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            rpc_callback_map: Arc::new(Mutex::new(HashMap::new())),
            authority_name,
        })
    }

    /// Create `UPClientZenoh` by applying the Zenoh Runtime and `UAuthority`. This can be used by uStreamer.
    ///
    /// # Arguments
    ///
    /// * `runtime` - Zenoh Runtime.
    /// * `authority_name` - The authority name. We need it to generate Zenoh key since authority might be omitted in `UUri`.
    ///
    /// # Errors
    /// Will return `Err` if unable to create `UPClientZenoh`
    pub async fn new_with_runtime(
        runtime: ZRuntime,
        authority_name: String,
    ) -> Result<UPClientZenoh, UStatus> {
        let Ok(session) = zenoh::init(runtime).res().await else {
            let msg = "Unable to open Zenoh session".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        };
        // Return UPClientZenoh
        Ok(UPClientZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            rpc_callback_map: Arc::new(Mutex::new(HashMap::new())),
            authority_name,
        })
    }

    fn uri_to_zenoh_key(&self, uri: &UUri) -> String {
        // authority_name
        let authority = if uri.authority_name.is_empty() {
            &self.authority_name
        } else {
            &uri.authority_name
        };
        // ue_id
        let ue_id = if uri.ue_id == WILDCARD_ENTITY_ID {
            "*".to_string()
        } else {
            format!("{:X}", uri.ue_id)
        };
        // ue_version_major
        let ue_version_major = if uri.ue_version_major == WILDCARD_ENTITY_VERSION {
            "*".to_string()
        } else {
            format!("{:X}", uri.ue_version_major)
        };
        // resource_id
        let resource_id = if uri.resource_id == WILDCARD_RESOURCE_ID {
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
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;
            if version != UATTRIBUTE_VERSION {
                let msg =
                    format!("UAttributes version is {version} (should be {UATTRIBUTE_VERSION})");
                log::error!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
            }
        } else {
            let msg = "Unable to get the UAttributes version".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        }
        let uattributes = if let Some((_, value)) = attachment_iter.next() {
            UAttributes::parse_from_bytes(value.as_slice())?
        } else {
            let msg = "Unable to get the UAttributes".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
        };
        Ok(uattributes)
    }

    /*        The table for mapping resource ID to message type
     *
     *  |   src rid   | sink rid | Publish | Notification | Request | Response |
     *  |-------------|----------|---------|--------------|---------|----------|
     *  | [8000-FFFF) |   None   |    V    |              |         |          |
     *  | [8000-FFFF) |     0    |         |      V       |         |          |
     *  |      0      | (0-8000) |         |              |    V    |          |
     *  |   (0-8000)  |     0    |         |              |         |    V     |
     *  |     FFFF    |     0    |         |      V       |         |    V     |
     *  |     FFFF    | (0-8000) |         |              |    V    |          |
     *  |      0      |   FFFF   |         |              |    V    |          |
     *  |   (0-8000)  |   FFFF   |         |              |         |    V     |
     *  | [8000-FFFF) |   FFFF   |         |      V       |         |          |
     *  |     FFFF    |   FFFF   |         |      V       |    V    |    V     |
     *
     *  Some organization:
     *  - Publish: {[8000-FFFF), None}
     *  - Notification: {[8000-FFFF), 0}, {[8000-FFFF), FFFF]}, {FFFF, 0}, {FFFF, FFFF}
     *  - Request: {0, (0-8000)}, {0, FFFF}, {FFFF, (0-8000)}, {FFFF, FFFF}
     *  - Response: {(0-8000), 0}, {(0-8000), FFFF}, (FFFF, 0), {FFFF, FFFF}
     */
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
                || (nonrpc_range.contains(&src_resource) && dst_resource == 0xFFFF)
                || (src_resource == 0xFFFF && dst_resource == 0)
                || (src_resource == 0xFFFF && dst_resource == 0xFFFF)
            {
                flag |= MessageFlag::Notification;
            }
            if (src_resource == 0 && rpc_range.contains(&dst_resource))
                || (src_resource == 0 && dst_resource == 0xFFFF)
                || (src_resource == 0xFFFF && rpc_range.contains(&dst_resource))
                || (src_resource == 0xFFFF && dst_resource == 0xFFFF)
            {
                flag |= MessageFlag::Request;
            }
            if (rpc_range.contains(&src_resource) && dst_resource == 0)
                || (rpc_range.contains(&src_resource) && dst_resource == 0xFFFF)
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
    use std::str::FromStr;

    use super::*;
    use test_case::test_case;
    use up_rust::UUri;

    // CY_TODO: Test invalid authority
    #[test_case("vehicle1".to_string(), true; "succeeds with both valid authority and entity")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_up_client_zenoh(authority: String, expected_result: bool) {
        let up_client_zenoh = UPClientZenoh::new(zenoh_config::Config::default(), authority).await;
        assert_eq!(up_client_zenoh.is_ok(), expected_result);
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
        let up_client_zenoh = UPClientZenoh::new(
            zenoh_config::Config::default(),
            String::from("192.168.1.100"),
        )
        .await
        .unwrap();
        let src = UUri::from_str(src_uri).unwrap();
        if let Some(sink) = sink_uri {
            let sink = UUri::from_str(sink).unwrap();
            assert_eq!(
                up_client_zenoh.to_zenoh_key_string(&src, Some(&sink)),
                zenoh_key.to_string()
            );
        } else {
            assert_eq!(
                up_client_zenoh.to_zenoh_key_string(&src, None),
                zenoh_key.to_string()
            );
        }
    }

    #[test_case("//192.168.1.100/10AB/3/80CD", None, Ok(MessageFlag::Publish); "Publish Message")]
    #[test_case("//192.168.1.100/10AB/3/80CD", Some("//192.168.1.101/20EF/4/0"), Ok(MessageFlag::Notification); "Notification Message")]
    #[test_case("//192.168.1.100/10AB/3/0", Some("//192.168.1.101/20EF/4/B"), Ok(MessageFlag::Request); "Request Message")]
    #[test_case("//192.168.1.101/20EF/4/B", Some("//192.168.1.100/10AB/3/0"), Ok(MessageFlag::Response); "Response Message")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//192.168.1.100/10AB/3/0"), Ok(MessageFlag::Notification | MessageFlag::Response); "Listen to Notification and Response Message")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//192.168.1.101/20EF/4/B"), Ok(MessageFlag::Request); "Listen to Request Message")]
    #[test_case("//192.168.1.100/10AB/3/0", Some("//*/FFFF/FF/FFFF"), Ok(MessageFlag::Request); "Broadcast Request Message")]
    #[test_case("//192.168.1.101/20EF/4/B", Some("//*/FFFF/FF/FFFF"), Ok(MessageFlag::Response); "Broadcast Response Message")]
    #[test_case("//192.168.1.100/10AB/3/80CD", Some("//*/FFFF/FF/FFFF"), Ok(MessageFlag::Notification); "Broadcast Notification Message")]
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
                UPClientZenoh::get_listener_message_type(&src, Some(&dst)),
                result
            );
        } else {
            assert_eq!(UPClientZenoh::get_listener_message_type(&src, None), result);
        }
    }
}
