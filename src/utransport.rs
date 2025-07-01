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
use crate::UPTransportZenoh;
use async_trait::async_trait;
use bytes::Bytes;
use protobuf::Message;
use std::sync::Arc;
use tracing::{debug, error};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UListener, UMessage, UPriority,
    UStatus, UTransport, UUri,
};
use zenoh::{bytes::ZBytes, qos::Priority};

// [impl->dsn~up-transport-zenoh-attributes-mapping~1]
fn uattributes_to_attachment(uattributes: &UAttributes) -> anyhow::Result<ZBytes> {
    let mut writer = ZBytes::writer();
    writer.append(ZBytes::from(
        crate::UPROTOCOL_MAJOR_VERSION.to_le_bytes().to_vec(),
    ));
    writer.append(ZBytes::from(uattributes.write_to_bytes()?));
    let zbytes = writer.finish();
    Ok(zbytes)
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
        // If uProtocol priority isn't specified, use CS1(DataLow) by default.
        // [impl->dsn~up-attributes-priority~1]
        UPriority::UPRIORITY_UNSPECIFIED => Priority::DataLow,
    }
}

// [impl->dsn~up-transport-zenoh-key-expr~1]
fn uri_to_zenoh_key(uri: &UUri, fallback_authority: &str) -> String {
    // authority_name
    let authority = if uri.authority_name.is_empty() {
        fallback_authority.to_string()
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
fn to_zenoh_key_string(src_uri: &UUri, dst_uri: Option<&UUri>, fallback_authority: &str) -> String {
    let src = uri_to_zenoh_key(src_uri, fallback_authority);
    let dst = dst_uri.map_or_else(
        || "{}/{}/{}/{}/{}".to_string(),
        |uuri| uri_to_zenoh_key(uuri, fallback_authority),
    );
    format!("up/{src}/{dst}")
}

impl UPTransportZenoh {
    async fn put_message(
        &self,
        zenoh_key: &str,
        payload: Bytes,
        attributes: &UAttributes,
    ) -> Result<(), UStatus> {
        // Transform UAttributes to user attachment in Zenoh
        let attachment = uattributes_to_attachment(attributes).map_err(|e| {
            let msg = format!("Unable to transform UAttributes to attachment: {e}");
            error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // Map the priority to Zenoh
        // [impl->dsn~up-transport-zenoh-message-priority-mapping~1]
        let priority = map_zenoh_priority(attributes.priority.enum_value_or_default());

        // Send data
        self.session
            // [impl->dsn~up-transport-zenoh-message-type-mapping~1]
            // [impl->dsn~up-transport-zenoh-payload-mapping~1]
            // [impl->dsn~supported-message-delivery-methods~1]
            .put(zenoh_key, payload)
            .priority(priority)
            // [impl->dsn~up-transport-zenoh-attributes-mapping~1]
            .attachment(attachment)
            .await
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("failed to put Zenoh message: {e}"),
                )
            })?;

        Ok(())
    }
}

#[async_trait]
impl UTransport for UPTransportZenoh {
    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        // [impl->dsn~utransport-receive-error-unimplemented~1]
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "not implemented",
        ))
    }

    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let attribs = message.attributes.as_ref().ok_or_else(|| {
            let msg = "Message has no attributes".to_string();
            debug!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // [impl->dsn~utransport-send-error-invalid-parameter~1]
        UAttributesValidators::get_validator_for_attributes(attribs)
            .validate(attribs)
            .map_err(|e| {
                let msg = e.to_string();
                debug!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;

        // Get Zenoh key
        // [impl->dsn~up-transport-zenoh-key-expr~1]
        let zenoh_key = to_zenoh_key_string(
            attribs.source.get_or_default(),
            attribs.sink.as_ref(),
            self.local_authority.as_str(),
        );

        // Get payload
        let payload = if let Some(payload) = message.payload {
            payload
        } else {
            Bytes::new()
        };

        self.put_message(&zenoh_key, payload, attribs).await
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // [impl->dsn~utransport-registerlistener-error-unimplemented~1]
        // [impl->dsn~utransport-registerlistener-error-invalid-parameter~1]
        up_rust::verify_filter_criteria(source_filter, sink_filter)?;
        let zenoh_key =
            to_zenoh_key_string(source_filter, sink_filter, self.local_authority.as_str());
        self.subscribers
            .register_subscriber(zenoh_key, listener)
            .await
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // [impl->dsn~utransport-unregisterlistener-error-unimplemented~1]
        // [impl->dsn~utransport-unregisterlistener-error-invalid-parameter~1]
        up_rust::verify_filter_criteria(source_filter, sink_filter)?;
        let zenoh_key =
            to_zenoh_key_string(source_filter, sink_filter, self.local_authority.as_str());
        self.subscribers
            .unregister(zenoh_key.as_str(), ComparableListener::new(listener))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use test_case::test_case;
    use up_rust::{MockUListener, UCode, UMessageBuilder, UMessageType, UUri};

    #[test]
    // [utest->dsn~up-transport-zenoh-attributes-mapping~1]
    fn test_uattributes_to_attachment() {
        let msg = UMessageBuilder::publish(
            UUri::try_from_parts("vehicle", 0xABCD, 1, 0xA165).expect("invalid topic"),
        )
        .build()
        .expect("failed to create message");
        let attributes = msg.attributes.as_ref().expect("message has no attributes");
        let attachment =
            uattributes_to_attachment(attributes).expect("failed to create attachment");

        assert!(attachment.len() >= 2);
        let attachment_bytes = attachment.to_bytes();
        let ver = attachment_bytes[0];
        assert!(ver == crate::UPROTOCOL_MAJOR_VERSION);
        assert!(UAttributes::parse_from_bytes(&attachment_bytes[1..])
            .is_ok_and(|deserialized_attributes| &deserialized_attributes == attributes));
    }

    #[test_case(UPriority::UPRIORITY_CS0, Priority::Background; "for CS0")]
    #[test_case(UPriority::UPRIORITY_CS1, Priority::DataLow; "for CS1")]
    #[test_case(UPriority::UPRIORITY_CS2, Priority::Data; "for CS2")]
    #[test_case(UPriority::UPRIORITY_CS3, Priority::DataHigh; "for CS3")]
    #[test_case(UPriority::UPRIORITY_CS4, Priority::InteractiveLow; "for CS4")]
    #[test_case(UPriority::UPRIORITY_CS5, Priority::InteractiveHigh; "for CS5")]
    #[test_case(UPriority::UPRIORITY_CS6, Priority::RealTime; "for CS6")]
    #[test_case(UPriority::UPRIORITY_UNSPECIFIED, Priority::DataLow; "for UNSPECIFIED")]
    // [utest->dsn~up-transport-zenoh-message-priority-mapping~1]
    fn test_map_zenoh_priority(prio: UPriority, expected_zenoh_prio: Priority) {
        assert_eq!(map_zenoh_priority(prio), expected_zenoh_prio);
    }

    #[test]
    // [utest->dsn~up-attributes-priority~1]
    fn test_map_zenoh_priority_applies_default_priority() {
        let default_zenoh_priority = map_zenoh_priority(UPriority::UPRIORITY_UNSPECIFIED);
        assert_eq!(
            default_zenoh_priority,
            map_zenoh_priority(UPriority::UPRIORITY_CS1)
        );
    }

    #[test_case("//1.2.3.4/1234/2/8001", "1.2.3.4/1234/0/2/8001"; "Standard")]
    #[test_case("/1234/2/8001", "local_authority/1234/0/2/8001"; "Standard without authority")]
    #[test_case("//1.2.3.4/12345678/2/8001", "1.2.3.4/5678/1234/2/8001"; "Standard with entity instance")]
    #[test_case("//1.2.3.4/FFFF5678/2/8001", "1.2.3.4/5678/*/2/8001"; "Standard with wildcard entity instance")]
    #[test_case("//*/FFFFFFFF/FF/FFFF", "*/*/*/*/*"; "All wildcard")]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~up-transport-zenoh-key-expr~1]
    async fn test_uri_to_zenoh_key(src_uri: &str, zenoh_key: &str) {
        let src = UUri::from_str(src_uri).expect("invalid source URI");
        let zenoh_key_string = uri_to_zenoh_key(&src, "local_authority");
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
        let src = UUri::from_str(src_uri).unwrap();
        if let Some(sink) = sink_uri {
            let sink = UUri::from_str(sink).unwrap();
            assert_eq!(
                to_zenoh_key_string(&src, Some(&sink), "device1"),
                zenoh_key.to_string()
            );
        } else {
            assert_eq!(
                to_zenoh_key_string(&src, None, "device1"),
                zenoh_key.to_string()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-receive-error-unimplemented~1]
    async fn test_receive_fails_due_to_unimplemented() {
        let up_transport_zenoh = UPTransportZenoh::builder("vehicle1")
            .expect("invalid authority name")
            .build()
            .await
            .expect("failed to create transport");
        let source_filter = UUri::try_from_parts("vehicle2", 0xAACC, 0x01, 0x1)
            .expect("failed to create source filter");
        assert!(up_transport_zenoh
            .receive(&source_filter, None)
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::UNIMPLEMENTED)));
    }

    #[test_case(
        "//vehicle1/AA/1/0",
        Some("//vehicle2/BB/1/0");
        "source and sink both having resource ID 0")]
    #[test_case(
        "//vehicle1/AA/1/CC",
        Some("//vehicle2/BB/1/1A");
        "sink is RPC but source has invalid resource ID")]
    #[test_case(
        "//vehicle1/AA/1/CC",
        None;
        "sink is empty but source has non-topic resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-registerlistener-error-invalid-parameter~1]
    // [utest->dsn~utransport-unregisterlistener-error-invalid-parameter~1]
    async fn test_register_listener_fails_for(
        source_filter_uri: &str,
        sink_filter_uri: Option<&str>,
    ) {
        let up_transport_zenoh = UPTransportZenoh::builder("vehicle1")
            .expect("invalid authority name")
            .build()
            .await
            .expect("failed to create transport");

        let source_filter =
            UUri::from_str(source_filter_uri).expect("failed to create source filter");
        let sink_filter =
            sink_filter_uri.map(|f| UUri::from_str(f).expect("failed to create sink filter"));

        assert!(up_transport_zenoh
            .register_listener(
                &source_filter,
                sink_filter.as_ref(),
                Arc::new(MockUListener::new())
            )
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
        assert!(up_transport_zenoh
            .unregister_listener(&source_filter, None, Arc::new(MockUListener::new()))
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
    }

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-registerlistener-error-resource-exhausted~1]
    // [utest->req~utransport-registerlistener-max-listeners~1]
    async fn test_register_listener_fails_if_max_listeners_has_been_reached() {
        let up_transport_zenoh = UPTransportZenoh::builder("vehicle1")
            .expect("invalid authority name")
            .with_max_listeners(1)
            .build()
            .await
            .expect("failed to create transport");

        // fill the listener registry
        let source_filter = UUri::try_from_parts("vehicle", 0xaabb, 0x07, 0xabcd).unwrap();
        let listener = Arc::new(MockUListener::new());
        assert!(up_transport_zenoh
            .register_listener(&source_filter, None, listener)
            .await
            .is_ok());

        // now it should fail to register a new listener
        let source_filter = UUri::try_from_parts("vehicle", 0xccdd, 0x01, 0xef10).unwrap();
        assert!(up_transport_zenoh
            .register_listener(&source_filter, None, Arc::new(MockUListener::new()))
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::RESOURCE_EXHAUSTED)));
    }

    #[test_case("//src/1/1/8000", None; "Listen to Publish")]
    #[test_case("//src/1/1/8000", Some("//dst/2/1/0"); "Listen to specific Notification")]
    #[test_case("//src/1/1/0", Some("//dst/2/1/1"); "Listen to specific Request")]
    #[test_case("//src/1/1/1", Some("//dst/2/1/0"); "Listen to specific Response")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/1"); "Listen to all Requests")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/0"); "Listen to all Notification and Response Messages")]
    #[test_case("//src/FFFF/FF/FFFF", Some("//*/FFFF/FF/FFFF"); "uStreamer case")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_and_unregister(source_filter_uri: &str, sink_filter_uri: Option<&str>) {
        let up_transport_zenoh = UPTransportZenoh::builder("vehicle1")
            .expect("invalid authority name")
            .build()
            .await
            .expect("failed to create transport");
        let source_filter = UUri::from_str(source_filter_uri).unwrap();
        let sink_filter = sink_filter_uri.map(|f| UUri::from_str(f).unwrap());
        let foo_listener = Arc::new(MockUListener::new());

        // initially registering the listener should work
        // [utest->dsn~utransport-registerlistener-error-unimplemented~1]
        assert!(
            up_transport_zenoh
                .register_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
                .await
                .is_ok(),
            "Failed to register listener"
        );

        // registering the same listener again should work (and have no impact)
        assert!(
            up_transport_zenoh
                .register_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
                .await
                .is_ok(),
            "Failed to repeat registration using same parameters"
        );

        // it should also be possible to unregister the listener again
        // [utest->dsn~utransport-unregisterlistener-error-unimplemented~1]
        assert!(
            up_transport_zenoh
                .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
                .await
                .is_ok(),
            "Failed to unregister listener"
        );

        // however, it should not be possible to unregister the same listener again
        // [utest->dsn~utransport-unregisterlistener-error-notfound~1]
        assert!(
            up_transport_zenoh
                .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
                .await
                .is_err_and(|err| matches!(err.get_code(), UCode::NOT_FOUND)),
            "Expected unregister to fail after already being unregistered"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-send-error-invalid-parameter~1]
    async fn test_send_fails_for_invalid_attributes() {
        let up_transport_zenoh = UPTransportZenoh::builder("vehicle2")
            .expect("invalid authority name")
            .build()
            .await
            .expect("failed to create transport");
        // invalid notification message, lacking sink address
        let message = UMessage {
            attributes: Some(UAttributes {
                type_: UMessageType::UMESSAGE_TYPE_NOTIFICATION.into(),
                source: Some(
                    UUri::try_from_parts("vehicle1", 0xAACC, 0x01, 0x1)
                        .expect("failed to create source filter"),
                )
                .into(),
                sink: None.into(),
                ..Default::default()
            })
            .into(),
            payload: None,
            ..Default::default()
        };
        assert!(up_transport_zenoh
            .send(message)
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
    }
}
