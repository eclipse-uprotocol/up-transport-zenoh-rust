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
mod test_lib;

use async_trait::async_trait;
use std::{str::FromStr, sync::Arc};
use tokio::{sync::Notify, time::Duration};
use tracing::info;
use up_rust::{
    MockUListener, UCode, UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus,
    UTransport, UUri, UUID,
};

const MESSAGE_DATA: &str = "Hello World!";

struct MessageHandler(UMessage, Arc<Notify>);

#[async_trait]
impl UListener for MessageHandler {
    async fn on_receive(&self, msg: UMessage) {
        // [utest->dsn~up-transport-zenoh-payload-mapping~1]
        // [utest->dsn~up-transport-zenoh-attributes-mapping~1]
        assert_eq!(self.0, msg);
        self.1.notify_one();
    }
}

// [utest->dsn~up-transport-zenoh-message-type-mapping~1]
async fn register_listener_and_send(
    authority: &str,
    umessage: UMessage,
    source_filter: &UUri,
    sink_filter: Option<&UUri>,
) -> Result<(), Box<dyn std::error::Error>> {
    let local_uuri = UUri::try_from_parts(authority, 0xABC, 1, 0)?;
    let transport = test_lib::create_up_transport_zenoh(local_uuri.to_uri(false).as_str()).await?;

    let notify = Arc::new(Notify::new());
    let listener = Arc::new(MessageHandler(umessage.clone(), notify.clone()));
    // [itest->dsn~supported-message-delivery-methods~1]
    transport
        .register_listener(source_filter, sink_filter, listener.clone())
        .await?;

    // Send UMessage
    info!(
        "sending message: [id: {}, type: {}]",
        umessage.id_unchecked().to_hyphenated_string(),
        umessage.type_unchecked().to_cloudevent_type()
    );
    transport.send(umessage).await?;
    Ok(
        tokio::time::timeout(Duration::from_secs(3), notify.notified())
            .await
            .map_err(|_| {
                UStatus::fail_with_code(UCode::DEADLINE_EXCEEDED, "did not receive message in time")
            })?,
    )
}

#[test_case::test_case("vehicle1", 12_000, "//vehicle1/10A10B/1/CA5D", "//vehicle1/10A10B/1/CA5D"; "specific source filter")]
// [utest->dsn~up-attributes-ttl~1]
#[test_case::test_case("vehicle1", 0, "/D5A/3/9999", "//vehicle1/D5A/3/FFFF"; "source filter with wildcard resource ID")]
#[test_case::test_case("vehicle1", 12_000, "//vehicle1/70222/2/8001", "//*/FFFF0222/2/8001"; "source filter with wildcard authority and service instance ID")]
#[tokio::test(flavor = "multi_thread")]
async fn test_publish_message_gets_delivered_to_listener(
    authority: &str,
    ttl: u32,
    topic_uri: &str,
    source_filter_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    test_lib::before_test();

    let topic = UUri::from_str(topic_uri)?;
    let source_filter = UUri::from_str(source_filter_uri)?;
    let umessage = UMessageBuilder::publish(topic.clone())
        .with_priority(up_rust::UPriority::UPRIORITY_CS5)
        .with_traceparent("traceparent")
        .with_ttl(ttl)
        .build_with_payload(MESSAGE_DATA, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;

    // [utest->dsn~utransport-registerlistener-start-invoking-listeners~1]
    register_listener_and_send(authority, umessage, &source_filter, None).await
}

#[test_case::test_case(
    "vehicle1",
    12_000,
    "//vehicle1/10A10B/1/CA5D", "//vehicle1/55A1/2/0",
    "//vehicle1/10A10B/1/CA5D", "//vehicle1/FFFFFFFF/FF/0";
    "specific source filter")]
// [utest->dsn~up-attributes-ttl~1]
#[test_case::test_case(
    "vehicle1",
     0,
    "/D5A/3/9999", "//vehicle1/355A1/2/0",
    "//vehicle1/D5A/3/FFFF", "//vehicle1/FFFF55A1/FF/0";
    "source filter with wildcard resource ID")]
#[test_case::test_case(
    "vehicle1",
    12_000,
    "//vehicle1/70222/2/87A", "//vehicle1/55A1/2/0",
    "//vehicle1/FFFFFFFF/FF/FFFF", "//*/FFFFFFFF/FF/FFFF";
    "for all messages from specific authority")]
// [utest->dsn~up-attributes-ttl~1]
#[test_case::test_case(
    "vehicle1",
    0,
    "//vehicle1/70222/2/87A", "//vehicle1/55A1/2/0",
    "//*/FFFF0222/FF/FFFF", "//vehicle1/55A1/2/0";
    "source filter with wildcard authority and service instance ID")]
#[tokio::test(flavor = "multi_thread")]
async fn test_notification_message_gets_delivered_to_listener(
    authority: &str,
    ttl: u32,
    source_uri: &str,
    sink_uri: &str,
    source_filter_uri: &str,
    sink_filter_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    test_lib::before_test();

    let source = UUri::from_str(source_uri)?;
    let sink = UUri::from_str(sink_uri)?;
    let source_filter = UUri::from_str(source_filter_uri)?;
    let sink_filter = UUri::from_str(sink_filter_uri)?;
    let umessage = UMessageBuilder::notification(source, sink)
        .with_priority(up_rust::UPriority::UPRIORITY_CS2)
        .with_traceparent("traceparent")
        .with_ttl(ttl)
        .build_with_payload(MESSAGE_DATA, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;

    // [utest->dsn~utransport-registerlistener-start-invoking-listeners~1]
    register_listener_and_send(authority, umessage, &source_filter, Some(&sink_filter)).await
}

#[test_case::test_case(
    "vehicle1",
    "//vehicle1/10A10B/1/0", "//vehicle1/55A1/2/A1",
    "//vehicle1/10A10B/1/0", "//vehicle1/55A1/2/A1";
    "specific filters")]
#[test_case::test_case(
    "vehicle1",
    "/D5A/3/0", "//vehicle1/355A1/2/BB1",
    "//vehicle1/D5A/3/FFFF", "//vehicle1/FFFF55A1/2/BB1";
    "source filter with wildcard resource ID")]
#[test_case::test_case(
    "vehicle1",
    "//vehicle1/70222/2/0", "//vehicle1/5A55A1/2/D29",
    "//*/FFFF0222/FF/FFFF", "//vehicle1/5A55A1/2/FFFF";
    "source filter with wildcard authority and service instance ID")]
#[test_case::test_case(
    "vehicle1",
    "//vehicle1/1200BA/1/0", "//vehicle1/5A1/2/D2",
    "//*/FFFFFFFF/FF/FFFF", "//vehicle1/FFFFFFFF/FF/FFFF";
    "all RPC requests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_rpc_request_message_gets_delivered_to_listener(
    authority: &str,
    reply_to_uri: &str,
    method_to_invoke_uri: &str,
    source_filter_uri: &str,
    sink_filter_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    test_lib::before_test();

    let reply_to = UUri::from_str(reply_to_uri)?;
    let method_to_invoke = UUri::from_str(method_to_invoke_uri)?;
    let source_filter = UUri::from_str(source_filter_uri)?;
    let sink_filter = UUri::from_str(sink_filter_uri)?;
    let umessage = UMessageBuilder::request(method_to_invoke, reply_to, 5_000)
        .with_priority(up_rust::UPriority::UPRIORITY_CS5)
        .with_token("token")
        .with_traceparent("traceparent")
        .with_permission_level(15)
        .build_with_payload(MESSAGE_DATA, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;

    // [utest->dsn~utransport-registerlistener-start-invoking-listeners~1]
    register_listener_and_send(authority, umessage, &source_filter, Some(&sink_filter)).await
}

#[test_case::test_case(
    "vehicle1",
    "//vehicle1/10A10B/1/0", "//vehicle1/55A1/2/A1",
    "//vehicle1/10A10B/1/0", "//vehicle1/55A1/2/A1";
    "specific filters")]
#[test_case::test_case(
    "vehicle1",
    "/D5A/3/0", "//vehicle1/355A1/2/BB1",
    "//vehicle1/D5A/3/FFFF", "//vehicle1/FFFF55A1/2/BB1";
    "source filter with wildcard resource ID")]
#[test_case::test_case(
    "vehicle1",
    "//vehicle1/70222/2/0", "//vehicle1/5A55A1/2/D29",
    "//vehicle1/FFFF0222/FF/FFFF", "//*/5A55A1/2/FFFF";
    "source filter with wildcard authority and service instance ID")]
#[tokio::test(flavor = "multi_thread")]
async fn test_rpc_response_message_gets_delivered_to_listener(
    authority: &str,
    reply_to_uri: &str,
    invoked_method_uri: &str,
    sink_filter_uri: &str,
    source_filter_uri: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    test_lib::before_test();

    let reply_to = UUri::from_str(reply_to_uri)?;
    let invoked_method = UUri::from_str(invoked_method_uri)?;
    let source_filter = UUri::from_str(source_filter_uri)?;
    let sink_filter = UUri::from_str(sink_filter_uri)?;
    let umessage = UMessageBuilder::response(reply_to, UUID::build(), invoked_method)
        .with_ttl(5_000)
        .with_priority(up_rust::UPriority::UPRIORITY_CS5)
        .with_traceparent("traceparent")
        .with_comm_status(up_rust::UCode::NOT_FOUND)
        .build_with_payload(MESSAGE_DATA, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;

    // [utest->dsn~utransport-registerlistener-start-invoking-listeners~1]
    register_listener_and_send(authority, umessage, &source_filter, Some(&sink_filter)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_expired_rpc_request_message_is_not_delivered_to_listener() {
    test_lib::before_test();

    let reply_to = UUri::from_str("//vehicle1/10A10B/1/0").expect("invalid URI");
    let method_to_invoke = UUri::from_str("//vehicle1/55A1/2/A1").expect("invalid URI");
    let source_filter = UUri::from_str("//vehicle1/10A10B/1/0").expect("invalid URI");
    let sink_filter = UUri::from_str("//vehicle1/55A1/2/A1").expect("invalid URI");
    // timestamp = 0x018D548EA8E0 (Monday, 29 January 2024, 9:30:52 AM GMT)
    // ver = 0b0111
    // variant = 0b10
    let uuid = UUID {
        msb: 0x018D_548E_A8E0_7000u64,
        lsb: 0x8000_0000_0000_0000u64,
        ..Default::default()
    };
    // create message that is already expired, based on the timestamp in
    // the UUID
    let umessage = UMessageBuilder::request(method_to_invoke, reply_to, 5_000)
        .with_message_id(uuid)
        .with_priority(up_rust::UPriority::UPRIORITY_CS5)
        .with_token("token")
        .with_traceparent("traceparent")
        .with_permission_level(15)
        .build_with_payload(MESSAGE_DATA, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .expect("failed to create message");

    // [utest->dsn~up-attributes-ttl-timeout~1]
    assert!(
        register_listener_and_send("vehicle1", umessage, &source_filter, Some(&sink_filter))
            .await
            .is_err_and(|e| {
                let err = e.downcast_ref::<UStatus>().unwrap();
                matches!(err.get_code(), UCode::DEADLINE_EXCEEDED)
            }),
        "Expected to fail with DEADLINE_EXCEEDED error for expired message"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unregister_listener_stops_processing_of_messages() {
    let local_uuri = UUri::try_from_parts("vehicle", 0xABC, 1, 0).expect("invalid local URI");
    let transport = test_lib::create_up_transport_zenoh(local_uuri.to_uri(false).as_str())
        .await
        .expect("failed to create transport");

    let message_received = Arc::new(Notify::new());
    let message_received_barrier = message_received.clone();
    let mut listener = MockUListener::new();
    listener.expect_on_receive().returning(move |_msg| {
        message_received.notify_one();
    });

    let listener_to_register = Arc::new(listener);
    let msg =
        UMessageBuilder::publish(UUri::from_str("//vehicle/123/1/9000").expect("invalid topic"))
            .build()
            .expect("failed to create message");

    // [utest->dsn~utransport-registerlistener-start-invoking-listeners~1]
    assert!(transport
        .register_listener(&UUri::any(), None, listener_to_register.clone())
        .await
        .is_ok());

    // first message is expected to be processed by listener
    assert!(transport.send(msg.clone()).await.is_ok());
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier.notified())
            .await
            .is_ok()
    );

    // [utest->dsn~utransport-unregisterlistener-stop-invoking-listeners~1]
    // after unregistering the listener,
    assert!(transport
        .unregister_listener(&UUri::any(), None, listener_to_register)
        .await
        .is_ok());
    //  no further messages should be processed
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier.notified())
            .await
            .is_err(),
        "Expected no further messages to be processed after unregistering the listener"
    );
}

#[tokio::test(flavor = "multi_thread")]
// [utest->dsn~utransport-registerlistener-listener-reuse~1]
async fn test_same_listener_can_be_registered_for_multiple_filters() {
    let local_uuri = UUri::try_from_parts("vehicle", 0xABC, 1, 0).expect("invalid local URI");
    let transport = test_lib::create_up_transport_zenoh(local_uuri.to_uri(false).as_str())
        .await
        .expect("failed to create transport");

    let message_received = Arc::new(Notify::new());
    let message_received_barrier = message_received.clone();
    let mut listener = MockUListener::new();
    listener.expect_on_receive().returning(move |_msg| {
        message_received.notify_one();
    });

    let listener_to_register = Arc::new(listener);
    let topic1 = UUri::try_from_parts("vehicle", 0x1234, 0x01, 0x9000).expect("invalid topic");
    let topic2 = UUri::try_from_parts("vehicle", 0xABCD, 0x01, 0xA000).expect("invalid topic");

    assert!(transport
        .register_listener(&topic1, None, listener_to_register.clone())
        .await
        .is_ok());
    assert!(transport
        .register_listener(&topic2, None, listener_to_register.clone())
        .await
        .is_ok());

    assert!(transport
        .send(
            UMessageBuilder::publish(topic1.clone())
                .build()
                .expect("failed to create message")
        )
        .await
        .is_ok());
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier.notified())
            .await
            .is_ok()
    );

    assert!(transport
        .send(
            UMessageBuilder::publish(topic2.clone())
                .build()
                .expect("failed to create message")
        )
        .await
        .is_ok());
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier.notified())
            .await
            .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread")]
// [utest->dsn~utransport-registerlistener-number-of-listeners~1]
async fn test_multiple_listeners_can_be_registered_for_the_same_filter() {
    let local_uuri = UUri::try_from_parts("vehicle", 0xABC, 1, 0).expect("invalid local URI");
    let transport = test_lib::create_up_transport_zenoh(local_uuri.to_uri(false).as_str())
        .await
        .expect("failed to create transport");

    let message_received1 = Arc::new(Notify::new());
    let message_received_barrier1 = message_received1.clone();
    let mut listener1 = MockUListener::new();
    listener1.expect_on_receive().returning(move |_msg| {
        message_received1.notify_one();
    });
    let message_received2 = Arc::new(Notify::new());
    let message_received_barrier2 = message_received2.clone();
    let mut listener2 = MockUListener::new();
    listener2.expect_on_receive().returning(move |_msg| {
        message_received2.notify_one();
    });

    let topic = UUri::try_from_parts("vehicle", 0x1234, 0x01, 0x9000).expect("invalid topic");

    assert!(transport
        .register_listener(&topic, None, Arc::new(listener1))
        .await
        .is_ok());
    assert!(transport
        .register_listener(&topic, None, Arc::new(listener2))
        .await
        .is_ok());

    assert!(transport
        .send(
            UMessageBuilder::publish(topic.clone())
                .build()
                .expect("failed to create message")
        )
        .await
        .is_ok());
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier1.notified())
            .await
            .is_ok()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), message_received_barrier2.notified())
            .await
            .is_ok()
    );
}
