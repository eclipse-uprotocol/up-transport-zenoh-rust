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
pub mod test_lib;

use async_trait::async_trait;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use test_case::test_case;
use tokio::time::{sleep, Duration};
use up_rust::{
    LocalUriProvider, UListener, UMessage, UMessageBuilder, UPayloadFormat, UTransport, UUri,
};

struct PublishNotificationListener {
    recv_data: Arc<Mutex<String>>,
}
impl PublishNotificationListener {
    fn new() -> Self {
        PublishNotificationListener {
            recv_data: Arc::new(Mutex::new(String::new())),
        }
    }
    fn get_recv_data(&self) -> String {
        self.recv_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for PublishNotificationListener {
    async fn on_receive(&self, msg: UMessage) {
        let data = msg.payload.unwrap();
        let value = data.into_iter().map(|c| c as char).collect::<String>();
        *self.recv_data.lock().unwrap() = value;
    }
}

#[test_case("//publisher/1/1/0", 0x8000, "//publisher/1/1/8000"; "Normal UUri")]
#[tokio::test(flavor = "multi_thread")]
async fn test_publish_and_subscribe(src_uuri: &str, resource_id: u16, listen_uuri: &str) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let uptransport_send = test_lib::create_up_transport_zenoh(src_uuri).await.unwrap();
    let uptransport_recv = test_lib::create_up_transport_zenoh("//subscriber/3/1/0")
        .await
        .unwrap();
    let publish_uuri = uptransport_send.get_resource_uri(resource_id);
    let listen_uuri = UUri::from_str(listen_uuri).unwrap();

    // Register the listener
    let pub_listener = Arc::new(PublishNotificationListener::new());
    uptransport_recv
        .register_listener(&listen_uuri, None, pub_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    sleep(Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::publish(publish_uuri)
        .build_with_payload(target_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();
    uptransport_send.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    sleep(Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(pub_listener.get_recv_data(), target_data);

    // Cleanup
    uptransport_recv
        .unregister_listener(&listen_uuri, None, pub_listener)
        .await
        .unwrap();
}

#[test_case("//sender/1/1/0", 0x8000, "//receiver/2/1/0", "//*/FFFF/FF/FFFF", Some("//receiver/2/1/0"); "Any source UUri")]
#[test_case("//sender/1/1/0", 0x8000, "//receiver/2/1/0", "//sender/1/1/8000", Some("//receiver/2/1/0"); "Specific source UUri")]
#[test_case("//ustreamer_sender/1/1/0", 0x8000, "//ustreamer_receiver/2/1/0", "//ustreamer_sender/FFFF/FF/FFFF", Some("//*/FFFF/FF/FFFF"); "uStreamer case for notification")]
#[tokio::test(flavor = "multi_thread")]
async fn test_notification_and_subscribe(
    src_uuri: &str,
    resource_id: u16,
    sink_uuri: &str,
    src_filter: &str,
    sink_filter: Option<&str>,
) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let uptransport_sender = test_lib::create_up_transport_zenoh(src_uuri).await.unwrap();
    let uptransport_receiver = test_lib::create_up_transport_zenoh(sink_uuri)
        .await
        .unwrap();
    let src_uuri = uptransport_sender.get_resource_uri(resource_id);
    let sink_uuri = UUri::from_str(sink_uuri).unwrap();
    let src_filter = UUri::from_str(src_filter).unwrap();
    let sink_filter = sink_filter.map(|f| UUri::from_str(f).unwrap());

    // Register the listener
    let notification_listener = Arc::new(PublishNotificationListener::new());
    uptransport_receiver
        .register_listener(
            &src_filter,
            sink_filter.as_ref(),
            notification_listener.clone(),
        )
        .await
        .unwrap();
    // Waiting for listener to take effect
    sleep(Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::notification(src_uuri, sink_uuri)
        .build_with_payload(target_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();
    uptransport_sender.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    sleep(Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(notification_listener.get_recv_data(), target_data);

    // Cleanup
    uptransport_receiver
        .unregister_listener(&src_filter, sink_filter.as_ref(), notification_listener)
        .await
        .unwrap();
}
