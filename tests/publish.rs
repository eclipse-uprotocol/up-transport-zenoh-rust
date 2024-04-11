//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
pub mod test_lib;

use async_std::task;
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time,
};
use test_case::test_case;
use up_rust::{
    Data, UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUIDBuilder,
    UUri,
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
        if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            *self.recv_data.lock().unwrap() = value;
        } else {
            panic!("The message should be Data::Value type.");
        }
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0), test_lib::create_utransport_uuri(Some(0), 0, 0); "Normal UUri")]
#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0), test_lib::create_special_uuri(0); "Special UUri")]
#[async_std::test]
async fn test_publish_and_subscribe(origin_uuri: UUri, dst_uuri: UUri) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient_send = test_lib::create_up_client_zenoh(0, 0).await.unwrap();
    let upclient_recv = test_lib::create_up_client_zenoh(1, 1).await.unwrap();

    // Register the listener
    let pub_listener = Arc::new(PublishNotificationListener::new());
    upclient_recv
        .register_listener(dst_uuri.clone(), pub_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    task::sleep(time::Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::publish(origin_uuri.clone())
        .with_message_id(UUIDBuilder::build())
        .build_with_payload(
            target_data.as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient_send.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(pub_listener.get_recv_data(), target_data);

    // Cleanup
    upclient_recv
        .unregister_listener(dst_uuri.clone(), pub_listener)
        .await
        .unwrap();
}

#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0), test_lib::create_utransport_uuri(Some(1), 1, 1), test_lib::create_utransport_uuri(Some(1), 1, 1); "Normal UUri")]
#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0), test_lib::create_utransport_uuri(Some(1), 1, 1), test_lib::create_special_uuri(1); "Special UUri")]
#[async_std::test]
async fn test_notification_and_subscribe(origin_uuri: UUri, dst_uuri: UUri, listen_uuri: UUri) {
    test_lib::before_test();

    // Initialization
    let target_data = String::from("Hello World!");
    let upclient_notify = test_lib::create_up_client_zenoh(0, 0).await.unwrap();
    let upclient_recv = test_lib::create_up_client_zenoh(1, 1).await.unwrap();

    // Register the listener
    let notification_listener = Arc::new(PublishNotificationListener::new());
    upclient_recv
        .register_listener(listen_uuri.clone(), notification_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    task::sleep(time::Duration::from_millis(1000)).await;

    // Send UMessage
    let umessage = UMessageBuilder::notification(origin_uuri.clone(), dst_uuri.clone())
        .with_message_id(UUIDBuilder::build())
        .build_with_payload(
            target_data.as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient_notify.send(umessage).await.unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Compare the result
    assert_eq!(notification_listener.get_recv_data(), target_data);

    // Cleanup
    upclient_recv
        .unregister_listener(listen_uuri.clone(), notification_listener)
        .await
        .unwrap();
}
