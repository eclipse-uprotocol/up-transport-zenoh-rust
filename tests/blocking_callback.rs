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

use async_std::task;
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time,
};
use test_case::test_case;
use up_rust::{
    Data, UListener, UMessage, UMessageBuilder, UPayloadFormat, UStatus, UTransport, UUri,
};

struct DelayListener {
    recv_data: Arc<Mutex<String>>,
}
impl DelayListener {
    fn new() -> Self {
        DelayListener {
            recv_data: Arc::new(Mutex::new(String::new())),
        }
    }
    fn get_recv_data(&self) -> String {
        self.recv_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for DelayListener {
    async fn on_receive(&self, msg: UMessage) {
        if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            // Delay the receive time of the first message
            if value == "Pub 0" {
                task::sleep(time::Duration::from_millis(3000)).await;
            }
            *self.recv_data.lock().unwrap() = value;
        } else {
            panic!("The message should be Data::Value type.");
        }
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

// The test is used to check whether blocking user callback will affect receiving messages
#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0); "Normal UUri")]
#[async_std::test]
async fn test_blocking_user_callback(pub_uuri: UUri) {
    test_lib::before_test();

    // Initialization
    let upclient_send = test_lib::create_up_client_zenoh(0, 0).await.unwrap();
    let upclient_recv = test_lib::create_up_client_zenoh(1, 1).await.unwrap();

    // Register the listener
    let pub_listener = Arc::new(DelayListener::new());
    upclient_recv
        .register_listener(pub_uuri.clone(), pub_listener.clone())
        .await
        .unwrap();
    // Waiting for listener to take effect
    task::sleep(time::Duration::from_millis(1000)).await;

    // Send 2 UMessage
    let umsg0 = UMessageBuilder::publish(pub_uuri.clone())
        .build_with_payload(
            "Pub 0".as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    let umsg1 = UMessageBuilder::publish(pub_uuri.clone())
        .build_with_payload(
            "Pub 1".as_bytes().to_vec().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        )
        .unwrap();
    upclient_send.send(umsg0).await.unwrap();
    upclient_send.send(umsg1).await.unwrap();

    // Receive the data in reverse order due to the delay time
    // Waiting for the subscriber to receive 2nd data
    task::sleep(time::Duration::from_millis(1000)).await;
    assert_eq!(pub_listener.get_recv_data(), "Pub 1".to_string());
    // Waiting for the subscriber to receive 1st data
    task::sleep(time::Duration::from_millis(3000)).await;
    assert_eq!(pub_listener.get_recv_data(), "Pub 0".to_string());

    // Cleanup
    upclient_recv
        .unregister_listener(pub_uuri.clone(), pub_listener)
        .await
        .unwrap();
}
