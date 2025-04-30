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
use tokio::{
    runtime::Handle,
    task,
    time::{sleep, Duration},
};
use up_rust::{
    LocalUriProvider, UListener, UMessage, UMessageBuilder, UMessageType, UPayloadFormat,
    UTransport, UUri,
};
use up_transport_zenoh::UPTransportZenoh;

// UStreamerListener
struct UStreamerListener {
    up_streamer: Arc<UPTransportZenoh>,
    // Notification data ustreamer received
    recv_notification_data: Arc<Mutex<String>>,
    // RPC Response data ustreamer received
    recv_response_data: Arc<Mutex<String>>,
    // Const String to reply
    predefined_resp_data: String,
}
impl UStreamerListener {
    fn new(up_streamer: Arc<UPTransportZenoh>) -> Self {
        UStreamerListener {
            up_streamer,
            recv_notification_data: Arc::new(Mutex::new(String::new())),
            recv_response_data: Arc::new(Mutex::new(String::new())),
            predefined_resp_data: String::from("Response from uStreamer"),
        }
    }
    fn get_recv_notification_data(&self) -> String {
        self.recv_notification_data.lock().unwrap().clone()
    }
    fn get_recv_response_data(&self) -> String {
        self.recv_response_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for UStreamerListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage {
            attributes,
            payload,
            ..
        } = msg;
        match attributes.clone().0.unwrap().type_.enum_value().unwrap() {
            UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                let data = payload.unwrap();
                let value = data.into_iter().map(|c| c as char).collect::<String>();
                *self.recv_notification_data.lock().unwrap() = value;
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                let umessage = UMessageBuilder::response_for_request(&attributes)
                    .build_with_payload(
                        self.predefined_resp_data.clone(),
                        UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                    )
                    .unwrap();
                task::block_in_place(|| {
                    Handle::current()
                        .block_on(self.up_streamer.send(umessage))
                        .unwrap();
                });
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                let data = payload.unwrap();
                let value = data.into_iter().map(|c| c as char).collect::<String>();
                *self.recv_response_data.lock().unwrap() = value;
            }
            _ => {
                panic!("Wrong UMessageType!");
            }
        }
    }
}

// ResponseListener
struct ResponseListener {
    response_data: Arc<Mutex<String>>,
}
impl ResponseListener {
    fn new() -> Self {
        ResponseListener {
            response_data: Arc::new(Mutex::new(String::new())),
        }
    }
    fn get_response_data(&self) -> String {
        self.response_data.lock().unwrap().clone()
    }
}
#[async_trait]
impl UListener for ResponseListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage { payload, .. } = msg;
        // Check the response data
        let value = payload
            .unwrap()
            .into_iter()
            .map(|c| c as char)
            .collect::<String>();
        *self.response_data.lock().unwrap() = value;
    }
}

// RequestListener
struct RequestListener {
    up_transport: Arc<UPTransportZenoh>,
    predefined_resp_data: String,
}
impl RequestListener {
    fn new(up_transport: Arc<UPTransportZenoh>) -> Self {
        RequestListener {
            up_transport,
            predefined_resp_data: String::from("Response from client"),
        }
    }
}
#[async_trait]
impl UListener for RequestListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage { attributes, .. } = msg;
        // Send back result
        let umessage = UMessageBuilder::response_for_request(&attributes)
            .build_with_payload(
                self.predefined_resp_data.clone(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        task::block_in_place(|| {
            Handle::current()
                .block_on(self.up_transport.send(umessage))
                .unwrap();
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ustreamer() {
    test_lib::before_test();

    // Initialization
    let uclient = Arc::new(
        test_lib::create_up_transport_zenoh("//uclient/1/1/0")
            .await
            .unwrap(),
    );
    let ustreamer = Arc::new(
        test_lib::create_up_transport_zenoh("//ustreamer/2/1/0")
            .await
            .unwrap(),
    );

    // Setup uStreamer listener
    let src_filter = UUri::from_str("//uclient/FFFF/FF/FFFF").unwrap();
    let sink_filter = UUri::from_str("//*/FFFF/FF/FFFF").unwrap();
    let ustreamer_listener = Arc::new(UStreamerListener::new(ustreamer.clone()));
    ustreamer
        .register_listener(&src_filter, Some(&sink_filter), ustreamer_listener.clone())
        .await
        .unwrap();
    // Need some time for queryable to run
    sleep(Duration::from_millis(1000)).await;

    // Send Notification (uclient => uStreamer)
    {
        let target_data = String::from("Notification");
        let src_uuri = uclient.get_resource_uri(0x8000);
        let sink_uuri = ustreamer.get_source_uri();
        let umessage = UMessageBuilder::notification(src_uuri, sink_uuri)
            .build_with_payload(target_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
            .unwrap();
        uclient.send(umessage).await.unwrap();

        // Waiting for the subscriber to receive data
        sleep(Duration::from_millis(1000)).await;

        // Compare the result
        assert_eq!(ustreamer_listener.get_recv_notification_data(), target_data);
    }

    // Send Request (uclient => uStreamer)
    {
        let src_uuri = uclient.get_source_uri();
        let sink_uuri = ustreamer.get_resource_uri(1);

        // Register Response callback
        let response_listener = Arc::new(ResponseListener::new());
        uclient
            .register_listener(&sink_uuri, Some(&src_uuri), response_listener.clone())
            .await
            .unwrap();

        // Send Request
        let umessage = UMessageBuilder::request(sink_uuri.clone(), src_uuri.clone(), 1000)
            .build_with_payload(
                String::from("Not matter"),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        uclient.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        sleep(Duration::from_millis(2000)).await;

        // Compare the result
        assert_eq!(
            response_listener.get_response_data(),
            ustreamer_listener.predefined_resp_data
        );

        // Cleanup
        uclient
            .unregister_listener(&sink_uuri, Some(&src_uuri), response_listener.clone())
            .await
            .unwrap();
    }

    // Send Response (uclient => uStreamer)
    // In other words, Send Request (uStreamer => uclient)
    {
        let src_uuri = ustreamer.get_source_uri();
        let sink_uuri = uclient.get_resource_uri(1);

        // Register RPC callback
        let rpc_listener = Arc::new(RequestListener::new(uclient.clone()));
        uclient
            .register_listener(&src_uuri, Some(&sink_uuri), rpc_listener.clone())
            .await
            .unwrap();

        // Waiting for the callback to be ready
        sleep(Duration::from_millis(1000)).await;

        // Send Request (uStreamer => uclient)
        let umessage = UMessageBuilder::request(sink_uuri.clone(), src_uuri.clone(), 1000)
            .build_with_payload(
                String::from("Not matter"),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        ustreamer.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        sleep(Duration::from_millis(2000)).await;

        // Compare the result
        assert_eq!(
            rpc_listener.predefined_resp_data,
            ustreamer_listener.get_recv_response_data()
        );

        // Cleanup
        uclient
            .unregister_listener(&src_uuri, Some(&sink_uuri), rpc_listener.clone())
            .await
            .unwrap();
    }

    // Cleanup
    ustreamer
        .unregister_listener(&src_filter, Some(&sink_filter), ustreamer_listener.clone())
        .await
        .unwrap();
}
