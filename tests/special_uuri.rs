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

use async_std::task::{self, block_on};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time,
};
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    CallOptions, Data, RpcClient, UListener, UMessage, UMessageBuilder, UMessageType, UPayload,
    UPayloadFormat, UStatus, UTransport, UUIDBuilder,
};

struct SpecialListener {
    up_client: Arc<UPClientZenoh>,
    recv_data: Arc<Mutex<String>>,
    request_data: String,
    response_data: String,
}
impl SpecialListener {
    fn new(up_client: Arc<UPClientZenoh>, request_data: String, response_data: String) -> Self {
        SpecialListener {
            up_client,
            recv_data: Arc::new(Mutex::new(String::new())),
            request_data,
            response_data,
        }
    }
    // TODO: Should be removed later
    #[allow(dead_code)]
    fn get_recv_data(&self) -> String {
        self.recv_data.lock().unwrap().clone()
    }
}

#[async_trait]
impl UListener for SpecialListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage {
            attributes,
            payload,
            ..
        } = msg;
        let value = if let Data::Value(v) = payload.clone().unwrap().data.unwrap() {
            v.into_iter().map(|c| c as char).collect::<String>()
        } else {
            panic!("The message should be Data::Value type.");
        };
        match attributes.type_.enum_value().unwrap() {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                *self.recv_data.lock().unwrap() = value;
            }
            UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                panic!("Notification type");
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                assert_eq!(self.request_data, value);
                // Send back result
                let umessage = UMessageBuilder::response_for_request(&attributes)
                    .with_message_id(UUIDBuilder::build())
                    .build_with_payload(
                        self.response_data.as_bytes().to_vec().into(),
                        UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                    )
                    .unwrap();
                block_on(self.up_client.send(umessage)).unwrap();
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                panic!("Response type");
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                panic!("Unknown type");
            }
        }
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[async_std::test]
async fn test_register_listener_with_special_uuri() {
    test_lib::before_test();

    // Initialization
    let upclient1 = Arc::new(test_lib::create_up_client_zenoh().await.unwrap());
    let upclient2 = test_lib::create_up_client_zenoh().await.unwrap();
    let publish_data = String::from("Hello World!");
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");

    // Register the listener
    let listener_uuri = test_lib::create_special_uuri();
    let special_listener = Arc::new(SpecialListener::new(
        upclient1.clone(),
        request_data.clone(),
        response_data.clone(),
    ));
    upclient1
        .register_listener(listener_uuri.clone(), special_listener.clone())
        .await
        .unwrap();

    // Send Publish
    {
        // Initialization
        let mut publish_uuri = test_lib::create_utransport_uuri(0);
        publish_uuri.authority = Some(test_lib::create_authority()).into();

        // Send Publish data
        let umessage = UMessageBuilder::publish(publish_uuri)
            .with_message_id(UUIDBuilder::build())
            .build_with_payload(
                publish_data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        upclient2.send(umessage).await.unwrap();

        // Waiting for the subscriber to receive data
        task::sleep(time::Duration::from_millis(1000)).await;

        // Compare the result
        //TODO: Update the UAuthority
        //assert_eq!(special_listener.get_recv_data(), response_data);
    }

    // Send Request
    {
        // Initialization
        let mut request_uuri = test_lib::create_rpcserver_uuri();
        request_uuri.authority = Some(test_lib::create_authority()).into();

        // RpcClient: Send Request data
        let payload = UPayload {
            format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
            data: Some(Data::Value(request_data.as_bytes().to_vec())),
            ..Default::default()
        };
        let result = upclient2
            .invoke_method(
                request_uuri,
                payload,
                CallOptions {
                    ttl: 1000,
                    ..Default::default()
                },
            )
            .await;

        // Process the result
        if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            assert_eq!(response_data, value);
        } else {
            panic!("Failed to get result from invoke_method.");
        }
    }

    // Cleanup
    upclient1
        .unregister_listener(listener_uuri, special_listener.clone())
        .await
        .unwrap();
}
