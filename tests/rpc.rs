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
use test_case::test_case;
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    CallOptions, Data, RpcClient, UListener, UMessage, UMessageBuilder, UPayload, UPayloadFormat,
    UStatus, UTransport, UUIDBuilder, UUri,
};

struct RequestListener {
    up_client: Arc<UPClientZenoh>,
    request_data: String,
    response_data: String,
}
impl RequestListener {
    fn new(up_client: Arc<UPClientZenoh>, request_data: String, response_data: String) -> Self {
        RequestListener {
            up_client,
            request_data,
            response_data,
        }
    }
}
#[async_trait]
impl UListener for RequestListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage {
            attributes,
            payload,
            ..
        } = msg;
        // Check the payload of request
        if let Data::Value(v) = payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            assert_eq!(self.request_data, value);
        } else {
            panic!("The message should be Data::Value type.");
        }
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
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

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
        if let Data::Value(v) = payload.unwrap().data.unwrap() {
            let value = v.into_iter().map(|c| c as char).collect::<String>();
            *self.response_data.lock().unwrap() = value;
        } else {
            panic!("The message should be Data::Value type.");
        }
    }
    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[test_case(test_lib::create_rpcserver_uuri(Some(1), 1), test_lib::create_rpcserver_uuri(Some(1), 1); "Normal RPC UUri")]
#[test_case(test_lib::create_rpcserver_uuri(Some(1), 1), test_lib::create_special_uuri(1); "Special listen UUri")]
#[async_std::test]
async fn test_rpc_server_client(dst_uuri: UUri, listen_uuri: UUri) {
    test_lib::before_test();

    // Initialization
    let upclient_client = test_lib::create_up_client_zenoh(0, 0).await.unwrap();
    let upclient_server = Arc::new(test_lib::create_up_client_zenoh(1, 1).await.unwrap());
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");

    // Setup RpcServer callback
    let request_listener = Arc::new(RequestListener::new(
        upclient_server.clone(),
        request_data.clone(),
        response_data.clone(),
    ));
    upclient_server
        .register_listener(listen_uuri.clone(), request_listener.clone())
        .await
        .unwrap();
    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // Send Request with invoke_method
    {
        let payload = UPayload {
            format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
            data: Some(Data::Value(request_data.as_bytes().to_vec())),
            ..Default::default()
        };
        let result = upclient_client
            .invoke_method(
                dst_uuri.clone(),
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
            assert_eq!(response_data.clone(), value);
        } else {
            panic!("Failed to get result from invoke_method.");
        }
    }

    // Send Request with send
    {
        // Register Response callback
        let response_uuri = upclient_client.get_response_uuri();
        let response_listener = Arc::new(ResponseListener::new());
        upclient_client
            .register_listener(response_uuri.clone(), response_listener.clone())
            .await
            .unwrap();

        // Send request
        let umessage = UMessageBuilder::request(dst_uuri.clone(), response_uuri.clone(), 1000)
            .with_message_id(UUIDBuilder::build())
            .build_with_payload(
                request_data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        upclient_client.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        task::sleep(time::Duration::from_millis(2000)).await;

        // Compare the result
        assert_eq!(response_listener.get_response_data(), response_data);

        // Cleanup
        upclient_client
            .unregister_listener(response_uuri.clone(), response_listener.clone())
            .await
            .unwrap();
    }

    // Cleanup
    upclient_server
        .unregister_listener(listen_uuri.clone(), request_listener.clone())
        .await
        .unwrap();
}
