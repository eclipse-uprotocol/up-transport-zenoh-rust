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
use tokio::{
    runtime::Handle,
    task,
    time::{sleep, Duration},
};
use up_rust::{
    communication::{CallOptions, RpcClient, UPayload},
    LocalUriProvider, UListener, UMessage, UMessageBuilder, UPayloadFormat, UPriority, UTransport,
    UUri, UUID,
};
use up_transport_zenoh::{UPTransportZenoh, ZenohRpcClient};

// RequestListener
struct RequestListener {
    up_transport: Arc<UPTransportZenoh>,
    request_data: String,
    response_data: String,
}
impl RequestListener {
    fn new(
        up_transport: Arc<UPTransportZenoh>,
        request_data: String,
        response_data: String,
    ) -> Self {
        RequestListener {
            up_transport,
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
        let value = payload
            .unwrap()
            .into_iter()
            .map(|c| c as char)
            .collect::<String>();
        assert_eq!(self.request_data, value);
        // Send back result
        let umessage = UMessageBuilder::response_for_request(&attributes)
            .build_with_payload(
                self.response_data.clone(),
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

#[test_case("//requester/1/1/0", "//responder/2/1/0", 0x1, "//*/FFFF/FF/0", Some("//responder/2/1/1"); "Any source UUri")]
#[test_case("//requester/1/1/0", "//responder/2/1/0", 0x1, "//requester/1/1/0", Some("//responder/2/1/1"); "Specific source UUri")]
#[test_case("//ustreamer_requester/1/1/0", "//ustreamer_responder/2/1/0", 0x1, "//ustreamer_requester/FFFF/FF/FFFF", Some("//*/FFFF/FF/FFFF"); "uStreamer case for RPC")]
#[tokio::test(flavor = "multi_thread")]
async fn test_rpc_server_client(
    src_uuri: &str,
    sink_uuri: &str,
    resource_id: u16,
    src_filter: &str,
    sink_filter: Option<&str>,
) {
    test_lib::before_test();

    // Initialization
    let uptransport_client = Arc::new(test_lib::create_up_transport_zenoh(src_uuri).await.unwrap());
    let uptransport_server = Arc::new(
        test_lib::create_up_transport_zenoh(sink_uuri)
            .await
            .unwrap(),
    );
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");
    let src_uuri = UUri::from_str(src_uuri).unwrap();
    let sink_uuri = uptransport_server.get_resource_uri(resource_id);
    let src_filter = UUri::from_str(src_filter).unwrap();
    let sink_filter = sink_filter.map(|s| UUri::from_str(s).unwrap());

    // Setup RpcServer callback
    let request_listener = Arc::new(RequestListener::new(
        uptransport_server.clone(),
        request_data.clone(),
        response_data.clone(),
    ));
    uptransport_server
        .register_listener(&src_filter, sink_filter.as_ref(), request_listener.clone())
        .await
        .unwrap();
    // Need some time for queryable to run
    sleep(Duration::from_millis(1000)).await;

    // Send Request with ZenohRpcClient (L2 API)
    {
        let rpc_client = Arc::new(ZenohRpcClient::new(uptransport_client.clone()));

        let payload = UPayload::new(request_data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT);
        let call_options = CallOptions::for_rpc_request(
            5_000,
            Some(UUID::build()),
            Some("my_token".to_string()),
            Some(UPriority::UPRIORITY_CS6),
        );
        let result = rpc_client
            .invoke_method(sink_uuri.clone(), call_options, Some(payload))
            .await
            .unwrap();

        // Process the result
        let payload = result.unwrap().payload();
        let value = payload.into_iter().map(|c| c as char).collect::<String>();
        assert_eq!(response_data.clone(), value);
    }

    // Send Request with Transport Layer (L1 API)
    {
        // Register Response callback
        let response_listener = Arc::new(ResponseListener::new());
        uptransport_client
            .register_listener(&sink_uuri, Some(&src_uuri), response_listener.clone())
            .await
            .unwrap();

        // Send request
        let umessage = UMessageBuilder::request(sink_uuri.clone(), src_uuri.clone(), 1000)
            .build_with_payload(request_data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
            .unwrap();
        uptransport_client.send(umessage).await.unwrap();

        // Waiting for the callback to process data
        sleep(Duration::from_millis(2000)).await;

        // Compare the result
        assert_eq!(response_listener.get_response_data(), response_data);

        // Cleanup
        uptransport_client
            .unregister_listener(&sink_uuri, Some(&src_uuri), response_listener.clone())
            .await
            .unwrap();
    }

    // Cleanup
    uptransport_server
        .unregister_listener(&src_filter, sink_filter.as_ref(), request_listener.clone())
        .await
        .unwrap();
}
