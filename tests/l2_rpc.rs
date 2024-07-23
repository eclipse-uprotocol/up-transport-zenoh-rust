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

use std::{str::FromStr, sync::Arc};
use tokio::time::{sleep, Duration};
use up_rust::{
    communication::{
        CallOptions, InMemoryRpcServer, RequestHandler, RpcClient, RpcServer,
        ServiceInvocationError, UPayload,
    },
    LocalUriProvider, UPayloadFormat, UUri,
};
use up_transport_zenoh::ZenohRpcClient;

const METHOD_RESOURCE_ID: u16 = 0x00a0;

struct ExampleHandler {
    request_data: String,
    response_data: String,
}
impl ExampleHandler {
    pub fn new(request_data: String, response_data: String) -> Self {
        ExampleHandler {
            request_data,
            response_data,
        }
    }
}
#[async_trait::async_trait]
impl RequestHandler for ExampleHandler {
    async fn invoke_method(
        &self,
        _resource_id: u16,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        let payload = request_payload.unwrap().payload();
        let data = payload.into_iter().map(|c| c as char).collect::<String>();
        // compare the request data
        assert_eq!(data, self.request_data);
        // return
        let payload = UPayload::new(
            self.response_data.clone().into(),
            UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
        );
        Ok(Some(payload))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_l2_rpc() {
    test_lib::before_test();

    // Initialization
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");
    let uptransport_server = Arc::new(
        test_lib::create_up_transport_zenoh("//l2_responder/2/1/0")
            .await
            .unwrap(),
    );
    let uptransport_client = Arc::new(
        test_lib::create_up_transport_zenoh("//l2_requester/1/1/0")
            .await
            .unwrap(),
    );

    // Create L2 RPC server
    let rpc_server = InMemoryRpcServer::new(uptransport_server.clone(), uptransport_server.clone());
    let example_handler = Arc::new(ExampleHandler::new(
        request_data.clone(),
        response_data.clone(),
    ));
    // CY_TODO: Verify the what kind of resource_id we can listen to
    rpc_server
        .register_endpoint(
            Some(&UUri::from_str("//*/FFFF/FF/0").unwrap()),
            METHOD_RESOURCE_ID,
            example_handler.clone(),
        )
        .await
        .unwrap();
    // Need some time for queryable to run
    sleep(Duration::from_millis(1000)).await;

    // Create L2 RPC client
    let rpc_client = Arc::new(ZenohRpcClient::new(uptransport_client.clone()));

    let payload = UPayload::new(request_data.into(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT);
    let call_options = CallOptions::for_rpc_request(5_000, None, None, None);
    let result = rpc_client
        .invoke_method(
            uptransport_server.get_resource_uri(METHOD_RESOURCE_ID),
            call_options,
            Some(payload),
        )
        .await
        .unwrap();

    // Process the result
    let payload = result.unwrap().payload();
    let value = payload.into_iter().map(|c| c as char).collect::<String>();
    assert_eq!(response_data, value);
}
