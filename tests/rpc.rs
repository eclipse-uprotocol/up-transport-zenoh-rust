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
mod test_lib;

use async_std::task::{self, block_on};
use std::{sync::Arc, time};
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    CallOptions, Data, RpcClient, UMessage, UMessageType, UPayload, UPayloadFormat, UStatus,
    UTransport,
};
use zenoh::config::Config;

#[async_std::test]
async fn test_rpc_server_client() {
    test_lib::before_test();

    let upclient_client = UPClientZenoh::new(Config::default()).await.unwrap();
    let upclient_server = Arc::new(UPClientZenoh::new(Config::default()).await.unwrap());
    let request_data = String::from("This is the request data");
    let response_data = String::from("This is the response data");
    let uuri = test_lib::create_rpcserver_uuri();

    // setup RpcServer callback
    let upclient_server_cloned = upclient_server.clone();
    let response_data_cloned = response_data.clone();
    let request_data_cloned = request_data.clone();
    let callback = move |result: Result<UMessage, UStatus>| {
        match result {
            Ok(msg) => {
                let UMessage {
                    attributes,
                    payload,
                    ..
                } = msg;
                // Get the UUri
                let source = attributes.clone().unwrap().source.unwrap();
                let sink = attributes.clone().unwrap().sink.unwrap();
                // Build the payload to send back
                if let Data::Value(v) = payload.unwrap().data.unwrap() {
                    let value = v.into_iter().map(|c| c as char).collect::<String>();
                    assert_eq!(request_data_cloned, value);
                } else {
                    panic!("The message should be Data::Value type.");
                }
                let upayload = UPayload {
                    format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
                    data: Some(Data::Value(response_data_cloned.as_bytes().to_vec())),
                    ..Default::default()
                };
                // Set the attributes type to Response
                let mut uattributes = attributes.unwrap();
                uattributes.type_ = UMessageType::UMESSAGE_TYPE_RESPONSE.into();
                uattributes.sink = Some(source.clone()).into();
                uattributes.source = Some(sink.clone()).into();
                // Send back result
                block_on(upclient_server_cloned.send(UMessage {
                    attributes: Some(uattributes).into(),
                    payload: Some(upayload).into(),
                    ..Default::default()
                }))
                .unwrap();
            }
            Err(ustatus) => {
                panic!("Internal Error: {ustatus:?}");
            }
        }
    };
    upclient_server
        .register_listener(uuri.clone(), Box::new(callback))
        .await
        .unwrap();
    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // Run RpcClient
    let payload = UPayload {
        format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
        data: Some(Data::Value(request_data.as_bytes().to_vec())),
        ..Default::default()
    };
    let result = upclient_client
        .invoke_method(
            uuri,
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
