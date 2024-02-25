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
use async_std::task::{self, block_on};
use std::sync::Arc;
use std::time;
use up_rust::{
    rpc::{CallOptionsBuilder, RpcClient, RpcServer},
    transport::{builder::UAttributesBuilder, datamodel::UTransport},
    uprotocol::{
        Data, UCode, UEntity, UMessage, UMessageType, UPayload, UPayloadFormat, UPriority,
        UResource, UStatus, UUri,
    },
    uri::builder::resourcebuilder::UResourceBuilder,
};
use uprotocol_zenoh::UPClientZenoh;
use zenoh::config::Config;

// TODO: Need to check whether the way to create ID is correct?
fn create_utransport_uuri() -> UUri {
    UUri {
        entity: Some(UEntity {
            name: "body.access".to_string(),
            version_major: Some(1),
            id: Some(1234),
            ..Default::default()
        })
        .into(),
        resource: Some(UResource {
            name: "door".to_string(),
            instance: Some("front_left".to_string()),
            message: Some("Door".to_string()),
            id: Some(5678),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    }
}

// TODO: Need to check whether the way to create ID is correct?
fn create_rpcserver_uuri() -> UUri {
    UUri {
        entity: Some(UEntity {
            name: "test_rpc.app".to_string(),
            version_major: Some(1),
            id: Some(1234),
            ..Default::default()
        })
        .into(),
        resource: Some(UResourceBuilder::for_rpc_request(
            Some("SimpleTest".to_string()),
            Some(5678),
        ))
        .into(),
        ..Default::default()
    }
}

#[async_std::test]
async fn test_utransport_register_and_unregister() {
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = create_utransport_uuri();

    // Compare the return string
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "up/0100162e04d20100_0");

    // Able to ungister
    let result = upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
    assert_eq!(result, ());

    // Unable to ungister
    let result = upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Publish listener doesn't exist"
        ))
    )
}

#[async_std::test]
async fn test_rpcserver_register_and_unregister() {
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = create_rpcserver_uuri();

    // Compare the return string
    let listener_string = upclient
        .register_rpc_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "up/0100162e04d20100_0");

    // Able to ungister
    let result = upclient
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
    assert_eq!(result, ());

    // Unable to ungister
    let result = upclient
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "RPC request listener doesn't exist"
        ))
    )
}

#[async_std::test]
async fn test_publish_and_subscribe() {
    let target_data = String::from("Hello World!");
    let upclient = UPClientZenoh::new(Config::default()).await.unwrap();
    let uuri = create_utransport_uuri();

    // Register the listener
    let uuri_cloned = uuri.clone();
    let data_cloned = target_data.clone();
    let listener = move |result: Result<UMessage, UStatus>| match result {
        Ok(msg) => {
            if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
                let value = v.into_iter().map(|c| c as char).collect::<String>();
                assert_eq!(msg.attributes.unwrap().sink.unwrap(), uuri_cloned);
                assert_eq!(value, data_cloned);
            } else {
                panic!("The message should be Data::Value type.");
            }
        }
        Err(ustatus) => panic!("Internal Error: {:?}", ustatus),
    };
    let listener_string = upclient
        .register_listener(uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    // Create uattributes
    let mut attributes = UAttributesBuilder::publish(UPriority::UPRIORITY_CS4).build();
    attributes.sink = Some(uuri.clone()).into();
    // TODO: Check what source we should fill
    attributes.source = Some(uuri.clone()).into();

    // Publish the data
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
        data: Some(Data::Value(target_data.as_bytes().to_vec())),
        ..Default::default()
    };
    upclient
        .send(UMessage {
            attributes: Some(attributes.clone()).into(),
            payload: Some(payload).into(),
            ..Default::default()
        })
        .await
        .unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Cleanup
    upclient
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
}

#[async_std::test]
async fn test_rpc_server_client() {
    let upclient_client = UPClientZenoh::new(Config::default()).await.unwrap();
    let upclient_server = Arc::new(UPClientZenoh::new(Config::default()).await.unwrap());
    let client_data = String::from("This is the client data");
    let server_data = String::from("This is the server data");
    let uuri = create_rpcserver_uuri();

    // setup RpcServer callback
    let upclient_server_cloned = upclient_server.clone();
    let server_data_cloned = server_data.clone();
    let client_data_cloned = client_data.clone();
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
                    assert_eq!(client_data_cloned, value);
                } else {
                    panic!("The message should be Data::Value type.");
                }
                let upayload = UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
                    data: Some(Data::Value(server_data_cloned.as_bytes().to_vec())),
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
                panic!("Internal Error: {:?}", ustatus);
            }
        }
    };
    upclient_server
        .register_rpc_listener(uuri.clone(), Box::new(callback))
        .await
        .unwrap();
    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // TODO: Need to check whether we don't need uattributes.
    //// Create uattributes
    //// TODO: Check TTL (Should TTL map to Zenoh's timeout?)
    //let attributes = UAttributesBuilder::request(UPriority::UPRIORITY_CS4, uuri.clone(), 100)
    //    .with_reqid(UUIDv8Builder::new().build())
    //    .build();

    // Run RpcClient
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
        data: Some(Data::Value(client_data.as_bytes().to_vec())),
        ..Default::default()
    };
    let result = upclient_client
        .invoke_method(uuri, payload, CallOptionsBuilder::default().build())
        .await;

    // Process the result
    if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
        let value = v.into_iter().map(|c| c as char).collect::<String>();
        assert_eq!(server_data, value);
    } else {
        panic!("Failed to get result from invoke_method.");
    }
}
