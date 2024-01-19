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
use std::sync::{Arc, Mutex};
use std::time;
use uprotocol_sdk::{
    rpc::{RpcClient, RpcServer},
    transport::builder::UAttributesBuilder,
    transport::datamodel::UTransport,
    uprotocol::{
        Data, UCode, UEntity, UMessage, UMessageType, UPayload, UPayloadFormat, UPriority,
        UResource, UStatus, UUri,
    },
    uri::builder::resourcebuilder::UResourceBuilder,
    uuid::builder::UUIDv8Builder,
};
use uprotocol_zenoh_rust::ULinkZenoh;
use zenoh::config::Config;

// TODO: Need to check whether the way to create ID is correct?
fn create_utransport_uuri() -> UUri {
    UUri {
        entity: Some(UEntity {
            name: "body.access".to_string(),
            version_major: Some(1),
            id: Some(1234),
            ..Default::default()
        }),
        resource: Some(UResource {
            name: "door".to_string(),
            instance: Some("front_left".to_string()),
            message: Some("Door".to_string()),
            id: Some(5678),
        }),
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
        }),
        resource: Some(UResourceBuilder::for_rpc_request(
            Some("SimpleTest".to_string()),
            Some(5678),
        )),
        ..Default::default()
    }
}

#[async_std::test]
async fn test_utransport_register_and_unregister() {
    let ulinkzenoh = ULinkZenoh::new(Config::default()).await.unwrap();
    let uuri = create_utransport_uuri();

    // Compare the return string
    let listener_string = ulinkzenoh
        .register_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "0100162e04d20100_0");

    // Able to ungister
    let result = ulinkzenoh
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
    assert_eq!(result, ());

    // Unable to ungister
    let result = ulinkzenoh
        .unregister_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "Listener doesn't exist"
        ))
    )
}

#[async_std::test]
async fn test_rpcserver_register_and_unregister() {
    let ulinkzenoh = ULinkZenoh::new(Config::default()).await.unwrap();
    let uuri = create_rpcserver_uuri();

    // Compare the return string
    let listener_string = ulinkzenoh
        .register_rpc_listener(uuri.clone(), Box::new(|_| {}))
        .await
        .unwrap();
    assert_eq!(listener_string, "0100162e04d20100_0");

    // Able to ungister
    let result = ulinkzenoh
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
    assert_eq!(result, ());

    // Unable to ungister
    let result = ulinkzenoh
        .unregister_rpc_listener(uuri.clone(), &listener_string)
        .await;
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "Listener doesn't exist"
        ))
    )
}

#[async_std::test]
async fn test_publish_and_subscribe() {
    let target_data = String::from("Hello World!");
    let ulinkzenoh = ULinkZenoh::new(Config::default()).await.unwrap();
    let uuri = create_utransport_uuri();

    // Register the listener
    let uuri_cloned = uuri.clone();
    let data_cloned = target_data.clone();
    let listener = move |result: Result<UMessage, UStatus>| match result {
        Ok(msg) => {
            if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
                let value = v.into_iter().map(|c| c as char).collect::<String>();
                assert_eq!(msg.source.unwrap(), uuri_cloned);
                assert_eq!(value, data_cloned);
            } else {
                panic!("The message should be Data::Value type.");
            }
        }
        Err(ustatus) => println!("Internal Error: {:?}", ustatus),
    };
    let listener_string = ulinkzenoh
        .register_listener(uuri.clone(), Box::new(listener))
        .await
        .unwrap();

    // Create uattributes
    let attributes = UAttributesBuilder::publish(UPriority::UpriorityCs4).build();

    // Publish the data
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UpayloadFormatText as i32,
        data: Some(Data::Value(target_data.as_bytes().to_vec())),
    };
    ulinkzenoh
        .send(uuri.clone(), payload, attributes.clone())
        .await
        .unwrap();

    // Waiting for the subscriber to receive data
    task::sleep(time::Duration::from_millis(1000)).await;

    // Cleanup
    ulinkzenoh
        .unregister_listener(uuri.clone(), &listener_string)
        .await
        .unwrap();
}

#[async_std::test]
async fn test_rpc_server_client() {
    let ulinkzenoh_client = ULinkZenoh::new(Config::default()).await.unwrap();
    let ulinkzenoh_server = Arc::new(Mutex::new(
        ULinkZenoh::new(Config::default()).await.unwrap(),
    ));
    let client_data = String::from("This is the client data");
    let server_data = String::from("This is the server data");
    let uuri = create_rpcserver_uuri();

    // setup RpcServer callback
    let ulinkzenoh_server_cloned = ulinkzenoh_server.clone();
    let server_data_cloned = server_data.clone();
    let client_data_cloned = client_data.clone();
    let callback = move |result: Result<UMessage, UStatus>| {
        match result {
            Ok(msg) => {
                let UMessage {
                    source,
                    attributes,
                    payload,
                } = msg;
                // Get the UUri
                let uuri = source.unwrap();
                // Build the payload to send back
                if let Data::Value(v) = payload.unwrap().data.unwrap() {
                    let value = v.into_iter().map(|c| c as char).collect::<String>();
                    assert_eq!(client_data_cloned, value);
                } else {
                    panic!("The message should be Data::Value type.");
                }
                let upayload = UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UpayloadFormatText as i32,
                    data: Some(Data::Value(server_data_cloned.as_bytes().to_vec())),
                };
                // Set the attributes type to Response
                let mut uattributes = attributes.unwrap();
                uattributes.set_type(UMessageType::UmessageTypeResponse);
                // Send back result
                block_on(ulinkzenoh_server_cloned.lock().unwrap().send(
                    uuri,
                    upayload,
                    uattributes,
                ))
                .unwrap();
            }
            Err(ustatus) => {
                panic!("Internal Error: {:?}", ustatus);
            }
        }
    };
    ulinkzenoh_server
        .lock()
        .unwrap()
        .register_rpc_listener(uuri.clone(), Box::new(callback))
        .await
        .unwrap();
    // Need some time for queryable to run
    task::sleep(time::Duration::from_millis(1000)).await;

    // Create uattributes
    // TODO: Check TTL (Should TTL map to Zenoh's timeout?)
    let attributes = UAttributesBuilder::request(UPriority::UpriorityCs4, uuri.clone(), 100)
        .with_reqid(UUIDv8Builder::new().build())
        .build();

    // Run RpcClient
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UpayloadFormatText as i32,
        data: Some(Data::Value(client_data.as_bytes().to_vec())),
    };
    let result = ulinkzenoh_client
        .invoke_method(uuri, payload, attributes)
        .await;

    // Process the result
    if let Data::Value(v) = result.unwrap().data.unwrap() {
        let value = v.into_iter().map(|c| c as char).collect::<String>();
        assert_eq!(server_data, value);
    } else {
        panic!("Failed to get result from invoke_method.");
    }
}
