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
use up_rust::{
    rpc::{CallOptionsBuilder, RpcClient},
    uprotocol::{Data, UEntity, UPayload, UPayloadFormat, UUri},
    uri::builder::resourcebuilder::UResourceBuilder,
};
use uprotocol_zenoh::UPClientZenoh;
use zenoh::config::Config;

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();

    println!("uProtocol RPC client example");
    let rpc_client = UPClientZenoh::new(Config::default()).await.unwrap();

    // create uuri
    // TODO: Need to check whether the way to create ID is correct?
    let uuri = UUri {
        entity: Some(UEntity {
            name: "test_rpc.app".to_string(),
            version_major: Some(1),
            id: Some(1234),
            ..Default::default()
        })
        .into(),
        resource: Some(UResourceBuilder::for_rpc_request(
            Some("getTime".to_string()),
            Some(5678),
        ))
        .into(),
        ..Default::default()
    };

    // TODO: Need to check whether we don't need uattributes.
    // create uattributes
    //// TODO: Check TTL (Should TTL map to Zenoh's timeout?)
    //let attributes = UAttributesBuilder::request(UPriority::UPRIORITY_CS4, uuri.clone(), 100)
    //    .with_reqid(UUIDv8Builder::new().build())
    //    .build();

    // create uPayload
    let data = String::from("GetCurrentTime");
    let payload = UPayload {
        length: Some(0),
        format: UPayloadFormat::UPAYLOAD_FORMAT_TEXT.into(),
        data: Some(Data::Value(data.as_bytes().to_vec())),
        ..Default::default()
    };

    // invoke RPC method
    println!("Send request to {}", uuri.to_string());
    let result = rpc_client
        .invoke_method(uuri, payload, CallOptionsBuilder::default().build())
        .await;

    // process the result
    if let Data::Value(v) = result.unwrap().payload.unwrap().data.unwrap() {
        let value = v.into_iter().map(|c| c as char).collect::<String>();
        println!("Receive {}", value);
    } else {
        println!("Failed to get result from invoke_method.");
    }
}
