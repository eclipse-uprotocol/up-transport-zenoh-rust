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
use async_std::task;
use std::time;
use up_client_zenoh::UPClientZenoh;
use up_rust::{
    transport::{builder::UMessageBuilder, datamodel::UTransport},
    uprotocol::{UEntity, UPayloadFormat, UResource, UUri},
};
use zenoh::config::Config;

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();

    println!("uProtocol publisher example");
    let publisher = UPClientZenoh::new(Config::default()).await.unwrap();

    // create uuri
    // TODO: Need to check whether the way to create ID is correct?
    let uuri = UUri {
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
    };

    let mut cnt: u64 = 0;
    loop {
        let data = format!("{cnt}");
        let umessage = UMessageBuilder::publish(&uuri)
            .build_with_payload(
                publisher.get_uuid_builder(),
                data.as_bytes().to_vec().into(),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        println!("Sending {data} to {uuri}...");
        publisher.send(umessage).await.unwrap();
        task::sleep(time::Duration::from_millis(1000)).await;
        cnt += 1;
    }
}
