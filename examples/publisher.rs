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
use uprotocol_sdk::{
    transport::builder::UAttributesBuilder,
    transport::datamodel::UTransport,
    uprotocol::{Data, UEntity, UPayload, UPayloadFormat, UPriority, UResource, UUri},
};
use uprotocol_zenoh_rust::ULinkZenoh;
use zenoh::config::Config;

#[async_std::main]
async fn main() {
    println!("uProtocol publisher example");
    let publisher = ULinkZenoh::new(Config::default()).await.unwrap();

    // create uuri
    // TODO: Need to check whether the way to create ID is correct?
    let uuri = UUri {
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
    };

    // create uattributes
    let attributes = UAttributesBuilder::publish(UPriority::UpriorityCs4).build();

    let mut cnt: u64 = 0;
    loop {
        let data = format!("{}", cnt);
        let payload = UPayload {
            length: Some(0),
            format: UPayloadFormat::UpayloadFormatText as i32,
            data: Some(Data::Value(data.as_bytes().to_vec())),
        };
        println!("Sending {} to {}...", data, uuri.to_string());
        publisher
            .send(uuri.clone(), payload, attributes.clone())
            .await
            .unwrap();
        task::sleep(time::Duration::from_millis(1000)).await;
        cnt += 1;
    }
}
