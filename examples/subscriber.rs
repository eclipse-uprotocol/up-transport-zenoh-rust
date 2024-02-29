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
use up_rust::{
    transport::datamodel::UTransport,
    uprotocol::{Data, UEntity, UMessage, UResource, UStatus, UUri},
};
use uprotocol_zenoh::UPClientZenoh;
use zenoh::config::Config;

fn callback(result: Result<UMessage, UStatus>) {
    match result {
        Ok(msg) => {
            let uri = msg.attributes.unwrap().source.unwrap().to_string();
            if let Data::Value(v) = msg.payload.unwrap().data.unwrap() {
                let value = v.into_iter().map(|c| c as char).collect::<String>();
                println!("Receiving {} from {}", value, uri);
            }
        }
        Err(ustatus) => println!("Internal Error: {:?}", ustatus),
    }
}

#[async_std::main]
async fn main() {
    // initiate logging
    env_logger::init();

    println!("uProtocol subscriber example");
    let subscriber = UPClientZenoh::new(Config::default()).await.unwrap();

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

    println!("Register the listener...");
    subscriber
        .register_listener(uuri, Box::new(callback))
        .await
        .unwrap();

    loop {
        task::sleep(time::Duration::from_millis(1000)).await;
    }
}
