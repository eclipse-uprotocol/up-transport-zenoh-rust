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

/*!
This example illustrates how uProtocol's Transport Layer API can be used to publish
messages to a topic using the Zenoh transport.

This example works in conjunction with the `subscriber`, which should be started in
another terminal first.
*/

mod common;

use up_rust::{LocalUriProvider, StaticUriProvider, UMessageBuilder, UPayloadFormat, UTransport};
use up_transport_zenoh::UPTransportZenoh;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol publisher example");
    let uri_provider = StaticUriProvider::new("publisher", 0x3_b1da, 1);
    let transport = UPTransportZenoh::builder(uri_provider.get_authority())
        .expect("invalid authority name")
        .with_config(common::get_zenoh_config())
        .build()
        .await?;

    // create topic uuri
    let topic = uri_provider.get_resource_uri(0x8001);

    for cnt in 1..=100 {
        let data = format!("event {cnt}");
        println!(
            "Publishing message [topic: {}, payload: {data}]",
            topic.to_uri(false)
        );
        let umessage = UMessageBuilder::publish(topic.clone())
            .build_with_payload(data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;
        transport.send(umessage).await?;
        tokio::time::sleep(core::time::Duration::from_secs(1)).await;
    }
    Ok(())
}
