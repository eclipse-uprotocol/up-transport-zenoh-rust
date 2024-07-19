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
mod common;

use std::str::FromStr;
use tokio::time::{sleep, Duration};
use up_rust::{UMessageBuilder, UPayloadFormat, UTransport, UUri};
use up_transport_zenoh::UPTransportZenoh;

#[tokio::main]
async fn main() {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol publisher example");
    let publisher = UPTransportZenoh::new(common::get_zenoh_config(), "publisher")
        .await
        .unwrap();

    // create uuri
    let uuri = UUri::from_str("//publisher/1/1/8001").unwrap();

    let mut cnt: u64 = 0;
    loop {
        let data = format!("{cnt}");
        let umessage = UMessageBuilder::publish(uuri.clone())
            .build_with_payload(data.clone(), UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
            .unwrap();
        println!("Publishing {data} from {uuri}...");
        publisher.send(umessage).await.unwrap();
        sleep(Duration::from_millis(1000)).await;
        cnt += 1;
    }
}
