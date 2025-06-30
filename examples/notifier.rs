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

use up_rust::{LocalUriProvider, UMessageBuilder, UPayloadFormat, UTransport, UUri};
use up_transport_zenoh::UPTransportZenoh;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol notifier example");
    let notifier = UPTransportZenoh::builder("//notification/1/1/0")
        .expect("invalid URI")
        .with_config(common::get_zenoh_config())
        .build()
        .await?;

    // create uuri
    let sink_uuri = UUri::from_str("//receiver/2/1/0")?;

    let data = "The notification data";
    let umessage =
        UMessageBuilder::notification(notifier.get_resource_uri(0x8001), sink_uuri.clone())
            .build_with_payload(data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;
    println!("Sending notification '{data}' to {sink_uuri}...");
    notifier.send(umessage).await.map_err(Box::from)
}
