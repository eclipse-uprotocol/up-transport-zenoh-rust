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
This example illustrates how uProtocol's Transport Layer API can be used to receive
notifications sent by another uEntity using the Zenoh transport.

This example works in conjunction with the `notifier`, which should be started in
another terminal after having started this receiver.
*/

mod common;

use async_trait::async_trait;
use std::{str::FromStr, sync::Arc};
use up_rust::{LocalUriProvider, StaticUriProvider, UListener, UMessage, UTransport, UUri};
use up_transport_zenoh::UPTransportZenoh;

struct NotificationListener(tokio::runtime::Runtime);
#[async_trait]
impl UListener for NotificationListener {
    async fn on_receive(&self, msg: UMessage) {
        // Offload processing of the message to a dedicated tokio runtime using
        // threads not used by Zenoh.
        self.0.spawn(async move {
            let payload = msg.payload.unwrap();
            let value = String::from_utf8(payload.to_vec()).unwrap();
            let uri = msg.attributes.unwrap().source.unwrap().to_uri(false);
            println!("Received notification [source: {uri}, payload: {value}]");
        });
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol notification receiver example");
    let uri_provider = StaticUriProvider::new("receiver", 0x10_ab10, 1);
    let transport = UPTransportZenoh::builder(uri_provider.get_authority())
        .expect("invalid authority name")
        .with_config(common::get_zenoh_config())
        .build()
        .await?;

    let source_filter = UUri::from_str("//*/FFFFA1B2/1/8001")?;
    let sink_filter = uri_provider.get_source_uri();

    println!(
        "Registering notification listener [source filter: {}, sink filter: {}]",
        source_filter.to_uri(false),
        sink_filter.to_uri(false)
    );

    let message_processing_rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("message-processing")
        .worker_threads(1)
        .build()?;
    transport
        .register_listener(
            &source_filter,
            Some(&sink_filter),
            Arc::new(NotificationListener(message_processing_rt)),
        )
        .await?;

    tokio::signal::ctrl_c().await.map_err(Box::from)
}
