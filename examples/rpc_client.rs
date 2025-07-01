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
This example illustrates how uProtocol's Transport Layer API can be used to perform
an RPC using the Zenoh transport.

In order to successfully run this example, the `rpc_server` example needs to be started
first.
*/

mod common;

use async_trait::async_trait;
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tracing::{error, info};
use up_rust::{
    LocalUriProvider, StaticUriProvider, UListener, UMessage, UMessageBuilder, UPayloadFormat,
    UTransport, UUri,
};
use up_transport_zenoh::UPTransportZenoh;

const REQUEST_TTL: u32 = 1000;

// ResponseListener
struct ResponseListener(Arc<Notify>);

#[async_trait]
impl UListener for ResponseListener {
    async fn on_receive(&self, msg: UMessage) {
        let payload = msg.payload.unwrap();
        let value = String::from_utf8(payload.to_vec()).unwrap();
        let uri = msg.attributes.unwrap().source.unwrap().to_uri(false);
        info!("Received RPC response [from: {uri}, payload: {value}]");
        self.0.notify_one();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    info!("uProtocol RPC client example");
    let uri_provider = StaticUriProvider::new("l1_rpc_client", 0xdd00, 2);
    let transport = UPTransportZenoh::builder(uri_provider.get_authority())
        .expect("invalid authority name")
        .with_config(common::get_zenoh_config())
        .build()
        .await?;

    // create uuri
    let operation_uuri = UUri::from_str("//rpc_server/AAA/1/6A10")?;
    let reply_to_uuri = uri_provider.get_source_uri();

    // register response callback
    let notify = Arc::new(Notify::new());
    let resp_listener = Arc::new(ResponseListener(notify.clone()));
    transport
        .register_listener(&operation_uuri, Some(&reply_to_uuri), resp_listener.clone())
        .await?;

    // create and send request message
    let request_message =
        UMessageBuilder::request(operation_uuri.clone(), reply_to_uuri.clone(), REQUEST_TTL)
            .build_with_payload("GetCurrentTime", UPayloadFormat::UPAYLOAD_FORMAT_TEXT)?;
    info!(
        "Sending RPC request [from: {}, to: {}]",
        reply_to_uuri.to_uri(false),
        operation_uuri.to_uri(false)
    );
    transport.send(request_message).await?;

    tokio::time::timeout(
        Duration::from_millis(u64::from(REQUEST_TTL * 2)),
        notify.notified(),
    )
    .await
    .map_err(|e| {
        error!("Failed to receive reply from service in time");
        Box::from(e)
    })
}
