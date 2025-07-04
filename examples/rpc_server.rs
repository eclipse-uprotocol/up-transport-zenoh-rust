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
This example illustrates how uProtocol's Transport Layer API can be used to implement
an operation that can be invoked by means of an RPC using the Zenoh transport.

After having started the RPC server, the `rpc_client` and/or `l2_rpc_client` examples
can be run to invoke the operation.
*/

mod common;

use async_trait::async_trait;
use chrono::Utc;
use std::{str::FromStr, sync::Arc};
use up_rust::{
    LocalUriProvider, StaticUriProvider, UListener, UMessage, UMessageBuilder, UPayloadFormat,
    UTransport, UUri,
};
use up_transport_zenoh::UPTransportZenoh;

struct RpcListener(Arc<UPTransportZenoh>);

#[async_trait]
impl UListener for RpcListener {
    async fn on_receive(&self, msg: UMessage) {
        if let (Some(attributes), Some(payload)) = (msg.attributes.as_ref(), msg.payload) {
            let request_value = String::from_utf8(payload.to_vec()).unwrap_or("N/A".to_string());
            println!(
                "Processing request [from: {}, to: {}, payload: {request_value}]",
                attributes.source.to_uri(false),
                attributes.sink.to_uri(false)
            );

            // Send back result
            let umessage = UMessageBuilder::response_for_request(attributes)
                .build_with_payload(
                    // Get current time
                    format!("{}", Utc::now()),
                    UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
                )
                .unwrap();
            let _ = self.0.send(umessage).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol RPC server example");
    let operation_uuri = UUri::from_str("//rpc_server/AAA/1/6A10")?;
    let uri_provider = StaticUriProvider::try_from(&operation_uuri)?;
    let transport = UPTransportZenoh::builder(uri_provider.get_authority())
        .expect("invalid authority name")
        .with_config(common::get_zenoh_config())
        .build()
        .await
        .map(Arc::new)?;

    // Filter matching any RPC request
    let source_filter = UUri::from_str("//*/FFFFFFFF/FF/0")?;

    println!(
        "Registering RPC request handler [source filter: {}, sink filter: {}]",
        source_filter.to_uri(false),
        operation_uuri.to_uri(false)
    );
    transport
        .register_listener(
            &source_filter,
            Some(&operation_uuri),
            Arc::new(RpcListener(transport.clone())),
        )
        .await?;

    tokio::signal::ctrl_c().await.map_err(Box::from)
}
