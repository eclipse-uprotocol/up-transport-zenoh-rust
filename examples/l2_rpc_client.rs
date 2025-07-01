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
This example illustrates how uProtocol's Communication Layer API can be used to perform
an RPC using the Zenoh transport.

In order to successfully run this example, the `rpc_server` example needs to be started
first.
*/

mod common;

use std::{str::FromStr, sync::Arc};
use tracing::{error, info};
use up_rust::{
    communication::{CallOptions, InMemoryRpcClient, RpcClient, UPayload},
    LocalUriProvider, StaticUriProvider, UPayloadFormat, UPriority, UUri, UUID,
};
use up_transport_zenoh::UPTransportZenoh;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    info!("uProtocol RPC client example");
    let uri_provider = Arc::new(StaticUriProvider::new("l2_rpc_client", 0x10_ab10, 1));
    let transport = UPTransportZenoh::builder(uri_provider.get_authority())
        .expect("invalid authority name")
        .with_config(common::get_zenoh_config())
        .build()
        .await
        .map(Arc::new)?;
    let rpc_client = InMemoryRpcClient::new(transport, uri_provider.clone())
        .await
        .map(Arc::new)?;

    let operation_uuri = UUri::from_str("//rpc_server/AAA/1/6A10")?;

    // create and send request
    let payload = UPayload::new("GetCurrentTime", UPayloadFormat::UPAYLOAD_FORMAT_TEXT);
    let call_options = CallOptions::for_rpc_request(
        5_000,
        Some(UUID::build()),
        Some("my_token".to_string()),
        Some(UPriority::UPRIORITY_CS6),
    );
    info!(
        "Sending request [source: {}, sink: {}]",
        uri_provider.get_source_uri().to_uri(false),
        operation_uuri.to_uri(false)
    );

    match rpc_client
        .invoke_method(operation_uuri, call_options, Some(payload))
        .await
    {
        Err(_) => {
            error!("Failed to receive reply from service");
        }
        Ok(Some(payload)) => {
            let value = String::from_utf8(payload.payload().to_vec())?;
            info!("Received reply [payload: {value}]");
        }
        _ => {
            error!("Reply did not contain payload");
        }
    }
    Ok(())
}
