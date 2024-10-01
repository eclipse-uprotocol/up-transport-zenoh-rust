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

use std::{str::FromStr, sync::Arc};
use up_rust::{
    communication::{CallOptions, RpcClient, UPayload},
    LocalUriProvider, UPayloadFormat, UPriority, UUri, UUID,
};
use up_transport_zenoh::{UPTransportZenoh, ZenohRpcClient};

#[tokio::main]
async fn main() {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol RPC client example");
    let zenoh_transport = Arc::new(
        UPTransportZenoh::new(common::get_zenoh_config(), "//rpc_client/1/1/0")
            .await
            .unwrap(),
    );
    let rpc_client = Arc::new(ZenohRpcClient::new(zenoh_transport.clone()));

    let sink_uuri = UUri::from_str("//rpc_server/1/1/1").unwrap();

    // create uPayload and send request
    let data = String::from("GetCurrentTime");
    let payload = UPayload::new(data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT);
    let call_options = CallOptions::for_rpc_request(
        5_000,
        Some(UUID::build()),
        Some("my_token".to_string()),
        Some(UPriority::UPRIORITY_CS6),
    );
    println!(
        "Sending request from {} to {}",
        zenoh_transport.get_source_uri(),
        sink_uuri
    );
    let result = rpc_client
        .invoke_method(sink_uuri, call_options, Some(payload))
        .await;

    // process the result
    match result {
        Ok(result) => {
            let payload = result.unwrap().payload();
            let value = payload.into_iter().map(|c| c as char).collect::<String>();
            println!("Receive {value}");
        }
        Err(_) => {
            println!("Failed to receive the reply");
        }
    }
}
