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
use up_rust::{UMessageBuilder, UPayloadFormat, UUri};
use up_transport_zenoh::UPClientZenoh;

#[tokio::main]
async fn main() {
    // initiate logging
    UPClientZenoh::try_init_log_from_env();

    println!("uProtocol RPC client example");
    let _rpc_client = UPClientZenoh::new(common::get_zenoh_config(), String::from("rpc_client"))
        .await
        .unwrap();

    // create uuri
    let src_uuri = UUri::from_str("//rpc_client/1/1/0").unwrap();
    let sink_uuri = UUri::from_str("//rpc_server/1/1/1").unwrap();

    // create uPayload
    let data = String::from("GetCurrentTime");
    let _umsg = UMessageBuilder::request(sink_uuri.clone(), src_uuri.clone(), 1000)
        .build_with_payload(data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();

    //// invoke RPC method
    //println!("Send request from {src_uuri} to {sink_uuri}");
    //let result = rpc_client.invoke_method(sink_uuri, umsg).await;

    //// process the result
    //let payload = result.unwrap().payload.unwrap();
    //let value = payload.into_iter().map(|c| c as char).collect::<String>();
    //println!("Receive {value}");
}
