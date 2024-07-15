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

use async_trait::async_trait;
use chrono::Utc;
use std::{str::FromStr, sync::Arc};
use tokio::{
    runtime::Handle,
    task,
    time::{sleep, Duration},
};
use up_rust::{UListener, UMessage, UMessageBuilder, UPayloadFormat, UTransport, UUri};
use up_transport_zenoh::UPClientZenoh;

struct RpcListener {
    up_client: Arc<UPClientZenoh>,
}
impl RpcListener {
    fn new(up_client: Arc<UPClientZenoh>) -> Self {
        RpcListener { up_client }
    }
}
#[async_trait]
impl UListener for RpcListener {
    async fn on_receive(&self, msg: UMessage) {
        let UMessage {
            attributes,
            payload,
            ..
        } = msg;

        // Build the payload to send back
        let value = payload
            .unwrap()
            .into_iter()
            .map(|c| c as char)
            .collect::<String>();
        let source = attributes.clone().unwrap().source.unwrap();
        let sink = attributes.clone().unwrap().sink.unwrap();
        println!("Receive {value} from {source} to {sink}");

        // Send back result
        let umessage = UMessageBuilder::response_for_request(&attributes)
            .build_with_payload(
                // Get current time
                format!("{}", Utc::now()),
                UPayloadFormat::UPAYLOAD_FORMAT_TEXT,
            )
            .unwrap();
        task::block_in_place(|| {
            Handle::current()
                .block_on(self.up_client.send(umessage))
                .unwrap();
        });
    }
}

#[tokio::main]
async fn main() {
    // initiate logging
    UPClientZenoh::try_init_log_from_env();

    println!("uProtocol RPC server example");
    let rpc_server = Arc::new(
        UPClientZenoh::new(common::get_zenoh_config(), String::from("rpc_server"))
            .await
            .unwrap(),
    );

    // create uuri
    let src_uuri = UUri::from_str("//*/FFFF/FF/FFFF").unwrap();
    let sink_uuri = UUri::from_str("//rpc_server/1/1/1").unwrap();

    println!("Register the listener...");
    rpc_server
        .register_listener(
            &src_uuri,
            Some(&sink_uuri),
            Arc::new(RpcListener::new(rpc_server.clone())),
        )
        .await
        .unwrap();

    loop {
        sleep(Duration::from_millis(1000)).await;
    }
}
