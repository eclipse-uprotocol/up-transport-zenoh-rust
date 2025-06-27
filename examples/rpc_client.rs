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
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Notify;
use up_rust::{
    LocalUriProvider, UListener, UMessage, UMessageBuilder, UPayloadFormat, UTransport, UUri,
};
use up_transport_zenoh::UPTransportZenoh;

const DEFAULT_TIMEOUT: u32 = 1000;

// ResponseListener
struct ResponseListener {
    notify: Arc<Notify>,
}
impl ResponseListener {
    fn new(notify: Arc<Notify>) -> Self {
        Self { notify }
    }
}
#[async_trait]
impl UListener for ResponseListener {
    async fn on_receive(&self, msg: UMessage) {
        let payload = msg.payload.unwrap();
        let value = payload.into_iter().map(|c| c as char).collect::<String>();
        let uri = msg.attributes.unwrap().source.unwrap().to_string();
        println!("Receiving response '{value}' from {uri}");
        self.notify.notify_one();
    }
}

#[tokio::main]
async fn main() {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol RPC client example");
    let rpc_client = UPTransportZenoh::new(common::get_zenoh_config(), "//rpc_client/1/1/0", 10)
        .await
        .unwrap();

    // create uuri
    let src_uuri = rpc_client.get_source_uri();
    let sink_uuri = UUri::from_str("//rpc_server/1/1/1").unwrap();

    // register response callback
    let notify = Arc::new(Notify::new());
    let resp_listener = Arc::new(ResponseListener::new(notify.clone()));
    rpc_client
        .register_listener(&sink_uuri, Some(&src_uuri), resp_listener.clone())
        .await
        .unwrap();

    // create uPayload and send request
    let data = String::from("GetCurrentTime");
    let umsg = UMessageBuilder::request(sink_uuri.clone(), src_uuri.clone(), DEFAULT_TIMEOUT)
        .build_with_payload(data, UPayloadFormat::UPAYLOAD_FORMAT_TEXT)
        .unwrap();
    println!("Sending request from {src_uuri} to {sink_uuri}");
    rpc_client.send(umsg).await.unwrap();

    match tokio::time::timeout(
        Duration::from_millis(u64::from(DEFAULT_TIMEOUT)),
        notify.notified(),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => println!("Failed to receive the reply"),
    };
}
