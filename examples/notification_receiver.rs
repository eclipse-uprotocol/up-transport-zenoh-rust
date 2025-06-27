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
use std::{str::FromStr, sync::Arc};
use tokio::time::{sleep, Duration};
use up_rust::{LocalUriProvider, UListener, UMessage, UTransport, UUri};
use up_transport_zenoh::UPTransportZenoh;

struct SubscriberListener;
#[async_trait]
impl UListener for SubscriberListener {
    async fn on_receive(&self, msg: UMessage) {
        let payload = msg.payload.unwrap();
        let value = payload.into_iter().map(|c| c as char).collect::<String>();
        let uri = msg.attributes.unwrap().source.unwrap().to_string();
        println!("Receiving notification '{value}' from {uri}");
    }
}

#[tokio::main]
async fn main() {
    // initiate logging
    UPTransportZenoh::try_init_log_from_env();

    println!("uProtocol notification receiver example");
    let receiver = UPTransportZenoh::new(common::get_zenoh_config(), "//receiver/2/1/0", 10)
        .await
        .unwrap();

    // create uuri
    let source_uuri = UUri::from_str("//notification/1/1/8001").unwrap();

    println!("Register the listener...");
    receiver
        .register_listener(
            &source_uuri,
            Some(&receiver.get_source_uri()),
            Arc::new(SubscriberListener {}),
        )
        .await
        .unwrap();

    loop {
        sleep(Duration::from_millis(1000)).await;
    }
}
