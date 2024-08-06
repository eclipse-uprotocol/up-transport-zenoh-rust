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
pub mod test_lib;

use async_trait::async_trait;
use std::{str::FromStr, sync::Arc};
use test_case::test_case;
use up_rust::{UListener, UMessage, UTransport, UUri};

struct FooListener;
#[async_trait]
impl UListener for FooListener {
    async fn on_receive(&self, _msg: UMessage) {}
}

#[test_case("//src/1/1/8000", None; "Listen to Publish")]
#[test_case("//src/1/1/8000", Some("//dst/2/1/0"); "Listen to specific Notification")]
#[test_case("//src/1/1/0", Some("//dst/2/1/1"); "Listen to specific Request")]
#[test_case("//src/1/1/1", Some("//dst/2/1/0"); "Listen to specific Response")]
#[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/1"); "Listen to all Requests")]
#[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/0"); "Listen to all Notification and Response Messages")]
#[test_case("//src/FFFF/FF/FFFF", Some("//*/FFFF/FF/FFFF"); "uStreamer case")]
#[tokio::test(flavor = "multi_thread")]
async fn test_register_and_unregister(source_filter: &str, sink_filter: Option<&str>) {
    test_lib::before_test();

    // Initialization
    let uptransport = test_lib::create_up_transport_zenoh("//dst/2/1/0")
        .await
        .unwrap();
    let foo_listener = Arc::new(FooListener);
    let source_filter = UUri::from_str(source_filter).unwrap();
    let sink_filter = sink_filter.map(|f| UUri::from_str(f).unwrap());

    // Register the listener
    uptransport
        .register_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
        .await
        .unwrap();

    // Able to unregister
    uptransport
        .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
        .await
        .unwrap();

    // Unable to unregister
    let result = uptransport
        .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
        .await;
    assert!(result.is_err());
}
