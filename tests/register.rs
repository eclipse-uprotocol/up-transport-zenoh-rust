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

use std::sync::Arc;

use async_trait::async_trait;
use test_case::test_case;
use up_rust::{UListener, UMessage, UStatus, UTransport, UUri};

struct FooListener;
#[async_trait]
impl UListener for FooListener {
    async fn on_receive(&self, _msg: UMessage) {}
    async fn on_error(&self, _err: UStatus) {}
}

#[test_case(test_lib::create_utransport_uuri(Some(0), 0, 0); "Publish / Notification register_listener")]
#[test_case(test_lib::create_rpcserver_uuri(Some(0), 0); "RPC register_listener")]
#[test_case(test_lib::create_special_uuri(0); "Special UUri register_listener")]
#[async_std::test]
async fn test_register_and_unregister(uuri: UUri) {
    test_lib::before_test();

    // Initialization
    let upclient = test_lib::create_up_client_zenoh(0, 0).await.unwrap();
    let foo_listener = Arc::new(FooListener);

    // Register the listener
    upclient
        .register_listener(uuri.clone(), foo_listener.clone())
        .await
        .unwrap();

    // Able to ungister
    upclient
        .unregister_listener(uuri.clone(), foo_listener.clone())
        .await
        .unwrap();

    // Unable to ungister
    let result = upclient
        .unregister_listener(uuri.clone(), foo_listener.clone())
        .await;
    assert!(result.is_err());
}
