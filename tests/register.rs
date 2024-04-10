//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
pub mod test_lib;

use std::sync::Arc;

use async_trait::async_trait;
use up_rust::{UCode, UListener, UMessage, UStatus, UTransport};

struct FooListener;
#[async_trait]
impl UListener for FooListener {
    async fn on_receive(&self, _msg: UMessage) {}
    async fn on_error(&self, _err: UStatus) {}
}
#[async_std::test]
async fn test_utransport_register_and_unregister() {
    test_lib::before_test();

    // Initialization
    let upclient = test_lib::create_up_client_zenoh().await.unwrap();
    let uuri = test_lib::create_utransport_uuri(0);
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
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Publish listener doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_rpcserver_register_and_unregister() {
    test_lib::before_test();

    // Initialization
    let upclient = test_lib::create_up_client_zenoh().await.unwrap();
    let uuri = test_lib::create_rpcserver_uuri();
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
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "RPC request listener doesn't exist"
        ))
    );
}

#[async_std::test]
async fn test_utransport_special_uuri_register_and_unregister() {
    test_lib::before_test();

    // Initialization
    let upclient = test_lib::create_up_client_zenoh().await.unwrap();
    let uuri = test_lib::create_special_uuri();
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
    assert_eq!(
        result,
        Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "RPC response callback doesn't exist"
        ))
    );
}
