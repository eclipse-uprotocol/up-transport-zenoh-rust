/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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

use std::{str::FromStr, sync::Arc, time::Duration};

use tokio::sync::Notify;
use up_rust::{MockUListener, UMessage, UMessageBuilder, UStatus, UTransport, UUri, UUID};
use up_transport_zenoh::UPTransportZenoh;

mod test_lib;

const PEER_CONFIG: &str = r#"
{
    mode: "peer",
    listen: {
        endpoints: {
            peer: ["tcp/127.0.0.1:15000"]
        }
    },
    scouting: {
        multicast: {
            enabled: false
        },
        gossip: {
            enabled: false
        },
    },
    transport: {
        auth: {
            usrpwd: {
                dictionary_file: "tests/users.txt"
            }
        }
    }
}"#;

const PEER_ACL_CONFIG: &str = r#"
{
    "enabled": true,
    "default_permission": "deny",
    "rules": [
        {
            "id": "allowed_topics",
            "permission": "allow",
            "flows": ["ingress"],
            "messages": ["put"],
            "key_exprs": ["up/client/ABCD/0/1/A000/{}/{}/{}/{}/{}"]
        },
        {
            "id": "allowed_subscriber_declarations",
            "permission": "allow",
            "flows": ["ingress"],
            "messages": ["declare_subscriber"],
            "key_exprs": ["up/*/*/*/*/*/service_provider/ABCD/0/1/**"]
        },
        {
            "id": "allowed_rpc_requests",
            "permission": "allow",
            "flows": ["ingress", "egress"],
            "messages": ["put"],
            "key_exprs": ["up/*/*/*/*/0/service_provider/ABCD/0/1/**"]
        },
        {
            "id": "allowed_rpc_responses",
            "permission": "allow",
            "flows": ["ingress"],
            "messages": ["put"],
            "key_exprs": ["up/service_provider/ABCD/0/1/*/*/*/*/*/0"]
        }
    ],
    "subjects": [
        {
            "id": "authorized_clients",
            "usernames": ["uprotocol"],
        }
    ],
    "policies": [
        {
            "subjects": ["authorized_clients"],
            "rules": [
                "allowed_topics",
                "allowed_subscriber_declarations",
                "allowed_rpc_requests",
                "allowed_rpc_responses"
            ]
        }
    ]
}
"#;

const CLIENT_CONFIG: &str = r#"
{
    mode: "client",
    connect: {
        endpoints: [
            "tcp/127.0.0.1:15000"
        ]
    },
    scouting: {
        multicast: {
            enabled: false
        },
        gossip: {
            enabled: false
        },
    }
}
"#;

const CLIENT_CREDENTIALS_AUTHORIZED: &str = r#"
{
    "usrpwd": {
        "user": "uprotocol",
        "password": "zenoh"
    }
}
"#;
const CLIENT_CREDENTIALS_UNAUTHORIZED: &str = r#"
{
    "usrpwd": {
        "user": "attacker",
        "password": "youwontknow"
    }
}
"#;
const AUTHORIZED_TOPIC: &str = "//client/ABCD/1/A000";
const UNAUTHORIZED_TOPIC: &str = "//client/ABCD/1/BBBB";

async fn create_peer_transport() -> Result<UPTransportZenoh, UStatus> {
    let mut peer_config = zenoh::config::Config::from_json5(PEER_CONFIG)
        .expect("Failed to load zenoh config from file");
    peer_config
        .insert_json5("access_control", PEER_ACL_CONFIG)
        .expect("failed to set access control config");

    test_lib::create_up_transport_zenoh("peer", Some(peer_config)).await
}

#[test_case::test_case(
    UMessageBuilder::publish(UUri::from_str(AUTHORIZED_TOPIC)
        .expect("invalid topic"))
        .build()
        .expect("Failed to build message"),
        CLIENT_CREDENTIALS_AUTHORIZED => true;
        "succeeds for authorized topic and client")]
#[test_case::test_case(
    UMessageBuilder::request(
        UUri::from_str("//service_provider/ABCD/1/7100").expect("invalid method-to-invoke"),
        UUri::from_str("//client/BBBB/1/0").expect("invalid reply-to address"),
        5000
    )
    .build()
    .expect("Failed to build message"),
    CLIENT_CREDENTIALS_AUTHORIZED => true;
    "succeeds for authorized method-to-invoke and client")]
#[test_case::test_case(
    UMessageBuilder::response(
        UUri::from_str("//client/BBBB/1/0").expect("invalid reply-to address"),
        UUID::build(),
        UUri::from_str("//service_provider/ABCD/1/7100").expect("invalid invoked-method")
    )
    .build()
    .expect("Failed to build message"),
    CLIENT_CREDENTIALS_AUTHORIZED => true;
    "succeeds for authorized invoked-method and client")]
#[test_case::test_case(
    UMessageBuilder::publish(UUri::from_str(AUTHORIZED_TOPIC).expect("invalid topic"))
        .build()
        .expect("Failed to build message"),
    CLIENT_CREDENTIALS_UNAUTHORIZED => false;
    "fails for authorized topic but unauthorized client")]
#[test_case::test_case(
    UMessageBuilder::publish(UUri::from_str(UNAUTHORIZED_TOPIC).expect("invalid topic"))
        .build()
        .expect("Failed to build message"),
    CLIENT_CREDENTIALS_AUTHORIZED => false;
    "fails for unauthorized topic but authorized client")]
#[test_case::test_case(
    UMessageBuilder::request(
        UUri::from_str("//service_provider/AAAA/2/56").expect("invalid method-to-invoke"),
        UUri::from_str("//client/BBBB/1/0").expect("invalid reply-to address"),
        5000
    )
    .build()
    .expect("Failed to build message"),
    CLIENT_CREDENTIALS_AUTHORIZED => false;
    "fails for unauthorized method-to-invoke but authorized client")]
#[test_case::test_case(
    UMessageBuilder::response(
        UUri::from_str("//client/BBBB/1/0").expect("invalid reply-to address"),
        UUID::build(),
        UUri::from_str("//service_provider/AAAA/2/56").expect("invalid invoked-method")
    )
    .build()
    .expect("Failed to build message"),
    CLIENT_CREDENTIALS_AUTHORIZED => false;
    "fails for unauthorized invoked-method but authorized client")]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
// [itest->dsn~utransport-authorization~1]
async fn test_send_message_to_peer(msg: UMessage, credentials: &str) -> bool {
    test_lib::before_test();
    let peer_transport = create_peer_transport()
        .await
        .expect("Failed to create peer transport");

    let message_received = Arc::new(Notify::new());
    let message_received_barrier = message_received.clone();
    let mut mock_listener = MockUListener::new();
    mock_listener.expect_on_receive().returning(move |_msg| {
        message_received_barrier.notify_one();
    });
    let listener = Arc::new(mock_listener);
    peer_transport
        .register_listener(&UUri::any(), Some(&UUri::any()), listener.clone())
        .await
        .expect("Failed to register listener");

    let mut client_config = zenoh::config::Config::from_json5(CLIENT_CONFIG)
        .expect("Failed to load zenoh config from file");
    client_config
        .insert_json5("transport/auth", credentials)
        .expect("failed to set auth config");

    let client_transport = UPTransportZenoh::builder("client")
        .expect("Failed to create transport builder")
        .with_config(client_config)
        .build()
        .await
        .expect("Failed to create transport");

    assert!(
        client_transport.send(msg).await.is_ok(),
        "failed to send message"
    );
    tokio::time::timeout(Duration::from_millis(1000), message_received.notified())
        .await
        .is_ok()
}

#[test_case::test_case(
    "//service_provider/ABCD/1/7100", CLIENT_CREDENTIALS_AUTHORIZED => true;
    "succeeds for authorized method-to-invoke and client")]
#[test_case::test_case(
    "//service_provider/ABCD/1/7100", CLIENT_CREDENTIALS_UNAUTHORIZED => false;
    "fails for authorized method-to-invoke but unauthorized client")]
#[test_case::test_case(
    "//other_service_provider/ABCD/1/7100", CLIENT_CREDENTIALS_AUTHORIZED => false;
    "fails for unauthorized method-to-invoke but authorized client")]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
// [itest->dsn~utransport-authorization~1]
async fn test_subscribe_for_messages_from_peer(sink_filter_uri: &str, credentials: &str) -> bool {
    test_lib::before_test();
    let sink_filter = UUri::from_str(sink_filter_uri).expect("invalid sink filter");

    let peer_transport = create_peer_transport()
        .await
        .expect("Failed to create peer transport");

    let request_received = Arc::new(Notify::new());
    let request_received_barrier = request_received.clone();
    let mut mock_rpc_request_listener = MockUListener::new();
    mock_rpc_request_listener
        .expect_on_receive()
        .returning(move |_msg| {
            request_received_barrier.notify_one();
        });
    let rpc_request_listener = Arc::new(mock_rpc_request_listener);

    let mut service_provider_config = zenoh::config::Config::from_json5(CLIENT_CONFIG)
        .expect("Failed to load zenoh service provider config from file");
    service_provider_config
        .insert_json5("transport/auth", credentials)
        .expect("failed to set auth config");
    let service_provider_transport = UPTransportZenoh::builder("service_provider")
        .expect("Failed to create service provider transport builder")
        .with_config(service_provider_config)
        .build()
        .await
        .map(Arc::new)
        .expect("Failed to create service provider transport");

    service_provider_transport
        .register_listener(&UUri::any(), Some(&sink_filter), rpc_request_listener)
        .await
        .expect("Failed to register RPC handler");

    // allow for the subscriber declaration to propagate through the network
    tokio::time::sleep(Duration::from_millis(500)).await;

    let reply_to_address =
        UUri::from_str("//peer/BBBB/1/0").expect("Failed to create reply-to address URI");
    let rpc_request = UMessageBuilder::request(sink_filter.clone(), reply_to_address, 5000)
        .build()
        .expect("Failed to build RPC request message");
    peer_transport
        .send(rpc_request)
        .await
        .expect("Failed to send RPC request");

    tokio::time::timeout(Duration::from_millis(1000), request_received.notified())
        .await
        .is_ok()
}
