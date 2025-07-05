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
mod test_lib;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use std::sync::Arc;
use tokio::{
    sync::mpsc::Sender,
    time::{sleep, Duration},
};
use up_rust::{UListener, UMessage, UMessageBuilder, UPayloadFormat, UTransport, UUri, UUID};

struct DelayListener(Sender<UUID>);

#[async_trait]
impl UListener for DelayListener {
    async fn on_receive(&self, msg: UMessage) {
        let msg_id = msg.id_unchecked().to_owned();
        if let Some(mut pl) = msg.payload {
            if !pl.is_empty() {
                let delay_millis = pl.get_u32();
                if delay_millis > 0 {
                    sleep(Duration::from_millis(u64::from(delay_millis))).await;
                }
            }
        }
        self.0.send(msg_id).await.expect("failed to send response");
    }
}

// The test is used to check whether blocking user callback will affect receiving messages
#[tokio::test(flavor = "multi_thread")]
async fn test_blocking_user_callback() {
    test_lib::before_test();

    // create subscriber
    let topic = UUri::try_from_parts("vehicle", 0xaa0, 1, 0x8500).expect("invalid topic");
    let (tx, mut rx) = tokio::sync::mpsc::channel(5);
    let transport = test_lib::create_up_transport_zenoh(topic.authority_name().as_str())
        .await
        .expect("failed to create transport");
    transport
        .register_listener(&topic, None, Arc::new(DelayListener(tx)))
        .await
        .expect("failed to register listener");

    // send first message with a payload that instructs the listener to
    // wait for 2 secs before acknowledging the reception of the message
    let mut buf = vec![];
    buf.put_u32(1000);
    let msg0_id = UUID::build();
    let msg0 = UMessageBuilder::publish(topic.clone())
        .with_message_id(msg0_id.clone())
        .build_with_payload(buf, UPayloadFormat::UPAYLOAD_FORMAT_RAW)
        .expect("failed to create message");
    transport.send(msg0).await.expect("failed to send message");

    // send second message which will be acknowledged immediately
    let msg1_id = UUID::build();
    let msg1 = UMessageBuilder::publish(topic.clone())
        .with_message_id(msg1_id.clone())
        .build()
        .expect("failed to create message");
    transport.send(msg1).await.expect("failed to send message");

    let request_uuids = tokio::time::timeout(Duration::from_secs(5), async {
        let uuid1 = rx.recv().await;
        let uuid2 = rx.recv().await;
        (uuid1, uuid2)
    })
    .await
    .expect("did not receive request UUIDs in time");

    // make sure that second request has been acked first
    assert!(
        request_uuids.0.is_some_and(|v| v == msg1_id)
            && request_uuids.1.is_some_and(|v| v == msg0_id)
    );
}
