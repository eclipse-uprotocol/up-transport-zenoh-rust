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
use crate::{UPTransportZenoh, CB_RUNTIME};
use async_trait::async_trait;
use bytes::Bytes;
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use tracing::{error, warn};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UListener, UMessage, UStatus,
    UTransport, UUri,
};
use zenoh::sample::Sample;

lazy_static! {
    static ref TOKIO_RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new().unwrap());
}

#[inline]
fn spawn_nonblock_callback(listener: &Arc<dyn UListener>, listener_msg: UMessage) {
    let listener = listener.clone();
    CB_RUNTIME.spawn(async move {
        listener.on_receive(listener_msg).await;
    });
}

impl UPTransportZenoh {
    async fn put_message(
        &self,
        zenoh_key: &str,
        payload: Bytes,
        attributes: &UAttributes,
    ) -> Result<(), UStatus> {
        // Transform UAttributes to user attachment in Zenoh
        let attachment = UPTransportZenoh::uattributes_to_attachment(attributes).map_err(|e| {
            let msg = format!("Unable to transform UAttributes to attachment: {e}");
            error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // Map the priority to Zenoh
        // [impl->dsn~up-transport-zenoh-message-priority-mapping~1]
        let priority =
            UPTransportZenoh::map_zenoh_priority(attributes.priority.enum_value_or_default());

        // Send data
        self.session
            // [impl->dsn~up-transport-zenoh-message-type-mapping~1]
            // [impl->dsn~up-transport-zenoh-payload-mapping~1]
            .put(zenoh_key, payload)
            .priority(priority)
            // [impl->dsn~up-transport-zenoh-attributes-mapping~1]
            .attachment(attachment)
            .await
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("failed to put Zenoh message: {e}"),
                )
            })?;

        Ok(())
    }

    async fn register_subscriber(
        &self,
        zenoh_key: &String,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // Setup callback
        let listener_cloned = listener.clone();
        let callback = move |sample: Sample| {
            // Get the UAttribute from Zenoh user attachment
            let Some(attachment) = sample.attachment() else {
                warn!("Unable to get attachment");
                return;
            };
            let u_attribute = match UPTransportZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    warn!("Unable to transform attachement to UAttributes: {e:?}");
                    return;
                }
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute).into(),
                payload: Some(sample.payload().to_bytes().to_vec().into()),
                ..Default::default()
            };
            spawn_nonblock_callback(&listener_cloned, msg);
        };

        // Create Zenoh subscriber
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(zenoh_key)
            .callback_mut(callback)
            .await
        {
            self.subscriber_map.lock().unwrap().insert(
                (zenoh_key.clone(), ComparableListener::new(listener)),
                subscriber,
            );
        } else {
            let msg = "Unable to register callback with Zenoh";
            error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(())
    }
}

#[async_trait]
impl UTransport for UPTransportZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let attribs = message.attributes.as_ref().ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        UAttributesValidators::get_validator_for_attributes(attribs)
            .validate(attribs)
            .map_err(|e| {
                let msg = e.to_string();
                error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;

        // Get Zenoh key
        // [impl->dsn~up-transport-zenoh-key-expr~1]
        let zenoh_key =
            self.to_zenoh_key_string(attribs.source.get_or_default(), attribs.sink.as_ref());

        // Get payload
        let payload = if let Some(payload) = message.payload {
            payload
        } else {
            Bytes::new()
        };

        self.put_message(&zenoh_key, payload, attribs).await
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
        self.register_subscriber(&zenoh_key, listener.clone()).await
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
        if self
            .subscriber_map
            .lock()
            .unwrap()
            .remove(&(zenoh_key.clone(), ComparableListener::new(listener.clone())))
            .is_none()
        {
            let msg = "Publish / Notifcation listener doesn't exist".to_string();
            warn!("{msg}");
            return Err(UStatus::fail_with_code(UCode::NOT_FOUND, msg));
        }
        Ok(())
    }
}
