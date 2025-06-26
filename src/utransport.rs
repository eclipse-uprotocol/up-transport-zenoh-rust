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
use std::sync::Arc;
use tracing::{debug, enabled, error, info, warn, Level};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UListener, UMessage, UStatus,
    UTransport, UUri,
};
use zenoh::sample::Sample;

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
            // [impl->dsn~supported-message-delivery-methods~1]
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
            let attributes = match UPTransportZenoh::attachment_to_uattributes(attachment) {
                Ok(attribs) => attribs,
                Err(e) => {
                    warn!("Unable to transform attachment to UAttributes: {e:?}");
                    return;
                }
            };
            // we must only process messages that have not expired yet
            // [impl->dsn~up-attributes-ttl~1]
            // [impl->dsn~up-attributes-ttl-timeout~1]
            if attributes.check_expired().is_ok() {
                // Create UMessage
                let msg = UMessage {
                    attributes: Some(attributes).into(),
                    payload: Some(sample.payload().to_bytes().to_vec().into()),
                    ..Default::default()
                };
                spawn_nonblock_callback(&listener_cloned, msg);
            } else if enabled!(Level::DEBUG) {
                if let Some(id) = attributes.id.as_ref() {
                    if let (Some(ts), Some(ttl)) = (id.get_time(), attributes.ttl) {
                        debug!(
                            "discarding expired message [id: {}, created: {ts}, ttl: {ttl}]",
                            id.to_hyphenated_string(),
                        );
                    }
                }
            }
        };

        // Create Zenoh subscriber
        // [impl->dsn~supported-message-delivery-methods~1]
        match self
            .session
            .declare_subscriber(zenoh_key)
            .callback_mut(callback)
            .await
        {
            Ok(subscriber) => {
                self.subscriber_map.lock().await.insert(
                    (zenoh_key.clone(), ComparableListener::new(listener)),
                    subscriber,
                );
                Ok(())
            }
            Err(e) => {
                let msg = "Failed to register listener";
                info!("{msg}: {e}");
                Err(UStatus::fail_with_code(UCode::INTERNAL, msg))
            }
        }
    }
}

#[async_trait]
impl UTransport for UPTransportZenoh {
    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        // [impl->dsn~utransport-receive-error-unimplemented~1]
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "not implemented",
        ))
    }

    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let attribs = message.attributes.as_ref().ok_or_else(|| {
            let msg = "Message has no attributes".to_string();
            debug!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // [impl->dsn~utransport-send-error-invalid-parameter~1]
        UAttributesValidators::get_validator_for_attributes(attribs)
            .validate(attribs)
            .map_err(|e| {
                let msg = e.to_string();
                debug!("{msg}");
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
        // [impl->dsn~utransport-registerlistener-error-unimplemented~1]
        // [impl->dsn~utransport-registerlistener-error-invalid-parameter~1]
        up_rust::verify_filter_criteria(source_filter, sink_filter)?;
        let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
        self.register_subscriber(&zenoh_key, listener).await
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // [impl->dsn~utransport-unregisterlistener-error-unimplemented~1]
        // [impl->dsn~utransport-unregisterlistener-error-invalid-parameter~1]
        up_rust::verify_filter_criteria(source_filter, sink_filter)?;
        let zenoh_key = self.to_zenoh_key_string(source_filter, sink_filter);
        // [impl->dsn~utransport-unregisterlistener-error-notfound~1]
        if self
            .subscriber_map
            .lock()
            .await
            .remove(&(zenoh_key.clone(), ComparableListener::new(listener.clone())))
            .is_none()
        {
            let msg = "No such listener registered".to_string();
            debug!("{msg}");
            return Err(UStatus::fail_with_code(UCode::NOT_FOUND, msg));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use test_case::test_case;
    use up_rust::{MockUListener, UCode, UMessageType, UUri};

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-receive-error-unimplemented~1]
    async fn test_receive_fails() {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh::config::Config::default(), "//vehicle1/AABB/7/0")
                .await
                .expect("failed to create transport");
        let source_filter = UUri::try_from_parts("vehicle2", 0xAACC, 0x01, 0x1)
            .expect("failed to create source filter");
        assert!(up_transport_zenoh
            .receive(&source_filter, None)
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::UNIMPLEMENTED)));
    }

    #[test_case(
        "//vehicle1/AA/1/0",
        Some("//vehicle2/BB/1/0");
        "source and sink both having resource ID 0")]
    #[test_case(
        "//vehicle1/AA/1/CC",
        Some("//vehicle2/BB/1/1A");
        "sink is RPC but source has invalid resource ID")]
    #[test_case(
        "//vehicle1/AA/1/CC",
        None;
        "sink is empty but source has non-topic resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-registerlistener-error-invalid-parameter~1]
    // [utest->dsn~utransport-unregisterlistener-error-invalid-parameter~1]
    async fn test_register_listener_fails_for(
        source_filter_uri: &str,
        sink_filter_uri: Option<&str>,
    ) {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh::config::Config::default(), "//vehicle2/AABB/7/0")
                .await
                .expect("failed to create transport");

        let source_filter =
            UUri::from_str(source_filter_uri).expect("failed to create source filter");
        let sink_filter =
            sink_filter_uri.map(|f| UUri::from_str(f).expect("failed to create sink filter"));

        assert!(up_transport_zenoh
            .register_listener(
                &source_filter,
                sink_filter.as_ref(),
                Arc::new(MockUListener::new())
            )
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
        assert!(up_transport_zenoh
            .unregister_listener(&source_filter, None, Arc::new(MockUListener::new()))
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
    }

    #[test_case("//src/1/1/8000", None; "Listen to Publish")]
    #[test_case("//src/1/1/8000", Some("//dst/2/1/0"); "Listen to specific Notification")]
    #[test_case("//src/1/1/0", Some("//dst/2/1/1"); "Listen to specific Request")]
    #[test_case("//src/1/1/1", Some("//dst/2/1/0"); "Listen to specific Response")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/1"); "Listen to all Requests")]
    #[test_case("//*/FFFF/FF/FFFF", Some("//dst/2/1/0"); "Listen to all Notification and Response Messages")]
    #[test_case("//src/FFFF/FF/FFFF", Some("//*/FFFF/FF/FFFF"); "uStreamer case")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_and_unregister(source_filter_uri: &str, sink_filter_uri: Option<&str>) {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh::config::Config::default(), "//vehicle2/AABB/7/0")
                .await
                .expect("failed to create transport");
        let source_filter = UUri::from_str(source_filter_uri).unwrap();
        let sink_filter = sink_filter_uri.map(|f| UUri::from_str(f).unwrap());
        let foo_listener = Arc::new(MockUListener::new());

        // Register the listener
        // [utest->dsn~utransport-registerlistener-error-unimplemented~1]
        up_transport_zenoh
            .register_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
            .await
            .expect("failed to register listener");

        // Able to unregister
        // [utest->dsn~utransport-unregisterlistener-error-unimplemented~1]
        up_transport_zenoh
            .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
            .await
            .expect("failed to unregister listener");

        // Unable to unregister again
        // [utest->dsn~utransport-unregisterlistener-error-notfound~1]
        let result = up_transport_zenoh
            .unregister_listener(&source_filter, sink_filter.as_ref(), foo_listener.clone())
            .await;
        assert!(
            result.is_err_and(|err| matches!(err.get_code(), UCode::NOT_FOUND)),
            "Expected unregister to fail after already being unregistered"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-send-error-invalid-parameter~1]
    async fn test_send_fails_for_invalid_attributes() {
        let up_transport_zenoh =
            UPTransportZenoh::new(zenoh::config::Config::default(), "//vehicle2/AABB/7/0")
                .await
                .expect("failed to create transport");
        // invalid notification message, lacking sink address
        let message = UMessage {
            attributes: Some(UAttributes {
                type_: UMessageType::UMESSAGE_TYPE_NOTIFICATION.into(),
                source: Some(
                    UUri::try_from_parts("vehicle1", 0xAACC, 0x01, 0x1)
                        .expect("failed to create source filter"),
                )
                .into(),
                sink: None.into(),
                ..Default::default()
            })
            .into(),
            payload: None,
            ..Default::default()
        };
        assert!(up_transport_zenoh
            .send(message)
            .await
            .is_err_and(|err| matches!(err.get_code(), UCode::INVALID_ARGUMENT)));
    }
}
