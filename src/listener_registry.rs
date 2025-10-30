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

use std::{collections::HashMap, sync::Arc};

use protobuf::Message;
use tokio::sync::Mutex;
use tracing::{debug, enabled, info, warn, Level};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UListener, UMessage, UStatus,
};
use zenoh::{bytes::ZBytes, pubsub::Subscriber, sample::Sample, Session};

fn attachment_to_uattributes(attachment: &ZBytes) -> anyhow::Result<UAttributes> {
    if attachment.len() < 2 {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "message has no/invalid attachment",
        )
        .into());
    }

    let attachment_bytes = attachment.to_bytes();
    let ver = attachment_bytes[0];
    if ver != crate::UPROTOCOL_MAJOR_VERSION {
        let msg = format!(
            "Expected UAttributes version {} but found version {}",
            crate::UPROTOCOL_MAJOR_VERSION,
            ver
        );
        info!("{msg}");
        return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg).into());
    }
    // Get the attributes
    let uattributes = UAttributes::parse_from_bytes(&attachment_bytes[1..])?;
    // [impl->dsn~utransport-registerlistener-discard-invalid-messages~1]
    let validator = UAttributesValidators::get_validator_for_attributes(&uattributes);
    Ok(validator.validate(&uattributes).map(|()| uattributes)?)
}

// mapping of (Zenoh Key expression, Message Listener) ->  Zenoh Subscriber
type SubscriberMap = Mutex<HashMap<(String, ComparableListener), Subscriber<()>>>;

pub(crate) struct ListenerRegistry {
    subscribers: SubscriberMap,
    session: Arc<Session>,
    max_subscribers: usize,
}

impl ListenerRegistry {
    /// Creates a new registry with a given capacity.
    ///
    /// # Arguments
    ///
    /// * `zenoh_session` - The Zenoh session to use for creating subscribers.
    /// * `max_subscribers` - The maximum number of subscribers that can be registered.
    // [impl->req~utransport-registerlistener-max-listeners~1]
    pub fn new(zenoh_session: Arc<Session>, max_subscribers: usize) -> Self {
        Self {
            subscribers: Mutex::new(HashMap::new()),
            session: zenoh_session,
            max_subscribers,
        }
    }

    pub async fn register_subscriber(
        &self,
        zenoh_key: String,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // get a lock on the listener registry so that we can safely check and register the listener
        let mut locked_subscribers = self.subscribers.lock().await;
        let comparable_listener = ComparableListener::new(listener);

        // [impl->dsn~utransport-registerlistener-idempotent~1]
        if locked_subscribers.contains_key(&(zenoh_key.clone(), comparable_listener.clone())) {
            debug!("Listener already registered");
            return Ok(());
        }
        // [impl->dsn~utransport-registerlistener-error-resource-exhausted~1]
        if locked_subscribers.len() >= self.max_subscribers {
            return Err(UStatus::fail_with_code(
                UCode::RESOURCE_EXHAUSTED,
                format!(
                    "Maximum number of listeners reached: {}",
                    self.max_subscribers
                ),
            ));
        }

        let listener_to_invoke_in_callback = comparable_listener.clone();

        // Setup callback
        let callback = move |sample: Sample| {
            let listener_cloned = listener_to_invoke_in_callback.clone();
            // Get the UAttribute from Zenoh user attachment
            let Some(attachment) = sample.attachment() else {
                warn!(
                    "Ignoring Zenoh Sample without attachment [key expr: {}]",
                    sample.key_expr()
                );
                return;
            };
            let attributes = match attachment_to_uattributes(attachment) {
                Ok(attr) => attr,
                Err(e) => {
                    warn!("Unable to transform attachment to valid UAttributes: {e:?}");
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
                // Note that we are invoking the listener in a dedicated task
                // to avoid blocking the Zenoh callback thread
                // while still using Zenoh's underlying tokio runtime.
                // It is the responsibility of the listener to offload the
                // processing of the message to a dedicated tokio runtime or thread,
                // if necessary.
                // This is a deliberate design choice to let implementers of
                // `UListener` decide how to handle incoming messages and use
                // a custom tokio runtime configuration.
                tokio::spawn(async move {
                    listener_cloned.on_receive(msg).await;
                });
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
            // [impl->dsn~utransport-registerlistener-start-invoking-listeners~1]
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .await
        {
            Ok(subscriber) => {
                // [impl->dsn~utransport-registerlistener-number-of-listeners~1]
                // [impl->dsn~utransport-registerlistener-listener-reuse~1]
                locked_subscribers.insert((zenoh_key, comparable_listener), subscriber);
                Ok(())
            }
            Err(e) => {
                let msg = "Failed to register listener";
                warn!("{msg}: {e}");
                Err(UStatus::fail_with_code(UCode::INTERNAL, msg))
            }
        }
    }

    pub async fn unregister(
        &self,
        key_expr: &str,
        listener: ComparableListener,
    ) -> Result<(), UStatus> {
        // the callback registered with the Zenoh Subscriber will stop receiving messages
        // once it goes out of scope, so we can simply remove it from the map to stop
        // having the listener being invoked
        // [impl->dsn~utransport-unregisterlistener-stop-invoking-listeners~1]
        if self
            .subscribers
            .lock()
            .await
            .remove(&(key_expr.to_string(), listener.clone()))
            .is_none()
        {
            // [impl->dsn~utransport-unregisterlistener-error-notfound~1]
            return Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                format!("No such listener registered for key expression: {key_expr}",),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use up_rust::{MockUListener, UMessageType, UUri, UUID};

    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-registerlistener-idempotent~1]
    async fn test_register_ignores_duplicate_registration() {
        let session = zenoh::open(zenoh::config::Config::default())
            .await
            .expect("Failed to open Zenoh session");
        let registry = ListenerRegistry::new(Arc::new(session), 10);
        let listener = Arc::new(MockUListener::new());

        assert!(registry
            .register_subscriber("key1".to_string(), listener.clone())
            .await
            .is_ok());
        // Registering the same listener again should not fail
        assert!(registry
            .register_subscriber("key1".to_string(), listener.clone())
            .await
            .is_ok());

        {
            let locked_subscribers = registry.subscribers.lock().await;
            assert!(locked_subscribers
                .contains_key(&("key1".to_string(), ComparableListener::new(listener))));
        }
    }

    #[test_case::test_case(
        UAttributes {
            type_: UMessageType::UMESSAGE_TYPE_PUBLISH.into(),
            id: Some(UUID::build()).into(),
            source: Some(UUri::try_from_parts("source", 0xAA1, 0x01, 0x9000).expect("failed to create source")).into(),
            ..Default::default()
        } => true;
        "valid PUBLISH attributes"
    )]
    #[test_case::test_case(
        UAttributes {
            type_: UMessageType::UMESSAGE_TYPE_PUBLISH.into(),
            id: Some(UUID::build()).into(),
            // source is missing
            ..Default::default()
        } => false;
        "invalid PUBLISH attributes"
    )]
    #[tokio::test(flavor = "multi_thread")]
    // [utest->dsn~utransport-registerlistener-discard-invalid-messages~1]
    async fn test_attachment_to_uattributes_fails_for_invalid_attributes(
        attribs: UAttributes,
    ) -> bool {
        let attachment = crate::utransport::uattributes_to_attachment(&attribs)
            .expect("failed to create attachment from invalid UAttributes");
        attachment_to_uattributes(&attachment).is_ok()
    }
}
