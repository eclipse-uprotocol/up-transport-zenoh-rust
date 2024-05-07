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
use crate::{CallbackChannelMessage, UPClientZenoh};
use async_trait::async_trait;
use crossbeam_channel::Sender;
use std::{sync::Arc, time::Duration};
use tokio::{runtime::Handle, task};
use up_rust::{
    ComparableListener, Data, UAttributes, UAttributesValidators, UCode, UListener, UMessage,
    UMessageType, UPayload, UPayloadFormat, UStatus, UTransport, UUri, UriValidator,
};
use zenoh::{
    prelude::{r#async::*, Sample},
    query::Reply,
    queryable::Query,
};

#[inline]
fn error_callback(
    sender: &Sender<CallbackChannelMessage>,
    listener: &Arc<dyn UListener>,
    msg: &str,
) {
    log::error!("{msg}");
    if sender
        .send(CallbackChannelMessage {
            listener: listener.clone(),
            result: Err(UStatus::fail_with_code(UCode::INTERNAL, msg)),
        })
        .is_err()
    {
        log::error!("Unable to call the user callback");
    }
}

#[inline]
fn success_callback(
    sender: &Sender<CallbackChannelMessage>,
    listener: &Arc<dyn UListener>,
    msg: UMessage,
) {
    if sender
        .send(CallbackChannelMessage {
            listener: listener.clone(),
            result: Ok(msg),
        })
        .is_err()
    {
        log::error!("Unable to call the user callback");
    }
}

impl UPClientZenoh {
    async fn send_publish_notification(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Map the priority to Zenoh
        let priority =
            UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().map_err(|_| {
                let msg = "Unable to map to Zenoh priority".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?);

        // Send data
        let putbuilder = self
            .session
            .put(zenoh_key, buf)
            .encoding(Encoding::WithSuffix(
                KnownEncoding::AppCustom,
                payload.format.value().to_string().into(),
            ))
            .priority(priority)
            .with_attachment(attachment.build());
        putbuilder
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to send with Zenoh"))?;

        Ok(())
    }

    // TODO: Refactor the code
    #[allow(clippy::too_many_lines)]
    async fn send_request(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Retrieve the callback
        let source_uuri = *attributes.source.0.ok_or_else(|| {
            let msg = "Lack of source address".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        let resp_callback = self
            .rpc_callback_map
            .lock()
            .unwrap()
            .get(&source_uuri)
            .ok_or_else(|| {
                let msg = "Unable to get callback".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();
        let zenoh_callback = move |reply: Reply| {
            match reply.sample {
                Ok(sample) => {
                    // Get the encoding of UPayload
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                        let msg = "Unable to get the encoding".to_string();
                        log::error!("{msg}");
                        task::block_in_place(|| {
                            Handle::current().block_on(
                                resp_callback
                                    .on_error(UStatus::fail_with_code(UCode::INTERNAL, msg)),
                            );
                        });
                        return;
                    };
                    // Get UAttribute from the attachment
                    let Some(attachment) = sample.attachment() else {
                        let msg = "Unable to get the attachment".to_string();
                        log::error!("{msg}");
                        task::block_in_place(|| {
                            Handle::current().block_on(
                                resp_callback
                                    .on_error(UStatus::fail_with_code(UCode::INTERNAL, msg)),
                            );
                        });
                        return;
                    };
                    let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                        Ok(uattr) => uattr,
                        Err(e) => {
                            let msg = format!("Transform attachment to UAttributes failed: {e:?}");
                            log::error!("{msg}");
                            task::block_in_place(|| {
                                Handle::current().block_on(
                                    resp_callback
                                        .on_error(UStatus::fail_with_code(UCode::INTERNAL, msg)),
                                );
                            });
                            return;
                        }
                    };
                    // Create UMessage
                    task::block_in_place(|| {
                        Handle::current().block_on(
                            resp_callback.on_receive(UMessage {
                                attributes: Some(u_attribute).into(),
                                payload: Some(UPayload {
                                    length: Some(0),
                                    format: encoding.into(),
                                    data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                                    ..Default::default()
                                })
                                .into(),
                                ..Default::default()
                            }),
                        );
                    });
                }
                Err(e) => {
                    let msg = format!("Error while parsing Zenoh reply: {e:?}");
                    log::error!("{msg}");
                    task::block_in_place(|| {
                        Handle::current().block_on(
                            resp_callback.on_error(UStatus::fail_with_code(UCode::INTERNAL, msg)),
                        );
                    });
                }
            }
        };

        // Send query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let getbuilder = self
            .session
            .get(zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(
                attributes.ttl.unwrap_or(1000),
            )))
            .callback(zenoh_callback);
        getbuilder.res().await.map_err(|e| {
            let msg = format!("Unable to send get with Zenoh: {e:?}");
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INTERNAL, msg)
        })?;

        Ok(())
    }

    async fn send_response(
        &self,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Transform UAttributes to user attachment in Zenoh
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to attachment".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
        };

        // Find out the corresponding query from HashMap
        let reqid = attributes.reqid.to_string();
        let query = self
            .query_map
            .lock()
            .unwrap()
            .remove(&reqid)
            .ok_or_else(|| {
                let msg = "query doesn't exist".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .clone();

        // Send back the query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let reply = Ok(Sample::new(query.key_expr().clone(), value));
        query
            .reply(reply)
            .with_attachment(attachment.build())
            .map_err(|_| {
                let msg = "Unable to add attachment";
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?
            .res()
            .await
            .map_err(|e| {
                let msg = format!("Unable to reply with Zenoh: {e:?}");
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INTERNAL, msg)
            })?;

        Ok(())
    }

    async fn register_publish_notification_listener(
        &self,
        topic: &UUri,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Setup callback
        let listener_cloned = listener.clone();
        let cb_sender = self.cb_sender.clone();
        let callback = move |sample: Sample| {
            // Get the UAttribute from Zenoh user attachment
            let Some(attachment) = sample.attachment() else {
                error_callback(&cb_sender, &listener_cloned, "Unable to get attachment");
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    error_callback(
                        &cb_sender,
                        &listener_cloned,
                        &format!("Unable to transform attachment to UAttributes: {e:?}"),
                    );
                    return;
                }
            };
            // Create UPayload
            let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                error_callback(
                    &cb_sender,
                    &listener_cloned,
                    "Unable to get payload encoding",
                );
                return;
            };
            let u_payload = UPayload {
                length: Some(0),
                format: encoding.into(),
                data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                ..Default::default()
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            success_callback(&cb_sender, &listener_cloned, msg);
        };

        // Create Zenoh subscriber
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.subscriber_map.lock().unwrap().insert(
                (topic.clone(), ComparableListener::new(listener)),
                subscriber,
            );
        } else {
            let msg = "Unable to register callback with Zenoh";
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(())
    }

    async fn register_request_listener(
        &self,
        topic: &UUri,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Setup callback
        let listener_cloned = listener.clone();
        let query_map = self.query_map.clone();
        let cb_sender = self.cb_sender.clone();
        let callback = move |query: Query| {
            // Create UAttribute from Zenoh user attachment
            let Some(attachment) = query.attachment() else {
                error_callback(&cb_sender, &listener_cloned, "Unable to get attachment");
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    error_callback(
                        &cb_sender,
                        &listener_cloned,
                        &format!("Unable to transform user attachment to UAttributes: {e:?}"),
                    );
                    return;
                }
            };
            // Create UPayload from Zenoh data
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&value.encoding) else {
                        error_callback(
                            &cb_sender,
                            &listener_cloned,
                            "Unable to get payload encoding",
                        );
                        return;
                    };
                    UPayload {
                        length: Some(0),
                        format: encoding.into(),
                        data: Some(Data::Value(value.payload.contiguous().to_vec())),
                        ..Default::default()
                    }
                }
                None => UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UPAYLOAD_FORMAT_UNSPECIFIED.into(),
                    data: None,
                    ..Default::default()
                },
            };
            // Create UMessage and store the query into HashMap (Will be used in send_response)
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.id.to_string(), query);
            success_callback(&cb_sender, &listener_cloned, msg);
        };

        // Create Zenoh queryable
        if let Ok(queryable) = self
            .session
            .declare_queryable(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.queryable_map.lock().unwrap().insert(
                (topic.clone(), ComparableListener::new(listener)),
                queryable,
            );
        } else {
            let msg = "Unable to register callback with Zenoh".to_string();
            log::error!("{msg}");
            return Err(UStatus::fail_with_code(UCode::INTERNAL, msg));
        }

        Ok(())
    }

    fn register_response_listener(&self, topic: &UUri, listener: Arc<dyn UListener>) {
        // Store the response callback (Will be used in send_request)
        self.rpc_callback_map
            .lock()
            .unwrap()
            .insert(topic.clone(), listener);
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let payload = *message.payload.0.ok_or_else(|| {
            let msg = "Invalid UPayload".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;
        let attributes = *message.attributes.0.ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            log::error!("{msg}");
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
        })?;

        // Check the type of UAttributes (Publish / Notification / Request / Response)
        match attributes
            .type_
            .enum_value()
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type"))?
        {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                UAttributesValidators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Publish UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key: Publication => source is Zenoh key
                let topic = attributes.clone().source;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish_notification(&zenoh_key, payload, attributes)
                    .await
            }
            UMessageType::UMESSAGE_TYPE_NOTIFICATION => {
                UAttributesValidators::Notification
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Notification UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key: Notification => sink is Zenoh key
                let topic = attributes.clone().sink;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish_notification(&zenoh_key, payload, attributes)
                    .await
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                UAttributesValidators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Request UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Get Zenoh key: Request => sink is Zenoh key
                let topic = attributes.clone().sink;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Request
                self.send_request(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                UAttributesValidators::Response
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        let msg = format!("Wrong Response UAttributes: {e:?}");
                        log::error!("{msg}");
                        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
                    })?;
                // Send Response
                self.send_response(payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => {
                let msg = "Wrong Message type in UAttributes".to_string();
                log::error!("{msg}");
                Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg))
            }
        }
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        let msg = "Not implemented".to_string();
        log::error!("{msg}");
        Err(UStatus::fail_with_code(UCode::UNIMPLEMENTED, msg))
    }

    async fn register_listener(
        &self,
        topic: UUri,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to register for all of Publish, Notification, Request, and Response
            // RPC response
            self.register_response_listener(&topic, listener.clone());
            // RPC request
            self.register_request_listener(&topic, listener.clone())
                .await?;
            // Publish & Notification
            self.register_publish_notification_listener(&topic, listener.clone())
                .await?;
            Ok(())
        } else {
            // Do the validation
            UriValidator::validate(&topic)
                .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

            if UriValidator::is_rpc_response(&topic) {
                // RPC response
                self.register_response_listener(&topic, listener.clone());
                Ok(())
            } else if UriValidator::is_rpc_method(&topic) {
                // RPC request
                self.register_request_listener(&topic, listener.clone())
                    .await
            } else {
                // Publish & Notification
                self.register_publish_notification_listener(&topic, listener.clone())
                    .await
            }
        }
    }

    async fn unregister_listener(
        &self,
        topic: UUri,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        let mut remove_pub_listener = false;
        let mut remove_req_listener = false;
        let mut remove_resp_listener = false;

        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to unregister all listeners
            remove_pub_listener = true;
            remove_req_listener = true;
            remove_resp_listener = true;
        } else {
            // Do the validation
            UriValidator::validate(&topic).map_err(|_| {
                let msg = "Invalid topic".to_string();
                log::error!("{msg}");
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg)
            })?;

            if UriValidator::is_rpc_response(&topic) {
                remove_resp_listener = true;
            } else if UriValidator::is_rpc_method(&topic) {
                remove_req_listener = true;
            } else {
                remove_pub_listener = true;
            }
        }
        if remove_resp_listener {
            // RPC response
            if self
                .rpc_callback_map
                .lock()
                .unwrap()
                .remove(&topic)
                .is_none()
            {
                let msg = "RPC response callback doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        if remove_req_listener {
            // RPC request
            if self
                .queryable_map
                .lock()
                .unwrap()
                .remove(&(topic.clone(), ComparableListener::new(listener.clone())))
                .is_none()
            {
                let msg = "RPC request listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        if remove_pub_listener {
            // Normal publish
            if self
                .subscriber_map
                .lock()
                .unwrap()
                .remove(&(topic.clone(), ComparableListener::new(listener.clone())))
                .is_none()
            {
                let msg = "Publish listener doesn't exist".to_string();
                log::warn!("{msg}");
                return Err(UStatus::fail_with_code(UCode::INVALID_ARGUMENT, msg));
            }
        }
        Ok(())
    }
}
