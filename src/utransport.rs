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
use crate::UPClientZenoh;
use async_trait::async_trait;
use std::collections::hash_map::Entry;
use std::{sync::Arc, time::Duration};
use up_rust::listener_wrapper::ListenerWrapper;
use up_rust::ulistener::UListener;
use up_rust::{
    Data, UAttributes, UAttributesValidators, UCode, UMessage, UMessageType, UPayload,
    UPayloadFormat, UStatus, UTransport, UUri, UriValidator,
};
use zenoh::{
    prelude::{r#async::*, Sample},
    query::Reply,
    queryable::Query,
};

impl UPClientZenoh {
    async fn send_publish(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let buf = if let Some(Data::Value(buf)) = payload.data {
            buf
        } else {
            // TODO: Assume we only have Value here, no reference for shared memory
            vec![]
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };

        let priority =
            UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().map_err(|_| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid priority")
            })?);

        let putbuilder = self
            .session
            .put(zenoh_key, buf)
            .encoding(Encoding::WithSuffix(
                KnownEncoding::AppCustom,
                payload.format.value().to_string().into(),
            ))
            .priority(priority)
            .with_attachment(attachment.build());

        // Send data
        putbuilder
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to send with Zenoh"))?;

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn send_request(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let buf = if let Some(Data::Value(buf)) = payload.data {
            buf
        } else {
            // TODO: Assume we only have Value here, no reference for shared memory
            vec![]
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };
        let attachment = attachment.build();

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));

        let resp_listener_clone = attributes.source.0.clone().ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Lack of source address",
        ))?;

        let callbacks = {
            let rpc_callback_map = self.rpc_callback_map.lock().unwrap();
            rpc_callback_map
                .get(&resp_listener_clone)
                .map_or_else(Vec::new, |listener_set| {
                    listener_set.iter().cloned().collect::<Vec<_>>()
                })
        };

        if callbacks.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                format!("No listeners registered for topic: {resp_listener_clone}",),
            ));
        }

        for listener in callbacks {
            let zenoh_callback = move |reply: Reply| {
                let msg = match reply.sample {
                    Ok(sample) => {
                        let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding)
                        else {
                            listener.on_receive(Err(UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to get the encoding",
                            )));
                            return;
                        };
                        // TODO: Get the attributes
                        // Create UAttribute
                        let Some(attachment) = sample.attachment() else {
                            listener.on_receive(Err(UStatus::fail_with_code(
                                UCode::INTERNAL,
                                "Unable to get attachment",
                            )));
                            return;
                        };
                        let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment)
                        {
                            Ok(uattr) => uattr,
                            Err(e) => {
                                log::error!("attachment_to_uattributes error: {:?}", e);
                                listener.on_receive(Err(UStatus::fail_with_code(
                                    UCode::INTERNAL,
                                    "Unable to decode attribute",
                                )));
                                return;
                            }
                        };
                        Ok(UMessage {
                            attributes: Some(u_attribute).into(),
                            payload: Some(UPayload {
                                length: Some(0),
                                format: encoding.into(),
                                data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                                ..Default::default()
                            })
                            .into(),
                            ..Default::default()
                        })
                    }
                    Err(e) => Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        format!("Error while parsing Zenoh reply: {e:?}"),
                    )),
                };
                listener.on_receive(msg);
            };

            // TODO: Adjust the timeout
            let getbuilder = self
                .session
                .get(zenoh_key)
                .with_value(value.clone())
                .with_attachment(attachment.clone())
                .target(QueryTarget::BestMatching)
                .timeout(Duration::from_millis(1000))
                .callback(zenoh_callback);
            getbuilder.res().await.map_err(|e| {
                log::error!("Zenoh error: {e:?}");
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to send get with Zenoh")
            })?;
        }

        Ok(())
    }

    async fn send_response(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let buf = if let Some(Data::Value(buf)) = payload.data {
            buf
        } else {
            // TODO: Assume we only have Value here, no reference for shared memory
            vec![]
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            log::error!("send_response: Invalide uAttributes");
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };
        // Get reqid
        let reqid = attributes.reqid.to_string();

        // Send back query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let reply = Ok(Sample::new(
            KeyExpr::new(zenoh_key.to_string()).map_err(|e| {
                log::error!("Unable to create Zenoh key: {e:?}");
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to create Zenoh key")
            })?,
            value,
        ));
        let query = self
            .query_map
            .lock()
            .unwrap()
            .get(&reqid)
            .ok_or(UStatus::fail_with_code(
                UCode::INTERNAL,
                "query doesn't exist",
            ))?
            .clone();

        // Send data
        query
            .reply(reply)
            .with_attachment(attachment.build())
            .map_err(|_| {
                // TODO: The latest Zenoh version can print error.
                log::error!("Unable to add attachment");
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to add attachment")
            })?
            .res()
            .await
            .map_err(|e| {
                log::error!("Unable to reply with Zenoh: {e:?}");
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to reply with Zenoh: {e:?}"),
                )
            })?;

        Ok(())
    }
    async fn register_publish_listener<T>(&self, topic: &UUri, listener: T) -> Result<(), UStatus>
    where
        T: Copy + UListener + 'static,
    {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        // Setup callback
        let callback = move |sample: Sample| {
            let listener_wrapper = Arc::new(ListenerWrapper::new(listener));
            // Create UAttribute
            let Some(attachment) = sample.attachment() else {
                listener_wrapper.on_receive(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                )));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener_wrapper.on_receive(Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    )));
                    return;
                }
            };
            // Create UPayload
            let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                listener_wrapper.on_receive(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get payload encoding",
                )));
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
            listener_wrapper.on_receive(Ok(msg));
        };
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            let listener_wrapper = Arc::new(ListenerWrapper::new(listener));

            // Explicitly bind the lock guard to extend its lifetime
            let mut subscriber_map_guard = self.subscriber_map.lock().unwrap();

            // Now the lock guard is explicitly held until the end of the block
            let listeners = subscriber_map_guard.entry(topic.clone()).or_default();
            listeners.insert(listener_wrapper, subscriber);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(())
    }

    async fn register_request_listener<T>(&self, topic: &UUri, listener: T) -> Result<(), UStatus>
    where
        T: Copy + UListener + 'static,
    {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(topic)?;

        let query_map = self.query_map.clone();
        // Setup callback
        let callback = move |query: Query| {
            let listener_wrapper = Arc::new(ListenerWrapper::new(listener));
            // Create UAttribute
            let Some(attachment) = query.attachment() else {
                listener.on_receive(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                )));
                return;
            };
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener_wrapper.on_receive(Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    )));
                    return;
                }
            };
            // Create UPayload
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&value.encoding) else {
                        listener_wrapper.on_receive(Err(UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to get payload encoding",
                        )));
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
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.reqid.to_string(), query);
            listener_wrapper.on_receive(Ok(msg));
        };
        let listener_wrapper = Arc::new(ListenerWrapper::new(listener));
        if let Ok(queryable) = self
            .session
            .declare_queryable(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            let mut queryable_map_guard = self.queryable_map.lock().unwrap();

            // Now the lock guard is explicitly held until the end of the block
            let listeners = queryable_map_guard.entry(topic.clone()).or_default();
            listeners.insert(listener_wrapper, queryable);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn register_response_listener<T>(&self, topic: &UUri, listener: T) -> Result<(), UStatus>
    where
        T: Copy + UListener + 'static,
    {
        let listener_wrapper = Arc::new(ListenerWrapper::new(listener));

        let mut rpc_callback_map_guard = self.rpc_callback_map.lock().unwrap();

        let listeners = rpc_callback_map_guard.entry(topic.clone()).or_default();

        listeners.insert(listener_wrapper);

        Ok(())
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let payload = *message.payload.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uPayload",
        ))?;
        let attributes = *message.attributes.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uAttributes",
        ))?;

        // Check the type of UAttributes (Publish / Request / Response)
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
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Publish UAttributes {e:?}"),
                        )
                    })?;
                // Get Zenoh key
                let topic = if attributes.sink.is_some() {
                    // If sink is not empty, this is Notification => topic is sink
                    attributes.clone().sink
                } else {
                    // If sink is empty, this is Publication => topic is source
                    attributes.clone().source
                };
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Publish
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                UAttributesValidators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Request UAttributes {e:?}"),
                        )
                    })?;
                // Get Zenoh key
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
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Response UAttributes {e:?}"),
                        )
                    })?;
                // Get Zenoh key
                let topic = attributes.clone().source;
                let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
                // Send Response
                self.send_response(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Wrong Message type in UAttributes",
            )),
        }
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "Not implemented",
        ))
    }

    async fn register_listener<T>(&self, topic: UUri, listener: T) -> Result<(), UStatus>
    where
        T: Copy + UListener + 'static,
    {
        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to register for all of Publish, Request, and Response
            // RPC response
            self.register_response_listener(&topic, listener)?;
            // RPC request
            self.register_request_listener(&topic, listener).await?;
            // Normal publish
            self.register_publish_listener(&topic, listener).await?;
            Ok(())
        } else {
            // Do the validation
            UriValidator::validate(&topic)
                .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

            if UriValidator::is_rpc_response(&topic) {
                // RPC response
                self.register_response_listener(&topic, listener)
            } else if UriValidator::is_rpc_method(&topic) {
                // RPC request
                self.register_request_listener(&topic, listener).await
            } else {
                // Normal publish
                self.register_publish_listener(&topic, listener).await
            }
        }
    }

    async fn unregister_listener<T>(&self, topic: UUri, listener: T) -> Result<(), UStatus>
    where
        T: Copy + UListener + 'static,
    {
        if topic.authority.is_some() && topic.entity.is_none() && topic.resource.is_none() {
            // This is special UUri which means we need to unregister both listeners
        } else {
            // Do the validation
            UriValidator::validate(&topic)
                .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;
        }

        if self
            .rpc_callback_map
            .lock()
            .unwrap()
            .remove(&topic)
            .is_none()
        {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "RPC response callback doesn't exist",
            ));
        }
        let mut queryable_map_guard = self.queryable_map.lock().unwrap();

        let listeners = queryable_map_guard.entry(topic.clone());
        match listeners {
            Entry::Vacant(_) => {
                return Err(UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    format!("No listeners registered for topic: {:?}", &topic),
                ))
            }
            Entry::Occupied(mut e) => {
                let occupied = e.get_mut();
                let identified_listener = ListenerWrapper::new(listener);
                occupied.remove(&identified_listener);
            }
        }

        let mut subscriber_map_guard = self.subscriber_map.lock().unwrap();

        let listeners = subscriber_map_guard.entry(topic.clone());
        match listeners {
            Entry::Vacant(_) => {
                return Err(UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    format!("No listeners registered for topic: {:?}", &topic),
                ))
            }
            Entry::Occupied(mut e) => {
                let occupied = e.get_mut();
                let identified_listener = ListenerWrapper::new(listener);
                occupied.remove(&identified_listener);
            }
        }

        Ok(())
    }
}
