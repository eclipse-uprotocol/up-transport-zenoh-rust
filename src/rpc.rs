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
use crate::UPTransportZenoh;
use async_trait::async_trait;
use std::{string::ToString, sync::Arc, time::Duration};
use tracing::error;
use up_rust::{
    communication::{CallOptions, RpcClient, ServiceInvocationError, UPayload},
    LocalUriProvider, UAttributes, UCode, UMessageType, UPayloadFormat, UPriority, UStatus, UUri,
    UUID,
};
use zenoh::{query::QueryTarget, sample::SampleBuilderTrait};

pub struct ZenohRpcClient {
    transport: Arc<UPTransportZenoh>,
}
impl ZenohRpcClient {
    /// Creates a new RPC client for the Zenoh transport.
    ///
    /// # Arguments
    ///
    /// * `transport` - The Zenoh uProtocol Transport Layer.
    pub fn new(transport: Arc<UPTransportZenoh>) -> Self {
        ZenohRpcClient { transport }
    }
}

#[async_trait]
impl RpcClient for ZenohRpcClient {
    async fn invoke_method(
        &self,
        method: UUri,
        call_options: CallOptions,
        payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // Get data and format from UPayload
        let mut payload_data = None;
        let mut payload_format = UPayloadFormat::UPAYLOAD_FORMAT_UNSPECIFIED;
        if let Some(payload) = payload {
            payload_format = payload.payload_format();
            payload_data = Some(payload.payload());
        }

        // Get source UUri
        let source_uri = self.transport.get_source_uri();

        let attributes = UAttributes {
            type_: UMessageType::UMESSAGE_TYPE_REQUEST.into(),
            id: Some(call_options.message_id().unwrap_or_else(UUID::build)).into(),
            priority: call_options
                .priority()
                .unwrap_or(UPriority::UPRIORITY_CS4)
                .into(),
            source: Some(source_uri.clone()).into(),
            sink: Some(method.clone()).into(),
            ttl: Some(call_options.ttl()),
            token: call_options.token(),
            payload_format: payload_format.into(),
            ..Default::default()
        };

        // Get Zenoh key
        let zenoh_key = self
            .transport
            .to_zenoh_key_string(&source_uri, Some(&method));

        // Put UAttributes into Zenoh user attachment
        let Ok(attachment) = UPTransportZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to user attachment in Zenoh".to_string();
            error!("{msg}");
            return Err(ServiceInvocationError::Internal(msg));
        };

        // Send the query
        let mut getbuilder = self.transport.session.get(&zenoh_key);
        getbuilder = match payload_data {
            Some(data) => getbuilder.payload(data),
            None => getbuilder,
        }
        .attachment(attachment)
        .target(QueryTarget::BestMatching)
        .timeout(Duration::from_millis(u64::from(call_options.ttl())));
        let Ok(replies) = getbuilder.await else {
            let msg = "Error while sending Zenoh query".to_string();
            error!("{msg}");
            return Err(ServiceInvocationError::RpcError(UStatus {
                code: UCode::INTERNAL.into(),
                message: Some(msg),
                ..Default::default()
            }));
        };

        // Receive the reply
        let Ok(reply) = replies.recv_async().await else {
            let msg = "Error while receiving Zenoh reply".to_string();
            error!("{msg}");
            return Err(ServiceInvocationError::RpcError(UStatus {
                code: UCode::INTERNAL.into(),
                message: Some(msg),
                ..Default::default()
            }));
        };
        match reply.result() {
            Ok(sample) => {
                let Some(payload_format) = sample
                    .attachment()
                    .and_then(|attach| UPTransportZenoh::attachment_to_uattributes(attach).ok())
                    .map(|attr| attr.payload_format.enum_value_or_default())
                else {
                    let msg = "Unable to get the UPayloadFormat from the attachment".to_string();
                    error!("{msg}");
                    return Err(ServiceInvocationError::RpcError(UStatus {
                        code: UCode::INTERNAL.into(),
                        message: Some(msg),
                        ..Default::default()
                    }));
                };
                Ok(Some(UPayload::new(sample.payload().into(), payload_format)))
            }
            Err(e) => {
                let msg = format!("Error while parsing Zenoh reply: {e:?}");
                error!("{msg}");
                return Err(ServiceInvocationError::RpcError(UStatus {
                    code: UCode::INTERNAL.into(),
                    message: Some(msg),
                    ..Default::default()
                }));
            }
        }
    }
}
