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
use crate::UPClientZenoh;
use async_trait::async_trait;
use std::{string::ToString, time::Duration};
use tracing::error;
use up_rust::{
    communication::{CallOptions, RpcClient, ServiceInvocationError, UPayload},
    UAttributesError, UMessage, UMessageError, UUri,
};
use zenoh::prelude::r#async::*;

#[async_trait]
impl RpcClient for UPClientZenoh {
    async fn invoke_method(
        &self,
        method: UUri,
        call_options: CallOptions,
        payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        //async fn invoke_method(&self, _method: UUri, request: UMessage) -> RpcClientResult {
        // Get Zenoh key
        let source = *request.attributes.source.0.clone().ok_or_else(|| {
            let msg = "attributes.source should not be empty".to_string();
            error!("{msg}");
            UMessageError::PayloadError(msg)
        })?;
        let zenoh_key = if let Some(sink) = request.attributes.sink.0.clone() {
            self.to_zenoh_key_string(&source, Some(&sink))
        } else {
            self.to_zenoh_key_string(&source, None)
        };

        // Create UAttributes and put into Zenoh user attachment
        let attributes = *request.attributes.0.clone().ok_or_else(|| {
            let msg = "Invalid UAttributes".to_string();
            error!("{msg}");
            UMessageError::AttributesValidationError(UAttributesError::ParsingError(msg))
        })?;
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            let msg = "Unable to transform UAttributes to user attachment in Zenoh".to_string();
            error!("{msg}");
            return Err(UMessageError::AttributesValidationError(
                UAttributesError::ParsingError(msg),
            ));
        };

        // Get the data from UPayload
        let value = if let Some(payload) = request.payload {
            Value::new(payload.to_vec().into())
        } else {
            Value::new(vec![].into())
        };

        // Send the query
        let mut getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching);
        if let Some(ttl) = request.attributes.ttl {
            getbuilder = getbuilder.timeout(Duration::from_millis(u64::from(ttl)));
        }
        let Ok(replies) = getbuilder.res().await else {
            let msg = "Error while sending Zenoh query".to_string();
            error!("{msg}");
            return Err(UMessageError::PayloadError(msg));
        };

        // Receive the reply
        let Ok(reply) = replies.recv_async().await else {
            let msg = "Error while receiving Zenoh reply".to_string();
            error!("{msg}");
            return Err(UMessageError::PayloadError(msg));
        };
        match reply.sample {
            Ok(sample) => Ok(UMessage {
                attributes: Some(attributes).into(),
                payload: Some(sample.payload.contiguous().to_vec().into()),
                ..Default::default()
            }),
            Err(e) => {
                let msg = format!("Error while parsing Zenoh reply: {e:?}");
                error!("{msg}");
                Err(UMessageError::PayloadError(msg))
            }
        }
    }
}
