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
use up_rust::{
    CallOptions, Data, RpcClient, RpcClientResult, RpcMapperError, UAttributes, UMessage, UPayload,
    UUIDBuilder, UUri, UriValidator,
};
use zenoh::prelude::r#async::*;

#[async_trait]
impl RpcClient for UPClientZenoh {
    async fn invoke_method(
        &self,
        topic: UUri,
        payload: UPayload,
        options: CallOptions,
    ) -> RpcClientResult {
        // Validate UUri
        UriValidator::validate(&topic).map_err(|_| {
            let msg = "Invalid UUri for invoke_method".to_string();
            log::error!("{msg}");
            RpcMapperError::UnexpectedError(msg)
        })?;

        // Get Zenoh key
        let Ok(zenoh_key) = UPClientZenoh::to_zenoh_key_string(&topic) else {
            let msg = "Unable to transform UUri to Zenoh key".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };

        // Create UAttributes and put into Zenoh user attachment
        let uattributes = UAttributes::request(
            UUIDBuilder::build(),
            topic,
            self.get_response_uuri(),
            options.clone(),
        );
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&uattributes) else {
            let msg = "Unable to transform UAttributes to user attachment in Zenoh".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };

        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // Assume we only have Value here, no reference for shared memory
            let msg = "The data in UPayload should be Data::Value".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::InvalidPayload(msg));
        };
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));

        // Send the query
        let getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(options.ttl)));
        let Ok(replies) = getbuilder.res().await else {
            let msg = "Error while sending Zenoh query".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };

        // Receive the reply
        let Ok(reply) = replies.recv_async().await else {
            let msg = "Error while receiving Zenoh reply".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };
        match reply.sample {
            Ok(sample) => {
                let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                    let msg = "Error while parsing Zenoh encoding".to_string();
                    log::error!("{msg}");
                    return Err(RpcMapperError::UnexpectedError(msg));
                };
                Ok(UMessage {
                    attributes: Some(uattributes).into(),
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
            Err(e) => {
                let msg = format!("Error while parsing Zenoh reply: {e:?}");
                log::error!("{msg}");
                Err(RpcMapperError::UnexpectedError(msg))
            }
        }
    }
}
