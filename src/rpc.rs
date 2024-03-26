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
use std::{string::ToString, time::Duration};
use up_rust::{
    CallOptions, Data, RpcClient, RpcClientResult, RpcMapperError, UAttributes, UMessage, UPayload,
    UResourceBuilder, UUIDBuilder, UUri, UriValidator,
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
            let msg = "Unable to transform to Zenoh key".to_string();
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

        // Generate UAttributes
        let uuid_builder = UUIDBuilder::new();
        // Create response address
        let mut source = topic.clone();
        source.resource = Some(UResourceBuilder::for_rpc_response()).into();
        // Create UAttributes
        let uattributes =
            UAttributes::request(uuid_builder.build(), topic, source, options.clone());
        // Put into attachment
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&uattributes) else {
            let msg = "Unable to transform UAttributes to user attachment in Zenoh".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        // TODO: Query should support .encoding
        let getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(u64::from(options.ttl)));

        // Send the query
        let Ok(replies) = getbuilder.res().await else {
            let msg = "Error while sending Zenoh query".to_string();
            log::error!("{msg}");
            return Err(RpcMapperError::UnexpectedError(msg));
        };

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
                // TODO: Need to check attributes is correct or not
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
