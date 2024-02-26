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
use std::time::Duration;
use up_rust::{
    rpc::{CallOptions, RpcClient, RpcClientResult, RpcMapperError, RpcServer},
    transport::datamodel::UTransport,
    uprotocol::{Data, UAttributes, UMessage, UPayload, UStatus, UUri},
    uri::{builder::resourcebuilder::UResourceBuilder, validator::UriValidator},
    uuid::builder::UUIDBuilder,
};
use zenoh::prelude::r#async::*;

// TODO: Need to check how to use CallOptions
#[async_trait]
impl RpcClient for UPClientZenoh {
    async fn invoke_method(
        &self,
        topic: UUri,
        payload: UPayload,
        _options: CallOptions,
    ) -> RpcClientResult {
        // Validate UUri
        UriValidator::validate(&topic)
            .map_err(|_| RpcMapperError::UnexpectedError(String::from("Wrong UUri")))?;

        // TODO: Comment it and check whether uAttributes is not necessary.
        //// Validate UAttributes
        //{
        //    // TODO: Check why the validator doesn't have Send
        //    let validator = Validators::Request.validator();
        //    if let Err(e) = validator.validate(&attributes) {
        //        return Err(RpcMapperError::UnexpectedError(format!(
        //            "Wrong UAttributes {e:?}",
        //        )));
        //    }
        //}

        // Get Zenoh key
        let Ok(zenoh_key) = UPClientZenoh::to_zenoh_key_string(&topic) else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Unable to transform to Zenoh key",
            )));
        };

        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(RpcMapperError::InvalidPayload(String::from(
                "Wrong UPayload",
            )));
        };

        // TODO: Check how to generate uAttributes, ex: source, reqid...
        let mut uattributes = UAttributes::new();
        let mut source = topic.clone();
        source.resource = Some(UResourceBuilder::for_rpc_response()).into();
        uattributes.source = Some(source).into();
        uattributes.sink = Some(topic).into();
        uattributes.reqid = Some(UUIDBuilder::new().build()).into();
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&uattributes) else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Invalid uAttributes",
            )));
        };

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        // TODO: Query should support .encoding
        // TODO: Adjust the timeout
        let getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(1000));

        // Send the query
        let Ok(replies) = getbuilder.res().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while sending Zenoh query",
            )));
        };

        let Ok(reply) = replies.recv_async().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while receiving Zenoh reply",
            )));
        };
        match reply.sample {
            Ok(sample) => {
                let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
                    return Err(RpcMapperError::UnexpectedError(String::from(
                        "Error while parsing Zenoh encoding",
                    )));
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
            Err(_) => Err(RpcMapperError::UnexpectedError(String::from(
                "Error while parsing Zenoh reply",
            ))),
        }
    }
}

#[async_trait]
impl RpcServer for UPClientZenoh {
    async fn register_rpc_listener(
        &self,
        method: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        self.register_listener(method, listener).await
    }

    async fn unregister_rpc_listener(&self, method: UUri, listener: &str) -> Result<(), UStatus> {
        self.unregister_listener(method, listener).await
    }
}