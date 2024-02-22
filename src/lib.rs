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
pub mod rpc;
pub mod utransport;

use protobuf::{Enum, Message};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex},
};
use up_rust::{
    uprotocol::{Data, UAttributes, UCode, UPayload, UPayloadFormat, UPriority, UStatus, UUri},
    uri::serializer::{MicroUriSerializer, UriSerializer},
};
use zenoh::{
    config::Config,
    prelude::{r#async::*, Sample},
    queryable::{Query, Queryable},
    sample::{Attachment, AttachmentBuilder},
    subscriber::Subscriber,
};

pub struct ZenohListener {}
pub struct UPClientZenoh {
    session: Arc<Session>,
    subscriber_map: Arc<Mutex<HashMap<String, Subscriber<'static, ()>>>>,
    queryable_map: Arc<Mutex<HashMap<String, Queryable<'static, ()>>>>,
    query_map: Arc<Mutex<HashMap<String, Query>>>,
    callback_counter: AtomicU64,
}

impl UPClientZenoh {
    /// # Errors
    /// Will return `Err` if unable to create Zenoh session
    pub async fn new(config: Config) -> Result<UPClientZenoh, UStatus> {
        let Ok(session) = zenoh::open(config).res().await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to open Zenoh session",
            ));
        };
        Ok(UPClientZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            callback_counter: AtomicU64::new(0),
        })
    }

    fn to_zenoh_key_string(uri: &UUri) -> Result<String, UStatus> {
        let micro_uuri = MicroUriSerializer::serialize(uri).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Unable to serialize into micro format",
            )
        })?;
        let micro_zenoh_key: String = micro_uuri
            .iter()
            .fold(String::new(), |s, c| s + &format!("{c:02x}"));
        Ok(micro_zenoh_key)
    }

    #[allow(clippy::match_same_arms)]
    fn map_zenoh_priority(upriority: UPriority) -> Priority {
        match upriority {
            UPriority::UPRIORITY_CS0 => Priority::Background,
            UPriority::UPRIORITY_CS1 => Priority::DataLow,
            UPriority::UPRIORITY_CS2 => Priority::Data,
            UPriority::UPRIORITY_CS3 => Priority::DataHigh,
            UPriority::UPRIORITY_CS4 => Priority::InteractiveLow,
            UPriority::UPRIORITY_CS5 => Priority::InteractiveHigh,
            UPriority::UPRIORITY_CS6 => Priority::RealTime,
            // If uProtocol prioritiy isn't specified, use CS1(DataLow) by default.
            // https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/basics/qos.adoc
            UPriority::UPRIORITY_UNSPECIFIED => Priority::DataLow,
        }
    }

    fn to_upayload_format(encoding: &Encoding) -> Option<UPayloadFormat> {
        let Ok(value) = encoding.suffix().parse::<i32>() else {
            return None;
        };
        UPayloadFormat::from_i32(value)
    }

    fn uattributes_to_attachment(uattributes: &UAttributes) -> anyhow::Result<AttachmentBuilder> {
        let mut attachment = AttachmentBuilder::new();
        attachment.insert("attr", &uattributes.write_to_bytes()?);
        /* TODO: We send the whole uattributes directly for the time being and do the benchmark later.
        attachment.insert("id", &uattributes.id.write_to_bytes()?);
        attachment.insert(
            "type_",
            &uattributes
                .type_
                .enum_value()
                .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type"))?
                .to_type_string(),
        );
        attachment.insert("source", &uattributes.source.write_to_bytes()?);
        attachment.insert("sink", &uattributes.sink.write_to_bytes()?);
        // TODO: Check whether request & response need priority or not
        attachment.insert("priority", &uattributes.priority.value().to_string());
        if let Some(ttl) = uattributes.ttl {
            attachment.insert("ttl", &ttl.to_string());
        }
        if let Some(plevel) = uattributes.permission_level {
            attachment.insert("permission_level", &plevel.to_string());
        }
        if let Some(commstatus) = uattributes.commstatus {
            attachment.insert("commstatus", &commstatus.to_string());
        }
        attachment.insert("reqid", &uattributes.reqid.write_to_bytes()?);
        if let Some(token) = uattributes.token.clone() {
            attachment.insert("token", &token);
        }
        if let Some(traceparent) = uattributes.traceparent.clone() {
            attachment.insert("traceparent", &traceparent);
        }
        */
        Ok(attachment)
    }

    fn attachment_to_uattributes(attachment: &Attachment) -> anyhow::Result<UAttributes> {
        let uattributes = UAttributes::parse_from_bytes(
            attachment
                .get(&"attr".as_bytes())
                .ok_or(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get uAttributes",
                ))?
                .as_slice(),
        )?;
        /* TODO: We send the whole uattributes directly for the time being and do the benchmark later.
        let mut uattributes = UAttributes::new();
        if let Some(id) = attachment.get(&"id".as_bytes()) {
            let uuid = UUID::parse_from_bytes(&id)?;
            uattributes.id = Some(uuid).into();
        } else {
            return Err(UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse id").into());
        }
        if let Some(type_) = attachment.get(&"type_".as_bytes()) {
            let uuid = UMessageType::from_type_string(type_.to_string());
            uattributes.type_ = uuid.into();
        } else {
            return Err(UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type_").into());
        }
        if let Some(source) = attachment.get(&"source".as_bytes()) {
            let source = UUri::parse_from_bytes(&source)?;
            uattributes.source = Some(source).into();
        } else {
            return Err(UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse source").into());
        }
        if let Some(sink) = attachment.get(&"sink".as_bytes()) {
            let sink = UUri::parse_from_bytes(&sink)?;
            uattributes.sink = Some(sink).into();
        } else {
            return Err(UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse sink").into());
        }
        if let Some(priority) = attachment.get(&"priority".as_bytes()) {
            let priority =
                UPriority::from_i32(String::from_utf8(priority.to_vec())?.parse()?).ok_or(
                    UStatus::fail_with_code(UCode::INTERNAL, "Wrong priority type"),
                )?;
            uattributes.priority = priority.into();
        } else {
            return Err(
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse priority").into(),
            );
        }
        if let Some(ttl) = attachment.get(&"ttl".as_bytes()) {
            let ttl = String::from_utf8(ttl.to_vec())?.parse::<i32>()?;
            uattributes.ttl = Some(ttl);
        }
        if let Some(permission_level) = attachment.get(&"permission_level".as_bytes()) {
            let permission_level = String::from_utf8(permission_level.to_vec())?.parse::<i32>()?;
            uattributes.permission_level = Some(permission_level);
        }
        if let Some(commstatus) = attachment.get(&"commstatus".as_bytes()) {
            let commstatus = String::from_utf8(commstatus.to_vec())?.parse::<i32>()?;
            uattributes.commstatus = Some(commstatus);
        }
        if let Some(reqid) = attachment.get(&"reqid".as_bytes()) {
            let reqid = UUID::parse_from_bytes(&reqid)?;
            uattributes.reqid = Some(reqid).into();
        }
        if let Some(token) = attachment.get(&"token".as_bytes()) {
            let token = token.to_string();
            uattributes.token = Some(token);
        }
        if let Some(traceparent) = attachment.get(&"traceparent".as_bytes()) {
            let traceparent = traceparent.to_string();
            uattributes.traceparent = Some(traceparent);
        }
        */
        Ok(uattributes)
    }

    async fn send_publish(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid data",
            ));
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

    async fn send_response(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid data",
            ));
        };

        // Serialized UAttributes into protobuf
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
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
            KeyExpr::new(zenoh_key.to_string()).map_err(|_| {
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
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to add attachment"))?
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to reply with Zenoh"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use up_rust::uprotocol::{UEntity, UResource, UUri};

    #[test]
    fn test_to_zenoh_key_string() {
        // create uuri for test
        let uuri = UUri {
            entity: Some(UEntity {
                name: "body.access".to_string(),
                version_major: Some(1),
                id: Some(1234),
                ..Default::default()
            })
            .into(),
            resource: Some(UResource {
                name: "door".to_string(),
                instance: Some("front_left".to_string()),
                message: Some("Door".to_string()),
                id: Some(5678),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        assert_eq!(
            UPClientZenoh::to_zenoh_key_string(&uuri).unwrap(),
            String::from("0100162e04d20100")
        );
    }
}
