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
use std::sync::Once;
use up_client_zenoh::UPClientZenoh;
use up_rust::{Number, UAuthority, UEntity, UResource, UResourceBuilder, UStatus, UUri};
use zenoh::config::Config;

static INIT: Once = Once::new();
static AUTH_NAME: &str = "auth_name";
static ENTITY_NAME: &str = "entity_name";
static RESOURCE_NAME: &str = "resource_name";
static INSTANCE_NAME: &str = "instance_name";
static MESSAGE_NAME: &str = "message_name";

pub fn before_test() {
    INIT.call_once(env_logger::init);
}

#[allow(clippy::must_use_candidate)]
pub fn create_authority(idx: u8) -> UAuthority {
    UAuthority {
        name: Some(format!("{AUTH_NAME}{idx}")),
        number: Some(Number::Id(vec![1, 2, 3, 4 + idx])),
        ..Default::default()
    }
}

#[allow(clippy::must_use_candidate)]
pub fn create_entity(idx: u32) -> UEntity {
    UEntity {
        name: format!("{ENTITY_NAME}{idx}"),
        id: Some(idx),
        version_major: Some(1),
        version_minor: None,
        ..Default::default()
    }
}

#[allow(clippy::must_use_candidate)]
pub fn create_resource(idx: u32) -> UResource {
    UResource {
        name: format!("{RESOURCE_NAME}{idx}"),
        instance: Some(format!("{INSTANCE_NAME}{idx}")),
        message: Some(format!("{MESSAGE_NAME}{idx}")),
        id: Some(idx),
        ..Default::default()
    }
}

/// # Errors
/// Will return `Err` if unable to create `UPClientZenoh`
pub async fn create_up_client_zenoh(
    auth_idx: u8,
    entity_idx: u32,
) -> Result<UPClientZenoh, UStatus> {
    let uauthority = create_authority(auth_idx);
    let uentity = create_entity(entity_idx);
    UPClientZenoh::new(Config::default(), uauthority, uentity).await
}

#[allow(clippy::must_use_candidate)]
pub fn create_utransport_uuri(auth_idx: Option<u8>, entity_idx: u32, resource_idx: u32) -> UUri {
    UUri {
        authority: if let Some(idx) = auth_idx {
            // Remote UUri
            Some(create_authority(idx)).into()
        } else {
            // Local UUri
            None.into()
        },
        entity: Some(create_entity(entity_idx)).into(),
        resource: Some(create_resource(resource_idx)).into(),
        ..Default::default()
    }
}

#[allow(clippy::must_use_candidate)]
pub fn create_rpcserver_uuri(auth_idx: Option<u8>, entity_idx: u32) -> UUri {
    UUri {
        authority: if let Some(idx) = auth_idx {
            // Remote UUri
            Some(create_authority(idx)).into()
        } else {
            // Local UUri
            None.into()
        },
        entity: Some(create_entity(entity_idx)).into(),
        resource: Some(UResourceBuilder::for_rpc_request(
            Some("SimpleTest".to_string()),
            Some(5678),
        ))
        .into(),
        ..Default::default()
    }
}

#[allow(clippy::must_use_candidate)]
pub fn create_special_uuri(idx: u8) -> UUri {
    UUri {
        authority: Some(create_authority(idx)).into(),
        ..Default::default()
    }
}
