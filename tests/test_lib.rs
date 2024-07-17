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
use up_rust::{UStatus, UUri};
use up_transport_zenoh::{zenoh_config, UPClientZenoh};

static INIT: Once = Once::new();

pub fn before_test() {
    INIT.call_once(UPClientZenoh::try_init_log_from_env);
}

/// # Errors
/// Will return `Err` if unable to create `UPClientZenoh`
pub async fn create_up_client_zenoh(uauthority: &str) -> Result<UPClientZenoh, UStatus> {
    UPClientZenoh::new(zenoh_config::Config::default(), uauthority).await
}

#[allow(clippy::must_use_candidate)]
pub fn new_uuri(authority: &str, ue_id: u32, ue_version_major: u8, resource_id: u16) -> UUri {
    UUri {
        authority_name: authority.to_string(),
        ue_id,
        ue_version_major: ue_version_major.into(),
        resource_id: resource_id.into(),
        ..Default::default()
    }
}
