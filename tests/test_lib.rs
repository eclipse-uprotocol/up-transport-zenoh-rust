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
use up_rust::{UCode, UStatus};
use up_transport_zenoh::UPTransportZenoh;

static INIT: Once = Once::new();

pub fn before_test() {
    INIT.call_once(UPTransportZenoh::try_init_log_from_env);
}

/// # Errors
/// Will return `Err` if unable to create `UPTransportZenoh`
pub async fn create_up_transport_zenoh(
    local_authority_name: &str,
    config: Option<zenoh::config::Config>,
) -> Result<UPTransportZenoh, UStatus> {
    let builder = UPTransportZenoh::builder(local_authority_name).map_err(|e| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, format!("Invalid URI: {e}"))
    })?;
    builder
        .with_config(config.unwrap_or_default())
        .build()
        .await
}
