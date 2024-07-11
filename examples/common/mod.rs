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
use up_transport_zenoh::zenoh_config;

#[allow(clippy::must_use_candidate, clippy::missing_panics_doc)]
pub fn get_zenoh_config() -> zenoh_config::Config {
    // Load the config from file path
    // Config Examples: https://github.com/eclipse-zenoh/zenoh/blob/0.10.1-rc/DEFAULT_CONFIG.json5
    // let mut zenoh_cfg = Config::from_file("./DEFAULT_CONFIG.json5").unwrap();

    // Loat the default config struct
    let mut zenoh_cfg = zenoh_config::Config::default();
    // You can choose from Router, Peer, Client
    zenoh_cfg
        .set_mode(Some(zenoh_config::WhatAmI::Peer))
        .unwrap();

    zenoh_cfg
}
