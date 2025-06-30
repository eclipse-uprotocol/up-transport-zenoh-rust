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
use clap::Parser;
use serde_json::json;
use up_transport_zenoh::zenoh_config;

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WhatAmIType {
    Peer,
    Client,
    Router,
}

impl WhatAmIType {
    const fn to_str(self) -> &'static str {
        match self {
            Self::Peer => "peer",
            Self::Client => "client",
            Self::Router => "router",
        }
    }
}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Args {
    #[arg(short, long)]
    /// A configuration file.
    config: Option<String>,
    #[arg(short, long)]
    /// The Zenoh session mode [default: peer].
    mode: Option<WhatAmIType>,
    #[arg(short = 'e', long)]
    /// Endpoints to connect to.
    connect: Vec<String>,
    #[arg(short, long)]
    /// Endpoints to listen on.
    listen: Vec<String>,
    #[arg(long)]
    /// Disable the multicast-based scouting mechanism.
    no_multicast_scouting: bool,
}

// #[allow(clippy::must_use_candidate, clippy::missing_panics_doc)]
pub(crate) fn get_zenoh_config() -> zenoh_config::Config {
    let args = Args::parse();

    // Load the config from file path
    let mut zenoh_cfg = match &args.config {
        Some(path) => zenoh_config::Config::from_file(path).unwrap(),
        None => zenoh_config::Config::default(),
    };

    // You can choose from Router, Peer, Client
    if let Some(mode) = args.mode {
        zenoh_cfg
            .insert_json5("mode", &json!(mode.to_str()).to_string())
            .unwrap();
    }

    // Set connection address
    if !args.connect.is_empty() {
        zenoh_cfg
            .insert_json5("connect/endpoints", &json!(args.connect).to_string())
            .unwrap();
    }

    // Set listener address
    if !args.listen.is_empty() {
        zenoh_cfg
            .insert_json5("listen/endpoints", &json!(args.listen).to_string())
            .unwrap();
    }

    // Set multicast configuration
    if args.no_multicast_scouting {
        zenoh_cfg
            .insert_json5("scouting/multicast/enabled", &json!(false).to_string())
            .unwrap();
    }

    zenoh_cfg
}
