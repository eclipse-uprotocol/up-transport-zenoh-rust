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
use up_transport_zenoh::zenoh_config;

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WhatAmIType {
    Peer,
    Client,
    Router,
}

#[cfg(not(feature = "zenoh-unstable"))]
#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
pub struct Args {
    #[arg(short, long)]
    /// A configuration file.
    config: Option<String>,
}

#[cfg(feature = "zenoh-unstable")]
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

#[allow(clippy::must_use_candidate, clippy::missing_panics_doc)]
pub fn get_zenoh_config() -> zenoh_config::Config {
    let args = Args::parse();

    // Load the config from file path
    #[allow(unused_mut)]
    let mut zenoh_cfg = match &args.config {
        Some(path) => zenoh_config::Config::from_file(path).unwrap(),
        None => zenoh_config::Config::default(),
    };

    #[cfg(feature = "zenoh-unstable")]
    {
        // You can choose from Router, Peer, Client
        match args.mode {
            Some(WhatAmIType::Peer) => zenoh_cfg.set_mode(Some(zenoh::config::WhatAmI::Peer)),
            Some(WhatAmIType::Client) => zenoh_cfg.set_mode(Some(zenoh::config::WhatAmI::Client)),
            Some(WhatAmIType::Router) => zenoh_cfg.set_mode(Some(zenoh::config::WhatAmI::Router)),
            None => Ok(None),
        }
        .unwrap();

        // Set connection address
        if !args.connect.is_empty() {
            zenoh_cfg
                .connect
                .endpoints
                .set(args.connect.iter().map(|v| v.parse().unwrap()).collect())
                .unwrap();
        }

        // Set listener address
        if !args.listen.is_empty() {
            zenoh_cfg
                .listen
                .endpoints
                .set(args.listen.iter().map(|v| v.parse().unwrap()).collect())
                .unwrap();
        }

        // Set multicast configuration
        if args.no_multicast_scouting {
            zenoh_cfg
                .scouting
                .multicast
                .set_enabled(Some(false))
                .unwrap();
        }
    }

    zenoh_cfg
}
