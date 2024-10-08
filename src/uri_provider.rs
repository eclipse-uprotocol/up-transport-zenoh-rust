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

use crate::UPTransportZenoh;
use up_rust::{LocalUriProvider, UUri};

impl LocalUriProvider for UPTransportZenoh {
    fn get_authority(&self) -> String {
        self.local_uri.authority_name.clone()
    }

    fn get_resource_uri(&self, resource_id: u16) -> UUri {
        let mut uri = self.get_source_uri();
        uri.resource_id = u32::from(resource_id);
        uri
    }

    fn get_source_uri(&self) -> UUri {
        self.local_uri.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::str::FromStr;
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case("//vehicle1/AABB/7/0", "vehicle1", 0x8001, UUri::from_str("//vehicle1/AABB/7/0").unwrap(); "Publish/Notification Resource ID")]
    #[test_case("//192.168.0.1/A1B2/1/0", "192.168.0.1", 0xA, UUri::from_str("//192.168.0.1/A1B2/1/0").unwrap(); "RPC Resource ID")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_local_uri_provider(
        uri: &str,
        authority: &str,
        resource_id: u16,
        source_uri: UUri,
    ) {
        let up_transport_zenoh = UPTransportZenoh::new(zenoh_config::Config::default(), uri)
            .await
            .unwrap();
        assert_eq!(up_transport_zenoh.get_authority(), authority);
        assert_eq!(
            up_transport_zenoh.get_resource_uri(resource_id).resource_id,
            u32::from(resource_id)
        );
        assert_eq!(up_transport_zenoh.get_source_uri(), source_uri);
    }
}
