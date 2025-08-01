// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_rlp::{Decodable, Encodable, Header, RlpDecodable, RlpEncodable, encode_list};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::Message;
use monad_types::NodeId;

use crate::{MonadNameRecord, PeerDiscoveryEvent};

const PEER_DISCOVERY_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub enum PeerDiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Ping(Ping<ST>),
    Pong(Pong),
    PeerLookupRequest(PeerLookupRequest<ST>),
    PeerLookupResponse(PeerLookupResponse<ST>),
}

impl<ST: CertificateSignatureRecoverable> Message for PeerDiscoveryMessage<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = PeerDiscoveryEvent<ST>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        match self {
            PeerDiscoveryMessage::Ping(ping) => PeerDiscoveryEvent::PingRequest { from, ping },
            PeerDiscoveryMessage::Pong(pong) => PeerDiscoveryEvent::PongResponse { from, pong },
            PeerDiscoveryMessage::PeerLookupRequest(request) => {
                PeerDiscoveryEvent::PeerLookupRequest { from, request }
            }
            PeerDiscoveryMessage::PeerLookupResponse(response) => {
                PeerDiscoveryEvent::PeerLookupResponse { from, response }
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Encodable for PeerDiscoveryMessage<ST> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let version = PEER_DISCOVERY_VERSION;

        match self {
            PeerDiscoveryMessage::Ping(ping) => {
                let enc: [&dyn Encodable; 3] = [&version, &1_u8, ping];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::Pong(pong) => {
                let enc: [&dyn Encodable; 3] = [&version, &2_u8, pong];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request) => {
                let enc: [&dyn Encodable; 3] = [&version, &3_u8, peer_lookup_request];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            PeerDiscoveryMessage::PeerLookupResponse(peer_lookup_response) => {
                let enc: [&dyn Encodable; 3] = [&version, &4_u8, peer_lookup_response];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Decodable for PeerDiscoveryMessage<ST> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let _peer_discovery_version = u16::decode(&mut payload)?;

        match u8::decode(&mut payload)? {
            1 => Ok(Self::Ping(Ping::decode(&mut payload)?)),
            2 => Ok(Self::Pong(Pong::decode(&mut payload)?)),
            3 => Ok(Self::PeerLookupRequest(PeerLookupRequest::decode(
                &mut payload,
            )?)),
            4 => Ok(Self::PeerLookupResponse(PeerLookupResponse::decode(
                &mut payload,
            )?)),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown PeerDiscoveryMessage",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, RlpDecodable, RlpEncodable)]
#[rlp(trailing)]
pub struct Ping<ST: CertificateSignatureRecoverable> {
    pub id: u32,
    pub local_name_record: Option<MonadNameRecord<ST>>,
}

impl<ST: CertificateSignatureRecoverable> Eq for Ping<ST> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct Pong {
    pub ping_id: u32,
    pub local_record_seq: u64,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable)]
pub struct PeerLookupRequest<ST: CertificateSignatureRecoverable> {
    pub lookup_id: u32,
    pub target: NodeId<CertificateSignaturePubKey<ST>>,
    pub open_discovery: bool,
}

#[derive(Debug, Clone, RlpDecodable, RlpEncodable)]
pub struct PeerLookupResponse<ST: CertificateSignatureRecoverable> {
    pub lookup_id: u32,
    pub target: NodeId<CertificateSignaturePubKey<ST>>,
    pub name_records: Vec<MonadNameRecord<ST>>,
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddrV4, str::FromStr};

    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;

    use super::*;
    use crate::NameRecord;

    type SignatureType = SecpSignature;

    #[test]
    fn test_ping_with_record_rlp_roundtrip() {
        let key = get_key::<SignatureType>(37);
        let ping = Ping {
            id: 257,
            local_name_record: Some(MonadNameRecord::<SignatureType>::new(
                NameRecord {
                    ip: SocketAddrV4::from_str("127.0.0.1:8000")
                        .unwrap()
                        .ip()
                        .clone(),
                    tcp_port: 8000,
                    udp_port: 8000,
                    direct_udp_port: None,
                    capabilities: 0,
                    seq: 2,
                },
                &key,
            )),
        };

        let mut encoded = Vec::new();
        ping.encode(&mut encoded);

        let decoded = Ping::<SignatureType>::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ping, decoded);
    }

    #[test]
    fn test_ping_without_record_rlp_roundtrip() {
        let ping = Ping {
            id: 257,
            local_name_record: None,
        };

        let mut encoded = Vec::new();
        ping.encode(&mut encoded);

        let decoded = Ping::<SignatureType>::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(ping, decoded);
    }
}
