use std::{
    collections::{BTreeSet, HashMap},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, encode_list};
use message::{PeerLookupRequest, PeerLookupResponse, Ping, Pong};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId, Round};
use tracing::warn;

pub mod discovery;
pub mod driver;
pub mod message;
pub mod mock;

pub use driver::{PeerDiscoveryDriver, PeerDiscoveryEmit, PeerDiscoveryHandle};
pub use message::PeerDiscoveryMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NameRecord {
    pub address: SocketAddrV4,
    pub seq: u64,
}

impl Encodable for NameRecord {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let enc: [&dyn Encodable; 3] =
            [&self.address.ip().octets(), &self.address.port(), &self.seq];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for NameRecord {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let buf = &mut alloy_rlp::Header::decode_bytes(buf, true)?;

        let Ok(ip) = <[u8; 4]>::decode(buf) else {
            warn!("ip address decode failed: {:?}", buf);
            return Err(alloy_rlp::Error::Custom("Invalid IPv4 address"));
        };
        let port = u16::decode(buf)?;
        let addr = SocketAddrV4::new(Ipv4Addr::from(ip), port);
        let seq = u64::decode(buf)?;

        Ok(Self { address: addr, seq })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, RlpEncodable, RlpDecodable, Eq)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub name_record: NameRecord,
    pub signature: ST,
}

impl<ST: CertificateSignatureRecoverable> MonadNameRecord<ST> {
    pub fn new(name_record: NameRecord, key: &ST::KeyPairType) -> Self {
        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let signature = ST::sign::<signing_domain::NameRecord>(&encoded, key);
        Self {
            name_record,
            signature,
        }
    }

    pub fn recover_pubkey(
        &self,
    ) -> Result<NodeId<CertificateSignaturePubKey<ST>>, <ST as CertificateSignature>::Error> {
        let mut encoded = Vec::new();
        self.name_record.encode(&mut encoded);
        let pubkey = self
            .signature
            .recover_pubkey::<signing_domain::NameRecord>(&encoded)?;
        Ok(NodeId::new(pubkey))
    }

    pub fn address(&self) -> SocketAddrV4 {
        self.name_record.address
    }

    pub fn seq(&self) -> u64 {
        self.name_record.seq
    }
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        to: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PingRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping<ST>,
    },
    PongResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    },
    PingTimeout {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    },
    SendPeerLookup {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        open_discovery: bool,
    },
    PeerLookupRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    },
    PeerLookupResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    },
    PeerLookupTimeout {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    },
    UpdateCurrentRound {
        round: Round,
        epoch: Epoch,
    },
    UpdateValidatorSet {
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    },
    UpdatePeers {
        peers: Vec<PeerEntry<ST>>,
    },
    UpdateConfirmGroup {
        end_round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    },
    Refresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimerKind {
    SendPing,
    PingTimeout,
    RetryPeerLookup { lookup_id: u32 },
    Refresh,
}

#[derive(Debug)]
pub enum PeerDiscoveryTimerCommand<E, ST: CertificateSignatureRecoverable> {
    Schedule {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
        duration: Duration,
        on_timeout: E,
    },
    ScheduleReset {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
    },
}

#[derive(Debug)]
pub struct PeerDiscoveryMetricsCommand(ExecutorMetrics);

#[derive(Debug)]
pub enum PeerDiscoveryCommand<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    TimerCommand(PeerDiscoveryTimerCommand<PeerDiscoveryEvent<ST>, ST>),
    MetricsCommand(PeerDiscoveryMetricsCommand),
}

// A trait implemented by types that supports looking up the address of a peer by its ID.
pub trait PeerLookup {
    type SignatureType: CertificateSignatureRecoverable;

    fn lookup_addr_v4(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4>;

    fn known_addrs_v4(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<Self::SignatureType>>, SocketAddrV4>;

    fn name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    >;

    fn known_addrs(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<Self::SignatureType>>, SocketAddr> {
        self.known_addrs_v4()
            .into_iter()
            .map(|(id, addr)| (id, SocketAddr::V4(addr)))
            .collect()
    }

    fn lookup_addr(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddr> {
        self.lookup_addr_v4(id).map(SocketAddr::V4)
    }
}

pub trait PeerDiscoveryAlgo: PeerLookup {
    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        open_discovery: bool,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        request: PeerLookupRequest<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        response: PeerLookupResponse<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_current_round(
        &mut self,
        round: Round,
        epoch: Epoch,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_validator_set(
        &mut self,
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_peers(
        &mut self,
        peers: Vec<PeerEntry<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<Self::SignatureType>>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn metrics(&self) -> &ExecutorMetrics;

    fn peer_table(&self) -> PeerTable<Self::SignatureType>;
}

pub trait PeerDiscoveryAlgoBuilder {
    type PeerDiscoveryAlgoType: PeerDiscoveryAlgo;

    #[expect(clippy::type_complexity)]
    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerLookup>::SignatureType>>,
    );
}

#[derive(Debug, Clone)]
pub struct PeerTable<ST: CertificateSignatureRecoverable> {
    // for the time being we allow the raw name_records and addr_table
    // to misalign in mock tests.
    name_records: HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    addr_table: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
}

impl<ST> From<HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>>
    for PeerTable<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn from(
        name_records: HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    ) -> Self {
        let addr_table = name_records
            .iter()
            .map(|(id, record)| (*id, record.address()))
            .collect();
        Self {
            name_records,
            addr_table,
        }
    }
}

impl<ST> PeerTable<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // used for testing only
    pub(crate) fn addr_table_only(
        addr_table: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    ) -> Self {
        Self {
            name_records: HashMap::new(),
            addr_table,
        }
    }
}

impl<ST> PeerLookup for PeerTable<ST>
where
    ST: CertificateSignatureRecoverable,
{
    type SignatureType = ST;

    fn lookup_addr_v4(&self, id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddrV4> {
        self.addr_table.get(id).copied()
    }

    fn known_addrs_v4(&self) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4> {
        self.addr_table.clone()
    }

    fn name_records(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<Self::SignatureType>> {
        self.name_records.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_name_record_v4_rlp() {
        let name_record = NameRecord {
            address: SocketAddrV4::from_str("1.1.1.1:8000").unwrap(),
            seq: 2,
        };

        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);

        let result = NameRecord::decode(&mut encoded.as_slice());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), name_record);
    }
}
