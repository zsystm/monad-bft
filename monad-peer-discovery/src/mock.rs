use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    net::SocketAddrV4,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId, Round};
use tracing::debug;

use crate::{
    MonadNameRecord, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand, PeerLookup,
    PeerLookupRequest, PeerLookupResponse, Ping, Pong,
};

pub struct NopDiscovery<ST: CertificateSignatureRecoverable> {
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    metrics: ExecutorMetrics,
    pd: PhantomData<ST>,
}

pub struct NopDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    pub pd: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> Default for NopDiscoveryBuilder<ST> {
    fn default() -> Self {
        Self {
            known_addresses: HashMap::new(),
            pd: PhantomData,
        }
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder for NopDiscoveryBuilder<ST> {
    type PeerDiscoveryAlgoType = NopDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerLookup>::SignatureType>>,
    ) {
        let state = NopDiscovery {
            known_addresses: self.known_addresses,
            metrics: ExecutorMetrics::default(),
            pd: PhantomData,
        };
        let cmds = Vec::new();

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> PeerLookup for NopDiscovery<ST> {
    type SignatureType = ST;

    fn lookup_addr_v4(&self, id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddrV4> {
        self.known_addresses.get(id).copied()
    }

    fn known_addrs_v4(&self) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4> {
        self.known_addresses.clone()
    }

    fn name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    > {
        HashMap::new()
    }
}

impl<ST> PeerDiscoveryAlgo for NopDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?target, "handle send ping");

        Vec::new()
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping, "handle ping");

        Vec::new()
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong, "handle pong");

        Vec::new()
    }

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?ping_id, "handling ping timeout");

        Vec::new()
    }

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        _open_discovery: bool,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");

        Vec::new()
    }

    fn handle_peer_lookup_timeout(
        &mut self,
        _to: NodeId<CertificateSignaturePubKey<ST>>,
        _target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?lookup_id, "peer lookup request timeout");

        Vec::new()
    }

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("pruning unresponsive peer nodes");

        Vec::new()
    }

    fn update_current_round(
        &mut self,
        round: Round,
        epoch: Epoch,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?round, ?epoch, "updating current round");

        Vec::new()
    }

    fn update_validator_set(
        &mut self,
        _epoch: Epoch,
        _validators: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("updating validator set");

        Vec::new()
    }

    fn update_peers(&mut self, peers: Vec<PeerEntry<ST>>) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("updating peers");

        for peer in peers {
            let node_id = NodeId::new(peer.pubkey);
            self.known_addresses.insert(node_id, peer.addr);
        }

        Vec::new()
    }

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?round, ?peers, "updating peer participation");

        Vec::new()
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn peer_table(&self) -> crate::PeerTable<ST> {
        crate::PeerTable::addr_table_only(self.known_addresses.clone())
    }
}
