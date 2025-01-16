use std::fmt::Debug;

use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::ConsensusBlockBody,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hashable, Hasher, HasherType},
};
use monad_types::ExecutionProtocol;

/// Consensus protocol vote message
///
/// The signature is a protocol signature, can be collected into the
/// corresponding SignatureCollection type, used to create QC from the votes
#[derive(PartialEq, Eq, Clone)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

/// Explicitly implementing Copy because derive macro can't resolve that
/// SCT::SignatureType is actually Copy
impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.vote.hash(state);
        self.sig.hash(state);
    }
}

impl<SCT: SignatureCollection> VoteMessage<SCT> {
    pub fn new(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_hash = HasherType::hash_object(&vote);

        let sig = <SCT::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), key);

        Self { vote, sig }
    }
}

/// Consensus protocol timeout message
///
/// The signature is a protocol signature,can be collected into the
/// corresponding SignatureCollection type, used to create TC from the timeouts
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutMessage<SCT: SignatureCollection> {
    pub timeout: Timeout<SCT>,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> TimeoutMessage<SCT> {
    pub fn new(timeout: Timeout<SCT>, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let tmo_hash = timeout.tminfo.timeout_digest();
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_hash.as_ref(), key);

        Self { timeout, sig }
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for TimeoutMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.timeout.hash(state);
        self.sig.hash(state);
    }
}

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block_header: ConsensusBlockHeader<ST, SCT, EPT>,
    pub block_body: ConsensusBlockBody<EPT>,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// The last_round_tc can be independently verified. The message hash is over
/// the block only
impl<ST, SCT, EPT> Hashable for ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        self.block_header.hash(state);
    }
}
