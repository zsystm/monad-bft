use std::{sync::Arc, time::Duration};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{NodeId, Round};

pub struct RaptorCastConfig<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // Both primary and secondary instances need to sign raptorcast messages but
    // CertificateKeyPair can't be copied. This Config is also passed (as &ref)
    // to both primary and secondary instances, so we don't wan't to partially
    // move the Config when wrapping shared_key into an Arc during the ctor.
    pub shared_key: Arc<ST::KeyPairType>,

    // For splitting large app messages (e.g. block proposals) into chunks that
    // fit into UDP datagrams for the raptorcast protocol.
    pub mtu: u16,

    // The primary instance owns the receive side of the UDP traffic used for
    // raptorcast and hence is mandatory in all configuration cases.
    pub primary_instance: RaptorCastConfigPrimary<ST>,

    // The secondary instance deals with (re-) broadcasts to full-nodes.
    // Broadcasting when we are running as validator, re-broadcasting when
    // running as full-node.
    // Validators and full-nodes who do not want to participate in validator-
    // to-full-node raptor-casting may opt out of this.
    pub secondary_instance: SecondaryRaptorCastModeConfig<ST>,
}

impl<ST> Clone for RaptorCastConfig<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn clone(&self) -> Self {
        RaptorCastConfig {
            shared_key: self.shared_key.clone(),
            mtu: self.mtu,
            primary_instance: self.primary_instance.clone(),
            secondary_instance: self.secondary_instance.clone(),
        }
    }
}

/// Configuration for the primary instance of RaptorCast (group of validators)
#[derive(Clone)]
pub struct RaptorCastConfigPrimary<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // This refers to the full-nodes we as a validator will be broadcasting full
    // app-messages (e.g. block proposals) directly
    pub fullnode_dedicated: Vec<NodeId<CertificateSignaturePubKey<ST>>>,

    /// Amount of redundancy (in Raptor10 encoding) to send.
    /// A value of 2 == send 2x total payload size.
    /// Higher values make the broadcasting more tolerant to UDP packet drops.
    /// This applies to raptor-casting across validator
    pub raptor10_redundancy: u8,
}

impl<ST> Default for RaptorCastConfigPrimary<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn default() -> RaptorCastConfigPrimary<ST> {
        RaptorCastConfigPrimary {
            fullnode_dedicated: Vec::new(),
            raptor10_redundancy: 3, // for validators
        }
    }
}

/// Configuration for the secondary instance of RaptorCast (group of full-nodes)
#[derive(Clone)]
pub enum SecondaryRaptorCastModeConfig<ST>
where
    ST: CertificateSignatureRecoverable,
{
    None, // Not participating in any raptor-casting to full-nodes.
    Client(RaptorCastConfigSecondaryClient), // i.e. we are a full-node
    Publisher(RaptorCastConfigSecondaryPublisher<ST>), // we are a validator
}

#[derive(Clone)]
pub struct RaptorCastConfigSecondaryClient {
    // This determines whether we as a full node will accept an invite to join
    // some validator's temporary raptorcast group.
    pub bandwidth_cost_per_group_member: u64,
    pub bandwidth_capacity: u64,
    // When being invited to a raptorcast group, we will only accept the invite
    // if the group is not too far or too soon in the future, unless we haven't
    // seen any proposals in `invite_accept_heartbeat`
    pub invite_future_dist_min: Round,
    pub invite_future_dist_max: Round,
    pub invite_accept_heartbeat: Duration,
    /// This applies when sending point-to-point group messages
    pub raptor10_redundancy: u8,
}

impl Default for RaptorCastConfigSecondaryClient {
    fn default() -> RaptorCastConfigSecondaryClient {
        RaptorCastConfigSecondaryClient {
            bandwidth_cost_per_group_member: 1,
            bandwidth_capacity: u64::MAX,
            invite_future_dist_min: Round(1),
            invite_future_dist_max: Round(600), // ~5 minutes into the future, with current round length of 500ms
            invite_accept_heartbeat: Duration::from_secs(10),
            raptor10_redundancy: 2,
        }
    }
}

// Only relevant to the secondary RaptorCast instance, and only when running as full-node
#[derive(Clone)]
pub struct RaptorCastConfigSecondaryPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    /// These are the full-nodes that we, as validator, will always ask to join
    /// raptorcast groups. AKA `always_ask_full_nodes`.`
    pub full_nodes_prioritized: Vec<NodeId<CertificateSignaturePubKey<ST>>>,

    /// Group here means a temporary raptorcast group consisting of random
    /// full-nodes and which only lasts for a few rounds.
    pub group_scheduling: GroupSchedulingConfig,

    /// This applies to raptor-casting across full-nodes
    pub raptor10_redundancy: u8,
}

impl<ST> Default for RaptorCastConfigSecondaryPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn default() -> RaptorCastConfigSecondaryPublisher<ST> {
        RaptorCastConfigSecondaryPublisher {
            full_nodes_prioritized: Vec::new(),
            group_scheduling: Default::default(),
            raptor10_redundancy: 2, // for full-nodes
        }
    }
}

#[derive(Clone, Copy)]
pub struct GroupSchedulingConfig {
    pub max_group_size: usize, // how large should groups be, in number of nodes
    pub round_span: Round,     // how many rounds should groups last for
    pub invite_lookahead: Round, // how many rounds (or time) in advance should we start inviting full-nodes to future groups
    pub max_invite_wait: Round, // how long to wait for a node to respond to an invite before we try some other ones.
    pub deadline_round_dist: Round, // group formation deadlined in terms of how close to the group's start_round we are
    pub init_empty_round_span: Round, // like round_span but for the case when we need an empty, locked group right now
}

impl Default for GroupSchedulingConfig {
    fn default() -> GroupSchedulingConfig {
        // Note: by default a Round lasts 500ms. See config entry ChainParams::vote_pace
        GroupSchedulingConfig {
            max_group_size: 50,
            round_span: Round(120),
            invite_lookahead: Round(1200),
            max_invite_wait: Round(2),
            deadline_round_dist: Round(10),
            init_empty_round_span: Round(4 * 2 + 10), // allow for 4 invite timeout ticks + deadline_round_dist
        }
    }
}
