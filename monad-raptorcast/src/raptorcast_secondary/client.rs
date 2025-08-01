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

use std::{collections::BTreeMap, time::Instant};

use iset::IntervalMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_types::{NodeId, Round, RoundSpan, GENESIS_ROUND};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, warn};

use super::{
    super::{config::RaptorCastConfigSecondaryClient, util::Group},
    group_message::{ConfirmGroup, FullNodesGroupMessage, PrepareGroup, PrepareGroupResponse},
};

/// Metrics constant
pub const CLIENT_NUM_CURRENT_GROUPS: &str =
    "monad.bft.raptorcast.secondary.client.num_current_groups";
pub const CLIENT_RECEIVED_INVITES: &str = "monad.bft.raptorcast.secondary.client.received_invites";
pub const CLIENT_RECEIVED_CONFIRMS: &str =
    "monad.bft.raptorcast.secondary.client.received_confirms";

type GroupAsClient<ST> = Group<ST>;

// This is for when the router is playing the role of a client
// That is, we are a full-node receiving group invites from a validator
pub struct Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    client_node_id: NodeId<CertificateSignaturePubKey<ST>>, // Our (full-node) node_id as an invitee

    // Full nodes may choose to reject a request if it doesn’t have enough
    // upload bandwidth to broadcast chunk to a large group.
    config: RaptorCastConfigSecondaryClient,

    // [start_round, end_round) -> GroupAsClient
    // Represents all raptorcast groups that we have accepted. They may overlap.
    confirmed_groups: IntervalMap<Round, GroupAsClient<ST>>,

    // start_round -> validator_id -> group invite
    // Once we receive an invite, we remember how the invite looked like, so
    // that we don't blindly accept any confirmation message.
    pending_confirms:
        BTreeMap<Round, BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PrepareGroup<ST>>>,

    // Once a group is confirmed, it is sent to this channel
    group_sink_channel: UnboundedSender<GroupAsClient<ST>>,

    // For avoiding accepting invites/confirms for rounds we've already started
    curr_round: Round,

    // Helps bootstrap the full-node when it is not yet receiving proposals and
    // cannot call enter_round() to advance state as it doesn't know what round
    // we are at.
    last_round_heartbeat: Instant,

    // Metrics
    metrics: ExecutorMetrics,
}

impl<ST> Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        client_node_id: NodeId<CertificateSignaturePubKey<ST>>,
        group_sink_channel: UnboundedSender<GroupAsClient<ST>>,
        config: RaptorCastConfigSecondaryClient,
    ) -> Self {
        // There's no Instant::zero(). Use a value such that we will accept the
        // first invite, even though its far off `curr_round`.
        let last_round_heartbeat = Instant::now() - config.invite_accept_heartbeat;
        Self {
            client_node_id,
            config,
            confirmed_groups: IntervalMap::new(),
            pending_confirms: BTreeMap::new(),
            group_sink_channel,
            curr_round: GENESIS_ROUND,
            last_round_heartbeat,
            metrics: ExecutorMetrics::default(),
        }
    }

    // Called from UpdateCurrentRound
    pub fn enter_round(&mut self, new_round: Round) {
        // Sanity check on curr_round
        if new_round < self.curr_round {
            error!(
                "RaptorCastSecondary ignoring backwards round \
                {:?} -> {:?}",
                self.curr_round, new_round
            );
            return;
        } else if new_round > self.curr_round + Round(1) {
            debug!(
                "RaptorCastSecondary detected round gap \
                {:?} -> {:?}",
                self.curr_round, new_round
            );
        }

        self.curr_round = new_round;
        self.last_round_heartbeat = Instant::now();

        // Clean up old invitations.
        self.pending_confirms.retain(|&key, _| key > new_round);

        // Send out group information to the Primary instance, so that it can
        // re-broadcast raptorcast chunks. This is the normal path when we are
        // receiving proposals and thus the round increases.
        let consume_end = new_round + Round(1);
        let mut current_group_count = 0;
        for group in self.confirmed_groups.values(new_round..consume_end) {
            current_group_count += 1;
            if let Err(error) = self.group_sink_channel.send(group.clone()) {
                error!(
                    "Failed to send group to secondary Raptorcast instance: {}",
                    error
                );
            }
        }
        self.metrics[CLIENT_NUM_CURRENT_GROUPS] = current_group_count;

        // Remove all groups that should already have been consumed
        let mut keys_to_remove = Vec::new();
        for iv in self.confirmed_groups.intervals(..consume_end) {
            keys_to_remove.push(iv);
        }
        for iv in keys_to_remove {
            self.confirmed_groups.remove(iv);
        }
    }

    // If we are not receiving proposals, then we don't know what the current
    // round is, and should advance the state machine despite self.curr_round
    fn is_receiving_proposals(&self) -> bool {
        Instant::now() < self.last_round_heartbeat + self.config.invite_accept_heartbeat
    }

    fn validate_prepare_group_message(&self, invite_msg: &PrepareGroup<ST>) -> bool {
        // Sanity check the message
        if invite_msg.start_round >= invite_msg.end_round {
            warn!(
                "RaptorCastSecondary rejecting invite message due to \
                        failed sanity check: {:?}",
                invite_msg
            );
            return false;
        }

        // Reject late round
        if invite_msg.start_round <= self.curr_round {
            warn!(
                "RaptorCastSecondary rejecting invite for round that \
                        already started, curr_round {:?}, invite = {:?}",
                self.curr_round, invite_msg
            );
            return false;
        }

        // Reject invite outside config invitation bounds, unless we are
        // not receiving any proposals ATM and should accept first group
        // invite, even if far from what we believe is the current round.
        if (invite_msg.start_round < self.curr_round + self.config.invite_future_dist_min
            || invite_msg.start_round > self.curr_round + self.config.invite_future_dist_max)
            && self.is_receiving_proposals()
        {
            warn!(
                curr_round =? self.curr_round,
                ?invite_msg,
                "RaptorCastSecondary rejecting invite outside bounds \
                        [{:?}, {:?}]",
                self.config.invite_future_dist_min,
                self.config.invite_future_dist_max,
            );
            return false;
        }

        let mut bandwidth = BandwidthAggregator::new(
            self.config.bandwidth_capacity,
            self.config.bandwidth_cost_per_group_member,
        );

        let log_bandwidth_overflow = || {
            debug!(
                validator_id =? invite_msg.validator_id,
                "RaptorCastSecondary rejected invite for rounds \
                        [{:?}, {:?}) due to low bandwidth",
                invite_msg.start_round, invite_msg.end_round,
            );
        };

        if bandwidth.add_and_check(invite_msg.max_group_size).is_none() {
            log_bandwidth_overflow();
            return false;
        };

        // Check confirmed groups
        for group in self
            .confirmed_groups
            .values(invite_msg.start_round..invite_msg.end_round)
        {
            // Check if we already have an overlapping invite from same
            // validator, e.g. [30, 40)->validator3 but we already
            // have [25, 35)->validator3
            // Note that we accept overlaps across different validators,
            // e.g. [30, 40)->validator3 + [25, 35)->validator4
            if group.get_validator_id() == &invite_msg.validator_id {
                warn!(
                validator_id =? invite_msg.validator_id,
                    "RaptorCastSecondary received self-overlapping \
                            invite for rounds [{:?}, {:?})",
                    invite_msg.start_round, invite_msg.end_round
                );
                return false;
            }

            // Check that we'll have enough bandwidth during round span
            if bandwidth.add_and_check(group.size_excl_self()).is_none() {
                log_bandwidth_overflow();
                return false;
            }
        }

        // Check groups we were invited to but are still unconfirmed
        for (&key, other_invites) in self.pending_confirms.iter() {
            if key >= invite_msg.end_round {
                // Remaining keys are outside the invite range
                break;
            }

            for other in other_invites.values() {
                if !Self::overlaps(other.start_round, other.end_round, invite_msg) {
                    continue;
                }

                if bandwidth.add_and_check(other.max_group_size).is_none() {
                    log_bandwidth_overflow();
                    return false;
                }
            }
        }

        true
    }

    fn handle_prepare_group_message(
        &mut self,
        invite_msg: PrepareGroup<ST>,
    ) -> PrepareGroupResponse<ST> {
        debug!(
            ?invite_msg,
            "RaptorCastSecondary Client received group invite"
        );
        self.metrics[CLIENT_RECEIVED_INVITES] += 1;

        // Check the invite for duplicates & bandwidth requirements
        let accept = self.validate_prepare_group_message(&invite_msg);

        if accept {
            // Let's remember about this invite so that we don't blindly
            // accept any confirmation message.
            self.pending_confirms
                .entry(invite_msg.start_round)
                .or_default()
                .insert(invite_msg.validator_id, invite_msg.clone());
        }

        PrepareGroupResponse {
            req: invite_msg,
            node_id: self.client_node_id,
            accept,
        }
    }

    fn handle_confirm_group_message(&mut self, confirm_msg: ConfirmGroup<ST>) {
        let start_round = &confirm_msg.prepare.start_round;

        // Drop the message if the round span is invalid
        let Some(round_span) = RoundSpan::new(
            confirm_msg.prepare.start_round,
            confirm_msg.prepare.end_round,
        ) else {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary ignoring invalid round span"
            );
            return;
        };

        // Drop the group if we've already entered the round
        if start_round <= &self.curr_round {
            warn!(
                curr_round =? self.curr_round,
                ?confirm_msg,
                "RaptorCastSecondary ignoring late confirm"
            );
            return;
        }

        let is_receiving_proposals = self.is_receiving_proposals();
        let Some(invites) = self.pending_confirms.get_mut(start_round) else {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary Ignoring confirmation message \
                        for unrecognized start round"
            );
            return;
        };

        let maybe_entry = invites.get(&confirm_msg.prepare.validator_id);
        let Some(old_invite) = maybe_entry else {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary ignoring ConfirmGroup from \
                            unrecognized validator id"
            );
            return;
        };

        if old_invite != &confirm_msg.prepare {
            warn!(
                expected_old_invite =? old_invite,
                received_msg =? confirm_msg.prepare,
                "RaptorCastSecondary ignoring ConfirmGroup that \
                                doesn't match the original invite"
            );
            return;
        }

        if confirm_msg.peers.len() > confirm_msg.prepare.max_group_size {
            warn!(
                actual_size =? confirm_msg.peers.len(),
                max_group_size =? confirm_msg.prepare.max_group_size,
                ?confirm_msg,
                "RaptorCastSecondary ignoring ConfirmGroup that \
                                is larger than the promised max_group_size"
            );
            return;
        }

        if !confirm_msg.peers.contains(&self.client_node_id) {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary ignoring ConfirmGroup \
                                with a group that does not contain our node_id"
            );
            return;
        }

        let group = GroupAsClient::new_fullnode_group(
            confirm_msg.peers,
            &self.client_node_id,
            confirm_msg.prepare.validator_id,
            round_span,
        );

        // Send the group to primary instance right away.
        // Doing so helps resolve activation of groups for
        // bootstrapping full-nodes, as they aren't receiving
        // proposals and hence can't advance (enter) rounds.
        // The primary instance use the group for determining
        // which other full-nodes to re-broadcast raptorcast
        // chunks to.
        if !is_receiving_proposals {
            if let Err(error) = self.group_sink_channel.send(group.clone()) {
                tracing::error!(
                    ?error,
                    "RaptorCastSecondary failed to send group to primary \
                                        Raptorcast instance"
                );
            }
            // Pulse a heartbeat, giving the new group above some time to be
            // picked up by UDP state and be used. Without this pulse, there's
            // a risk that the same validator will send us an invite for a
            // future group before we receive the first proposal via raptorcast,
            // causing the `is_receiving_proposals` check above to eagerly send
            // the future group to primary and eject the current one.
            self.last_round_heartbeat = Instant::now();
        }

        self.metrics[CLIENT_RECEIVED_CONFIRMS] += 1;
        debug!(
            start_round =? confirm_msg.prepare.start_round,
            end_round =? confirm_msg.prepare.end_round,
            validator =? confirm_msg.prepare.validator_id,
            "RaptorCastSecondary Client confirmed group"
        );

        self.confirmed_groups
            .force_insert(round_span.start..round_span.end, group);
        invites.remove(&confirm_msg.prepare.validator_id);
    }

    // Called when group invite or group confirmation is received from validator
    pub fn on_receive_group_message(
        &mut self,
        msg: FullNodesGroupMessage<ST>,
    ) -> Option<(
        FullNodesGroupMessage<ST>,
        NodeId<CertificateSignaturePubKey<ST>>,
    )> {
        match msg {
            FullNodesGroupMessage::PrepareGroup(invite_msg) => {
                let dest_node_id = invite_msg.validator_id;
                let resp = self.handle_prepare_group_message(invite_msg);
                Some((
                    FullNodesGroupMessage::PrepareGroupResponse(resp),
                    dest_node_id,
                ))
            }
            FullNodesGroupMessage::ConfirmGroup(confirm_msg) => {
                self.handle_confirm_group_message(confirm_msg);
                None
            }
            FullNodesGroupMessage::PrepareGroupResponse(_) => {
                error!(
                    "RaptorCastSecondary client received a \
                                PrepareGroupResponse message"
                );
                None
            }
        }
    }

    fn overlaps(begin: Round, end: Round, group: &PrepareGroup<ST>) -> bool {
        assert!(begin <= end);
        assert!(group.start_round <= group.end_round);
        group.start_round < end && group.end_round > begin
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    #[cfg(test)]
    pub fn get_curr_round(&self) -> Round {
        self.curr_round
    }

    #[cfg(test)]
    pub fn get_client_node_id(&self) -> NodeId<CertificateSignaturePubKey<ST>> {
        self.client_node_id
    }

    #[cfg(test)]
    pub fn num_pending_confirms(&self) -> usize {
        self.pending_confirms.len()
    }
}

struct BandwidthAggregator {
    cap: u64,
    sum: u64,
    cost_per_node: u64,
}

impl BandwidthAggregator {
    fn new(cap: u64, cost_per_node: u64) -> Self {
        Self {
            cap,
            cost_per_node,
            sum: 0,
        }
    }

    fn add_and_check(&mut self, group_size: usize) -> Option<()> {
        let group_size = group_size as u64;
        let group_bandwidth = group_size.checked_mul(self.cost_per_node)?;
        let updated_sum = self.sum.checked_add(group_bandwidth)?;
        if updated_sum > self.cap {
            return None;
        }

        self.sum = updated_sum;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    use super::{
        super::{
            super::{config::RaptorCastConfigSecondaryClient, util::Group},
            Client,
        },
        *,
    };

    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type RcToRcChannelGrp<ST> = (UnboundedSender<Group<ST>>, UnboundedReceiver<Group<ST>>);

    #[test]
    fn malformed_prepare_messages() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp<ST> = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                // we can broadcast to most 5 peers
                bandwidth_cost_per_group_member: u64::MAX / 5,
                bandwidth_capacity: u64::MAX,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        clt.confirmed_groups.insert(
            Round(1)..Round(5),
            GroupAsClient::new_fullnode_group(
                vec![self_id, nid(3), nid(4), nid(5)],
                &self_id,
                nid(2),
                RoundSpan::new(Round(1), Round(5)).unwrap(),
            ),
        );

        let malformed_messages = [
            // group size overflow
            PrepareGroup {
                max_group_size: usize::MAX,
                validator_id: nid(2),
                start_round: Round(5),
                end_round: Round(6),
            },
            // invalid round span
            PrepareGroup {
                max_group_size: 1,
                validator_id: nid(2),
                start_round: Round(7),
                end_round: Round(5),
            },
        ];

        for message in malformed_messages {
            // handles without crashing
            let resp = clt.handle_prepare_group_message(message);
            assert!(!resp.accept)
        }
    }

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }
}
