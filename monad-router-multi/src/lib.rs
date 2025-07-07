use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use alloy_rlp::{Decodable, Encodable};
use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::DataplaneBuilder;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
};
use monad_raptorcast::{
    config::{RaptorCastConfig, SecondaryRaptorCastModeConfig},
    raptorcast_secondary::{group_message::FullNodesGroupMessage, RaptorCastSecondary},
    util::Group,
    RaptorCast, RaptorCastEvent,
};
pub use tracing::{debug, error, info, warn, Level};

//==============================================================================
pub struct MultiRouter<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    rc_primary: RaptorCast<ST, M, OM, SE, PD>,
    rc_secondary: Option<RaptorCastSecondary<ST, M, OM, SE, PD>>,
    phantom: PhantomData<(OM, SE)>,
}

impl<ST, M, OM, SE, PD> MultiRouter<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new<B>(
        cfg: RaptorCastConfig<ST>,
        dataplane_builder: DataplaneBuilder,
        peer_discovery_builder: B,
    ) -> Self
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>,
    {
        // Peer discovery needs to be shared among primary and secondary
        let pdd = PeerDiscoveryDriver::new(peer_discovery_builder);
        let shared_pdd = Arc::new(Mutex::new(pdd));

        let dataplane = dataplane_builder.build();
        let (dataplane_reader, dataplane_writer) = dataplane.split();

        // Create a Channels between primary and secondary raptorcast instances.
        // Fundamentally this is needed because, while both can send, only the
        // primary can receive data from the network.
        let (send_net_messages, recv_net_messages) =
            std::sync::mpsc::channel::<FullNodesGroupMessage<ST>>();
        let (send_group_infos, recv_group_infos) = std::sync::mpsc::channel::<Group<ST>>();

        let rc_secondary = match &cfg.secondary_instance {
            SecondaryRaptorCastModeConfig::None => None,
            _ => Some(RaptorCastSecondary::new(
                cfg.clone(),
                dataplane_writer.clone(),
                shared_pdd.clone(),
                recv_net_messages,
                send_group_infos,
            )),
        };

        let rc_primary = RaptorCast::new(cfg, dataplane_reader, dataplane_writer, shared_pdd)
            .bind_channel_to_secondary_raptorcast(send_net_messages, recv_group_infos);

        Self {
            rc_primary,
            rc_secondary,
            phantom: PhantomData,
        }
    }
}

//==============================================================================
impl<ST, M, OM, SE, PD> Executor for MultiRouter<ST, M, OM, SE, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    RaptorCast<ST, M, OM, SE, PD>: Unpin,
{
    type Command = RouterCommand<ST, OM>;

    fn exec(&mut self, a_commands: Vec<Self::Command>) {
        let mut validator_cmds = Vec::new();
        let mut fullnodes_cmds = Vec::new();
        for cmd in a_commands {
            match cmd {
                RouterCommand::Publish { .. } => validator_cmds.push(cmd),
                RouterCommand::AddEpochValidatorSet { .. } => validator_cmds.push(cmd),
                RouterCommand::GetPeers => validator_cmds.push(cmd),
                RouterCommand::UpdatePeers { .. } => validator_cmds.push(cmd),

                RouterCommand::PublishToFullNodes { .. } => fullnodes_cmds.push(cmd),
                RouterCommand::GetFullNodes => validator_cmds.push(cmd),
                RouterCommand::UpdateFullNodes { .. } => validator_cmds.push(cmd),

                RouterCommand::UpdateCurrentRound(epoch, round) => {
                    let cmd_cpy = RouterCommand::UpdateCurrentRound(epoch, round);
                    validator_cmds.push(cmd);
                    fullnodes_cmds.push(cmd_cpy);
                }
            }
        }
        self.rc_primary.exec(validator_cmds);
        if self.rc_secondary.is_some() {
            self.rc_secondary.as_mut().unwrap().exec(fullnodes_cmds);
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        let m1 = self.rc_primary.metrics();
        let res: ExecutorMetricsChain = if self.rc_secondary.is_some() {
            let m2 = self.rc_secondary.as_ref().unwrap().metrics();
            m1.chain(m2)
        } else {
            m1
        };
        res
    }
}

//==============================================================================
impl<ST, M, OM, E, PD> Stream for MultiRouter<ST, M, OM, E, PD>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Decodable,
    OM: Encodable + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, ST>>,
    Self: Unpin,
    RaptorCast<ST, M, OM, E, PD>: Unpin,
    RaptorCastSecondary<ST, M, OM, E, PD>: Unpin,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
    PeerDiscoveryDriver<PD>: Unpin,
{
    type Item = E;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let pinned_this = self.as_mut().get_mut();

        // Primary RC instance polls for inbound TCP, UDP raptorcast
        // and FullNodesGroupMessage intended for the secondary RC instance.
        match pinned_this.rc_primary.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
            Poll::Ready(None) => {
                error!("Primary RaptorCast stream ended unexpectedly.");
            }
            Poll::Pending => {}
        }

        // Secondary RC instance polls for FullNodesGroupMessage coming in from
        // the Channel Primary->Secondary.
        if self.rc_secondary.is_some() {
            let fn_stream = self.rc_secondary.as_mut().unwrap();
            match fn_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(Some(event)),
                Poll::Ready(None) => {
                    error!("Secondary RaptorCast stream ended unexpectedly.");
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}
