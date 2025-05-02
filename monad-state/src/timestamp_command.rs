use monad_consensus_types::{block::BlockPolicy, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{Command, MonadEvent, RouterCommand};
use monad_state_backend::StateBackend;
use monad_types::{ExecutionProtocol, NodeId, PingSequence, RouterTarget};

use crate::VerifiedMonadMessage;

pub enum BlockTimestampCommand<SCT>
where
    SCT: SignatureCollection,
{
    SendPing {
        target: NodeId<SCT::NodeIdPubKey>,
        sequence: PingSequence,
    },
    SendPong {
        target: NodeId<SCT::NodeIdPubKey>,
        sequence: PingSequence,
    },
}

impl<ST, SCT, EPT, BPT, SBT> From<BlockTimestampCommand<SCT>>
    for Vec<
        Command<
            MonadEvent<ST, SCT, EPT>,
            VerifiedMonadMessage<ST, SCT, EPT>,
            ST,
            SCT,
            EPT,
            BPT,
            SBT,
        >,
    >
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn from(command: BlockTimestampCommand<SCT>) -> Self {
        match command {
            BlockTimestampCommand::SendPing { target, sequence } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::TcpPointToPoint {
                        to: target,
                        completion: None,
                    },
                    message: VerifiedMonadMessage::PingRequest(sequence),
                })]
            }
            BlockTimestampCommand::SendPong { target, sequence } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::TcpPointToPoint {
                        to: target,
                        completion: None,
                    },
                    message: VerifiedMonadMessage::PingResponse(sequence),
                })]
            }
        }
    }
}
