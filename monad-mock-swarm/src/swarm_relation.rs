use bytes::Bytes;
use monad_consensus_types::{
    block::{BlockPolicy, PassthruBlockPolicy},
    block_validator::{BlockValidator, MockValidator},
    signature_collection::SignatureCollection,
    txpool::{MockTxPool, TxPool},
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    NopSignature,
};
use monad_executor_glue::{LedgerCommand, MonadEvent, StateRootHashCommand, StateSyncCommand};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{BytesRouterScheduler, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_state_backend::{InMemoryState, StateBackend};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_updaters::{
    ledger::{MockLedger, MockableLedger},
    state_root_hash::{MockStateRootHashNop, MockableStateRootHash},
    statesync::{MockStateSyncExecutor, MockableStateSync},
};
use monad_validator::{
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{BoxedValidatorSetTypeFactory, ValidatorSetFactory, ValidatorSetTypeFactory},
};

use crate::{mock::MockExecutor, node::Node, transformer::MonadMessageTransformerPipeline};

pub type SwarmRelationStateType<S> = MonadState<
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::BlockPolicyType,
    <S as SwarmRelation>::StateBackendType,
    <S as SwarmRelation>::ValidatorSetTypeFactory,
    <S as SwarmRelation>::LeaderElection,
    <S as SwarmRelation>::TxPool,
    <S as SwarmRelation>::BlockValidator,
>;
pub trait SwarmRelation
where
    Self: Sized + 'static,
    Node<Self>: Send,
    MockExecutor<Self>: Unpin,
{
    type SignatureType: CertificateSignatureRecoverable;
    type SignatureCollectionType: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
    >;
    type BlockPolicyType: BlockPolicy<Self::SignatureCollectionType, Self::StateBackendType>
        + Send
        + Sync
        + Unpin;
    type StateBackendType: StateBackend + Send + Sync + Unpin;

    type TransportMessage: PartialEq + Eq + Send + Sync + Unpin;

    type BlockValidator: BlockValidator<Self::SignatureCollectionType, Self::BlockPolicyType, Self::StateBackendType>
        + Send
        + Sync
        + Unpin;
    type ValidatorSetTypeFactory: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Send
        + Sync
        + Unpin;
    type LeaderElection: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Send
        + Sync
        + Unpin;
    type TxPool: TxPool<Self::SignatureCollectionType, Self::BlockPolicyType, Self::StateBackendType>
        + Send
        + Sync
        + Unpin;
    type Ledger: MockableLedger<
            SignatureCollection = Self::SignatureCollectionType,
            Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        > + Send
        + Unpin;

    type RouterScheduler: RouterScheduler<
            NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
            InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
            OutboundMessage = VerifiedMonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
            >,
            TransportMessage = Self::TransportMessage,
        > + Send
        + Unpin;

    type Pipeline: Pipeline<
            Self::TransportMessage,
            NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
        > + Send
        + Sync
        + Unpin;

    type StateRootHashExecutor: MockableStateRootHash<
            Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            SignatureCollection = Self::SignatureCollectionType,
        > + Send
        + Sync
        + Unpin;
    type StateSyncExecutor: MockableStateSync<
            SignatureType = Self::SignatureType,
            SignatureCollectionType = Self::SignatureCollectionType,
        > + Send
        + Sync
        + Unpin;
}

pub struct DebugSwarmRelation;
impl SwarmRelation for DebugSwarmRelation {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;

    type TransportMessage = Bytes;

    type BlockValidator = Box<
        dyn BlockValidator<
                Self::SignatureCollectionType,
                Self::BlockPolicyType,
                Self::StateBackendType,
            > + Send
            + Sync,
    >;
    type ValidatorSetTypeFactory =
        BoxedValidatorSetTypeFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = Box<
        dyn LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
            + Send
            + Sync,
    >;
    type TxPool = Box<
        dyn TxPool<Self::SignatureCollectionType, Self::BlockPolicyType, Self::StateBackendType>
            + Send
            + Sync,
    >;
    type Ledger = Box<
        dyn MockableLedger<
                SignatureCollection = Self::SignatureCollectionType,
                Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
                Command = LedgerCommand<Self::SignatureCollectionType>,
                Item = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            > + Send
            + Sync,
    >;

    type RouterScheduler = Box<
        dyn RouterScheduler<
                NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
                TransportMessage = Self::TransportMessage,
                InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
                OutboundMessage = VerifiedMonadMessage<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                >,
            > + Send
            + Sync,
    >;

    type Pipeline = Box<
        dyn Pipeline<
                Self::TransportMessage,
                NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
            > + Send
            + Sync,
    >;

    type StateRootHashExecutor = Box<
        dyn MockableStateRootHash<
                Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
                SignatureCollection = Self::SignatureCollectionType,
                Command = StateRootHashCommand<Self::SignatureCollectionType>,
                Item = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            > + Send
            + Sync,
    >;
    type StateSyncExecutor = Box<
        dyn MockableStateSync<
                SignatureType = Self::SignatureType,
                SignatureCollectionType = Self::SignatureCollectionType,
                Command = StateSyncCommand<CertificateSignaturePubKey<Self::SignatureType>>,
            > + Send
            + Sync,
    >;
}

// default swarm relation impl
pub struct NoSerSwarm;
impl SwarmRelation for NoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type Ledger = MockLedger<Self::SignatureType, Self::SignatureCollectionType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
    type StateSyncExecutor =
        MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
}

pub struct BytesSwarm;
impl SwarmRelation for BytesSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type Ledger = MockLedger<Self::SignatureType, Self::SignatureCollectionType>;

    type RouterScheduler = BytesRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
    type StateSyncExecutor =
        MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
}

pub struct MonadMessageNoSerSwarm;
impl SwarmRelation for MonadMessageNoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type Ledger = MockLedger<Self::SignatureType, Self::SignatureCollectionType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline =
        MonadMessageTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
    type StateSyncExecutor =
        MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
}
