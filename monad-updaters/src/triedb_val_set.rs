use std::{
    marker::PhantomData,
    ops::DerefMut,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::validator_data::{ValidatorSetDataWithEpoch, ValidatorsConfig};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, ValSetCommand};
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::error;

/// Updater that gets validator set updates from triedb
pub struct ValSetUpdater<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    validators_path: PathBuf,

    next_val_data: Option<ValidatorSetDataWithEpoch<SCT>>,
    last_emitted_val_data: Option<SeqNum>,
    val_set_update_interval: SeqNum,

    state_backend: SBT,

    waker: Option<Waker>,

    metrics: ExecutorMetrics,
    phantom: PhantomData<(ST, SCT)>,
}

impl<SBT> ValSetUpdater<SecpSignature, BlsSignatureCollection<PubKey>, SBT>
where
    SBT: StateBackend<SecpSignature, BlsSignatureCollection<PubKey>>,
{
    pub fn new(
        val_set_update_interval: SeqNum,
        state_backend: SBT,
        validators_path: &Path,
    ) -> Self {
        Self {
            validators_path: validators_path.to_owned(),

            next_val_data: None,
            last_emitted_val_data: None,
            val_set_update_interval,

            state_backend,

            waker: None,
            metrics: Default::default(),
            phantom: PhantomData,
        }
    }
}

impl<ST, SCT, SBT> Stream for ValSetUpdater<ST, SCT, SBT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(next_val_data) = this.next_val_data.take() {
            return Poll::Ready(Some(MonadEvent::ValidatorEvent(
                monad_executor_glue::ValidatorEvent::UpdateValidators(next_val_data),
            )));
        }

        Poll::Pending
    }
}

impl<ST, SCT, SBT> Executor for ValSetUpdater<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    type Command = ValSetCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                ValSetCommand::NotifyFinalized(seq_num) => {
                    if seq_num.is_epoch_end(self.val_set_update_interval)
                        && self.last_emitted_val_data != Some(seq_num)
                    {
                        if self.next_val_data.is_some() {
                            error!("Validator set data is not consumed");
                        }
                        let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
                        assert_eq!(
                            locked_epoch,
                            seq_num.to_epoch(self.val_set_update_interval) + Epoch(1)
                        );
                        self.next_val_data = Some(ValidatorSetDataWithEpoch {
                            epoch: locked_epoch,
                            validators: ValidatorsConfig::read_from_path(&self.validators_path)
                                // I'm hesitant to provide any fallback for this, because
                                // having the wrong validator set can be catastrophic.
                                //
                                // This file should never be manually edited anyways.
                                .expect("failed to read validators_path")
                                .get_validator_set(&locked_epoch)
                                .clone(),
                        });
                        self.last_emitted_val_data = Some(seq_num);

                        wake = true;
                    }

                    // if seq_num > SeqNum(0)
                    //     && (seq_num - SeqNum(1)).is_epoch_end(val_set_update_interval)
                    //     && self.last_emitted_val_data != Some(seq_num)
                    // {
                    //     if self.next_val_data.is_some() {
                    //         error!("Validator set data is not consumed");
                    //     }
                    //     let locked_epoch = seq_num.get_locked_epoch(self.val_set_update_interval);
                    //     assert_eq!(
                    //         locked_epoch,
                    //         seq_num.to_epoch(self.val_set_update_interval) + Epoch(1)
                    //     );
                    //     let validators = self
                    //         .state_backend
                    //         .read_next_valset(seq_num)
                    //         .into_iter()
                    //         .map(|(secp_key, bls_key, stake)| (secp_key, stake, bls_key))
                    //         .collect();
                    //     let validator_set_data: ValidatorSetData<SCT> = ValidatorSetData::new(validators);
                    //     info!(?validator_set_data, "read validator set from triedb");
                    //     self.next_val_data = Some(ValidatorSetDataWithEpoch {
                    //         epoch: locked_epoch,
                    //         validators: validator_set_data,
                    //     });
                    //     self.last_emitted_val_data = Some(seq_num);

                    //     wake = true
                    // }
                }
            }
        }

        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
