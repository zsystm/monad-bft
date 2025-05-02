use std::{marker::PhantomData, path::PathBuf};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_executor::Executor;
use monad_executor_glue::CheckpointCommand;
use monad_metrics::MetricsPolicy;

pub struct MockCheckpoint<SCT: SignatureCollection, MP: MetricsPolicy> {
    pub checkpoint: Option<CheckpointCommand<SCT>>,
    _phantom: PhantomData<MP>,
}

impl<SCT, MP> Default for MockCheckpoint<SCT, MP>
where
    SCT: SignatureCollection,
    MP: MetricsPolicy,
{
    fn default() -> Self {
        Self {
            checkpoint: None,
            _phantom: PhantomData,
        }
    }
}

impl<SCT, MP> Executor<MP> for MockCheckpoint<SCT, MP>
where
    SCT: SignatureCollection,
    MP: MetricsPolicy,
{
    type Command = CheckpointCommand<SCT>;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            self.checkpoint = Some(command);
        }
    }

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}

pub struct FileCheckpoint<SCT> {
    out_path: PathBuf,
    phantom: PhantomData<SCT>,
}

impl<SCT> FileCheckpoint<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(out_path: PathBuf) -> Self {
        Self {
            out_path,
            phantom: PhantomData,
        }
    }
}

impl<SCT, MP> Executor<MP> for FileCheckpoint<SCT>
where
    SCT: SignatureCollection + Clone,
    MP: MetricsPolicy,
{
    type Command = CheckpointCommand<SCT>;
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            let CheckpointCommand {
                root_seq_num,
                high_qc_round,
                checkpoint,
            } = command;
            let checkpoint_str =
                toml::to_string_pretty(&checkpoint).expect("failed to serialize checkpoint");
            let temp_path = {
                let mut file_name = self
                    .out_path
                    .file_name()
                    .expect("invalid checkpoint file name")
                    .to_owned();
                file_name.push(".wip");

                let mut temp_path = self.out_path.clone();
                temp_path.set_file_name(file_name);
                temp_path
            };
            std::fs::write(
                format!(
                    "{}.{}.{}",
                    self.out_path.to_string_lossy(),
                    root_seq_num.0,
                    high_qc_round.0,
                ),
                &checkpoint_str,
            )
            .expect("failed to write checkpoint backup");
            std::fs::write(&temp_path, &checkpoint_str).expect("failed to write checkpoint");
            std::fs::rename(&temp_path, &self.out_path).expect("failed to rename checkpoint");
        }
    }

    fn metrics(&self) -> &Self::Metrics {
        &()
    }
}
