use std::marker::PhantomData;

use monad_executor::Executor;
use monad_executor_glue::EvidenceCommand;

pub struct NopEvidenceCollector<P> {
    phantom: PhantomData<P>,
}

impl<P> Default for NopEvidenceCollector<P> {
    fn default() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<P> Executor for NopEvidenceCollector<P> {
    type Command = EvidenceCommand<P>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                EvidenceCommand::CollectEvidence(_e) => {}
            }
        }
    }
}
