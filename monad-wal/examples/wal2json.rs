use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::Parser;
use monad_bls::BlsSignatureCollection;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::{LogFriendlyMonadEvent, MonadEvent};
use monad_proto::proto::event::ProtoMonadEvent;
use monad_secp::SecpSignature;
use monad_wal::wal::WALoggerConfig;
use serde::Serialize;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    wal_path: PathBuf,
}

type SigType = SecpSignature;
type SigColType = BlsSignatureCollection<CertificateSignaturePubKey<SigType>>;
type ExecutionProtocolType = EthExecutionProtocol;
type WalEvent = MonadEvent<SigType, SigColType, ExecutionProtocolType>;
type WrappedEvent = LogFriendlyMonadEvent<SigType, SigColType, ExecutionProtocolType>;

fn main() {
    let args = Args::parse();

    let logger_config: WALoggerConfig<WrappedEvent> = WALoggerConfig::new(args.wal_path, false);
    logger_config.events().for_each(|event| {
        let proto_event: ProtoMonadEvent = (&event.event).into();

        // TODO replace with proto serialization for LogFriendlyMonadEvent
        #[derive(Serialize)]
        struct TsEvent {
            timestamp: DateTime<Utc>,
            event: ProtoMonadEvent,
        }

        let event = TsEvent {
            timestamp: event.timestamp,
            event: proto_event,
        };

        let event_json = serde_json::to_string(&event).expect("failed to serialize event");
        println!("{}", event_json);
    })
}
