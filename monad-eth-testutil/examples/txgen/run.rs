use monad_consensus_types::checkpoint::RootInfo;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use tokio::join;

use crate::prelude::*;
use crate::shared::erc20::ERC20;
use crate::shared::json_rpc::JsonRpc;
use crate::simple::*;

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    let root = PrivateKey::new(config.root_private_key);
    let (rpc_sender, gen_rx) = mpsc::channel(10);
    let (gen_sender, refresh_rx) = mpsc::channel(10);
    let (refresh_sender, rpc_rx) = mpsc::channel(10);
    let (recipient_sender, recipient_gen_rx) = mpsc::channel(100);

    let (erc20, nonce, native_bal) = join!(
        ERC20::deploy(&root, client.clone()),
        client.get_transaction_count(&root.0),
        client.get_balance(&root.0)
    );
    let erc20 = erc20?;

    let root = SimpleAccount {
        nonce: nonce?,
        native_bal: native_bal?,
        erc20_bal: U256::ZERO,
        key: root.1,
        addr: root.0,
    };

    let gen = Generator {
        refresh_rx,
        rpc_sender,
        recipient_sender,
        client: client.clone(),
        erc20: erc20.clone(),
        root,
        last_used_root: Instant::now() - Duration::from_secs(60 * 60),
        to_generator: ToAcctGenerator {
            seed: config.seed,
            rng: SmallRng::seed_from_u64(config.seed.into()),
            buf: VecDeque::with_capacity(1000),
        },
        seed_native: U256::from(10e9),
        min_native: U256::from(10e6),
        min_erc20: U256::from(10e3),
        mode: config.mode,
    };

    let refresher = Refresher {
        rpc_rx,
        gen_sender,
        client: client.clone(),
        erc20,
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
    };

    let rpc_sender = RpcSender {
        gen_rx,
        refresh_sender,
        client: client.clone(),
        target_tps: config.target_tps,
    };

    let recipient_tracker = RecipientTracker {
        gen_rx: recipient_gen_rx,
    };

    let (_, _, _, _) = tokio::join!(
        tokio::spawn(refresher.run()),
        tokio::spawn(rpc_sender.run()),
        tokio::spawn(gen.run(config.num_senders)),
        tokio::spawn(recipient_tracker.run())
    );

    Ok(())
}
