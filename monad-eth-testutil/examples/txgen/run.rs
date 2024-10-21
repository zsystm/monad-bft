use rand::rngs::SmallRng;
use rand::SeedableRng;
use tokio::join;

use crate::prelude::*;
use crate::shared::erc20::ERC20;
use crate::shared::json_rpc::JsonRpc;
use crate::workers::*;

pub async fn run(client: ReqwestClient, config: Config) -> Result<()> {
    let root = PrivateKey::new(config.root_private_key);
    let (rpc_sender, gen_rx) = mpsc::channel(10);
    let (gen_sender, refresh_rx) = mpsc::channel(100);
    let (refresh_sender, rpc_rx) = mpsc::channel(10);
    let (recipient_sender, recipient_gen_rx) = mpsc::channel(10000);

    let (erc20, nonce, native_bal) = join!(
        ERC20::deploy(&root, client.clone()),
        client.get_transaction_count(&root.0),
        client.get_balance(&root.0)
    );
    let erc20 = erc20?;

    let root = SimpleAccount {
        nonce: nonce? + 1, // erc20 deploy
        native_bal: native_bal?,
        erc20_bal: U256::ZERO,
        key: root.1,
        addr: root.0,
    };

    let metrics = Arc::new(Metrics::default());

    let gen = Generator {
        refresh_rx,
        rpc_sender,
        client: client.clone(),
        erc20: erc20.clone(),
        root,
        last_used_root: Instant::now() - Duration::from_secs(60 * 60),
        to_generator: ToAcctGenerator {
            seed: config.seed,
            rng: SmallRng::seed_from_u64(config.seed.into()),
            buf: Vec::with_capacity(1000),
        },
        seed_native: U256::from(10e12),
        min_native: U256::from(10e7),
        min_erc20: U256::from(10e3),
        mode: config.mode,
        metrics: Arc::clone(&metrics),
    };

    let refresher = Refresher {
        rpc_rx,
        gen_sender,
        client: client.clone(),
        erc20,
        delay: Duration::from_secs_f64(config.refresh_delay_secs),
        metrics: Arc::clone(&metrics),
    };

    let rpc_sender = RpcSender {
        gen_rx,
        recipient_sender,
        refresh_sender,
        client: client.clone(),
        target_tps: config.tps,
        metrics: Arc::clone(&metrics),
    };

    let recipient_tracker = RecipientTracker {
        rpc_sender_rx: recipient_gen_rx,
        client,
        erc20,
        delay: refresher.delay,
        metrics: Arc::clone(&metrics),
    };

    let (_, _, _, _, _) = tokio::join!(
        tokio::spawn(refresher.run()),
        tokio::spawn(rpc_sender.run()),
        tokio::spawn(gen.run(config.num_senders)),
        tokio::spawn(recipient_tracker.run()),
        tokio::spawn(metrics.run()),
    );

    Ok(())
}
