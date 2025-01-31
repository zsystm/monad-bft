use std::{
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs},
    ops::DerefMut,
    path::PathBuf,
    task::{Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::Executor;
use monad_executor_glue::{ConfigEvent, ConfigReloadCommand, ConfigUpdate, MonadEvent};
use monad_node_config::NodeConfig;
use monad_types::{ExecutionProtocol, NodeId};

pub struct MockConfigLoader<ST, SCT, EPT> {
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> Default for MockConfigLoader<ST, SCT, EPT> {
    fn default() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

impl<ST, SCT, EPT> Stream for MockConfigLoader<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl<ST, SCT, EPT> Executor for MockConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    type Command = ConfigReloadCommand;

    fn exec(&mut self, _commands: Vec<Self::Command>) {}

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
        Default::default()
    }
}

pub struct ConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    config_path: PathBuf,
    pending_event: Option<ConfigEvent<SCT>>,
    waker: Option<Waker>,
    _phantom: PhantomData<(ST, EPT)>,
}

impl<ST, SCT, EPT> ConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    pub fn new(config_path: PathBuf) -> Self {
        Self {
            config_path,
            pending_event: None,
            waker: None,
            _phantom: PhantomData,
        }
    }

    fn resolve_domain(domain: &String) -> Result<Option<SocketAddr>, std::io::Error> {
        let mut dns_response = domain.to_socket_addrs()?;
        Ok(dns_response.next())
    }

    fn extract_config_update(node_config: NodeConfig<SCT::NodeIdPubKey>) -> ConfigEvent<SCT> {
        let mut error_message = String::new();

        let full_nodes = node_config
            .fullnode
            .identities
            .iter()
            .map(|full_node| NodeId::new(full_node.secp256k1_pubkey))
            .collect();

        let mut known_peers = Vec::new();
        let mut dns_failure = false;
        for peer in node_config.bootstrap.peers {
            let node_id = NodeId::new(peer.secp256k1_pubkey);
            let addr = match Self::resolve_domain(&peer.address) {
                Ok(Some(addr)) => addr,
                Ok(None) => {
                    dns_failure = true;
                    error_message.push_str(&format!(
                        "Bootstrap peers not updated: no dns record found for address={}\n",
                        peer.address
                    ));
                    break;
                }
                Err(err) => {
                    dns_failure = true;
                    error_message.push_str(&format!(
                        "Bootstrap peers not updated: unable to resolve address={} err={}\n",
                        peer.address, err
                    ));
                    break;
                }
            };
            known_peers.push((node_id, addr));
        }
        let maybe_known_peers = if dns_failure { None } else { Some(known_peers) };

        let blocksync_override_peers = node_config
            .blocksync_override
            .peers
            .into_iter()
            .map(|p| NodeId::new(p.secp256k1_pubkey))
            .collect();

        ConfigEvent::ConfigUpdate(ConfigUpdate {
            full_nodes,
            maybe_known_peers,
            blocksync_override_peers,
            error_message,
        })
    }

    fn reload(&mut self) {
        match std::fs::read_to_string(&self.config_path) {
            Ok(config_string) => {
                match toml::from_str::<NodeConfig<SCT::NodeIdPubKey>>(&config_string) {
                    Ok(node_config) => {
                        self.pending_event = Some(Self::extract_config_update(node_config));
                    }
                    Err(err) => self.pending_event = Some(ConfigEvent::LoadError(err.to_string())),
                };
            }
            Err(err) => {
                self.pending_event = Some(ConfigEvent::LoadError(err.to_string()));
            }
        }
    }
}

impl<ST, SCT, EPT> Stream for ConfigLoader<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(event) = this.pending_event.take() {
            return Poll::Ready(Some(MonadEvent::ConfigEvent(event)));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<ST, SCT, EPT> Executor for ConfigLoader<ST, SCT, EPT>
where
    SCT: SignatureCollection,
{
    type Command = ConfigReloadCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ConfigReloadCommand::ReloadConfig => {
                    self.reload();
                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }
    }

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
        Default::default()
    }
}
