use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    thread,
    time::Duration,
};

use addrlist::Addrlist;
use bytes::Bytes;
use futures::channel::oneshot;
use monoio::{spawn, time::Instant, IoUringDriver, RuntimeBuilder};
use tcp::{TcpConfig, TcpControl};
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::warn;

pub(crate) mod addrlist;
pub(crate) mod ban_expiry;
pub(crate) mod buffer_ext;
pub mod tcp;
pub mod udp;

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    trusted_addresses: Vec<IpAddr>,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    udp_up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
    tcp_connections_limit: usize,
    tcp_per_ip_connections_limit: usize,
    tcp_rps: usize,
    tcp_rps_burst: usize,
    ban_duration: Duration,
}

impl DataplaneBuilder {
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        Self {
            local_addr: *local_addr,
            udp_up_bandwidth_mbps: up_bandwidth_mbps,
            udp_buffer_size: None,
            trusted_addresses: vec![],
            tcp_connections_limit: 10000,
            tcp_per_ip_connections_limit: 100,
            tcp_rps: 1000,
            tcp_rps_burst: 100,
            ban_duration: Duration::from_secs(5 * 60), // 5 minutes
        }
    }

    /// with_buffer_size sets the buffer size for udp and tcp sockets that are managed by dataplane
    /// to a requested value.
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    /// with_tcp_connections_limit sets total and per_ip connection limit. if per_ip is zero it will be
    /// equal to total.
    pub fn with_tcp_connections_limit(mut self, total: usize, per_ip: usize) -> Self {
        self.tcp_connections_limit = total;
        self.tcp_per_ip_connections_limit = if per_ip == 0 { total } else { per_ip };
        self
    }

    /// with_tcp_rps_burst sets the rate limit and burst for tcp connections.
    pub fn with_tcp_rps_burst(mut self, rps: usize, burst: usize) -> Self {
        self.tcp_rps = rps;
        self.tcp_rps_burst = burst;
        self
    }

    /// with trusted_ips sets the list of trusted ip addresses.
    pub fn with_trusted_ips(mut self, ips: Vec<IpAddr>) -> Self {
        self.trusted_addresses = ips;
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            local_addr,
            udp_up_bandwidth_mbps: up_bandwidth_mbps,
            udp_buffer_size,
            trusted_addresses: trusted,
            tcp_connections_limit,
            tcp_per_ip_connections_limit,
            tcp_rps,
            tcp_rps_burst,
            ban_duration,
        } = self;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);
        let (banned_ips_tx, banned_ips_rx) = mpsc::unbounded_channel();
        let addrlist = Arc::new(Addrlist::new());
        for ip in trusted {
            addrlist.add_trusted(ip);
        }
        let tcp_control_map = TcpControl::new();
        thread::Builder::new()
            .name("monad-dataplane".into())
            .spawn({
                let tcp_control_map = tcp_control_map.clone();
                let addrlist = addrlist.clone();
                move || {
                    RuntimeBuilder::<IoUringDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(async move {
                            spawn(ban_expiry::task(
                                addrlist.clone(),
                                banned_ips_rx,
                                ban_duration,
                            ));

                            tcp::spawn_tasks(
                                TcpConfig {
                                    rps: tcp_rps,
                                    rps_burst: tcp_rps_burst,
                                    connections_limit: tcp_connections_limit,
                                    per_ip_connections_limit: tcp_per_ip_connections_limit,
                                },
                                tcp_control_map,
                                addrlist.clone(),
                                local_addr,
                                tcp_ingress_tx,
                                tcp_egress_rx,
                            );

                            udp::spawn_tasks(
                                local_addr,
                                udp_ingress_tx,
                                udp_egress_rx,
                                up_bandwidth_mbps,
                                udp_buffer_size,
                            );

                            futures::future::pending::<()>().await;
                        });
                }
            })
            .expect("failed to spawn dataplane thread");

        Dataplane {
            tcp_ingress_rx,
            tcp_egress_tx,
            udp_ingress_rx,
            udp_egress_tx,

            tcp_msgs_dropped: 0,
            udp_msgs_dropped: 0,
            tcp_control_map,
            notify_ban_expiry: banned_ips_tx,
            addrlist,
        }
    }
}

pub struct Dataplane {
    tcp_ingress_rx: mpsc::Receiver<(SocketAddr, Bytes)>,
    tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    udp_ingress_rx: mpsc::Receiver<RecvMsg>,
    udp_egress_tx: mpsc::Sender<(SocketAddr, Bytes, u16)>,

    tcp_control_map: TcpControl,
    notify_ban_expiry: mpsc::UnboundedSender<(IpAddr, Instant)>,
    addrlist: Arc<Addrlist>,

    tcp_msgs_dropped: usize,
    udp_msgs_dropped: usize,
}

#[derive(Clone)]
pub struct BroadcastMsg {
    pub targets: Vec<SocketAddr>,
    pub payload: Bytes,
    pub stride: u16,
}

#[derive(Clone)]
pub struct UnicastMsg {
    pub msgs: Vec<(SocketAddr, Bytes)>,
    pub stride: u16,
}

#[derive(Clone)]
pub struct RecvMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
}

pub struct TcpMsg {
    pub msg: Bytes,
    pub completion: Option<oneshot::Sender<()>>,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

impl Dataplane {
    /// add_trusted marks ip address as trusted.
    /// connections or rate limits are not applied to trusted ips.
    pub fn add_trusted(&self, addr: IpAddr) {
        self.addrlist.add_trusted(addr);
    }

    /// remove_trusted removes ip address from trusted list.
    pub fn remove_trusted(&self, addr: IpAddr) {
        self.addrlist.remove_trusted(addr);
    }

    /// ban ip address. ban duration is specified in dataplane config.
    pub fn ban(&self, ip: IpAddr) {
        let now = Instant::now();
        self.addrlist.ban(ip, now);
        self.notify_ban_expiry.send((ip, now)).unwrap();
        self.disconnect_ip(ip);
    }

    /// disconnect all connections from specified ip address.
    pub fn disconnect_ip(&self, ip: IpAddr) {
        self.tcp_control_map.disconnect_ip(ip);
    }

    /// disconnect single connection.
    pub fn disconnect(&self, addr: SocketAddr) {
        self.tcp_control_map
            .disconnect_socket(addr.ip(), addr.port());
    }

    pub async fn tcp_read(&mut self) -> (SocketAddr, Bytes) {
        self.tcp_ingress_rx
            .recv()
            .await
            .expect("tcp_ingress_rx channel should never be closed")
    }

    pub fn tcp_write(&mut self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.tcp_msgs_dropped += 1;

                warn!(
                    num_msgs_dropped = 1,
                    total_tcp_msgs_dropped = self.tcp_msgs_dropped,
                    ?addr,
                    msg_length,
                    "tcp_egress_tx channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("tcp_egress_tx channel closed"),
        }
    }

    pub async fn udp_read(&mut self) -> RecvMsg {
        self.udp_ingress_rx
            .recv()
            .await
            .expect("udp_ingress_rx channel should never be closed")
    }

    pub fn udp_write_broadcast(&mut self, msg: BroadcastMsg) {
        let num_targets = msg.targets.len();

        for (i, target) in msg.targets.into_iter().enumerate() {
            match self
                .udp_egress_tx
                .try_send((target, msg.payload.clone(), msg.stride))
            {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_targets - i;

                    self.udp_msgs_dropped += num_msgs_dropped;

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = self.udp_msgs_dropped,
                        msg_length = msg.payload.len(),
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }

    pub fn udp_write_unicast(&mut self, msg: UnicastMsg) {
        let num_msgs = msg.msgs.len();

        for (i, (addr, payload)) in msg.msgs.into_iter().enumerate() {
            match self.udp_egress_tx.try_send((addr, payload, msg.stride)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_msgs - i;

                    self.udp_msgs_dropped += num_msgs_dropped;

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = self.udp_msgs_dropped,
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }
}
