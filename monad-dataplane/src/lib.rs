use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use bytes::Bytes;
use futures::channel::oneshot;
use monoio::{IoUringDriver, RuntimeBuilder};
use tokio::sync::{mpsc, mpsc::error::TrySendError};
use tracing::warn;

pub(crate) mod buffer_ext;
pub mod tcp;
pub mod udp;

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
}

impl DataplaneBuilder {
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        Self {
            local_addr: *local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: None,
        }
    }

    /// with_udp_buffer_size sets the buffer size for udp socket that is managed by dataplane
    /// to a requested value.
    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: buffer_size,
        } = self;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        thread::Builder::new()
            .name("monad-dataplane".into())
            .spawn(move || {
                RuntimeBuilder::<IoUringDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("Failed building the Runtime")
                    .block_on(async move {
                        tcp::spawn_tasks(local_addr, tcp_ingress_tx, tcp_egress_rx);

                        udp::spawn_tasks(
                            local_addr,
                            udp_ingress_tx,
                            udp_egress_rx,
                            up_bandwidth_mbps,
                            buffer_size,
                        );

                        futures::future::pending::<()>().await;
                    });
            })
            .expect("failed to spawn dataplane thread");

        let writer = DataplaneWriter::new(tcp_egress_tx, udp_egress_tx);
        let reader = DataplaneReader::new(tcp_ingress_rx, udp_ingress_rx);

        Dataplane { writer, reader }
    }
}

pub struct Dataplane {
    writer: DataplaneWriter,
    reader: DataplaneReader,
}

pub struct DataplaneReader {
    tcp_ingress_rx: mpsc::Receiver<RecvTcpMsg>,
    udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

pub struct DataplaneWriter {
    tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    udp_egress_tx: mpsc::Sender<(SocketAddr, Bytes, u16)>,

    tcp_msgs_dropped: AtomicU64,
    udp_msgs_dropped: AtomicU64,
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
pub struct RecvUdpMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
}

#[derive(Clone)]
pub struct RecvTcpMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
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
    pub fn split(self) -> (DataplaneWriter, DataplaneReader) {
        (self.writer, self.reader)
    }

    pub async fn tcp_read(&mut self) -> RecvTcpMsg {
        self.reader.tcp_read().await
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        self.writer.tcp_write(addr, msg)
    }

    pub async fn udp_read(&mut self) -> RecvUdpMsg {
        self.reader.udp_read().await
    }

    pub fn udp_write_broadcast(&self, msg: BroadcastMsg) {
        self.writer.udp_write_broadcast(msg);
    }

    pub fn udp_write_unicast(&self, msg: UnicastMsg) {
        self.writer.udp_write_unicast(msg);
    }
}

impl DataplaneReader {
    fn new(
        tcp_ingress_rx: mpsc::Receiver<RecvTcpMsg>,
        udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
    ) -> Self {
        Self {
            tcp_ingress_rx,
            udp_ingress_rx,
        }
    }

    pub async fn tcp_read(&mut self) -> RecvTcpMsg {
        match self.tcp_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("tcp_ingress_rx channel closed"),
        }
    }

    pub async fn udp_read(&mut self) -> RecvUdpMsg {
        match self.udp_ingress_rx.recv().await {
            Some(msg) => msg,
            None => panic!("udp_ingress_rx channel closed"),
        }
    }

    pub fn split(self) -> (TcpReader, UdpReader) {
        (
            TcpReader(self.tcp_ingress_rx),
            UdpReader(self.udp_ingress_rx),
        )
    }
}

pub struct TcpReader(mpsc::Receiver<RecvTcpMsg>);
pub struct UdpReader(mpsc::Receiver<RecvUdpMsg>);

impl TcpReader {
    pub async fn read(&mut self) -> RecvTcpMsg {
        match self.0.recv().await {
            Some(msg) => msg,
            None => panic!("tcp_ingress_rx channel closed"),
        }
    }
}

impl UdpReader {
    pub async fn read(&mut self) -> RecvUdpMsg {
        match self.0.recv().await {
            Some(msg) => msg,
            None => panic!("udp_ingress_rx channel closed"),
        }
    }
}

impl DataplaneWriter {
    fn new(
        tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
        udp_egress_tx: mpsc::Sender<(SocketAddr, Bytes, u16)>,
    ) -> Self {
        Self {
            tcp_egress_tx,
            udp_egress_tx,
            tcp_msgs_dropped: AtomicU64::new(0),
            udp_msgs_dropped: AtomicU64::new(0),
        }
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let tcp_msgs_dropped = self.tcp_msgs_dropped.fetch_add(1, Ordering::Relaxed);

                warn!(
                    num_msgs_dropped = 1,
                    total_tcp_msgs_dropped = tcp_msgs_dropped,
                    ?addr,
                    msg_length,
                    "tcp_egress_tx channel full, dropping message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("tcp_egress_tx channel closed"),
        }
    }

    pub fn udp_write_broadcast(&self, msg: BroadcastMsg) {
        let num_targets = msg.targets.len() as u64;

        for (i, target) in msg.targets.into_iter().enumerate() {
            match self
                .udp_egress_tx
                .try_send((target, msg.payload.clone(), msg.stride))
            {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_targets - i as u64;
                    let udp_msgs_dropped = self
                        .udp_msgs_dropped
                        .fetch_add(num_msgs_dropped, Ordering::Relaxed);

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = udp_msgs_dropped,
                        msg_length = msg.payload.len(),
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }

    pub fn udp_write_unicast(&self, msg: UnicastMsg) {
        let num_msgs = msg.msgs.len() as u64;

        for (i, (addr, payload)) in msg.msgs.into_iter().enumerate() {
            match self.udp_egress_tx.try_send((addr, payload, msg.stride)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    let num_msgs_dropped = num_msgs - i as u64;
                    let udp_msgs_dropped = self
                        .udp_msgs_dropped
                        .fetch_add(num_msgs_dropped, Ordering::Relaxed);

                    warn!(
                        num_msgs_dropped,
                        total_udp_msgs_dropped = udp_msgs_dropped,
                        "udp_egress_tx channel full, dropping message"
                    );

                    return;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }
    }
}
