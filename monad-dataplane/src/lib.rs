// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use bytes::Bytes;
use futures::channel::oneshot;
use monoio::{IoUringDriver, RuntimeBuilder};
use tokio::sync::{mpsc, mpsc::error::TrySendError};
use tracing::warn;

pub(crate) mod buffer_ext;
pub mod tcp;
pub mod udp;

pub use udp::UdpMessageType;

pub struct DataplaneBuilder {
    local_addr: SocketAddr,
    /// 1_000 = 1 Gbps, 10_000 = 10 Gbps
    up_bandwidth_mbps: u64,
    udp_buffer_size: Option<usize>,
    direct_socket_port: Option<u16>,
}

impl DataplaneBuilder {
    pub fn new(local_addr: &SocketAddr, up_bandwidth_mbps: u64) -> Self {
        Self {
            local_addr: *local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: None,
            direct_socket_port: None,
        }
    }

    /// with_udp_buffer_size sets the buffer size for udp socket that is managed by dataplane
    /// to a requested value.
    pub fn with_udp_buffer_size(mut self, buffer_size: usize) -> Self {
        self.udp_buffer_size = Some(buffer_size);
        self
    }

    /// with_direct_socket configures an additional UDP socket for direct peer communication
    pub fn with_direct_socket(mut self, port: u16) -> Self {
        self.direct_socket_port = Some(port);
        self
    }

    pub fn build(self) -> Dataplane {
        let DataplaneBuilder {
            local_addr,
            up_bandwidth_mbps,
            udp_buffer_size: buffer_size,
            direct_socket_port,
        } = self;

        let (tcp_ingress_tx, tcp_ingress_rx) = mpsc::channel(TCP_INGRESS_CHANNEL_SIZE);
        let (tcp_egress_tx, tcp_egress_rx) = mpsc::channel(TCP_EGRESS_CHANNEL_SIZE);
        let (udp_ingress_tx, udp_ingress_rx) = mpsc::channel(UDP_INGRESS_CHANNEL_SIZE);
        let (udp_egress_tx, udp_egress_rx) = mpsc::channel(UDP_EGRESS_CHANNEL_SIZE);

        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();

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
                            direct_socket_port,
                            udp_ingress_tx,
                            udp_egress_rx,
                            up_bandwidth_mbps,
                            buffer_size,
                        );

                        ready_clone.store(true, Ordering::Release);

                        futures::future::pending::<()>().await;
                    });
            })
            .expect("failed to spawn dataplane thread");

        let writer = DataplaneWriter::new(tcp_egress_tx, udp_egress_tx);
        let reader = DataplaneReader::new(tcp_ingress_rx, udp_ingress_rx);

        Dataplane {
            writer,
            reader,
            ready,
        }
    }
}

pub struct Dataplane {
    writer: DataplaneWriter,
    reader: DataplaneReader,
    ready: Arc<AtomicBool>,
}

pub struct DataplaneReader {
    tcp_ingress_rx: mpsc::Receiver<RecvTcpMsg>,
    udp_ingress_rx: mpsc::Receiver<RecvUdpMsg>,
}

#[derive(Clone)]
pub struct DataplaneWriter {
    inner: Arc<DataplaneWriterInner>,
}

struct DataplaneWriterInner {
    tcp_egress_tx: mpsc::Sender<(SocketAddr, TcpMsg)>,
    udp_egress_tx: mpsc::Sender<(SocketAddr, UdpMsg)>,

    tcp_msgs_dropped: AtomicUsize,
    udp_msgs_dropped: AtomicUsize,
}

#[derive(Clone)]
pub struct BroadcastMsg {
    pub targets: Vec<SocketAddr>,
    pub payload: Bytes,
    pub stride: u16,
}

impl BroadcastMsg {
    fn msg_count(&self) -> usize {
        self.targets.len()
    }

    fn into_iter(self) -> impl Iterator<Item = (SocketAddr, UdpMsg)> {
        let Self {
            targets,
            payload,
            stride,
        } = self;
        targets.into_iter().map(move |dst| {
            (
                dst,
                UdpMsg {
                    payload: payload.clone(),
                    stride,
                    msg_type: UdpMessageType::Broadcast,
                },
            )
        })
    }
}

#[derive(Clone)]
pub struct UnicastMsg {
    pub msgs: Vec<(SocketAddr, Bytes)>,
    pub stride: u16,
}

impl UnicastMsg {
    fn msg_count(&self) -> usize {
        self.msgs.len()
    }

    fn into_iter(self) -> impl Iterator<Item = (SocketAddr, UdpMsg)> {
        let Self { msgs, stride } = self;
        msgs.into_iter().map(move |(dst, payload)| {
            (
                dst,
                UdpMsg {
                    payload,
                    stride,
                    msg_type: UdpMessageType::Broadcast,
                },
            )
        })
    }
}

#[derive(Clone)]
pub struct RecvUdpMsg {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
    pub msg_type: UdpMessageType,
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

pub struct UdpMsg {
    pub payload: Bytes,
    pub stride: u16,
    pub msg_type: UdpMessageType,
}

const TCP_INGRESS_CHANNEL_SIZE: usize = 1024;
const TCP_EGRESS_CHANNEL_SIZE: usize = 1024;
const UDP_INGRESS_CHANNEL_SIZE: usize = 12_800;
const UDP_EGRESS_CHANNEL_SIZE: usize = 12_800;

impl Dataplane {
    pub fn split(self) -> (DataplaneReader, DataplaneWriter) {
        (self.reader, self.writer)
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

    pub fn udp_write_direct(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        self.writer.udp_write_direct(dst, payload, stride);
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn block_until_ready(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while !self.ready() {
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        true
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
        udp_egress_tx: mpsc::Sender<(SocketAddr, UdpMsg)>,
    ) -> Self {
        let inner = DataplaneWriterInner {
            tcp_egress_tx,
            udp_egress_tx,
            tcp_msgs_dropped: AtomicUsize::new(0),
            udp_msgs_dropped: AtomicUsize::new(0),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn tcp_write(&self, addr: SocketAddr, msg: TcpMsg) {
        let msg_length = msg.msg.len();

        match self.inner.tcp_egress_tx.try_send((addr, msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let tcp_msgs_dropped = self.inner.tcp_msgs_dropped.fetch_add(1, Ordering::Relaxed);

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

    #[tracing::instrument(
        level="trace", 
        skip_all,
        fields(len = msg.payload.len(), targets = msg.targets.len())
    )]
    pub fn udp_write_broadcast(&self, msg: BroadcastMsg) {
        let mut pending_count = msg.msg_count();
        let msg_len = msg.payload.len();

        for (dst, udp_msg) in msg.into_iter() {
            match self.inner.udp_egress_tx.try_send((dst, udp_msg)) {
                Ok(()) => {
                    pending_count -= 1;
                }
                Err(TrySendError::Full(_)) => {
                    break;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }

        if pending_count == 0 {
            return;
        }

        let udp_msgs_dropped = self
            .inner
            .udp_msgs_dropped
            .fetch_add(pending_count, Ordering::Relaxed);

        warn!(
            num_msgs_dropped = pending_count,
            total_udp_msgs_dropped = udp_msgs_dropped,
            msg_length = msg_len,
            "udp_egress_tx channel full, dropping message"
        );
    }

    #[tracing::instrument(
        level="trace", 
        skip_all,
        fields(msgs = msg.msgs.len())
    )]
    pub fn udp_write_unicast(&self, msg: UnicastMsg) {
        let mut pending_count = msg.msg_count();

        for (dst, udp_msg) in msg.into_iter() {
            match self.inner.udp_egress_tx.try_send((dst, udp_msg)) {
                Ok(()) => {
                    pending_count -= 1;
                }
                Err(TrySendError::Full(_)) => {
                    break;
                }
                Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
            }
        }

        if pending_count == 0 {
            return;
        }

        let udp_msgs_dropped = self
            .inner
            .udp_msgs_dropped
            .fetch_add(pending_count, Ordering::Relaxed);

        warn!(
            num_msgs_dropped = pending_count,
            total_udp_msgs_dropped = udp_msgs_dropped,
            "udp_egress_tx channel full, dropping message"
        );
    }

    pub fn udp_write_direct(&self, dst: SocketAddr, payload: Bytes, stride: u16) {
        let msg_length = payload.len();
        let udp_msg = UdpMsg {
            payload,
            stride,
            msg_type: UdpMessageType::Direct,
        };

        match self.inner.udp_egress_tx.try_send((dst, udp_msg)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let udp_msgs_dropped = self.inner.udp_msgs_dropped.fetch_add(1, Ordering::Relaxed);

                warn!(
                    num_msgs_dropped = 1,
                    total_udp_msgs_dropped = udp_msgs_dropped,
                    ?dst,
                    msg_length,
                    "udp_egress_tx channel full, dropping direct message"
                );
            }
            Err(TrySendError::Closed(_)) => panic!("udp_egress_tx channel closed"),
        }
    }
}
