use std::{cmp::Reverse, collections::HashMap, time::Duration};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::RouterCommand;
use monad_peer_discovery::{
    PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand, PeerDiscoveryEvent,
    PeerDiscoveryMessage, PeerDiscoveryTimerCommand, TimerKind,
};
use monad_types::{NodeId, RouterTarget};
use priority_queue::PriorityQueue;

#[derive(Debug)]
struct TimerEvent<E> {
    expire_at: Duration,
    on_timeout: E,
}

struct MockDiscTimer<E, ST: CertificateSignatureRecoverable> {
    timers: HashMap<(NodeId<CertificateSignaturePubKey<ST>>, TimerKind), TimerEvent<E>>,
    priority_queue:
        PriorityQueue<(NodeId<CertificateSignaturePubKey<ST>>, TimerKind), Reverse<Duration>>,

    tick: Duration,
}

impl<E, ST: CertificateSignatureRecoverable> Default for MockDiscTimer<E, ST> {
    fn default() -> Self {
        Self {
            timers: Default::default(),
            priority_queue: Default::default(),
            tick: Default::default(),
        }
    }
}

impl<E, ST: CertificateSignatureRecoverable> MockDiscTimer<E, ST> {
    fn exec(&mut self, cmds: Vec<PeerDiscoveryTimerCommand<E, ST>>) {
        for cmd in cmds {
            match cmd {
                PeerDiscoveryTimerCommand::Schedule {
                    node_id,
                    timer_kind,
                    duration,
                    on_timeout,
                } => {
                    let event = TimerEvent {
                        expire_at: self.tick + duration,
                        on_timeout,
                    };

                    self.priority_queue
                        .push((node_id, timer_kind), Reverse(event.expire_at));
                    self.timers.insert((node_id, timer_kind), event);
                }
                PeerDiscoveryTimerCommand::ScheduleReset {
                    node_id,
                    timer_kind,
                } => {
                    self.timers.remove(&(node_id, timer_kind));
                }
            }
        }
    }

    // returns the first tick in priority. if timer was reset before step_until
    // is called, step_until is a no-op
    fn peek_tick(&self) -> Option<Duration> {
        self.priority_queue.peek().map(|(_, rev_d)| rev_d.0)
    }

    fn step_until(&mut self, until: Duration) -> Option<E> {
        while let Some(tick) = self.peek_tick() {
            if tick > until {
                break;
            }
            self.tick = tick;

            let (identifier, Reverse(tick)) =
                self.priority_queue.pop().expect("priority queue non-empty");

            // timer was reset before popped from priority queue
            if self
                .timers
                .get(&identifier)
                .is_none_or(|event| event.expire_at != tick)
            {
                continue;
            }

            let event = self.timers.remove(&identifier).expect("some").on_timeout;

            return Some(event);
        }
        None
    }
}

pub struct MockDiscoveryDriver<PDT, E, ST: CertificateSignatureRecoverable> {
    algo: PDT,
    timer: MockDiscTimer<E, ST>,
}

impl<PDT, ST> MockDiscoveryDriver<PDT, PeerDiscoveryEvent<ST>, ST>
where
    PDT: PeerDiscoveryAlgo<SignatureType = ST>,
    ST: CertificateSignatureRecoverable,
{
    pub fn new<B>(algo_builder: B) -> (Self, Vec<RouterCommand<ST, PeerDiscoveryMessage<ST>>>)
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PDT>,
    {
        let (algo, init_cmds) = algo_builder.build();

        let mut driver = MockDiscoveryDriver {
            algo,
            timer: Default::default(),
        };

        let router_cmds = driver.filter_and_exec(init_cmds);

        (driver, router_cmds)
    }

    pub fn update(
        &mut self,
        event: PeerDiscoveryEvent<ST>,
    ) -> Vec<RouterCommand<ST, PeerDiscoveryMessage<ST>>> {
        let cmds = match event {
            PeerDiscoveryEvent::SendPing { to } => self.algo.send_ping(to),
            PeerDiscoveryEvent::PingRequest { from, ping } => self.algo.handle_ping(from, ping),
            PeerDiscoveryEvent::PongResponse { from, pong } => self.algo.handle_pong(from, pong),
            PeerDiscoveryEvent::PingTimeout { to, ping_id } => {
                self.algo.handle_ping_timeout(to, ping_id)
            }
            PeerDiscoveryEvent::SendPeerLookup {
                to,
                target,
                open_discovery,
            } => self
                .algo
                .send_peer_lookup_request(to, target, open_discovery),
            PeerDiscoveryEvent::PeerLookupRequest { from, request } => {
                self.algo.handle_peer_lookup_request(from, request)
            }
            PeerDiscoveryEvent::PeerLookupResponse { from, response } => {
                self.algo.handle_peer_lookup_response(from, response)
            }
            PeerDiscoveryEvent::PeerLookupTimeout {
                to,
                target,
                lookup_id,
            } => self.algo.handle_peer_lookup_timeout(to, target, lookup_id),
            PeerDiscoveryEvent::UpdateCurrentEpoch { epoch } => {
                self.algo.update_current_epoch(epoch)
            }
            PeerDiscoveryEvent::UpdateValidatorSet { epoch, validators } => {
                self.algo.update_validator_set(epoch, validators)
            }
            PeerDiscoveryEvent::UpdatePeers { peers } => self.algo.update_peers(peers),
            PeerDiscoveryEvent::Refresh => self.algo.refresh(),
        };

        self.filter_and_exec(cmds)
    }

    // selectively execute the timer commands. Returns router commands in the
    // original order
    fn filter_and_exec(
        &mut self,
        cmds: Vec<PeerDiscoveryCommand<ST>>,
    ) -> Vec<RouterCommand<ST, PeerDiscoveryMessage<ST>>> {
        let mut router_cmds = Vec::new();

        for cmd in cmds {
            match cmd {
                PeerDiscoveryCommand::RouterCommand { target, message } => {
                    router_cmds.push(RouterCommand::Publish {
                        target: RouterTarget::PointToPoint(target),
                        message,
                    })
                }
                PeerDiscoveryCommand::TimerCommand(peer_discovery_timer_command) => {
                    self.timer.exec(vec![peer_discovery_timer_command]);
                }
                PeerDiscoveryCommand::MetricsCommand(_) => {
                    // ignore metrics command in swarm
                }
            }
        }
        router_cmds
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.timer.peek_tick()
    }

    pub fn step_until(&mut self, until: Duration) -> Option<PeerDiscoveryEvent<ST>> {
        self.timer.step_until(until)
    }

    pub fn get_peer_disc_state(&self) -> &PDT {
        &self.algo
    }
}
