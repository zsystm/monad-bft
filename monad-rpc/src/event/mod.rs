pub(super) use self::event::EventServerEvent;
pub use self::{
    client::{EventServerClient, EventServerClientError},
    server::EventServer,
};

const BROADCAST_CHANNEL_SIZE: usize = 1024 * 10;

mod client;
mod event;
mod server;
