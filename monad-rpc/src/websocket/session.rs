use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use actix_http::ws::{CloseReason, Message};
use actix_web::web::Bytes;
use bytestring::ByteString;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(super) struct Session {
    inner: Option<Sender<Message>>,
    closed: Arc<AtomicBool>,
}

/// The error representing a closed websocket session
#[derive(Debug)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Session is closed")
    }
}

impl std::error::Error for Closed {}

impl Session {
    pub fn new(inner: Sender<Message>) -> Self {
        Session {
            inner: Some(inner),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn pre_check(&mut self) {
        if self.closed.load(Ordering::Relaxed) {
            self.inner.take();
        }
    }

    /// Sends text into the WebSocket.
    ///
    /// ```no_run
    /// # use actix_ws::Session;
    /// # async fn test(mut session: Session) {
    /// if session.text("Some text").await.is_err() {
    ///     // session closed
    /// }
    /// # }
    /// ```
    pub async fn text(&mut self, msg: impl Into<ByteString>) -> Result<(), Closed> {
        self.pre_check();
        if let Some(inner) = self.inner.as_mut() {
            inner
                .send(Message::Text(msg.into()))
                .await
                .map_err(|_| Closed)
        } else {
            Err(Closed)
        }
    }

    /// Sends raw bytes into the WebSocket.
    ///
    /// ```no_run
    /// # use actix_ws::Session;
    /// # async fn test(mut session: Session) {
    /// if session.binary(&b"some bytes"[..]).await.is_err() {
    ///     // session closed
    /// }
    /// # }
    /// ```
    pub async fn binary(&mut self, msg: impl Into<Bytes>) -> Result<(), Closed> {
        self.pre_check();
        if let Some(inner) = self.inner.as_mut() {
            inner
                .send(Message::Binary(msg.into()))
                .await
                .map_err(|_| Closed)
        } else {
            Err(Closed)
        }
    }

    /// Pings the client.
    ///
    /// For many applications, it will be important to send regular pings to keep track of if the
    /// client has disconnected
    ///
    /// ```no_run
    /// # use actix_ws::Session;
    /// # async fn test(mut session: Session) {
    /// if session.ping(b"").await.is_err() {
    ///     // session is closed
    /// }
    /// # }
    /// ```
    pub async fn ping(&mut self, msg: &[u8]) -> Result<(), Closed> {
        self.pre_check();
        if let Some(inner) = self.inner.as_mut() {
            inner
                .send(Message::Ping(Bytes::copy_from_slice(msg)))
                .await
                .map_err(|_| Closed)
        } else {
            Err(Closed)
        }
    }

    /// Pongs the client.
    ///
    /// ```no_run
    /// # use actix_ws::{Message, Session};
    /// # async fn test(mut session: Session, msg: Message) {
    /// match msg {
    ///     Message::Ping(bytes) => {
    ///         let _ = session.pong(&bytes).await;
    ///     }
    ///     _ => (),
    /// }
    /// # }
    pub async fn pong(&mut self, msg: &[u8]) -> Result<(), Closed> {
        self.pre_check();
        if let Some(inner) = self.inner.as_mut() {
            inner
                .send(Message::Pong(Bytes::copy_from_slice(msg)))
                .await
                .map_err(|_| Closed)
        } else {
            Err(Closed)
        }
    }

    /// Sends a close message, and consumes the session.
    ///
    /// All clones will return `Err(Closed)` if used after this call.
    ///
    /// ```no_run
    /// # use actix_ws::{Closed, Session};
    /// # async fn test(mut session: Session) -> Result<(), Closed> {
    /// session.close(None).await
    /// # }
    /// ```
    pub async fn close(mut self, reason: Option<CloseReason>) -> Result<(), Closed> {
        self.pre_check();

        if let Some(inner) = self.inner.take() {
            self.closed.store(true, Ordering::Relaxed);
            inner.send(Message::Close(reason)).await.map_err(|_| Closed)
        } else {
            Err(Closed)
        }
    }
}
