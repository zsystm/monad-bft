use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use actix_codec::{Decoder, Encoder};
use actix_http::{
    ws::{Codec, Frame, Message, ProtocolError},
    Payload,
};
use actix_web::{
    web::{Bytes, BytesMut},
    Error,
};
use bytestring::ByteString;
use futures::stream::Stream;
use tokio::sync::mpsc::Receiver;
use tracing::error;

pub(super) struct StreamingBody {
    session_rx: Receiver<Message>,
    codec: Codec,
    closing: bool,
}

impl StreamingBody {
    pub(super) fn new(session_rx: Receiver<Message>) -> Self {
        StreamingBody {
            session_rx,
            codec: Codec::new(),
            closing: false,
        }
    }
}

impl Stream for StreamingBody {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.closing {
            return Poll::Ready(None);
        }

        let msg = match Pin::new(&mut this.session_rx).poll_recv(cx) {
            Poll::Ready(Some(msg)) => msg,
            Poll::Ready(None) => {
                this.closing = true;
                return Poll::Pending;
            }
            Poll::Pending => return Poll::Pending,
        };

        let mut buf = BytesMut::new();

        if let Err(err) = this.codec.encode(msg, &mut buf) {
            error!("Error encoding message: {}", err);
            return Poll::Ready(Some(Err(err.into())));
        }

        Poll::Ready(Some(Ok(buf.freeze())))
    }
}

pub struct MessageStream {
    payload: Payload,

    messages: VecDeque<Message>,
    buf: BytesMut,
    codec: Codec,
    closing: bool,
}

impl MessageStream {
    pub(super) fn new(payload: Payload) -> Self {
        MessageStream {
            payload,
            messages: VecDeque::new(),
            buf: BytesMut::new(),
            codec: Codec::new(),
            closing: false,
        }
    }

    #[must_use]
    pub fn max_frame_size(mut self, max_size: usize) -> Self {
        self.codec = self.codec.max_size(max_size);
        self
    }
}

impl Stream for MessageStream {
    type Item = Result<Message, ProtocolError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Return the first message in the queue if one exists
        //
        // This is faster than polling and parsing
        if let Some(msg) = this.messages.pop_front() {
            return Poll::Ready(Some(Ok(msg)));
        }

        if !this.closing {
            // Read in bytes until there's nothing left to read
            loop {
                match Pin::new(&mut this.payload).poll_next(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        this.buf.extend_from_slice(&bytes);
                    }
                    Poll::Ready(Some(Err(err))) => {
                        error!("Error reading payload: {}", err);
                        return Poll::Ready(Some(Err(ProtocolError::Io(io::Error::other(err)))));
                    }
                    Poll::Ready(None) => {
                        this.closing = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }

        // Create messages until there's no more bytes left
        while let Some(frame) = this.codec.decode(&mut this.buf)? {
            let message = match frame {
                Frame::Text(bytes) => {
                    ByteString::try_from(bytes)
                        .map(Message::Text)
                        .map_err(|err| {
                            error!("Invalid UTF-8 sequence: {}", err);
                            ProtocolError::Io(io::Error::new(io::ErrorKind::InvalidData, err))
                        })?
                }
                Frame::Binary(bytes) => Message::Binary(bytes),
                Frame::Ping(bytes) => Message::Ping(bytes),
                Frame::Pong(bytes) => Message::Pong(bytes),
                Frame::Close(reason) => Message::Close(reason),
                Frame::Continuation(item) => Message::Continuation(item),
            };

            this.messages.push_back(message);
        }

        // Return the first message in the queue
        if let Some(msg) = this.messages.pop_front() {
            return Poll::Ready(Some(Ok(msg)));
        }

        // If we've exhausted our message queue and we're closing, close the stream
        if this.closing {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}
