use std::{
    collections::HashMap,
    pin::pin,
    time::{Duration, Instant},
};

use actix_http::ws;
use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::{AggregatedMessage, CloseCode, CloseReason};
use alloy_rpc_types::eth::{pubsub::Params, Filter, FilteredParams};
use futures::StreamExt;
use itertools::Either;
use monad_exec_events::BlockCommitState;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use tokio::sync::broadcast;
use tracing::{debug, error, warn};

use crate::{
    eth_json_types::{
        serialize_result, EthSubscribeRequest, EthSubscribeResult, EthUnsubscribeRequest,
        FixedData, MonadNotification, SubscriptionKind,
    },
    event::{EventServerClient, EventServerClientError, EventServerEvent},
    handlers::{resources::MonadRpcResources, rpc_select},
    jsonrpc::{JsonRpcError, Request, RequestWrapper},
    serialize::SharedJsonSerialized,
    timing::RequestId,
};

const RECV_MAX_CONTINUATION_SIZE: usize = 2 * 1024 * 1024;
const RECV_MAX_FRAME_SIZE: usize = 256 * 1024;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const CLIENT_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Deserialize, Serialize)]
pub struct SubscriptionId(pub FixedData<16>);

pub async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    app_state: web::Data<MonadRpcResources>,
    event_server_client: web::Data<EventServerClient>,
) -> Result<HttpResponse, actix_web::Error> {
    let rx = event_server_client.subscribe().map_err(|err| {
        match err {
            EventServerClientError::ServerCrashed => {
                warn!("Closing websocket connection with internal server error, reason: WebSocketServer crashed!");
            },
        }

        actix_web::error::ErrorInternalServerError("WebSocketServer is currently unavailable, please try again later.")
    })?;

    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;

    actix_rt::spawn(handler(
        session,
        msg_stream,
        req.connection_info().host().to_string(),
        req.connection_info().peer_addr().map(ToString::to_string),
        rx,
        app_state,
    ));

    Ok(res)
}

async fn handler(
    mut session: actix_ws::Session,
    msg_stream: actix_ws::MessageStream,
    hostname: String,
    peer_addr: Option<String>,
    rx: broadcast::Receiver<EventServerEvent>,
    app_state: web::Data<MonadRpcResources>,
) {
    debug!(?hostname, ?peer_addr, "ws connection opened");

    if let Some(metrics) = &app_state.metrics {
        metrics.record_websocket_connection(1);
    }

    let mut last_heartbeat = Instant::now();
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);

    let msg_stream = msg_stream
        .max_frame_size(RECV_MAX_FRAME_SIZE)
        .aggregate_continuations()
        .max_continuation_size(RECV_MAX_CONTINUATION_SIZE);

    let mut msg_stream = pin!(msg_stream);
    let mut broadcast_rx = pin!(rx);

    let mut subscriptions: HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>> =
        HashMap::new();

    let close_reason = loop {
        tokio::select! {
            _ = interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > Duration::from_secs(CLIENT_TIMEOUT_SECS) {
                    break Some(CloseReason {
                        code: ws::CloseCode::Protocol,
                        description: Some(format!("ws server did not receive ping in {CLIENT_TIMEOUT_SECS}s"))
                    });
                }

                if let Err(err) = session.ping(b"").await {
                    warn!(?hostname, ?peer_addr, ?err, "ws handler ping error");
                }
            }
            msg = msg_stream.next() => {
                match msg {
                    Some(Ok(AggregatedMessage::Ping(bytes))) => {
                        last_heartbeat = Instant::now();

                        if let Err(err) = session.pong(&bytes).await {
                            warn!(?hostname, ?peer_addr, ?err, "ws handler pong error");
                        }
                    }
                    Some(Ok(AggregatedMessage::Pong(_))) => {
                        last_heartbeat = Instant::now();
                    }
                    Some(Ok(AggregatedMessage::Text(body))) => {
                        last_heartbeat = Instant::now();

                        let request = to_request::<Request>(&body);

                        match request {
                            Ok(req) => {
                                if let Err(close_reason) = handle_request(&mut session, &mut subscriptions, &app_state, req).await {
                                    break Some(close_reason);
                                }
                            }
                            Err(e) => {
                                if let Err(err) = session
                                    .text(to_response(&crate::jsonrpc::Response::from_error(e)))
                                    .await {
                                    warn!(?err, "ws handler AggregatedMessage text error");
                                    break None;
                                }
                            }
                        };
                    }
                    Some(Ok(AggregatedMessage::Binary(body))) => {
                        last_heartbeat = Instant::now();

                        let request = to_request::<Request>(&body);

                        match request {
                            Ok(req) => {
                                if let Err(close_reason) = handle_request(&mut session, &mut subscriptions, &app_state, req).await {
                                    break Some(close_reason);
                                }
                            }
                            Err(e) => {
                                if let Err(err) = session
                                    .binary(to_response(&crate::jsonrpc::Response::from_error(e)))
                                    .await {
                                    warn!(?err, "ws handler AggregatedMessage binary error");
                                    break None;
                                }
                            }
                        };
                    }
                    Some(Ok(AggregatedMessage::Close(close_reason))) => {
                        debug!(?hostname, ?peer_addr, ?close_reason, "ws connection closed by request");
                        break None;
                    }
                    Some(Err(err)) => {
                        error!(?err, "ws connection protocol error");
                        break Some(CloseReason {
                            code: ws::CloseCode::Protocol,
                            description: Some(format!("ws protocol error: {err:#?}"))
                        });
                    }
                    None => {
                        warn!(?hostname, ?peer_addr, "ws connection closed abruptly");
                        break None;
                    }
                }
            }
            cmd = broadcast_rx.recv() => {
                match cmd {
                    Ok(msg) => {
                        if let Err(close_reason) = handle_notification(&mut session, &subscriptions, msg).await {
                            break Some(close_reason);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped_messages)) => {
                        warn!(?skipped_messages, "ws handler lagging");

                        break Some(CloseReason {
                            code: ws::CloseCode::Error,
                            description: Some("ws server lagging, please try again later".to_string())
                        });
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        error!("ws handler detected event server close");

                        break Some(CloseReason {
                            code: ws::CloseCode::Error,
                            description: Some("ws server shutdown".to_string())
                        });
                    }
                }
            }
        };
    };

    debug!(?hostname, ?peer_addr, ?close_reason, "ws connection closed");

    if let Some(reason) = close_reason {
        session
            .close(Some(reason))
            .await
            .unwrap_or_else(|err| warn!("ws handler close error: {err:?}"));
    }

    if let Some(metrics) = &app_state.metrics {
        metrics.record_websocket_connection(-1);

        subscriptions.iter().for_each(|(_, subs)| {
            metrics.record_websocket_topic(-(subs.len() as i64));
        });
    }
}

async fn handle_notification(
    mut session: &mut actix_ws::Session,
    subscriptions: &HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>>,
    msg: EventServerEvent,
) -> Result<(), CloseReason> {
    match msg {
        EventServerEvent::Gap => {
            return Err(CloseReason::from((
                CloseCode::Error,
                "websocket server gapped",
            )));
        }
        EventServerEvent::Block {
            header,
            block,
            logs,
        } => {
            debug!(
                block_id =? header.block_id, logs =? logs.len(),
                block_number =? block.data.header.number,
                txns =? block.data.transactions.len(),
                logs =? logs.len(),
                "websocket broadcasting events"
            );

            for (id, _) in subscriptions
                .get(&SubscriptionKind::MonadNewHeads)
                .map(|x| x.iter())
                .unwrap_or_default()
            {
                send_notification(session, id, header.as_ref()).await?;
            }

            for (id, filter) in subscriptions
                .get(&SubscriptionKind::MonadLogs)
                .map(|x| x.iter())
                .unwrap_or_default()
            {
                let Some(logs) = apply_logs_filter(filter, header.data.as_ref(), logs.iter())
                else {
                    continue;
                };

                for log in logs {
                    send_notification(session, id, log.as_ref()).await?;
                }
            }

            if header.commit_state == BlockCommitState::Finalized {
                for (id, _) in subscriptions
                    .get(&SubscriptionKind::NewHeads)
                    .map(|x| x.iter())
                    .unwrap_or_default()
                {
                    send_notification(session, id, header.data.as_ref()).await?;
                }

                for (id, filter) in subscriptions
                    .get(&SubscriptionKind::Logs)
                    .map(|x| x.iter())
                    .unwrap_or_default()
                {
                    let Some(logs) = apply_logs_filter(filter, header.data.as_ref(), logs.iter())
                    else {
                        continue;
                    };

                    for log in logs {
                        send_notification(session, id, log.data.as_ref()).await?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[inline]
fn apply_logs_filter<'a>(
    filter: &'a Option<Filter>,
    header: &alloy_rpc_types::eth::Header,
    logs: impl Iterator<
            Item = &'a SharedJsonSerialized<
                MonadNotification<SharedJsonSerialized<alloy_rpc_types::Log>>,
            >,
        > + 'a,
) -> Option<
    impl Iterator<
            Item = &'a SharedJsonSerialized<
                MonadNotification<SharedJsonSerialized<alloy_rpc_types::Log>>,
            >,
        > + 'a,
> {
    if let Some(filter) = filter {
        let filtered_params: FilteredParams = FilteredParams::new(Some(filter.clone()));

        if !filtered_params.filter_block_range(header.number)
            || !filtered_params.filter_block_hash(header.hash)
        {
            return None;
        }

        if !FilteredParams::matches_address(
            header.logs_bloom,
            &FilteredParams::address_filter(&filter.address),
        ) || !FilteredParams::matches_topics(
            header.logs_bloom,
            &FilteredParams::topics_filter(&filter.topics),
        ) {
            // The block's bloom filter doesn't match the filter, so we can skip this block.
            return None;
        }

        Some(Either::Left(logs.filter(move |log| {
            filtered_params.filter_address(&log.data.address())
                && filtered_params.filter_topics(log.data.topics())
        })))
    } else {
        Some(Either::Right(logs))
    }
}

async fn handle_request(
    ctx: &mut actix_ws::Session,
    subscriptions: &mut HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>>,
    app_state: &MonadRpcResources,
    request: Request,
) -> Result<(), CloseReason> {
    match request.method.as_str() {
        "eth_subscribe" => {
            let Ok(req) = serde_json::from_value::<EthSubscribeRequest>(request.params) else {
                if let Err(err) = ctx
                    .text(to_response(&crate::jsonrpc::Response::new(
                        None,
                        Some(JsonRpcError::invalid_params()),
                        request.id,
                    )))
                    .await
                {
                    warn!(
                        ?err,
                        "ws handle_request eth_subscribe failed to send invalid_params error"
                    );
                    return Err(CloseReason {
                        code: ws::CloseCode::Error,
                        description: None,
                    });
                }

                return Ok(());
            };

            let filter = match req.params {
                Params::None => None,
                Params::Logs(filter) => Some(*filter),
                Params::Bool(_) => {
                    if let Err(err) = ctx
                        .text(to_response(&crate::jsonrpc::Response::new(
                            None,
                            Some(JsonRpcError::invalid_params()),
                            request.id,
                        )))
                        .await
                    {
                        warn!(
                            ?err,
                            "ws handle_request eth_subscribe failed to send invalid_params error"
                        );
                        return Err(CloseReason {
                            code: ws::CloseCode::Error,
                            description: None,
                        });
                    }

                    return Ok(());
                }
            };

            let mut rng = rand::thread_rng();
            let random_bytes: [u8; 16] = rng.gen();
            let id = SubscriptionId(FixedData(random_bytes));

            debug!(subscription_kind = ?req.kind, "ws handle_request eth_subscribe received subscribe request");

            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::from_result(
                    request.id,
                    serialize_result(id),
                )))
                .await
            {
                warn!(
                    ?err,
                    "ws handle_request eth_subscribe failed to send subscription response"
                );
                return Err(CloseReason {
                    code: ws::CloseCode::Error,
                    description: None,
                });
            }

            subscriptions
                .entry(req.kind)
                .or_default()
                .push((id, filter));

            if let Some(metrics) = &app_state.metrics {
                metrics.record_websocket_topic(1);
            }
        }
        "eth_unsubscribe" => {
            let Ok(req) = serde_json::from_value::<EthUnsubscribeRequest>(request.params) else {
                if let Err(err) = ctx
                    .text(to_response(&crate::jsonrpc::Response::new(
                        None,
                        Some(JsonRpcError::invalid_params()),
                        request.id,
                    )))
                    .await
                {
                    warn!(
                        ?err,
                        "ws handle_request eth_unsubscribe failed to send invalid_params error"
                    );
                    return Err(CloseReason {
                        code: ws::CloseCode::Error,
                        description: None,
                    });
                }

                return Ok(());
            };

            debug!(subscription_id = ?req.id, "ws handle_request eth_unsubscribe received unsubscribe request");

            let mut exists: bool = false;
            for vec in subscriptions.values_mut() {
                let original_len = vec.len();
                vec.retain(|x| x.0 != SubscriptionId(req.id));
                if vec.len() < original_len {
                    exists = true;
                    break;
                }
            }

            if exists {
                if let Some(metrics) = &app_state.metrics {
                    metrics.record_websocket_topic(-1);
                }
            }

            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::from_result(
                    request.id,
                    serialize_result(exists),
                )))
                .await
            {
                warn!(
                    ?err,
                    "ws handle_request eth_unsubscribe failed to send unsubscribe response"
                );
                return Err(CloseReason {
                    code: ws::CloseCode::Error,
                    description: None,
                });
            }
        }
        method => {
            let result = rpc_select(app_state, method, request.params, RequestId::random()).await;
            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::from_result(
                    request.id, result,
                )))
                .await
            {
                warn!(?err, "ws handle_request failed to send rpc response");
                return Err(CloseReason {
                    code: ws::CloseCode::Error,
                    description: None,
                });
            }
        }
    }

    Ok(())
}

#[inline]
async fn send_notification(
    session: &mut actix_ws::Session,
    id: &SubscriptionId,
    result: impl AsRef<RawValue>,
) -> Result<(), CloseReason> {
    let subscribe_result = EthSubscribeResult::new(id.0, result.as_ref());

    let notification =
        crate::jsonrpc::Notification::new("eth_subscription".to_string(), subscribe_result);

    if session.text(to_response(&notification)).await.is_err() {
        return Err(CloseReason {
            code: CloseCode::Error,
            description: Some("ws server failed to send notification".to_string()),
        });
    }

    Ok(())
}

#[inline]
fn to_response<S: Serialize + std::fmt::Debug>(resp: &S) -> String {
    match serde_json::to_string(resp) {
        Ok(resp) => resp,
        Err(e) => {
            error!("error serializing response: {:?} for {:?}", e, resp);
            serde_json::to_string(&crate::jsonrpc::Response::from_error(
                JsonRpcError::internal_error("serializing response".to_string()),
            ))
            .expect("failed to serialize error response")
        }
    }
}

fn to_request<T: serde::de::DeserializeOwned>(
    body: impl AsRef<[u8]>,
) -> Result<Request, JsonRpcError> {
    let request: RequestWrapper<Value> =
        serde_json::from_slice(body.as_ref()).map_err(|_| JsonRpcError::invalid_params())?;

    let request = match request {
        RequestWrapper::Single(req) => {
            serde_json::from_value::<Request>(req).map_err(|_| JsonRpcError::invalid_params())
        }
        _ => Err(JsonRpcError::invalid_params()), // TODO: handle batch requests
    }?;

    Ok(request)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_http::{ws, ws::Frame};
    use actix_web::{web, App};
    use bytes::Bytes;
    use futures_util::{SinkExt as _, StreamExt as _};
    use monad_event_ring::SnapshotEventRing;
    use serde_json::json;

    use super::ws_handler;
    use crate::{
        eth_json_types::{EthSubscribeResult, FixedData},
        event::EventServer,
        hex,
    };

    fn create_test_server() -> actix_test::TestServer {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../../../monad-exec-events/test/data/exec-events-emn-30b-15m.zst");

        let snapshot =
            SnapshotEventRing::new_from_zstd_bytes(SNAPSHOT_ZSTD_BYTES, SNAPSHOT_NAME).unwrap();

        let ws_server_handle =
            EventServer::start_for_testing_with_delay(snapshot, Duration::from_secs(1));

        actix_test::start(move || {
            App::new()
                .app_data(web::JsonConfig::default().limit(8192))
                .app_data(web::Data::new(ws_server_handle.clone()))
                .service(web::resource("/ws/").route(web::get().to(ws_handler)))
        })
    }

    #[actix_rt::test]
    #[ignore]
    async fn websocket_wait_for_ping() {
        let mut server = create_test_server();
        let mut framed = server.ws_at("/ws/").await.unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert_eq!(frame, Frame::Ping(Bytes::from_static(b"")));
        framed
            .send(ws::Message::Pong(Bytes::from_static(b"")))
            .await
            .unwrap();
    }

    #[actix_rt::test]
    #[ignore]
    async fn websocket_eth_subscribe() {
        let mut server: actix_test::TestServer = create_test_server();
        let mut framed = server.ws_at("/ws/").await.unwrap();

        let _frame = framed.next().await.unwrap().unwrap();
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        });

        framed
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let frame = framed.next().await.unwrap().unwrap();

        assert!(matches!(frame, Frame::Text(_)));
        let subscription_id = if let Frame::Text(resp) = frame {
            let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
            let resp: crate::jsonrpc::Response = serde_json::from_value(resp).unwrap();
            let resp: FixedData<16> = serde_json::from_str(resp.result.unwrap().get()).unwrap();
            resp
        } else {
            panic!("Expected a text frame");
        };

        // Receive some messages, then unsubscribe
        let mut count: usize = 0;
        loop {
            if let Some(frame) = framed.next().await {
                if let Ok(frame) = frame {
                    match frame {
                        Frame::Ping(_) => {
                            framed
                                .send(ws::Message::Pong(Bytes::from_static(b"")))
                                .await
                                .unwrap();
                        }
                        Frame::Text(update) => {
                            let update: crate::jsonrpc::Notification<EthSubscribeResult> =
                                serde_json::from_slice(&update).unwrap();
                            assert_eq!(update.params.subscription.0, subscription_id.0);
                        }
                        _ => panic!("unexpected frame"),
                    }
                }
                count += 1;
            }

            if count > 2 {
                let body = json!({
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [hex::encode(&subscription_id.0)],
                    "id": 1
                });

                framed
                    .send(ws::Message::Text(body.to_string().into()))
                    .await
                    .unwrap();

                loop {
                    let frame = framed.next().await.unwrap().unwrap();
                    assert!(matches!(frame, Frame::Text(_)));
                    if let Frame::Text(resp) = frame {
                        let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
                        let resp: crate::jsonrpc::Response = match serde_json::from_value(resp) {
                            Ok(resp) => resp,
                            Err(_) => continue,
                        };
                        let resp: bool = serde_json::from_str(resp.result.unwrap().get()).unwrap();
                        assert!(resp);
                        return;
                    } else {
                        panic!("Expected a text frame");
                    };
                }
            }
        }
    }

    #[actix_rt::test]
    #[ignore]
    async fn websocket_multiple_connections() {
        // Create a test server with two connections
        let mut server = create_test_server();
        let mut conn0 = server.ws_at("/ws/").await.unwrap();
        let mut conn1 = server.ws_at("/ws/").await.unwrap();

        // Subscribe both connections to newHeads
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        });

        // Send subscription requests
        conn0
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        conn1
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();

        // Handle initial ping and subscription responses
        let mut frames0 = Vec::new();
        let mut frames1 = Vec::new();

        // Collect initial frames (ping + subscription response)
        for _ in 0..2 {
            frames0.push(conn0.next().await.unwrap().unwrap());
            frames1.push(conn1.next().await.unwrap().unwrap());
        }

        // Both connections should receive the block update
        let mut update0 = None;
        let mut update1 = None;

        // Keep reading until we get updates or timeout
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        while (update0.is_none() || update1.is_none()) && start.elapsed() < timeout {
            tokio::select! {
                frame = conn0.next(), if update0.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        update0 = Some(frame);
                    }
                }
                frame = conn1.next(), if update1.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        update1 = Some(frame);
                    }
                }
            }
        }

        assert!(
            update0.is_some(),
            "Connection 0 did not receive block update"
        );
        assert!(
            update1.is_some(),
            "Connection 1 did not receive block update"
        );
    }
}
