use std::{collections::HashMap, sync::Arc};

use async_lock::{Mutex, RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use futures::future::{Either, select};
use leaf_stream::{
    StreamGenesis,
    encoding::Encodable,
    types::{IncomingEvent, LeafQuery, LeafSubscribeQuery, SqlRows},
};
use parity_scale_codec::{Decode, Encode};
use socketioxide::extract::{AckSender, SocketRef, TryData};
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use ulid::Ulid;

use crate::{error::LogError, storage::STORAGE};

pub fn setup_socket_handlers(socket: &SocketRef, did: String) {
    let span = Span::current();

    let open_streams = Arc::new(RwLock::new(HashMap::new()));
    let unsubscribers = Arc::new(Mutex::new(HashMap::new()));

    let did_ = did.clone();
    let span_ = span.clone();
    socket.on(
        "wasm/upload",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let data = data?;
                let hash = STORAGE.upload_wasm(&did_, data.to_vec()).await?;
                anyhow::Ok(hash)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle wasm/upload"))
            .await;

            ack.send(&Encodable(result.map(Encodable)).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );
    let span_ = span.clone();
    socket.on(
        "wasm/has",
        async move |TryData::<bytes::Bytes>(hash_hex), ack: AckSender| {
            let result = async {
                let hash_bytes: [u8; 32] = hash_hex?.as_ref().try_into()?;
                let hash = Hash::from_bytes(hash_bytes);
                let has_module = STORAGE.has_wasm_blob(hash).await?;
                anyhow::Ok(has_module)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle wasm/has"))
            .await;

            ack.send(&Encodable(result).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/create",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                // Create the stream
                let input = data?;
                let genesis = StreamGenesis::decode(&mut &input[..])?;
                if genesis.creator != did_ {
                    anyhow::bail!("Stream creator is not the same as authenticated user.")
                }

                let stream = STORAGE.create_stream(genesis).await?;
                let id = stream.id();
                open_streams_.write().await.insert(id, stream);
                anyhow::Ok(id)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            ack.send(&Encodable(result.map(Encodable)).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/event_batch",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let bytes = data?;
                let request = StreamEventBatchArgs::decode(&mut &bytes[..])?;
                let stream_id = request.stream_id.0;
                let events = request.events;

                // TODO: maybe we just shouldn't have the client send the requesting user since we
                // already have it.
                if !events.iter().all(|e| e.user == did_) {
                    anyhow::bail!(
                        "Some events in batch are not authored by the authenticated user."
                    );
                }

                let open_streams = open_streams_.upgradable_read().await;

                let stream = if let Some(stream) = open_streams.get(&stream_id) {
                    stream.clone()
                } else {
                    let Some(stream) = STORAGE.open_stream(stream_id).await? else {
                        anyhow::bail!("Stream does not exist with ID: {stream_id}");
                    };
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_id, stream.clone());
                    stream
                };

                stream.add_events(events).await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/event"))
            .await;

            ack.send(&Encodable(result).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    let socket_ = socket.clone();
    let unsubscribers_ = unsubscribers.clone();
    socket.on(
        "stream/subscribe",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let args = data?;
                let args = StreamSubscribeArgs::decode(&mut &args[..])?;
                let stream_id = args.stream_id.0;
                let query = args.query;
                let subscription_id = Ulid::new();

                // TODO: maybe we just shouldn't have the client send the requesting user since we
                // already have it.
                if query.requesting_user != did_ {
                    anyhow::bail!(
                        "Some events in batch are not authored by the authenticated user."
                    );
                }

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_id) {
                    stream.clone()
                } else {
                    let Some(stream) = STORAGE.open_stream(stream_id).await? else {
                        anyhow::bail!("Stream does not exist with ID: {stream_id}");
                    };
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_id, stream.clone());
                    stream
                };

                let receiver = stream.subscribe(query).await;

                tokio::spawn(async move {
                    let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();

                    unsubscribers_
                        .lock()
                        .await
                        .insert(subscription_id, unsubscribe_tx);

                    let mut next_event = Box::pin(receiver.recv());
                    let mut unsubscribe = unsubscribe_rx;
                    while let Either::Left((Ok(event), _)) =
                        select(next_event, &mut unsubscribe).await
                    {
                        next_event = Box::pin(receiver.recv());

                        if socket_.connected() {
                            if let Err(e) = socket_.emit(
                                "stream/subscription_response",
                                &StreamSubscribeNotification {
                                    subscription_id: Encodable(subscription_id),
                                    response: event.map_err(|e| e.to_string()),
                                }
                                .encode(),
                            ) {
                                // TODO: better error message
                                tracing::error!("Error sending event, unsubscribing: {e}");
                            }
                        } else if let Some(unsubscriber) =
                            unsubscribers_.lock().await.remove(&subscription_id)
                        {
                            tracing::info!("Client disconnected, canceling subscription");
                            unsubscriber.send(()).ok();
                        }
                    }
                });

                anyhow::Ok(subscription_id.to_string())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/subscribe"))
            .await;

            ack.send(&Encodable(result).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let unsubscribers_ = unsubscribers.clone();
    socket.on(
        "stream/unsubscribe",
        async move |TryData::<Ulid>(data), ack: AckSender| {
            let result = async {
                let subscription_id = data?;
                let unsubscriber = unsubscribers_.lock().await.remove(&subscription_id);
                let was_subscribed = unsubscriber.is_some();
                unsubscriber.map(|x| x.send(()));
                anyhow::Ok(was_subscribed)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/fetch"))
            .await;

            ack.send(&Encodable(result).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/query",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let bytes = data?;
                let args = StreamQueryArgs::decode(&mut &bytes[..])?;
                let stream_id = args.stream_id.0;
                let query = args.query;

                // TODO: maybe we just shouldn't have the client send the requesting user since we
                // already have it.
                if query.requesting_user != did_ {
                    anyhow::bail!("Requesting user does not match authenticated user");
                }

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_id) {
                    stream.clone()
                } else {
                    let Some(stream) = STORAGE.open_stream(stream_id).await? else {
                        anyhow::bail!("Stream does not exist with ID: {stream_id}");
                    };
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_id, stream.clone());
                    stream
                };

                let response = stream.query(query).await?;

                anyhow::Ok(response)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/fetch"))
            .await;

            ack.send(&Encodable(result).encode())
                .log_error("Internal error sending response")
                .ok();
        },
    );
}

#[derive(Decode)]
struct StreamEventBatchArgs {
    stream_id: Encodable<Hash>,
    events: Vec<IncomingEvent>,
}

#[derive(Decode)]
struct StreamQueryArgs {
    stream_id: Encodable<Hash>,
    query: LeafQuery,
}

#[derive(Encode)]
struct StreamSubscribeNotification {
    subscription_id: Encodable<Ulid>,
    response: Result<SqlRows, String>,
}

#[derive(Decode)]
struct StreamSubscribeArgs {
    stream_id: Encodable<Hash>,
    query: LeafSubscribeQuery,
}
