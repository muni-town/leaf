use std::{collections::HashMap, sync::Arc};

use async_lock::{Mutex, RwLock, RwLockUpgradableReadGuard};
use futures::future::{Either, select};
use leaf_stream::{
    atproto_plc::Did,
    dasl::{self, cid::Cid, drisl::serde_bytes},
    types::{IncomingEvent, LeafQuery, ModuleCodec, SqlRows},
};
use serde::{Deserialize, Serialize};
use socketioxide::extract::{AckSender, SocketRef, TryData};
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use ulid::Ulid;

fn response<T: Serialize>(v: anyhow::Result<T>) -> bytes::Bytes {
    bytes::Bytes::from_owner(
        dasl::drisl::to_vec(&v.map_err(|e| e.to_string()))
            .expect("Unable to serialize API response"),
    )
}

use crate::{did::create_did, error::LogError, storage::STORAGE, streams::STREAMS};

pub fn setup_socket_handlers(socket: &SocketRef, did: Option<String>) {
    let span = Span::current();

    let open_streams = Arc::new(RwLock::new(HashMap::new()));
    let unsubscribers = Arc::new(Mutex::new(HashMap::new()));

    let did_ = did.clone();
    let span_ = span.clone();
    socket.on(
        "module/upload",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can upload module");
                };
                let bytes = bytes?;
                if bytes.len() > 1024 * 1024 * 10 {
                    anyhow::bail!("Module larger than 10MB maximum size.");
                }
                let args: ModuleUploadArgs = dasl::drisl::from_slice(&bytes[..])?;
                let hash = STORAGE.upload_module(&did_, args.module).await?;
                anyhow::Ok(hash)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle module/upload"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );
    let span_ = span.clone();
    socket.on(
        "module/exists",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let args: ModuleExistsArgs = dasl::drisl::from_slice(&bytes?[..])?;
                let cid = args.cid;
                let has_module = STORAGE.has_module_blob(cid).await?;
                anyhow::Ok(has_module)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle module/exists"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/create",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can create_streams");
                };
                let StreamCreateArgs { module_cid } = dasl::drisl::from_slice(&bytes?[..])?;

                // Abort early if we don't have the module
                if !STORAGE.has_module_blob(module_cid).await? {
                    anyhow::bail!(
                        "Module not found, it must be uploaded \
                        before creating a stream with it: {module_cid}"
                    )
                }

                let stream_did = create_did(did_.clone()).await?;

                let stream = STORAGE.create_stream(stream_did.clone(), did_).await?;
                open_streams_
                    .write()
                    .await
                    .insert(stream_did.clone(), stream.clone());

                // Set the stream's module
                STREAMS.update_module(stream, module_cid).await?;

                anyhow::Ok(stream_did)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/info",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async move {
                let StreamInfoArgs { stream_did } = dasl::drisl::from_slice(&bytes?[..])?;

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
                    stream
                };

                anyhow::Ok(StreamInfoResp {
                    module_cid: stream.module_cid().await,
                })
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/info"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/update_module",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async move {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only the stream creator can update its module");
                };

                let StreamUpdateModuleArgs {
                    stream_did,
                    module_cid,
                } = dasl::drisl::from_slice(&bytes?[..])?;

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                let stream_owners = STORAGE.get_did_owners(stream_did.clone()).await?;
                if !stream_owners.iter().any(|x| x == &did_) {
                    anyhow::bail!("Only a stream owner can update its module");
                }

                STREAMS.update_module(stream, module_cid).await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/update_module"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/event_batch",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can send events");
                };

                let StreamEventBatchArgs { stream_did, events } =
                    dasl::drisl::from_slice(&bytes?[..])?;

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
                    stream
                };

                stream
                    .add_events(
                        events
                            .into_iter()
                            .map(|x| IncomingEvent {
                                user: did_.clone(),
                                payload: x.0,
                            })
                            .collect(),
                    )
                    .await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/event_batch"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    // TODO: right now there's a weird situation where, even if you get an unauthorized error when
    // subscribing to a query, it will just keep returning a new unauthorized result every time a
    // new event comes in, which gives you info about the frequency of events and lets you leech
    // server resources a bit. Not sure if we need to adjust that or not yet. Rate limiting would
    // probably be good.

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    let socket_ = socket.clone();
    let unsubscribers_ = unsubscribers.clone();
    socket.on(
        "stream/subscribe",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let StreamSubscribeArgs { stream_did, query } =
                    dasl::drisl::from_slice(&bytes?[..])?;

                let subscription_id = Ulid::new();

                // TODO: maybe we just shouldn't have the client send the requesting user since we
                // already have it.
                if query.user != did_ {
                    anyhow::bail!(
                        "Some events in batch are not authored by the authenticated user."
                    );
                }

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
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
                            if let Err(e) = dasl::drisl::to_vec(&StreamSubscribeNotification {
                                subscription_id,
                                response: event.map_err(|e| e.to_string()),
                            })
                            .map_err(anyhow::Error::from)
                            .and_then(|encoded| {
                                socket_
                                    .emit(
                                        "stream/subscription_response",
                                        &bytes::Bytes::from_owner(encoded),
                                    )
                                    .map_err(anyhow::Error::from)
                            }) {
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

                anyhow::Ok(subscription_id)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/subscribe"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let unsubscribers_ = unsubscribers.clone();
    socket.on(
        "stream/unsubscribe",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let subscription_id: Ulid = dasl::drisl::from_slice(&bytes?[..])?;
                let unsubscriber = unsubscribers_.lock().await.remove(&subscription_id);
                let was_subscribed = unsubscriber.is_some();
                unsubscriber.map(|x| x.send(()));
                anyhow::Ok(was_subscribed)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/fetch"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/query",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let StreamQueryArgs { stream_did, query } = dasl::drisl::from_slice(&bytes?[..])?;

                // TODO: maybe we just shouldn't have the client send the requesting user since we
                // already have it.
                if query.user != did_ {
                    anyhow::bail!("Requesting user does not match authenticated user");
                }

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
                    stream
                };

                let response = stream.query(query).await?;

                anyhow::Ok(response)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/query"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );
}

#[derive(Deserialize)]
struct ModuleUploadArgs {
    module: ModuleCodec,
}

#[derive(Deserialize)]
struct ModuleExistsArgs {
    cid: Cid,
}

#[derive(Deserialize)]
struct StreamInfoArgs {
    stream_did: Did,
}

#[derive(Deserialize)]
struct StreamCreateArgs {
    module_cid: Cid,
}

#[derive(Deserialize)]
struct StreamUpdateModuleArgs {
    stream_did: Did,
    module_cid: Cid,
}

#[derive(Deserialize)]
struct StreamEventBatchArgs {
    stream_did: Did,
    events: Vec<EventPayload>,
}

#[derive(Deserialize)]
struct EventPayload(#[serde(with = "serde_bytes")] Vec<u8>);

#[derive(Deserialize)]
struct StreamQueryArgs {
    stream_did: Did,
    query: LeafQuery,
}

#[derive(Serialize)]
struct StreamSubscribeNotification {
    subscription_id: Ulid,
    response: Result<SqlRows, String>,
}

#[derive(Serialize)]
struct StreamInfoResp {
    module_cid: Option<Cid>,
}

#[derive(Deserialize)]
struct StreamSubscribeArgs {
    stream_did: Did,
    query: LeafQuery,
}
