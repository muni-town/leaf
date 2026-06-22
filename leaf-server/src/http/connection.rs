use std::{collections::HashMap, sync::Arc, time::Duration};

use async_lock::{Mutex, RwLock, RwLockUpgradableReadGuard};
use futures::future::{Either, select};
use leaf_stream::{
    atproto_plc::Did,
    dasl::{self, cid::Cid, drisl::serde_bytes},
    types::{IncomingEvent, LeafQuery, LeafSubscribeEventsResponse, ModuleCodec},
};
use serde::{Deserialize, Serialize};
use socketioxide::{
    SendError,
    extract::{AckSender, SocketRef, TryData},
};
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use ulid::Ulid;

fn response<T: Serialize>(v: anyhow::Result<T>) -> bytes::Bytes {
    bytes::Bytes::from_owner(
        dasl::drisl::to_vec(&v.map_err(|e| e.to_string()))
            .expect("Unable to serialize API response"),
    )
}

use crate::{
    ARGS,
    cli::Command,
    did::{create_did, update_did_handle},
    error::LogError,
    storage::STORAGE,
    streams::STREAMS,
};

pub fn setup_socket_handlers(socket: &SocketRef, did: Option<String>, is_unsafe_auth: bool) {
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
                let module_cid = STORAGE.upload_module(&did_, args.module).await?;
                anyhow::Ok(ModuleUploadResp { module_cid })
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
                let cid = args.module_cid;
                let has_module = STORAGE.has_module_blob(cid).await?;
                anyhow::Ok(ModuleExistsResp {
                    module_exists: has_module,
                })
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

                let stream = STORAGE.create_stream(stream_did.clone()).await?;
                open_streams_
                    .write()
                    .await
                    .insert(stream_did.clone(), stream.clone());

                // Set the stream's module
                STREAMS.update_module(stream, module_cid).await?;

                anyhow::Ok(StreamCreateResp { stream_did })
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

                let mut module_admins = STORAGE
                    // The stream owners are allowed to admin the stream module
                    .get_did_owners(stream_did.clone())
                    .await?
                    .into_iter()
                    // As well as the module admins specified when starting the Leaf server
                    .chain(ARGS.module_admins.iter().cloned());

                if !module_admins.any(|x| x == did_) {
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

                let StreamEventBatchArgs {
                    stream_did,
                    events,
                    user_override,
                } = dasl::drisl::from_slice(&bytes?[..])?;

                // Determine the effective user DID for this batch.
                let user = match user_override {
                    Some(override_did) if is_unsafe_auth => override_did,
                    Some(_) => {
                        anyhow::bail!(
                            "user_override is only allowed for trusted (unsafe_auth_token) connections"
                        );
                    }
                    None => did_.clone(),
                };

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                let signing_key = STORAGE.get_did_signing_key(stream_did).await?;

                stream
                    .add_events(
                        signing_key,
                        events
                            .into_iter()
                            .map(|x| IncomingEvent {
                                user: user.clone(),
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

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/state_event_batch",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can send state events");
                };

                let StreamStateEventBatchArgs {
                    stream_did,
                    events,
                    user_override,
                } = dasl::drisl::from_slice(&bytes?[..])?;

                // Determine the effective user DID for this batch.
                let user = match user_override {
                    Some(override_did) if is_unsafe_auth => override_did,
                    Some(_) => {
                        anyhow::bail!(
                            "user_override is only allowed for trusted (unsafe_auth_token) connections"
                        );
                    }
                    None => did_.clone(),
                };

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                stream
                    .add_state_events(
                        events
                            .into_iter()
                            .map(|x| IncomingEvent {
                                user: user.clone(),
                                payload: x.0,
                            })
                            .collect(),
                    )
                    .await?;

                anyhow::Ok(())
            }
            .instrument(
                tracing::info_span!(parent: span_.clone(), "handle stream/state_event_batch"),
            )
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
        "stream/clear_state",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can clear state");
                };

                let StreamClearStateArgs { stream_did } = dasl::drisl::from_slice(&bytes?[..])?;

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                // Check if user owns the stream
                let stream_owners = STORAGE.get_did_owners(stream_did.clone()).await?;
                if !stream_owners.iter().any(|x| x == &did_) {
                    anyhow::bail!("Only a stream owner can set its handle");
                }

                stream.clear_state_db().await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/clear_state"))
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
        "stream/subscribe_events",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let StreamSubscribeArgs { stream_did, query } =
                    dasl::drisl::from_slice(&bytes?[..])?;

                let subscription_id = Ulid::new();

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
                    stream
                };

                let receiver = stream.subscribe_events(did_.clone(), query).await;
                let stream_for_task = stream.clone();

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
                            let encoded = match dasl::drisl::to_vec(&StreamSubscribeNotification {
                                subscription_id,
                                response: event.map_err(|e| e.to_string()),
                            }) {
                                Ok(encoded) => encoded,
                                Err(e) => {
                                    tracing::error!("Error encoding subscription response: {e}");
                                    continue;
                                }
                            };
                            let data = bytes::Bytes::from_owner(encoded);

                            // Retry loop for backpressure: if engine.io's internal channel is
                            // full (client overwhelmed), wait and retry instead of dropping the
                            // message.
                            let mut retry_delay = Duration::from_millis(10);
                            let max_delay = Duration::from_secs(1);
                            loop {
                                match socket_.emit("stream/subscription_response", &data) {
                                    Ok(()) => break,
                                    Err(SendError::Socket(
                                        socketioxide::SocketError::InternalChannelFull,
                                    )) => {
                                        if !socket_.connected() {
                                            tracing::warn!(
                                                "Client disconnected while retrying emit, stopping"
                                            );
                                            break;
                                        }
                                        tracing::debug!(
                                            ?retry_delay,
                                            "Engine.io channel full, retrying emit"
                                        );
                                        tokio::time::sleep(retry_delay).await;
                                        retry_delay = (retry_delay * 2).min(max_delay);
                                    }
                                    Err(e) => {
                                        tracing::error!("Error sending event via socket.io: {e}");
                                        break;
                                    }
                                }
                            }
                        } else if let Some(unsubscriber) =
                            unsubscribers_.lock().await.remove(&subscription_id)
                        {
                            tracing::info!("Client disconnected, canceling subscription");
                            unsubscriber.send(()).ok();
                        }
                    }

                    // Eagerly clean up the subscription from the stream
                    stream_for_task.close_query_subscription(subscription_id).await;
                });

                anyhow::Ok(StreamSubscribeResp { subscription_id })
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
                let StreamUnsubscribeArgs { subscription_id } =
                    dasl::drisl::from_slice(&bytes?[..])?;
                let unsubscriber = unsubscribers_.lock().await.remove(&subscription_id);
                let was_subscribed = unsubscriber.is_some();
                unsubscriber.map(|x| x.send(()));
                anyhow::Ok(StreamUnsubscribeResp { was_subscribed })
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

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did, stream.clone());
                    stream
                };

                let response = stream.query(did_.clone(), query).await?;

                anyhow::Ok(response)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/query"))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    socket.on(
        "admin/list_streams",
        async move |_data: TryData<bytes::Bytes>, ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Authentication required");
                };

                // Only allow the server's own DID (i.e. unsafe auth token) to list all DIDs
                let Command::Server(server_args) = &ARGS.command else {
                    anyhow::bail!("Unreachable: not in server mode");
                };
                if did_ != server_args.did {
                    anyhow::bail!("Only the server admin can list all DIDs");
                }

                let streams = STORAGE.list_streams().await?;
                let dids: Vec<AdminListStreamsItem> = streams
                    .into_iter()
                    .map(|s| AdminListStreamsItem { did: s.did })
                    .collect();

                anyhow::Ok(AdminListStreamsResp { streams: dids })
            }
            .instrument(tracing::info_span!(
                parent: span_.clone(),
                "handle admin/list_streams"
            ))
            .await;

            ack.send(&response(result))
                .log_error("Internal error sending response")
                .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    socket.on(
        "stream/set_handle",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let Some(did_) = did_ else {
                    anyhow::bail!("Only the stream creator can update its handle");
                };

                let StreamSetHandleArgs { stream_did, handle } =
                    dasl::drisl::from_slice(&bytes?[..])?;

                // Check if user owns the stream
                let stream_owners = STORAGE.get_did_owners(stream_did.clone()).await?;
                if !stream_owners.iter().any(|x| x == &did_) {
                    anyhow::bail!("Only a stream owner can set its handle");
                }

                // Update the DID document with the new handle
                update_did_handle(stream_did, handle).await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/set_handle"))
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ModuleUploadResp {
    module_cid: Cid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModuleExistsArgs {
    module_cid: Cid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ModuleExistsResp {
    module_exists: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamInfoArgs {
    stream_did: Did,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamInfoResp {
    module_cid: Option<Cid>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamCreateArgs {
    module_cid: Cid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamCreateResp {
    stream_did: Did,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamUpdateModuleArgs {
    stream_did: Did,
    module_cid: Cid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamEventBatchArgs {
    stream_did: Did,
    events: Vec<EventPayload>,
    /// Override the `user` field on all events in this batch.
    ///
    /// Only accepted when the connection authenticated via the shared
    /// `unsafe_auth_token` — regular JWT-authenticated connections must
    /// use their own DID and will receive an error if this is set.
    #[serde(default)]
    user_override: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamStateEventBatchArgs {
    stream_did: Did,
    events: Vec<EventPayload>,
    /// Override the `user` field on all events in this batch.
    ///
    /// Only accepted when the connection authenticated via the shared
    /// `unsafe_auth_token`.
    #[serde(default)]
    user_override: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamClearStateArgs {
    stream_did: Did,
}

#[derive(Deserialize)]
struct EventPayload(#[serde(with = "serde_bytes")] Vec<u8>);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamQueryArgs {
    stream_did: Did,
    query: LeafQuery,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamSetHandleArgs {
    stream_did: Did,
    handle: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamSubscribeArgs {
    stream_did: Did,
    query: LeafQuery,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamSubscribeResp {
    subscription_id: Ulid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamSubscribeNotification {
    subscription_id: Ulid,
    response: Result<LeafSubscribeEventsResponse, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminListStreamsItem {
    did: Did,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminListStreamsResp {
    streams: Vec<AdminListStreamsItem>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamUnsubscribeArgs {
    subscription_id: Ulid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamUnsubscribeResp {
    was_subscribed: bool,
}
