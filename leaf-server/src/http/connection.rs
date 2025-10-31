use std::{collections::HashMap, sync::Arc};

use async_lock::{Mutex, RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use bytes::Bytes;
use futures::future::{Either, select};
use leaf_stream::{StreamGenesis, encoding::Encodable, types::LeafQuery};
use serde::{Deserialize, Serialize};
use serde_json::json;
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
            match result {
                Ok(hash) => ack.send(&json!({ "hash": hash.to_hex().to_string() })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );
    let span_ = span.clone();
    socket.on(
        "wasm/has",
        async move |TryData::<String>(hash_hex), ack: AckSender| {
            let result = async {
                let hash_hex = hash_hex?;
                let hash = Hash::from_hex(hash_hex)?;
                let has_module = STORAGE.has_blob(hash).await?;
                anyhow::Ok(has_module)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle wasm/has"))
            .await;

            match result {
                Ok(response) => ack.send(&json!({ "hasModule": response })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/create",
        async move |TryData::<StreamCreateArgs>(data), ack: AckSender| {
            let result = async {
                // Create the stream
                let input = data?;
                let genesis = StreamGenesis {
                    stamp: Encodable(input.ulid),
                    creator: did_,
                    module: Hash::from_hex(input.module)?.into(),
                    params: input.params,
                };
                let stream_id = STORAGE.create_stream(genesis).await?;
                anyhow::Ok(stream_id)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            match result {
                Ok(stream) => {
                    let id = stream.id();
                    open_streams_.write().await.insert(id, stream);
                    ack.send(&json!({ "streamId": id.to_hex().as_str() }))
                }
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/event",
        async move |TryData::<StreamEventArgs>(data), ack: AckSender| {
            let result = async {
                let request = data?;
                let stream_id = Hash::from_hex(request.id)?;
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

                stream.handle_event(did_, request.payload).await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_, "handle stream/event"))
            .await;

            match result {
                Ok(_) => ack.send(&json!({ "ok": true })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/event_batch",
        async move |TryData::<StreamEventBatchArgs>(data), ack: AckSender| {
            let result = async {
                let request = data?;
                let stream_id = Hash::from_hex(request.id)?;
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

                for paylaod in request.payloads {
                    stream.handle_event(did_.clone(), paylaod.into()).await?;
                }

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/event"))
            .await;

            match result {
                Ok(_) => ack.send(&json!({ "ok": true })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
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
        async move |TryData::<String>(data), ack: AckSender| {
            let result = async {
                let stream_id = data?;
                let stream_id = Hash::from_hex(stream_id)?;
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

                let mut receiver = stream.subscribe(&did_).await;

                tokio::spawn(async move {
                    let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();

                    unsubscribers_
                        .lock()
                        .await
                        .insert(stream_id, unsubscribe_tx);

                    let mut next_event = receiver.recv();
                    let mut unsubscribe = unsubscribe_rx;
                    while let Either::Left((Ok(event), _)) =
                        select(next_event, &mut unsubscribe).await
                    {
                        next_event = receiver.recv();

                        if socket_.connected() {
                            if let Err(e) = socket_.emit(
                                "stream/event",
                                &StreamEventNotification {
                                    stream: stream_id.to_hex().to_string(),
                                    idx: event.idx,
                                    user: event.user,
                                    payload: event.payload.into(),
                                },
                            ) {
                                // TODO: better error message
                                tracing::error!("Error sending event, unsubscribing: {e}");
                            }
                        } else if let Some(unsubscriber) =
                            unsubscribers_.lock().await.remove(&stream_id)
                        {
                            tracing::info!("Client disconnected, canceling subscription");
                            unsubscriber.send(()).ok();
                        }
                    }
                });

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/subscribe"))
            .await;

            match result {
                Ok(_) => ack.send(&json!({ "ok": true })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let unsubscribers_ = unsubscribers.clone();
    socket.on(
        "stream/unsubscribe",
        async move |TryData::<String>(data), ack: AckSender| {
            let result = async {
                let stream_id_hex = data?;
                let stream_id = Hash::from_hex(stream_id_hex)?;

                let unsubscriber = unsubscribers_.lock().await.remove(&stream_id);
                let was_subscribed = unsubscriber.is_some();
                unsubscriber.map(|x| x.send(()));
                anyhow::Ok(was_subscribed)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/fetch"))
            .await;

            match result {
                Ok(was_subscribed) => {
                    ack.send(&json!({ "ok": true, "wasSubscribed": was_subscribed }))
                }
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/fetch",
        async move |TryData::<StreamFetchArgs>(data), ack: AckSender| {
            let result = async {
                let args = data?;
                let stream_id = Hash::from_hex(args.id)?;
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

                let events = stream
                    .fetch_events(LeafQuery {
                        requesting_user: did_.clone(),
                        start: Some(args.offset.try_into()?),
                        end: None,
                        limit: args.limit.try_into()?,
                        filter: args.filter.map(Into::into),
                    })
                    .await?
                    .into_iter()
                    .map(|x| StreamFetchResponseEvent {
                        idx: x.idx,
                        user: x.user,
                        payload: x.payload.into(),
                    })
                    .collect::<Vec<_>>();

                anyhow::Ok(events)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/fetch"))
            .await;

            match result {
                Ok(events) => ack.send(&StreamFetchResponse { events }),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/info",
        async move |TryData::<StreamInfoArgs>(data), ack: AckSender| {
            let result = async {
                let StreamInfoArgs { stream_id } = data?;
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

                let genesis = stream.genesis().await?;

                anyhow::Ok(StreamInfoResponse::from(genesis))
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/info"))
            .await;

            match result {
                Ok(resp) => ack.send(&resp),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );
}

// TODO: make these types more compact by making their field names shorter, or maybe just use the
// non-self-describing SCALE codec with the client to make it even more compact? The same goes for
// the event names. Maybe we can collapse `stream/event` to `s/e`?
//
// Maybe that's not necessary, but at the same time maybe it's wasteful to send longer names when
// _that_ is unnecessary.

#[derive(Deserialize)]
struct StreamCreateArgs {
    /// The hex-encoded, blake3 hash of the WASM module to use to create the stream.
    module: String,
    params: Vec<u8>,
    ulid: Ulid,
}

#[derive(Deserialize)]
struct StreamEventArgs {
    /// The hex-encoded stream ID
    id: String,
    /// The event payload
    payload: Vec<u8>,
}

#[derive(Deserialize)]
struct StreamEventBatchArgs {
    /// The hex-encoded stream ID
    id: String,
    /// The event payload
    payloads: Vec<bytes::Bytes>,
}

#[derive(Deserialize, Debug)]
struct StreamFetchArgs {
    /// The hex-encoded stream ID
    pub id: String,
    /// The event offset to start fetching from
    pub offset: u64,
    /// The limit on the number of events to fetch
    pub limit: u64,
    /// An optional filter to provide to limit the results returned by the filter module
    pub filter: Option<Bytes>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamEventNotification {
    pub stream: String,
    pub idx: i64,
    pub user: String,
    pub payload: bytes::Bytes,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamFetchResponseEvent {
    pub idx: i64,
    pub user: String,
    pub payload: bytes::Bytes,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamFetchResponse {
    events: Vec<StreamFetchResponseEvent>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StreamInfoArgs {
    #[serde(deserialize_with = "deserialize_from_str")]
    pub stream_id: Hash,
}

#[derive(Clone, Debug, Serialize)]
pub struct StreamInfoResponse {
    pub stamp: Ulid,
    pub creator: String,
    pub module: String,
    pub params: bytes::Bytes,
}

impl From<StreamGenesis> for StreamInfoResponse {
    fn from(v: StreamGenesis) -> Self {
        Self {
            stamp: v.stamp.0,
            creator: v.creator,
            module: v.module.0.to_hex().to_string(),
            params: v.params.into(),
        }
    }
}

fn deserialize_from_str<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(serde::de::Error::custom)
}
