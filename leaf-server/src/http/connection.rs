use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use async_lock::{Mutex, RwLock, RwLockUpgradableReadGuard};
use futures::future::{Either, select};
use leaf_stream::{
    atproto_plc::Did,
    dasl::{self, cid::Cid, drisl::serde_bytes},
    types::{IncomingEvent, LeafQuery, LeafSubscribeEventsResponse, ModuleCodec},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use socketioxide::extract::{AckSender, SocketRef, TryData};
use tokio::sync::oneshot;
use tracing::{Instrument, Span};

use crate::cli::Command;
use ulid::Ulid;

fn response<T: Serialize>(v: anyhow::Result<T>) -> bytes::Bytes {
    bytes::Bytes::from_owner(
        dasl::drisl::to_vec(&v.map_err(|e| e.to_string()))
            .expect("Unable to serialize API response"),
    )
}

use crate::{
    ARGS,
    did::{create_did, update_did_handle},
    error::LogError,
    storage::STORAGE,
    streams::STREAMS,
};

#[derive(Clone, Copy)]
enum ThrottleEndpoint {
    StreamCreate,
    StreamSubscribe,
}

impl ThrottleEndpoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::StreamCreate => "stream/create",
            Self::StreamSubscribe => "stream/subscribe_events",
        }
    }
}

#[derive(Clone, Copy)]
struct ThrottleConfig {
    window: Duration,
    stream_create_limit: u32,
    stream_subscribe_limit: u32,
    max_active_subscriptions: u32,
}

impl ThrottleConfig {
    fn from_args() -> Self {
        let Command::Server(server_args) = &ARGS.command;
        Self {
            window: Duration::from_secs(server_args.throttle_window_secs.max(1)),
            stream_create_limit: server_args.stream_create_limit_per_window.max(1),
            stream_subscribe_limit: server_args.stream_subscribe_limit_per_window.max(1),
            max_active_subscriptions: server_args.max_active_subscriptions_per_actor.max(1),
        }
    }
}

#[derive(Default)]
struct ActorThrottleState {
    window_started: Option<Instant>,
    stream_create_attempts: u32,
    stream_subscribe_attempts: u32,
    active_subscriptions: u32,
}

struct ThrottleCheck {
    allowed: bool,
    attempts_in_window: u32,
    limit: u32,
    active_subscriptions: u32,
    retry_after: Duration,
}

#[derive(Default)]
struct ThrottleStats {
    limited_requests_total: u64,
}

struct ConnectionThrottler {
    config: ThrottleConfig,
    state: Mutex<HashMap<String, ActorThrottleState>>,
    stats: Mutex<ThrottleStats>,
}

impl ConnectionThrottler {
    fn new(config: ThrottleConfig) -> Self {
        Self {
            config,
            state: Mutex::new(HashMap::new()),
            stats: Mutex::new(ThrottleStats::default()),
        }
    }

    fn prune_stale_actor_state(
        &self,
        state: &mut HashMap<String, ActorThrottleState>,
        now: Instant,
    ) {
        let retention = self.config.window.saturating_mul(2);
        state.retain(|_, actor_state| {
            actor_state.active_subscriptions > 0
                || actor_state
                    .window_started
                    .is_some_and(|started| now.duration_since(started) < retention)
        });
    }

    async fn check_and_record(&self, actor_key: &str, endpoint: ThrottleEndpoint) -> ThrottleCheck {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        self.prune_stale_actor_state(&mut state, now);
        let actor_state = state.entry(actor_key.to_string()).or_default();

        let window_started = actor_state.window_started.get_or_insert(now);
        if now.duration_since(*window_started) >= self.config.window {
            actor_state.window_started = Some(now);
            actor_state.stream_create_attempts = 0;
            actor_state.stream_subscribe_attempts = 0;
        }

        let retry_after = self
            .config
            .window
            .saturating_sub(now.duration_since(actor_state.window_started.unwrap_or(now)));

        let (attempts, limit) = match endpoint {
            ThrottleEndpoint::StreamCreate => {
                actor_state.stream_create_attempts =
                    actor_state.stream_create_attempts.saturating_add(1);
                (
                    actor_state.stream_create_attempts,
                    self.config.stream_create_limit,
                )
            }
            ThrottleEndpoint::StreamSubscribe => {
                actor_state.stream_subscribe_attempts =
                    actor_state.stream_subscribe_attempts.saturating_add(1);
                (
                    actor_state.stream_subscribe_attempts,
                    self.config.stream_subscribe_limit,
                )
            }
        };

        let mut allowed = attempts <= limit;
        if matches!(endpoint, ThrottleEndpoint::StreamSubscribe)
            && actor_state.active_subscriptions >= self.config.max_active_subscriptions
        {
            allowed = false;
        }

        if !allowed {
            let mut stats = self.stats.lock().await;
            stats.limited_requests_total = stats.limited_requests_total.saturating_add(1);
            tracing::warn!(
                actor_key,
                endpoint = endpoint.as_str(),
                attempts_in_window = attempts,
                limit,
                active_subscriptions = actor_state.active_subscriptions,
                limited_requests_total = stats.limited_requests_total,
                "Throttled high-risk endpoint"
            );
        }

        ThrottleCheck {
            allowed,
            attempts_in_window: attempts,
            limit,
            active_subscriptions: actor_state.active_subscriptions,
            retry_after,
        }
    }

    async fn inc_active_subscription(&self, actor_key: &str) -> u32 {
        let mut state = self.state.lock().await;
        let actor_state = state.entry(actor_key.to_string()).or_default();
        actor_state.active_subscriptions = actor_state.active_subscriptions.saturating_add(1);
        actor_state.active_subscriptions
    }

    async fn dec_active_subscription(&self, actor_key: &str) -> u32 {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        let actor_state = state.entry(actor_key.to_string()).or_default();
        actor_state.active_subscriptions = actor_state.active_subscriptions.saturating_sub(1);
        let active = actor_state.active_subscriptions;
        self.prune_stale_actor_state(&mut state, now);
        active
    }
}

#[cfg(test)]
impl ConnectionThrottler {
    async fn actor_state_len(&self) -> usize {
        self.state.lock().await.len()
    }
}

static CONNECTION_THROTTLER: LazyLock<ConnectionThrottler> =
    LazyLock::new(|| ConnectionThrottler::new(ThrottleConfig::from_args()));

fn actor_key(did: Option<&String>, anonymous_key: &str) -> String {
    did.cloned().unwrap_or_else(|| anonymous_key.to_string())
}

fn throttle_error_message(
    endpoint: ThrottleEndpoint,
    check: &ThrottleCheck,
    max_active_subscriptions: u32,
) -> String {
    json!({
        "code": "rate_limited",
        "endpoint": endpoint.as_str(),
        "attemptsInWindow": check.attempts_in_window,
        "limit": check.limit,
        "activeSubscriptions": check.active_subscriptions,
        "maxActiveSubscriptions": max_active_subscriptions,
        "retryAfterMs": check.retry_after.as_millis(),
    })
    .to_string()
}

struct SubscriptionControl {
    unsubscribe_tx: oneshot::Sender<()>,
    actor_key: String,
}

pub fn setup_socket_handlers(socket: &SocketRef, did: Option<String>) {
    let span = Span::current();

    let open_streams = Arc::new(RwLock::new(HashMap::new()));
    let unsubscribers: Arc<Mutex<HashMap<Ulid, SubscriptionControl>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let anonymous_actor_key = format!("anonymous:{}", Ulid::new());

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
    let anonymous_actor_key_ = anonymous_actor_key.clone();
    socket.on(
        "stream/create",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let actor_key = actor_key(did_.as_ref(), &anonymous_actor_key_);
                let check = CONNECTION_THROTTLER
                    .check_and_record(&actor_key, ThrottleEndpoint::StreamCreate)
                    .await;
                tracing::info!(
                    actor_key,
                    endpoint = "stream/create",
                    create_attempts_in_window = check.attempts_in_window,
                    create_limit = check.limit,
                    "Recorded stream/create attempt"
                );
                if !check.allowed {
                    anyhow::bail!(
                        "{}",
                        throttle_error_message(
                            ThrottleEndpoint::StreamCreate,
                            &check,
                            CONNECTION_THROTTLER.config.max_active_subscriptions,
                        )
                    );
                }

                let Some(did_) = did_ else {
                    anyhow::bail!("Only authenticated users can create_streams");
                };
                let StreamCreateArgs {
                    module_cid,
                    client_stamp,
                } = dasl::drisl::from_slice(&bytes?[..])?;

                // Abort early if we don't have the module
                if !STORAGE.has_module_blob(module_cid).await? {
                    anyhow::bail!(
                        "Module not found, it must be uploaded \
                        before creating a stream with it: {module_cid}"
                    )
                }

                let stream_did = create_did(did_.clone()).await?;

                let stream = STORAGE
                    .create_stream(
                        stream_did.clone(),
                        did_,
                        client_stamp.map(|stamp| stamp.to_string()),
                    )
                    .await?;
                open_streams_
                    .write()
                    .await
                    .insert(stream_did.clone(), stream.clone());

                // Set the stream's module
                STREAMS.update_module(stream, module_cid).await?;

                anyhow::Ok(StreamCreateResp {
                    stream_did,
                    client_stamp,
                })
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
                let _stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                let (module_cid, client_stamp) = STORAGE.get_stream_info(&stream_did).await?;

                anyhow::Ok(StreamInfoResp {
                    module_cid,
                    client_stamp,
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

                let StreamStateEventBatchArgs { stream_did, events } =
                    dasl::drisl::from_slice(&bytes?[..])?;

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
                                user: did_.clone(),
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
    let anonymous_actor_key_ = anonymous_actor_key.clone();
    socket.on(
        "stream/subscribe_events",
        async move |TryData::<bytes::Bytes>(bytes), ack: AckSender| {
            let result = async {
                let actor_key = actor_key(did_.as_ref(), &anonymous_actor_key_);
                let check = CONNECTION_THROTTLER
                    .check_and_record(&actor_key, ThrottleEndpoint::StreamSubscribe)
                    .await;
                tracing::info!(
                    actor_key,
                    endpoint = "stream/subscribe_events",
                    subscribe_attempts_in_window = check.attempts_in_window,
                    subscribe_limit = check.limit,
                    active_subscriptions = check.active_subscriptions,
                    "Recorded stream/subscribe_events attempt"
                );
                if !check.allowed {
                    anyhow::bail!(
                        "{}",
                        throttle_error_message(
                            ThrottleEndpoint::StreamSubscribe,
                            &check,
                            CONNECTION_THROTTLER.config.max_active_subscriptions,
                        )
                    );
                }

                let StreamSubscribeArgs { stream_did, query } =
                    dasl::drisl::from_slice(&bytes?[..])?;

                let subscription_id = Ulid::new();

                let open_streams = open_streams_.upgradable_read().await;
                let stream = if let Some(stream) = open_streams.get(&stream_did) {
                    stream.clone()
                } else {
                    let stream = STREAMS.load(stream_did.clone()).await?;
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_did.clone(), stream.clone());
                    stream
                };

                let receiver = stream.subscribe_events(did_.clone(), query).await;

                let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
                unsubscribers_.lock().await.insert(
                    subscription_id,
                    SubscriptionControl {
                        unsubscribe_tx,
                        actor_key: actor_key.clone(),
                    },
                );
                let active_subscriptions = CONNECTION_THROTTLER
                    .inc_active_subscription(&actor_key)
                    .await;
                tracing::info!(
                    actor_key,
                    active_subscriptions,
                    endpoint = "stream/subscribe_events",
                    "Active subscription count increased"
                );

                tokio::spawn(async move {
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
                                tracing::error!("Error sending event, unsubscribing: {e}");
                            }
                        } else if let Some(unsubscriber) =
                            unsubscribers_.lock().await.remove(&subscription_id)
                        {
                            tracing::info!("Client disconnected, canceling subscription");
                            let active_subscriptions = CONNECTION_THROTTLER
                                .dec_active_subscription(&unsubscriber.actor_key)
                                .await;
                            tracing::info!(
                                actor_key = unsubscriber.actor_key,
                                active_subscriptions,
                                endpoint = "stream/subscribe_events",
                                "Active subscription count decreased"
                            );
                            unsubscriber.unsubscribe_tx.send(()).ok();
                        }
                    }

                    if let Some(subscription) = unsubscribers_.lock().await.remove(&subscription_id)
                    {
                        let active_subscriptions = CONNECTION_THROTTLER
                            .dec_active_subscription(&subscription.actor_key)
                            .await;
                        tracing::info!(
                            actor_key = subscription.actor_key,
                            active_subscriptions,
                            endpoint = "stream/subscribe_events",
                            "Active subscription count decreased"
                        );
                    }
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
                if let Some(subscription) = unsubscriber {
                    let active_subscriptions = CONNECTION_THROTTLER
                        .dec_active_subscription(&subscription.actor_key)
                        .await;
                    tracing::info!(
                        actor_key = subscription.actor_key,
                        active_subscriptions,
                        endpoint = "stream/subscribe_events",
                        "Active subscription count decreased"
                    );
                    subscription.unsubscribe_tx.send(()).ok();
                }
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
                    open_streams.insert(stream_did.clone(), stream.clone());
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
    client_stamp: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamCreateArgs {
    module_cid: Cid,
    client_stamp: Option<Ulid>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamCreateResp {
    stream_did: Did,
    client_stamp: Option<Ulid>,
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionThrottler, StreamCreateArgs, StreamCreateResp, StreamInfoResp, ThrottleConfig,
        ThrottleEndpoint,
    };
    use leaf_stream::{
        atproto_plc::Did,
        dasl::{
            cid::Cid,
            drisl::{from_slice, to_vec},
        },
    };
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use ulid::Ulid;

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LegacyStreamInfoResp {
        module_cid: Option<Cid>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct LegacyStreamInfoClientView {
        module_cid: Option<Cid>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StreamInfoRespCompat {
        module_cid: Option<Cid>,
        client_stamp: Option<String>,
    }

    #[test]
    fn stream_info_legacy_shape_deserializes_without_client_stamp() {
        let module_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
            .parse::<Cid>()
            .expect("parse cid");
        let bytes = to_vec(&LegacyStreamInfoResp {
            module_cid: Some(module_cid.clone()),
        })
        .expect("serialize legacy stream info response");

        let parsed: StreamInfoRespCompat =
            from_slice(&bytes).expect("deserialize legacy stream info");

        assert_eq!(parsed.module_cid, Some(module_cid));
        assert_eq!(parsed.client_stamp, None);
    }

    #[test]
    fn stream_info_with_client_stamp_round_trips() {
        let module_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
            .parse::<Cid>()
            .expect("parse cid");
        let client_stamp = "01J9A90M5Q6VFXV9PRN00TS9TW".to_string();
        let bytes = to_vec(&StreamInfoResp {
            module_cid: Some(module_cid.clone()),
            client_stamp: Some(client_stamp.clone()),
        })
        .expect("serialize stamped stream info response");

        let parsed: StreamInfoRespCompat =
            from_slice(&bytes).expect("deserialize stamped stream info");

        assert_eq!(parsed.module_cid, Some(module_cid));
        assert_eq!(parsed.client_stamp, Some(client_stamp));
    }
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct LegacyStreamCreateArgs {
        module_cid: Cid,
    }

    #[test]
    fn stream_create_args_old_shape_still_deserializes() {
        let module_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
            .parse::<Cid>()
            .expect("parse cid");
        let bytes = to_vec(&LegacyStreamCreateArgs {
            module_cid: module_cid.clone(),
        })
        .expect("serialize old request shape");

        let parsed: StreamCreateArgs = from_slice(&bytes).expect("deserialize old request shape");

        assert_eq!(parsed.module_cid, module_cid);
        assert_eq!(parsed.client_stamp, None);
    }

    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct StreamCreateArgsWithClientStamp {
        module_cid: Cid,
        client_stamp: Option<Ulid>,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct StreamCreateAckCompat {
        stream_did: Did,
        client_stamp: Option<Ulid>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct LegacyStreamCreateAckClientView {
        stream_did: Did,
    }

    #[test]
    fn stream_create_with_client_stamp_round_trips() {
        let module_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
            .parse::<Cid>()
            .expect("parse cid");
        let stream_did = "did:plc:z72i7hdynmk6r22z27h6tvur"
            .parse::<Did>()
            .expect("parse did");
        let client_stamp = Ulid::new();

        let args_bytes = to_vec(&StreamCreateArgsWithClientStamp {
            module_cid,
            client_stamp: Some(client_stamp),
        })
        .expect("serialize request with client stamp");

        let parsed_args: StreamCreateArgs =
            from_slice(&args_bytes).expect("deserialize request with client stamp");
        assert_eq!(parsed_args.client_stamp, Some(client_stamp));

        let resp_bytes = to_vec(&StreamCreateResp {
            stream_did: stream_did.clone(),
            client_stamp: Some(client_stamp),
        })
        .expect("serialize response");

        let parsed_resp: StreamCreateAckCompat =
            from_slice(&resp_bytes).expect("deserialize response with client stamp");
        assert_eq!(parsed_resp.stream_did, stream_did);
        assert_eq!(parsed_resp.client_stamp, Some(client_stamp));
    }

    #[test]
    fn stream_info_upgraded_response_is_ignored_by_legacy_client_parser() {
        let module_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
            .parse::<Cid>()
            .expect("parse cid");
        let bytes = to_vec(&StreamInfoResp {
            module_cid: Some(module_cid.clone()),
            client_stamp: Some("01J9A90M5Q6VFXV9PRN00TS9TW".to_string()),
        })
        .expect("serialize upgraded stream info response");

        let parsed: LegacyStreamInfoClientView =
            from_slice(&bytes).expect("deserialize into legacy client stream info shape");

        assert_eq!(parsed.module_cid, Some(module_cid));
    }

    #[tokio::test]
    async fn stream_create_throttle_allows_under_limit() {
        let throttler = ConnectionThrottler::new(ThrottleConfig {
            window: Duration::from_millis(200),
            stream_create_limit: 3,
            stream_subscribe_limit: 10,
            max_active_subscriptions: 5,
        });

        let first = throttler
            .check_and_record("did:plc:underlimit", ThrottleEndpoint::StreamCreate)
            .await;
        let second = throttler
            .check_and_record("did:plc:underlimit", ThrottleEndpoint::StreamCreate)
            .await;

        assert!(first.allowed);
        assert!(second.allowed);
        assert_eq!(second.attempts_in_window, 2);
    }

    #[tokio::test]
    async fn stream_create_throttle_rejects_over_limit() {
        let throttler = ConnectionThrottler::new(ThrottleConfig {
            window: Duration::from_millis(200),
            stream_create_limit: 1,
            stream_subscribe_limit: 10,
            max_active_subscriptions: 5,
        });

        let first = throttler
            .check_and_record("did:plc:overlimit", ThrottleEndpoint::StreamCreate)
            .await;
        let second = throttler
            .check_and_record("did:plc:overlimit", ThrottleEndpoint::StreamCreate)
            .await;

        assert!(first.allowed);
        assert!(!second.allowed);
        assert_eq!(second.attempts_in_window, 2);
    }

    #[tokio::test]
    async fn throttle_prunes_stale_anonymous_actor_entries() {
        let throttler = ConnectionThrottler::new(ThrottleConfig {
            window: Duration::from_millis(20),
            stream_create_limit: 4,
            stream_subscribe_limit: 4,
            max_active_subscriptions: 2,
        });

        let _ = throttler
            .check_and_record("anonymous:one", ThrottleEndpoint::StreamCreate)
            .await;
        tokio::time::sleep(Duration::from_millis(45)).await;
        let _ = throttler
            .check_and_record("anonymous:two", ThrottleEndpoint::StreamCreate)
            .await;

        assert_eq!(throttler.actor_state_len().await, 1);
    }

    #[tokio::test]
    async fn stream_create_throttle_resets_after_window_expiry() {
        let throttler = ConnectionThrottler::new(ThrottleConfig {
            window: Duration::from_millis(25),
            stream_create_limit: 1,
            stream_subscribe_limit: 10,
            max_active_subscriptions: 5,
        });

        let first = throttler
            .check_and_record("did:plc:reset", ThrottleEndpoint::StreamCreate)
            .await;
        let second = throttler
            .check_and_record("did:plc:reset", ThrottleEndpoint::StreamCreate)
            .await;
        tokio::time::sleep(Duration::from_millis(35)).await;
        let third = throttler
            .check_and_record("did:plc:reset", ThrottleEndpoint::StreamCreate)
            .await;

        assert!(first.allowed);
        assert!(!second.allowed);
        assert!(third.allowed);
        assert_eq!(third.attempts_in_window, 1);
    }
    #[test]
    fn stream_create_upgraded_response_is_ignored_by_legacy_client_parser() {
        let stream_did = "did:plc:z72i7hdynmk6r22z27h6tvur"
            .parse::<Did>()
            .expect("parse did");

        let bytes = to_vec(&StreamCreateResp {
            stream_did: stream_did.clone(),
            client_stamp: Some(Ulid::new()),
        })
        .expect("serialize upgraded stream create response");

        let parsed: LegacyStreamCreateAckClientView =
            from_slice(&bytes).expect("deserialize into legacy client stream create shape");

        assert_eq!(parsed.stream_did, stream_did);
    }
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamStateEventBatchArgs {
    stream_did: Did,
    events: Vec<EventPayload>,
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
