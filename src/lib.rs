use deadpool_redis::Pool;
use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{future::Future, sync::Arc, time::Duration};

// ── Observability ─────────────────────────────────────────────────────────────

pub const PUBSUB_CHANNEL: &str = "queue:events";
pub const HISTORY_KEY: &str = "queue:history";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEvent {
    pub queue: String,
    pub kind: EventKind,
    pub ts: i64, // unix ms
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    Enqueued,
    Processing,
    Succeeded { latency_ms: u64 },
    Failed { error: String },
    DeserFailed,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Fire-and-forget: publish a QueueEvent to pub/sub and append to history.
/// Gets its own pooled connection; never fails the caller.
async fn publish_event(redis: &Pool, queue: &str, kind: EventKind) {
    let event = QueueEvent {
        queue: queue.to_string(),
        kind,
        ts: now_ms(),
    };
    let json = match serde_json::to_string(&event) {
        Ok(j) => j,
        Err(_) => return,
    };
    let mut conn = match redis.get().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let _: Result<i64, _> = conn.publish(PUBSUB_CHANNEL, &json).await;
    let _: Result<i64, _> = conn.lpush(HISTORY_KEY, &json).await;
    let _: Result<i64, _> = conn.ltrim(HISTORY_KEY, 0, 499).await;
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Push a serializable message onto the left end of a Redis list.
/// Also publishes an `Enqueued` event to the monitoring pub/sub channel.
pub async fn enqueue<T: Serialize>(redis: &Pool, queue: &str, msg: &T) -> Result<(), String> {
    let mut conn = redis
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {}", e))?;

    let data =
        serde_json::to_vec(msg).map_err(|e| format!("Failed to serialize message: {}", e))?;

    conn.lpush::<_, _, ()>(queue, data)
        .await
        .map_err(|e| format!("Failed to enqueue message: {}", e))?;

    // Fire-and-forget observability (reuse same connection)
    let event = QueueEvent {
        queue: queue.to_string(),
        kind: EventKind::Enqueued,
        ts: now_ms(),
    };
    if let Ok(json) = serde_json::to_string(&event) {
        let _: Result<i64, _> = conn.publish(PUBSUB_CHANNEL, &json).await;
        let _: Result<i64, _> = conn.lpush(HISTORY_KEY, &json).await;
        let _: Result<i64, _> = conn.ltrim(HISTORY_KEY, 0, 499).await;
    }

    Ok(())
}

/// Pop a message from the right end of a Redis list, blocking up to `timeout_secs`.
/// Returns `None` on timeout.
pub async fn dequeue(
    redis: &Pool,
    queue: &str,
    timeout_secs: f64,
) -> Result<Option<Vec<u8>>, String> {
    let mut conn = redis
        .get()
        .await
        .map_err(|e| format!("Failed to get Redis connection: {}", e))?;

    let result: Option<(String, Vec<u8>)> = conn
        .brpop(queue, timeout_secs)
        .await
        .map_err(|e| format!("Failed to dequeue message: {}", e))?;

    Ok(result.map(|(_, data)| data))
}

// ── Worker ────────────────────────────────────────────────────────────────────

/// A queue consumer that owns the BRPOP loop and JSON deserialization.
///
/// Retry logic stays in your handler closure — this keeps custom consumers
/// (e.g. those that update job status before retrying) from being forced into
/// a generic retry mold.
///
/// Queue lifecycle events (`Enqueued`, `Processing`, `Succeeded`, `Failed`) are
/// automatically published to `queue:events` (Redis pub/sub) and appended to
/// `queue:history` for real-time monitoring.
///
/// # Example
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use redis_queue::Worker;
/// # async fn example(redis: Arc<deadpool_redis::Pool>) {
/// Worker::new(&redis, "my_queue")
///     .spawn(move |msg: MyMessage| async move {
///         process(&msg).await.map_err(|e| e.to_string())
///     });
/// # }
/// # #[derive(serde::Deserialize)] struct MyMessage;
/// # async fn process(_: &MyMessage) -> Result<(), String> { Ok(()) }
/// ```
pub struct Worker {
    redis: Arc<Pool>,
    queue: &'static str,
}

impl Worker {
    pub fn new(redis: &Arc<Pool>, queue: &'static str) -> Self {
        Self {
            redis: redis.clone(),
            queue,
        }
    }

    /// Spawn a background task that continuously dequeues and handles messages.
    ///
    /// - Reconnects automatically on Redis errors (with a 1 s back-off).
    /// - Discards and logs messages that fail JSON deserialization.
    /// - Calls `handler` with the typed message; logs any `Err` returned.
    ///   The handler itself is responsible for any retry/DLQ logic.
    /// - Publishes `Processing`, `Succeeded`, `Failed`, or `DeserFailed` events
    ///   to `queue:events` for real-time monitoring.
    pub fn spawn<T, F, Fut, E>(self, handler: F) -> tokio::task::JoinHandle<()>
    where
        T: DeserializeOwned + Send + 'static,
        E: std::fmt::Display + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let redis = self.redis;
        let queue = self.queue;

        tokio::spawn(async move {
            loop {
                let data = match dequeue(&redis, queue, 5.0).await {
                    Ok(Some(d)) => d,
                    Ok(None) => continue,
                    Err(e) => {
                        eprintln!("[{}] Dequeue error: {}", queue, e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                let message: T = match serde_json::from_slice(&data) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("[{}] Deserialize error: {}", queue, e);
                        publish_event(&redis, queue, EventKind::DeserFailed).await;
                        continue;
                    }
                };

                publish_event(&redis, queue, EventKind::Processing).await;
                let start = std::time::Instant::now();

                match handler(message).await {
                    Ok(_) => {
                        let latency_ms = start.elapsed().as_millis() as u64;
                        publish_event(&redis, queue, EventKind::Succeeded { latency_ms }).await;
                    }
                    Err(e) => {
                        eprintln!("[{}] Handler error: {}", queue, e);
                        publish_event(
                            &redis,
                            queue,
                            EventKind::Failed {
                                error: e.to_string(),
                            },
                        )
                        .await;
                    }
                }
            }
        })
    }
}
