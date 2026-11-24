//! Injected network for the fake servers: a round-trip time per request,
//! one shared link of a given bandwidth, and request and byte counts.
//! Stands in for `tc netem`, which CI kernels often lack.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::HttpBody;
use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;
use tokio::time::Instant;

#[derive(Default)]
pub struct Net {
    rtt_ms: AtomicU64,
    /// Bytes per second over the link, both directions together. 0: unlimited.
    bandwidth: AtomicU64,
    /// When the link has sent everything queued so far.
    link_free: std::sync::Mutex<Option<Instant>>,
    up: AtomicU64,
    down: AtomicU64,
    log: std::sync::Mutex<Vec<String>>,
}

impl Net {
    pub fn set_rtt(&self, rtt: Duration) {
        self.rtt_ms.store(rtt.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn set_bandwidth(&self, bytes_per_s: u64) {
        self.bandwidth.store(bytes_per_s, Ordering::Relaxed);
    }

    /// `METHOD /path` of every request since the last call.
    pub fn take(&self) -> Vec<String> {
        std::mem::take(&mut self.log.lock().unwrap())
    }

    /// Body bytes `(sent to the server, received from it)` since the last call.
    pub fn take_bytes(&self) -> (u64, u64) {
        (
            self.up.swap(0, Ordering::Relaxed),
            self.down.swap(0, Ordering::Relaxed),
        )
    }

    pub fn layer<S: Clone + Send + Sync + 'static>(
        self: &Arc<Self>,
        router: Router<S>,
    ) -> Router<S> {
        router.layer(from_fn_with_state(self.clone(), delay))
    }

    /// Queue `bytes` on the link, first come first served, and return when
    /// the last of them is through.
    fn transfer(&self, bytes: u64) -> Option<Instant> {
        let bandwidth = self.bandwidth.load(Ordering::Relaxed);
        if bandwidth == 0 || bytes == 0 {
            return None;
        }
        let mut free = self.link_free.lock().unwrap();
        let now = Instant::now();
        let done = free.map_or(now, |f| f.max(now))
            + Duration::from_secs_f64(bytes as f64 / bandwidth as f64);
        *free = Some(done);
        Some(done)
    }
}

fn body_len(headers: &HeaderMap) -> u64 {
    headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok()?.parse().ok())
        .unwrap_or(0)
}

async fn delay(State(net): State<Arc<Net>>, req: Request, next: Next) -> Response {
    net.log
        .lock()
        .unwrap()
        .push(format!("{} {}", req.method(), req.uri().path()));
    let up = body_len(req.headers());
    net.up.fetch_add(up, Ordering::Relaxed);
    let rtt = net.rtt_ms.load(Ordering::Relaxed);
    if rtt > 0 {
        tokio::time::sleep(Duration::from_millis(rtt)).await;
    }
    if let Some(done) = net.transfer(up) {
        tokio::time::sleep_until(done).await;
    }
    let response = next.run(req).await;
    let down = response.body().size_hint().exact().unwrap_or(0);
    net.down.fetch_add(down, Ordering::Relaxed);
    if let Some(done) = net.transfer(down) {
        tokio::time::sleep_until(done).await;
    }
    response
}
