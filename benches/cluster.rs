//! One root shared by a build cluster, over the fake bucket with an
//! injected round trip and link speed: what a builder pays to start
//! serving, to answer Nix, to drain, and to follow other writers, as the
//! root grows and as segments pile up between compactions.
//!
//!   cargo bench --bench cluster
//!   HESTIA_BENCH_PATHS=1000000 HESTIA_BENCH_SEGMENTS=50 cargo bench --bench cluster
//!
//! PATHS store paths of SIZE bytes are published as SEGMENTS drains of one
//! pack each. Then, with RTT_MS per request and MBIT shared:
//!
//!   cold load     a daemon starting: heads, then every `.meta`
//!   narinfo       lookups in the loaded snapshot, hits and misses
//!   nar url       finding the path a `nar/<hash>.nar.zst` URL names
//!   fetch         FETCH paths through the substituter, 25 at a time:
//!                 neighbours in one pack, then paths from all over
//!   drain         NEW paths built, CLOSURE stored paths used
//!   follow        another daemon picking up that drain
//!   compact       folding the pending segments into one
//!   follow        another daemon picking up the compaction

#[path = "../tests/support/mod.rs"]
mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream};
use hestia::manifest::PathHash;
use hestia::pipeline::AccessLog;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};
use hestia::trust::Trust;
use support::fake_s3::FakeS3;
use support::net::Net;
use support::sim::{SimCache, SimPath};

const ROOT: &str = "main";
const PARALLEL: usize = 25;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn path(i: u64, size: usize) -> SimPath {
    SimPath::new(&format!("p{i}"), i, size)
}

fn hash_of(i: u64) -> PathHash {
    path(i, 0).path_hash()
}

/// Deterministic spread of `n` indexes below `max`.
fn sample(n: u64, max: u64) -> Vec<u64> {
    (0..n)
        .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) % max)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn row(name: &str, started: Instant, net: &Net) {
    let wall = started.elapsed();
    let requests = net.take().len();
    let (up, down) = net.take_bytes();
    println!(
        "{name:<34} {:>9.1} ms {requests:>6} requests {:>8.2} MB down {:>8.2} MB up",
        wall.as_secs_f64() * 1e3,
        down as f64 / 1e6,
        up as f64 / 1e6
    );
}

/// A fresh daemon serving `wanted` to Nix: every narinfo, then every NAR.
async fn fetch_paths(
    name: &str,
    snapshot: &Arc<Snapshot>,
    sim: &SimCache,
    net: &Net,
    wanted: &[u64],
) {
    let store = ManifestStore::new();
    store.set_snapshot(snapshot.clone());
    let router = Substituter::new(
        Default::default(),
        store,
        AccessLog::new(),
        sim.backend.clone(),
    )
    .into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let http = reqwest::Client::new();
    let get = |url: String| {
        let http = &http;
        async move {
            let r = http.get(&url).send().await.unwrap();
            assert_eq!(r.status(), 200, "{url}");
            r.bytes().await.unwrap()
        }
    };
    let t = Instant::now();
    let narinfos: Vec<String> = stream::iter(wanted)
        .map(|i| get(format!("{base}/{}.narinfo", hash_of(*i))))
        .buffered(PARALLEL)
        .map(|b| String::from_utf8(b.to_vec()).unwrap())
        .collect()
        .await;
    stream::iter(narinfos)
        .map(|n| {
            let url = n.lines().find_map(|l| l.strip_prefix("URL: ")).unwrap();
            get(format!("{base}/{url}"))
        })
        .buffer_unordered(PARALLEL)
        .for_each(|_| async {})
        .await;
    row(&format!("{name} ({} paths)", wanted.len()), t, net);
    server.abort();
}

#[tokio::main]
async fn main() {
    let paths = env("HESTIA_BENCH_PATHS", 100_000);
    let segments = env("HESTIA_BENCH_SEGMENTS", 20);
    let size = env("HESTIA_BENCH_SIZE", 2048) as usize;
    let rtt = env("HESTIA_BENCH_RTT_MS", 30);
    let mbit = env("HESTIA_BENCH_MBIT", 1000);
    let fetch = env("HESTIA_BENCH_FETCH", 200);
    let closure = env("HESTIA_BENCH_CLOSURE", 2000);
    let new = env("HESTIA_BENCH_NEW", 20);

    let fake = FakeS3::start().await;
    let net = fake.net.clone();
    // Heads are dated by this clock; the setup's lie in the past so the
    // compaction at the end is not held off by them.
    let clock = Arc::new(AtomicU64::new(1_700_000_000));
    let sim = {
        let clock = clock.clone();
        SimCache::with(
            fake.backend(),
            Arc::new(move || clock.fetch_add(1, Ordering::Relaxed)),
        )
    };
    let roots = [ROOT.to_owned()];
    let load = |previous: Option<Arc<Snapshot>>| {
        let backend = sim.backend.clone();
        let roots = &roots;
        async move {
            Arc::new(
                Snapshot::load(backend, Trust::open(), roots, previous.as_deref())
                    .await
                    .unwrap(),
            )
        }
    };

    let started = Instant::now();
    let per = paths.div_ceil(segments);
    for s in 0..segments {
        let batch: Vec<SimPath> = (s * per..((s + 1) * per).min(paths))
            .map(|i| path(i, size))
            .collect();
        let refs: Vec<&SimPath> = batch.iter().collect();
        sim.push(ROOT, &refs, &refs).await;
    }
    println!(
        "{paths} paths of {size} B in {segments} segments ({:.0} s to set up); {rtt} ms a request, {mbit} Mbit/s\n",
        started.elapsed().as_secs_f64()
    );
    net.take();
    net.take_bytes();
    net.set_rtt(Duration::from_millis(rtt));
    net.set_bandwidth(mbit * 1_000_000 / 8);

    let t = Instant::now();
    let cold = load(None).await;
    assert_eq!(cold.path_count() as u64, paths);
    row("cold load", t, &net);

    let hits = sample(20_000, paths);
    let t = Instant::now();
    for &i in &hits {
        assert!(cold.lookup(&hash_of(i)).is_some());
    }
    let hit = t.elapsed().as_secs_f64() * 1e6 / hits.len() as f64;
    let t = Instant::now();
    for &i in &hits {
        assert!(cold.lookup(&hash_of(paths + i)).is_none());
    }
    let miss = t.elapsed().as_secs_f64() * 1e6 / hits.len() as f64;
    // Both include deriving the path hash, timed alone here.
    let t = Instant::now();
    for &i in &hits {
        std::hint::black_box(hash_of(i));
    }
    let derive = t.elapsed().as_secs_f64() * 1e6 / hits.len() as f64;
    println!(
        "{:<34} {:>9.2} µs a hit, {:.2} µs a miss",
        "narinfo",
        hit - derive,
        miss - derive
    );

    let urls: Vec<_> = sample(50, paths)
        .into_iter()
        .map(|i| cold.lookup(&hash_of(i)).unwrap().nar_hash)
        .collect();
    let t = Instant::now();
    assert!(cold.by_nar_hash(&urls[0]).is_some());
    let first_url = t.elapsed();
    let t = Instant::now();
    for h in &urls {
        assert!(cold.by_nar_hash(h).is_some());
    }
    println!(
        "{:<34} {:>9.2} µs each, {:.1} ms the first",
        "nar url",
        t.elapsed().as_secs_f64() * 1e6 / urls.len() as f64,
        first_url.as_secs_f64() * 1e3
    );

    // One build's outputs, as they lie in a pack, then paths from all over.
    let first = paths / 2;
    let adjacent: Vec<u64> = (first..(first + fetch).min(paths)).collect();
    fetch_paths("fetch, adjacent", &cold, &sim, &net, &adjacent).await;
    fetch_paths("fetch, scattered", &cold, &sim, &net, &sample(fetch, paths)).await;

    // A second daemon that loaded before the drain below.
    let other = load(None).await;
    net.take();
    net.take_bytes();

    let built: Vec<SimPath> = (paths..paths + new).map(|i| path(i, size)).collect();
    let built_refs: Vec<&SimPath> = built.iter().collect();
    let used: Vec<PathHash> = sample(closure, paths).into_iter().map(hash_of).collect();
    clock.store(hestia::pipeline::now_unix(), Ordering::Relaxed);
    let t = Instant::now();
    let names: BTreeSet<&str> = built.iter().map(|p| p.name.as_str()).collect();
    cold.load_indexes_for(&names).await.unwrap();
    let sealed = sim
        .drain(&cold, ROOT, &built_refs, &used)
        .await
        .expect("published");
    let drained = Arc::new(cold.refresh_with(&sealed).await.unwrap());
    row(&format!("drain {new} new, {} used", used.len()), t, &net);

    let t = Instant::now();
    let other = load(Some(other)).await;
    assert_eq!(other.path_count() as u64, paths + new + used.len() as u64);
    row("follow the drain", t, &net);

    let t = Instant::now();
    let now = hestia::pipeline::now_unix() + 3600;
    let head = drained.maybe_compact(ROOT, now, 0.0).await.unwrap();
    assert!(head.is_some(), "compaction did not run");
    row("compact", t, &net);

    let t = Instant::now();
    let other = load(Some(other)).await;
    assert_eq!(other.path_count() as u64, paths + new);
    row("follow the compaction", t, &net);
}
