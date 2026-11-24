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
//!   builders      BUILDERS machines fetching FETCH paths each at once, half
//!                 of them common: a daemon each, then one shared daemon
//!   drain         NEW paths built, CLOSURE stored paths used
//!   follow        another daemon picking up that drain
//!   compact       folding the pending segments into one
//!   follow        another daemon picking up the compaction
//!   compact       the same again after four more drains
//!   follow        another daemon picking that up
//!
//! With HISTORY set, that many small drains follow, a compaction for every
//! four and no GC, and a drain and a refresh are measured again.

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

/// A daemon serving `snapshot` to Nix. Returns its URL.
async fn serve(snapshot: &Arc<Snapshot>, sim: &SimCache) -> (String, tokio::task::JoinHandle<()>) {
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
    (base, server)
}

/// What Nix asks a substituter for `wanted`: every narinfo, then every NAR.
async fn substitute(base: &str, wanted: &[u64]) {
    let http = reqwest::Client::new();
    let get = |url: String| {
        let http = &http;
        async move {
            let r = http.get(&url).send().await.unwrap();
            assert_eq!(r.status(), 200, "{url}");
            r.bytes().await.unwrap()
        }
    };
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
}

/// A fresh daemon serving `wanted` to Nix.
async fn fetch_paths(
    name: &str,
    snapshot: &Arc<Snapshot>,
    sim: &SimCache,
    net: &Net,
    wanted: &[u64],
) {
    let (base, server) = serve(snapshot, sim).await;
    let t = Instant::now();
    substitute(&base, wanted).await;
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

    // BUILDERS machines start a build at once. Each needs FETCH paths,
    // half of them the same on every machine. With a daemon each, as
    // today, and with one daemon they all ask, as a cache node would be.
    let builders = env("HESTIA_BENCH_BUILDERS", 20);
    let needs: Vec<Vec<u64>> = (0..builders)
        .map(|b| {
            let common = (paths / 3..).take(fetch as usize / 2);
            let own = sample(fetch / 2, paths)
                .into_iter()
                .map(|i| (i + 7919 * (b + 1)) % paths);
            common.chain(own).collect()
        })
        .collect();
    let t = Instant::now();
    futures_util::future::join_all(needs.iter().map(|wanted| {
        let load = &load;
        let sim = &sim;
        async move {
            let snapshot = load(None).await;
            let (base, server) = serve(&snapshot, sim).await;
            substitute(&base, wanted).await;
            server.abort();
        }
    }))
    .await;
    row(&format!("{builders} builders, a daemon each"), t, &net);
    let node = load(None).await;
    let (base, server) = serve(&node, &sim).await;
    net.take();
    net.take_bytes();
    let t = Instant::now();
    futures_util::future::join_all(needs.iter().map(|wanted| substitute(&base, wanted))).await;
    row(&format!("{builders} builders, one shared daemon"), t, &net);
    server.abort();

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
    assert_eq!(other.path_count() as u64, paths + new);
    row("follow the drain", t, &net);

    let t = Instant::now();
    let now = hestia::pipeline::now_unix() + 3600;
    let head = drained.maybe_compact(ROOT, now, 0.0).await.unwrap();
    assert!(head.is_some(), "compaction did not run");
    row("compact", t, &net);

    let t = Instant::now();
    let other = load(Some(other)).await;
    row("follow the compaction", t, &net);

    // Steady state: the root is one large segment, and drains keep coming.
    let mut snapshot = load(Some(drained)).await;
    for round in 1..=4 {
        let first = paths + round * new;
        let built: Vec<SimPath> = (first..first + new).map(|i| path(i, size)).collect();
        let built_refs: Vec<&SimPath> = built.iter().collect();
        let sealed = sim
            .drain(&snapshot, ROOT, &built_refs, &used)
            .await
            .expect("published");
        snapshot = Arc::new(snapshot.refresh_with(&sealed).await.unwrap());
    }
    let other = load(Some(other)).await;
    net.take();
    net.take_bytes();
    let t = Instant::now();
    let head = snapshot.maybe_compact(ROOT, now + 3600, 0.0).await.unwrap();
    assert!(head.is_some(), "compaction did not run");
    row("compact, 4 drains later", t, &net);
    let t = Instant::now();
    let other = load(Some(other)).await;
    row("follow that compaction", t, &net);

    // BUILDERS machines finish a build at once. Each publishes its own
    // segment, as today; then each hands what it sealed to one committer
    // that publishes a single segment for all, as a cache node would.
    let snapshot = load(Some(snapshot)).await;
    let other = load(Some(other)).await;
    let outputs = |wave: u64| -> Vec<Vec<SimPath>> {
        (0..builders)
            .map(|b| {
                let first = paths + (5000 + wave * builders + b) * new;
                (first..first + new).map(|i| path(i, size)).collect()
            })
            .collect()
    };
    net.take();
    net.take_bytes();
    let wave = outputs(0);
    let t = Instant::now();
    futures_util::future::join_all(wave.iter().map(|built| {
        let refs: Vec<&SimPath> = built.iter().collect();
        let (sim, snapshot, used) = (&sim, &snapshot, &used);
        async move {
            sim.drain(snapshot, ROOT, &refs, used)
                .await
                .expect("published")
        }
    }))
    .await;
    row(&format!("{builders} builders drain, a head each"), t, &net);
    let t = Instant::now();
    let other = load(Some(other)).await;
    row("follow them", t, &net);

    let mut snapshot = load(Some(snapshot)).await;
    net.take();
    net.take_bytes();
    let wave = outputs(1);
    let t = Instant::now();
    let parts: Vec<_> = futures_util::future::join_all(wave.iter().map(|built| {
        let refs: Vec<&SimPath> = built.iter().collect();
        let (sim, snapshot, used) = (&sim, &snapshot, &used);
        async move { sim.prepare(snapshot, &refs, used).await.expect("sealed") }
    }))
    .await;
    let merged = hestia::store::merge_sealed(&parts, &BTreeSet::new()).unwrap();
    sim.publish(&snapshot, ROOT, &merged).await;
    row(&format!("{builders} builders drain, one head"), t, &net);
    let t = Instant::now();
    let other = load(Some(other)).await;
    row("follow them", t, &net);
    println!("{:<34} {} segments live", "", other.view.roots[ROOT].len());

    // A day in the life of the root: many small drains, a compaction for
    // every four, no GC in between. What do the heads cost by then?
    let history = env("HESTIA_BENCH_HISTORY", 0);
    if history == 0 {
        return;
    }
    net.set_rtt(Duration::ZERO);
    net.set_bandwidth(0);
    let started = Instant::now();
    let mut later = now + 7200;
    for round in 0..history {
        let i = paths + 1000 * new + round;
        let built = [path(i, size)];
        let sealed = sim
            .drain(&snapshot, ROOT, &[&built[0]], &used[..10])
            .await
            .expect("published");
        snapshot = Arc::new(snapshot.refresh_with(&sealed).await.unwrap());
        if round % 4 == 3 {
            later += 120;
            snapshot.maybe_compact(ROOT, later, 0.0).await.unwrap();
            snapshot = load(Some(snapshot)).await;
        }
    }
    println!(
        "\n{history} drains later ({:.0} s to set up), {} segments live",
        started.elapsed().as_secs_f64(),
        snapshot.view.roots[ROOT].len()
    );
    net.take();
    net.take_bytes();
    net.set_rtt(Duration::from_millis(rtt));
    net.set_bandwidth(mbit * 1_000_000 / 8);
    let t = Instant::now();
    let _ = load(Some(other)).await;
    row("follow", t, &net);
    let built = [path(paths + 2000 * new, size)];
    let t = Instant::now();
    let sealed = sim
        .drain(&snapshot, ROOT, &[&built[0]], &used[..10])
        .await
        .expect("published");
    let snapshot = Arc::new(snapshot.refresh_with(&sealed).await.unwrap());
    row("drain 1 new, 10 used", t, &net);
    let t = Instant::now();
    let _ = load(Some(snapshot)).await;
    row("follow, nothing new", t, &net);
}
