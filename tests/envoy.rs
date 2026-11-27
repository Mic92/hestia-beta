//! A sharded store behind a real Envoy: `hestia serve --sharded`
//! processes as cache nodes, routed by `tests/fixtures/envoy.yaml`.
//! Skipped when `envoy` or the nix tooling is not on PATH.
//!
//! `stress` is not run by default:
//!
//! ```text
//! cargo test --test envoy stress -- --ignored --nocapture
//! ```
//!
//! `HESTIA_STRESS_SECONDS`, `_NODES`, `_BUILDERS`, `_READERS`, `_PATHS`,
//! `_SIZE` and `_SEED` change its scale. What goes wrong is chosen with
//! `HESTIA_STRESS_FAULTS`, a list out of `nodes` (killed, stopped,
//! started, taken out of Envoy's list), `pause` (SIGSTOP) and `bucket`
//! (a node's connection to the bucket stalls or is cut); the default is
//! all three, and an empty list leaves the nodes alone. `_RTT` is the
//! bucket's latency in ms, `_STRAY` the share of drains (in percent)
//! sent to a node the proxy would not have picked, `_REPEAT` the share
//! that push paths another drain has or had, `_GC` runs a garbage
//! collection every so many seconds, and `_CACHE` is the size of each
//! node's pack cache. Timings mean something with
//! `--release` only.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hestia::shard::Shard;
use hestia::store::Snapshot;
use support::cluster::{
    Cluster, Link, add_fixtures, add_packages, builder, fixture_data, package_files,
};
use support::common::{TEST_ROOT_KEY, assert_all_chunks_locatable, path_hash_of, to_path_set};
use support::store::ScratchStore;

const SIZE: usize = 4096;

fn shard_of(path: &Path) -> Shard {
    Shard::of(&path_hash_of(path))
}

fn narinfo_url(base: &str, path: &Path) -> String {
    format!("{base}/{}.narinfo", path_hash_of(path))
}

/// The NAR of `path` as `base` serves it, or what went wrong.
async fn fetch_nar(
    http: &reqwest::Client,
    base: &str,
    path: &Path,
) -> Result<bytes::Bytes, String> {
    let narinfo = http
        .get(narinfo_url(base, path))
        .send()
        .await
        .map_err(|err| format!("narinfo: {err}"))?;
    if narinfo.status() != 200 {
        return Err(format!("narinfo: {}", narinfo.status()));
    }
    let text = narinfo
        .text()
        .await
        .map_err(|err| format!("narinfo: {err}"))?;
    let url = text
        .lines()
        .find_map(|line| line.strip_prefix("URL: "))
        .ok_or("narinfo without URL")?;
    let nar = http
        .get(format!("{base}/{url}"))
        .send()
        .await
        .map_err(|err| format!("nar: {err}"))?;
    if nar.status() != 200 {
        return Err(format!("nar: {}", nar.status()));
    }
    let body = nar
        .bytes()
        .await
        .map_err(|err| format!("nar body: {err}"))?;
    zstd::decode_all(&body[..])
        .map(bytes::Bytes::from)
        .map_err(|err| format!("nar does not decode: {err}"))
}

/// Every root of the bucket, loaded without a node.
async fn whole_bucket(cluster: &Cluster) -> Snapshot {
    let trust = hestia::trust::Trust::open;
    let heads = Snapshot::load(cluster.s3.backend(), trust(), &[], None)
        .await
        .unwrap();
    let roots: Vec<String> = heads.view.roots.keys().cloned().collect();
    Snapshot::load(cluster.s3.backend(), trust(), &roots, None)
        .await
        .unwrap()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Every plan, pack, commit, narinfo and NAR of a shard reaches the same
/// node. The nodes do not follow the store here, so a path is known to
/// the node that committed it and to no other: a request sent elsewhere
/// is a miss.
#[tokio::test(flavor = "multi_thread")]
async fn envoy_sends_every_request_of_a_shard_to_one_node() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // The store database exists once something was added.
    let scratch = tempfile::tempdir().unwrap();
    let all = add_fixtures(&store, scratch.path(), 0, 96, SIZE);
    let Some(mut cluster) = Cluster::start(3, None, &store).await else {
        eprintln!("envoy not on PATH, skipping");
        return;
    };
    // One path of every shard first, the rest afterwards.
    let mut seen = BTreeSet::new();
    let (first, second): (Vec<PathBuf>, Vec<PathBuf>) =
        all.iter().cloned().partition(|p| seen.insert(shard_of(p)));
    assert!(first.len() > 16 && second.len() > 16);
    let http = cluster.http.clone();
    let builder = cluster.builder(&store);

    let paths: Vec<&Path> = first.iter().map(|p| p.as_path()).collect();
    let stats = builder
        .run(to_path_set(&paths), Default::default())
        .await
        .unwrap_or_else(|err| panic!("first drain: {err}\n{}", cluster.logs()));
    assert_eq!(stats.pushed, first.len());

    // Asked directly, every node now holds every shard as of this drain.
    for node in &cluster.nodes {
        for path in &first {
            let answer = http.get(narinfo_url(&node.url(), path)).send().await;
            assert_eq!(answer.unwrap().status(), 200);
        }
    }

    // The rest, one shard at a time. A node reloads what it holds when
    // it publishes, so right after the drain of a shard its paths are
    // known to the node that committed them and to no other.
    let mut by_shard: BTreeMap<Shard, Vec<&Path>> = BTreeMap::new();
    for path in &second {
        by_shard.entry(shard_of(path)).or_default().push(path);
    }
    let mut owners = BTreeSet::new();
    for (shard, paths) in &by_shard {
        let stats = builder
            .run(to_path_set(paths), Default::default())
            .await
            .unwrap_or_else(|err| panic!("drain of {shard}: {err}\n{}", cluster.logs()));
        assert_eq!(stats.pushed, paths.len());

        let mut owner = None;
        for path in paths {
            // Through Envoy it is found, narinfo and NAR ...
            let seed = all.iter().position(|p| p == path).unwrap() as u64;
            let nar = fetch_nar(&http, &cluster.url, path)
                .await
                .unwrap_or_else(|err| panic!("{}: {err}", path.display()));
            assert!(contains(&nar, &fixture_data(seed, SIZE)));
            // ... and only because Envoy picked the node that committed.
            let mut knowing = Vec::new();
            for (index, node) in cluster.nodes.iter().enumerate() {
                let answer = http.get(narinfo_url(&node.url(), path)).send().await;
                if answer.unwrap().status() == 200 {
                    knowing.push(index);
                }
            }
            assert_eq!(knowing.len(), 1, "{}: {knowing:?}", path.display());
            assert_eq!(*owner.get_or_insert(knowing[0]), knowing[0], "{shard}");
        }
        owners.extend(owner);
    }
    // Each node got some shards.
    assert_eq!(owners.len(), 3, "{owners:?}");

    // A node that stops by itself leaves nothing behind.
    for index in 0..3 {
        let status = cluster.stop(index).await;
        assert!(
            status.success(),
            "node {index}: {status}\n{}",
            cluster.logs()
        );
    }
    let snapshot = whole_bucket(&cluster).await;
    assert_eq!(snapshot.path_hashes().len(), all.len());
    assert_all_chunks_locatable(&snapshot).await;
    assert!(snapshot.roots.iter().all(|r| r.starts_with(TEST_ROOT_KEY)));
}

/// Until `path` is served through the proxy with the right bytes.
async fn eventually_served(cluster: &Cluster, path: &Path, seed: u64, within: Duration) {
    let start = Instant::now();
    loop {
        match fetch_nar(&cluster.http, &cluster.url, path).await {
            Ok(nar) => {
                assert!(contains(&nar, &fixture_data(seed, SIZE)));
                return;
            }
            Err(err) if start.elapsed() > within => {
                panic!("{}: {err}\n{}", path.display(), cluster.logs())
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// Nodes are added, killed and taken out while the store is used: the
/// shards of a node that left are served by the others, and drains go on.
#[tokio::test(flavor = "multi_thread")]
async fn a_shard_moves_when_the_nodes_change() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // The store database exists once something was added.
    let scratch = tempfile::tempdir().unwrap();
    let all = add_fixtures(&store, scratch.path(), 0, 120, SIZE);
    let Some(mut cluster) = Cluster::start(2, Some(1), &store).await else {
        eprintln!("envoy not on PATH, skipping");
        return;
    };
    let builder = cluster.builder(&store);
    let drain = async |paths: &[PathBuf], cluster: &Cluster| {
        let paths: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
        // A drain that met a node on its way out is made again.
        for attempt in 0.. {
            match builder.run(to_path_set(&paths), Default::default()).await {
                Ok(_) => return,
                Err(err) if attempt < 5 => eprintln!("drain again: {err}"),
                Err(err) => panic!("drain: {err}\n{}", cluster.logs()),
            }
        }
    };
    let within = Duration::from_secs(10);

    drain(&all[..30], &cluster).await;

    // A third node joins: what moves to it, it loads when asked.
    let third = cluster.add_node().await;
    cluster.route(vec![0, 1, third]).await;
    for (seed, path) in all[..30].iter().enumerate() {
        eventually_served(&cluster, path, seed as u64, within).await;
    }
    drain(&all[30..60], &cluster).await;

    // A node dies without warning.
    cluster.kill(0);
    drain(&all[60..90], &cluster).await;
    for (seed, path) in all[..90].iter().enumerate() {
        eventually_served(&cluster, path, seed as u64, within).await;
    }

    // It comes back with the same address, and another one is taken out.
    cluster.start_node(0).await;
    cluster.route(vec![0, third]).await;
    assert!(cluster.stop(1).await.success());
    drain(&all[90..], &cluster).await;
    for (seed, path) in all.iter().enumerate() {
        eventually_served(&cluster, path, seed as u64, within).await;
    }
}

/// A node told to stop fails its health check and refuses new drains,
/// which Envoy then sends to the next node: no builder sees the stop.
/// The node exits once Envoy has stopped sending to it.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_node_hands_its_shards_over() {
    const SHARDS: &str = "0123456789abcdfghijklmnpqrsvwxyz";
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // The store database exists once something was added.
    let scratch = tempfile::tempdir().unwrap();
    add_fixtures(&store, scratch.path(), 0, 1, SIZE);
    let Some(mut cluster) = Cluster::start(3, None, &store).await else {
        eprintln!("envoy not on PATH, skipping");
        return;
    };
    let victim = format!("127.0.0.1:{}", cluster.nodes[0].port);
    let (http, url) = (cluster.http.clone(), cluster.url.clone());
    let stop = Arc::new(AtomicBool::new(false));
    // (when, the node that answered, the status) of a plan per shard,
    // over and over.
    let begin = Instant::now();
    let plans = tokio::spawn({
        let stop = stop.clone();
        async move {
            let mut answers = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                for shard in SHARDS.chars() {
                    let answer = http
                        .post(format!("{url}/hestia/plan"))
                        .header(hestia::shard::HEADER, shard.to_string())
                        .body(r#"{"paths":[]}"#)
                        .send()
                        .await
                        .unwrap();
                    let node = answer.headers().get("x-hestia-node");
                    let node = node.map(|n| n.to_str().unwrap().to_owned());
                    answers.push((ms(begin), node, answer.status()));
                }
            }
            answers
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let told = ms(begin);
    let status = cluster.stop(0).await;
    let exited = ms(begin);
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, Ordering::Relaxed);
    let answers = plans.await.unwrap();

    assert!(status.success(), "{status}\n{}", cluster.logs());
    let refused: Vec<_> = answers.iter().filter(|(_, _, s)| *s != 200).collect();
    assert!(refused.is_empty(), "{refused:?}");
    let by_victim = |from: u64, to: u64| {
        let in_time = answers.iter().filter(|(at, ..)| (from..to).contains(at));
        in_time
            .filter(|(_, n, _)| n.as_deref() == Some(&victim))
            .count()
    };
    assert!(by_victim(0, told) > 0, "the node had shards before");
    // The signal takes a moment to arrive.
    let after = by_victim(told + 100, u64::MAX);
    assert_eq!(after, 0, "and answered no plan after");
    // The health check runs every 200 ms, and the node waits for 300 ms
    // without a request.
    assert!(exited - told < 3000, "took {} ms to stop", exited - told);
}

/// Two builders build the same derivation at the same time, and it is
/// not reproducible: the same path is pushed twice, with two NAR
/// hashes. The first push stays what every node serves: nix keeps a
/// narinfo for weeks and fails a build over a NAR that is gone.
#[tokio::test(flavor = "multi_thread")]
async fn of_a_path_built_twice_differently_the_first_push_stays() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let scratch = tempfile::tempdir().unwrap();
    let path = add_fixtures(&store, scratch.path(), 0, 1, SIZE).remove(0);
    let Some(mut cluster) = Cluster::start(2, None, &store).await else {
        eprintln!("envoy not on PATH, skipping");
        return;
    };
    let http = cluster.http.clone();
    let drain = async |node: usize, cluster: &Cluster| {
        let builder = builder(cluster.s3.backend(), &cluster.nodes[node].url(), &store);
        let stats = builder
            .run(to_path_set(&[path.as_path()]), Default::default())
            .await
            .unwrap_or_else(|err| panic!("drain: {err}\n{}", cluster.logs()));
        assert_eq!(stats.pushed, 1);
    };
    let narinfo = async |node: &str| {
        let answer = http.get(narinfo_url(node, &path)).send().await.unwrap();
        assert_eq!(answer.status(), 200);
        answer.text().await.unwrap()
    };
    let nar_of = |narinfo: &str| {
        let url = narinfo.lines().find_map(|l| l.strip_prefix("URL: "));
        url.unwrap().to_owned()
    };

    // Neither node follows the store, and the second holds the shard
    // from before the first one's push, so it does not know of it when
    // it is asked: as if both builders had planned before either
    // committed.
    let early = narinfo_url(&cluster.nodes[1].url(), &path);
    assert_eq!(http.get(early).send().await.unwrap().status(), 404);
    drain(0, &cluster).await;
    let first = narinfo(&cluster.nodes[0].url()).await;
    store.rebuild_differently(&path, "data", &fixture_data(99, SIZE));
    drain(1, &cluster).await;
    // The node sees the first push when it commits, and leaves it.
    let second = narinfo(&cluster.nodes[1].url()).await;
    assert_eq!(nar_of(&first), nar_of(&second));

    // Every node as it sees the bucket now: the first two started
    // again, and a third one.
    let third = cluster.add_node().await;
    for index in [0, 1] {
        assert!(cluster.stop(index).await.success());
        cluster.start_node(index).await;
    }
    for round in 0..2 {
        for node in &cluster.nodes {
            assert_eq!(narinfo(&node.url()).await, first, "round {round}");
            let url = format!("{}/{}", node.url(), nar_of(&first));
            let answer = http.get(&url).send().await.unwrap();
            assert_eq!(answer.status(), 200, "round {round}");
            let nar = zstd::decode_all(&answer.bytes().await.unwrap()[..]).unwrap();
            assert!(contains(&nar, &fixture_data(0, SIZE)));
        }
        for index in [0, 1, third] {
            assert!(cluster.stop(index).await.success());
            cluster.start_node(index).await;
        }
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Clone>(&mut self, of: &[T]) -> T {
        of[self.below(of.len() as u64) as usize].clone()
    }
}

#[derive(Default)]
struct Counters {
    drains: AtomicU64,
    drains_failed: AtomicU64,
    pushed: AtomicU64,
    new_chunks: AtomicU64,
    reads: AtomicU64,
    reads_late: AtomicU64,
    /// Acknowledged paths a garbage collection dropped.
    reads_dropped: AtomicU64,
    fresh_misses: AtomicU64,
    /// The longest a drain, a late read and a stop took, in ms.
    slowest_drain: AtomicU64,
    slowest_read: AtomicU64,
    slowest_stop: AtomicU64,
    /// Why drains failed, by the start of the message.
    failures: Mutex<BTreeMap<String, u32>>,
    /// What happened when, in ms since the start: node events and failed
    /// drains. Printed with `HESTIA_STRESS_TIMELINE=1`.
    timeline: Mutex<Vec<(u64, String)>>,
    violations: Mutex<Vec<String>>,
}

impl Counters {
    fn violation(&self, what: String) {
        self.violations.lock().unwrap().push(what);
    }

    fn note(&self, begin: Instant, what: String) {
        self.timeline.lock().unwrap().push((ms(begin), what));
    }
}

fn ms(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}

/// Whether `nar` is package `seed`.
fn is_package(nar: &[u8], seed: usize, size: usize) -> bool {
    package_files(seed as u64, size)
        .iter()
        .all(|file| contains(nar, file))
}

/// Builders drain and readers fetch through Envoy while things go wrong:
/// nodes are killed, stopped, paused, restarted, added and taken out of
/// the node list, lose the bucket for a while, and get drains of shards
/// the proxy has given to another node. The paths share names and
/// chunks, and some are pushed by several drains at once.
///
/// What has to hold:
///
/// * a path whose drain was acknowledged is served with the right bytes,
///   at the latest `GRACE` after it was asked for (a node may have to
///   notice that a shard is now its own);
/// * a node told to stop exits cleanly;
/// * at the end the bucket alone has every acknowledged path, with
///   every chunk in a pack.
///
/// A garbage collection drops what no drain named since the one before,
/// so with `HESTIA_STRESS_GC` an acknowledged path may be gone. Then a
/// path the proxy has a narinfo for has to be served with the right
/// bytes, and what the bucket has at the end has to be whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test, run on demand"]
async fn stress() {
    const GRACE: Duration = Duration::from_secs(15);
    /// A paused node and a lost bucket come back after this long.
    const TROUBLE: Duration = Duration::from_secs(3);
    let seconds = env("HESTIA_STRESS_SECONDS", 20);
    let node_count = env("HESTIA_STRESS_NODES", 4) as usize;
    let builders = env("HESTIA_STRESS_BUILDERS", 8);
    let readers = env("HESTIA_STRESS_READERS", 8);
    let size = env("HESTIA_STRESS_SIZE", 2048) as usize;
    let seed = env("HESTIA_STRESS_SEED", 1);
    let total = env("HESTIA_STRESS_PATHS", 3000);
    let rtt = env("HESTIA_STRESS_RTT", 0);
    let stray = env("HESTIA_STRESS_STRAY", 10);
    let repeat = env("HESTIA_STRESS_REPEAT", 20);
    let gc_every = env("HESTIA_STRESS_GC", 0);
    let faults = std::env::var("HESTIA_STRESS_FAULTS").unwrap_or("nodes,pause,bucket".into());
    let fault = |name: &str| faults.split(',').any(|f| f == name);

    let Some(store) = ScratchStore::create() else {
        return;
    };
    // The store database exists once something was added.
    let scratch = tempfile::tempdir().unwrap();
    let paths = Arc::new(add_packages(&store, scratch.path(), total, size));
    let arguments = match std::env::var("HESTIA_STRESS_CACHE") {
        Ok(size) => vec!["--pack-cache-size".to_owned(), size],
        Err(_) => Vec::new(),
    };
    let started = Cluster::start_with(node_count, Some(1), &store, arguments).await;
    let Some(mut cluster) = started else {
        eprintln!("envoy not on PATH, skipping");
        return;
    };
    cluster.s3.set_rtt(Duration::from_millis(rtt));
    cluster.s3.take_requests();
    let store = Arc::new(store);

    let begin = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicUsize::new(0));
    // Seeds of the paths whose drain was acknowledged.
    let acked: Arc<Mutex<Vec<usize>>> = Arc::default();
    // The nodes a stray drain may be sent to.
    let well: Arc<Mutex<Vec<String>>> = Arc::default();
    let counters = Arc::new(Counters::default());
    let url = cluster.url.clone();
    let mut tasks = Vec::new();

    for b in 0..builders {
        let (stop, next, acked, well, paths, store, counters) = (
            stop.clone(),
            next.clone(),
            acked.clone(),
            well.clone(),
            paths.clone(),
            store.clone(),
            counters.clone(),
        );
        let backend = cluster.s3.backend();
        let http = cluster.http.clone();
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(seed * 1000 + b + 1);
            while !stop.load(Ordering::Relaxed) {
                let want = 1 + rng.below(8) as usize;
                let handed = next.load(Ordering::Relaxed).min(paths.len());
                let batch: BTreeSet<usize> = if handed > 0 && rng.below(100) < repeat {
                    // Paths another drain pushed, or is pushing now.
                    (0..want)
                        .map(|_| rng.below(handed as u64) as usize)
                        .collect()
                } else {
                    let from = next.fetch_add(want, Ordering::Relaxed);
                    if from >= paths.len() {
                        return;
                    }
                    (from..(from + want).min(paths.len())).collect()
                };
                let set = to_path_set(
                    &batch
                        .iter()
                        .map(|&i| paths[i].as_path())
                        .collect::<Vec<_>>(),
                );
                let started = Instant::now();
                let through_proxy = loop {
                    let direct = match rng.below(100) < stray {
                        true => {
                            let well = well.lock().unwrap();
                            (!well.is_empty()).then(|| rng.pick(&well[..]))
                        }
                        false => None,
                    };
                    let target = direct.as_deref().unwrap_or(&url);
                    let context = builder(backend.clone(), target, &store);
                    match context.run(set.clone(), Default::default()).await {
                        Ok(stats) => {
                            let add = |c: &AtomicU64, n| c.fetch_add(n as u64, Ordering::Relaxed);
                            add(&counters.pushed, stats.pushed);
                            add(&counters.new_chunks, stats.new_chunks);
                            break direct.is_none();
                        }
                        Err(err) => {
                            counters.drains_failed.fetch_add(1, Ordering::Relaxed);
                            let why: String = err.to_string().chars().take(220).collect();
                            let short = why.rsplit(": ").next().unwrap_or_default();
                            counters.note(begin, format!("  builder {b}: {short}"));
                            *counters.failures.lock().unwrap().entry(why).or_default() += 1;
                            if started.elapsed() > GRACE * 2 {
                                counters.violation(format!("a drain kept failing: {err}"));
                                return;
                            }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                };
                counters.drains.fetch_add(1, Ordering::Relaxed);
                counters
                    .slowest_drain
                    .fetch_max(ms(started), Ordering::Relaxed);
                // Whether the builder finds its own path right away.
                if through_proxy {
                    let first = &paths[*batch.first().unwrap()];
                    let own = http.get(narinfo_url(&url, first)).send().await;
                    if !own.is_ok_and(|a| a.status() == 200) {
                        counters.fresh_misses.fetch_add(1, Ordering::Relaxed);
                    }
                }
                acked.lock().unwrap().extend(batch);
            }
        }));
    }

    for r in 0..readers {
        let (stop, acked, paths, counters) =
            (stop.clone(), acked.clone(), paths.clone(), counters.clone());
        let http = cluster.http.clone();
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(seed * 7919 + r + 1);
            while !stop.load(Ordering::Relaxed) {
                let pick = {
                    let acked = acked.lock().unwrap();
                    (!acked.is_empty()).then(|| rng.pick(&acked[..]))
                };
                let Some(index) = pick else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };
                let started = Instant::now();
                let mut late = false;
                loop {
                    match fetch_nar(&http, &url, &paths[index]).await {
                        Ok(nar) => {
                            if !is_package(&nar, index, size) {
                                counters
                                    .violation(format!("{}: wrong bytes", paths[index].display()));
                            }
                            break;
                        }
                        Err(err) if gc_every > 0 && err == "narinfo: 404 Not Found" => {
                            counters.reads_dropped.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(err) if started.elapsed() > GRACE => {
                            counters.violation(format!(
                                "{}: not served for {GRACE:?}: {err}",
                                paths[index].display()
                            ));
                            break;
                        }
                        Err(_) => {
                            late = true;
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
                counters.reads.fetch_add(1, Ordering::Relaxed);
                if late {
                    counters.reads_late.fetch_add(1, Ordering::Relaxed);
                    counters
                        .slowest_read
                        .fetch_max(ms(started), Ordering::Relaxed);
                }
            }
        }));
    }

    // Garbage collections, some of them given up half way as if the
    // collector had died.
    let collections = Arc::new(Mutex::new(Vec::new()));
    if gc_every > 0 {
        cluster.s3.follow_the_clock();
        let (stop, counters, collections) = (stop.clone(), counters.clone(), collections.clone());
        let gc = hestia::gc::Gc {
            backend: cluster.s3.backend(),
            trust: hestia::trust::Trust::open(),
            policy: hestia::gc::GcPolicy {
                // Longer than a drain takes, shorter than the test.
                min_age: 8,
                min_interval: 2,
                ..Default::default()
            },
            dry_run: false,
        };
        tasks.push(tokio::spawn(async move {
            let mut rng = Rng(seed * 31 + 7);
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(gc_every)).await;
                let limit = match rng.below(3) {
                    0 => Duration::from_millis(rng.below(300)),
                    _ => Duration::from_secs(60),
                };
                let now = hestia::pipeline::now_unix();
                let what = match tokio::time::timeout(limit, gc.run(now)).await {
                    Ok(Ok(stats)) => format!("{stats:?}"),
                    Ok(Err(err)) => format!("failed: {err}"),
                    Err(_) => format!("given up after {limit:?}"),
                };
                counters.note(begin, format!("gc: {what}"));
                collections.lock().unwrap().push(what);
            }
        }));
    }

    // The trouble.
    let mut rng = Rng(seed);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut events: BTreeMap<&str, u32> = BTreeMap::new();
    let mut troubled: BTreeMap<usize, Instant> = BTreeMap::new();
    let nodes_well = |c: &Cluster| -> Vec<String> {
        let well = c.nodes.iter().filter(|n| n.well());
        well.map(|n| n.url()).collect()
    };
    *well.lock().unwrap() = nodes_well(&cluster);
    while Instant::now() < deadline && next.load(Ordering::Relaxed) < paths.len() {
        tokio::time::sleep(Duration::from_millis(200 + rng.below(1200))).await;
        if faults.is_empty() {
            continue;
        }
        let nodes = |c: &Cluster, f: &dyn Fn(&support::cluster::Node) -> bool| -> Vec<usize> {
            (0..c.nodes.len()).filter(|&n| f(&c.nodes[n])).collect()
        };
        let running = nodes(&cluster, &|n| n.running());
        let healthy = nodes(&cluster, &|n| n.well());
        let down = nodes(&cluster, &|n| !n.running());
        // Whether a node can be taken from those Envoy sends to.
        let spare = cluster
            .routed
            .iter()
            .filter(|&&n| cluster.nodes[n].well())
            .count()
            > 1;
        let over = troubled
            .iter()
            .find(|(_, since)| since.elapsed() > TROUBLE)
            .map(|(&n, _)| n);
        let event = match rng.below(9) {
            _ if over.is_some() => {
                let node = over.unwrap();
                troubled.remove(&node);
                if cluster.nodes[node].running() {
                    cluster.pause(node, false);
                }
                cluster.set_link(node, Link::Open);
                "mend"
            }
            0 if spare && fault("nodes") => {
                let victim = rng.pick(&running);
                cluster.kill(victim);
                "kill"
            }
            1 if spare && fault("nodes") => {
                let victim = rng.pick(&healthy);
                let started = Instant::now();
                let status = cluster.stop(victim).await;
                counters
                    .slowest_stop
                    .fetch_max(ms(started), Ordering::Relaxed);
                if !status.success() {
                    counters.violation(format!(
                        "node {victim} stopped with {status}:\n{}",
                        cluster.nodes[victim].log()
                    ));
                }
                "stop"
            }
            2 | 3 if !down.is_empty() => {
                cluster.start_node(rng.pick(&down)).await;
                "start"
            }
            4 if cluster.routed.len() > 1 && spare && fault("nodes") => {
                // Out of the node list, but still running.
                let mut routed = cluster.routed.clone();
                routed.remove(rng.below(routed.len() as u64) as usize);
                if routed.iter().any(|&n| cluster.nodes[n].well()) {
                    cluster.route(routed).await;
                    "unroute"
                } else {
                    "nothing"
                }
            }
            5 if fault("nodes") => {
                if cluster.nodes.len() < node_count + 2 {
                    cluster.add_node().await;
                }
                let all: Vec<usize> = (0..cluster.nodes.len()).collect();
                cluster.route(all).await;
                "route all"
            }
            6 if spare && fault("pause") => {
                let victim = rng.pick(&healthy);
                cluster.pause(victim, true);
                troubled.insert(victim, Instant::now());
                "pause"
            }
            7 | 8 if spare && fault("bucket") => {
                let victim = rng.pick(&healthy);
                let link = rng.pick(&[Link::Stalled, Link::Cut]);
                cluster.set_link(victim, link);
                troubled.insert(victim, Instant::now());
                match link {
                    Link::Stalled => "bucket stalls",
                    _ => "bucket cut",
                }
            }
            _ => "nothing",
        };
        *well.lock().unwrap() = nodes_well(&cluster);
        let now: Vec<String> = (0..cluster.nodes.len())
            .map(|n| {
                let node = &cluster.nodes[n];
                let mut state = match (node.running(), node.paused) {
                    (false, _) => "down".to_owned(),
                    (true, true) => "paused".to_owned(),
                    (true, false) => "up".to_owned(),
                };
                if node.link() != Link::Open {
                    state.push_str(&format!(" {:?}", node.link()));
                }
                if !cluster.routed.contains(&n) {
                    state.push_str(" unrouted");
                }
                format!("{n} {state}")
            })
            .collect();
        counters.note(begin, format!("{event}: {}", now.join(", ")));
        *events.entry(event).or_default() += 1;
    }
    // What the bucket was asked, by method and kind of object.
    let mut asked: BTreeMap<String, u32> = BTreeMap::new();
    for request in cluster.s3.take_requests() {
        let (method, path) = request.split_once(' ').unwrap_or_default();
        let name = path.rsplit('/').next().unwrap_or_default();
        let kind: String = name.chars().take_while(|c| c.is_alphabetic()).collect();
        let kind = if name.is_empty() { "list" } else { &kind };
        *asked.entry(format!("{method} {kind}")).or_default() += 1;
    }

    // Everything mended and back up before the builders are told to end:
    // a drain at a node without its bucket does not end.
    for index in 0..cluster.nodes.len() {
        cluster.set_link(index, Link::Open);
        match cluster.nodes[index].running() {
            true => cluster.pause(index, false),
            false => cluster.start_node(index).await,
        }
    }
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.unwrap();
    }
    let all: Vec<usize> = (0..cluster.nodes.len()).collect();
    cluster.route(all).await;
    if gc_every > 0 {
        // Until every node has seen the last collection.
        tokio::time::sleep(Duration::from_millis(2500)).await;
    }

    // Every acknowledged path through the proxy.
    let acked: BTreeSet<usize> = acked.lock().unwrap().iter().copied().collect();
    let mut not_served = BTreeSet::new();
    for &index in &acked {
        let started = Instant::now();
        loop {
            match fetch_nar(&cluster.http, &url, &paths[index]).await {
                Ok(nar) if is_package(&nar, index, size) => break,
                Ok(_) => {
                    let path = paths[index].display();
                    counters.violation(format!("{path}: wrong bytes at the end"));
                    break;
                }
                Err(err) if gc_every > 0 && err == "narinfo: 404 Not Found" => {
                    not_served.insert(index);
                    break;
                }
                Err(err) if started.elapsed() > GRACE => {
                    let path = paths[index].display();
                    counters.violation(format!("{path}: gone at the end: {err}"));
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }

    // Nodes stop cleanly, and the bucket alone has it all.
    for index in 0..cluster.nodes.len() {
        let status = cluster.stop(index).await;
        if !status.success() {
            counters.violation(format!("node {index} stopped with {status} at the end"));
        }
    }
    let snapshot = whole_bucket(&cluster).await;
    let stored: BTreeSet<_> = snapshot.path_hashes().into_iter().collect();
    let mut dropped = 0;
    for &index in &acked {
        let hash = path_hash_of(&paths[index]);
        let path = paths[index].display();
        match (stored.contains(&hash), not_served.contains(&index)) {
            (true, true) => counters.violation(format!("{path}: stored, and not served")),
            (false, false) if gc_every > 0 => {
                counters.violation(format!("{path}: served, and not stored"))
            }
            (false, _) if gc_every > 0 => dropped += 1,
            (false, _) => counters.violation(format!("{path}: not in the bucket")),
            (true, false) => {}
        }
    }
    let keys = cluster.s3.keys();
    for hash in &stored {
        match snapshot.resolve(hash).await {
            Ok(Some(resolved)) => {
                for pack in resolved.map.packs.keys() {
                    let key = hestia::chunker::pack_cache_key(pack);
                    if !keys.iter().any(|k| k.ends_with(&key)) {
                        counters.violation(format!("{hash} needs {key}, which is gone"));
                    }
                }
            }
            Ok(None) => counters.violation(format!("{hash}: listed, and does not resolve")),
            Err(err) => counters.violation(format!("{hash}: {err}")),
        }
    }

    let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
    eprintln!(
        "stress: {} paths acknowledged in {} drains ({} attempts failed), \
         {} paths and {} chunks pushed\n\
         {} reads ({} answered late), {} builders missed their own path right after the drain\n\
         slowest: drain {} ms, late read {} ms, stop {} ms\n\
         events: {events:?}\nbucket requests: {asked:?}\nfailed drains: {:#?}",
        acked.len(),
        load(&counters.drains),
        load(&counters.drains_failed),
        load(&counters.pushed),
        load(&counters.new_chunks),
        load(&counters.reads),
        load(&counters.reads_late),
        load(&counters.fresh_misses),
        load(&counters.slowest_drain),
        load(&counters.slowest_read),
        load(&counters.slowest_stop),
        counters.failures.lock().unwrap(),
    );
    if gc_every > 0 {
        eprintln!(
            "gc: {} runs, {dropped} acknowledged paths dropped, {} reads found theirs dropped",
            collections.lock().unwrap().len(),
            load(&counters.reads_dropped),
        );
    }
    if env("HESTIA_STRESS_TIMELINE", 0) != 0 {
        let mut timeline = counters.timeline.lock().unwrap().clone();
        timeline.sort();
        for (at, what) in timeline {
            eprintln!("{at:>7} ms  {what}");
        }
    }
    // What the nodes complained about, most frequent first.
    let mut complaints: BTreeMap<String, u32> = BTreeMap::new();
    for line in cluster.logs().lines() {
        let said = [
            "final drain:",
            "hook socket",
            "pack cache served",
            "shutdown requested",
            "--- node",
        ];
        if !said.iter().any(|s| line.contains(s)) {
            let digits = |c: char| c.is_ascii_digit();
            let shape: String = line.chars().filter(|c| !digits(*c)).take(160).collect();
            *complaints.entry(shape).or_default() += 1;
        }
    }
    let mut complaints: Vec<_> = complaints.into_iter().collect();
    complaints.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (line, n) in complaints.iter().take(12) {
        eprintln!("node log, {n} times: {line}");
    }
    let violations = counters.violations.lock().unwrap();
    if !violations.is_empty() {
        let logs = cluster.logs();
        let tail: String = logs.lines().rev().take(200).collect::<Vec<_>>().join("\n");
        panic!(
            "{} violations, the first of them:\n{}\n\nnode logs (newest first):\n{tail}",
            violations.len(),
            violations
                .iter()
                .take(20)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
}
