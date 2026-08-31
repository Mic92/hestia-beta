//! Serving from the segmented store through the fake GHA backend, with
//! nix as the oracle.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use hestia::manifest::Hash32;
use hestia::pipeline::AccessLog;
use hestia::protocol::DrainStats;
use hestia::store::{Heads, Snapshot};
use hestia::substituter::{ManifestStore, Substituter};

use support::common::{TEST_ROOT_KEY, pipeline_context, to_path_set};
use support::fake_gha::FakeGha;
use support::store::{ScratchStore, assert_trees_equal, nix_copy};

async fn timed<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("test timed out")
}

async fn push(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
    paths: &[&std::path::Path],
) -> DrainStats {
    let ctx = pipeline_context(fake, http, store.database());
    ctx.run(to_path_set(paths), BTreeSet::new())
        .await
        .expect("pipeline run")
}

/// Serves what the drains published.
async fn serve(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
) -> (String, AccessLog, tokio::task::JoinHandle<()>) {
    let backend = fake.backend(http);
    let snapshot = Snapshot::load(backend.clone(), &[TEST_ROOT_KEY.to_string()], None)
        .await
        .unwrap();
    let manifest_store = ManifestStore::new();
    manifest_store.set_snapshot(Arc::new(snapshot));
    let access_log = AccessLog::new();
    let router = Substituter::new(
        store.database().store_dir().clone(),
        manifest_store,
        access_log.clone(),
        backend,
    )
    .into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), access_log, task)
}

#[tokio::test]
async fn narinfo_and_nar_from_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("segserve", 91);
        let (expected_hash, expected_size) = store.nar_hash_oracle(&fixture).expect("nix oracle");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&fixture]).await;
        let (base, access_log, _task) = serve(&fake, &http, &store).await;

        let hash = &fixture.file_name().unwrap().to_str().unwrap()[..32];
        let narinfo = http
            .get(format!("{base}/{hash}.narinfo"))
            .send()
            .await
            .unwrap();
        assert_eq!(narinfo.status(), 200);
        let text = narinfo.text().await.unwrap();
        let url = text.lines().find_map(|l| l.strip_prefix("URL: ")).unwrap();
        assert!(
            text.contains(&format!("NarSize: {expected_size}")),
            "{text}"
        );

        let nar = http.get(format!("{base}/{url}")).send().await.unwrap();
        assert_eq!(nar.status(), 200);
        let body = nar.bytes().await.unwrap();
        assert_eq!(body.len() as u64, expected_size);
        assert_eq!(Hash32::digest(&body), expected_hash);
        assert!(access_log.snapshot().contains(&hash.parse().unwrap()));

        let miss = http
            .get(format!("{base}/00000000000000000000000000000000.narinfo"))
            .send()
            .await
            .unwrap();
        assert_eq!(miss.status(), 404);
    })
    .await;
}

#[tokio::test]
async fn nix_copy_closure_from_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("segcopy");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&top, &dep]).await;
        let (base, _log, _task) = serve(&fake, &http, &store).await;
        let store_url = format!("{base}?store={}", store.store_dir_path().display());

        let destination = store.create_destination();
        let output = nix_copy(&store_url, &destination.uri, &top).await;
        assert!(
            output.status.success(),
            "nix copy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&top, &destination.physical_path(&top));
        assert_trees_equal(&dep, &destination.physical_path(&dep));
    })
    .await;
}

#[tokio::test]
async fn unserved_root_is_invisible() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("segroot", 92);
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&fixture]).await;

        let snapshot = Snapshot::load(fake.backend(&http), &["other-root".to_string()], None)
            .await
            .unwrap();
        assert_eq!(snapshot.path_count(), 0);
        let snapshot = Snapshot::load(fake.backend(&http), &[TEST_ROOT_KEY.to_string()], None)
            .await
            .unwrap();
        assert_eq!(snapshot.path_count(), 1);
        assert!(snapshot.view.roots.contains_key(TEST_ROOT_KEY));
    })
    .await;
}

/// The second drain sees the first one's paths and chunks only through
/// segments (its legacy manifest family is empty) and must not store
/// them again.
#[tokio::test]
async fn drain_dedups_against_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("segdedup");
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let first = push(&fake, &http, &store, &[&dep]).await;
        assert_eq!(first.pushed, 1);

        let mut ctx = pipeline_context(&fake, &http, store.database());
        let publish = ManifestStore::new();
        let snapshot = Snapshot::load(fake.backend(&http), &[TEST_ROOT_KEY.to_string()], None)
            .await
            .unwrap();
        publish.set_snapshot(Arc::new(snapshot));
        ctx.publish = Some(publish.clone());
        let second = ctx
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(second.pushed, 1);
        assert_eq!(second.skipped_existing, 1);

        // Read-your-writes: the drain published its segment into `publish`.
        let served = publish.snapshot().unwrap();
        for p in [&top, &dep] {
            assert!(served.contains(&support::common::path_hash_of(p)));
        }
    })
    .await;
}

/// The tree of the segment holding a path was evicted. Re-claiming it by
/// copy is impossible and it is unservable as stored, so a drain pushing
/// it again stores it anew instead of failing on every retry.
#[tokio::test]
async fn stored_path_with_evicted_tree_is_pushed_again() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let a = store.add_fixture("evicted-tree-a", 1);
        let b = store.add_fixture("evicted-tree-b", 2);
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        assert_eq!(push(&fake, &http, &store, &[&a]).await.pushed, 1);
        let heads = Heads::load(&fake.backend(&http)).await.unwrap();
        let meta = hestia::store::fetch_meta(&fake.backend(&http), &heads.view.heads[0].1.seg)
            .await
            .unwrap();
        fake.evict(&http, &format!("tree-{}", meta.tree)).await;

        let stats = push(&fake, &http, &store, &[&a, &b]).await;
        assert_eq!((stats.pushed, stats.skipped_existing), (2, 0), "{stats:?}");
        let served = support::common::load_snapshot(&fake, &http).await;
        assert!(
            served
                .resolve(&support::common::path_hash_of(&a))
                .await
                .is_ok()
        );
    })
    .await;
}

/// The listing API is down (or rate limited) when the job ends. The final
/// drain claims against the view it already has rather than taking the
/// job's paths down with it.
#[tokio::test]
async fn drain_with_listing_down_claims_against_the_served_view() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let a = store.add_fixture("nolist-a", 1);
        let b = store.add_fixture("nolist-b", 2);
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        assert_eq!(push(&fake, &http, &store, &[&a]).await.pushed, 1);
        let publish = ManifestStore::new();
        publish.set_snapshot(support::common::load_snapshot(&fake, &http).await);

        let mut ctx = pipeline_context(&fake, &http, store.database());
        ctx.backend =
            hestia::backend::Backend::new(fake.twirp(&http), Err("GITHUB_TOKEN"), http.clone());
        ctx.publish = Some(publish.clone());
        let stats = ctx
            .run(to_path_set(&[&a, &b]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!((stats.pushed, stats.skipped_existing), (1, 1), "{stats:?}");
        let served = support::common::load_snapshot(&fake, &http).await;
        for p in [&a, &b] {
            assert!(served.contains(&support::common::path_hash_of(p)));
        }
    })
    .await;
}

/// `serve` loads its view when the job starts; a job may outlive two GC
/// runs. The drain at its end must claim against the store as it is now,
/// not as it was, or the next GC ignores the head as stale and the job's
/// paths are gone.
#[tokio::test]
async fn drain_after_two_gc_runs_is_not_stale() {
    use hestia::gc::{Gc, GcPolicy};
    const HOUR: u64 = 3600;
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("stale");
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let t0 = 1_700_000_000;
        fake.set_clock(t0);
        assert_eq!(push(&fake, &http, &store, &[&dep]).await.pushed, 1);

        // The job starts: serve loads the view.
        let publish = ManifestStore::new();
        publish.set_snapshot(support::common::load_snapshot(&fake, &http).await);
        let gc = Gc {
            backend: fake.backend(&http),
            policy: GcPolicy::default(),
            dry_run: false,
        };
        let gc = |now| {
            fake.set_clock(now);
            gc.run(now)
        };
        gc(t0 + 2 * HOUR).await.unwrap();
        gc(t0 + 4 * HOUR).await.unwrap();

        // The job ends: drain.
        fake.set_clock(t0 + 5 * HOUR);
        let mut ctx = pipeline_context(&fake, &http, store.database());
        ctx.publish = Some(publish.clone());
        let stats = ctx
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!((stats.pushed, stats.skipped_existing), (1, 1));

        let stats = gc(t0 + 7 * HOUR).await.unwrap();
        assert_eq!(stats.roots, 1, "{stats:?}");
        let served = support::common::load_snapshot(&fake, &http).await;
        for p in [&top, &dep] {
            assert!(
                served.contains(&support::common::path_hash_of(p)),
                "{} lost",
                p.display()
            );
        }
    })
    .await;
}
