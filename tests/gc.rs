//! End-to-end GC over the segmented store against the fake GHA backend.
//! Paths are fabricated (see `support::sim`), everything else is
//! production code, and the substituter verifies live paths stay readable.

mod support;

use std::future::Future;
use std::time::Duration;

use hestia::gc::{Error as GcError, GcPolicy, SECS_PER_DAY, SECS_PER_HOUR};
use hestia::store::Heads;

use support::fake_gha::FakeGha;
use support::sim::{SimCache, SimPath, one_byte_reads};

const T0: u64 = 1_750_000_000;
const DAY: u64 = SECS_PER_DAY;
const HOUR: u64 = SECS_PER_HOUR;
const ROOT: &str = "main-x86_64-linux";

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), future)
        .await
        .expect("test timed out")
}

async fn setup() -> (FakeGha, SimCache) {
    let fake = FakeGha::start().await;
    fake.set_clock(T0);
    let sim = SimCache::new(&fake, &reqwest::Client::new());
    (fake, sim)
}

async fn stored_pack_bytes(fake: &FakeGha, sim: &SimCache) -> u64 {
    let packs = fake.rest(&sim.http).list_caches("pack-").await.unwrap();
    packs.iter().map(|e| e.size_in_bytes).sum()
}

#[tokio::test]
async fn compacts_each_root_to_one_segment_and_folds_heads() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 2, 200_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push(ROOT, &[&b], &[&a, &b]).await;
        sim.push("other-x86_64-linux", &[&b], &[&b]).await;
        assert_eq!(sim.stored_keys("h-").await.len(), 3);

        fake.set_clock(T0 + 2 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!(
            (stats.epoch, stats.roots, stats.segments_written),
            (1, 2, 1)
        );
        assert_eq!(stats.deleted, 0, "the loaded view survives one epoch");

        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.epoch, 1);
        assert!(heads.view.roots.values().all(|s| s.len() == 1));
        assert!(heads.view.heads.is_empty(), "folded");
        sim.assert_readable(&[&a, &b]).await;
        assert_eq!(sim.stored_keys("seg-").await.len(), 4);

        fake.set_clock(T0 + 4 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 4 * HOUR).await;
        assert_eq!(
            stats.deleted,
            3 + 2 * 2,
            "the folded heads, main's two input segments and their trees"
        );
        assert!(sim.stored_keys("h-").await.is_empty());
        assert_eq!(sim.stored_keys("seg-").await.len(), 2);
        sim.assert_readable(&[&a, &b]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}

#[tokio::test]
async fn refuses_to_run_right_after_the_previous_gc() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        fake.set_clock(T0 + HOUR);
        sim.run_gc(GcPolicy::default(), T0 + HOUR).await;
        let err = sim
            .gc(GcPolicy::default())
            .run(T0 + HOUR + 120)
            .await
            .unwrap_err();
        assert!(matches!(err, GcError::TooSoon(..)), "{err}");
    })
    .await;
}

#[tokio::test]
async fn dry_run_changes_nothing() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 2, 200_000);
        sim.push(ROOT, &[&a, &b], &[&a, &b]).await;
        sim.push(ROOT, &[], &[&a]).await;
        let before = sim.stored_keys("").await;
        fake.set_clock(T0 + 2 * HOUR);
        let mut gc = sim.gc(GcPolicy::default());
        gc.dry_run = true;
        let stats = gc.run(T0 + 2 * HOUR).await.unwrap();
        assert_eq!(stats.segments_written, 1);
        assert_eq!(sim.stored_keys("").await, before);
    })
    .await;
}

/// The newest drains name what the root keeps. A path no drain since the
/// last GC mentioned is dropped, its pack repacked, and the old pack swept
/// by the run after.
#[tokio::test]
async fn dropped_path_gets_repacked_away() {
    timed(async {
        let (fake, sim) = setup().await;
        let keep = SimPath::new("keep", 1, 200_000);
        let drop = SimPath::new("drop", 2, 600_000);
        sim.push(ROOT, &[&keep, &drop], &[&keep, &drop]).await;
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        let both = stored_pack_bytes(&fake, &sim).await;

        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        fake.set_clock(T0 + DAY);
        sim.push(ROOT, &[], &[&keep]).await;
        fake.set_clock(T0 + 2 * DAY);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * DAY).await;
        assert_eq!((stats.paths_dropped, stats.packs_repacked), (0, 1));
        assert_eq!(
            sim.stored_keys("pack-").await.len(),
            2,
            "old pack stays one epoch"
        );
        sim.assert_readable(&[&keep]).await;
        sim.assert_unavailable(&[&drop]).await;

        fake.set_clock(T0 + 3 * DAY);
        sim.run_gc(GcPolicy::default(), T0 + 3 * DAY).await;
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        assert!(stored_pack_bytes(&fake, &sim).await < both / 2);
        sim.assert_readable(&[&keep]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}

#[tokio::test]
async fn stale_root_expires_and_its_storage_is_swept() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 100_000);
        let f = SimPath::new("f", 2, 100_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push("feature-x86_64-linux", &[&f], &[&f]).await;
        let policy = GcPolicy {
            root_ttl: 3 * DAY,
            ..GcPolicy::default()
        };
        sim.run_gc(policy.clone(), T0 + 2 * HOUR).await;
        for day in 1..=5 {
            let now = T0 + day * DAY;
            fake.set_clock(now);
            sim.push(ROOT, &[], &[&a]).await;
            sim.run_gc(policy.clone(), now + HOUR).await;
        }
        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.roots.keys().collect::<Vec<_>>(), [ROOT]);
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        sim.assert_readable(&[&a]).await;
        sim.assert_unavailable(&[&f]).await;
    })
    .await;
}

#[tokio::test]
async fn evicted_pack_drops_its_paths_and_a_repush_restores_them() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 100_000);
        let b = SimPath::new("b", 2, 100_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push(ROOT, &[&b], &[&a, &b]).await;
        let packs: Vec<String> = sim.stored_keys("pack-").await.into_iter().collect();
        assert_eq!(packs.len(), 2);
        // Evict a's pack behind hestia's back.
        let a_pack = {
            let snap = sim.snapshot().await;
            let r = snap.resolve(&a.path_hash()).await.unwrap().unwrap();
            hestia::chunker::pack_cache_key(&r.map.chunks.values().next().unwrap().pack)
        };
        sim.backend.delete(&a_pack).await.unwrap();

        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!((stats.packs_evicted, stats.paths_dropped), (1, 1));
        sim.assert_unavailable(&[&a]).await;
        sim.assert_readable(&[&b]).await;

        fake.set_clock(T0 + 3 * HOUR);
        sim.push(ROOT, &[&a], &[&a, &b]).await;
        sim.assert_readable(&[&a, &b]).await;
    })
    .await;
}

#[tokio::test]
async fn orphan_pack_is_swept_only_once_old_enough() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let orphan = sim.upload_orphan_pack(99);
        let orphan = orphan.await;

        let stats = sim.run_gc(GcPolicy::default(), T0 + 10).await;
        assert_eq!(stats.deleted, 0, "the orphan is younger than min_age");
        assert!(sim.stored_keys("pack-").await.contains(&orphan));

        fake.set_clock(T0 + 3 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 3 * HOUR).await;
        assert_eq!(stats.deleted, 2, "orphan pack and the folded head");
        assert!(!sim.stored_keys("pack-").await.contains(&orphan));
        sim.assert_readable(&[&a]).await;
    })
    .await;
}

#[tokio::test]
async fn idle_live_packs_are_touched() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let pack = sim.stored_keys("pack-").await.into_iter().next().unwrap();
        let now = T0 + 5 * DAY;
        fake.set_clock(now);
        let stats = sim.run_gc(GcPolicy::default(), now).await;
        assert_eq!(stats.touched, 3, "pack, segment, tree");
        assert_eq!(one_byte_reads(&fake, &pack), 1);
        let entry = fake.rest(&sim.http).list_caches(&pack).await.unwrap();
        assert!(entry[0].last_accessed_unix().unwrap() >= now);
    })
    .await;
}

/// A drain that read the view before GC published and publishes after it
/// (base_epoch = epoch-1) is still picked up by the next run.
#[tokio::test]
async fn drain_racing_gc_is_kept() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        let b = SimPath::new("b", 2, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let stale_view = sim.snapshot().await;
        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;

        // Publish b against the pre-GC view.
        fake.set_clock(T0 + 3 * HOUR);
        let mut writer = hestia::segment::SegmentWriter::default();
        let (tree, chunks) = b.chunked();
        let mut builder = hestia::chunker::PackBuilder::new();
        for c in &chunks {
            builder.add(c).unwrap();
        }
        let pack = builder.finish();
        hestia::pipeline::upload_pack(&sim.backend, &pack)
            .await
            .unwrap();
        let mut known = hestia::store::KnownChunks::default();
        known.add(pack.hash, &pack.index());
        let entry = hestia::manifest::PathEntry {
            store_path: b.store_path(),
            nar_hash: hestia::manifest::NarHash::digest(b"unused"),
            nar_size: 1,
            references: vec![],
            ca: None,
            deriver: None,
            tree,
        };
        hestia::store::push_entry(&mut writer, &entry, &known).unwrap();
        stale_view
            .copy_entry(&a.path_hash(), &mut writer)
            .await
            .unwrap();
        let sealed = writer.seal().unwrap();
        hestia::store::publish(&sim.backend, &stale_view.view, ROOT, &sealed, T0 + DAY)
            .await
            .unwrap();

        fake.set_clock(T0 + DAY);
        sim.run_gc(GcPolicy::default(), T0 + DAY).await;
        let snap = sim.snapshot().await;
        assert!(snap.contains(&a.path_hash()) && snap.contains(&b.path_hash()));
        fake.set_clock(T0 + 2 * DAY);
        sim.run_gc(GcPolicy::default(), T0 + 2 * DAY).await;
        sim.assert_no_dangling_references().await;
        sim.assert_readable(&[&a]).await;
    })
    .await;
}

#[tokio::test]
async fn thirty_day_history_converges_to_live_set_storage() {
    timed(async {
        let (fake, sim) = setup().await;
        let policy = GcPolicy::default();
        let base: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("base-{i}"), 1000 + i, 100_000))
            .collect();
        let mut generation = 0u64;
        let mut apps: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("app-gen0-{i}"), 2000 + i, 20_000))
            .collect();
        let mut dailies: Vec<SimPath> = Vec::new();
        let feature: Vec<SimPath> = (0..2)
            .map(|i| SimPath::new(&format!("feature-{i}"), 3000 + i, 20_000))
            .collect();

        for day in 0..30u64 {
            let now = T0 + day * DAY;
            fake.set_clock(now);
            if day > 0 && day % 7 == 0 {
                generation += 1;
                apps = (0..10)
                    .map(|i| {
                        SimPath::new(
                            &format!("app-gen{generation}-{i}"),
                            2000 + generation * 100 + i,
                            20_000,
                        )
                    })
                    .collect();
            }
            for i in 0..2u64 {
                dailies.push(SimPath::new(
                    &format!("daily-{day}-{i}"),
                    4000 + day * 10 + i,
                    10_000,
                ));
            }
            while dailies.len() > 6 {
                dailies.remove(0);
            }
            let closure: Vec<SimPath> = base
                .iter()
                .chain(apps.iter())
                .chain(dailies.iter())
                .cloned()
                .collect();
            let refs: Vec<&SimPath> = closure.iter().collect();
            sim.push(ROOT, &refs, &refs).await;
            if day == 3 {
                let refs: Vec<&SimPath> = feature.iter().collect();
                sim.push("feature-x86_64-linux", &refs, &refs).await;
            }

            fake.set_clock(now + 2 * HOUR);
            sim.run_gc(policy.clone(), now + 2 * HOUR).await;
            sim.assert_no_dangling_references().await;
        }

        let closure: Vec<&SimPath> = base
            .iter()
            .chain(apps.iter())
            .chain(dailies.iter())
            .collect();
        sim.assert_readable(&closure).await;
        sim.assert_unavailable(&feature.iter().collect::<Vec<_>>())
            .await;
        let live = sim.live_chunk_bytes().await;
        let stored = stored_pack_bytes(&fake, &sim).await;
        assert!(
            stored <= live * 2,
            "storage {stored} should converge towards the live set {live}"
        );
        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.roots.len(), 1);
        assert_eq!(
            sim.stored_keys("g-").await.len(),
            2,
            "this run's and the one before"
        );
    })
    .await;
}

/// A drain that builds nothing re-claims the GC base
/// byte for byte. Several of those plus one job with a different claim
/// must leave the root the union of all claims, and repeated GC over
/// nothing but re-claims must never empty the root.
#[tokio::test]
async fn reclaiming_drains_never_shrink_the_root() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 2, 200_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        fake.set_clock(T0 + 2 * HOUR);
        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;

        // Epoch 1: base G = {a}. Four cached CI runs re-claim exactly G,
        // one other workflow pushes b only.
        for _ in 0..4 {
            sim.push(ROOT, &[], &[&a]).await;
        }
        sim.push(ROOT, &[&b], &[&b]).await;
        let view = sim.snapshot().await.view;
        assert_eq!(view.heads.len(), 5);
        sim.assert_readable(&[&a, &b]).await;

        fake.set_clock(T0 + 4 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 4 * HOUR).await;
        assert_eq!((stats.roots, stats.paths_dropped), (1, 0), "{stats:?}");
        sim.assert_readable(&[&a, &b]).await;

        // Epoch 2..4: only re-claims of the whole base.
        for i in 0..3u64 {
            for _ in 0..4 {
                sim.push(ROOT, &[], &[&a, &b]).await;
            }
            let t = T0 + (6 + 2 * i) * HOUR;
            fake.set_clock(t);
            let stats = sim.run_gc(GcPolicy::default(), t).await;
            assert_eq!(stats.roots, 1, "epoch {} {stats:?}", stats.epoch);
            sim.assert_readable(&[&a, &b]).await;
        }
        assert_eq!(
            sim.stored_keys("h-").await.len(),
            4,
            "the last epoch's, one more run"
        );
        sim.assert_no_dangling_references().await;
    })
    .await;
}

/// A pull-request job writes into its own cache scope. GC runs on the
/// default branch and must neither count that scope's heads (it cannot
/// read their segments) nor delete its objects as orphans: listing and
/// deleting are confined to GC's own ref.
#[tokio::test]
async fn gc_leaves_other_scopes_alone() {
    timed(async {
        const PR: &str = "refs/pull/7/merge";
        let (fake, main) = setup().await;
        let pr = SimCache::on_ref(&fake, &main.http, PR);
        let a = SimPath::new("a", 1, 80_000);
        let b = SimPath::new("b", 2, 80_000);

        fake.set_clock(T0);
        main.push(ROOT, &[&a], &[&a]).await;
        pr.push("pr-7-x86_64-linux", &[&b], &[&a, &b]).await;
        let before = fake.keys_in(PR);
        assert!(before.iter().any(|k| k.starts_with("h-")), "{before:?}");

        fake.set_clock(T0 + 2 * HOUR);
        let stats = main.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!(stats.roots, 1, "{stats:?}");
        assert_eq!(fake.keys_in(PR), before, "PR scope untouched");
        pr.assert_readable(&[&a, &b]).await;
    })
    .await;
}

/// A claim whose tree cannot be fetched is not merged and not dropped:
/// the root carries all its claims over as they are, so a transient read
/// error costs one epoch of compaction, never a path.
#[tokio::test]
async fn unreadable_claim_is_carried_over_not_dropped() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 80_000);
        let b = SimPath::new("b", 2, 80_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push(ROOT, &[&b], &[&b]).await;
        let heads = Heads::load(&sim.backend).await.unwrap();
        let (_, newest) = heads.view.heads.iter().max_by_key(|(_, r)| r.time).unwrap();
        let meta = hestia::store::fetch_meta(&sim.backend, &newest.seg)
            .await
            .unwrap();
        fake.evict(&sim.http, &format!("tree-{}", meta.tree)).await;

        fake.set_clock(T0 + 2 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!(
            (stats.roots, stats.roots_unchanged, stats.paths_dropped),
            (1, 1, 0),
            "{stats:?}"
        );
        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(
            heads.view.roots[ROOT].len(),
            2,
            "both claims are the base now"
        );
        sim.assert_readable(&[&a]).await;
        assert!(sim.snapshot().await.lookup(&b.path_hash()).is_some());
    })
    .await;
}

/// A `g-*` that is listed but cannot be fetched stops GC: computing from
/// the record before it would bring back what that run dropped and sweep
/// what it wrote.
#[tokio::test]
async fn gc_refuses_to_run_blind_to_the_newest_record() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 80_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        fake.set_clock(T0 + 2 * HOUR);
        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;

        fake.set_stale_lookups(&sim.http, true).await;
        let err = sim
            .gc(GcPolicy::default())
            .run(T0 + 4 * HOUR)
            .await
            .expect_err("newest g-* unreadable");
        assert!(matches!(err, GcError::HeadUnreadable(_)), "{err}");
        fake.set_stale_lookups(&sim.http, false).await;
        assert_eq!(sim.stored_keys("g-").await.len(), 1, "nothing published");
    })
    .await;
}
