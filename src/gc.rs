//! Garbage collection over the segmented store: compact every root to one
//! segment, repack mostly-dead packs, publish `g-<epoch+1>`, then delete
//! what neither the new view nor this run's inputs name. GC is the only
//! deleter and the only clock (`docs/spec/segments.qnt`).

use std::collections::{BTreeSet, HashMap};
use std::process::ExitCode;

use crate::backend::{self, Backend, Listed};
use crate::chunker::{PackBuilder, coalesce_adjacent, extract_chunk, pack_cache_key};
use crate::cli::GcArgs;
use crate::heads::{GcRecord, RootRow};
use crate::manifest::{ChunkHash, PackKey, SegKey};
use crate::pipeline::{now_unix, upload_pack};
use crate::segment::{self, Meta, PackRow, Relocated, Tree};
use crate::store::{self, Heads, fetch_pack_index, meta_key, tree_key};
use crate::trust::Trust;

pub const SECS_PER_HOUR: u64 = 3_600;
pub const SECS_PER_DAY: u64 = 86_400;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Backend(#[from] backend::Error),
    #[error(transparent)]
    Store(#[from] store::Error),
    #[error(transparent)]
    Trust(#[from] crate::trust::Error),
    #[error(transparent)]
    Segment(#[from] segment::Error),
    #[error(transparent)]
    Chunker(#[from] crate::chunker::Error),
    #[error("previous GC ran {0}s ago, less than the minimum interval")]
    TooSoon(u64),
    /// Computing a view without the newest GC record would resurrect what
    /// that run dropped and sweep what it wrote.
    #[error("{0} is listed but cannot be fetched; refusing to run without it")]
    HeadUnreadable(String),
}

#[derive(Debug, Clone)]
pub struct GcPolicy {
    /// Roots without a drain for this long are dropped (deleted branches).
    pub root_ttl: u64,
    /// Live packs not accessed for this long get a 1-byte LRU touch.
    pub touch_age: u64,
    /// Packs whose live-chunk ratio falls below this get repacked.
    pub min_liveness: f64,
    /// Unreferenced objects younger than this are kept: a drain may have
    /// uploaded them without having published its head yet.
    pub min_age: u64,
    /// Two GC heads closer than this would let a reader's view fall two
    /// epochs behind.
    pub min_interval: u64,
    pub pack_target_size: u64,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            root_ttl: 14 * SECS_PER_DAY,
            touch_age: 4 * SECS_PER_DAY,
            min_liveness: 0.5,
            min_age: SECS_PER_HOUR,
            min_interval: SECS_PER_HOUR,
            pack_target_size: crate::pipeline::PACK_TARGET_SIZE,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcStats {
    pub epoch: u64,
    pub roots: usize,
    pub roots_expired: usize,
    /// Roots whose claims could not be merged this run and were carried over.
    pub roots_unchanged: usize,
    pub segments_written: usize,
    pub paths_dropped: usize,
    pub packs_repacked: usize,
    pub packs_evicted: usize,
    pub deleted: usize,
    pub touched: usize,
}

pub struct Gc {
    pub backend: Backend,
    pub trust: Trust,
    pub policy: GcPolicy,
    pub dry_run: bool,
}

struct Root {
    name: String,
    stamp: u64,
    /// R5: what the heads since the base claimed, else the base.
    claimed: Vec<SegKey>,
}

/// What a run did with one root.
enum Outcome {
    /// One segment now holds the root.
    Compacted(SegKey),
    /// The inputs could not all be read or merged; they stay the base as
    /// they are and the next run retries. Nothing is dropped on an error
    /// that may be transient.
    Unchanged,
}

/// Live chunks of one pack, OR-ed over every input segment.
struct PackUse {
    row: PackRow,
    bits: Vec<u8>,
}

impl PackUse {
    fn add(&mut self, bits: &[u8]) {
        if self.bits.len() < bits.len() {
            self.bits.resize(bits.len(), 0);
        }
        for (a, b) in self.bits.iter_mut().zip(bits) {
            *a |= b;
        }
    }
    fn ratio(&self) -> f64 {
        let live: u32 = self.bits.iter().map(|b| b.count_ones()).sum();
        f64::from(live) / f64::from(self.row.chunks.max(1))
    }
    fn is_live(&self, i: usize) -> bool {
        self.bits
            .get(i / 8)
            .is_some_and(|b| b & (1 << (i % 8)) != 0)
    }
}

/// Copies live chunks out of packs into new ones and remembers where they went.
#[derive(Default)]
struct Repacker {
    builder: PackBuilder,
    staged: Vec<((PackKey, u32), ChunkHash)>,
    moved: HashMap<(PackKey, u32), Relocated>,
}

impl Repacker {
    /// Forget chunks of `source` staged into the open pack (a repack that
    /// failed midway). Chunks already sealed into earlier packs are just
    /// dead weight there.
    fn unstage(&mut self, source: PackKey) {
        self.staged.retain(|((p, _), _)| *p != source);
        self.moved.retain(|(p, _), _| *p != source);
    }

    async fn seal(&mut self, gc: &Gc) -> Result<(), Error> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let pack = std::mem::take(&mut self.builder).finish();
        if !gc.dry_run {
            upload_pack(&gc.backend, &pack).await?;
        }
        let size = pack.index().size();
        let n = pack.chunks.len() as u32;
        let position: HashMap<ChunkHash, u32> = pack
            .chunks
            .iter()
            .enumerate()
            .map(|(j, (h, _))| (*h, j as u32))
            .collect();
        for (from, hash) in self.staged.drain(..) {
            self.moved
                .insert(from, (pack.hash, size, n, position[&hash]));
        }
        Ok(())
    }
}

/// Every key a view pins: its heads, segments, their trees and packs.
/// A segment without a meta in `metas` pins only its own key.
fn reachable<'a>(
    heads: impl IntoIterator<Item = &'a str>,
    segs: impl IntoIterator<Item = SegKey>,
    metas: &HashMap<SegKey, Meta>,
) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = heads.into_iter().map(str::to_owned).collect();
    for d in segs {
        out.insert(meta_key(&d));
        if let Some(m) = metas.get(&d) {
            out.insert(tree_key(&m.tree));
            out.extend(m.packs.iter().map(|p| pack_cache_key(&p.hash)));
        }
    }
    out
}

impl Gc {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), Error> {
        if !self.dry_run {
            self.backend.put(key, body.into()).await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, Error> {
        if self.dry_run {
            return Ok(true);
        }
        Ok(self.backend.delete(key).await?)
    }

    /// `.meta` of every segment in `segs`, fetched once. A segment whose meta
    /// cannot be read is absent from the map.
    async fn metas(
        &self,
        segs: impl IntoIterator<Item = SegKey>,
        into: &mut HashMap<SegKey, Meta>,
    ) {
        for d in segs {
            if into.contains_key(&d) {
                continue;
            }
            match store::fetch_meta(&self.backend, &d).await {
                Ok(m) => {
                    into.insert(d, m);
                }
                Err(err) => eprintln!("hestia gc: {err}"),
            }
        }
    }

    pub async fn run(&self, now: u64) -> Result<GcStats, Error> {
        let mut stats = GcStats::default();
        let heads = Heads::load(&self.backend, &self.trust).await?;
        if let Some(name) = heads.gc_missing.first() {
            return Err(Error::HeadUnreadable(name.clone()));
        }
        let prev = heads.gc.as_ref().map(|(_, r)| r);
        if let Some(age) = prev.map(|g| now.saturating_sub(g.time))
            && age < self.policy.min_interval
        {
            return Err(Error::TooSoon(age));
        }
        stats.epoch = prev.map_or(0, |g| g.epoch) + 1;

        let objects = self.backend.list_objects().await?;
        let stored: HashMap<&str, &Listed> = objects
            .iter()
            .flatten()
            .chain(&heads.listed)
            .map(|l| (l.key.as_str(), l))
            .collect();
        let stored_packs: Option<BTreeSet<PackKey>> = objects.as_ref().map(|o| {
            o.iter()
                .filter_map(|l| PackKey::parse(l.key.strip_prefix("pack-")?))
                .collect()
        });

        // Roots. R5: a drain's segment names every path it wants kept, so
        // once any drain published, the previous base is not an input.
        let prev_rows: HashMap<&str, &RootRow> = prev
            .into_iter()
            .flat_map(|g| g.roots.iter().map(|r| (r.name.as_str(), r)))
            .collect();
        let mut claims: HashMap<&str, BTreeSet<SegKey>> = HashMap::new();
        for (_, r) in &heads.view.heads {
            claims.entry(r.root.as_str()).or_default().insert(r.seg);
        }
        let mut roots = Vec::new();
        for name in heads.view.roots.keys() {
            let base = prev_rows.get(name.as_str());
            let claimed = claims.get(name.as_str());
            let stamp = match (base, claimed) {
                (Some(b), None) => b.stamp,
                _ => now,
            };
            if now.saturating_sub(stamp) > self.policy.root_ttl {
                stats.roots_expired += 1;
                continue;
            }
            let claimed: Vec<SegKey> = match (claimed, base) {
                (Some(c), _) => c.iter().copied().collect(),
                (None, Some(b)) => b.segments().collect(),
                (None, None) => unreachable!("a root is in the view by base or by claim"),
            };
            roots.push(Root {
                name: name.clone(),
                stamp,
                claimed,
            });
        }
        let mut metas: HashMap<SegKey, Meta> = HashMap::new();
        self.metas(
            roots.iter().flat_map(|r| r.claimed.iter().copied()),
            &mut metas,
        )
        .await;

        // Repack packs whose live ratio over all inputs is too low. A pack
        // the store no longer has is lost and the paths needing it go; a
        // pack that cannot be repacked for any other reason stays as it is.
        let mut usage: HashMap<PackKey, PackUse> = HashMap::new();
        for meta in roots
            .iter()
            .flat_map(|r| &r.claimed)
            .filter_map(|d| metas.get(d))
        {
            for row in &meta.packs {
                usage
                    .entry(row.hash)
                    .or_insert(PackUse {
                        row: PackRow::new(row.hash, row.size, row.chunks),
                        bits: Vec::new(),
                    })
                    .add(&row.live_bits);
            }
        }
        let lost: BTreeSet<PackKey> = match &stored_packs {
            Some(stored) => usage
                .keys()
                .filter(|p| !stored.contains(p))
                .copied()
                .collect(),
            None => BTreeSet::new(),
        };
        stats.packs_evicted = lost.len();
        for p in &lost {
            eprintln!("hestia gc: pack {p} was evicted, dropping the paths that need it");
        }
        let mut repacker = Repacker::default();
        let mut repacked: BTreeSet<PackKey> = BTreeSet::new();
        for (source, used) in &usage {
            if lost.contains(source) || used.ratio() >= self.policy.min_liveness {
                continue;
            }
            match self.repack(*source, used, &mut repacker).await {
                Ok(()) => {
                    repacked.insert(*source);
                }
                Err(err) => {
                    eprintln!("hestia gc: pack {source} left as is: {err}");
                    repacker.unstage(*source);
                }
            }
        }
        repacker.seal(self).await?;
        stats.packs_repacked = repacked.len();
        let affected = |m: &Meta| {
            m.packs
                .iter()
                .any(|p| lost.contains(&p.hash) || repacked.contains(&p.hash))
        };

        // Compact each root to one segment.
        let mut rows = Vec::new();
        for root in &roots {
            let only = match root.claimed.as_slice() {
                [d] if heads.view.roots[&root.name] == [*d] => metas.get(d),
                _ => None,
            };
            let outcome = match only {
                Some(m) if !affected(m) => Outcome::Compacted(root.claimed[0]),
                _ => {
                    self.merge(
                        &root.name,
                        &root.claimed,
                        &metas,
                        &lost,
                        &repacker,
                        &mut stats,
                    )
                    .await?
                }
            };
            let (seg, unmerged) = match outcome {
                Outcome::Compacted(seg) => (seg, vec![]),
                Outcome::Unchanged => {
                    stats.roots_unchanged += 1;
                    (root.claimed[0], root.claimed[1..].to_vec())
                }
            };
            rows.push(RootRow {
                name: root.name.clone(),
                seg,
                stamp: root.stamp,
                unmerged,
            });
        }
        stats.roots = rows.len();
        self.metas(rows.iter().map(|r| r.seg), &mut metas).await;

        // Every listed head is folded, including writer heads too old for
        // the view: once their segment is gone they must not come back.
        let record = GcRecord {
            epoch: stats.epoch,
            roots: rows,
            folded: heads
                .listed
                .iter()
                .filter(|l| l.key.starts_with("h-"))
                .map(|l| l.key.clone())
                .collect(),
            time: now,
        };
        let body = store::signed(&self.trust, record.encode()).await?;
        let name = record.head_name(&body).to_string();
        self.put(&name, body).await?;

        // Sweep: everything listed that neither the new view nor the one
        // this run loaded reaches. The loaded view stays because a reader
        // may hold it until it next reloads; unreferenced objects younger
        // than `min_age` stay because a drain may not have published yet.
        let new = reachable(
            [name.as_str()],
            record.roots.iter().flat_map(RootRow::segments),
            &metas,
        );
        self.metas(heads.view.segments(), &mut metas).await;
        let loaded = reachable(
            heads
                .gc
                .iter()
                .map(|(n, _)| n.as_str())
                .chain(heads.view.heads.iter().map(|(n, _)| n.as_str())),
            heads.view.segments(),
            &metas,
        );
        let old = |l: &Listed| match l.key.split_once('-').map(|(k, _)| k) {
            Some("g" | "h") => true,
            _ => [l.created, l.last_accessed]
                .into_iter()
                .flatten()
                .max()
                .is_some_and(|t| now.saturating_sub(t) > self.policy.min_age),
        };
        for l in stored.values().filter(|l| old(l)) {
            if !new.contains(&l.key) && !loaded.contains(&l.key) && self.delete(&l.key).await? {
                stats.deleted += 1;
            }
        }

        // Keep the LRU clock of everything the new view reaches ticking.
        if !self.dry_run {
            for key in &new {
                let idle = stored
                    .get(key.as_str())
                    .and_then(|l| l.last_accessed)
                    .map_or(0, |t| now.saturating_sub(t));
                if idle > self.policy.touch_age {
                    match self.backend.touch(key).await {
                        Ok(touched) => stats.touched += usize::from(touched),
                        Err(err) => eprintln!("hestia gc: touch {key} failed: {err}"),
                    }
                }
            }
            self.backend.flush().await?;
        }
        Ok(stats)
    }

    /// Copy the live chunks of `source` into the repacker. Geometry comes
    /// from the pack's own index, checked against what the segments say.
    async fn repack(
        &self,
        source: PackKey,
        used: &PackUse,
        out: &mut Repacker,
    ) -> Result<(), Error> {
        let index = fetch_pack_index(&self.backend, &used.row).await?;
        if index.entries.len() != used.row.chunks as usize {
            return Err(store::Error::Corrupt(pack_cache_key(&source)).into());
        }
        let live = index
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| used.is_live(*i));
        for run in coalesce_adjacent(live, |(_, e)| (e.offset, e.compressed_size)) {
            let start = run[0].1.offset;
            let last = run[run.len() - 1].1;
            let end = last.offset + u64::from(last.compressed_size);
            let Some(data) = self
                .backend
                .get(&pack_cache_key(&source), Some(start..end))
                .await?
                .filter(|d| d.len() as u64 == end - start)
            else {
                return Err(store::Error::MissingPack(source).into());
            };
            for (i, e) in run {
                let from = (e.offset - start) as usize;
                let frame = &data[from..from + e.compressed_size as usize];
                let raw = extract_chunk(frame, &e.hash)?;
                out.builder.add_compressed(e.hash, frame, raw.len() as u32);
                out.staged.push(((source, i as u32), e.hash));
                if out.builder.compressed_size() >= self.policy.pack_target_size
                    || out.builder.is_full()
                {
                    out.seal(self).await?;
                }
            }
        }
        Ok(())
    }

    async fn merge(
        &self,
        root: &str,
        claimed: &[SegKey],
        metas: &HashMap<SegKey, Meta>,
        lost: &BTreeSet<PackKey>,
        repacker: &Repacker,
        stats: &mut GcStats,
    ) -> Result<Outcome, Error> {
        let mut pairs = Vec::new();
        let mut trees = Vec::new();
        for d in claimed {
            let Some(m) = metas.get(d) else {
                eprintln!("hestia gc: {root}: left unmerged, seg-{d} unreadable");
                return Ok(Outcome::Unchanged);
            };
            match store::fetch_tree(&self.backend, &m.tree).await {
                Ok(t) => trees.push(t),
                Err(err) => {
                    eprintln!("hestia gc: {root}: left unmerged: {err}");
                    return Ok(Outcome::Unchanged);
                }
            }
            pairs.push(m);
        }
        let pairs: Vec<(&Meta, &Tree)> = pairs.into_iter().zip(&trees).collect();
        let merged = segment::merge(&pairs, |row, i| {
            if lost.contains(&row.hash) {
                return None;
            }
            let here = (row.hash, row.size, row.chunks, i);
            Some(repacker.moved.get(&(row.hash, i)).copied().unwrap_or(here))
        });
        let (sealed, dropped) = match merged {
            Ok(m) => m,
            Err(err) => {
                eprintln!("hestia gc: {root}: left unmerged: {err}");
                return Ok(Outcome::Unchanged);
            }
        };
        stats.paths_dropped += dropped;
        stats.segments_written += 1;
        self.put(&tree_key(&sealed.tree_key), sealed.tree).await?;
        self.put(&meta_key(&sealed.key), sealed.meta).await?;
        Ok(Outcome::Compacted(sealed.key))
    }
}

pub async fn run(args: &GcArgs) -> ExitCode {
    let http = reqwest::Client::new();
    let backend = match Backend::from_env(http.clone()) {
        Ok(b) => b,
        Err(err) => {
            eprintln!(
                "hestia gc: {err}\n\
                 hint: GC needs the cache tokens the hestia action exports and \
                 GITHUB_TOKEN with `actions: write`"
            );
            return ExitCode::FAILURE;
        }
    };
    let trust = match Trust::from_env() {
        Ok(t) => t,
        Err(err) => {
            eprintln!("hestia gc: {err}");
            return ExitCode::FAILURE;
        }
    };
    let gc = Gc {
        backend,
        trust,
        policy: GcPolicy {
            root_ttl: args.root_ttl * SECS_PER_DAY,
            touch_age: args.touch_age * SECS_PER_DAY,
            ..GcPolicy::default()
        },
        dry_run: args.dry_run,
    };
    match gc.run(now_unix()).await {
        Ok(stats) => {
            eprintln!("hestia gc: {stats:?}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("hestia gc: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_use_ors_bitsets() {
        let mut u = PackUse {
            row: PackRow::new(
                crate::manifest::ObjKey {
                    digest: crate::manifest::PackHash([0; 32]),
                    nonce: 0,
                },
                0,
                16,
            ),
            bits: vec![],
        };
        u.add(&[0b0000_0101]);
        u.add(&[0b0000_0100, 0b1]);
        assert_eq!(u.ratio(), 3.0 / 16.0);
        assert!(u.is_live(0) && u.is_live(2) && u.is_live(8) && !u.is_live(1));
    }
}
