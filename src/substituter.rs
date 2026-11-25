//! The substituter: Nix binary cache protocol served from the manifest.
//!
//! Routes (axum), mounted into `hestia serve`:
//!
//! * `GET /nix-cache-info` — store dir, mass-query flag, priority.
//! * `GET /{hash}.narinfo` — manifest lookup; a hit is recorded in the
//!   [`AccessLog`] (narinfo hits are the liveness signal: accessed paths
//!   join this run's GC root).
//! * `GET /nar/{narhash}.nar.zst` — chunks are fetched from packs (batched
//!   Range requests, parallel across packs, signed URLs cached and
//!   refreshed on 403), the NAR is synthesized from the manifest tree, and
//!   its hash is verified before a single byte leaves the process. Any
//!   failure (evicted pack, missing chunk, hash mismatch) turns into a 404
//!   so Nix falls through to the next substituter — never partial or
//!   corrupt data.
//! * `GET /closure/{hashes}` — the closure of the given path hashes
//!   (comma-separated), restricted to manifest members, streamed in
//!   `nix-store --export` format for a one-request prefetch via
//!   `nix-store --import`.
//! * `GET /closure/{hashes}/external-references` — references required by
//!   that export but omitted from the manifest, one store path per line.
//!
//! A semaphore caps concurrent pack reads so parallel narinfo queries
//! from Nix (`WantMassQuery: 1`) do not flood the GHA cache API.
//!
//! Responses are unsigned: the action configures the store URL with
//! `?trusted=true`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use tokio::sync::Semaphore;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use serde::Deserialize;

use harmonia_store_nar_info::{build_narinfo, format_narinfo_txt};
use harmonia_store_path::StoreDir;
use harmonia_store_path_info::{NarHash, UnkeyedValidPathInfo, ValidPathInfo};

use crate::backend::Backend;
use crate::chunker::{self, flatten_tree, pack_cache_key};
use crate::gha::Error as GhaError;
use crate::manifest::{
    ChunkHash, ChunkLocation, FileSystemObject, Hash32, PackHash, PathEntry, PathHash,
};
use crate::narzstd::{self, Frame};
use crate::pipeline::AccessLog;
use crate::refnorm::RefTable;
use crate::segment::PackIndex;
use crate::store::{ChunkMap, Resolved, Snapshot};

/// Priority advertised in /nix-cache-info. Lower wins: 30 puts hestia ahead
/// of cache.nixos.org (40), so Nix asks the local cache first and only falls
/// through to upstream on a miss.
const PRIORITY: u32 = 30;

/// Upper bound for decompressed chunks kept in memory across NAR requests.
/// Oldest chunks are dropped first.
const CHUNK_CACHE_BUDGET: usize = 256 * 1024 * 1024;

/// Chunks of one pack whose gap is at most this are fetched in one Range
/// read: dedup and generational scatter punch small holes into otherwise
/// contiguous chunk runs, and re-downloading the hole is far cheaper than
/// another ~66 ms round trip.
const PACK_FETCH_GAP_BYTES: u64 = 128 * 1024;

/// A Range read is extended by a window of up to this many bytes (clamped
/// to the pack) and all chunks inside the region go into the chunk cache.
/// Packs are written in drain order, so the neighbours of a requested
/// chunk are often what nix asks for next, and read-ahead turns thousands
/// of per-path round trips into a few large reads. The window is kept per
/// pack: it doubles while reads continue where the last one stopped and
/// halves when they land elsewhere, since for scattered paths of a shared
/// root everything read ahead is wasted (`benches/cluster.rs`).
const PACK_READ_AHEAD_BYTES: u64 = 4 * 1024 * 1024;
const PACK_READ_AHEAD_MIN: u64 = PACK_FETCH_GAP_BYTES;
const PACK_READ_AHEAD_START: u64 = 1024 * 1024;

/// What the substituter serves from.
#[derive(Default)]
struct ManifestView {
    snapshot: Option<Arc<Snapshot>>,
    /// Packs observed to be evicted from the cache. Affected paths 404
    /// already at narinfo time. Lives inside the view so every snapshot
    /// replacement starts from an empty set.
    missing_packs: Mutex<BTreeSet<PackHash>>,
}

impl ManifestView {
    fn mark_pack_missing(&self, pack: PackHash) {
        self.missing_packs
            .lock()
            .expect("missing pack set poisoned")
            .insert(pack);
    }

    /// Whether no chunk of `hash` lives in a known-missing pack. The
    /// tree walk only runs after an eviction was actually observed.
    async fn available(&self, hash: &PathHash) -> bool {
        let missing = self
            .missing_packs
            .lock()
            .expect("missing pack set poisoned")
            .clone();
        let Some(snapshot) = self.snapshot.as_ref().filter(|_| !missing.is_empty()) else {
            return true;
        };
        snapshot
            .packs_of(hash)
            .await
            .is_ok_and(|packs| packs.is_disjoint(&missing))
    }

    fn lookup(&self, hash: &PathHash) -> Option<PathEntry> {
        self.snapshot.as_ref()?.lookup(hash)
    }

    fn contains(&self, hash: &PathHash) -> bool {
        self.snapshot.as_ref().is_some_and(|s| s.contains(hash))
    }

    async fn resolve(&self, hash: &PathHash) -> Result<Option<Resolved>, FetchError> {
        let Some(s) = &self.snapshot else {
            return Ok(None);
        };
        Ok(s.resolve(hash).await?)
    }
}

/// Shared, replaceable view: the substituter reads it on every request,
/// the daemon replaces it at startup and after every drain.
#[derive(Clone, Default)]
pub struct ManifestStore {
    inner: Arc<RwLock<Arc<ManifestView>>>,
    asked: Arc<Mutex<Asked>>,
    /// Held by whoever builds the next snapshot from the served one, so
    /// that no two of them replace each other's work.
    updates: Arc<tokio::sync::Mutex<()>>,
    shards: Option<Arc<Shards>>,
}

/// What a store in shards ([`crate::shard`]) is loaded with.
struct Shards {
    /// The roots whose shards are served, the one published into first.
    roots: Vec<String>,
    backend: Backend,
    trust: crate::trust::Trust,
}

/// Paths that were asked for and not served, to tell how often a path
/// another node published was asked for here before this node saw it.
#[derive(Default)]
struct Asked {
    missed: HashMap<PathHash, std::time::Instant>,
    /// Misses of paths a later [`ManifestStore::set_followed`] brought.
    early: u64,
}

/// A miss older than this says nothing about a path that shows up now.
const ASKED_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);
const ASKED_MAX: usize = 100_000;

impl ManifestStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves the shards of `roots` that requests ask for
    /// ([`ManifestStore::hold`]), and nothing before the first request.
    pub fn sharded(roots: &[String], backend: Backend, trust: crate::trust::Trust) -> Self {
        ManifestStore {
            shards: Some(Arc::new(Shards {
                roots: roots.to_vec(),
                backend,
                trust,
            })),
            ..Self::default()
        }
    }

    /// Whoever replaces the snapshot by one made from it holds this.
    pub async fn updating(&self) -> tokio::sync::OwnedMutexGuard<()> {
        self.updates.clone().lock_owned().await
    }

    /// Serve the shard of `hash`: the first request for a shard loads
    /// it from the store, so a node that stands in for another serves
    /// what that one published. Nothing to do in a store without shards.
    pub async fn hold(&self, hash: &PathHash) -> Result<(), crate::store::Error> {
        let Some(shards) = &self.shards else {
            return Ok(());
        };
        let shard = crate::shard::Shard::of(hash);
        let wanted: Vec<String> = shards.roots.iter().map(|r| shard.root(r)).collect();
        let held = || {
            self.snapshot()
                .is_some_and(|s| wanted.iter().all(|r| s.roots.contains(r)))
        };
        if held() {
            return Ok(());
        }
        let _updating = self.updating().await;
        if held() {
            return Ok(());
        }
        let previous = self.snapshot();
        let mut roots = previous.as_ref().map_or_else(Vec::new, |p| p.roots.clone());
        roots.extend(
            wanted
                .iter()
                .filter(|r| !roots.contains(r))
                .cloned()
                .collect::<Vec<_>>(),
        );
        let snapshot = Snapshot::load(
            shards.backend.clone(),
            shards.trust.clone(),
            &roots,
            previous.as_deref(),
        )
        .await?;
        self.set_followed(Arc::new(snapshot));
        Ok(())
    }

    /// Serve `snapshot`, which this daemon published into.
    pub fn set_snapshot(&self, snapshot: Arc<Snapshot>) {
        self.replace(snapshot, false);
    }

    /// Serve `snapshot`, which was loaded from the store and may hold
    /// what others published. Returns how many of its paths were asked
    /// for here, and missed, before this call: each may have been built
    /// a second time.
    pub fn set_followed(&self, snapshot: Arc<Snapshot>) -> usize {
        self.replace(snapshot, true)
    }

    fn replace(&self, snapshot: Arc<Snapshot>, followed: bool) -> usize {
        let early = {
            let mut asked = self.asked.lock().expect("asked lock poisoned");
            let before = asked.missed.len();
            asked
                .missed
                .retain(|hash, at| at.elapsed() < ASKED_WINDOW && !snapshot.contains(hash));
            before - asked.missed.len()
        };
        *self.inner.write().expect("manifest lock poisoned") = Arc::new(ManifestView {
            snapshot: Some(snapshot),
            missing_packs: Mutex::default(),
        });
        if !followed {
            return 0;
        }
        self.asked.lock().expect("asked lock poisoned").early += early as u64;
        early
    }

    /// A path was asked for and is not served.
    fn note_miss(&self, hash: PathHash) {
        let mut asked = self.asked.lock().expect("asked lock poisoned");
        if asked.missed.len() >= ASKED_MAX {
            asked.missed.clear();
        }
        asked.missed.insert(hash, std::time::Instant::now());
    }

    /// Misses of paths that a later load from the store brought.
    pub fn early_misses(&self) -> u64 {
        self.asked.lock().expect("asked lock poisoned").early
    }

    pub fn snapshot(&self) -> Option<Arc<Snapshot>> {
        self.view().snapshot.clone()
    }

    fn view(&self) -> Arc<ManifestView> {
        Arc::clone(&self.inner.read().expect("manifest lock poisoned"))
    }

    /// Stored and not known to touch an evicted pack.
    pub async fn available(&self, hash: &PathHash) -> bool {
        let view = self.view();
        view.contains(hash) && view.available(hash).await
    }

    /// Number of paths currently servable.
    pub fn path_count(&self) -> usize {
        self.view().snapshot.as_ref().map_or(0, |s| s.path_count())
    }
}

#[derive(Debug, thiserror::Error)]
enum FetchError {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("chunk {0} has no known location")]
    UnknownChunk(ChunkHash),

    #[error("pack {} is not in the cache (evicted?)", pack_cache_key(.0))]
    PackUnavailable(PackHash),

    #[error("chunk extraction failed: {0}")]
    Chunker(#[from] chunker::Error),

    #[error(transparent)]
    Store(crate::store::Error),
}

impl From<crate::store::Error> for FetchError {
    fn from(err: crate::store::Error) -> Self {
        match err {
            crate::store::Error::MissingPack(p) => FetchError::PackUnavailable(p),
            err => FetchError::Store(err),
        }
    }
}

/// Chunk frames kept in memory, evicted least-recently-used first
/// once over budget: chunks shared across paths (dedup) and repeated NAR
/// requests keep hitting early-inserted chunks, so insertion-order
/// eviction would drop the hot set first.
#[derive(Default)]
struct ChunkCache {
    /// Frame and the tick of its last use.
    chunks: HashMap<ChunkHash, (Frame, u64)>,
    /// Tick of last use to chunk: the first entry is the one to evict.
    order: BTreeMap<u64, ChunkHash>,
    tick: u64,
    total: usize,
}

impl ChunkCache {
    fn get(&mut self, hash: &ChunkHash) -> Option<Frame> {
        let (data, used) = self.chunks.get_mut(hash)?;
        // A hit must postpone eviction.
        self.order.remove(used);
        self.tick += 1;
        *used = self.tick;
        self.order.insert(self.tick, *hash);
        Some(data.clone())
    }

    fn insert(&mut self, hash: ChunkHash, data: Frame) {
        if self.chunks.contains_key(&hash) {
            return;
        }
        self.total += data.zstd.len();
        self.tick += 1;
        self.chunks.insert(hash, (data, self.tick));
        self.order.insert(self.tick, hash);
        while self.total > CHUNK_CACHE_BUDGET {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((dropped, _)) = self.chunks.remove(&oldest) {
                self.total -= dropped.zstd.len();
            }
        }
    }
}

/// Range reads of one pack that are under way, and how far to read ahead
/// in it.
struct PackReads {
    inflight: Vec<InflightRead>,
    window: u64,
    /// Where the last planned read stops.
    last_end: Option<u64>,
}

struct InflightRead {
    id: u64,
    range: std::ops::Range<u64>,
    /// Closed when the read is over, whether it worked or not.
    done: tokio::sync::watch::Receiver<()>,
}

impl Default for PackReads {
    fn default() -> Self {
        Self {
            inflight: Vec::new(),
            window: PACK_READ_AHEAD_START,
            last_end: None,
        }
    }
}

impl PackReads {
    /// Adapt the window to where `start` lies relative to the last read.
    fn window_for(&mut self, start: u64) -> u64 {
        if let Some(last_end) = self.last_end {
            self.window = if start.abs_diff(last_end) <= self.window {
                (self.window * 2).min(PACK_READ_AHEAD_BYTES)
            } else {
                (self.window / 2).max(PACK_READ_AHEAD_MIN)
            };
        }
        self.window
    }
}

/// Takes this request's reads off the in-flight list, also when the
/// request is dropped half way, and so wakes whoever waits for them.
struct ReadGuard<'a> {
    reads: &'a Mutex<HashMap<PackHash, PackReads>>,
    pack: PackHash,
    ids: std::ops::Range<u64>,
    _done: tokio::sync::watch::Sender<()>,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(state) = self
            .reads
            .lock()
            .expect("pack reads poisoned")
            .get_mut(&self.pack)
        {
            state.inflight.retain(|r| !self.ids.contains(&r.id));
        }
    }
}

/// Fetches chunks from pack blobs.
struct ChunkFetcher {
    backend: Backend,
    /// Filled by NAR requests.
    chunk_cache: Mutex<ChunkCache>,
    /// Per-path serialization: concurrent NAR requests for the same path
    /// must not fetch the same chunks twice.
    path_locks: Mutex<HashMap<PathHash, Arc<tokio::sync::Mutex<()>>>>,
    /// Caps pack reads that hit the GHA cache API. Acquired per pack,
    /// *after* the per-path lock and the cache check, so idle waiters and
    /// cache hits never pin a permit. FIFO: a many-pack path cannot
    /// starve others.
    fetch_semaphore: Semaphore,
    /// Concurrent requests for chunks of one region share a read.
    pack_reads: Mutex<HashMap<PackHash, PackReads>>,
    next_read_id: std::sync::atomic::AtomicU64,
}

impl ChunkFetcher {
    fn new(backend: Backend) -> Self {
        Self {
            fetch_semaphore: Semaphore::new(backend.read_concurrency()),
            backend,
            chunk_cache: Mutex::new(ChunkCache::default()),
            path_locks: Mutex::new(HashMap::new()),
            pack_reads: Mutex::new(HashMap::new()),
            next_read_id: Default::default(),
        }
    }

    fn path_lock(&self, path: PathHash) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.path_locks.lock().expect("path lock map poisoned");
        // Drop locks no request holds anymore: without pruning the map
        // grows by one entry per distinct path for the process lifetime.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        Arc::clone(locks.entry(path).or_default())
    }

    async fn read_pack_range(
        &self,
        pack: PackHash,
        range: std::ops::Range<u64>,
        pack_len: u64,
    ) -> Result<Bytes, FetchError> {
        self.backend
            .get_pack(&pack_cache_key(&pack), range, pack_len)
            .await?
            .ok_or(FetchError::PackUnavailable(pack))
    }

    /// Fetch all chunks of `entry`, using cached chunks where possible.
    async fn fetch_path_chunks(
        &self,
        path: PathHash,
        resolved: &Resolved,
    ) -> Result<BTreeMap<ChunkHash, Frame>, FetchError> {
        // Serialize per path so concurrent NAR requests for the same
        // path do the work once.
        let lock = self.path_lock(path);
        let _guard = lock.lock().await;
        self.fetch_chunks(&resolved.map, entry_chunks(&resolved.entry))
            .await
    }

    /// Fetch a set of chunks, using cached chunks where possible.
    ///
    /// Missing chunks are grouped by pack; nearby chunks within a pack are
    /// coalesced into single Range requests (with read-ahead, see
    /// [`PACK_READ_AHEAD_BYTES`]); packs are fetched in parallel.
    async fn fetch_chunks(
        &self,
        source: &ChunkMap,
        needed: BTreeSet<ChunkHash>,
    ) -> Result<BTreeMap<ChunkHash, Frame>, FetchError> {
        let mut result: BTreeMap<ChunkHash, Frame> = BTreeMap::new();
        let mut missing: BTreeMap<PackHash, Vec<(ChunkHash, ChunkLocation)>> = BTreeMap::new();
        {
            let mut cache = self.chunk_cache.lock().expect("chunk cache poisoned");
            for chunk in needed {
                if let Some(data) = cache.get(&chunk) {
                    result.insert(chunk, data);
                    continue;
                }
                let location = source
                    .chunks
                    .get(&chunk)
                    .ok_or(FetchError::UnknownChunk(chunk))?;
                missing
                    .entry(location.pack)
                    .or_default()
                    .push((chunk, location.clone()));
            }
        }

        // Fetch packs in parallel.
        let fetches = missing.into_iter().map(|(pack, chunks)| {
            let index = Arc::clone(&source.packs[&pack]);
            self.fetch_from_pack(pack, chunks, index)
        });
        for fetched in futures_util::future::try_join_all(fetches).await? {
            result.extend(fetched);
        }

        Ok(result)
    }

    /// Fetch a set of chunks from one pack with as few Range requests as
    /// possible (gap coalescing plus read-ahead, see [`plan_pack_reads`]).
    /// Chunks inside a region another request is reading are waited for
    /// instead of read again: Nix asks for neighbouring paths in parallel,
    /// and each of them reading ahead over the same region multiplied the
    /// download. Every chunk in a fetched region goes into the chunk cache.
    async fn fetch_from_pack(
        &self,
        pack: PackHash,
        mut wanted: Vec<(ChunkHash, ChunkLocation)>,
        index: Arc<PackIndex>,
    ) -> Result<Vec<(ChunkHash, Frame)>, FetchError> {
        wanted.sort_by_key(|(_, location)| location.offset);
        let chunk_count = wanted.len();
        let started = Instant::now();
        let (mut range_count, mut range_bytes) = (0, 0u64);
        let mut out: Vec<(ChunkHash, Frame)> = Vec::with_capacity(wanted.len());
        loop {
            {
                let mut cache = self.chunk_cache.lock().expect("chunk cache poisoned");
                wanted.retain(|(hash, _)| match cache.get(hash) {
                    Some(frame) => {
                        out.push((*hash, frame));
                        false
                    }
                    None => true,
                });
            }
            if wanted.is_empty() {
                break;
            }
            // Split what is left into chunks someone is already reading
            // and chunks this request reads, and announce the latter.
            let mut waits: BTreeMap<u64, tokio::sync::watch::Receiver<()>> = BTreeMap::new();
            let (ranges, guard) = {
                let mut reads = self.pack_reads.lock().expect("pack reads poisoned");
                let state = reads.entry(pack).or_default();
                let mut spans = Vec::new();
                for (_, l) in &wanted {
                    let end = l.offset.saturating_add(u64::from(l.compressed_size));
                    match state
                        .inflight
                        .iter()
                        .find(|r| r.range.start <= l.offset && end <= r.range.end)
                    {
                        Some(r) => _ = waits.insert(r.id, r.done.clone()),
                        None => spans.push((l.offset, l.compressed_size)),
                    }
                }
                // A backend that keeps packs reads ahead itself, in blocks
                // that a later read finds again.
                let local = self.backend.keeps_packs();
                let ranges = plan_pack_reads(&spans, index.size(), |start| {
                    if local { 0 } else { state.window_for(start) }
                });
                state.last_end = ranges.last().map(|r| r.end).or(state.last_end);
                let (tx, rx) = tokio::sync::watch::channel(());
                let n = ranges.len() as u64;
                let first = self
                    .next_read_id
                    .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                for (id, range) in (first..).zip(&ranges) {
                    state.inflight.push(InflightRead {
                        id,
                        range: range.clone(),
                        done: rx.clone(),
                    });
                }
                let guard = ReadGuard {
                    reads: &self.pack_reads,
                    pack,
                    ids: first..first + n,
                    _done: tx,
                };
                (ranges, guard)
            };
            if !ranges.is_empty() {
                // One global permit while this talks to the store. The
                // semaphore is never closed, so acquire cannot fail.
                let _permit = self
                    .fetch_semaphore
                    .acquire()
                    .await
                    .expect("fetch semaphore closed");
                for range in ranges {
                    range_count += 1;
                    range_bytes += range.end - range.start;
                    // Slices are in bounds: the backend fails unless the
                    // response is the whole range.
                    let data = self
                        .read_pack_range(pack, range.clone(), index.size())
                        .await?;
                    let mut cache = self.chunk_cache.lock().expect("chunk cache poisoned");
                    for e in chunks_in_range(&index, &range) {
                        let from = (e.offset - range.start) as usize;
                        let frame = Frame {
                            zstd: data.slice(from..from + e.compressed_size as usize),
                            size: e.uncompressed_size,
                        };
                        // Handed over directly: a cache over budget may
                        // not keep them until the next round.
                        if let Some(i) = wanted.iter().position(|(h, _)| *h == e.hash) {
                            wanted.swap_remove(i);
                            out.push((e.hash, frame.clone()));
                        }
                        cache.insert(e.hash, frame);
                    }
                }
            }
            drop(guard);
            // A read that failed or whose chunks were evicted again
            // leaves them wanted, and the next round reads them itself.
            for mut done in waits.into_values() {
                let _ = done.changed().await;
            }
        }
        // One line per pack fetch (= per burst of GHA cache traffic): shows
        // whether chunks coalesce into few large Range reads or degrade
        // into many small ones. A busy job produces hundreds of these, so
        // they only appear when the workflow is re-run with debug logging
        // (RUNNER_DEBUG=1).
        if std::env::var_os("RUNNER_DEBUG").is_some_and(|value| value == "1") {
            eprintln!(
                "hestia substituter: pack {}: {chunk_count} chunks requested                  in {range_count} range reads ({}, {:.1}s)",
                pack_cache_key(&pack),
                crate::drain::human_bytes(range_bytes),
                started.elapsed().as_secs_f64(),
            );
        }
        Ok(out)
    }
}

/// Plan the Range reads for one pack: coalesce chunk spans whose gap is at
/// most [`PACK_FETCH_GAP_BYTES`], extend each read to at least the
/// read-ahead `window` for its start (clamped to `pack_end`), and merge
/// reads that the extension made overlap. `spans` are `(offset, size)`,
/// sorted by offset.
fn plan_pack_reads(
    spans: &[(u64, u32)],
    pack_end: u64,
    mut window: impl FnMut(u64) -> u64,
) -> Vec<std::ops::Range<u64>> {
    let mut runs: Vec<std::ops::Range<u64>> = Vec::new();
    for &(offset, size) in spans {
        // Checked: offsets come from the manifest; a corrupt value near
        // u64::MAX must not panic.
        let end = offset.saturating_add(u64::from(size));
        match runs.last_mut() {
            Some(run) if offset <= run.end.saturating_add(PACK_FETCH_GAP_BYTES) => {
                run.end = run.end.max(end);
            }
            _ => runs.push(offset..end),
        }
    }
    let mut reads: Vec<std::ops::Range<u64>> = Vec::new();
    for run in runs {
        if let Some(read) = reads.last_mut().filter(|read| run.start <= read.end) {
            read.end = read.end.max(run.end);
            continue;
        }
        let read_ahead = run.start.saturating_add(window(run.start)).min(pack_end);
        reads.push(run.start..run.end.max(read_ahead));
    }
    reads
}

/// All chunks of a pack that lie fully inside `range`, as
/// `(offset, size, hash)`.
fn chunks_in_range<'a>(
    index: &'a PackIndex,
    range: &'a std::ops::Range<u64>,
) -> impl Iterator<Item = &'a crate::segment::PackIndexEntry> + 'a {
    let start = index.entries.partition_point(|e| e.offset < range.start);
    index.entries[start..]
        .iter()
        .take_while(move |e| e.offset < range.end)
        .filter(move |e| e.offset + u64::from(e.compressed_size) <= range.end)
}

/// Mark manifest packs missing from one REST listing of `pack-*` entries
/// as evicted. Bails out above `max_entries`: a partial listing can prove
/// presence but never absence. Errors are logged, not fatal: the NAR
/// handler's lazy negative cache remains the backstop.
pub async fn verify_packs(backend: &Backend, store: &ManifestStore, max_entries: u64) {
    // One view for the whole comparison: marks must land on the same
    // generation the listing was compared against.
    let view = store.view();
    let packs = view
        .snapshot
        .as_ref()
        .map(|s| s.pack_hashes())
        .unwrap_or_default();
    if packs.is_empty() {
        return;
    }
    let listed = match backend.list("pack-", Some(max_entries)).await {
        Ok(Some(entries)) => entries,
        // Too many to list, or a store that cannot list packs.
        Ok(None) => return,
        // Listing needs GITHUB_TOKEN, which build jobs may not grant. The NAR
        // handler's lazy negative cache still catches evictions.
        Err(GhaError::MissingEnv(_)) => return,
        Err(err) => {
            eprintln!("hestia substituter: upfront pack verification failed: {err}");
            return;
        }
    };
    let present: BTreeSet<&str> = listed.iter().map(|entry| entry.key.as_str()).collect();
    let mut evicted = 0usize;
    for pack in &packs {
        if !present.contains(pack_cache_key(pack).as_str()) {
            view.mark_pack_missing(*pack);
            evicted += 1;
        }
    }
    if evicted > 0 {
        eprintln!(
            "hestia substituter: {evicted} of {} referenced packs were evicted from the \
             cache; their paths will be rebuilt or fetched upstream",
            packs.len()
        );
    }
}

/// Reloads the served view from the backend. The NAR handler invokes it
/// when a pack the current view points at is gone: a concurrent GC repack
/// moved live chunks into new packs, which only a fresh listing shows.
pub type ManifestReload =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Callback invoked on every substituter request (the daemon uses it to
/// reset its idle-exit timer: an actively substituting Nix counts as
/// activity). The returned guard is held for the whole request so that
/// long downloads count as in-flight work instead of only touching the
/// idle clock once at request start.
pub type ActivityHook = Arc<dyn Fn() -> Box<dyn Send> + Send + Sync>;

/// Signals that the startup load has finished. Narinfo requests block on
/// it so an early `nix build` cannot race the load and see spurious misses.
pub type ManifestReady = tokio::sync::watch::Receiver<bool>;

/// The substituter's shared state and configuration.
pub struct Substituter {
    store_dir: StoreDir,
    manifest: ManifestStore,
    access_log: AccessLog,
    fetcher: ChunkFetcher,
    activity_hook: Option<ActivityHook>,
    manifest_reload: Option<ManifestReload>,
    manifest_ready: Option<ManifestReady>,
}

impl Substituter {
    pub fn new(
        store_dir: StoreDir,
        manifest: ManifestStore,
        access_log: AccessLog,
        backend: Backend,
    ) -> Self {
        Self {
            store_dir,
            manifest,
            access_log,
            fetcher: ChunkFetcher::new(backend),
            activity_hook: None,
            manifest_reload: None,
            manifest_ready: None,
        }
    }

    /// Install a callback invoked on every request.
    pub fn with_activity_hook(mut self, hook: ActivityHook) -> Self {
        self.activity_hook = Some(hook);
        self
    }

    /// Install a manifest-reload callback (see [`ManifestReload`]).
    pub fn with_manifest_reload(mut self, reload: ManifestReload) -> Self {
        self.manifest_reload = Some(reload);
        self
    }

    /// Install a startup-load gate (see [`ManifestReady`]).
    pub fn with_manifest_ready(mut self, ready: ManifestReady) -> Self {
        self.manifest_ready = Some(ready);
        self
    }

    /// Block until the startup manifest load finished (no-op without a
    /// gate, or once it fired).
    /// Serve the shard of `hash` before looking it up. A shard that
    /// cannot be loaded is a miss for its paths.
    async fn hold(&self, hash: &PathHash) {
        if let Err(err) = self.manifest.hold(hash).await {
            eprintln!("hestia substituter: cannot load the shard of {hash}: {err}");
        }
    }

    async fn manifest_ready(&self) {
        if let Some(ready) = &self.manifest_ready {
            // Only fails if the sender is dropped without sending; serve
            // treats that as "nothing to wait for".
            let _ = ready.clone().wait_for(|ready| *ready).await;
        }
    }

    /// Build the axum router serving the binary cache protocol.
    pub fn into_router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route("/nix-cache-info", get(nix_cache_info))
            .route("/{file}", get(narinfo))
            .route("/nar/{file}", get(nar))
            .route("/build-trace-v2/{drv}/{file}", get(build_trace))
            .route("/closure/{hashes}", get(closure))
            .route(
                "/closure/{hashes}/external-references",
                get(closure_external_references),
            )
            .with_state(state)
    }

    /// Mark this request as in-flight work for the daemon's idle-exit
    /// timer; the guard must live until the response is built.
    fn touch(&self) -> Option<Box<dyn Send>> {
        self.activity_hook.as_ref().map(|hook| hook())
    }
}

async fn nix_cache_info(State(state): State<Arc<Substituter>>) -> Response {
    let _activity = state.touch();
    let body = format!(
        "StoreDir: {}\nWantMassQuery: 1\nPriority: {PRIORITY}\n",
        state.store_dir
    );
    ([(header::CONTENT_TYPE, "text/x-nix-cache-info")], body).into_response()
}

/// Convert a manifest entry into the narinfo metadata harmonia's formatter
/// expects.
fn narinfo_for_entry(store_dir: &StoreDir, entry: &PathEntry, hash: &str) -> Vec<u8> {
    let info = UnkeyedValidPathInfo {
        deriver: entry.deriver.clone(),
        nar_hash: NarHash::from_slice(&entry.nar_hash.0).expect("nar hash is always 32 bytes"),
        references: entry.references.iter().cloned().collect(),
        registration_time: None,
        nar_size: entry.nar_size,
        ultimate: false,
        // Unsigned: the store URL carries ?trusted=true.
        signatures: BTreeSet::new(),
        ca: entry.ca.as_deref().and_then(|ca| match ca.parse() {
            Ok(ca) => Some(ca),
            // Served without a CA line the path silently degrades to
            // input-addressed on the substituting side; leave a trace.
            Err(err) => {
                eprintln!(
                    "hestia substituter: dropping unparsable CA string {ca:?} for {}: {err}",
                    entry.store_path
                );
                None
            }
        }),
        store_dir: store_dir.clone(),
    };
    let mut narinfo = build_narinfo(
        store_dir,
        ValidPathInfo {
            path: entry.store_path.clone(),
            info,
        },
        hash,
        &[],
    );
    narinfo.info.url = narinfo.info.url.map(|u| u.replace(".nar?", ".nar.zst?"));
    narinfo.info.compression = Some("zstd".into());
    // The compressed stream is stitched per request: its hash and size are
    // not known here, and the formatter would claim the NAR's.
    let mut txt = Vec::new();
    for line in format_narinfo_txt(store_dir, &narinfo).split_inclusive(|b| *b == b'\n') {
        if !line.starts_with(b"FileHash:") && !line.starts_with(b"FileSize:") {
            txt.extend_from_slice(line);
        }
    }
    txt
}

async fn narinfo(State(state): State<Arc<Substituter>>, Path(file): Path<String>) -> Response {
    let _activity = state.touch();
    state.manifest_ready().await;
    if let Some(hash_str) = file.strip_suffix(".ls") {
        return listing(&state, hash_str).await;
    }
    let Some(hash_str) = file.strip_suffix(".narinfo") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(path_hash) = hash_str.parse::<PathHash>() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    state.hold(&path_hash).await;
    let view = state.manifest.view();
    let Some(entry) = view.lookup(&path_hash) else {
        state.manifest.note_miss(path_hash);
        return StatusCode::NOT_FOUND.into_response();
    };

    // A needed pack is evicted: miss, so Nix falls through instead of
    // attempting a doomed copy. No access recorded: an unservable path
    // must not join the GC root.
    if !view.available(&path_hash).await {
        return StatusCode::NOT_FOUND.into_response();
    }

    // A narinfo hit is the liveness signal: the accessed path joins this
    // run's GC root at the next drain.
    state.access_log.record(path_hash);

    let body = narinfo_for_entry(&state.store_dir, &entry, hash_str);
    ([(header::CONTENT_TYPE, "text/x-nix-narinfo")], body).into_response()
}

/// nix's `<hash>.ls` from the stored tree and pack indexes; no pack data.
async fn listing(state: &Substituter, hash_str: &str) -> Response {
    let Ok(path_hash) = hash_str.parse::<PathHash>() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    state.hold(&path_hash).await;
    let Ok(Some(r)) = state.manifest.view().resolve(&path_hash).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match crate::listing::listing(&r.entry.tree, |h| {
        r.map.chunks.get(h).map(|c| u64::from(c.uncompressed_size))
    }) {
        Ok(v) => ([(header::CONTENT_TYPE, "application/json")], v.to_string()).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// CA build trace lookup; nix follows up with the narinfo of `outPath`,
/// which records the access.
async fn build_trace(
    State(state): State<Arc<Substituter>>,
    Path((drv, file)): Path<(String, String)>,
) -> Response {
    let _activity = state.touch();
    state.manifest_ready().await;
    let Some(output) = file.strip_suffix(".doi") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(out_path) = state
        .manifest
        .view()
        .snapshot
        .as_ref()
        .and_then(|s| s.realisation(&format!("{drv}^{output}")))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let body = serde_json::json!({"outPath": out_path.to_string(), "signatures": []});
    (
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

#[derive(Deserialize)]
struct NarQuery {
    /// Store path hash, present when the URL came from one of our narinfo
    /// responses (`nar/<narhash>.nar.zst?hash=<pathhash>`).
    hash: Option<String>,
}

async fn nar(
    State(state): State<Arc<Substituter>>,
    Path(file): Path<String>,
    // Result: an unparsable query string must yield the same 404 as every
    // other NAR failure (the module contract Nix relies on to fall through
    // to the next substituter), not axum's 400 extractor rejection.
    query: Result<Query<NarQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let _activity = state.touch();
    let Some(nar_hash_str) = file.strip_suffix(".nar.zst") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(nar_hash) = Hash32::parse_sha256(&format!("sha256:{nar_hash_str}")) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // Resolve the path entry: by ?hash= if present, otherwise via the
    // NAR-hash index.
    let query = match query {
        Ok(Query(query)) => query,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    if let Some(Ok(path_hash)) = query.hash.as_ref().map(|h| h.parse::<PathHash>()) {
        state.hold(&path_hash).await;
    }
    let view = state.manifest.view();
    let path_hash = match &query.hash {
        Some(hash) => match hash.parse::<PathHash>() {
            Ok(path_hash) => path_hash,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        },
        None => match view
            .snapshot
            .as_ref()
            .and_then(|s| s.by_nar_hash(&nar_hash))
        {
            Some(path_hash) => path_hash,
            None => return StatusCode::NOT_FOUND.into_response(),
        },
    };
    if !view.contains(&path_hash) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut manifest_view = view;

    // A NAR download is an access (the GC liveness signal), just like a
    // narinfo hit. Nix caches narinfo lookups locally and may fetch a NAR
    // without re-requesting the narinfo, so recording only narinfo hits
    // would let GC collect paths that are actively being substituted.
    state.access_log.record(path_hash);

    // Fetch all chunks (concurrency-capped inside the fetcher); any
    // failure means 404 (Nix rebuilds or falls through), never partial
    // data. A missing pack gets one retry against a freshly loaded
    // manifest (see [`ManifestReload`]).
    let mut reloaded = false;
    let (resolved, chunks) = loop {
        let attempt = async {
            let Some(resolved) = manifest_view
                .resolve(&path_hash)
                .await?
                .filter(|r| r.entry.nar_hash == nar_hash)
            else {
                return Ok(None);
            };
            let chunks = state
                .fetcher
                .fetch_path_chunks(path_hash, &resolved)
                .await?;
            Ok::<_, FetchError>(Some((resolved, chunks)))
        };
        match attempt.await {
            Ok(Some(done)) => break done,
            Ok(None) => return StatusCode::NOT_FOUND.into_response(),
            Err(err @ (FetchError::PackUnavailable(_) | FetchError::UnknownChunk(_)))
                if !reloaded && state.manifest_reload.is_some() =>
            {
                reloaded = true;
                eprintln!("hestia substituter: {err}; reloading (concurrent gc repack?)");
                (state.manifest_reload.as_ref().expect("checked above"))().await;
                manifest_view = state.manifest.view();
            }
            Err(err) => {
                // Later narinfos for paths in this pack miss upfront.
                if let FetchError::PackUnavailable(pack) = err {
                    manifest_view.mark_pack_missing(pack);
                }
                eprintln!("hestia substituter: cannot serve NAR for {path_hash}: {err}");
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    let refs = RefTable::new(&resolved.entry.references);
    let body =
        tokio::task::spawn_blocking(move || narzstd::stitch(&resolved.entry.tree, &chunks, &refs))
            .await
            .expect("stitch task panicked");
    match body {
        Ok(body) => ([(header::CONTENT_TYPE, "application/x-nix-nar")], body).into_response(),
        Err(err) => {
            eprintln!("hestia substituter: cannot serve NAR for {path_hash}: {err}");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// All chunks referenced by an entry's file tree (deduplicated).
fn entry_chunks(entry: &PathEntry) -> BTreeSet<ChunkHash> {
    flatten_tree(&entry.tree)
        .into_iter()
        .filter_map(|(_, node)| match node {
            FileSystemObject::Regular(regular) => Some(regular.contents.chunks.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The plain NAR for the export stream, which nothing downstream
/// verifies, so its hash is checked here. CPU-bound: run off the runtime.
fn export_nar(entry: &PathEntry, frames: &BTreeMap<ChunkHash, Frame>) -> Result<Vec<u8>, String> {
    let refs = RefTable::new(&entry.references);
    let zst = narzstd::stitch(&entry.tree, frames, &refs).map_err(|e| e.to_string())?;
    let nar = zstd::decode_all(&zst[..]).map_err(|e| e.to_string())?;
    if nar.len() as u64 != entry.nar_size || Hash32::digest(&nar) != entry.nar_hash {
        return Err("NAR does not match its recorded hash".into());
    }
    Ok(nar)
}

// ---------------------------------------------------------------------------
// Closure export (prefetch)
// ---------------------------------------------------------------------------

/// Magic marker between the NAR and the metadata of one exported path
/// (nix's `exportMagic`).
const EXPORT_MAGIC: u64 = 0x4558494e;

/// NAR-byte budget of one closure-export window (one chunk-fetch batch).
/// Large batches turn thousands of latency-bound Range reads into a few
/// big ones; sizing by bytes (not path count) bounds peak memory (frames
/// plus chunks, times the stream lookahead of 2) no matter how the closure
/// splits into tiny and huge paths.
const CLOSURE_EXPORT_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// Split a closure into windows of roughly [`CLOSURE_EXPORT_WINDOW_BYTES`]
/// of NAR data, keeping the closure order (a path bigger than the budget
/// gets its own window).
fn export_windows(order: &[(PathHash, PathEntry)]) -> Vec<Vec<PathHash>> {
    let mut windows = Vec::new();
    let mut window = Vec::new();
    let mut window_bytes = 0u64;
    for (path_hash, entry) in order {
        let path_hash = *path_hash;
        let nar_size = entry.nar_size;
        if !window.is_empty() && window_bytes + nar_size > CLOSURE_EXPORT_WINDOW_BYTES {
            windows.push(std::mem::take(&mut window));
            window_bytes = 0;
        }
        window.push(path_hash);
        window_bytes += nar_size;
    }
    if !window.is_empty() {
        windows.push(window);
    }
    windows
}

/// Append a u64 in Nix wire format (8-byte little endian).
fn export_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Append a string in Nix wire format (length, bytes, zero-padded to 8).
fn export_str(out: &mut Vec<u8>, value: &str) {
    export_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
    out.resize(out.len() + (8 - value.len() % 8) % 8, 0);
}

/// One path framed for the export stream: entry marker, NAR, then path
/// metadata (what `nix-store --import` expects per path).
fn export_frame(store_dir: &StoreDir, entry: &PathEntry, nar: &[u8]) -> Vec<u8> {
    let full_path = |path: &crate::manifest::StorePath| format!("{store_dir}/{path}");
    let mut out = Vec::with_capacity(nar.len() + 512);
    export_u64(&mut out, 1);
    out.extend_from_slice(nar);
    export_u64(&mut out, EXPORT_MAGIC);
    export_str(&mut out, &full_path(&entry.store_path));
    export_u64(&mut out, entry.references.len() as u64);
    for reference in &entry.references {
        export_str(&mut out, &full_path(reference));
    }
    export_str(
        &mut out,
        &entry.deriver.as_ref().map(&full_path).unwrap_or_default(),
    );
    // Legacy signature slot, always empty.
    export_u64(&mut out, 0);
    out
}

/// The closure of `roots` restricted to manifest members, references
/// before referrers (`nix-store --import` registers paths in stream
/// order). References pointing outside the manifest (upstream paths) are
/// skipped. Iterative DFS: drv chains can be deep.
fn closure_order(view: &ManifestView, roots: &[PathHash]) -> Vec<(PathHash, PathEntry)> {
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for &root in roots {
        let mut stack = vec![(root, false)];
        while let Some((hash, children_done)) = stack.pop() {
            let Some(entry) = view.lookup(&hash) else {
                continue;
            };
            if children_done {
                order.push((hash, entry));
                continue;
            }
            if !seen.insert(hash) {
                continue;
            }
            stack.push((hash, true));
            for reference in &entry.references {
                let child = PathHash::from_store_path(reference);
                if child != hash && !seen.contains(&child) {
                    stack.push((child, false));
                }
            }
        }
    }
    order
}

fn closure_roots(view: &ManifestView, hashes: &str) -> Option<Vec<PathHash>> {
    let roots: Vec<PathHash> = hashes
        .split(',')
        .filter(|part| !part.is_empty())
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    (!roots.is_empty() && roots.iter().all(|root| view.contains(root))).then_some(roots)
}

/// Fetch and frame one window of a closure export.
async fn export_window(
    state: &Substituter,
    view: &ManifestView,
    window: &[PathHash],
) -> Result<Vec<u8>, String> {
    let mut entries = Vec::with_capacity(window.len());
    let mut source = ChunkMap::default();
    for hash in window {
        let r = view
            .resolve(hash)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("path vanished")?;
        source.chunks.extend(r.map.chunks);
        source.packs.extend(r.map.packs);
        entries.push((*hash, r.entry));
    }
    let needed: BTreeSet<ChunkHash> = entries
        .iter()
        .flat_map(|(_, entry)| entry_chunks(entry))
        .collect();
    let frames = state
        .fetcher
        .fetch_chunks(&source, needed)
        .await
        .map_err(|err| {
            eprintln!("hestia substituter: closure export chunk fetch failed: {err}");
            err.to_string()
        })?;
    for (path_hash, _) in &entries {
        state.access_log.record(*path_hash);
    }
    let store_dir = state.store_dir.clone();
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        for (path_hash, entry) in entries {
            let nar = export_nar(&entry, &frames).map_err(|err| {
                eprintln!("hestia substituter: closure export failed at {path_hash}: {err}");
                err
            })?;
            out.extend_from_slice(&export_frame(&store_dir, &entry, &nar));
        }
        Ok(out)
    })
    .await
    .expect("export task panicked")
}

async fn closure(State(state): State<Arc<Substituter>>, Path(hashes): Path<String>) -> Response {
    let _activity = state.touch();
    state.manifest_ready().await;

    let view = state.manifest.view();
    // Every requested root must be servable; a partial closure would make
    // the import succeed and the subsequent build fail confusingly.
    let Some(roots) = closure_roots(&view, &hashes) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let order = closure_order(&view, &roots);

    // Stream the closure in windows: each window's chunks are fetched as
    // one batch, and the next window downloads while the current one is
    // being sent. A fetch/assembly failure ends the stream mid-transfer,
    // which fails the client's import (never a silently truncated but
    // well-formed stream).
    use futures_util::StreamExt as _;
    let windows = export_windows(&order);
    let frames = futures_util::stream::iter(windows)
        .map(move |window| {
            let state = state.clone();
            let view = view.clone();
            async move {
                let result = export_window(&state, &view, &window).await;
                result.map(Bytes::from).map_err(std::io::Error::other)
            }
        })
        .buffered(2)
        .chain(futures_util::stream::once(async {
            // End-of-stream marker.
            Ok(Bytes::from_static(&[0u8; 8]))
        }));
    // Stop after the first error: everything behind it (including the end
    // marker) is dropped so the client sees a truncated stream.
    let stream = frames.scan(false, |failed, item| {
        let stop = *failed;
        *failed = *failed || item.is_err();
        futures_util::future::ready((!stop).then_some(item))
    });

    (
        [(header::CONTENT_TYPE, "application/x-nix-export")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

async fn closure_external_references(
    State(state): State<Arc<Substituter>>,
    Path(hashes): Path<String>,
) -> Result<String, StatusCode> {
    let _activity = state.touch();
    state.manifest_ready().await;

    let view = state.manifest.view();
    let roots = closure_roots(&view, &hashes).ok_or(StatusCode::NOT_FOUND)?;
    Ok(closure_order(&view, &roots)
        .iter()
        .flat_map(|(_, entry)| entry.references.iter())
        .filter(|reference| !view.contains(&PathHash::from_store_path(reference)))
        .map(|reference| format!("{}/{reference}", state.store_dir))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkList, FileTree, Regular};

    fn unused_backend() -> Backend {
        let http = reqwest::Client::new();
        let twirp = crate::gha::twirp::TwirpClient::new(http.clone(), "http://unused", "token");
        Backend::Gha(crate::backend::gha::Gha::new(twirp, None, http))
    }

    fn test_path_hash(seed: u8) -> PathHash {
        PathHash(crate::manifest::StorePathHash::new([seed; 20]))
    }

    fn test_entry(seed: u8) -> PathEntry {
        PathEntry {
            store_path: format!("{}-test-{seed}", test_path_hash(seed))
                .parse()
                .unwrap(),
            nar_hash: Hash32::digest([seed]),
            nar_size: 100,
            references: vec![],
            ca: None,
            deriver: None,
            realises: Vec::new(),
            tree: FileTree(FileSystemObject::Regular(Regular {
                executable: false,
                contents: ChunkList::default(),
            })),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn narinfo_waits_for_the_startup_load() {
        let store = ManifestStore::new();
        let (ready_tx, ready_rx) = tokio::sync::watch::channel(false);
        let state = Arc::new(
            Substituter::new(
                StoreDir::default(),
                store,
                AccessLog::new(),
                unused_backend(),
            )
            .with_manifest_ready(ready_rx),
        );

        let request = tokio::spawn(narinfo(
            State(state),
            Path(format!("{}.narinfo", test_path_hash(1))),
        ));
        // The gate is closed: the request must still be pending.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert!(!request.is_finished(), "narinfo answered before the load");

        ready_tx.send(true).unwrap();
        let response = request.await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn export_windows_split_by_nar_bytes() {
        let sizes = [
            CLOSURE_EXPORT_WINDOW_BYTES / 2,
            CLOSURE_EXPORT_WINDOW_BYTES / 2, // fills the first window
            CLOSURE_EXPORT_WINDOW_BYTES * 3, // oversized: its own window
            1,
            1,
        ];
        let entries: Vec<(PathHash, PathEntry)> = sizes
            .iter()
            .enumerate()
            .map(|(seed, &nar_size)| {
                let mut entry = test_entry(seed as u8);
                entry.nar_size = nar_size;
                (test_path_hash(seed as u8), entry)
            })
            .collect();
        let order: Vec<PathHash> = entries.iter().map(|(h, _)| *h).collect();
        let windows = export_windows(&entries);
        assert_eq!(
            windows,
            vec![
                vec![order[0], order[1]],
                vec![order[2]],
                vec![order[3], order[4]],
            ]
        );
    }

    #[test]
    fn plan_pack_reads_coalesces_gaps_and_reads_ahead() {
        let gap = PACK_FETCH_GAP_BYTES;
        let ahead = PACK_READ_AHEAD_BYTES;
        let pack_size = 100 * ahead;
        let plan = |spans: &[(u64, u32)], end| plan_pack_reads(spans, end, |_| ahead);

        // Small gap merges, big gap splits.
        let spans = [(0, 100), (gap + 100, 100), (10 * ahead, 100)];
        let reads = plan(&spans, pack_size);
        assert_eq!(reads, vec![0..ahead, 10 * ahead..11 * ahead]);

        // Read-ahead never runs past the pack end...
        let reads = plan(&[(pack_size - 50, 50)], pack_size);
        assert_eq!(reads, vec![pack_size - 50..pack_size]);

        // ...and never truncates a run that is already larger.
        let reads = plan(&[(0, u32::MAX)], 100);
        assert_eq!(reads, vec![0..u64::from(u32::MAX)]);

        // A run inside the read-ahead of the one before joins its read.
        let reads = plan(&[(0, 100), (2 * gap, 100)], pack_size);
        assert_eq!(reads, vec![0..ahead]);
    }

    #[test]
    fn read_ahead_grows_on_sequential_reads_and_shrinks_on_scattered_ones() {
        let mut reads = PackReads::default();
        let mut at = 0;
        let mut windows = Vec::new();
        for _ in 0..4 {
            let w = reads.window_for(at);
            windows.push(w);
            at += w;
            reads.last_end = Some(at);
        }
        let start = PACK_READ_AHEAD_START;
        assert_eq!(windows, vec![start, 2 * start, 4 * start, 4 * start]);
        assert_eq!(4 * start, PACK_READ_AHEAD_BYTES);

        let far = 1 << 40;
        for i in 1..=8 {
            let w = reads.window_for(i * far);
            reads.last_end = Some(i * far + w);
        }
        assert_eq!(reads.window, PACK_READ_AHEAD_MIN);
    }

    #[test]
    fn chunks_in_range_selects_only_fully_contained_chunks() {
        let e = |offset, seed: u8| crate::segment::PackIndexEntry {
            hash: ChunkHash::digest([seed]),
            offset,
            compressed_size: 100,
            uncompressed_size: 0,
        };
        let index = PackIndex {
            entries: vec![e(0, 0), e(100, 1), e(300, 2), e(350, 3)], // last straddles range end
        };
        let selected: Vec<ChunkHash> = chunks_in_range(&index, &(100..400))
            .map(|e| e.hash)
            .collect();
        assert_eq!(
            selected,
            vec![ChunkHash::digest([1]), ChunkHash::digest([2])]
        );
    }

    #[test]
    fn unused_path_locks_are_pruned() {
        let fetcher = ChunkFetcher::new(unused_backend());
        let held = fetcher.path_lock(test_path_hash(1));
        drop(fetcher.path_lock(test_path_hash(2)));
        // The next call prunes everything no request holds.
        let _other = fetcher.path_lock(test_path_hash(3));
        let locks = fetcher.path_locks.lock().unwrap();
        assert!(locks.contains_key(&test_path_hash(1)), "held lock kept");
        assert!(
            !locks.contains_key(&test_path_hash(2)),
            "released lock pruned"
        );
        drop(held);
    }

    #[test]
    fn chunk_cache_evicts_oldest_when_over_budget() {
        let mut cache = ChunkCache::default();
        // Three chunks of 100 MiB each: the third insert must evict the first.
        let big = Frame {
            zstd: Bytes::from(vec![0u8; 100 * 1024 * 1024]),
            size: 0,
        };
        for seed in 0..3u8 {
            cache.insert(ChunkHash::digest([seed]), big.clone());
        }
        assert!(
            cache.get(&ChunkHash::digest([0])).is_none(),
            "oldest evicted"
        );
        assert!(cache.get(&ChunkHash::digest([2])).is_some(), "newest kept");
        assert!(cache.total <= CHUNK_CACHE_BUDGET);
    }

    #[test]
    fn chunk_cache_hits_refresh_recency() {
        let mut cache = ChunkCache::default();
        let big = Frame {
            zstd: Bytes::from(vec![0u8; 100 * 1024 * 1024]),
            size: 0,
        };
        cache.insert(ChunkHash::digest([0]), big.clone());
        cache.insert(ChunkHash::digest([1]), big.clone());
        assert!(cache.get(&ChunkHash::digest([0])).is_some());
        cache.insert(ChunkHash::digest([2]), big.clone());
        assert!(cache.get(&ChunkHash::digest([0])).is_some(), "hit kept");
        assert!(
            cache.get(&ChunkHash::digest([1])).is_none(),
            "least recently used evicted"
        );
    }

    #[test]
    fn chunk_cache_insert_is_idempotent() {
        let mut cache = ChunkCache::default();
        let data = Frame {
            zstd: Bytes::from_static(b"chunk data"),
            size: 0,
        };
        let hash = ChunkHash::digest(&data.zstd);
        cache.insert(hash, data.clone());
        cache.insert(hash, data.clone());
        assert_eq!(cache.total, data.zstd.len(), "no double counting");
    }

    #[test]
    fn narinfo_text_has_required_fields() {
        let store_dir = StoreDir::default();
        let mut entry = test_entry(7);
        entry.references = vec![
            format!("{}-dep-a", test_path_hash(8)).parse().unwrap(),
            format!("{}-dep-b", test_path_hash(9)).parse().unwrap(),
        ];

        let hash = test_path_hash(7).to_string();
        let text = String::from_utf8(narinfo_for_entry(&store_dir, &entry, &hash)).unwrap();

        assert!(
            text.contains(&format!(
                "StorePath: /nix/store/{}-test-7\n",
                test_path_hash(7)
            )),
            "narinfo:\n{text}"
        );
        assert!(text.contains("Compression: zstd\n"), "narinfo:\n{text}");
        assert!(text.contains("NarSize: 100\n"), "narinfo:\n{text}");
        assert!(
            !text.contains("FileSize:") && !text.contains("FileHash:"),
            "narinfo:\n{text}"
        );
        assert!(text.contains("NarHash: sha256:"), "narinfo:\n{text}");
        assert!(
            text.contains("URL: nar/") && text.contains(&format!(".nar.zst?hash={hash}\n")),
            "narinfo:\n{text}"
        );
        // References: both deps, full basenames.
        assert!(
            text.contains(&format!("{}-dep-a", test_path_hash(8))),
            "narinfo:\n{text}"
        );
        assert!(
            text.contains(&format!("{}-dep-b", test_path_hash(9))),
            "narinfo:\n{text}"
        );
        // No signature lines: hestia serves unsigned (?trusted=true).
        assert!(!text.contains("Sig: "), "narinfo:\n{text}");
    }
}
