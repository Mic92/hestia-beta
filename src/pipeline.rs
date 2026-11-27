//! The write pipeline: store paths → chunks → packs → segment + head.
//!
//! Runs on drain (action post-step or idle-exit). Steps:
//!
//! 1. Query path info from the store database for every buffered path,
//!    expanded to its runtime closure unless disabled.
//! 2. Filter: invalid paths, upstream-signed paths (when the upstream
//!    cache filter is enabled. Derivation closures bypass it unless
//!    explicitly configured otherwise), paths already stored.
//! 3. Chunk each new path (FastCDC over NAR events) and verify the chunked
//!    representation reproduces the NAR hash recorded by Nix.
//! 4. Pack new chunks, upload each pack with its index.
//! 5. Publish everything this job pushed, found stored or substituted as
//!    one segment plus head under this root.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::spawn_blocking;

use crate::backend::Backend;
use crate::chunker::{
    self, Chunk, MAX_CHUNK_SIZE, Pack, PackBuilder, compress_chunks, ingest_path,
};
use crate::gha::Error as GhaError;
use crate::manifest::{ChunkHash, ChunkList, FileTree, NarHash, PathEntry, PathHash};
use crate::node::NodeClient;
use crate::pathinfo::{Error as PathInfoError, Lookup, PathInfo, StoreDatabase};
use crate::protocol::DrainStats;
use crate::refnorm::RefTable;
use crate::segment::{Meta, Sealed, SegmentWriter};
use crate::shard::Shard;
use crate::store::{self, Snapshot};
use crate::substituter::ManifestStore;
use crate::trust::Trust;
use crate::upstream::UpstreamFilter;

/// Compressed bytes per pack before a new pack is started.
pub const PACK_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// How many packs upload concurrently during a drain.
const UPLOAD_CONCURRENCY: usize = 4;

/// Upper bound on paths chunked and NAR-verified concurrently; the actual
/// width is capped at the CPU count.
const CHUNK_CONCURRENCY: usize = 32;

/// Budget for bytes in flight: files being read and chunked, and chunk
/// batches from compression until packed.
const CHUNK_INFLIGHT_NAR_BYTES: u64 = 512 * 1024 * 1024;
/// A file's new chunks go to the compressor in batches of at most this.
const COMPRESS_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Clamped so a file larger than the budget still runs (alone).
fn chunk_permits(size: u64) -> u32 {
    size.clamp(1, CHUNK_INFLIGHT_NAR_BYTES) as u32
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("chunking error: {0}")]
    Chunker(#[from] chunker::Error),

    #[error("store database error: {0}")]
    PathInfo(#[from] PathInfoError),

    #[error(transparent)]
    Store(#[from] store::Error),

    #[error(transparent)]
    Node(#[from] crate::node::Error),

    #[error("the root was collected since this drain was planned; drain again")]
    StalePlan,
}

/// Shared record of paths served through the substituter.
///
/// narinfo hits double as the liveness signal: an accessed path joins this
/// run's root even though it was not rebuilt, which keeps it (and its
/// closure) alive across GC. The substituter records hits; the pipeline
/// reads a snapshot at drain time.
///
/// Cloning is cheap (shared state): the daemon hands one clone to the
/// substituter and keeps one for drains.
#[derive(Debug, Default, Clone)]
pub struct AccessLog {
    inner: Arc<Mutex<BTreeSet<PathHash>>>,
}

impl AccessLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a path was served (or asked for and found).
    pub fn record(&self, hash: PathHash) {
        self.inner
            .lock()
            .expect("access log lock poisoned")
            .insert(hash);
    }

    /// All paths accessed so far.
    pub fn snapshot(&self) -> BTreeSet<PathHash> {
        self.inner.lock().expect("access log lock poisoned").clone()
    }

    /// The paths accessed since the last call. A node that never exits
    /// names each access once, in the next segment it publishes.
    pub fn take(&self) -> BTreeSet<PathHash> {
        std::mem::take(&mut *self.inner.lock().expect("access log lock poisoned"))
    }

    pub fn extend(&self, hashes: impl IntoIterator<Item = PathHash>) {
        self.inner
            .lock()
            .expect("access log lock poisoned")
            .extend(hashes);
    }
}

/// The Nix system string for the machine hestia runs on
/// (`x86_64-linux`, `aarch64-darwin`, …).
pub fn current_system() -> String {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        os => os,
    };
    // Rust arch names diverge from Nix system spellings on some platforms;
    // the value defaults the root key, so an unmapped spelling
    // fragments (or collides) GC roots against jobs passing --system with
    // the Nix spelling.
    let arch = match std::env::consts::ARCH {
        "x86" => "i686",
        // Rust reports "arm" for all 32-bit ARM; armv7l is the common
        // case. armv6l hosts must pass --system explicitly.
        "arm" => "armv7l",
        arch => arch,
    };
    format!("{arch}-{os}")
}

/// Root key for a branch + system pair, e.g. `main-x86_64-linux`.
pub fn root_key(branch: &str, system: &str) -> String {
    format!("{branch}-{system}")
}

/// Current unix time in seconds.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

/// Upload one pack. `false` when the backend already had it. Pack keys
/// are content-addressed, so an existing entry holds identical content.
/// That case refreshes the existing pack so its LRU clock and GC's age
/// guard see this writer's dependency before the head lands.
pub async fn upload_pack(backend: &Backend, pack: &Pack) -> Result<bool, GhaError> {
    let key = pack.cache_key();
    let created = backend.put(&key, pack.data.clone()).await?;
    if !created {
        backend.refresh(&key, pack.data.clone()).await?;
    }
    Ok(created)
}

/// Everything the pipeline needs to talk to the world.
pub struct PipelineContext {
    pub backend: Backend,
    pub trust: Trust,
    pub store: StoreDatabase,
    pub upstream: UpstreamFilter,
    /// Expand hooked paths to their runtime closure before pushing.
    /// Substituted dependencies never trigger the post-build-hook, so
    /// without expansion they are never cached.
    pub expand_closure: bool,
    /// Apply the upstream filter to derivation closure members instead of
    /// keeping those closures self-contained.
    pub filter_drv_closures: bool,
    /// Root key, e.g. `main-x86_64-linux`.
    pub root_key: String,
    /// The store keeps the root in shards ([`crate::shard`]): a drain
    /// publishes one segment for every shard it has paths of.
    pub sharded: bool,
    /// Compressed bytes per pack ([`PACK_TARGET_SIZE`] in production; tests
    /// use small values to exercise pack splitting).
    pub pack_target_size: u64,
    /// The write pipeline is skipped so a drain is a clean no-op. Set by
    /// `serve --read-only`, or by a background probe at startup
    /// ([`crate::serve`]) when the runtime token has no writable cache
    /// scope (`check_run`, fork `pull_request`) and the first reservation
    /// would fail anyway.
    pub read_only: Arc<AtomicBool>,
    /// Where the published segment is handed to the substituter, so the
    /// paths this drain pushed are served even while listings lag.
    pub publish: Option<ManifestStore>,
    /// Unix seconds for head names.
    pub clock: Clock,
    /// Drain through this cache node: it says what is stored and publishes
    /// the segment, so no index is loaded here ([`crate::node`]).
    pub node: Option<NodeClient>,
}

pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(now_unix)
}

/// A path that chunked and passed NAR verification.
struct ReadyPath {
    info: PathInfo,
    tree: FileTree<ChunkList>,
    nar_hash: NarHash,
    nar_size: u64,
    elapsed: Duration,
}

/// Result of the concurrent chunk-and-verify stage for one path.
enum Verified {
    // Boxed: far larger than the failure variants.
    Ready(Box<ReadyPath>),
    ChunkFailed,
    VerifyFailed,
}

/// Ends a path's ingest early when the packer is gone.
enum IngestError {
    Chunk(chunker::Error),
    PackerGone,
}

impl From<chunker::Error> for IngestError {
    fn from(e: chunker::Error) -> Self {
        IngestError::Chunk(e)
    }
}

impl PipelineContext {
    /// Run the write pipeline.
    ///
    /// `paths`: absolute store paths buffered from hooks.
    /// `accessed`: path hashes recorded by the substituter ([`AccessLog`]).
    pub async fn run(
        &self,
        paths: BTreeSet<String>,
        accessed: BTreeSet<PathHash>,
    ) -> Result<DrainStats, Error> {
        let mut stats = DrainStats {
            paths_received: paths.len(),
            ..DrainStats::default()
        };

        if paths.is_empty() && accessed.is_empty() {
            return Ok(stats);
        }

        if self.read_only.load(Ordering::Relaxed) {
            return Ok(stats);
        }

        let load_started = Instant::now();
        // Blocking sqlite I/O happens off the async runtime.
        let store = self.store.clone();
        let expand_closure = self.expand_closure;
        let filter_drv_closures = self.filter_drv_closures;
        let (lookups, upstream_filter_bypass) = spawn_blocking(move || {
            let bypass_roots: BTreeSet<String> = if expand_closure && !filter_drv_closures {
                paths
                    .iter()
                    .filter(|path| path.ends_with(".drv"))
                    .cloned()
                    .collect()
            } else {
                BTreeSet::new()
            };
            let lookups = if expand_closure {
                store.query_closure(paths)?
            } else {
                store.query_batch(paths)?
            };
            let bypass: BTreeSet<String> = store
                .query_closure(bypass_roots)?
                .into_iter()
                .map(|(path, _)| path)
                .collect();
            Ok::<_, PathInfoError>((lookups, bypass))
        })
        .await
        .expect("store database query task panicked")?;

        let root_paths: BTreeSet<PathHash> = accessed;
        let mut candidates: Vec<(String, PathInfo)> = Vec::new();

        for (path, lookup) in lookups {
            let info = match lookup {
                Lookup::Found(info) => *info,
                Lookup::Unknown => {
                    eprintln!("hestia: skipping {path}: not a valid path in the local store");
                    stats.skipped_invalid += 1;
                    continue;
                }
                Lookup::Malformed { reason } => {
                    eprintln!("hestia: skipping {path}: {reason}");
                    stats.skipped_invalid += 1;
                    continue;
                }
            };

            if !upstream_filter_bypass.contains(&path)
                && self.upstream.is_upstream_signed(&info.signatures)
            {
                stats.skipped_upstream += 1;
                continue;
            }

            candidates.push((path, info));
        }

        let mut parts: BTreeMap<Option<Shard>, Part> = BTreeMap::new();
        let shard_of = |hash: &PathHash| self.sharded.then(|| Shard::of(hash));
        for candidate in candidates {
            let shard = shard_of(&candidate.1.path_hash());
            let part = parts.entry(shard).or_insert_with(|| Part::new(shard));
            part.candidates.push(candidate);
        }
        for hash in root_paths {
            let shard = shard_of(&hash);
            let part = parts.entry(shard).or_insert_with(|| Part::new(shard));
            part.root_paths.insert(hash);
        }
        let query_ms = load_started.elapsed().as_millis() as u64;
        // One budget for all parts: together they hold no more of the
        // NARs in memory than a drain under one root does.
        let inflight = Arc::new(Semaphore::new(CHUNK_INFLIGHT_NAR_BYTES as usize));
        // Without a node this drain makes the next snapshot itself: the
        // heads are listed once and every part is published in one round.
        let _updating = match (&self.node, &self.publish) {
            (None, Some(publish)) => Some(publish.updating().await),
            _ => None,
        };
        let snapshot = match &self.node {
            Some(_) => None,
            None => {
                let roots: Vec<String> = parts.keys().map(|s| self.root_of(*s)).collect();
                let listed = self.relist(&roots).await?;
                stats.load_ms += load_started.elapsed().as_millis() as u64 - query_ms;
                Some(listed)
            }
        };
        let done: Vec<Result<_, Error>> = stream::iter(parts.into_values())
            .map(|part| self.drain_part(part, inflight.clone(), snapshot.clone()))
            .buffer_unordered(PART_CONCURRENCY)
            .collect()
            .await;
        // A part that failed fails the drain. Its paths are handed in
        // again, and what the other parts published is then stored.
        let mut failed = None;
        let mut sealed = Vec::new();
        for part in done {
            match part {
                Ok((part, segment)) => {
                    absorb(&mut stats, part);
                    sealed.extend(segment);
                }
                Err(err) => failed = Some(err),
            }
        }
        if let (Some(snapshot), false) = (&snapshot, sealed.is_empty()) {
            let commit_started = Instant::now();
            stats.head = self.publish_parts(snapshot, sealed).await?.pop();
            stats.commit_ms = commit_started.elapsed().as_millis() as u64;
        }
        stats.load_ms += query_ms;
        match failed {
            Some(err) => Err(err),
            None => Ok(stats),
        }
    }

    /// The root `shard` is published under.
    fn root_of(&self, shard: Option<Shard>) -> String {
        shard.map_or_else(|| self.root_key.clone(), |s| s.root(&self.root_key))
    }

    /// The part of a drain that goes under one root: all of it, or in a
    /// sharded store the paths of one shard.
    ///
    /// Through a node the part is committed there. Without one it is
    /// made against `snapshot`, and its segment comes back to be
    /// published with those of the other parts.
    async fn drain_part(
        &self,
        part: Part,
        inflight: Arc<Semaphore>,
        snapshot: Option<Arc<Snapshot>>,
    ) -> Result<(DrainStats, Option<(String, Sealed)>), Error> {
        let Part {
            shard,
            candidates,
            mut root_paths,
        } = part;
        let mut stats = DrainStats::default();
        let load_started = Instant::now();
        let root = self.root_of(shard);
        let node = self.node.as_ref().map(|n| n.for_shard(shard));
        let node = node.as_ref();

        // What is stored already, and where chunks worth reusing lie: a
        // rebuild mostly repeats the chunks of the previous build under
        // the same name, so the indexes of those packs are the ones wanted.
        let mut plan = None;
        let mut present: BTreeSet<PathHash> = BTreeSet::new();
        match (node, &snapshot) {
            (Some(node), _) => {
                let asked = candidates
                    .iter()
                    .map(|(_, i)| (i.path_hash(), i.store_path.name().to_string()))
                    .collect();
                let answer = node.plan(asked).await?;
                present.extend(answer.present.iter().copied());
                plan = Some(answer);
            }
            (None, Some(snapshot)) => {
                for (_, info) in &candidates {
                    let hash = info.path_hash();
                    // A path whose pack the substituter saw evicted goes up
                    // again.
                    let cached = match &self.publish {
                        Some(p) => p.available(&hash).await,
                        None => snapshot.contains(&hash),
                    };
                    if cached {
                        present.insert(hash);
                    }
                }
            }
            (None, None) => unreachable!("a drain without a node lists the root"),
        }
        // Paths that need chunking + upload.
        let (stored, to_push): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, info)| present.contains(&info.path_hash()));
        stats.skipped_existing += stored.len();
        root_paths.extend(stored.iter().map(|(_, info)| info.path_hash()));
        let known_chunks = match (&plan, &snapshot) {
            (Some(plan), _) => plan.known_chunks(),
            (None, Some(snapshot)) => {
                let names = to_push
                    .iter()
                    .map(|(_, i)| i.store_path.name().as_ref())
                    .collect();
                snapshot.load_indexes_for(&names).await?;
                snapshot.known_chunks()
            }
            (None, None) => unreachable!(),
        };
        let known_chunks = Arc::new(known_chunks);

        stats.load_ms = load_started.elapsed().as_millis() as u64;

        // Three stages joined below, each feeding the next over a bounded
        // channel: prepare (chunk + verify concurrently, then dedup),
        // pack (compress concurrently, then seal packs), upload. The
        // CPU-heavy chunk/verify and compress steps run across cores; the
        // dedup and packing glue is serial but cheap.
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(CHUNK_CONCURRENCY);
        let (chunks_tx, chunks_rx) =
            mpsc::channel::<(Vec<Chunk>, OwnedSemaphorePermit)>(concurrency);
        let (pack_tx, pack_rx) = mpsc::channel::<Pack>(2);

        let prepare = async {
            let mut prepared: Vec<PathEntry> = Vec::new();
            // Summed as a Duration, converted once: per-path as_millis()
            // truncation would underreport drains of many small paths.
            let mut chunk_time = Duration::ZERO;
            let mut failed_chunking = 0usize;
            let mut failed_verification = 0usize;
            // Chunks already emitted for this batch (cross-path dedup).
            let batch_chunks: Arc<Mutex<BTreeSet<ChunkHash>>> = Arc::default();

            // Several paths at once fill the cores; each file's new chunks
            // leave for the compressor as soon as they are cut. Failures are
            // skipped, not propagated: a pipeline error would re-buffer the
            // whole batch, and a deterministic one would then block every
            // later drain. A path failing verification has already fed
            // chunks into packs: dead weight, never referenced.
            let shared = (inflight, known_chunks.clone(), batch_chunks, chunks_tx);
            let mut verified = stream::iter(to_push)
                .map(move |(path, info)| {
                    let (inflight, known_chunks, batch_chunks, chunks_tx) = shared.clone();
                    tokio::spawn(async move {
                        let started = Instant::now();
                        // The path's own references drive both normalization
                        // (so chunks stay stable across dependency-hash
                        // changes) and the read-side restore.
                        let refs = RefTable::new(&info.references);
                        let reserve = |size: u64| {
                            let inflight = inflight.clone();
                            async move {
                                inflight
                                    .acquire_many_owned(chunk_permits(size))
                                    .await
                                    .expect("in-flight byte semaphore is never closed")
                            }
                        };
                        let emit = |file_chunks: Vec<Chunk>, reading: OwnedSemaphorePermit| {
                            let (inflight, known_chunks, batch_chunks, chunks_tx) =
                                (&inflight, &known_chunks, &batch_chunks, &chunks_tx);
                            async move {
                                let mut new: Vec<Chunk> = {
                                    let mut batch = batch_chunks.lock().unwrap();
                                    file_chunks
                                        .into_iter()
                                        .filter(|c| {
                                            !known_chunks.contains(&c.hash) && batch.insert(c.hash)
                                        })
                                        .collect()
                                };
                                // Batches take their own share until packed; a
                                // budget-sized file must not starve them.
                                drop(reading);
                                while !new.is_empty() {
                                    let mut bytes = 0;
                                    let n = new
                                        .iter()
                                        .take_while(|c| {
                                            bytes += c.data.len();
                                            bytes <= COMPRESS_BATCH_BYTES
                                        })
                                        .count()
                                        .max(1);
                                    let rest = new.split_off(n);
                                    let bytes: usize = new.iter().map(|c| c.data.len()).sum();
                                    let permit = inflight
                                        .clone()
                                        .acquire_many_owned(chunk_permits(bytes as u64))
                                        .await
                                        .expect("in-flight byte semaphore is never closed");
                                    chunks_tx
                                        .send((new, permit))
                                        .await
                                        .map_err(|_| IngestError::PackerGone)?;
                                    new = rest;
                                }
                                Ok(())
                            }
                        };
                        let ingested = match ingest_path(&path, &refs, reserve, emit).await {
                            Ok(ingested) => ingested,
                            Err(IngestError::PackerGone) => return Verified::ChunkFailed,
                            Err(IngestError::Chunk(err)) => {
                                eprintln!("hestia: NOT uploading {path}: chunking failed: {err}");
                                return Verified::ChunkFailed;
                            }
                        };
                        // Integrity gate: the chunked representation must
                        // reproduce the NAR hash Nix recorded.
                        let nar_hash = ingested.nar_hash;
                        if nar_hash != info.nar_hash || ingested.nar_size != info.nar_size {
                            eprintln!(
                                "hestia: NOT uploading {path}: chunked NAR hash {nar_hash} (size \
                                 {}) does not match the store's record {} (size {}); \
                                 this indicates a chunker bug or store corruption",
                                ingested.nar_size, info.nar_hash, info.nar_size
                            );
                            return Verified::VerifyFailed;
                        }
                        Verified::Ready(Box::new(ReadyPath {
                            nar_size: ingested.nar_size,
                            tree: ingested.tree,
                            info,
                            nar_hash,
                            elapsed: started.elapsed(),
                        }))
                    })
                })
                .buffer_unordered(concurrency);

            while let Some(joined) = verified.next().await {
                let ready = match joined.expect("chunk task panicked") {
                    Verified::Ready(ready) => ready,
                    Verified::ChunkFailed => {
                        failed_chunking += 1;
                        continue;
                    }
                    Verified::VerifyFailed => {
                        failed_verification += 1;
                        continue;
                    }
                };
                chunk_time += ready.elapsed;
                prepared.push(PathEntry {
                    // Verbatim, including any self-reference: this list
                    // becomes the narinfo References line, and stripping
                    // self would diverge substituted clients' store
                    // metadata from the builder's.
                    references: ready.info.references,
                    store_path: ready.info.store_path,
                    nar_hash: ready.nar_hash,
                    nar_size: ready.nar_size,
                    ca: ready.info.ca,
                    deriver: ready.info.deriver,
                    realises: ready.info.realises,
                    tree: ready.tree,
                });
            }
            Ok::<_, Error>((prepared, chunk_time, failed_chunking, failed_verification))
        };

        let pack = async {
            let mut pack_time = Duration::ZERO;
            let new_builder = || {
                PackBuilder::with_capacity(self.pack_target_size as usize + MAX_CHUNK_SIZE as usize)
            };
            let mut builder = new_builder();
            // Compress paths' new-chunk sets concurrently; frames arrive out
            // of order, which is fine -- packs are content-addressed.
            let chunk_stream = stream::unfold(chunks_rx, |mut rx| async move {
                rx.recv().await.map(|chunks| (chunks, rx))
            });
            let compressed = chunk_stream
                .map(|(new_chunks, permit)| {
                    spawn_blocking(move || Ok::<_, Error>((compress_chunks(new_chunks)?, permit)))
                })
                .buffer_unordered(concurrency);
            tokio::pin!(compressed);

            'pack: while let Some(joined) = compressed.next().await {
                // The permit goes once the frames sit in a pack buffer.
                let (frames, _permit) = joined.expect("compression task panicked")?;
                let mut pack_started = Instant::now();
                for frame in frames {
                    builder.add_compressed(frame.hash, &frame.frame, frame.uncompressed_size);
                    if builder.compressed_size() >= self.pack_target_size {
                        let sealed = std::mem::replace(&mut builder, new_builder()).finish();
                        // Pause the pack timer across the send: a full
                        // channel blocks on upload backpressure, which must
                        // not be booked as packing time.
                        pack_time += pack_started.elapsed();
                        if pack_tx.send(sealed).await.is_err() {
                            break 'pack;
                        }
                        pack_started = Instant::now();
                    }
                }
                pack_time += pack_started.elapsed();
            }
            if !builder.is_empty() {
                let _ = pack_tx.send(builder.finish()).await;
            }
            // pack_tx drops here, ending the uploader's stream.
            drop(pack_tx);
            Ok::<_, Error>(pack_time)
        };

        let upload_started = Instant::now();
        let consumer = async {
            let pack_stream = stream::unfold(pack_rx, |mut rx| async move {
                rx.recv().await.map(|pack| (pack, rx))
            });
            pack_stream
                .map(|mut pack| async move {
                    let uploaded = match node {
                        Some(node) => node.put_pack(&pack.cache_key(), pack.data.clone()).await?,
                        None => upload_pack(&self.backend, &pack).await?,
                    };
                    // Only metadata is read after upload; dropping the blob
                    // here keeps peak memory bounded by the in-flight packs
                    // instead of growing with the drain's total compressed
                    // size.
                    let size = pack.data.len() as u64;
                    pack.data = Bytes::new();
                    Ok::<_, Error>((uploaded, size, pack))
                })
                .buffer_unordered(UPLOAD_CONCURRENCY)
                .try_collect::<Vec<(bool, u64, Pack)>>()
                .await
        };

        let ((prepared, chunk_time, failed_chunking, failed_verification), pack_time, uploads) =
            tokio::try_join!(prepare, pack, consumer)?;
        stats.failed_chunking += failed_chunking;
        stats.failed_verification += failed_verification;
        stats.chunk_ms = chunk_time.as_millis() as u64;
        stats.pack_ms = pack_time.as_millis() as u64;
        // Stage times overlap now: chunk/pack are producer busy times,
        // upload is the wall time of the whole pipelined section.
        stats.upload_ms = upload_started.elapsed().as_millis() as u64;

        let mut known_chunks = Arc::into_inner(known_chunks).expect("prepare stage done");
        for (uploaded, size, pack) in uploads {
            if uploaded {
                stats.packs_uploaded += 1;
                stats.bytes_uploaded += size;
            }
            stats.new_chunks += pack.chunks.len();
            known_chunks.add(pack.hash, &pack.index());
        }

        // The segment is this drain's whole claim on the root: entries for
        // what it pushed, and the hashes of stored and substituted paths
        // it used. GC keeps what drains since its last run pushed or used
        // and drops the rest of the root.
        let mut writer = SegmentWriter::default();
        for entry in &prepared {
            store::push_entry(&mut writer, entry, &known_chunks)
                .expect("every chunk is either known or in a pack of this drain");
        }
        stats.pushed = prepared.len();
        for hash in &root_paths {
            // A node filters what it does not store when it commits.
            if !writer.contains(hash) && snapshot.as_ref().is_none_or(|s| s.contains(hash)) {
                writer.use_path(hash);
            }
        }
        if writer.is_empty() {
            return Ok((stats, None));
        }
        let sealed = writer.seal().map_err(store::Error::from)?;
        let (Some(node), Some(plan)) = (node, plan) else {
            return Ok((stats, Some((root, sealed))));
        };
        let commit_started = Instant::now();
        stats.head = Some(node.commit(&plan, &sealed).await?);
        stats.commit_ms = commit_started.elapsed().as_millis() as u64;
        Ok((stats, None))
    }

    /// List the heads again and serve `roots` on top of what is served.
    /// A GC since the last drain may have retired segments the served
    /// snapshot still names; the bodies of those still live are reused.
    async fn relist(&self, roots: &[String]) -> Result<Arc<Snapshot>, Error> {
        let previous = self.publish.as_ref().and_then(ManifestStore::snapshot);
        let mut all = previous.as_ref().map_or_else(Vec::new, |p| p.roots.clone());
        for root in roots {
            if !all.contains(root) {
                all.push(root.clone());
            }
        }
        Ok(Arc::new(
            Snapshot::load(
                self.backend.clone(),
                self.trust.clone(),
                &all,
                previous.as_deref(),
            )
            .await?,
        ))
    }

    /// As a cache node: publish one segment per shard for the drains of
    /// several builders, each sealed from a plan of GC epoch `.0` for
    /// the shard `.1` (`None` in a store without shards). `accessed` are
    /// paths of those shards the node served since it last published.
    /// Returns the head of each shard.
    pub async fn commit_group(
        &self,
        parts: &[(u64, Option<Shard>, Sealed)],
        accessed: &BTreeSet<PathHash>,
    ) -> Result<BTreeMap<Option<Shard>, String>, Error> {
        let _updating = match &self.publish {
            Some(publish) => Some(publish.updating().await),
            None => None,
        };
        let mut shards: BTreeMap<Option<Shard>, Vec<Sealed>> = BTreeMap::new();
        for (_, shard, sealed) in parts {
            shards.entry(*shard).or_default().push(sealed.clone());
        }
        let roots: Vec<String> = shards.keys().map(|s| self.root_of(*s)).collect();
        let snapshot = self.relist(&roots).await?;
        if parts
            .iter()
            .any(|(epoch, ..)| *epoch != snapshot.view.epoch)
        {
            return Err(Error::StalePlan);
        }
        let mut merged = Vec::new();
        for (shard, sealed) in &shards {
            let used = accessed
                .iter()
                .filter(|h| self.sharded.then(|| Shard::of(h)) == *shard)
                .filter(|h| snapshot.contains(h))
                .copied()
                .collect();
            let stored = self.stored_differently(&snapshot, sealed).await?;
            let sealed = store::merge_sealed(sealed, &used, &stored)?;
            merged.push((self.root_of(*shard), sealed));
        }
        let heads = self.publish_parts(&snapshot, merged).await?;
        Ok(shards.into_keys().zip(heads).collect())
    }

    /// Paths of `parts` that the store serves with another NAR hash: a
    /// derivation that is not reproducible, built by two builders that
    /// both planned before either committed. The first stays, because
    /// narinfos naming it are out, and nix keeps them for weeks.
    async fn stored_differently(
        &self,
        snapshot: &Snapshot,
        parts: &[Sealed],
    ) -> Result<BTreeSet<PathHash>, Error> {
        let mut stored = BTreeSet::new();
        for part in parts {
            let meta = Meta::open(&part.meta).map_err(store::Error::from)?;
            for i in 0..meta.len() {
                let hash = meta.hash(i);
                let differs = snapshot
                    .nar_hash_of(&hash)
                    .is_some_and(|nar| nar != meta.body(i).nar_hash);
                // A path whose pack is gone is pushed again to replace it.
                let lost = match &self.publish {
                    Some(publish) => publish.lost(&hash).await,
                    None => false,
                };
                if differs && !lost {
                    stored.insert(hash);
                }
            }
        }
        Ok(stored)
    }

    /// Publish each of `parts` under its root, serve them, and compact
    /// the roots that are due. The heads, in the order of `parts`.
    async fn publish_parts(
        &self,
        snapshot: &Snapshot,
        parts: Vec<(String, Sealed)>,
    ) -> Result<Vec<String>, Error> {
        let now = (self.clock)();
        let heads = futures_util::future::try_join_all(parts.iter().map(|(root, sealed)| {
            store::publish(
                &self.backend,
                &self.trust,
                &snapshot.view,
                root,
                sealed,
                now,
            )
        }))
        .await?;
        // Readers that cannot list need the head index to name the heads.
        if let Err(err) = self.backend.flush().await {
            eprintln!("hestia: cannot update the head index: {err}");
        }
        let sealed: Vec<&Sealed> = parts.iter().map(|(_, s)| s).collect();
        let next = match snapshot.refresh_with(&sealed).await {
            Ok(next) => next,
            Err(err) => {
                eprintln!("hestia: cannot refresh the served segments: {err}");
                return Ok(heads);
            }
        };
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let coin = f64::from(nanos) / 1e9;
        let compacting = &next;
        let compacted = join_all(parts.iter().map(|(root, _)| async move {
            (root, compacting.maybe_compact(root, now, coin).await)
        }))
        .await;
        let mut any = false;
        for (root, outcome) in compacted {
            match outcome {
                Ok(Some(name)) => {
                    eprintln!("hestia: compacted {root} into {name}");
                    any = true;
                }
                Ok(None) => {}
                Err(err) => eprintln!("hestia: compaction of {root} skipped: {err}"),
            }
        }
        if any && let Err(err) = self.backend.flush().await {
            eprintln!("hestia: cannot update the head index: {err}");
        }
        if let Some(publish) = &self.publish {
            publish.set_snapshot(Arc::new(next));
        }
        Ok(heads)
    }
}

/// What a drain has for one root.
struct Part {
    shard: Option<Shard>,
    candidates: Vec<(String, PathInfo)>,
    /// Stored and substituted paths the drain used.
    root_paths: BTreeSet<PathHash>,
}

impl Part {
    fn new(shard: Option<Shard>) -> Part {
        Part {
            shard,
            candidates: Vec::new(),
            root_paths: BTreeSet::new(),
        }
    }
}

/// Parts of a drain that run at once. A node publishes what arrives
/// within its commit interval as one round, so more at once means
/// fewer rounds to wait for.
const PART_CONCURRENCY: usize = 8;

/// Add what one part of a drain did to the drain's totals. The parts
/// run at once, so the longest stage time stands for all.
fn absorb(total: &mut DrainStats, part: DrainStats) {
    total.skipped_existing += part.skipped_existing;
    total.failed_verification += part.failed_verification;
    total.failed_chunking += part.failed_chunking;
    total.pushed += part.pushed;
    total.new_chunks += part.new_chunks;
    total.packs_uploaded += part.packs_uploaded;
    total.bytes_uploaded += part.bytes_uploaded;
    total.head = part.head.or(total.head.take());
    total.load_ms = total.load_ms.max(part.load_ms);
    total.chunk_ms = total.chunk_ms.max(part.chunk_ms);
    total.pack_ms = total.pack_ms.max(part.pack_ms);
    total.upload_ms = total.upload_ms.max(part.upload_ms);
    total.commit_ms = total.commit_ms.max(part.commit_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_system_matches_nix_convention() {
        // Assert the arch-os shape rather than enumerating blessed values:
        // the function must work on any host the binary is built for.
        let system = current_system();
        let (arch, os) = system.split_once('-').expect("system has arch-os form");
        assert!(!arch.is_empty() && !os.is_empty(), "system: {system}");
        assert!(!["x86", "arm", "macos"].contains(&arch), "arch: {arch}");
        assert_ne!(os, "macos", "os must use the Nix spelling");
    }

    #[test]
    fn chunk_permits_clamp_to_the_budget() {
        assert_eq!(chunk_permits(0), 1);
        assert_eq!(chunk_permits(4096), 4096);
        // A path bigger than the whole budget must still get permits it can
        // actually acquire (it runs alone).
        assert_eq!(u64::from(chunk_permits(u64::MAX)), CHUNK_INFLIGHT_NAR_BYTES);
    }

    #[test]
    fn root_key_layout() {
        assert_eq!(root_key("main", "x86_64-linux"), "main-x86_64-linux");
        assert_eq!(
            root_key("feature/foo", "aarch64-darwin"),
            "feature/foo-aarch64-darwin"
        );
    }

    #[test]
    fn access_log_is_shared_between_clones() {
        let log = AccessLog::new();
        let clone = log.clone();
        assert!(log.snapshot().is_empty());

        let hash: PathHash = "00000000000000000000000000000000"
            .parse()
            .expect("valid path hash");
        clone.record(hash);

        assert_eq!(log.snapshot(), BTreeSet::from([hash]));
        // Recording the same hash twice is idempotent.
        log.record(hash);
        assert_eq!(log.snapshot().len(), 1);
    }
}
