//! A build cluster on one root: what does a build step pay for the cache
//! today, and what would it pay with a sharded index that is read lazily,
//! dedup found by sampled chunk hashes, and cache nodes next to the builders?
//!
//!   cargo run --release --example scale_sim
//!   cargo run --release --example scale_sim -- --roots /nix/store/a-nixos-system-…,/nix/store/b-nixos-system-…
//!   cargo run --release --example scale_sim -- --fine [--shards 65536] [--window-days 3]
//!
//! `--fine` prices a derivation per compiled object instead of one per
//! package: short steps, sixteen at once on a machine, one small output.
//! Its sizes are assumed, not traced.
//!
//! Two parts.
//!
//! The trace (`--roots`, successive revisions of one closure, oldest first)
//! chunks real store paths with hestia's chunker and replays their upload
//! one job per `--paths-per-job` paths under each way of finding duplicates:
//!
//!   job only   no index, a job dedups against itself
//!   names      today: load the pack indexes behind stored paths of the same name
//!   1/N        look up the first chunk of every path and every chunk whose
//!              hash is 0 mod N in a global index of such chunks; a hit loads
//!              that pack's index
//!   exact      a global index of every chunk (the bound)
//!
//! The model then prices one build step (narinfo lookups, a few substituted
//! inputs, 1.3 outputs) for clusters of 10, 100 and 500 machines:
//!
//!   today          a daemon per step: `Heads::load`, the base `.tree` whole
//!                  on the first substitution, pack indexes for offsets, a
//!                  reload at the drain, name hints, four objects per drain
//!   today, kept    a daemon per machine that reloads between steps and
//!                  fetches the base again only after GC rewrote it
//!   shards         a daemon per step: 256 shards per root and kind in three
//!                  levels (merged every minute, hour and day), a key table
//!                  range-read on the first lookup that lands in a shard,
//!                  bodies carry the pack offsets, sampled dedup, a pack and
//!                  a run per drain
//!   cache node     the same index held in memory by nodes in the builders'
//!                  datacenter, which also hold recent packs and write to the
//!                  bucket behind the step's back
//!
//! `shards` has every step write its own run. Two more rows put one
//! coordinator in the builders' datacenter between the steps and the index:
//!
//!   coordinator    steps send it their rows; it writes them to the bucket in
//!                  group commits and acknowledges once they are there. A
//!                  step asks it for the rows no level holds yet and reads
//!                  the levels from the bucket as before
//!   coord. serves  it also holds every key table in memory and answers
//!                  lookups and sampled-chunk queries; chunks and pack
//!                  indexes still come from the bucket
//!
//! The group commit is simulated (Poisson arrivals, one flush at a time);
//! the block under each table gives the coordinator's load.
//!
//! Sizes per path and per chunk are hestia's own. The link figures and the
//! shape of a build step are assumptions, stated as constants below: the
//! output is a comparison under them, not a measurement.

mod common;
use common::{MIB, Rng};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::OnceLock;

use hestia::chunker::{PackBuilder, chunk_path};
use hestia::manifest::{ChunkHash, PathHash};
use hestia::pathinfo::{DEFAULT_DB_PATH, Lookup, StoreDatabase};
use hestia::refnorm::RefTable;

// ------------------------------------------------------------ sizes

// measured: manifest_bench, oci_probe (as in compaction_sim)
const META_BYTES_COMPACTED: f64 = 93.0;
const META_BYTES_PENDING: f64 = 350.0;
const TAG_BYTES: f64 = 90.0;
/// `PackIndex::ROW`
const PACK_INDEX_ROW: f64 = 32.0;

// proposed, by construction
/// Key table row: 20 byte key, offset and length of the body, last-used day.
const KEY_ROW: f64 = 32.0;
/// A sampled chunk's body: the pack that holds it.
const SAMPLE_BODY: f64 = 32.0;
/// The sampling rate the model prices (see the trace for the others).
const MODEL_SAMPLE_RATE: u64 = 16;

// ------------------------------------------------------------ assumptions

/// One end of a transfer. Seconds per request are time to first byte on a
/// kept-alive connection.
#[derive(Clone, Copy)]
struct Link {
    get_s: f64,
    put_s: f64,
    list_s: f64,
    list_page: f64,
    down_bps: f64,
    up_bps: f64,
}
/// An S3 bucket in another datacenter.
const BUCKET: Link = Link {
    get_s: 0.08,
    put_s: 0.15,
    list_s: 0.15,
    list_page: 1000.0,
    down_bps: 60e6,
    up_bps: 40e6,
};
/// A cache node in the builders' datacenter.
const NODE: Link = Link {
    get_s: 0.002,
    put_s: 0.003,
    list_s: 0.002,
    list_page: 1e9,
    down_bps: 500e6,
    up_bps: 500e6,
};
const PARALLEL: f64 = 32.0;

/// The shape of a build step and of the cluster around it.
#[derive(Clone, Copy)]
struct Shape {
    /// Seconds a step runs, and how many run on a machine at once.
    step_s: f64,
    jobs: f64,
    /// Narinfo queries, hit or miss.
    lookups: f64,
    /// Inputs not on the builder's disk.
    fetches: f64,
    /// Outputs.
    paths: f64,
    /// Paths stay in the root this long after their last use.
    window_days: f64,
    /// Shards per root and kind.
    shards: f64,
    /// A path's `.tree` bytes.
    tree_bytes: f64,
}

impl Shape {
    /// A path's body: what `.meta` and `.tree` hold for it today.
    fn body_bytes(&self) -> f64 {
        META_BYTES_COMPACTED + self.tree_bytes
    }
}

/// A package per step: one job a machine, four minutes, a 600 byte tree
/// (manifest_bench, oci_probe).
const COARSE: Shape = Shape {
    step_s: 240.0,
    jobs: 1.0,
    lookups: 60.0,
    fetches: 6.0,
    paths: 1.3,
    window_days: 14.0,
    shards: 256.0,
    tree_bytes: 600.0,
};

/// A compiled object per step. All assumed: ten seconds on one of sixteen
/// cores, the sources and headers mostly on disk already, one output of a
/// single file.
const FINE: Shape = Shape {
    step_s: 10.0,
    jobs: 16.0,
    lookups: 30.0,
    fetches: 2.0,
    paths: 1.0,
    window_days: 14.0,
    shards: 256.0,
    tree_bytes: 120.0,
};

static SHAPE: OnceLock<Shape> = OnceLock::new();
fn sh() -> &'static Shape {
    SHAPE.get().expect("shape is set in main")
}

const DAY_S: f64 = 86400.0;

// today's maintenance
const COMPACT_COOLDOWN_S: f64 = 300.0; // compaction_sim's `elect 300s`
const GC_INTERVAL_S: f64 = 3600.0; // `GcPolicy::min_interval`, the shortest listing

// proposed maintenance: runs are merged into the first level every minute,
// a level into the next when the next one's period is up
const LEVEL_PERIODS_S: [f64; 3] = [60.0, 3600.0, DAY_S];
const MERGE_PERIOD_S: f64 = LEVEL_PERIODS_S[0];
const LEVELS: f64 = LEVEL_PERIODS_S.len() as f64;
/// Of the substituted bytes, what a cache node already holds.
const NODE_HIT: f64 = 0.9;

// ------------------------------------------------------------ trace

struct TracePath {
    name: String,
    /// Unique chunks in pack order with their compressed size.
    chunks: Vec<(ChunkHash, u32)>,
}

#[derive(Clone, Copy, PartialEq)]
enum Finder {
    JobOnly,
    Names,
    Sampled(u64),
    Exact,
}

impl Finder {
    fn label(self) -> String {
        match self {
            Finder::JobOnly => "job only".into(),
            Finder::Names => "names".into(),
            Finder::Sampled(n) => format!("1/{n}"),
            Finder::Exact => "exact".into(),
        }
    }
}

fn is_sample(hash: &ChunkHash, rate: u64) -> bool {
    u64::from_le_bytes(hash.0[..8].try_into().unwrap()) % rate == 0
}

#[derive(Default, Clone, Copy)]
struct Tally {
    paths: f64,
    jobs: f64,
    chunks: f64,
    bytes: f64,
    upload_bytes: f64,
    lookups: f64,
    index_loads: f64,
    index_rows: f64,
    new_samples: f64,
    path_packs: f64,
}

/// What one way of finding duplicates has stored so far.
struct Store {
    finder: Finder,
    /// Pack id → its index.
    packs: Vec<HashSet<ChunkHash>>,
    by_name: HashMap<String, BTreeSet<u32>>,
    samples: HashMap<ChunkHash, u32>,
    all: HashMap<ChunkHash, u32>,
    first: Tally,
    later: Tally,
}

impl Store {
    fn new(finder: Finder) -> Self {
        Store {
            finder,
            packs: vec![],
            by_name: HashMap::new(),
            samples: HashMap::new(),
            all: HashMap::new(),
            first: Tally::default(),
            later: Tally::default(),
        }
    }

    fn job(&mut self, paths: &[&TracePath], first: bool) {
        let mut t = Tally {
            jobs: 1.0,
            ..Tally::default()
        };
        let mut load: BTreeSet<u32> = BTreeSet::new();
        match self.finder {
            Finder::Names => {
                for p in paths {
                    if let Some(packs) = self.by_name.get(&p.name) {
                        load.extend(packs);
                    }
                }
            }
            Finder::Sampled(rate) => {
                for p in paths {
                    for (i, (hash, _)) in p.chunks.iter().enumerate() {
                        if i == 0 || is_sample(hash, rate) {
                            t.lookups += 1.0;
                            if let Some(pack) = self.samples.get(hash) {
                                load.insert(*pack);
                            }
                        }
                    }
                }
            }
            Finder::Exact => t.lookups += paths.iter().map(|p| p.chunks.len() as f64).sum::<f64>(),
            Finder::JobOnly => {}
        }
        let mut known: HashMap<ChunkHash, u32> = HashMap::new();
        for pack in &load {
            t.index_loads += 1.0;
            t.index_rows += self.packs[*pack as usize].len() as f64;
            for hash in &self.packs[*pack as usize] {
                known.insert(*hash, *pack);
            }
        }

        let own = self.packs.len() as u32;
        let mut mine: HashSet<ChunkHash> = HashSet::new();
        for p in paths {
            let mut packs = BTreeSet::new();
            for (i, (hash, compressed)) in p.chunks.iter().enumerate() {
                t.chunks += 1.0;
                t.bytes += f64::from(*compressed);
                let found = if mine.contains(hash) {
                    Some(own)
                } else if self.finder == Finder::Exact {
                    self.all.get(hash).copied()
                } else {
                    known.get(hash).copied()
                };
                packs.insert(found.unwrap_or_else(|| {
                    mine.insert(*hash);
                    t.upload_bytes += f64::from(*compressed);
                    if let Finder::Sampled(rate) = self.finder
                        && (i == 0 || is_sample(hash, rate))
                    {
                        self.samples.insert(*hash, own);
                        t.new_samples += 1.0;
                    }
                    self.all.insert(*hash, own);
                    own
                }));
            }
            t.paths += 1.0;
            t.path_packs += packs.len() as f64;
            self.by_name
                .entry(p.name.clone())
                .or_default()
                .extend(packs);
        }
        if !mine.is_empty() {
            self.packs.push(mine);
        }

        let into = if first {
            &mut self.first
        } else {
            &mut self.later
        };
        for (a, b) in [
            (&mut into.paths, t.paths),
            (&mut into.jobs, t.jobs),
            (&mut into.chunks, t.chunks),
            (&mut into.bytes, t.bytes),
            (&mut into.upload_bytes, t.upload_bytes),
            (&mut into.lookups, t.lookups),
            (&mut into.index_loads, t.index_loads),
            (&mut into.index_rows, t.index_rows),
            (&mut into.new_samples, t.new_samples),
            (&mut into.path_packs, t.path_packs),
        ] {
            *a += b;
        }
    }
}

async fn load_trace(roots: &[String]) -> Vec<Vec<TracePath>> {
    let db = StoreDatabase::new(DEFAULT_DB_PATH);
    let store_dir = db.store_dir().to_string();
    let mut seen: HashSet<PathHash> = HashSet::new();
    let mut generations = Vec::new();
    for root in roots {
        let mut generation = Vec::new();
        for (path, lookup) in db.query_closure([root.clone()]).expect("query closure") {
            let Lookup::Found(info) = lookup else {
                eprintln!("skipping {path}");
                continue;
            };
            if !seen.insert(info.path_hash()) {
                continue;
            }
            let refs = RefTable::new(&info.references);
            let abs = format!("{store_dir}/{}", info.store_path);
            let chunked = match chunk_path(&abs, &refs).await {
                Ok(c) => c,
                Err(err) => {
                    eprintln!("skipping {abs}: {err}");
                    continue;
                }
            };
            let mut builder = PackBuilder::new();
            let mut chunks = Vec::with_capacity(chunked.chunks.len());
            for chunk in &chunked.chunks {
                let before = builder.compressed_size();
                builder.add(chunk).expect("compress chunk");
                chunks.push((chunk.hash, (builder.compressed_size() - before) as u32));
            }
            generation.push(TracePath {
                name: info.store_path.name().as_ref().to_string(),
                chunks,
            });
        }
        eprintln!("{root}: {} new paths", generation.len());
        generations.push(generation);
    }
    generations
}

/// What the model takes from a trace, per way of finding duplicates.
#[derive(Clone, Copy)]
struct Found {
    /// Compressed bytes uploaded per new path.
    upload_per_path: f64,
    /// Pack indexes loaded per new path, and the rows in one.
    loads_per_path: f64,
    rows_per_load: f64,
    lookups_per_path: f64,
    new_samples_per_path: f64,
    /// Packs a stored path's chunks sit in.
    packs_per_path: f64,
}

#[derive(Clone, Copy)]
struct Measured {
    /// Compressed bytes of a path, before dedup.
    path_bytes: f64,
    names: Found,
    sampled: Found,
}

/// `--roots` of four minimal NixOS 25.05 systems (channel revisions 807117,
/// 809091, 811135, 813814), one path per job, revisions two to four.
const MEASURED: Measured = Measured {
    path_bytes: 787692.0,
    names: Found {
        upload_per_path: 552316.0,
        loads_per_path: 1.362,
        rows_per_load: 27.4,
        lookups_per_path: 0.0,
        new_samples_per_path: 0.0,
        packs_per_path: 1.357,
    },
    sampled: Found {
        upload_per_path: 555673.0,
        loads_per_path: 0.765,
        rows_per_load: 80.6,
        lookups_per_path: 4.570,
        new_samples_per_path: 1.634,
        packs_per_path: 1.270,
    },
};

/// Not measured. A compiled object: 50 KiB, a third of that compressed,
/// one chunk, in one pack. An object that changed shares nothing with its
/// predecessor, so both finders upload it whole; the name hint still loads
/// the predecessor's pack index, the sampled lookup of the first chunk
/// misses.
const FINE_MEASURED: Measured = Measured {
    path_bytes: 17000.0,
    names: Found {
        upload_per_path: 17000.0,
        loads_per_path: 1.0,
        rows_per_load: 1.0,
        lookups_per_path: 0.0,
        new_samples_per_path: 0.0,
        packs_per_path: 1.0,
    },
    sampled: Found {
        upload_per_path: 17000.0,
        loads_per_path: 0.0,
        rows_per_load: 1.0,
        lookups_per_path: 1.0,
        new_samples_per_path: 1.0,
        packs_per_path: 1.0,
    },
};

fn found(t: &Tally) -> Found {
    Found {
        upload_per_path: t.upload_bytes / t.paths,
        loads_per_path: t.index_loads / t.paths,
        rows_per_load: t.index_rows / t.index_loads.max(1.0),
        lookups_per_path: t.lookups / t.paths,
        new_samples_per_path: t.new_samples / t.paths,
        packs_per_path: t.path_packs / t.paths,
    }
}

fn replay(generations: &[Vec<TracePath>], per_job: usize) -> Measured {
    let finders = [
        Finder::JobOnly,
        Finder::Names,
        Finder::Sampled(1024),
        Finder::Sampled(256),
        Finder::Sampled(64),
        Finder::Sampled(16),
        Finder::Sampled(4),
        Finder::Sampled(1),
        Finder::Exact,
    ];
    let mut stores: Vec<Store> = finders.iter().map(|f| Store::new(*f)).collect();
    for (g, generation) in generations.iter().enumerate() {
        let paths: Vec<&TracePath> = generation.iter().collect();
        for job in paths.chunks(per_job) {
            for store in &mut stores {
                store.job(job, g == 0);
            }
        }
    }

    let first = stores[0].first;
    let later = stores[0].later;
    println!(
        "trace: {} revisions, {per_job} path(s) per job; first revision {} paths, {:.0} chunks, {:.0} MiB packed; \
         later revisions {} new paths, {:.0} chunks, {:.0} MiB packed\n",
        generations.len(),
        first.paths,
        first.chunks,
        first.bytes / MIB,
        later.paths,
        later.chunks,
        later.bytes / MIB,
    );
    println!(
        "{:<9} {:>10} {:>7} | {:>9} {:>9} {:>9} | {:>9} {:>10}",
        "find by",
        "upload MiB",
        "of all",
        "lookups",
        "idx loads",
        "idx KiB",
        "idx rows",
        "packs/path"
    );
    println!(
        "{:<9} {:>10} {:>7} | {:>9} {:>9} {:>9} | {:>9} {:>10}",
        "", "later", "", "per path", "per path", "per path", "per path", ""
    );
    for store in &stores {
        let t = &store.later;
        if t.paths == 0.0 {
            continue;
        }
        let f = found(t);
        println!(
            "{:<9} {:>10.1} {:>6.1}% | {:>9.2} {:>9.2} {:>9.2} | {:>9.2} {:>10.2}",
            store.finder.label(),
            t.upload_bytes / MIB,
            100.0 * t.upload_bytes / t.bytes,
            f.lookups_per_path,
            f.loads_per_path,
            f.loads_per_path * f.rows_per_load * PACK_INDEX_ROW / 1024.0,
            match store.finder {
                Finder::Exact =>
                    t.upload_bytes.min(1.0) * store.all.len() as f64 / (store.first.paths + t.paths),
                _ => store.samples.len() as f64 / (store.first.paths + t.paths),
            },
            f.packs_per_path,
        );
    }
    println!(
        "\n  lookups: sampled chunks asked of the global index; idx loads: pack indexes fetched on a hit\n  \
         idx rows: rows the global index holds per stored path; packs/path: packs a path's chunks sit in\n"
    );

    let pick = |finder: Finder| {
        let s = stores.iter().find(|s| s.finder == finder).unwrap();
        found(if s.later.paths > 0.0 {
            &s.later
        } else {
            &s.first
        })
    };
    let all = if later.paths > 0.0 { later } else { first };
    let m = Measured {
        path_bytes: all.bytes / all.paths,
        names: pick(Finder::Names),
        sampled: pick(Finder::Sampled(MODEL_SAMPLE_RATE)),
    };
    for (label, f) in [("names", m.names), ("sampled", m.sampled)] {
        eprintln!(
            "    {label}: Found {{ upload_per_path: {:.0}.0, loads_per_path: {:.3}, rows_per_load: {:.1}, \
             lookups_per_path: {:.3}, new_samples_per_path: {:.3}, packs_per_path: {:.3} }},",
            f.upload_per_path,
            f.loads_per_path,
            f.rows_per_load,
            f.lookups_per_path,
            f.new_samples_per_path,
            f.packs_per_path
        );
    }
    eprintln!("    path_bytes: {:.0}.0,", m.path_bytes);
    m
}

// ------------------------------------------------------------ model

/// Requests, bytes and seconds of one part of a step.
#[derive(Default, Clone, Copy)]
struct Cost {
    requests: f64,
    bytes: f64,
    secs: f64,
}

impl Cost {
    fn get(&mut self, link: Link, objects: f64, bytes: f64) {
        self.requests += objects;
        self.bytes += bytes;
        self.secs += (objects / PARALLEL).ceil() * link.get_s + bytes / link.down_bps;
    }
    fn put(&mut self, link: Link, objects: f64, bytes: f64) {
        self.requests += objects;
        self.bytes += bytes;
        self.secs += (objects / PARALLEL).ceil() * link.put_s + bytes / link.up_bps;
    }
    fn list(&mut self, link: Link, keys: f64) {
        let pages = (keys / link.list_page).ceil().max(1.0);
        self.requests += pages;
        self.bytes += keys * TAG_BYTES;
        self.secs += pages * link.list_s + keys * TAG_BYTES / link.down_bps;
    }
    fn add(&mut self, other: Cost) {
        self.requests += other.requests;
        self.bytes += other.bytes;
        self.secs += other.secs;
    }
}

struct Step {
    /// Before the first lookup can be answered.
    start: Cost,
    /// The lookups and the substituted inputs, index only.
    read_index: Cost,
    /// The substituted inputs' chunks.
    read_data: Cost,
    /// Finding duplicates and publishing, without the new chunks.
    write_index: Cost,
    /// The new chunks.
    write_data: Cost,
    /// Index bytes maintenance rewrites per day, outside any step.
    upkeep_bytes_per_day: f64,
}

struct Cluster {
    steps_per_s: f64,
    root_paths: f64,
}

impl Cluster {
    fn of(machines: f64) -> Self {
        let steps_per_s = machines * sh().jobs / sh().step_s;
        Cluster {
            steps_per_s,
            root_paths: steps_per_s * sh().paths * sh().window_days * DAY_S,
        }
    }
}

/// Shards a batch of `keys` uniformly hashed keys lands in.
fn shards_hit(keys: f64) -> f64 {
    sh().shards * (1.0 - (1.0 - 1.0 / sh().shards).powf(keys))
}

fn today(c: &Cluster, m: &Measured, kept: bool) -> Step {
    let link = BUCKET;
    // Heads::load: list, the GC and compaction heads, every segment's .meta.
    let heads = c.steps_per_s * GC_INTERVAL_S / 2.0;
    let pending = c.steps_per_s * COMPACT_COOLDOWN_S / 2.0;
    let since_gc_paths = heads * sh().paths;
    // A reload reuses the segments it holds: only the pending ones are new.
    let mut reload = Cost::default();
    reload.list(link, heads);
    reload.get(link, 4.0, 0.0);
    reload.get(link, pending, pending * sh().paths * META_BYTES_PENDING);
    let mut load = reload;
    load.get(
        link,
        2.0,
        c.root_paths * META_BYTES_COMPACTED + since_gc_paths * META_BYTES_PENDING,
    );
    // The first resolve fetches the base segment's .tree whole.
    let mut tree = Cost::default();
    tree.get(link, 1.0, c.root_paths * sh().tree_bytes);

    let mut start = if kept { reload } else { load };
    let mut read_index = Cost::default();
    if kept {
        // The base .meta and .tree change when GC rewrites them: their
        // fetch is spread over the steps in between.
        let steps = GC_INTERVAL_S / sh().step_s;
        let mut base = Cost::default();
        base.get(link, 2.0, c.root_paths * sh().body_bytes());
        start.requests += base.requests / steps;
        start.bytes += base.bytes / steps;
        start.secs += base.secs / steps;
    } else {
        read_index.add(tree);
    }
    // Lookups are answered from memory. Each input needs its packs'
    // indexes for the offsets.
    let packs = sh().fetches * m.names.packs_per_path;
    read_index.get(link, packs, packs * m.names.rows_per_load * PACK_INDEX_ROW);
    let mut read_data = Cost::default();
    read_data.get(link, packs, sh().fetches * m.path_bytes);

    // The drain reloads, loads the name hints' pack indexes, and writes the
    // pack, .meta, .tree and head.
    let mut write_index = reload;
    let hints = sh().paths * m.names.loads_per_path;
    write_index.get(link, hints, hints * m.names.rows_per_load * PACK_INDEX_ROW);
    write_index.put(
        link,
        3.0,
        sh().paths * (META_BYTES_PENDING + sh().tree_bytes),
    );
    let mut write_data = Cost::default();
    write_data.put(link, 1.0, sh().paths * m.names.upload_per_path);

    Step {
        start,
        read_index,
        read_data,
        write_index,
        write_data,
        // GC folds everything into a new base .meta and .tree.
        upkeep_bytes_per_day: DAY_S / GC_INTERVAL_S * c.root_paths * sh().body_bytes(),
    }
}

/// Path rows and sample rows of the proposed index, and what merging costs.
struct Index {
    path_rows: f64,
    sample_rows: f64,
}

impl Index {
    fn of(c: &Cluster, m: &Measured) -> Self {
        Index {
            path_rows: c.root_paths,
            sample_rows: c.root_paths * m.sampled.new_samples_per_path,
        }
    }
    fn bytes(&self) -> f64 {
        self.path_rows * (KEY_ROW + sh().body_bytes()) + self.sample_rows * (KEY_ROW + SAMPLE_BODY)
    }
    /// All levels' key tables of one path shard.
    fn path_keys(&self) -> f64 {
        self.path_rows / sh().shards * KEY_ROW
    }
    /// All levels of one sample shard, keys and bodies.
    fn sample_shard(&self) -> f64 {
        self.sample_rows / sh().shards * (KEY_ROW + SAMPLE_BODY)
    }
    /// Bytes merges write per day. A level holds the rows younger than
    /// the next level's period, half of them on average, and is rewritten
    /// every period of its own; the last holds everything.
    fn upkeep(&self, periods: &[f64]) -> f64 {
        let per_s = self.bytes() / (sh().window_days * DAY_S);
        periods
            .iter()
            .enumerate()
            .map(|(i, period)| {
                DAY_S / period
                    * match periods.get(i + 1) {
                        Some(next) => per_s * next / 2.0,
                        None => self.bytes(),
                    }
            })
            .sum()
    }
}

fn shards(c: &Cluster, m: &Measured) -> Step {
    let link = BUCKET;
    let index = Index::of(c, m);

    // Rows not merged yet are in the runs of the last period: list them,
    // fetch them.
    let runs = c.steps_per_s * MERGE_PERIOD_S / 2.0;
    let run_bytes = run_bytes(m);
    let mut start = Cost::default();
    start.list(link, runs);
    start.get(link, runs, runs * run_bytes);

    // Every level's key table of every shard a lookup lands in, then one
    // body per input. Bodies carry pack offsets.
    let mut read_index = Cost::default();
    let hit = shards_hit(sh().lookups);
    read_index.get(link, LEVELS * hit, hit * index.path_keys());
    read_index.get(link, sh().fetches, sh().fetches * sh().body_bytes());
    let mut read_data = Cost::default();
    read_data.get(
        link,
        sh().fetches * m.sampled.packs_per_path,
        sh().fetches * m.path_bytes,
    );

    // Sampled chunks are looked up in their shards, hits load pack indexes,
    // then one run goes up beside the pack.
    let mut write_index = Cost::default();
    let hit = shards_hit(sh().paths * m.sampled.lookups_per_path);
    write_index.get(link, LEVELS * hit, hit * index.sample_shard());
    let loads = sh().paths * m.sampled.loads_per_path;
    write_index.get(
        link,
        loads,
        loads * m.sampled.rows_per_load * PACK_INDEX_ROW,
    );
    write_index.put(link, 1.0, run_bytes);
    let mut write_data = Cost::default();
    write_data.put(link, 1.0, sh().paths * m.sampled.upload_per_path);

    Step {
        start,
        read_index,
        read_data,
        write_index,
        write_data,
        upkeep_bytes_per_day: index.upkeep(&LEVEL_PERIODS_S),
    }
}

fn cache_node(c: &Cluster, m: &Measured) -> Step {
    let index = Index::of(c, m);
    // The node answers a batch of keys from memory and returns the bodies
    // asked for; new rows reach it directly, so there are no runs to read.
    let mut read_index = Cost::default();
    read_index.get(NODE, 1.0, sh().fetches * sh().body_bytes());
    let mut read_data = Cost::default();
    let ranges = sh().fetches * m.sampled.packs_per_path;
    read_data.get(NODE, ranges, NODE_HIT * sh().fetches * m.path_bytes);
    // What the node lacks it fetches from the bucket while the step waits.
    read_data.secs += (1.0 - NODE_HIT)
        * ((ranges / PARALLEL).ceil() * BUCKET.get_s
            + sh().fetches * m.path_bytes / BUCKET.down_bps);
    read_data.bytes += (1.0 - NODE_HIT) * sh().fetches * m.path_bytes;

    let mut write_index = Cost::default();
    write_index.get(NODE, 1.0, 0.0);
    let loads = sh().paths * m.sampled.loads_per_path;
    write_index.get(
        NODE,
        loads,
        loads * m.sampled.rows_per_load * PACK_INDEX_ROW,
    );
    write_index.put(NODE, 1.0, sh().paths * (KEY_ROW + sh().body_bytes()));
    let mut write_data = Cost::default();
    write_data.put(NODE, 1.0, sh().paths * m.sampled.upload_per_path);

    Step {
        start: Cost::default(),
        read_index,
        read_data,
        write_index,
        write_data,
        upkeep_bytes_per_day: index.upkeep(&LEVEL_PERIODS_S),
    }
}

/// Bytes of the rows one step publishes.
fn run_bytes(m: &Measured) -> f64 {
    sh().paths
        * (KEY_ROW + sh().body_bytes() + m.sampled.new_samples_per_path * (KEY_ROW + SAMPLE_BODY))
}

/// A coordinator's group commit: steps arrive at random, one flush runs at
/// a time and takes every step that arrived before it started.
struct Commit {
    /// From a step handing over its rows to their being in the bucket.
    mean_s: f64,
    p99_s: f64,
    flushes_per_s: f64,
}

fn group_commit(steps_per_s: f64, bytes_per_step: f64) -> Commit {
    const ARRIVALS: usize = 200_000;
    let mut rng = Rng(0x5eed);
    let mut arrivals = Vec::with_capacity(ARRIVALS);
    let mut t = 0.0;
    for _ in 0..ARRIVALS {
        t += rng.exponential(steps_per_s);
        arrivals.push(t);
    }
    let mut waits = Vec::with_capacity(ARRIVALS);
    let (mut free, mut next, mut flushes) = (0.0f64, 0usize, 0.0);
    while next < ARRIVALS {
        let start = free.max(arrivals[next]);
        let batch = arrivals[next..].partition_point(|a| *a <= start);
        free = start + BUCKET.put_s + batch as f64 * bytes_per_step / BUCKET.up_bps;
        waits.extend(arrivals[next..next + batch].iter().map(|a| free - a));
        next += batch;
        flushes += 1.0;
    }
    waits.sort_by(f64::total_cmp);
    Commit {
        mean_s: waits.iter().sum::<f64>() / waits.len() as f64,
        p99_s: waits[waits.len() * 99 / 100],
        flushes_per_s: flushes / free,
    }
}

/// One coordinator in the builders' datacenter takes every step's rows.
/// With `serves` it also answers the lookups from memory.
fn coordinator(c: &Cluster, m: &Measured, serves: bool) -> Step {
    let mut step = shards(c, m);
    let commit = group_commit(c.steps_per_s, run_bytes(m));
    let pending = c.steps_per_s * MERGE_PERIOD_S / 2.0 * run_bytes(m);

    step.start = Cost::default();
    let mut write_index = Cost::default();
    if serves {
        // One batch of keys, the bodies of the inputs back.
        step.read_index = Cost::default();
        step.read_index
            .get(NODE, 1.0, sh().fetches * sh().body_bytes());
        write_index.get(NODE, 1.0, 0.0);
    } else {
        // The rows no level holds yet, in one answer.
        step.start.get(NODE, 1.0, pending);
        let index = Index::of(c, m);
        let hit = shards_hit(sh().paths * m.sampled.lookups_per_path);
        write_index.get(BUCKET, LEVELS * hit, hit * index.sample_shard());
    }
    let loads = sh().paths * m.sampled.loads_per_path;
    write_index.get(
        BUCKET,
        loads,
        loads * m.sampled.rows_per_load * PACK_INDEX_ROW,
    );
    // The rows go to the coordinator, which answers once its flush is done.
    write_index.put(NODE, 1.0, run_bytes(m));
    write_index.secs += commit.mean_s;
    step.write_index = write_index;
    step
}

/// What the coordinator itself has to carry.
fn coordinator_load(c: &Cluster, m: &Measured) {
    let index = Index::of(c, m);
    let commit = group_commit(c.steps_per_s, run_bytes(m));
    let keys = index.path_rows * KEY_ROW + index.sample_rows * (KEY_ROW + SAMPLE_BODY);
    let mut restart = Cost::default();
    restart.list(BUCKET, LEVELS * 2.0 * sh().shards);
    restart.get(BUCKET, LEVELS * 2.0 * sh().shards, keys);
    // A flush is one PUT however many steps it carries: the ceiling is the
    // uplink.
    let ceiling = BUCKET.up_bps / run_bytes(m);
    println!(
        "  coordinator: {:.1} steps/s in, {:.0} lookups/s if it serves; commit {:.0} ms mean, {:.0} ms p99 (own run: {:.0} ms); {:.0} index objects/day instead of {:.0} runs",
        c.steps_per_s,
        c.steps_per_s * (sh().lookups + sh().paths * m.sampled.lookups_per_path),
        commit.mean_s * 1e3,
        commit.p99_s * 1e3,
        BUCKET.put_s * 1e3,
        commit.flushes_per_s * DAY_S,
        c.steps_per_s * DAY_S,
    );
    println!(
        "               serving holds {:.2} GB of key tables in memory, {:.0} s to load them after a restart; flushes keep up until {:.0} steps/s ({:.1} M machines)",
        keys / 1e9,
        restart.secs,
        ceiling,
        ceiling * sh().step_s / sh().jobs / 1e6,
    );
}

fn model(m: &Measured) {
    println!(
        "model: a step is {} lookups, {} substituted inputs of {:.2} MiB, {} outputs, takes {} s, {} at once per machine",
        sh().lookups,
        sh().fetches,
        m.path_bytes / MIB,
        sh().paths,
        sh().step_s,
        sh().jobs
    );
    println!(
        "       bucket {:.0} ms a GET, {:.0} ms a PUT, {:.0}/{:.0} MB/s; cache node {:.0} ms, {:.0} MB/s, holds {:.0}% of substituted bytes",
        BUCKET.get_s * 1e3,
        BUCKET.put_s * 1e3,
        BUCKET.down_bps / 1e6,
        BUCKET.up_bps / 1e6,
        NODE.get_s * 1e3,
        NODE.down_bps / 1e6,
        NODE_HIT * 100.0
    );
    println!(
        "       paths kept {} days; today: GC every {:.0} min, compaction cooldown {COMPACT_COOLDOWN_S} s; shards: {} per kind, runs merged every {MERGE_PERIOD_S} s, dedup sampled 1/{MODEL_SAMPLE_RATE}\n",
        sh().window_days,
        GC_INTERVAL_S / 60.0,
        sh().shards
    );

    for machines in [10.0, 100.0, 500.0] {
        let c = Cluster::of(machines);
        let index = Index::of(&c, m);
        println!(
            "{machines} machines: {:.0} steps/day, {:.2} M paths in the root, shard index {:.2} GB",
            c.steps_per_s * DAY_S,
            c.root_paths / 1e6,
            index.bytes() / 1e9
        );
        println!(
            "  {:<13} | {:>5} {:>8} {:>7} | {:>5} {:>8} {:>7} | {:>7} | {:>5} {:>8} {:>7} | {:>7} | {:>8} | {:>9}",
            "", "req", "MiB", "s", "req", "MiB", "s", "s", "req", "MiB", "s", "s", "s", "GB/day"
        );
        println!(
            "  {:<13} | {:^22} | {:^22} | {:>7} | {:^22} | {:>7} | {:>8} | {:>9}",
            "", "start", "read: index", "chunks", "write: index", "chunks", "step", "upkeep"
        );
        for (name, step) in [
            ("today", today(&c, m, false)),
            ("today, kept", today(&c, m, true)),
            ("shards", shards(&c, m)),
            ("coordinator", coordinator(&c, m, false)),
            ("coord. serves", coordinator(&c, m, true)),
            ("+ cache node", cache_node(&c, m)),
        ] {
            let mut total = Cost::default();
            for part in [
                step.start,
                step.read_index,
                step.read_data,
                step.write_index,
                step.write_data,
            ] {
                total.add(part);
            }
            println!(
                "  {:<13} | {:>5.0} {:>8.2} {:>7.2} | {:>5.0} {:>8.2} {:>7.2} | {:>7.2} | {:>5.0} {:>8.2} {:>7.2} | {:>7.2} | {:>8.2} | {:>9.1}",
                name,
                step.start.requests,
                step.start.bytes / MIB,
                step.start.secs,
                step.read_index.requests,
                step.read_index.bytes / MIB,
                step.read_index.secs,
                step.read_data.secs,
                step.write_index.requests,
                step.write_index.bytes / MIB,
                step.write_index.secs,
                step.write_data.secs,
                total.secs,
                step.upkeep_bytes_per_day / 1e9,
            );
        }
        coordinator_load(&c, m);
        println!(
            "  shards in one level merged every minute: {:.0} GB/day of upkeep; in two (minute, day): {:.0}\n",
            index.upkeep(&[MERGE_PERIOD_S]) / 1e9,
            index.upkeep(&[MERGE_PERIOD_S, DAY_S]) / 1e9
        );
    }
}

#[tokio::main]
async fn main() {
    let mut roots: Vec<String> = Vec::new();
    let mut per_job = 1usize;
    let mut shape = COARSE;
    let mut fine = false;
    let mut args = std::env::args().skip(1).peekable();
    while let Some(flag) = args.next() {
        if flag == "--fine" {
            (shape, fine) = (FINE, true);
            continue;
        }
        let value = args.next().unwrap_or_else(|| {
            eprintln!("missing value for {flag}");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--roots" => roots = value.split(',').map(|s| s.trim().to_string()).collect(),
            "--paths-per-job" => per_job = value.parse().expect("--paths-per-job takes a number"),
            "--shards" => shape.shards = value.parse().expect("--shards takes a number"),
            "--window-days" => {
                shape.window_days = value.parse().expect("--window-days takes a number")
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }

    SHAPE.set(shape).ok();
    let measured = if fine {
        println!("--fine: a compiled object per step, its sizes assumed (FINE_MEASURED)\n");
        FINE_MEASURED
    } else if roots.is_empty() {
        println!("no --roots: the model runs on the figures of the trace recorded in MEASURED\n");
        MEASURED
    } else {
        replay(&load_trace(&roots).await, per_job.max(1))
    };
    model(&measured);
}
