//! A cache for two workloads at once (a package per step, a compiled object
//! per step) on cache nodes in the builders' datacenter with a bucket in
//! another one behind them: score a design, then let a search improve it.
//!
//!   cargo run --release --example hybrid_sim
//!   cargo run --release --example hybrid_sim -- --fine-machines 0 --coarse-machines 500
//!   cargo run --release --example hybrid_sim -- --object-bytes 10000
//!   cargo run --release --example hybrid_sim -- --measured
//!   cargo run --release --example hybrid_sim -- --step-s 0.3 --lookups 28 --object-bytes 33000
//!
//! A design is a set of knobs (`Design`). `evaluate` turns it into dollars a
//! day: builder time spent waiting on the cache, node hardware, bucket
//! requests, storage and transfer, work redone after lost data, and steps
//! stalled by a restarting node. Designs that overload a node or do not fit
//! its memory or disk are infeasible.
//!
//! The search starts from the design as first written down and repeatedly
//! applies whichever single change (`mutations`) lowers the cost most,
//! printing each step with the cost term that paid for it. It is run for
//! the mixed cluster, for each workload alone, and under shifted prices and
//! failure rates, to see which choices survive all of them.
//!
//! Last, the mix is scaled from 1 to 2000 machines and the search confined
//! to one deployment at a time: no nodes (every builder keeps the keys and
//! uses the bucket itself), one node, two or more. `DETAIL=1` prints the
//! three designs behind each row.
//!
//! Everything under "assumptions" is assumed: link and node figures,
//! prices, failure rates, the shape of both steps. The per-path figures of
//! the coarse step are scale_sim's trace. The output ranks designs under
//! these assumptions; it measures nothing.

mod common;
use common::Rng;

const DAY: f64 = 86400.0;
/// `--object-bytes`: compressed size of a compiled object.
static OBJECT_BYTES: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
/// `--step-s`, `--lookups`: seconds a compile takes and inputs it looks up.
static FINE_STEP_S: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
static FINE_LOOKUPS: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
/// `--delta-share`: what is left of a recompiled object stored as a delta
/// to the object the same source file produced before. `object_delta`
/// measured 17% to 52% between git releases.
static DELTA_SHARE: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
/// A node packs a delta at this rate and unpacks at that (bytes of the raw
/// object, which is three times the compressed one): `object_delta`.
const DELTA_PACK_BPS: f64 = 180e6;
const DELTA_UNPACK_BPS: f64 = 600e6;
const RAW_PER_STORED: f64 = 3.0;
const GB: f64 = 1e9;
const KEY_ROW: f64 = 32.0;
const PACK_BYTES: f64 = 64.0 * 1048576.0;

// ------------------------------------------------------------ assumptions

// a request to a cache node: lognormal, plus bytes
const RPC_MEDIAN_S: f64 = 0.0015;
const RPC_SIGMA: f64 = 0.6;
const NODE_BPS: f64 = 500e6;
// the bucket, seen from a node
const BUCKET_GET_S: f64 = 0.08;
const BUCKET_PUT_S: f64 = 0.15;
const BUCKET_DOWN_BPS: f64 = 60e6;
const BUCKET_UP_BPS: f64 = 40e6;
/// What all nodes together may send to the bucket.
const UPLINK_BPS: f64 = 600e6;

// a node
const NODE_CORES: f64 = 16.0;
const NODE_RAM_MAX: f64 = 512.0 * GB;
const NODE_SSD_MAX: f64 = 8000.0 * GB;
const NODE_RAM_BASE: f64 = 1.0 * GB;
const CPU_KEY_SSD_S: f64 = 20e-6;
const CPU_WRITE_S: f64 = 50e-6;

/// What a node's disk and cores do. `ASSUMED` is what the search was first
/// run with; `MEASURED` (`--measured`) is what `assumption_bench` saw on a
/// 4-core VM with a virtio disk, using its faster variants.
struct Node {
    ssd_read_bps: f64,
    /// CPU to find one key held in memory.
    key_ram_s: f64,
    /// Wall time for a step's lookups when keys are on disk.
    ssd_lookup_s: f64,
    /// Hashing what passes through, per byte.
    byte_s: f64,
    /// Merging index levels, per byte.
    merge_byte_s: f64,
}

const ASSUMED: Node = Node {
    ssd_read_bps: 2e9,
    key_ram_s: 3e-6,
    ssd_lookup_s: 200e-6,
    byte_s: 1.0 / 2e9,
    merge_byte_s: 1.0 / 200e6,
};

const MEASURED: Node = Node {
    ssd_read_bps: 1.7e9,
    key_ram_s: 0.2e-6,
    ssd_lookup_s: 186e-6,
    byte_s: 1.0 / 3.2e9,
    merge_byte_s: 1.0 / 409e6,
};

static USE_MEASURED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn nd() -> &'static Node {
    if *USE_MEASURED.get().unwrap_or(&false) {
        &MEASURED
    } else {
        &ASSUMED
    }
}
const MAX_LOAD: f64 = 0.7;
/// A lost node is replaced after this long.
const REPLACE_S: f64 = 3600.0;
/// With uploads after the step: share of steps whose output is asked for
/// before it has landed.
const RACED: f64 = 0.1;

#[derive(Clone, Copy)]
struct World {
    builder_per_hour: f64,
    node_per_day: f64,
    ram_gb_day: f64,
    ssd_gb_day: f64,
    put: f64,
    get: f64,
    store_gb_day: f64,
    download_gb: f64,
    upload_gb: f64,
    /// Per node and day: a restart that keeps the disk, the loss of a node
    /// with its disk. Per day: the loss of every node at once.
    restarts: f64,
    node_losses: f64,
    disasters: f64,
}

const WORLD: World = World {
    builder_per_hour: 1.0,
    node_per_day: 5.0,
    ram_gb_day: 0.10,
    ssd_gb_day: 0.003,
    put: 5e-6,
    get: 0.4e-6,
    store_gb_day: 0.00077,
    download_gb: 0.05,
    upload_gb: 0.01,
    restarts: 1.0 / 7.0,
    node_losses: 0.5 / 365.0,
    disasters: 0.05 / 365.0,
};

/// One kind of build step and how much of it the cluster runs.
#[derive(Clone, Copy)]
struct Class {
    name: &'static str,
    steps_per_s: f64,
    step_s: f64,
    /// Share of a machine a step occupies.
    slot: f64,
    lookups: f64,
    fetches: f64,
    fetch_bytes: f64,
    outputs: f64,
    out_bytes: f64,
    /// Index body of an output.
    body_bytes: f64,
    /// With sampled dedup: keys looked up and rows added per output, pack
    /// index bytes loaded per output, share of the output not uploaded.
    sample_lookups: f64,
    sample_rows: f64,
    sample_load_bytes: f64,
    sample_saves: f64,
    /// Outputs live this long after their last use.
    window_days: f64,
    /// Mean age of a fetched input.
    fetch_age_days: f64,
    /// Share of live rows used on a day.
    touched: f64,
    /// Chance a lost output is needed again and has to be rebuilt.
    needed: f64,
    /// Size of an output: lognormal around `out_bytes`.
    size_sigma: f64,
}

/// A compiled object per step: scale_sim's `--fine`.
fn fine(machines: f64) -> Class {
    let object = *OBJECT_BYTES.get().unwrap_or(&17000.0);
    let step_s = *FINE_STEP_S.get().unwrap_or(&10.0);
    Class {
        name: "fine",
        steps_per_s: machines * 16.0 / step_s,
        step_s,
        slot: 1.0 / 16.0,
        lookups: *FINE_LOOKUPS.get().unwrap_or(&30.0),
        fetches: 2.0,
        fetch_bytes: object,
        outputs: 1.0,
        out_bytes: object,
        body_bytes: 93.0 + 120.0,
        sample_lookups: 1.0,
        sample_rows: 1.0,
        sample_load_bytes: 0.0,
        sample_saves: 0.02,
        window_days: 3.0,
        fetch_age_days: 0.5,
        touched: 0.3,
        needed: 0.3,
        size_sigma: 1.0,
    }
}

/// A package per step: scale_sim's trace of four NixOS revisions.
fn coarse(machines: f64) -> Class {
    Class {
        name: "coarse",
        steps_per_s: machines / 240.0,
        step_s: 240.0,
        slot: 1.0,
        lookups: 60.0,
        fetches: 6.0,
        fetch_bytes: 787692.0,
        outputs: 1.3,
        out_bytes: 787692.0,
        body_bytes: 93.0 + 600.0,
        sample_lookups: 4.57,
        sample_rows: 1.634,
        sample_load_bytes: 0.765 * 80.6 * 32.0,
        sample_saves: 1.0 - 555673.0 / 787692.0,
        window_days: 14.0,
        fetch_age_days: 2.0,
        touched: 0.2,
        needed: 0.6,
        size_sigma: 1.5,
    }
}

// ------------------------------------------------------------ design

#[derive(Clone, Copy, PartialEq, Debug)]
enum Expiry {
    /// Levels are merged; the last holds everything and is rewritten every
    /// `top_days` to drop expired rows. A use writes a touch row.
    Merge,
    /// The index is cut into partitions `top_days` long that are deleted
    /// whole. A use of a row in an older partition writes it again.
    Partitions,
}

#[derive(Clone, PartialEq, Debug)]
struct Design {
    nodes: f64,
    /// Per class (fine, coarse): nodes that hold a blob and its row.
    replicas: [f64; 2],
    /// A write is acknowledged once it is in the bucket (else: once the
    /// replicas have it on disk).
    ack_bucket: bool,
    /// Per class (fine, coarse): is it written to the bucket at all, how
    /// many days of it a node keeps, are its outputs sample-deduplicated,
    /// are its uploads batched into shared packs.
    bucket: [bool; 2],
    cache_days: [f64; 2],
    sampled: [bool; 2],
    batched: [bool; 2],
    /// Nodes write to the bucket this often.
    flush_s: f64,
    /// Every node holds every key (a lookup is one request) or only the
    /// keys of its slice (a lookup fans out).
    index_everywhere: bool,
    keys_on_ssd: bool,
    expiry: Expiry,
    levels: f64,
    top_days: f64,
    /// Shards per level: fixed, or (0) as many as keep one near `shard_bytes`.
    shards: f64,
    shard_bytes: f64,
    /// Outputs below this size are stored in their index row.
    inline_below: f64,
    /// A restarting node reads its keys from its own disk, not the bucket.
    local_checkpoint: bool,
    /// The answer to a lookup carries the inputs the step lacks.
    fetch_with_lookup: bool,
    /// A step reports before its outputs are uploaded; whoever asks for
    /// them early waits.
    write_behind: bool,
    /// No cache nodes: every builder keeps the keys on its own disk, synced
    /// from the bucket, and reads and writes blobs there itself. One of
    /// them merges the index.
    embedded: bool,
    /// A cache node is a process on a builder machine, not a machine.
    hosted: bool,
    /// A compiled object's index entry is one fixed row holding where the
    /// object is, with no separate body.
    compact_fine: bool,
    /// Nodes store a compiled object as a delta to the one the same source
    /// file produced before, and hand out whole objects.
    delta_fine: bool,
}

/// The hybrid design as first written down.
fn first_draft() -> Design {
    Design {
        nodes: 4.0,
        replicas: [2.0, 2.0],
        ack_bucket: false,
        bucket: [true, true],
        cache_days: [1.0, 1.0],
        sampled: [false, true],
        batched: [true, false],
        flush_s: 60.0,
        index_everywhere: false,
        keys_on_ssd: false,
        expiry: Expiry::Merge,
        levels: 3.0,
        top_days: 1.0,
        shards: 65536.0,
        shard_bytes: 4.0 * 1048576.0,
        inline_below: 0.0,
        local_checkpoint: false,
        fetch_with_lookup: false,
        write_behind: false,
        embedded: false,
        hosted: false,
        compact_fine: false,
        delta_fine: false,
    }
}

// ------------------------------------------------------------ evaluation

/// Mean of the slowest of `k` parallel node requests, k = 0..=64.
struct Rpc(Vec<f64>);

impl Rpc {
    fn new() -> Self {
        const ROUNDS: usize = 20_000;
        let mut rng = Rng(0x5eed);
        let mut sums = vec![0.0; 65];
        for _ in 0..ROUNDS {
            let mut slowest = 0.0f64;
            for sum in sums.iter_mut().skip(1) {
                slowest = slowest.max(rng.lognormal(RPC_MEDIAN_S, RPC_SIGMA));
                *sum += slowest;
            }
        }
        Rpc(sums.into_iter().map(|s| s / ROUNDS as f64).collect())
    }
    fn slowest_of(&self, k: f64) -> f64 {
        self.0[(k.ceil() as usize).min(64)]
    }
}

/// Share of outputs below `limit` bytes and their mean contribution to a
/// row, for a lognormal with the given mean.
fn below(mean: f64, sigma: f64, limit: f64) -> (f64, f64) {
    if limit <= 0.0 {
        return (0.0, 0.0);
    }
    let median = mean / (sigma * sigma / 2.0).exp();
    let mut rng = Rng(7);
    let (mut count, mut bytes) = (0.0, 0.0);
    const N: usize = 20_000;
    for _ in 0..N {
        let size = rng.lognormal(median, sigma);
        if size < limit {
            count += 1.0;
            bytes += size;
        }
    }
    (count / N as f64, bytes / N as f64)
}

const TERMS: [&str; 8] = [
    "wait fine",
    "wait coarse",
    "nodes",
    "requests",
    "storage",
    "download",
    "redo",
    "stall",
];

#[derive(Default, Clone)]
struct Report {
    /// Dollars a day, in the order of `TERMS`.
    terms: [f64; 8],
    infeasible: Option<String>,
    /// How far past its limits an infeasible design is: what the search
    /// descends on until something works.
    over: f64,
    wait_s: [f64; 2],
    ram_gb: f64,
    ssd_gb: f64,
    load: f64,
    puts_per_day: f64,
    up_gb_per_day: f64,
    index_up_gb_per_day: f64,
    restart_s: f64,
    redone_steps_per_year: f64,
}

impl Report {
    fn total(&self) -> f64 {
        if self.infeasible.is_some() {
            f64::INFINITY
        } else {
            self.terms.iter().sum()
        }
    }

    /// What the search minimises: dollars, or far above any dollars and
    /// growing with the overrun.
    fn search_cost(&self) -> f64 {
        if self.infeasible.is_some() {
            1e12 * (1.0 + self.over)
        } else {
            self.total()
        }
    }
}

/// The bucket's index above the runs: levels with periods spread evenly
/// (in ratio) between the flush and the top. Bytes and PUTs a day that
/// merging them costs, and the objects they are stored in.
fn index_upkeep(d: &Design, index_bps: f64, live_bytes: f64, window_days: f64) -> (f64, f64, f64) {
    let top = d.top_days * DAY;
    let levels = d.levels as usize;
    let period = |i: usize| match d.expiry {
        // p0 = flush .. p(levels-1) = top
        Expiry::Merge if levels == 1 => top,
        Expiry::Merge => d.flush_s * (top / d.flush_s).powf(i as f64 / (levels - 1) as f64),
        // p0 = flush .. p(levels) = top, the partition itself
        Expiry::Partitions => d.flush_s * (top / d.flush_s).powf(i as f64 / levels as f64),
    };
    let (mut up, mut puts, mut objects) = (0.0, 0.0, 0.0);
    for i in 0..levels {
        let held = if i + 1 < levels || d.expiry == Expiry::Partitions {
            index_bps * period(i + 1).min(top) / 2.0
        } else {
            live_bytes
        };
        let shards = if d.shards > 0.0 {
            d.shards
        } else {
            (held / d.shard_bytes).ceil().max(1.0)
        };
        let merges = DAY / period(i);
        up += merges * held;
        puts += merges * shards;
        objects += shards;
    }
    if d.expiry == Expiry::Partitions {
        // Older partitions are kept, not rewritten.
        objects *= (window_days / d.top_days).max(1.0);
    }
    (up, puts, objects)
}

/// A design without nodes: builders against the bucket, keys on their own
/// disks.
fn evaluate_embedded(d: &Design, classes: &[Class; 2], w: &World) -> Report {
    let mut r = Report::default();
    let slot_s = |c: &Class| w.builder_per_hour / 3600.0 * c.slot;
    let machines: f64 = classes
        .iter()
        .map(|c| c.steps_per_s * c.step_s * c.slot)
        .sum();
    let (mut key_disk, mut bucket_data, mut index_bps, mut live_index) = (0.0, 0.0, 0.0, 0.0);
    let (mut puts, mut gets, mut up, mut down, mut runs_per_s) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for (i, c) in classes.iter().enumerate() {
        if c.steps_per_s == 0.0 {
            continue;
        }
        if !d.bucket[i] {
            r.infeasible = Some(format!("{} has nowhere to go without nodes", c.name));
            r.over += 10.0;
        }
        let saved = if d.sampled[i] { c.sample_saves } else { 0.0 };
        let samples = if d.sampled[i] { c.sample_rows } else { 0.0 };
        let outputs_per_s = c.steps_per_s * c.outputs;
        let row_bytes = KEY_ROW + c.body_bytes + samples * 2.0 * KEY_ROW;
        let live = outputs_per_s * c.window_days * DAY;
        let out_bytes = c.outputs * c.out_bytes * (1.0 - saved);
        key_disk += live * row_bytes;
        live_index += live * row_bytes;
        bucket_data += outputs_per_s * c.out_bytes * (1.0 - saved) * c.window_days * DAY;
        index_bps += outputs_per_s * row_bytes + live * c.touched / DAY * KEY_ROW;

        // Lookups are local. Inputs and outputs cross to the bucket; an
        // output asked for before the keys have synced costs one GET.
        let mut wait = nd().ssd_lookup_s
            + BUCKET_GET_S
            + c.fetches * c.fetch_bytes / BUCKET_DOWN_BPS
            + RACED * BUCKET_GET_S;
        if d.sampled[i] && c.sample_load_bytes > 0.0 {
            wait += BUCKET_GET_S;
        }
        let write = 2.0 * BUCKET_PUT_S + out_bytes / BUCKET_UP_BPS;
        wait += if d.write_behind { RACED * write } else { write };
        r.wait_s[i] = wait;
        r.terms[i] = wait * c.steps_per_s * DAY * slot_s(c);

        // A pack and a run per step, or per machine and flush when the
        // step does not wait for them.
        let class_machines = c.steps_per_s * c.step_s * c.slot;
        let writes = if d.write_behind {
            (c.steps_per_s).min(class_machines / d.flush_s)
        } else {
            c.steps_per_s
        };
        runs_per_s += writes;
        puts += 2.0 * writes * DAY;
        gets += c.steps_per_s * (c.fetches + RACED) * DAY;
        up += outputs_per_s * c.out_bytes * (1.0 - saved) * DAY;
        down += c.steps_per_s * c.fetches * c.fetch_bytes * DAY;
    }
    // One builder merges; every builder lists once a flush and fetches what
    // is new: the runs, or the first level's shards if those are fewer.
    let (merged, merge_puts, _) = index_upkeep(d, index_bps, live_index, classes[1].window_days);
    let first_level = if d.shards > 0.0 { d.shards } else { 1.0 };
    let syncs = machines * DAY / d.flush_s;
    puts += merge_puts + syncs; // a LIST is priced as a PUT
    gets += syncs * (runs_per_s * d.flush_s).min(first_level).max(1.0);
    // A row comes down once as a run and once more from every level.
    down += machines * index_bps * DAY * (1.0 + d.levels);
    up += index_bps * DAY + merged;
    r.puts_per_day = puts;
    r.up_gb_per_day = up / GB;
    r.index_up_gb_per_day = (index_bps * DAY + merged) / GB;
    r.ssd_gb = key_disk / GB;
    r.terms[2] = machines * key_disk / GB * w.ssd_gb_day;
    r.terms[3] = puts * w.put + gets * w.get;
    r.terms[4] = (bucket_data + live_index) / GB * w.store_gb_day;
    r.terms[5] = up / GB * w.upload_gb + down / GB * w.download_gb;
    if up / DAY > UPLINK_BPS {
        r.over += up / DAY / UPLINK_BPS;
        r.infeasible = Some(format!("the uplink would carry {:.0} MB/s", up / DAY / 1e6));
    }
    r
}

fn evaluate(d: &Design, classes: &[Class; 2], w: &World, rpc: &Rpc) -> Report {
    if d.embedded {
        return evaluate_embedded(d, classes, w);
    }
    let mut r = Report::default();
    let slot_s = |c: &Class| w.builder_per_hour / 3600.0 * c.slot;
    // Share of a class's blobs one node holds, and of all keys.
    let share = |i: usize| d.replicas[i] / d.nodes;
    let active = |i: usize| classes[i].steps_per_s > 0.0;
    let key_copies = if d.index_everywhere {
        d.nodes
    } else {
        (0..2)
            .filter(|i| active(*i))
            .map(|i| d.replicas[i])
            .fold(1.0, f64::max)
    };
    let key_share = key_copies / d.nodes;

    // ---- what there is to hold
    let mut rows = 0.0; // live index rows
    let mut body_bytes = 0.0; // live index bodies
    let mut node_data = 0.0; // blob bytes on one node
    let mut bucket_data = 0.0; // blob bytes in the bucket
    let mut up_bps = 0.0; // blob bytes to the bucket
    let mut data_puts = 0.0;
    let mut index_bps = 0.0; // index bytes written, before merging
    let mut bucket_rows = 0.0;
    let mut bucket_index = 0.0;
    let mut cpu = 0.0;
    let mut miss_bps = 0.0;
    let mut inline = [(0.0, 0.0); 2];
    for (i, c) in classes.iter().enumerate() {
        if c.steps_per_s == 0.0 {
            continue;
        }
        inline[i] = below(c.out_bytes, c.size_sigma, d.inline_below);
        let (inline_share, inline_bytes) = inline[i];
        let saved = if d.sampled[i] { c.sample_saves } else { 0.0 };
        let outputs_per_s = c.steps_per_s * c.outputs;
        let samples = if d.sampled[i] { c.sample_rows } else { 0.0 };
        // A compact row needs the output to be one blob in one place.
        let compact = i == 0 && d.compact_fine && !d.sampled[i];
        let body = if compact { 0.0 } else { c.body_bytes };
        let row_bytes = KEY_ROW + body + inline_bytes + samples * 2.0 * KEY_ROW;
        let live = outputs_per_s * c.window_days * DAY;
        let delta = i == 0 && d.delta_fine && !d.sampled[i];
        let kept = if delta {
            *DELTA_SHARE.get().unwrap_or(&0.5)
        } else {
            1.0
        };
        let blob_bps = outputs_per_s * (c.out_bytes * (1.0 - saved) - inline_bytes) * kept;
        rows += live * (1.0 + samples);
        body_bytes += live * (row_bytes - KEY_ROW * (1.0 + samples));

        if !d.bucket[i] && d.cache_days[i] < c.window_days {
            r.over += 10.0;
            r.infeasible = Some(format!(
                "{} is in no bucket and expires on the nodes",
                c.name
            ));
        }
        node_data += blob_bps * d.cache_days[i].min(c.window_days) * DAY * share(i);
        // Uses and what they write: a touch row, or the row again.
        let used = live * c.touched / DAY;
        let rewrites = match d.expiry {
            Expiry::Merge => used * KEY_ROW,
            Expiry::Partitions => {
                live * (c.touched * d.top_days).min(1.0) / (d.top_days * DAY) * row_bytes
            }
        };
        if d.bucket[i] {
            bucket_data += blob_bps * c.window_days * DAY;
            up_bps += blob_bps;
            let writes = c.steps_per_s * DAY;
            data_puts += if d.batched[i] {
                (blob_bps * DAY / PACK_BYTES).max((d.nodes * DAY / d.flush_s).min(writes))
            } else {
                writes
            };
            index_bps += outputs_per_s * row_bytes + rewrites;
            bucket_rows += live * (1.0 + samples);
            bucket_index += live * row_bytes;
        }

        // ---- a step's wait
        let keys = c.lookups
            + if d.sampled[i] {
                c.outputs * c.sample_lookups
            } else {
                0.0
            };
        let fan = |keys: f64| {
            if d.index_everywhere {
                1.0
            } else {
                keys.min(d.nodes)
            }
        };
        let disk = if d.keys_on_ssd {
            nd().ssd_lookup_s
        } else {
            0.0
        };
        let mut wait = rpc.slowest_of(fan(c.lookups)) + disk;
        let hit = if d.cache_days[i] >= c.window_days {
            1.0
        } else {
            1.0 - (-d.cache_days[i] / c.fetch_age_days).exp()
        };
        let fetched = c.fetches * (1.0 - inline_share);
        let fetch_bytes = fetched * c.fetch_bytes;
        // Fetched with the lookup, a blob the asked node lacks costs a hop.
        wait += if d.fetch_with_lookup {
            (1.0 - share(i)) * rpc.slowest_of(1.0)
        } else {
            rpc.slowest_of(fetched)
        } + fetch_bytes / NODE_BPS
            + (1.0 - hit.powf(fetched)) * BUCKET_GET_S
            + (1.0 - hit) * fetch_bytes / BUCKET_DOWN_BPS;
        // A delta read back from the bucket needs its base as well.
        let from_bucket = if delta { kept + 1.0 } else { 1.0 };
        if delta {
            wait += (1.0 - hit.powf(fetched)) * BUCKET_GET_S
                + (1.0 - hit) * fetch_bytes * kept / BUCKET_DOWN_BPS
                + fetch_bytes * RAW_PER_STORED / DELTA_UNPACK_BPS;
            cpu += c.steps_per_s * fetch_bytes * RAW_PER_STORED / DELTA_UNPACK_BPS
                + outputs_per_s * c.out_bytes * RAW_PER_STORED / DELTA_PACK_BPS;
        }
        miss_bps += c.steps_per_s * (1.0 - hit) * fetch_bytes * from_bucket;
        if d.sampled[i] {
            wait += rpc.slowest_of(fan(c.outputs * c.sample_lookups)) + disk;
            if c.sample_load_bytes > 0.0 {
                wait += rpc.slowest_of(1.0);
            }
        }
        let out_bytes = c.outputs * c.out_bytes * (1.0 - saved);
        let mut write = rpc.slowest_of(d.replicas[i]) + out_bytes / NODE_BPS;
        if d.ack_bucket {
            let batch = (blob_bps / d.nodes * d.flush_s)
                .min(PACK_BYTES)
                .max(out_bytes);
            write += d.flush_s / 2.0 + BUCKET_PUT_S + batch / BUCKET_UP_BPS;
        }
        wait += if d.write_behind { RACED * write } else { write };
        r.wait_s[i] = wait;

        cpu += c.steps_per_s
            * keys
            * if d.keys_on_ssd {
                CPU_KEY_SSD_S
            } else {
                nd().key_ram_s
            }
            + outputs_per_s * CPU_WRITE_S * key_copies
            + (c.steps_per_s * fetch_bytes + outputs_per_s * out_bytes * d.replicas[i])
                * nd().byte_s;
    }

    // ---- nodes
    let keys_bytes = rows * KEY_ROW * key_share;
    let ram = NODE_RAM_BASE
        + if d.keys_on_ssd {
            keys_bytes / 16.0
        } else {
            keys_bytes
        };
    let ssd = node_data + body_bytes * key_share + keys_bytes;
    r.ram_gb = ram / GB;
    r.ssd_gb = ssd / GB;
    if ram > NODE_RAM_MAX {
        r.over += ram / NODE_RAM_MAX;
        r.infeasible = Some(format!("a node needs {:.0} GB of memory", ram / GB));
    }
    if ssd > NODE_SSD_MAX {
        r.over += ssd / NODE_SSD_MAX;
        r.infeasible = Some(format!("a node needs {:.0} GB of disk", ssd / GB));
    }
    let machine = if d.hosted { 0.0 } else { w.node_per_day };
    r.terms[2] = d.nodes * (machine + ram / GB * w.ram_gb_day + ssd / GB * w.ssd_gb_day);

    let (merged, merge_puts, index_objects) =
        index_upkeep(d, index_bps, bucket_index, classes[1].window_days);
    let index_up = index_bps * DAY + merged; // the runs, then the levels
    let index_puts = d.nodes * DAY / d.flush_s + merge_puts;
    cpu += index_up / DAY * nd().merge_byte_s;
    r.load = cpu / (d.nodes * NODE_CORES);
    if r.load > MAX_LOAD {
        r.over += r.load / MAX_LOAD;
        r.infeasible = Some(format!("nodes are {:.0}% busy", r.load * 100.0));
    }
    let queue = 1.0 / (1.0 - r.load.min(0.99));
    for (i, c) in classes.iter().enumerate() {
        // Only the node requests queue; the bucket's share of the wait is small.
        r.wait_s[i] *= if d.ack_bucket { 1.0 } else { queue };
        r.terms[i] = r.wait_s[i] * c.steps_per_s * DAY * slot_s(c);
    }
    r.index_up_gb_per_day = index_up / GB;
    r.up_gb_per_day = (up_bps * DAY + index_up) / GB;
    r.puts_per_day = data_puts + index_puts;
    if up_bps + index_up / DAY > UPLINK_BPS {
        r.over += (up_bps + index_up / DAY) / UPLINK_BPS;
        r.infeasible = Some(format!(
            "the uplink would carry {:.0} MB/s",
            (up_bps + index_up / DAY) / 1e6
        ));
    }

    // ---- restarts, lost nodes
    let key_load = bucket_rows * KEY_ROW * key_share;
    r.restart_s = if d.local_checkpoint || d.keys_on_ssd {
        5.0 + if d.keys_on_ssd {
            0.0
        } else {
            keys_bytes / nd().ssd_read_bps
        }
    } else {
        index_objects * key_share / 32.0 * BUCKET_GET_S
            + key_load / (32.0 * BUCKET_DOWN_BPS).min(1e9)
    };
    let restart_gets = if d.local_checkpoint || d.keys_on_ssd {
        0.0
    } else {
        index_objects * key_share
    };
    let restart_bytes = if restart_gets > 0.0 { key_load } else { 0.0 };
    let restarts = d.nodes * w.restarts;
    r.terms[3] = r.puts_per_day * w.put + (miss_bps * DAY / 1e6 + restarts * restart_gets) * w.get;
    r.terms[4] = (bucket_data + bucket_index) / GB * w.store_gb_day;
    r.terms[5] = r.up_gb_per_day * w.upload_gb;
    r.terms[5] += (miss_bps * DAY + restarts * restart_bytes) / GB * w.download_gb;

    // A blob is gone when every node that holds it is lost before it is in
    // the bucket (or at all, for a class the bucket never gets).
    let recopy_days = node_data / NODE_BPS / DAY;
    let losses = d.nodes * w.node_losses;
    let mut redo = 0.0;
    let mut redone = 0.0;
    for (i, c) in classes.iter().enumerate() {
        if !active(i) {
            continue;
        }
        let copies = d.replicas[i];
        let outputs_per_s = c.steps_per_s * c.outputs;
        let unsaved = if d.bucket[i] {
            if d.ack_bucket {
                0.0
            } else {
                outputs_per_s * (d.flush_s + BUCKET_PUT_S)
            }
        } else {
            outputs_per_s * c.window_days * DAY
        };
        let others = (w.node_losses * recopy_days * (copies - 1.0)).powf(copies - 1.0);
        let lost = w.disasters * unsaved + losses * unsaved * share(i) * others;
        redone += lost * c.needed * 365.0;
        redo += lost * c.needed * c.step_s * slot_s(c);
        if copies == 1.0 {
            let touches = 1.0 - (1.0 - 1.0 / d.nodes).powf(c.fetches);
            let waiting = c.steps_per_s * touches * slot_s(c);
            if d.bucket[i] {
                // The lost node's blobs come from the bucket until its
                // replacement has fetched them again.
                let kept = c.steps_per_s
                    * c.outputs
                    * c.out_bytes
                    * d.cache_days[i].min(c.window_days)
                    * DAY;
                r.terms[5] += losses * kept * share(i) / GB * w.download_gb;
                r.terms[7] +=
                    losses * REPLACE_S * waiting * (BUCKET_GET_S + c.fetch_bytes / BUCKET_DOWN_BPS);
            } else {
                // A restarting node's blobs are out of reach.
                r.terms[7] += restarts * r.restart_s * waiting * r.restart_s / 2.0;
            }
        }
    }
    // With one copy of the keys, nobody can look anything up while its node
    // is away: every machine idles.
    if key_copies == 1.0 {
        let idle: f64 = classes
            .iter()
            .map(|c| c.steps_per_s * c.step_s * slot_s(c))
            .sum();
        let away = if d.nodes == 1.0 {
            REPLACE_S
        } else {
            r.restart_s.max(60.0)
        };
        r.terms[7] += (restarts * r.restart_s + losses * away) * idle;
    }
    r.terms[6] = redo;
    r.redone_steps_per_year = redone;
    r
}

// ------------------------------------------------------------ search

type Mutation = (&'static str, fn(&mut Design, &[Class; 2]));

fn mutations() -> Vec<Mutation> {
    vec![
        ("one node more", |d, _| d.nodes += 1.0),
        ("one node fewer", |d, _| {
            d.nodes = (d.nodes - 1.0).max(d.replicas[0]).max(d.replicas[1])
        }),
        ("fine: one copy more", |d, _| {
            d.replicas[0] = (d.replicas[0] + 1.0).min(d.nodes)
        }),
        ("fine: one copy fewer", |d, _| {
            d.replicas[0] = (d.replicas[0] - 1.0).max(1.0)
        }),
        ("coarse: one copy more", |d, _| {
            d.replicas[1] = (d.replicas[1] + 1.0).min(d.nodes)
        }),
        ("coarse: one copy fewer", |d, _| {
            d.replicas[1] = (d.replicas[1] - 1.0).max(1.0)
        }),
        (
            "no nodes: builders hold the keys and use the bucket",
            |d, _| (d.embedded, d.bucket) = (true, [true, true]),
        ),
        ("cache nodes", |d, _| d.embedded = false),
        ("nodes run on builder machines", |d, _| d.hosted = true),
        ("nodes are machines of their own", |d, _| d.hosted = false),
        ("fine: one compact index row", |d, _| d.compact_fine = true),
        ("fine: index row and body", |d, _| d.compact_fine = false),
        ("fine: stored as deltas", |d, _| d.delta_fine = true),
        ("fine: stored whole", |d, _| d.delta_fine = false),
        ("inputs come with the lookup", |d, _| {
            d.fetch_with_lookup = true
        }),
        ("inputs are fetched after the lookup", |d, _| {
            d.fetch_with_lookup = false
        }),
        ("upload after the step reports", |d, _| {
            d.write_behind = true
        }),
        ("upload before the step reports", |d, _| {
            d.write_behind = false
        }),
        ("acknowledge from the bucket", |d, _| d.ack_bucket = true),
        ("acknowledge from the nodes", |d, _| d.ack_bucket = false),
        ("fine: nodes only, no bucket", |d, c| {
            (d.bucket[0], d.cache_days[0]) = (false, c[0].window_days)
        }),
        ("fine: back to the bucket", |d, _| d.bucket[0] = true),
        ("coarse: nodes only, no bucket", |d, c| {
            (d.bucket[1], d.cache_days[1]) = (false, c[1].window_days)
        }),
        ("coarse: back to the bucket", |d, _| d.bucket[1] = true),
        ("fine: nodes keep twice as long", |d, c| {
            d.cache_days[0] = (d.cache_days[0] * 2.0).min(c[0].window_days)
        }),
        ("fine: nodes keep half as long", |d, _| {
            d.cache_days[0] /= 2.0
        }),
        ("coarse: nodes keep twice as long", |d, c| {
            d.cache_days[1] = (d.cache_days[1] * 2.0).min(c[1].window_days)
        }),
        ("coarse: nodes keep half as long", |d, _| {
            d.cache_days[1] /= 2.0
        }),
        ("fine: sampled dedup on", |d, _| d.sampled[0] = true),
        ("fine: sampled dedup off", |d, _| d.sampled[0] = false),
        ("coarse: sampled dedup on", |d, _| d.sampled[1] = true),
        ("coarse: sampled dedup off", |d, _| d.sampled[1] = false),
        ("coarse: batch uploads into shared packs", |d, _| {
            d.batched[1] = true
        }),
        ("flush twice as often", |d, _| {
            d.flush_s = (d.flush_s / 2.0).max(1.0)
        }),
        ("flush half as often", |d, _| d.flush_s *= 2.0),
        ("every node holds every key", |d, _| {
            d.index_everywhere = true
        }),
        ("a node holds its slice of the keys", |d, _| {
            d.index_everywhere = false
        }),
        ("keys on disk", |d, _| d.keys_on_ssd = true),
        ("keys in memory", |d, _| d.keys_on_ssd = false),
        ("expire by deleting partitions", |d, _| {
            d.expiry = Expiry::Partitions
        }),
        ("expire by merging", |d, _| d.expiry = Expiry::Merge),
        ("one level more", |d, _| d.levels += 1.0),
        ("one level fewer", |d, _| {
            d.levels = (d.levels - 1.0).max(1.0)
        }),
        ("top period twice as long", |d, _| d.top_days *= 2.0),
        ("top period half as long", |d, _| d.top_days /= 2.0),
        ("shards sized, not counted", |d, _| d.shards = 0.0),
        ("shards four times larger", |d, _| d.shard_bytes *= 4.0),
        ("shards four times smaller", |d, _| d.shard_bytes /= 4.0),
        ("inline outputs below 1 KiB", |d, _| d.inline_below = 1024.0),
        ("inline outputs below 4 KiB", |d, _| d.inline_below = 4096.0),
        ("inline outputs below 16 KiB", |d, _| {
            d.inline_below = 16384.0
        }),
        ("inline nothing", |d, _| d.inline_below = 0.0),
        ("restart from the node's own disk", |d, _| {
            d.local_checkpoint = true
        }),
    ]
}

fn improve(classes: &[Class; 2], w: &World, rpc: &Rpc, verbose: bool) -> (Design, Report) {
    improve_within(classes, w, rpc, verbose, first_draft(), |_| true)
}

/// The search from `start`, over designs `allowed` admits.
fn improve_within(
    classes: &[Class; 2],
    w: &World,
    rpc: &Rpc,
    verbose: bool,
    start: Design,
    allowed: fn(&Design) -> bool,
) -> (Design, Report) {
    let mut design = start;
    let mut report = evaluate(&design, classes, w, rpc);
    if verbose {
        println!(
            "  {:>9}  first draft{}",
            money(report.total()),
            reason(&report)
        );
    }
    loop {
        let mut best: Option<(&str, Design, Report)> = None;
        for (name, apply) in mutations() {
            let mut next = design.clone();
            apply(&mut next, classes);
            if next == design || !allowed(&next) {
                continue;
            }
            let r = evaluate(&next, classes, w, rpc);
            if r.search_cost()
                < best
                    .as_ref()
                    .map_or(report.search_cost(), |b| b.2.search_cost())
            {
                best = Some((name, next, r));
            }
        }
        // Stop at changes worth less than a cent a day (or, from an
        // infeasible design, at none).
        let Some((name, next, r)) = best else { break };
        if report.total().is_finite() && report.total() - r.total() < 0.01 {
            break;
        }
        if verbose {
            // The term that moved most.
            let (term, delta) = (0..TERMS.len())
                .map(|i| (TERMS[i], r.terms[i] - report.terms[i]))
                .max_by(|a, b| a.1.abs().total_cmp(&b.1.abs()))
                .unwrap();
            let why = if report.total().is_finite() {
                format!("{term} {delta:+.2}")
            } else if let Some(reason) = &r.infeasible {
                format!("still infeasible: {reason}")
            } else {
                "feasible".into()
            };
            println!("  {:>9}  {name}  ({why})", money(r.total()));
        }
        (design, report) = (next, r);
    }
    (design, report)
}

fn money(v: f64) -> String {
    if v.is_finite() {
        format!("${v:.2}")
    } else {
        "-".into()
    }
}

fn reason(r: &Report) -> String {
    r.infeasible
        .as_ref()
        .map_or(String::new(), |why| format!("  (infeasible: {why})"))
}

fn describe(d: &Design, r: &Report, classes: &[Class; 2]) {
    let on = |b: bool, yes: &'static str, no: &'static str| if b { yes } else { no };
    if d.embedded {
        println!(
            "    no nodes: builders keep the keys ({:.1} GB each), sync every {:.0} s, upload {}; waits {:.0} ms (fine), {:.0} ms (coarse)",
            r.ssd_gb,
            d.flush_s,
            on(
                d.write_behind,
                "after the step reports",
                "before the step reports"
            ),
            r.wait_s[0] * 1e3,
            r.wait_s[1] * 1e3,
        );
        print!("    $/day:");
        for (name, v) in TERMS.iter().zip(r.terms) {
            print!("  {name} {v:.2}");
        }
        println!("  = {}", money(r.total()));
        return;
    }
    println!(
        "    {} nodes; inputs {}; outputs uploaded {}, acknowledged by {}; every {:.0} s to the bucket; {}; keys {}",
        d.nodes,
        on(
            d.fetch_with_lookup,
            "come with the lookup",
            "fetched after the lookup"
        ),
        on(
            d.write_behind,
            "after the step reports",
            "before the step reports"
        ),
        on(d.ack_bucket, "the bucket", "the nodes"),
        d.flush_s,
        on(
            d.index_everywhere,
            "every node holds every key",
            "keys sliced over the nodes"
        ),
        on(d.keys_on_ssd, "on disk", "in memory"),
    );
    for (i, c) in classes.iter().enumerate() {
        if c.steps_per_s == 0.0 {
            continue;
        }
        println!(
            "    {:<6} {}, {} copies on nodes for {} of {} days, dedup {}, uploads {}; waits {:.1} ms a step ({:.3}% of it)",
            c.name,
            on(d.bucket[i], "in the bucket", "on nodes only"),
            d.replicas[i],
            d.cache_days[i],
            c.window_days,
            if d.sampled[i] {
                "sampled"
            } else if i == 0 && d.delta_fine && d.compact_fine {
                "delta to the last object, one index row"
            } else if i == 0 && d.delta_fine {
                "delta to the last object"
            } else if i == 0 && d.compact_fine {
                "whole blob, one index row"
            } else {
                "whole blob"
            },
            on(d.batched[i], "batched", "one pack each"),
            r.wait_s[i] * 1e3,
            r.wait_s[i] / c.step_s * 100.0,
        );
    }
    println!(
        "    index: {:?}, {} levels under a top of {} days, shards {}, inline below {} B, restart from {}",
        d.expiry,
        d.levels,
        d.top_days,
        if d.shards > 0.0 {
            format!("{} a level", d.shards)
        } else {
            format!("of {:.0} MiB", d.shard_bytes / 1048576.0)
        },
        d.inline_below,
        on(
            d.local_checkpoint || d.keys_on_ssd,
            "own disk",
            "the bucket"
        ),
    );
    println!(
        "    a node: {:.0} GB memory, {:.0} GB disk, {:.1}% busy, back {:.0} s after a restart; bucket: {:.0} PUTs and {:.0} GB a day ({:.0} GB of it index); {:.0} steps redone a year",
        r.ram_gb,
        r.ssd_gb,
        r.load * 100.0,
        r.restart_s,
        r.puts_per_day,
        r.up_gb_per_day,
        r.index_up_gb_per_day,
        r.redone_steps_per_year,
    );
    print!("    $/day:");
    for (name, v) in TERMS.iter().zip(r.terms) {
        print!("  {name} {v:.2}");
    }
    println!("  = {}", money(r.total()));
}

fn main() {
    let (mut fine_machines, mut coarse_machines) = (450.0, 50.0);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--measured" {
            USE_MEASURED.set(true).ok();
            continue;
        }
        let value: f64 = args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
            eprintln!("{flag} takes a number");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--fine-machines" => fine_machines = value,
            "--coarse-machines" => coarse_machines = value,
            "--object-bytes" => {
                OBJECT_BYTES.set(value).ok();
            }
            "--step-s" => {
                FINE_STEP_S.set(value).ok();
            }
            "--lookups" => {
                FINE_LOOKUPS.set(value).ok();
            }
            "--delta-share" => {
                DELTA_SHARE.set(value).ok();
            }
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    let rpc = Rpc::new();
    let mixed = [fine(fine_machines), coarse(coarse_machines)];
    let all = fine_machines + coarse_machines;

    println!("{fine_machines} machines compile objects, {coarse_machines} build packages\n");
    let draft = first_draft();
    println!("first draft");
    describe(&draft, &evaluate(&draft, &mixed, &WORLD, &rpc), &mixed);
    println!(
        "\nsearch, one change a line: cost a day after it, the change, the term that moved most"
    );
    let (design, report) = improve(&mixed, &WORLD, &rpc, true);
    println!("\nresult");
    describe(&design, &report, &mixed);

    println!("\nthe same search elsewhere");
    let scenarios: [(&str, [Class; 2], World); 6] = [
        ("objects only", [fine(all), coarse(0.0)], WORLD),
        ("packages only", [fine(0.0), coarse(all)], WORLD),
        (
            "builders at a quarter of the price",
            mixed,
            World {
                builder_per_hour: 0.25,
                ..WORLD
            },
        ),
        (
            "failures ten times as frequent",
            mixed,
            World {
                restarts: WORLD.restarts * 10.0,
                node_losses: WORLD.node_losses * 10.0,
                disasters: WORLD.disasters * 10.0,
                ..WORLD
            },
        ),
        (
            "bucket ten times the price",
            mixed,
            World {
                put: WORLD.put * 10.0,
                get: WORLD.get * 10.0,
                store_gb_day: WORLD.store_gb_day * 10.0,
                download_gb: WORLD.download_gb * 10.0,
                upload_gb: WORLD.upload_gb * 10.0,
                ..WORLD
            },
        ),
        (
            "objects kept 14 days",
            [
                Class {
                    window_days: 14.0,
                    ..fine(fine_machines)
                },
                coarse(coarse_machines),
            ],
            WORLD,
        ),
    ];
    for (name, classes, world) in scenarios {
        let (d, r) = improve(&classes, &world, &rpc, false);
        // What the mixed cluster's design would cost here.
        let transplanted = evaluate(&design, &classes, &world, &rpc);
        println!(
            "  {name}: {} a day; the result above would cost {}{}",
            money(r.total()),
            money(transplanted.total()),
            reason(&transplanted)
        );
        describe(&d, &r, &classes);
    }

    // The same mix at other sizes, the search confined to one deployment
    // at a time.
    println!(
        "\nother cluster sizes, nine machines in ten compiling objects: $ a day by deployment"
    );
    println!(
        "  {:>8} | {:>9} {:>9} {:>9} | {:>7} | best: nodes, copies (fine, coarse), fine kept in, waits fine / coarse",
        "machines", "no nodes", "one node", "two up", "of cost"
    );
    let modes: [fn(&Design) -> bool; 3] = [
        |d| d.embedded,
        |d| !d.embedded && d.nodes == 1.0,
        |d| !d.embedded && d.nodes >= 2.0,
    ];
    for machines in [1.0, 3.0, 10.0, 30.0, 100.0, 300.0, 500.0, 2000.0] {
        let classes = [fine(machines * 0.9), coarse(machines * 0.1)];
        let results = modes.map(|allowed| {
            let mut start = first_draft();
            start.embedded = allowed(&Design {
                embedded: true,
                ..first_draft()
            });
            start.nodes = if allowed(&Design {
                nodes: 1.0,
                replicas: [1.0, 1.0],
                ..first_draft()
            }) {
                start.replicas = [1.0, 1.0];
                1.0
            } else {
                4.0
            };
            improve_within(&classes, &WORLD, &rpc, false, start, allowed)
        });
        let (d, r) = results
            .iter()
            .min_by(|a, b| a.1.total().total_cmp(&b.1.total()))
            .unwrap();
        if std::env::var("DETAIL").is_ok() {
            for (d, r) in &results {
                describe(d, r, &classes);
            }
        }
        let builders = machines * 24.0 * WORLD.builder_per_hour;
        println!(
            "  {:>8} | {:>9} {:>9} {:>9} | {:>6.2}% | {}, ({}, {}), {}, {:.0} / {:.0} ms",
            machines,
            money(results[0].1.total()),
            money(results[1].1.total()),
            money(results[2].1.total()),
            r.total() / builders * 100.0,
            if d.embedded {
                "none".to_string()
            } else {
                format!("{}{}", d.nodes, if d.hosted { " on builders" } else { "" })
            },
            d.replicas[0],
            d.replicas[1],
            if d.embedded || d.bucket[0] {
                "bucket"
            } else {
                "nodes only"
            },
            r.wait_s[0] * 1e3,
            r.wait_s[1] * 1e3,
        );
    }
}
