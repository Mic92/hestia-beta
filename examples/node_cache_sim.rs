//! Which packs should a cache node keep when its disk is smaller than what
//! the builders write?
//!
//!   cargo run --release --example node_cache_sim -- --trace trace.tsv --commits commits.tsv
//!       [--shards 4] [--window 100]
//!
//! The trace is a real one: every commit of a stretch of git's history,
//! topic branches included, built with a derivation per compiled object.
//! `trace.tsv` has a line per output of every commit (commit, path, hash of
//! the inputs, hash of the bytes, size); `commits.tsv` has a line per commit
//! (hash, parents, tab, commit time).
//!
//! CI builds every commit once, in the order they were committed. A step
//! whose inputs hash was built before is not run. A step that runs does so
//! on a builder with an empty store, so it reads its inputs from the node:
//! an archive reads its objects, the link reads the archives and the other
//! objects, and `--shards` test jobs each read the linked binary. A step's
//! output is written through the node unless the same bytes are stored
//! already. Sources are not in the trace, so compiles read nothing.
//!
//! The node keeps what fits its disk. A read it cannot serve goes to the
//! bucket. Compared, at several disk sizes:
//!
//!   reads only   least recently used, but an upload is not kept
//!   lru          least recently used, uploads kept
//!   fifo         oldest first, reads do not count
//!   sieve        oldest first, but one read buys a second round
//!   s3-fifo      new entries wait in a tenth of the disk and move to the
//!                rest only when read there
//!   foresight    evicts what is read again latest: not buildable, a bound
//!
//! each with an output as the unit of eviction, and the buildable ones again
//! with a pack, here everything one CI run wrote.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy)]
enum Event {
    Write(u32),
    Read(u32),
}

struct Trace {
    events: Vec<Event>,
    /// Bytes of each output.
    size: Vec<u64>,
    /// The CI run that wrote each output.
    run_of: Vec<u32>,
    runs: usize,
    steps_run: usize,
    steps_cached: usize,
}

fn group(path: &str) -> &'static str {
    if path.starts_with("xdiff/") {
        "xdiff/lib.a"
    } else if path.starts_with("reftable/") {
        "reftable/libreftable.a"
    } else if path.starts_with("builtin/") || path == "git.o" || path == "common-main.o" {
        "git"
    } else {
        "libgit.a"
    }
}

fn key_of(parts: &BTreeSet<String>) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut h);
    format!("{:016x}", h.finish())
}

fn load(trace: &str, commits: &str, shards: usize) -> Trace {
    let mut order: Vec<(u64, usize, String)> = Vec::new();
    let text = std::fs::read_to_string(commits).expect("read commits");
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        let (left, time) = line.split_once('\t').expect("commit line");
        let hash = left.split(' ').next().unwrap().to_string();
        // `git log` prints children first: among equal times, later lines build first.
        order.push((time.parse().expect("time"), lines.len() - i, hash));
    }
    order.sort();

    // commit -> path -> (key, bytes hash, size)
    let mut rows: HashMap<String, Vec<(String, String, String, u64)>> = HashMap::new();
    for line in std::fs::read_to_string(trace).expect("read trace").lines() {
        let f: Vec<&str> = line.split('\t').collect();
        rows.entry(f[0].to_string()).or_default().push((
            f[1].to_string(),
            f[2].to_string(),
            f[3].to_string(),
            f[4].parse().expect("size"),
        ));
    }

    let mut ids: HashMap<String, u32> = HashMap::new();
    let mut out = Trace {
        events: Vec::new(),
        size: Vec::new(),
        run_of: Vec::new(),
        runs: 0,
        steps_run: 0,
        steps_cached: 0,
    };
    let mut built: HashSet<String> = HashSet::new();
    for (_, _, commit) in &order {
        let Some(outputs) = rows.get(commit) else {
            continue; // did not build
        };
        let run = out.runs as u32;
        out.runs += 1;
        let mut id = |bytes: &str, size: u64, out: &mut Trace| -> (u32, bool) {
            if let Some(&id) = ids.get(bytes) {
                return (id, false);
            }
            let id = out.size.len() as u32;
            ids.insert(bytes.to_string(), id);
            out.size.push(size);
            out.run_of.push(run);
            (id, true)
        };
        // Compiles.
        let mut members: BTreeMap<&str, (BTreeSet<String>, Vec<u32>)> = BTreeMap::new();
        let mut files: HashMap<&str, (u32, bool)> = HashMap::new();
        for (path, key, bytes, size) in outputs {
            let (object, new) = id(bytes, *size, &mut out);
            if !path.ends_with(".o") {
                files.insert(path.as_str(), (object, new));
                continue;
            }
            if built.insert(key.clone()) {
                out.steps_run += 1;
                if new {
                    out.events.push(Event::Write(object));
                }
            } else {
                out.steps_cached += 1;
            }
            let entry = members.entry(group(path)).or_default();
            entry.0.insert(key.clone());
            entry.1.push(object);
        }
        // Archives, then the link, which takes the archives as inputs.
        let (mut link_keys, mut link_inputs) = members.remove("git").unwrap_or_default();
        for (archive, (keys, inputs)) in &members {
            let key = format!("{archive} {}", key_of(keys));
            link_keys.insert(key.clone());
            let Some(&(object, new)) = files.get(archive) else {
                continue;
            };
            link_inputs.push(object);
            if built.insert(key) {
                out.steps_run += 1;
                out.events.extend(inputs.iter().map(|&o| Event::Read(o)));
                if new {
                    out.events.push(Event::Write(object));
                }
            } else {
                out.steps_cached += 1;
            }
        }
        if let Some(&(object, new)) = files.get("git") {
            if built.insert(format!("git {}", key_of(&link_keys))) {
                out.steps_run += 1;
                out.events
                    .extend(link_inputs.iter().map(|&o| Event::Read(o)));
                if new {
                    out.events.push(Event::Write(object));
                }
            } else {
                out.steps_cached += 1;
            }
            for _ in 0..shards {
                out.events.push(Event::Read(object));
            }
        }
    }
    out
}

/// What decides the order of eviction. Units are outputs or packs.
trait Policy {
    fn insert(&mut self, unit: u32, bytes: u64);
    fn hit(&mut self, unit: u32);
    /// The unit grew while stored (a pack got another of its outputs back).
    fn grew(&mut self, _unit: u32, _bytes: u64) {}
    /// Picks the unit to drop and forgets it. `bytes` is its size.
    fn victim(&mut self, bytes: &dyn Fn(u32) -> u64) -> u32;
    /// Told the position of the event about to be played.
    fn at(&mut self, _position: usize) {}
}

#[derive(Default)]
struct Lru {
    tick: u64,
    last: HashMap<u32, u64>,
    by_tick: BTreeMap<u64, u32>,
}

impl Policy for Lru {
    fn insert(&mut self, unit: u32, _: u64) {
        self.tick += 1;
        self.last.insert(unit, self.tick);
        self.by_tick.insert(self.tick, unit);
    }
    fn hit(&mut self, unit: u32) {
        if let Some(old) = self.last.remove(&unit) {
            self.by_tick.remove(&old);
        }
        self.insert(unit, 0);
    }
    fn victim(&mut self, _: &dyn Fn(u32) -> u64) -> u32 {
        let (_, unit) = self.by_tick.pop_first().expect("something to evict");
        self.last.remove(&unit);
        unit
    }
}

#[derive(Default)]
struct Fifo(VecDeque<u32>);

impl Policy for Fifo {
    fn insert(&mut self, unit: u32, _: u64) {
        self.0.push_back(unit);
    }
    fn hit(&mut self, _: u32) {}
    fn victim(&mut self, _: &dyn Fn(u32) -> u64) -> u32 {
        self.0.pop_front().expect("something to evict")
    }
}

/// A queue from old to new with a hand that walks it from the old end,
/// dropping the first entry not read since the hand last passed it.
#[derive(Default)]
struct Sieve {
    older: HashMap<u32, u32>,
    newer: HashMap<u32, u32>,
    oldest: Option<u32>,
    newest: Option<u32>,
    hand: Option<u32>,
    read: HashSet<u32>,
}

impl Policy for Sieve {
    fn insert(&mut self, unit: u32, _: u64) {
        match self.newest {
            Some(n) => {
                self.newer.insert(n, unit);
                self.older.insert(unit, n);
            }
            None => self.oldest = Some(unit),
        }
        self.newest = Some(unit);
    }
    fn hit(&mut self, unit: u32) {
        self.read.insert(unit);
    }
    fn victim(&mut self, _: &dyn Fn(u32) -> u64) -> u32 {
        let mut at = self.hand.or(self.oldest).expect("something to evict");
        while self.read.remove(&at) {
            at = self.newer.get(&at).copied().or(self.oldest).unwrap();
        }
        let older = self.older.remove(&at);
        let newer = self.newer.remove(&at);
        match older {
            Some(o) => match newer {
                Some(n) => {
                    self.newer.insert(o, n);
                }
                None => {
                    self.newer.remove(&o);
                }
            },
            None => self.oldest = newer,
        }
        match newer {
            Some(n) => match older {
                Some(o) => {
                    self.older.insert(n, o);
                }
                None => {
                    self.older.remove(&n);
                }
            },
            None => self.newest = older,
        }
        self.hand = newer;
        at
    }
}

/// New entries wait in a small queue; only those read there reach the main
/// one. An entry dropped from the small queue is remembered for a while and
/// goes straight to the main queue if it comes back.
struct S3Fifo {
    small_target: u64,
    small_bytes: u64,
    small: VecDeque<u32>,
    main: VecDeque<u32>,
    in_small: HashSet<u32>,
    reads: HashMap<u32, u8>,
    ghosts: HashSet<u32>,
    ghost_order: VecDeque<u32>,
}

impl S3Fifo {
    fn new(capacity: u64) -> Self {
        S3Fifo {
            small_target: capacity / 10,
            small_bytes: 0,
            small: VecDeque::new(),
            main: VecDeque::new(),
            in_small: HashSet::new(),
            reads: HashMap::new(),
            ghosts: HashSet::new(),
            ghost_order: VecDeque::new(),
        }
    }
}

impl Policy for S3Fifo {
    fn insert(&mut self, unit: u32, bytes: u64) {
        self.reads.insert(unit, 0);
        if self.ghosts.remove(&unit) {
            self.main.push_back(unit);
        } else {
            self.small.push_back(unit);
            self.in_small.insert(unit);
            self.small_bytes += bytes;
        }
    }
    fn hit(&mut self, unit: u32) {
        let reads = self.reads.entry(unit).or_default();
        *reads = (*reads + 1).min(3);
    }
    fn grew(&mut self, unit: u32, bytes: u64) {
        if self.in_small.contains(&unit) {
            self.small_bytes += bytes;
        }
    }
    fn victim(&mut self, bytes: &dyn Fn(u32) -> u64) -> u32 {
        loop {
            if self.small_bytes > self.small_target || self.main.is_empty() {
                let unit = self.small.pop_front().expect("something to evict");
                self.in_small.remove(&unit);
                self.small_bytes -= bytes(unit);
                if self.reads.get(&unit).copied().unwrap_or(0) > 0 {
                    self.reads.insert(unit, 0);
                    self.main.push_back(unit);
                    continue;
                }
                self.reads.remove(&unit);
                self.ghosts.insert(unit);
                self.ghost_order.push_back(unit);
                // Remember about as many as the main queue holds.
                while self.ghost_order.len() > self.main.len().max(64) {
                    let old = self.ghost_order.pop_front().unwrap();
                    self.ghosts.remove(&old);
                }
                return unit;
            }
            let unit = self.main.pop_front().unwrap();
            let reads = self.reads.get(&unit).copied().unwrap_or(0);
            if reads > 0 {
                self.reads.insert(unit, reads - 1);
                self.main.push_back(unit);
                continue;
            }
            self.reads.remove(&unit);
            return unit;
        }
    }
}

/// Knows when every output is read next.
struct Foresight {
    /// For each event, the position of the next read of the same output.
    next_read: Vec<usize>,
    position: usize,
    of: HashMap<u32, usize>,
    order: BTreeSet<(usize, u32)>,
}

impl Foresight {
    fn new(events: &[Event], outputs: usize) -> Self {
        let mut next_read = vec![usize::MAX; events.len()];
        let mut seen = vec![usize::MAX; outputs];
        for (i, event) in events.iter().enumerate().rev() {
            let (Event::Read(o) | Event::Write(o)) = *event;
            next_read[i] = seen[o as usize];
            if matches!(event, Event::Read(_)) {
                seen[o as usize] = i;
            }
        }
        Foresight {
            next_read,
            position: 0,
            of: HashMap::new(),
            order: BTreeSet::new(),
        }
    }
}

impl Policy for Foresight {
    fn at(&mut self, position: usize) {
        self.position = position;
    }
    fn insert(&mut self, unit: u32, _: u64) {
        let next = self.next_read[self.position];
        self.of.insert(unit, next);
        self.order.insert((next, unit));
    }
    fn hit(&mut self, unit: u32) {
        if let Some(old) = self.of.remove(&unit) {
            self.order.remove(&(old, unit));
        }
        self.insert(unit, 0);
    }
    fn victim(&mut self, _: &dyn Fn(u32) -> u64) -> u32 {
        let (_, unit) = self.order.pop_last().expect("something to evict");
        self.of.remove(&unit);
        unit
    }
}

#[derive(Default, Clone, Copy)]
struct Outcome {
    read: u64,
    from_bucket: u64,
    reads: u64,
    misses: u64,
}

/// Plays the trace against a node with `capacity` bytes of disk.
fn play(
    trace: &Trace,
    capacity: u64,
    policy: &mut dyn Policy,
    keep_uploads: bool,
    packs: bool,
    skip: usize,
) -> Outcome {
    let unit_of = |o: u32| if packs { trace.run_of[o as usize] } else { o };
    let mut stored = vec![false; trace.size.len()];
    let mut held: HashMap<u32, (u64, Vec<u32>)> = HashMap::new();
    let mut used = 0u64;
    let mut out = Outcome::default();
    for (position, event) in trace.events.iter().enumerate() {
        policy.at(position);
        let (object, read) = match *event {
            Event::Read(o) => (o, true),
            Event::Write(o) => (o, false),
        };
        let size = trace.size[object as usize];
        let unit = unit_of(object);
        if read && position >= skip {
            out.read += size;
            out.reads += 1;
            if !stored[object as usize] {
                out.from_bucket += size;
                out.misses += 1;
            }
        }
        if stored[object as usize] {
            if read {
                policy.hit(unit);
            }
            continue;
        }
        if (!read && !keep_uploads) || size > capacity {
            continue;
        }
        stored[object as usize] = true;
        used += size;
        match held.get_mut(&unit) {
            Some(entry) => {
                entry.0 += size;
                entry.1.push(object);
                policy.grew(unit, size);
                if read {
                    policy.hit(unit);
                }
            }
            None => {
                held.insert(unit, (size, vec![object]));
                policy.insert(unit, size);
            }
        }
        while used > capacity {
            let victim = policy.victim(&|u| held.get(&u).map_or(0, |e| e.0));
            let (bytes, objects) = held.remove(&victim).expect("victim is held");
            used -= bytes;
            for o in objects {
                stored[o as usize] = false;
            }
        }
    }
    out
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1e6
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let trace_path = arg("--trace").expect("--trace <trace.tsv>");
    let commits_path = arg("--commits").expect("--commits <commits.tsv>");
    let shards: usize = arg("--shards").map_or(4, |s| s.parse().expect("--shards"));
    let window: usize = arg("--window").map_or(100, |s| s.parse().expect("--window"));
    let trace = load(&trace_path, &commits_path, shards);

    // What the trace looks like.
    let mut written = 0u64;
    let mut read = 0u64;
    let mut reads_of = vec![0u32; trace.size.len()];
    let mut was_written = vec![false; trace.size.len()];
    for event in &trace.events {
        match *event {
            Event::Write(o) => {
                written += trace.size[o as usize];
                was_written[o as usize] = true;
            }
            Event::Read(o) => {
                read += trace.size[o as usize];
                reads_of[o as usize] += 1;
            }
        }
    }
    let total: u64 = trace.size.iter().sum();
    println!(
        "{} CI runs, {} test jobs each: {} steps run, {} taken from the cache",
        trace.runs, shards, trace.steps_run, trace.steps_cached
    );
    println!(
        "{} outputs, {:.0} MB; written {:.0} MB ({:.1} MB a run), read {:.0} MB ({:.1} MB a run)",
        trace.size.len(),
        mb(total),
        mb(written),
        mb(written) / trace.runs as f64,
        mb(read),
        mb(read) / trace.runs as f64
    );
    println!("\nhow often an output is read after it was written:");
    println!("  {:>10} {:>9} {:>9}", "reads", "outputs", "MB");
    let buckets: [(u32, u32, &str); 6] = [
        (0, 0, "0"),
        (1, 1, "1"),
        (2, shards as u32 + 1, "a run's"),
        (shards as u32 + 2, 20, "to 20"),
        (21, 200, "to 200"),
        (201, u32::MAX, "more"),
    ];
    for (low, high, label) in buckets {
        let (mut count, mut bytes) = (0u64, 0u64);
        for (o, &n) in reads_of.iter().enumerate() {
            if was_written[o] && n >= low && n <= high {
                count += 1;
                bytes += trace.size[o];
            }
        }
        println!("  {:>10} {:>9} {:>9.0}", label, count, mb(bytes));
    }

    // What a window of recent runs touches: the disk that needs no eviction.
    let mut run_end = Vec::new();
    {
        // Runs end at the last read of the linked binary; approximate by
        // splitting the events evenly by the run that wrote each new output.
        let mut current = 0u32;
        for (i, event) in trace.events.iter().enumerate() {
            if let Event::Write(o) = *event {
                let run = trace.run_of[o as usize];
                if run != current {
                    run_end.push(i);
                    current = run;
                }
            }
        }
        run_end.push(trace.events.len());
    }
    println!("\nbytes touched by the last N runs with a write (largest over the trace):");
    for n in [1usize, 10, 50, 200] {
        let mut largest = 0u64;
        let mut i = n;
        while i < run_end.len() {
            let (from, to) = (run_end[i - n], run_end[i]);
            let mut seen = HashSet::new();
            let mut bytes = 0u64;
            for event in &trace.events[from..to] {
                let (Event::Read(o) | Event::Write(o)) = *event;
                if seen.insert(o) {
                    bytes += trace.size[o as usize];
                }
            }
            largest = largest.max(bytes);
            i += (n / 4).max(1);
        }
        println!("  {:>4} runs  {:>8.0} MB", n, mb(largest));
    }

    // Warm up over the first runs, then count.
    let skip = run_end
        .get(window.min(run_end.len() - 1))
        .copied()
        .unwrap_or(0);
    let counted = play(&trace, u64::MAX, &mut Fifo::default(), true, false, skip);
    println!(
        "\nbytes read from the bucket, as a share of the {:.0} MB read after the first {} runs",
        mb(counted.read),
        window
    );
    let sizes_mb: [u64; 8] = [50, 100, 200, 400, 800, 1600, 3200, 6400];
    for packs in [false, true] {
        println!(
            "\n  evicting {}:",
            if packs {
                "packs (all a run wrote)"
            } else {
                "single outputs"
            }
        );
        println!(
            "  {:>8} {:>10} {:>7} {:>7} {:>7} {:>8} {:>9}",
            "disk MB", "reads only", "lru", "fifo", "sieve", "s3-fifo", "foresight"
        );
        for size in sizes_mb {
            let capacity = size * 1_000_000;
            let share = |o: Outcome| 100.0 * o.from_bucket as f64 / o.read.max(1) as f64;
            let reads_only = play(&trace, capacity, &mut Lru::default(), false, packs, skip);
            let lru = play(&trace, capacity, &mut Lru::default(), true, packs, skip);
            let fifo = play(&trace, capacity, &mut Fifo::default(), true, packs, skip);
            let sieve = play(&trace, capacity, &mut Sieve::default(), true, packs, skip);
            let s3 = play(
                &trace,
                capacity,
                &mut S3Fifo::new(capacity),
                true,
                packs,
                skip,
            );
            let foresight = if packs {
                "-".to_string()
            } else {
                let mut policy = Foresight::new(&trace.events, trace.size.len());
                format!(
                    "{:.1}%",
                    share(play(&trace, capacity, &mut policy, true, false, skip))
                )
            };
            println!(
                "  {:>8} {:>9.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>7.1}% {:>9}",
                size,
                share(reads_only),
                share(lru),
                share(fifo),
                share(sieve),
                share(s3),
                foresight
            );
        }
    }
}
