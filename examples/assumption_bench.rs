//! Micro-benchmarks for figures hybrid_sim and scale_sim assume about a
//! cache node and about compiled objects, printed beside the assumption.
//!
//!   cargo run --release --example assumption_bench -- \
//!       [--dir target/assumption_bench] [--rows 50000000] [--objects dir,dir]
//!
//! Measured on whatever machine runs it, one core unless stated:
//!
//!   hashing        BLAKE3 over a large buffer and over object-sized blobs
//!   keys, memory   a lookup in a sorted table of 32 byte rows
//!   keys, disk     the same table in a file, one fence key per 4 KiB page
//!                  in memory, a lookup is one page read past the page cache
//!   disk           sequential write and read of that file
//!   merge          a four-way merge of sorted runs into zstd frames
//!   sync           an fsync per row, and per batch of rows
//!   request        a keep-alive HTTP round trip on loopback through the
//!                  stack hestia already uses (axum, reqwest): the software
//!                  floor of a node request, no network in it
//!   objects        `--objects`: size, compressed size and chunk count of
//!                  every `.o` below the given directories
//!
//! Not measured here: anything about the bucket, the network between
//! datacenters, prices, failure rates and the shape of a build step.

mod common;
use common::Rng;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::routing::post;
use hestia::chunker::{chunk_data, compress_chunk};

const PAGE: usize = 4096;
const ROW: usize = 32;
const ROWS_PER_PAGE: usize = PAGE / ROW;

fn fill(rng: &mut Rng, buf: &mut [u8]) {
    for word in buf.chunks_mut(8) {
        let v = rng.next().to_le_bytes();
        word.copy_from_slice(&v[..word.len()]);
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

fn line(what: &str, measured: String, assumed: &str) {
    println!("  {what:<44} {measured:<34} assumed: {assumed}");
}

// ------------------------------------------------------------ hashing

fn hashing() {
    println!("hashing");
    let mut rng = Rng(1);
    let mut big = vec![0u8; 256 << 20];
    fill(&mut rng, &mut big);
    let t = Instant::now();
    std::hint::black_box(blake3::hash(&big));
    let rate = big.len() as f64 / t.elapsed().as_secs_f64();
    line(
        "BLAKE3, 256 MiB",
        format!("{:.2} GB/s", rate / 1e9),
        "2 GB/s (CPU_BYTE_S)",
    );

    const BLOB: usize = 17_000;
    let blobs = 20_000;
    let t = Instant::now();
    for i in 0..blobs {
        let at = (i * 7919) % (big.len() - BLOB);
        std::hint::black_box(blake3::hash(&big[at..at + BLOB]));
    }
    let each = t.elapsed().as_secs_f64() / blobs as f64;
    line(
        "BLAKE3, 17 kB blobs",
        format!(
            "{:.1} us each, {:.2} GB/s",
            each * 1e6,
            BLOB as f64 / each / 1e9
        ),
        "8.5 us each",
    );
}

// ------------------------------------------------------------ keys

type Row = [u64; 4];

fn table(rows: usize) -> Vec<Row> {
    let mut rng = Rng(2);
    let mut table: Vec<Row> = (0..rows)
        .map(|_| [rng.next(), rng.next(), rng.next(), rng.next()])
        .collect();
    table.sort_unstable();
    table
}

fn keys_in_memory(table: &[Row]) {
    println!(
        "keys in memory, {} M rows ({:.1} GB)",
        table.len() / 1_000_000,
        (table.len() * ROW) as f64 / 1e9
    );
    let mut rng = Rng(3);
    let wanted: Vec<Row> = (0..2_000_000)
        .map(|_| table[rng.below(table.len())])
        .collect();
    let t = Instant::now();
    let mut found = 0usize;
    for key in &wanted {
        found += table.binary_search(key).is_ok() as usize;
    }
    assert_eq!(found, wanted.len());
    let each = t.elapsed().as_secs_f64() / wanted.len() as f64;
    line(
        "binary search, random keys",
        format!("{:.2} us a key", each * 1e6),
        "3 us a key (CPU_KEY_RAM_S)",
    );
    line(
        "a step's 30 keys",
        format!("{:.0} us", each * 30.0 * 1e6),
        "90 us",
    );
}

/// A page-aligned buffer for O_DIRECT.
struct Aligned(*mut u8, usize);
impl Aligned {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, PAGE).unwrap();
        Aligned(unsafe { std::alloc::alloc_zeroed(layout) }, len)
    }
    fn bytes(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.0, self.1) }
    }
}
impl Drop for Aligned {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.1, PAGE).unwrap();
        unsafe { std::alloc::dealloc(self.0, layout) }
    }
}
unsafe impl Send for Aligned {}

fn row_bytes(table: &[Row]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(table.as_ptr().cast(), std::mem::size_of_val(table)) }
}

fn keys_on_disk(table: &[Row], dir: &Path) {
    let path = dir.join("keys");
    let bytes = row_bytes(table);
    println!("disk ({})", dir.display());
    let t = Instant::now();
    let mut file = File::create(&path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    let wrote = bytes.len() as f64 / t.elapsed().as_secs_f64();
    line(
        "sequential write, synced",
        format!("{:.0} MB/s", wrote / 1e6),
        "500 MB/s to a node (NODE_BPS)",
    );
    drop(file);

    let direct = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path);
    let Ok(file) = direct else {
        println!("  O_DIRECT is refused here: nothing below would get past the page cache");
        return;
    };
    let mut buf = Aligned::new(8 << 20);
    let t = Instant::now();
    let mut at = 0u64;
    while (at as usize) < bytes.len() {
        at += file.read_at(buf.bytes(), at).unwrap() as u64;
    }
    let read = bytes.len() as f64 / t.elapsed().as_secs_f64();
    line(
        "sequential read, uncached",
        format!("{:.0} MB/s", read / 1e6),
        "2000 MB/s (SSD_READ_BPS)",
    );

    // One fence key per page in memory; a lookup reads one page.
    let fences: Vec<Row> = table.iter().step_by(ROWS_PER_PAGE).copied().collect();
    println!("keys on disk");
    line(
        "memory for the fences",
        format!(
            "{:.2} bytes a key",
            (fences.len() * ROW) as f64 / table.len() as f64
        ),
        "2 bytes a key (keys / 16)",
    );
    let lookup = |file: &File, buf: &mut Aligned, key: &Row| -> bool {
        let page = fences.partition_point(|f| f <= key) - 1;
        file.read_exact_at(buf.bytes(), (page * PAGE) as u64)
            .unwrap();
        let rows: &[Row] = unsafe { std::slice::from_raw_parts(buf.0.cast(), ROWS_PER_PAGE) };
        let rows = &rows[..ROWS_PER_PAGE.min(table.len() - page * ROWS_PER_PAGE)];
        rows.binary_search(key).is_ok()
    };
    let mut rng = Rng(4);
    let mut page = Aligned::new(PAGE);
    let mut times = Vec::new();
    for _ in 0..20_000 {
        let key = table[rng.below(table.len())];
        let t = Instant::now();
        assert!(lookup(&file, &mut page, &key));
        times.push(t.elapsed().as_secs_f64());
    }
    times.sort_by(f64::total_cmp);
    line(
        "one lookup, one page read",
        format!(
            "{:.0} us median, {:.0} us p99",
            percentile(&times, 0.5) * 1e6,
            percentile(&times, 0.99) * 1e6
        ),
        "200 us (SSD_LOOKUP_S)",
    );
    // A step's keys are read at once: 30 readers.
    const READERS: usize = 30;
    const EACH: usize = 2_000;
    let t = Instant::now();
    std::thread::scope(|scope| {
        for reader in 0..READERS {
            let (file, lookup, table) = (&file, &lookup, table);
            scope.spawn(move || {
                let mut rng = Rng(100 + reader as u64);
                let mut page = Aligned::new(PAGE);
                for _ in 0..EACH {
                    assert!(lookup(file, &mut page, &table[rng.below(table.len())]));
                }
            });
        }
    });
    let elapsed = t.elapsed().as_secs_f64();
    line(
        "30 lookups in flight",
        format!(
            "{:.0} lookups/s, {:.0} us a batch of 30",
            (READERS * EACH) as f64 / elapsed,
            elapsed / EACH as f64 * 1e6
        ),
        "200 us a batch; 25,000 lookups/s wanted",
    );
    std::fs::remove_file(&path).ok();
}

// ------------------------------------------------------------ merge

fn merge() {
    println!("merge");
    const RUNS: usize = 4;
    const PER_RUN: usize = 400_000;
    const BODY: usize = 213;
    // Bodies: half of every one is random, half repeats, roughly how a
    // CBOR row of hashes and small integers compresses.
    let mut rng = Rng(5);
    let runs: Vec<Vec<(Row, [u8; BODY])>> = (0..RUNS)
        .map(|_| {
            let mut run: Vec<(Row, [u8; BODY])> = (0..PER_RUN)
                .map(|_| {
                    let mut body = [7u8; BODY];
                    fill(&mut rng, &mut body[..BODY / 2]);
                    ([rng.next(), rng.next(), rng.next(), rng.next()], body)
                })
                .collect();
            run.sort_unstable_by_key(|r| r.0);
            run
        })
        .collect();
    let t = Instant::now();
    let mut heap: BinaryHeap<std::cmp::Reverse<(Row, usize, usize)>> = (0..RUNS)
        .map(|r| std::cmp::Reverse((runs[r][0].0, r, 0)))
        .collect();
    let mut keys = Vec::with_capacity(RUNS * PER_RUN * ROW);
    let mut frame = Vec::with_capacity(1 << 16);
    let (mut raw, mut packed) = (0usize, 0usize);
    while let Some(std::cmp::Reverse((key, run, at))) = heap.pop() {
        keys.extend_from_slice(row_bytes(std::slice::from_ref(&key)));
        frame.extend_from_slice(&runs[run][at].1);
        if frame.len() >= 1 << 16 {
            raw += frame.len();
            packed += zstd::bulk::compress(&frame, 3).unwrap().len();
            frame.clear();
        }
        if at + 1 < PER_RUN {
            heap.push(std::cmp::Reverse((runs[run][at + 1].0, run, at + 1)));
        }
    }
    let total = (raw + keys.len()) as f64;
    line(
        "4 runs into keys and zstd-3 frames",
        format!(
            "{:.0} MB/s (bodies to {:.0}%)",
            total / t.elapsed().as_secs_f64() / 1e6,
            packed as f64 / raw as f64 * 100.0
        ),
        "200 MB/s (CPU_MERGE_BYTE_S)",
    );
}

// ------------------------------------------------------------ sync

fn sync(dir: &Path) {
    println!("sync");
    let path = dir.join("log");
    let mut file = File::create(&path).unwrap();
    let row = [9u8; 245];
    let mut times = Vec::new();
    for _ in 0..300 {
        let t = Instant::now();
        file.write_all(&row).unwrap();
        file.sync_data().unwrap();
        times.push(t.elapsed().as_secs_f64());
    }
    times.sort_by(f64::total_cmp);
    let median = percentile(&times, 0.5);
    line(
        "append a row, fdatasync",
        format!(
            "{:.2} ms median, {:.2} ms p99",
            median * 1e3,
            percentile(&times, 0.99) * 1e3
        ),
        "inside a 1.5 ms node request",
    );
    line(
        "rows/s, one sync at a time",
        format!("{:.0} alone; x batch when grouped", 1.0 / median),
        "800 writes/s wanted, 50 us of CPU each",
    );
    // A 17 kB object beside it.
    let blob = vec![3u8; 17_000];
    let mut times = Vec::new();
    for _ in 0..300 {
        let t = Instant::now();
        file.write_all(&blob).unwrap();
        file.sync_data().unwrap();
        times.push(t.elapsed().as_secs_f64());
    }
    times.sort_by(f64::total_cmp);
    line(
        "append 17 kB, fdatasync",
        format!(
            "{:.2} ms median, {:.2} ms p99",
            percentile(&times, 0.5) * 1e3,
            percentile(&times, 0.99) * 1e3
        ),
        "inside a 1.5 ms node request",
    );
    std::fs::remove_file(&path).ok();
}

// ------------------------------------------------------------ request

async fn request() {
    println!("request (loopback, axum and reqwest, keep-alive)");
    let app = Router::new()
        // A lookup: 30 keys of 20 bytes in, 30 answers of 16 bytes out.
        .route(
            "/lookup",
            post(|body: Bytes| async move { vec![0u8; body.len() / 20 * 16] }),
        )
        // A write: the blob is hashed, as a node would to check it.
        .route(
            "/put",
            post(|body: Bytes| async move { blake3::hash(&body).as_bytes().to_vec() }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();
    for (what, path, len, assumed) in [
        (
            "lookup of 30 keys",
            "/lookup",
            600usize,
            "1.5 ms median, 6 ms p99 (with a network)",
        ),
        (
            "write of 17 kB",
            "/put",
            17_000,
            "1.5 ms median plus 34 us of transfer",
        ),
    ] {
        let body = Bytes::from(vec![5u8; len]);
        let url = format!("{base}{path}");
        let mut times = Vec::new();
        for i in 0..5_200 {
            let t = Instant::now();
            let answer = client.post(&url).body(body.clone()).send().await.unwrap();
            answer.bytes().await.unwrap();
            if i >= 200 {
                times.push(t.elapsed().as_secs_f64());
            }
        }
        times.sort_by(f64::total_cmp);
        line(
            what,
            format!(
                "{:.0} us median, {:.0} us p99",
                percentile(&times, 0.5) * 1e6,
                percentile(&times, 0.99) * 1e6
            ),
            assumed,
        );
    }
    // Throughput with client and server on the same cores.
    let url = format!("{base}/lookup");
    let body = Bytes::from(vec![5u8; 600]);
    let t = Instant::now();
    const CLIENTS: usize = 64;
    const EACH: usize = 1_000;
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let (client, url, body) = (client.clone(), url.clone(), body.clone());
            tokio::spawn(async move {
                for _ in 0..EACH {
                    client
                        .post(&url)
                        .body(body.clone())
                        .send()
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
    line(
        "64 clients, lookups",
        format!(
            "{:.0} requests/s, client included",
            (CLIENTS * EACH) as f64 / t.elapsed().as_secs_f64()
        ),
        "830 requests/s wanted at 500 machines",
    );
}

// ------------------------------------------------------------ objects

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "o") {
            out.push(path);
        }
    }
}

fn objects(dirs: &[String]) {
    for dir in dirs {
        let mut paths = Vec::new();
        walk(Path::new(dir), &mut paths);
        if paths.is_empty() {
            println!("objects: no .o below {dir}");
            continue;
        }
        println!("objects below {dir}");
        let mut sizes = Vec::new();
        let (mut raw, mut packed, mut chunks, mut one_chunk) = (0usize, 0usize, 0usize, 0usize);
        let mut packed_sizes = Vec::new();
        let mut seen = HashSet::new();
        let mut repeated = 0usize;
        let mut seconds = 0.0;
        for path in &paths {
            let Ok(data) = std::fs::read(path) else {
                continue;
            };
            if data.is_empty() {
                continue;
            }
            if !seen.insert(*blake3::hash(&data).as_bytes()) {
                repeated += 1;
            }
            let data = bytes::Bytes::from(data);
            let parts = chunk_data(&data);
            chunks += parts.len();
            one_chunk += (parts.len() == 1) as usize;
            let t = Instant::now();
            let size = compress_chunk(&data).map(|c| c.len()).unwrap_or(data.len());
            seconds += t.elapsed().as_secs_f64();
            raw += data.len();
            packed += size;
            packed_sizes.push(size as f64);
            sizes.push(data.len() as f64);
        }
        let n = sizes.len() as f64;
        sizes.sort_by(f64::total_cmp);
        packed_sizes.sort_by(f64::total_cmp);
        let mean_ln = sizes.iter().map(|s| s.ln()).sum::<f64>() / n;
        let sigma = (sizes
            .iter()
            .map(|s| (s.ln() - mean_ln).powi(2))
            .sum::<f64>()
            / n)
            .sqrt();
        let below = |limit: f64| packed_sizes.partition_point(|s| *s < limit) as f64 / n * 100.0;
        line(
            "objects",
            format!("{n:.0}, {repeated} with content seen before"),
            "-",
        );
        line(
            "size",
            format!(
                "{:.0} kB mean, {:.0} kB median",
                raw as f64 / n / 1e3,
                percentile(&sizes, 0.5) / 1e3
            ),
            "50 kB",
        );
        line(
            "spread of ln(size)",
            format!("{sigma:.2}"),
            "1.0 (size_sigma)",
        );
        line(
            "compressed (hestia's chunk codec)",
            format!(
                "{:.0} kB mean, {:.0}% of raw",
                packed as f64 / n / 1e3,
                packed as f64 / raw as f64 * 100.0
            ),
            "17 kB, 34%",
        );
        line(
            "compression speed",
            format!("{:.0} MB/s", raw as f64 / seconds / 1e6),
            "-",
        );
        line(
            "objects that are a single chunk",
            format!(
                "{:.0}% ({:.2} chunks an object)",
                one_chunk as f64 / n * 100.0,
                chunks as f64 / n
            ),
            "all of them",
        );
        line(
            "compressed below 1, 4, 16 KiB",
            format!(
                "{:.0}%, {:.0}%, {:.0}%",
                below(1024.0),
                below(4096.0),
                below(16384.0)
            ),
            "2%, 11%, 53% (lognormal)",
        );
    }
}

#[tokio::main]
async fn main() {
    let mut dir = PathBuf::from("target/assumption_bench");
    let mut rows = 50_000_000usize;
    let mut object_dirs: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| {
            eprintln!("missing value for {flag}");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--dir" => dir = PathBuf::from(value),
            "--rows" => rows = value.parse().expect("--rows takes a number"),
            "--objects" => object_dirs = value.split(',').map(str::to_string).collect(),
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    std::fs::create_dir_all(&dir).unwrap();
    println!(
        "{} cores; one core unless stated\n",
        std::thread::available_parallelism().map_or(1, |n| n.get())
    );
    hashing();
    let table = table(rows);
    keys_in_memory(&table);
    keys_on_disk(&table, &dir);
    drop(table);
    merge();
    sync(&dir);
    request().await;
    objects(&object_dirs);
}
