//! How small can a recompiled object be stored, given the object the same
//! source file produced before?
//!
//!   cargo run --release --example object_delta -- --old <build dir> --new <build dir>
//!
//! Compiled objects are too small for content-defined chunks to find
//! anything in them: nearly all are one chunk. But a recompile after a
//! header change mostly reproduces the object that was there before. This
//! takes two builds of the same project, pairs the `.o` files by path, and
//! for every object whose bytes changed measures what is left to store:
//!
//!   alone        zstd, as the chunk codec does today
//!   dictionary   zstd with one dictionary trained on the old build
//!   small chunks content-defined chunks of about 1, 2 and 4 KiB, minus
//!                those the old build already has, plus a reference a chunk
//!   delta        zstd with the old object of the same path as its prefix
//!
//! and how fast each packs and unpacks on one core.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use zstd::zstd_safe;

const LEVEL: i32 = 3;
/// What an index holds for a chunk of an object: a truncated hash, the
/// pack and the offset in it.
const CHUNK_REF: usize = 16 + 4 + 4;

fn objects(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir").flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "o") {
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                out.insert(rel, std::fs::read(&path).expect("read object"));
            }
        }
    }
    out
}

fn with_prefix(new: &[u8], old: &[u8]) -> Vec<u8> {
    let mut cctx = zstd_safe::CCtx::create();
    cctx.set_parameter(zstd_safe::CParameter::CompressionLevel(LEVEL))
        .unwrap();
    // The window has to reach back over the whole prefix.
    let reach = (new.len() + old.len()).next_power_of_two().trailing_zeros();
    cctx.set_parameter(zstd_safe::CParameter::WindowLog(reach.clamp(10, 27)))
        .unwrap();
    cctx.ref_prefix(old).unwrap();
    let mut out = Vec::with_capacity(zstd_safe::compress_bound(new.len()));
    cctx.compress2(&mut out, new).unwrap();
    out
}

fn from_prefix(packed: &[u8], old: &[u8], len: usize) -> Vec<u8> {
    let mut dctx = zstd_safe::DCtx::create();
    dctx.set_parameter(zstd_safe::DParameter::WindowLogMax(27))
        .unwrap();
    dctx.ref_prefix(old).unwrap();
    let mut out = Vec::with_capacity(len);
    dctx.decompress(&mut out, packed).unwrap();
    out
}

fn chunks(data: &[u8], avg: usize) -> Vec<&[u8]> {
    fastcdc::v2020::FastCDC::new(data, avg / 4, avg, avg * 4)
        .map(|c| &data[c.offset..c.offset + c.length])
        .collect()
}

fn row(name: &str, stored: usize, raw: usize, alone: usize, pack: f64, unpack: f64) {
    println!(
        "  {name:<22} {:>7.1} kB an object   {:>5.1}% of raw   {:>5.1}% of alone   packs {:>5.0} MB/s   unpacks {:>5.0} MB/s",
        stored as f64 / 1e3,
        100.0 * stored as f64 / raw as f64,
        100.0 * stored as f64 / alone as f64,
        pack,
        unpack
    );
}

fn main() {
    let (mut old_dir, mut new_dir) = (None, None);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().map(PathBuf::from);
        match flag.as_str() {
            "--old" => old_dir = value,
            "--new" => new_dir = value,
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    let (Some(old_dir), Some(new_dir)) = (old_dir, new_dir) else {
        eprintln!("usage: object_delta --old <build dir> --new <build dir>");
        std::process::exit(2);
    };
    let old = objects(&old_dir);
    let new = objects(&new_dir);

    let changed: Vec<(&[u8], Option<&[u8]>)> = new
        .iter()
        .filter(|(path, bytes)| old.get(*path) != Some(bytes))
        .map(|(path, bytes)| (bytes.as_slice(), old.get(path).map(Vec::as_slice)))
        .collect();
    let with_base = changed.iter().filter(|c| c.1.is_some()).count();
    let n = changed.len();
    let raw: usize = changed.iter().map(|c| c.0.len()).sum();
    println!(
        "{} objects, {} changed ({} of them have an earlier object of the same path), {:.1} kB each",
        new.len(),
        n,
        with_base,
        raw as f64 / n.max(1) as f64 / 1e3
    );
    if n == 0 {
        return;
    }
    let mb = raw as f64 / 1e6;
    let per = |total: usize| total / n;

    // alone
    let mut plain = zstd::bulk::Compressor::new(LEVEL).unwrap();
    let t = Instant::now();
    let packed: Vec<Vec<u8>> = changed
        .iter()
        .map(|c| plain.compress(c.0).unwrap())
        .collect();
    let pack = mb / t.elapsed().as_secs_f64();
    let alone: usize = packed.iter().map(Vec::len).sum();
    let mut plain_d = zstd::bulk::Decompressor::new().unwrap();
    let t = Instant::now();
    for (p, c) in packed.iter().zip(&changed) {
        std::hint::black_box(plain_d.decompress(p, c.0.len()).unwrap());
    }
    let unpack = mb / t.elapsed().as_secs_f64();
    row("alone", per(alone), per(raw), per(alone), pack, unpack);

    // one dictionary for the project, trained on the old build
    let samples: Vec<&[u8]> = old.values().map(Vec::as_slice).collect();
    for size in [112 << 10, 1 << 20] {
        let Ok(dict) = zstd::dict::from_samples(&samples, size) else {
            println!("  dictionary: training failed");
            continue;
        };
        let mut c = zstd::bulk::Compressor::with_dictionary(LEVEL, &dict).unwrap();
        let t = Instant::now();
        let packed: Vec<Vec<u8>> = changed.iter().map(|o| c.compress(o.0).unwrap()).collect();
        let pack = mb / t.elapsed().as_secs_f64();
        let stored: usize = packed.iter().map(Vec::len).sum();
        let mut d = zstd::bulk::Decompressor::with_dictionary(&dict).unwrap();
        let t = Instant::now();
        for (p, o) in packed.iter().zip(&changed) {
            std::hint::black_box(d.decompress(p, o.0.len()).unwrap());
        }
        let unpack = mb / t.elapsed().as_secs_f64();
        row(
            &format!("dictionary, {} KiB", dict.len() >> 10),
            per(stored),
            per(raw),
            per(alone),
            pack,
            unpack,
        );
    }

    // small content-defined chunks, minus what the old build has
    for avg in [1024usize, 2048, 4096] {
        let mut known: HashSet<[u8; 32]> = HashSet::new();
        for bytes in old.values() {
            for c in chunks(bytes, avg) {
                known.insert(*blake3::hash(c).as_bytes());
            }
        }
        let t = Instant::now();
        let (mut stored, mut refs, mut fresh) = (0, 0, 0);
        for (bytes, _) in &changed {
            // What is new in one object is compressed together.
            let mut rest = Vec::new();
            for c in chunks(bytes, avg) {
                refs += 1;
                if known.insert(*blake3::hash(c).as_bytes()) {
                    fresh += 1;
                    rest.extend_from_slice(c);
                }
            }
            stored += plain.compress(&rest).unwrap().len();
        }
        let pack = mb / t.elapsed().as_secs_f64();
        row(
            &format!("chunks of {} KiB", avg >> 10),
            per(stored + refs * CHUNK_REF),
            per(raw),
            per(alone),
            pack,
            f64::NAN,
        );
        println!(
            "  {:<22} {:.0} chunks an object, {:.0}% of them new, {:.1} kB of references",
            "",
            refs as f64 / n as f64,
            100.0 * fresh as f64 / refs as f64,
            (refs * CHUNK_REF) as f64 / n as f64 / 1e3
        );
    }

    // delta against the object the same path had before
    let t = Instant::now();
    let packed: Vec<Vec<u8>> = changed
        .iter()
        .map(|(bytes, base)| match base {
            Some(base) => with_prefix(bytes, base),
            None => plain.compress(bytes).unwrap(),
        })
        .collect();
    let pack = mb / t.elapsed().as_secs_f64();
    let stored: usize = packed.iter().map(Vec::len).sum();
    let t = Instant::now();
    for (p, (bytes, base)) in packed.iter().zip(&changed) {
        let back = match base {
            Some(base) => from_prefix(p, base, bytes.len()),
            None => plain_d.decompress(p, bytes.len()).unwrap(),
        };
        assert!(back == *bytes);
    }
    let unpack = mb / t.elapsed().as_secs_f64();
    row(
        "delta to same path",
        per(stored),
        per(raw),
        per(alone),
        pack,
        unpack,
    );

    let mut sizes: Vec<f64> = packed
        .iter()
        .zip(&changed)
        .filter(|(_, c)| c.1.is_some())
        .map(|(p, c)| p.len() as f64 / c.0.len() as f64)
        .collect();
    sizes.sort_by(f64::total_cmp);
    if !sizes.is_empty() {
        let at = |q: f64| 100.0 * sizes[((sizes.len() - 1) as f64 * q) as usize];
        println!(
            "  delta as a share of the object: median {:.1}%, p90 {:.1}%, worst {:.1}%",
            at(0.5),
            at(0.9),
            at(1.0)
        );
    }
}
