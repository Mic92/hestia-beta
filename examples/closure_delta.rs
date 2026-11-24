//! What is left to store of a new closure when the chunks that found no
//! duplicate are stored as deltas to a similar file, as DeltaNAR
//! (github.com/adisbladis/deltanar) does for deployments.
//!
//!   nix-store -qR <old system> > old.txt; nix-store -qR <new system> > new.txt
//!   cargo run --release --example closure_delta -- --old old.txt --new new.txt
//!
//! Every regular file of the new closure's store paths that the old closure
//! lacks is chunked with hestia's parameters. Chunks the old closure has
//! are free. For the rest, a reference file is chosen and the missed chunks
//! are compressed with that file as zstd's prefix:
//!
//!   by chunks  the old file most of the matched chunks came from (DeltaNAR)
//!   by name    with no matched chunk: the old file of the same package
//!              name and path inside it (hestia's name hint)
//!
//!   by path   failing that: the same, with digits removed from both, so a
//!              version number in a file name does not hide the match
//!
//! The delta is kept only where it is smaller than the chunks compressed
//! alone. Store path hashes in file contents are zeroed first, as hestia's
//! reference rewriting does before chunking; `--raw 1` leaves them.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::Bytes;
use hestia::chunker::chunk_data;
use zstd::zstd_safe;

const LEVEL: i32 = 3;
/// References larger than this are not loaded.
const MAX_REFERENCE: u64 = 512 << 20;

fn files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(meta) = std::fs::symlink_metadata(root) else {
        return;
    };
    if meta.is_file() {
        out.push(root.to_path_buf());
    } else if meta.is_dir() {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        let mut entries: Vec<_> = entries.flatten().map(|e| e.path()).collect();
        entries.sort();
        for e in entries {
            files(&e, out);
        }
    }
}

/// `/nix/store/<hash>-glibc-2.40-66/lib/libc.so.6` -> `glibc/lib/libc.so.6`
fn name_key(store_path: &Path, file: &Path) -> String {
    let base = store_path.file_name().unwrap().to_string_lossy();
    let name = base.split_once('-').map_or(&*base, |x| x.1);
    let mut pname = String::new();
    for part in name.split('-') {
        if part.starts_with(|c: char| c.is_ascii_digit()) {
            break;
        }
        if !pname.is_empty() {
            pname.push('-');
        }
        pname.push_str(part);
    }
    format!(
        "{pname}/{}",
        file.strip_prefix(store_path).unwrap().to_string_lossy()
    )
}

/// Zero the hash of every `/nix/store/<hash>-` in `data`.
fn normalize(data: &mut [u8]) {
    const PREFIX: &[u8] = b"/nix/store/";
    let mut i = 0;
    while i + PREFIX.len() + 33 <= data.len() {
        if data[i] == b'/' && data[i..].starts_with(PREFIX) {
            let h = i + PREFIX.len();
            let hash = &mut data[h..h + 32];
            if data_is_hash(hash) && data[h + 32] == b'-' {
                data[h..h + 32].fill(0);
                i = h + 33;
                continue;
            }
        }
        i += 1;
    }
}

fn data_is_hash(hash: &[u8]) -> bool {
    hash.iter()
        .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
}

fn without_digits(key: &str) -> String {
    key.chars().filter(|c| !c.is_ascii_digit()).collect()
}

fn compress(data: &[u8], prefix: Option<&[u8]>) -> usize {
    let mut cctx = zstd_safe::CCtx::create();
    cctx.set_parameter(zstd_safe::CParameter::CompressionLevel(LEVEL))
        .unwrap();
    if let Some(prefix) = prefix {
        let reach = (data.len() + prefix.len())
            .next_power_of_two()
            .trailing_zeros();
        cctx.set_parameter(zstd_safe::CParameter::WindowLog(reach.clamp(10, 30)))
            .unwrap();
        if prefix.len() > 4 << 20 {
            cctx.set_parameter(zstd_safe::CParameter::EnableLongDistanceMatching(true))
                .unwrap();
        }
        cctx.ref_prefix(prefix).unwrap();
    }
    let mut out = Vec::with_capacity(zstd_safe::compress_bound(data.len()));
    cctx.compress2(&mut out, data).unwrap();
    out.len()
}

#[derive(Default)]
struct Tally {
    files: usize,
    missed_raw: usize,
    alone: usize,
    delta: usize,
    kept: usize,
}

fn main() {
    let (mut old_list, mut new_list) = (None, None);
    let mut raw = false;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next();
        match flag.as_str() {
            "--old" => old_list = value,
            "--new" => new_list = value,
            "--raw" => raw = value.as_deref() == Some("1"),
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    let (Some(old_list), Some(new_list)) = (old_list, new_list) else {
        eprintln!("usage: closure_delta --old <path list> --new <path list>");
        std::process::exit(2);
    };
    let read_list = |p: &str| -> Vec<PathBuf> {
        std::fs::read_to_string(p)
            .expect("read path list")
            .lines()
            .filter(|l| !l.is_empty())
            .map(PathBuf::from)
            .collect()
    };
    let old_paths = read_list(&old_list);
    let old_set: HashSet<&PathBuf> = old_paths.iter().collect();
    let new_paths: Vec<PathBuf> = read_list(&new_list)
        .into_iter()
        .filter(|p| !old_set.contains(p))
        .collect();

    // ---- the old closure: who has which chunk, which file, which name
    let t = Instant::now();
    let mut old_files: Vec<PathBuf> = Vec::new();
    let mut chunk_owner: HashMap<[u8; 32], u32> = HashMap::new();
    let mut whole: HashSet<[u8; 32]> = HashSet::new();
    let mut by_name: HashMap<String, u32> = HashMap::new();
    let mut by_path: HashMap<String, u32> = HashMap::new();
    let load = |p: &Path| -> Option<Vec<u8>> {
        let mut data = std::fs::read(p).ok()?;
        if !raw {
            normalize(&mut data);
        }
        Some(data)
    };
    let mut old_bytes = 0usize;
    for sp in &old_paths {
        let mut fs = Vec::new();
        files(sp, &mut fs);
        for f in fs {
            let Some(data) = load(&f) else {
                continue;
            };
            let id = old_files.len() as u32;
            old_bytes += data.len();
            whole.insert(*blake3::hash(&data).as_bytes());
            let key = name_key(sp, &f);
            by_path.insert(without_digits(&key), id);
            by_name.insert(key, id);
            for c in chunk_data(&Bytes::from(data)) {
                chunk_owner
                    .entry(*blake3::hash(&c.data).as_bytes())
                    .or_insert(id);
            }
            old_files.push(f);
        }
    }
    println!(
        "old closure: {} paths, {} files, {:.2} GB, {} distinct chunks ({:.0} s)",
        old_paths.len(),
        old_files.len(),
        old_bytes as f64 / 1e9,
        chunk_owner.len(),
        t.elapsed().as_secs_f64()
    );

    // ---- the new closure
    let t = Instant::now();
    let (mut new_bytes, mut new_files, mut same_file_bytes, mut matched_bytes) = (0, 0, 0, 0);
    let mut by_chunks = Tally::default();
    let mut by_name_t = Tally::default();
    let mut by_path_t = Tally::default();
    let mut none = Tally::default();
    let mut delta_s = 0.0;
    let mut delta_in = 0usize;
    for sp in &new_paths {
        let mut fs = Vec::new();
        files(sp, &mut fs);
        for f in fs {
            let Some(data) = load(&f) else {
                continue;
            };
            new_files += 1;
            new_bytes += data.len();
            if !whole.insert(*blake3::hash(&data).as_bytes()) {
                same_file_bytes += data.len();
                continue;
            }
            let data = Bytes::from(data);
            let mut votes: HashMap<u32, usize> = HashMap::new();
            let mut missed: Vec<u8> = Vec::new();
            let mut alone = 0;
            for c in chunk_data(&data) {
                let h = *blake3::hash(&c.data).as_bytes();
                match chunk_owner.get(&h) {
                    Some(&owner) if owner != u32::MAX => {
                        *votes.entry(owner).or_default() += 1;
                        matched_bytes += c.data.len();
                    }
                    Some(_) => matched_bytes += c.data.len(),
                    None => {
                        // Later files of the new closure may share it, but
                        // it is no reference: the reader may not have it.
                        chunk_owner.insert(h, u32::MAX);
                        alone += compress(&c.data, None);
                        missed.extend_from_slice(&c.data);
                    }
                }
            }
            if missed.is_empty() {
                continue;
            }
            let voted = votes.iter().max_by_key(|v| (*v.1, *v.0)).map(|v| *v.0);
            let key = name_key(sp, &f);
            let named = by_name.get(&key).copied();
            let pathed = by_path.get(&without_digits(&key)).copied();
            let (tally, reference) = match (voted, named, pathed) {
                (Some(id), _, _) => (&mut by_chunks, Some(id)),
                (None, Some(id), _) => (&mut by_name_t, Some(id)),
                (None, None, Some(id)) => (&mut by_path_t, Some(id)),
                (None, None, None) => (&mut none, None),
            };
            tally.files += 1;
            tally.missed_raw += missed.len();
            tally.alone += alone;
            let delta = reference
                .map(|id| &old_files[id as usize])
                .filter(|p| std::fs::metadata(p).is_ok_and(|m| m.len() <= MAX_REFERENCE))
                .and_then(|p| load(p))
                .map(|base| {
                    let t = Instant::now();
                    let size = compress(&missed, Some(&base));
                    delta_s += t.elapsed().as_secs_f64();
                    delta_in += missed.len();
                    size
                });
            match delta {
                Some(d) if d < alone => {
                    tally.delta += d;
                    tally.kept += 1;
                }
                _ => tally.delta += alone,
            }
        }
    }
    let all = [&by_chunks, &by_name_t, &by_path_t, &none];
    let missed_raw: usize = all.iter().map(|t| t.missed_raw).sum();
    let alone: usize = all.iter().map(|t| t.alone).sum();
    let delta: usize = all.iter().map(|t| t.delta).sum();
    let gb = |b: usize| b as f64 / 1e9;
    println!(
        "new closure: {} new paths, {} files, {:.2} GB ({:.0} s)",
        new_paths.len(),
        new_files,
        gb(new_bytes),
        t.elapsed().as_secs_f64()
    );
    println!(
        "  whole files the old closure has  {:.2} GB",
        gb(same_file_bytes)
    );
    println!(
        "  chunks the old closure has       {:.2} GB",
        gb(matched_bytes)
    );
    println!(
        "  chunks it lacks                  {:.2} GB raw, {:.3} GB compressed alone",
        gb(missed_raw),
        gb(alone)
    );
    println!(
        "  the same, as deltas where smaller {:.3} GB ({:.0}% of alone), packed at {:.0} MB/s",
        gb(delta),
        100.0 * delta as f64 / alone as f64,
        delta_in as f64 / 1e6 / delta_s
    );
    for (name, t) in [
        ("reference by chunks", &by_chunks),
        ("reference by name", &by_name_t),
        ("reference by path", &by_path_t),
        ("no reference", &none),
    ] {
        println!(
            "    {name:<20} {:>6} files  {:>6.3} GB raw  {:>6.3} GB alone  {:>6.3} GB as delta ({:.0}%), delta kept for {} files",
            t.files,
            gb(t.missed_raw),
            gb(t.alone),
            gb(t.delta),
            100.0 * t.delta as f64 / t.alone.max(1) as f64,
            t.kept
        );
    }
}
