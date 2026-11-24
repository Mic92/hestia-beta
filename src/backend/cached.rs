//! A backend with packs kept on local disk: what a cache node puts
//! between its builders and the bucket.
//!
//! Packs are named by the hash of their bytes and never change, so a kept
//! range needs no invalidation. The cache keeps every pack written through
//! it and every range read through it, and drops the least recently used
//! pack when the disk budget is exceeded (`examples/node_cache_sim.rs`:
//! not keeping uploads sends a third of all read bytes to the bucket
//! whatever the disk size, and no other order of eviction beat this one).
//!
//! Only reads are answered from disk. Whether a pack exists is still the
//! bucket's word, so GC and the eviction check see what is really stored.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::ops::Range;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{Backend, Error};

const PACK_PREFIX: &str = "pack-";
/// Written by [`PackCache::save`], consumed by [`PackCache::open`].
const INDEX: &str = "index";
/// Missing parts of a pack are fetched in multiples of this.
const BLOCK: u64 = 256 * 1024;

#[derive(Default)]
struct Entry {
    tick: u64,
    /// Kept ranges, start to end, merged where they touch.
    extents: BTreeMap<u64, u64>,
    bytes: u64,
    /// The pack's length, once it was seen whole.
    len: Option<u64>,
}

impl Entry {
    fn covers(&self, range: &Range<u64>) -> bool {
        self.extents
            .range(..=range.start)
            .next_back()
            .is_some_and(|(_, &end)| end >= range.end)
    }

    /// The parts of `range` that are not kept, in order.
    fn missing(&self, range: &Range<u64>) -> Vec<Range<u64>> {
        let mut at = range.start;
        let mut out = Vec::new();
        if let Some((_, &end)) = self.extents.range(..=at).next_back() {
            at = at.max(end);
        }
        for (&start, &end) in self.extents.range(range.start + 1..range.end) {
            if start > at {
                out.push(at..start);
            }
            at = at.max(end);
        }
        if at < range.end {
            out.push(at..range.end);
        }
        out
    }

    /// Returns the bytes the extent added.
    fn add(&mut self, mut start: u64, mut end: u64) -> u64 {
        if start >= end {
            return 0;
        }
        let mut swallowed = 0;
        if let Some((&s, &e)) = self.extents.range(..=start).next_back()
            && e >= start
        {
            if e >= end {
                return 0;
            }
            start = s;
            swallowed += e - s;
            self.extents.remove(&s);
        }
        while let Some((&s, &e)) = self.extents.range(start..=end).next() {
            end = end.max(e);
            swallowed += e - s;
            self.extents.remove(&s);
        }
        self.extents.insert(start, end);
        let added = (end - start) - swallowed;
        self.bytes += added;
        added
    }
}

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    /// Tick of last use to pack: the first is the one to evict.
    order: BTreeMap<u64, String>,
    used: u64,
    tick: u64,
}

impl State {
    fn touch(&mut self, key: &str) {
        self.tick += 1;
        let tick = self.tick;
        if let Some(entry) = self.entries.get_mut(key) {
            self.order.remove(&entry.tick);
            entry.tick = tick;
            self.order.insert(tick, key.to_owned());
        }
    }

    fn forget(&mut self, key: &str) -> bool {
        let Some(entry) = self.entries.remove(key) else {
            return false;
        };
        self.order.remove(&entry.tick);
        self.used -= entry.bytes;
        true
    }
}

/// What the cache did since it was opened.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub hits: u64,
    pub hit_bytes: u64,
    pub misses: u64,
    pub miss_bytes: u64,
    pub evicted: u64,
    pub used: u64,
}

/// Pack ranges on local disk under a byte budget.
pub struct PackCache {
    dir: PathBuf,
    capacity: u64,
    state: Mutex<State>,
    hits: AtomicU64,
    hit_bytes: AtomicU64,
    misses: AtomicU64,
    miss_bytes: AtomicU64,
    evicted: AtomicU64,
}

impl PackCache {
    /// Takes over `dir`. What [`PackCache::save`] recorded there is kept;
    /// without that record (first start, or the last process died) the
    /// files cannot be trusted to hold what was written and are removed.
    pub fn open(dir: &Path, capacity: u64) -> io::Result<PackCache> {
        fs::create_dir_all(dir)?;
        let mut state = State::default();
        let index = dir.join(INDEX);
        let saved = fs::read_to_string(&index).ok();
        // A crash from here on must not find this record again.
        let _ = fs::remove_file(&index);
        let mut known: HashMap<String, Entry> = HashMap::new();
        for line in saved.as_deref().unwrap_or("").lines() {
            let mut fields = line.split(' ');
            let (Some(key), Some(len)) = (fields.next(), fields.next()) else {
                continue;
            };
            let entry = known.entry(key.to_owned()).or_default();
            entry.len = len.parse().ok();
            while let (Some(start), Some(end)) = (fields.next(), fields.next()) {
                if let (Ok(start), Ok(end)) = (start.parse(), end.parse()) {
                    entry.add(start, end);
                }
            }
        }
        // Lines are in order of last use, oldest first.
        for line in saved.as_deref().unwrap_or("").lines() {
            let key = line.split(' ').next().unwrap_or("");
            if let Some(mut entry) = known.remove(key)
                && Self::path_in(dir, key).is_file()
            {
                state.tick += 1;
                entry.tick = state.tick;
                state.used += entry.bytes;
                state.order.insert(entry.tick, key.to_owned());
                state.entries.insert(key.to_owned(), entry);
            }
        }
        for shard in fs::read_dir(dir)?.flatten() {
            if !shard.path().is_dir() {
                continue;
            }
            for file in fs::read_dir(shard.path())?.flatten() {
                let name = file.file_name();
                if !state.entries.contains_key(name.to_string_lossy().as_ref()) {
                    let _ = fs::remove_file(file.path());
                }
            }
        }
        let cache = PackCache {
            dir: dir.to_owned(),
            capacity,
            state: Mutex::new(state),
            hits: AtomicU64::new(0),
            hit_bytes: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            miss_bytes: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
        };
        cache.evict();
        Ok(cache)
    }

    /// Records what is kept, so the next [`PackCache::open`] starts warm.
    /// Call once nothing writes any more.
    pub fn save(&self) -> io::Result<()> {
        let mut out = String::new();
        {
            let state = self.state.lock().expect("pack cache lock poisoned");
            for key in state.order.values() {
                let entry = &state.entries[key];
                out.push_str(key);
                out.push(' ');
                out.push_str(&entry.len.map_or("-".to_owned(), |l| l.to_string()));
                for (start, end) in &entry.extents {
                    out.push_str(&format!(" {start} {end}"));
                }
                out.push('\n');
            }
        }
        let tmp = self.dir.join("index.tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(out.as_bytes())?;
        file.sync_all()?;
        fs::rename(tmp, self.dir.join(INDEX))
    }

    pub fn stats(&self) -> Stats {
        Stats {
            hits: self.hits.load(Ordering::Relaxed),
            hit_bytes: self.hit_bytes.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            miss_bytes: self.miss_bytes.load(Ordering::Relaxed),
            evicted: self.evicted.load(Ordering::Relaxed),
            used: self.state.lock().expect("pack cache lock poisoned").used,
        }
    }

    fn path_in(dir: &Path, key: &str) -> PathBuf {
        let shard = key.get(PACK_PREFIX.len()..PACK_PREFIX.len() + 2);
        dir.join(shard.unwrap_or("xx")).join(key)
    }

    fn path(&self, key: &str) -> PathBuf {
        Self::path_in(&self.dir, key)
    }

    /// What a read of `range` still has to fetch. A range that runs past
    /// the pack's known end is cut there.
    fn missing(&self, key: &str, range: &Range<u64>) -> Vec<Range<u64>> {
        let state = self.state.lock().expect("pack cache lock poisoned");
        match state.entries.get(key) {
            Some(entry) => {
                let end = entry.len.map_or(range.end, |len| range.end.min(len));
                entry.missing(&(range.start.min(end)..end))
            }
            None => vec![range.clone()],
        }
    }

    /// The range, or the whole pack, if all of it is kept.
    fn read(&self, key: &str, range: Option<Range<u64>>) -> Option<Bytes> {
        let range = {
            let mut state = self.state.lock().expect("pack cache lock poisoned");
            let entry = state.entries.get(key)?;
            let range = match (range, entry.len) {
                (Some(range), Some(len)) => range.start.min(len)..range.end.min(len),
                (Some(range), None) => range,
                (None, len) => 0..len?,
            };
            if !entry.covers(&range) {
                return None;
            }
            state.touch(key);
            range
        };
        let read = || -> io::Result<Vec<u8>> {
            let mut out = vec![0; (range.end - range.start) as usize];
            File::open(self.path(key))?.read_exact_at(&mut out, range.start)?;
            Ok(out)
        };
        match read() {
            Ok(out) => Some(out.into()),
            Err(_) => {
                // Evicted between the check and the read, or the file is
                // damaged: either way this pack is not kept.
                self.remove(key);
                None
            }
        }
    }

    /// Keeps `data`, which sits at `offset` in the pack. `last` says the
    /// pack ends where the data does.
    fn store(&self, key: &str, offset: u64, data: &[u8], last: bool) {
        if data.len() as u64 > self.capacity {
            return;
        }
        let path = self.path(key);
        let write = || -> io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?
                .write_all_at(data, offset)
        };
        if write().is_err() {
            self.remove(key);
            return;
        }
        {
            let mut state = self.state.lock().expect("pack cache lock poisoned");
            if !state.entries.contains_key(key) {
                state.entries.insert(key.to_owned(), Entry::default());
            }
            state.touch(key);
            let entry = state.entries.get_mut(key).expect("just inserted");
            if last {
                entry.len = Some(offset + data.len() as u64);
            }
            let added = entry.add(offset, offset + data.len() as u64);
            state.used += added;
        }
        self.evict();
    }

    fn remove(&self, key: &str) {
        let known = self
            .state
            .lock()
            .expect("pack cache lock poisoned")
            .forget(key);
        if known {
            let _ = fs::remove_file(self.path(key));
        }
    }

    fn evict(&self) {
        loop {
            let victim = {
                let mut state = self.state.lock().expect("pack cache lock poisoned");
                if state.used <= self.capacity {
                    return;
                }
                let Some((_, key)) = state.order.pop_first() else {
                    return;
                };
                let entry = state.entries.remove(&key).expect("ordered pack is kept");
                state.used -= entry.bytes;
                key
            };
            self.evicted.fetch_add(1, Ordering::Relaxed);
            let _ = fs::remove_file(self.path(&victim));
        }
    }
}

/// `inner` with its packs read through and written through a [`PackCache`].
pub struct Cached {
    pub inner: Backend,
    pub packs: Arc<PackCache>,
}

impl Cached {
    pub fn new(inner: Backend, packs: Arc<PackCache>) -> Arc<Cached> {
        Arc::new(Cached { inner, packs })
    }

    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        if !key.starts_with(PACK_PREFIX) {
            return self.inner.get(key, range).await;
        }
        if let Some(data) = self.kept(key, range.clone()).await {
            return Ok(Some(data));
        }
        if let Some(range) = range {
            return self.fill(key, range.clone(), range).await;
        }
        let Some(data) = self.inner.get(key, None).await? else {
            return Ok(None);
        };
        self.packs.misses.fetch_add(1, Ordering::Relaxed);
        self.packs
            .miss_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.keep(key, 0, &data, true).await;
        Ok(Some(data))
    }

    /// A range of a pack that is `len` bytes long. What is missing is
    /// fetched in whole blocks, so reads that overlap earlier ones find
    /// them, and a read's neighbours come along as they would with
    /// read-ahead.
    pub async fn get_pack(
        &self,
        key: &str,
        range: Range<u64>,
        len: u64,
    ) -> Result<Option<Bytes>, Error> {
        if let Some(data) = self.kept(key, Some(range.clone())).await {
            return Ok(Some(data));
        }
        let start = range.start / BLOCK * BLOCK;
        let end = range.end.div_ceil(BLOCK).saturating_mul(BLOCK).min(len);
        self.fill(key, range.clone(), start..end.max(range.end))
            .await
    }

    async fn kept(&self, key: &str, range: Option<Range<u64>>) -> Option<Bytes> {
        let (packs, key) = (Arc::clone(&self.packs), key.to_owned());
        let data = tokio::task::spawn_blocking(move || packs.read(&key, range))
            .await
            .ok()
            .flatten()?;
        self.packs.hits.fetch_add(1, Ordering::Relaxed);
        self.packs
            .hit_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Some(data)
    }

    /// Fetches what of `around` is not kept and answers `range`, which
    /// lies inside it.
    async fn fill(
        &self,
        key: &str,
        range: Range<u64>,
        around: Range<u64>,
    ) -> Result<Option<Bytes>, Error> {
        let mut fetched = 0;
        for gap in self.packs.missing(key, &around) {
            let Some(data) = self.inner.get(key, Some(gap.clone())).await? else {
                return Ok(None);
            };
            fetched += data.len() as u64;
            self.keep(key, gap.start, &data, false).await;
        }
        self.packs.misses.fetch_add(1, Ordering::Relaxed);
        self.packs.miss_bytes.fetch_add(fetched, Ordering::Relaxed);
        let (packs, owned) = (Arc::clone(&self.packs), key.to_owned());
        let wanted = range.clone();
        let kept = tokio::task::spawn_blocking(move || packs.read(&owned, Some(wanted)))
            .await
            .ok()
            .flatten();
        match kept {
            Some(data) => Ok(Some(data)),
            // Evicted in between: this read goes around the cache.
            None => self.inner.get(key, Some(range)).await,
        }
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        let created = self.inner.put(key, data.clone()).await?;
        if key.starts_with(PACK_PREFIX) {
            self.keep(key, 0, &data, true).await;
        }
        Ok(created)
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        if key.starts_with(PACK_PREFIX) {
            self.packs.remove(key);
        }
        self.inner.delete(key).await
    }

    async fn keep(&self, key: &str, offset: u64, data: &Bytes, whole: bool) {
        let (packs, key, data) = (Arc::clone(&self.packs), key.to_owned(), data.clone());
        let _ = tokio::task::spawn_blocking(move || packs.store(&key, offset, &data, whole)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u8) -> String {
        format!("pack-{}", format!("{n:02x}").repeat(32))
    }

    #[test]
    fn extents_merge_and_count_once() {
        let mut entry = Entry::default();
        assert_eq!(entry.add(10, 20), 10);
        assert_eq!(entry.add(30, 40), 10);
        assert!(!entry.covers(&(10..40)));
        assert_eq!(entry.missing(&(5..45)), [5..10, 20..30, 40..45]);
        assert_eq!(
            entry.missing(&(15..35)).as_slice(),
            std::slice::from_ref(&(20..30))
        );
        assert_eq!(entry.add(15, 35), 10, "only the gap is new");
        assert!(entry.covers(&(10..40)));
        assert_eq!(entry.extents.len(), 1);
        assert_eq!(entry.add(12, 18), 0);
        assert_eq!(entry.missing(&(0..60)), [0..10, 40..60]);
        assert_eq!(entry.missing(&(12..30)), []);
        assert_eq!(entry.add(40, 50), 10, "touching extents merge");
        assert_eq!(entry.extents.into_iter().collect::<Vec<_>>(), [(10, 50)]);
        assert_eq!(entry.bytes, 40);
    }

    #[test]
    fn ranges_are_served_only_when_kept() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::open(dir.path(), 1 << 20).unwrap();
        cache.store(&key(1), 100, b"hello world", false);
        assert_eq!(cache.read(&key(1), Some(106..111)).unwrap(), "world");
        assert!(cache.read(&key(1), Some(90..105)).is_none());
        assert!(cache.read(&key(1), None).is_none(), "length unknown");
        // Once the end of the pack is known, a range past it is cut there.
        // A read that came back short tells where the pack ends.
        cache.store(&key(1), 111, b"!", true);
        assert_eq!(cache.missing(&key(1), &(100..500)), []);
        assert_eq!(cache.read(&key(1), Some(100..500)).unwrap(), "hello world!");
        cache.store(&key(2), 0, b"whole pack", true);
        assert_eq!(cache.read(&key(2), None).unwrap(), "whole pack");
    }

    #[test]
    fn the_least_recently_used_pack_goes_first() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::open(dir.path(), 250).unwrap();
        for n in 1..=2 {
            cache.store(&key(n), 0, &[n; 100], true);
        }
        assert!(cache.read(&key(1), None).is_some(), "a read counts as use");
        cache.store(&key(3), 0, &[3; 100], true);
        assert!(cache.read(&key(2), None).is_none(), "the unread one went");
        assert!(cache.read(&key(1), None).is_some());
        assert!(cache.read(&key(3), None).is_some());
        assert!(!cache.path(&key(2)).exists());
        let stats = cache.stats();
        assert_eq!((stats.evicted, stats.used), (1, 200));
    }

    #[test]
    fn a_saved_cache_reopens_warm_and_an_unsaved_one_empty() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::open(dir.path(), 1 << 20).unwrap();
        cache.store(&key(1), 0, b"first", true);
        cache.store(&key(2), 7, b"second", false);
        cache.save().unwrap();
        drop(cache);

        let cache = PackCache::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(cache.read(&key(1), None).unwrap(), "first");
        assert_eq!(cache.read(&key(2), Some(7..13)).unwrap(), "second");
        assert_eq!(cache.stats().used, 11);
        drop(cache);

        // No save: the process is taken to have died.
        let cache = PackCache::open(dir.path(), 1 << 20).unwrap();
        assert!(cache.read(&key(1), None).is_none());
        assert!(!cache.path(&key(1)).exists());
        assert_eq!(cache.stats().used, 0);
    }
}
