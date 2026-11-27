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
//!
//! A pack a builder hands in ([`Cached::put_behind`]) is answered once it
//! is on this disk and goes to the bucket afterwards. Until it is there
//! it cannot be evicted, and whoever publishes a segment naming it waits
//! for it first ([`Cached::settled`]).

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
use crate::chunker::pack_cache_key;
use crate::manifest::PackHash;

const PACK_PREFIX: &str = "pack-";
/// Written by [`PackCache::save`], consumed by [`PackCache::open`].
const INDEX: &str = "index";
/// Missing parts of a pack are fetched in multiples of this.
const BLOCK: u64 = 256 * 1024;
/// Packs on their way to the bucket behind the builders' backs, at once.
const BEHIND_UPLOADS: usize = 8;

#[derive(Default)]
struct Entry {
    tick: u64,
    /// Kept ranges, start to end, merged where they touch.
    extents: BTreeMap<u64, u64>,
    bytes: u64,
    /// The pack's length, once it was seen whole.
    len: Option<u64>,
    /// The bucket does not have this pack yet: not in `State::order`, so
    /// never evicted.
    pinned: bool,
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
            if !entry.pinned {
                self.order.insert(tick, key.to_owned());
            }
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

    /// Holds a pack that is kept whole until [`PackCache::unpin`].
    fn pin(&self, key: &str) -> bool {
        let mut state = self.state.lock().expect("pack cache lock poisoned");
        let Some(entry) = state.entries.get_mut(key) else {
            return false;
        };
        if !entry.len.is_some_and(|len| entry.covers(&(0..len))) {
            return false;
        }
        entry.pinned = true;
        let tick = entry.tick;
        state.order.remove(&tick);
        true
    }

    fn unpin(&self, key: &str) {
        {
            let mut state = self.state.lock().expect("pack cache lock poisoned");
            let Some(entry) = state.entries.get_mut(key) else {
                return;
            };
            entry.pinned = false;
            let tick = entry.tick;
            state.order.insert(tick, key.to_owned());
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

/// Whether a pack handed in by a builder reached the bucket: `None`
/// while it is on its way.
type Landed = tokio::sync::watch::Receiver<Option<Result<(), String>>>;

/// One lock per block of a pack that is being fetched, so that readers
/// missing the same block fetch it once.
#[derive(Default)]
struct Flights(Mutex<HashMap<(String, u64), BlockLock>>);

type BlockLock = Arc<tokio::sync::Mutex<()>>;

struct Flight<'a> {
    flights: &'a Flights,
    key: &'a str,
    held: Vec<(u64, tokio::sync::OwnedMutexGuard<()>)>,
}

impl Flights {
    /// Blocks are taken in ascending order, so two readers never wait on
    /// each other.
    async fn hold<'a>(&'a self, key: &'a str, range: &Range<u64>) -> Flight<'a> {
        let mut flight = Flight {
            flights: self,
            key,
            held: Vec::new(),
        };
        for block in range.start / BLOCK..range.end.div_ceil(BLOCK) {
            let lock = Arc::clone(
                self.0
                    .lock()
                    .expect("flight lock poisoned")
                    .entry((key.to_owned(), block))
                    .or_default(),
            );
            flight.held.push((block, lock.lock_owned().await));
        }
        flight
    }
}

impl Drop for Flight<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.0.lock().expect("flight lock poisoned");
        for (block, guard) in self.held.drain(..) {
            drop(guard);
            let id = (self.key.to_owned(), block);
            // Whoever waits for this block holds a reference of their own.
            if flights.get(&id).is_some_and(|l| Arc::strong_count(l) == 1) {
                flights.remove(&id);
            }
        }
    }
}

/// `inner` with its packs read through and written through a [`PackCache`].
pub struct Cached {
    pub inner: Backend,
    pub packs: Arc<PackCache>,
    flights: Flights,
    /// Packs builders were told are taken that the bucket may not have:
    /// on their way, or failed and not handed in again since.
    behind: Mutex<HashMap<String, Landed>>,
    uploads: tokio::sync::Semaphore,
    /// KiB of packs that may wait for the bucket at once.
    waiting: Arc<tokio::sync::Semaphore>,
    waiting_kib: u32,
}

impl Cached {
    pub fn new(inner: Backend, packs: Arc<PackCache>) -> Arc<Cached> {
        // Pinned packs must leave room for what is read.
        let waiting_kib = u32::try_from(packs.capacity / 2 / 1024).unwrap_or(u32::MAX);
        Arc::new(Cached {
            inner,
            packs,
            flights: Flights::default(),
            behind: Mutex::default(),
            uploads: tokio::sync::Semaphore::new(BEHIND_UPLOADS),
            waiting: Arc::new(tokio::sync::Semaphore::new(waiting_kib as usize)),
            waiting_kib,
        })
    }

    /// Takes a builder's pack and answers `None` once it is on this disk;
    /// the bucket gets it afterwards. A pack too large to wait here goes
    /// to the bucket first, as [`Cached::put`] does, and the answer says
    /// whether the bucket lacked it.
    pub async fn put_behind(
        self: &Arc<Self>,
        key: &str,
        data: Bytes,
    ) -> Result<Option<bool>, Error> {
        let kib = (data.len() as u64).div_ceil(1024);
        if kib > u64::from(self.waiting_kib) {
            let created = self.put(key, data.clone()).await?;
            if !created {
                self.inner.refresh(key, data).await?;
            }
            return Ok(Some(created));
        }
        let (landed, landing) = tokio::sync::watch::channel(None);
        {
            let mut behind = self.behind.lock().expect("behind lock poisoned");
            if behind.get(key).is_some_and(|l| l.borrow().is_none()) {
                // The same pack from another builder is on its way.
                return Ok(None);
            }
            behind.insert(key.to_owned(), landing);
        }
        let (kept, on_disk) = tokio::sync::oneshot::channel();
        let (this, key) = (Arc::clone(self), key.to_owned());
        // A task of its own: the builder hanging up must not leave the
        // pack recorded as on its way for ever.
        tokio::spawn(async move {
            let outcome = this.land(&key, data, kib as u32, kept).await;
            match &outcome {
                Ok(()) => {
                    this.behind
                        .lock()
                        .expect("behind lock poisoned")
                        .remove(&key);
                }
                Err(err) => {
                    eprintln!("hestia: {key} did not reach the store: {err}");
                    this.packs.remove(&key);
                }
            }
            let _ = landed.send(Some(outcome));
        });
        // Dropped unsent when the pack could not be kept: then this waits
        // for the bucket.
        let _ = on_disk.await;
        Ok(None)
    }

    async fn land(
        &self,
        key: &str,
        data: Bytes,
        kib: u32,
        kept: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), String> {
        let _room = Arc::clone(&self.waiting)
            .acquire_many_owned(kib)
            .await
            .map_err(|err| err.to_string())?;
        self.keep(key, 0, &data, true).await;
        let pinned = self.packs.pin(key);
        // From here the pack is read from disk again when its turn comes,
        // so waiting packs cost no memory.
        let data = if pinned {
            let _ = kept.send(());
            None
        } else {
            Some(data)
        };
        let sent = async {
            let _turn = self.uploads.acquire().await.map_err(|e| e.to_string())?;
            let data = match data {
                Some(data) => data,
                None => {
                    // The key was checked against the bytes that came
                    // in; these are the bytes the disk gave back.
                    let (packs, owned) = (Arc::clone(&self.packs), key.to_owned());
                    tokio::task::spawn_blocking(move || {
                        let data = packs.read(&owned, None).ok_or("the kept copy is gone")?;
                        let name = pack_cache_key(&PackHash::digest(&data));
                        (name == owned)
                            .then_some(data)
                            .ok_or("the kept copy is damaged")
                    })
                    .await
                    .map_err(|err| err.to_string())??
                }
            };
            let created = self.inner.put(key, data.clone()).await;
            if !created.map_err(|e| e.to_string())? {
                // Stored by someone who may never have published it.
                let refreshed = self.inner.refresh(key, data).await;
                refreshed.map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        .await;
        if pinned {
            self.packs.unpin(key);
        }
        sent
    }

    /// Waits until the bucket has every pack of `keys` that was handed in
    /// through [`Cached::put_behind`]. A pack that did not get there
    /// fails this until it is handed in again.
    pub async fn settled(&self, keys: &[String]) -> Result<(), String> {
        let waiting: Vec<(String, Landed)> = {
            let behind = self.behind.lock().expect("behind lock poisoned");
            keys.iter()
                .filter_map(|key| Some((key.clone(), behind.get(key)?.clone())))
                .collect()
        };
        for (key, mut landed) in waiting {
            let outcome = landed
                .wait_for(Option::is_some)
                .await
                .map_err(|_| format!("{key}: upload abandoned"))?
                .clone();
            if let Some(Err(err)) = outcome {
                return Err(format!("{key} did not reach the store: {err}"));
            }
        }
        Ok(())
    }

    /// Waits for every pack that is on its way to the bucket.
    pub async fn settle_all(&self) {
        let keys: Vec<String> = {
            let behind = self.behind.lock().expect("behind lock poisoned");
            behind.keys().cloned().collect()
        };
        let _ = self.settled(&keys).await;
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
        // Another reader may be fetching these blocks: then nothing is
        // missing once it is done.
        let flight = self.flights.hold(key, &around).await;
        for gap in self.packs.missing(key, &around) {
            let Some(data) = self.inner.get(key, Some(gap.clone())).await? else {
                return Ok(None);
            };
            fetched += data.len() as u64;
            self.keep(key, gap.start, &data, false).await;
        }
        drop(flight);
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
            // The bucket has it now, whatever happened to an earlier try.
            let mut behind = self.behind.lock().expect("behind lock poisoned");
            if behind.get(key).is_some_and(|l| l.borrow().is_some()) {
                behind.remove(key);
            }
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
    fn a_pack_the_store_lacks_is_not_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::open(dir.path(), 250).unwrap();
        cache.store(&key(1), 0, &[1; 100], false);
        assert!(!cache.pin(&key(1)), "only a whole pack can be held");
        cache.store(&key(1), 0, &[1; 100], true);
        assert!(cache.pin(&key(1)));
        for n in 2..=4 {
            cache.store(&key(n), 0, &[n; 100], true);
        }
        assert!(cache.read(&key(1), None).is_some(), "oldest, but held");
        assert!(cache.read(&key(2), None).is_none());
        cache.unpin(&key(1));
        cache.store(&key(5), 0, &[5; 100], true);
        assert!(
            cache.read(&key(3), None).is_none(),
            "the read made 1 recent"
        );
        assert!(cache.read(&key(1), None).is_some());
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
