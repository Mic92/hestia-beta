//! Read side of the segmented store: heads → view → `.meta` of the served
//! roots. `.tree` and pack indexes are fetched on first use.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use futures_util::future::join_all;
use tokio::sync::OnceCell;

use crate::backend::{Backend, Listed};
use crate::chunker::pack_cache_key;
use crate::heads::{self, GcRecord, HeadName, HeadRecord, Signed, View};
use crate::manifest::{
    ChunkHash, ChunkList, ChunkLocation, Directory, FileSystemObject, FileTree, Hash32, PackKey,
    PathEntry, PathHash, Regular, SegKey, Symlink,
};
use crate::segment::{
    self, ChunkRef, Chunks, Meta, Node, PackIndex, PackRow, Sealed, SegmentWriter, Tree,
};
use crate::trust::{GC_ROW, Trust};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Backend(#[from] crate::backend::Error),
    #[error(transparent)]
    Segment(#[from] segment::Error),
    #[error(transparent)]
    Trust(#[from] crate::trust::Error),
    #[error("{0} missing from the store")]
    Missing(String),
    #[error("{0} does not hash to its name")]
    Corrupt(String),
    #[error("pack {0} missing from the store")]
    MissingPack(PackKey),
}

impl Error {
    /// The object is gone or unusable for good (evicted, corrupt), as
    /// opposed to a backend hiccup a retry may cure.
    pub fn is_absent(&self) -> bool {
        matches!(
            self,
            Error::Missing(_) | Error::MissingPack(_) | Error::Corrupt(_) | Error::Segment(_)
        )
    }
}

/// Every key but a head's is `<kind>-<sha256 of the body>`.
pub fn meta_key(d: &SegKey) -> String {
    format!("seg-{d}")
}
pub fn tree_key(d: &SegKey) -> String {
    format!("tree-{d}")
}

pub struct Segment {
    pub digest: SegKey,
    pub meta: Meta,
    tree: OnceCell<Tree>,
    /// `Some(base_epoch)` for a segment this process published: served
    /// while a GC would still honour its head, whether or not the (lagging)
    /// listing shows it yet.
    own: Option<u64>,
}

/// Where chunks live, plus the index of each pack involved (read-ahead).
#[derive(Default)]
pub struct ChunkMap {
    pub chunks: BTreeMap<ChunkHash, ChunkLocation>,
    pub packs: HashMap<PackKey, Arc<PackIndex>>,
}

pub struct Resolved {
    pub entry: PathEntry,
    pub map: ChunkMap,
}

/// The store as seen through a set of roots. Immutable. A refresh builds
/// a new one that shares loaded segments and pack indexes.
pub struct Snapshot {
    backend: Backend,
    trust: Trust,
    roots: Vec<String>,
    pub view: View,
    /// In lookup priority: served roots in order, newest segment first.
    segments: Vec<Arc<Segment>>,
    pack_indexes: Mutex<HashMap<PackKey, Arc<PackIndex>>>,
}

async fn fetch(backend: &Backend, key: &str) -> Result<bytes::Bytes, Error> {
    backend
        .get(key, None)
        .await?
        .ok_or_else(|| Error::Missing(key.to_owned()))
}

/// Fetch a content object and check it is what its key names.
async fn fetch_verified(
    backend: &Backend,
    kind: &str,
    key: &SegKey,
) -> Result<bytes::Bytes, Error> {
    let name = format!("{kind}-{key}");
    let body = fetch(backend, &name).await?;
    if !key.verifies(&body) {
        return Err(Error::Corrupt(name));
    }
    Ok(body)
}

pub async fn fetch_meta(backend: &Backend, key: &SegKey) -> Result<Meta, Error> {
    Ok(Meta::open(&fetch_verified(backend, "seg", key).await?)?)
}

pub async fn fetch_tree(backend: &Backend, key: &SegKey) -> Result<Tree, Error> {
    Ok(Tree::open(&fetch_verified(backend, "tree", key).await?)?)
}

pub async fn fetch_pack_index(backend: &Backend, row: &PackRow) -> Result<PackIndex, Error> {
    let range = PackIndex::range(row.size, row.chunks);
    let bytes = backend
        .get(&pack_cache_key(&row.hash), Some(range))
        .await?
        .ok_or(Error::MissingPack(row.hash))?;
    Ok(PackIndex::decode(&bytes)?)
}

async fn signed_record(backend: &Backend, name: &str) -> Result<Option<(Signed, Vec<u8>)>, Error> {
    Ok(backend
        .get(name, None)
        .await?
        .and_then(|body| Some((Signed::decode(&body).ok()?, body.to_vec()))))
}

/// What a listed `g-*` name resolved to.
enum GcHead {
    /// Listed but not fetchable (listing lag, eviction, a delete in flight).
    Missing,
    /// Garbled, misnamed or its proof rejected: as if never published.
    Invalid,
    Valid(GcRecord),
}

async fn gc_record(backend: &Backend, trust: &Trust, name: &str) -> Result<GcHead, Error> {
    let Some(body) = backend.get(name, None).await? else {
        return Ok(GcHead::Missing);
    };
    let Some((s, r)) = Signed::decode(&body)
        .ok()
        .and_then(|s| Some((GcRecord::decode(&s.record).ok()?, s)))
        .map(|(r, s)| (s, r))
        .filter(|(_, r)| Some(r.head_name(&body)) == HeadName::parse(name))
    else {
        return Ok(GcHead::Invalid);
    };
    Ok(match trust.verify(GC_ROW, &s.record, &s.proof).await? {
        true => GcHead::Valid(r),
        false => GcHead::Invalid,
    })
}

/// A drain head whose body matches its name and whose proof `trust`
/// accepts for the root it claims.
async fn head_record(
    backend: &Backend,
    trust: &Trust,
    name: &str,
) -> Result<Option<HeadRecord>, Error> {
    let Some((s, body)) = signed_record(backend, name).await? else {
        return Ok(None);
    };
    let Some(r) = HeadRecord::decode(&s.record)
        .ok()
        .filter(|r| r.head_name(&body).to_string() == name)
    else {
        return Ok(None);
    };
    Ok(trust
        .verify(&r.root, &s.record, &s.proof)
        .await?
        .then_some(r))
}

/// The head listing and what it resolves to.
pub struct Heads {
    pub listed: Vec<Listed>,
    /// Newest valid GC record, with its name.
    pub gc: Option<(String, GcRecord)>,
    /// `g-*` newer than `gc` that were listed but could not be fetched.
    /// A reader makes do with the older record; GC must not, or it would
    /// recompute from a view that run already retired.
    pub gc_missing: Vec<String>,
    pub view: View,
}

impl Heads {
    /// Heads whose proof `trust` rejects count as not listed.
    pub async fn load(backend: &Backend, trust: &Trust) -> Result<Heads, Error> {
        let mut listed = Vec::new();
        for prefix in ["g-", "h-"] {
            listed.extend(backend.list(prefix, None).await?.expect("unbounded"));
        }
        let names = || listed.iter().map(|l| l.key.as_str());
        let mut gc = None;
        let mut gc_missing = Vec::new();
        for name in heads::newest_gc(names()) {
            match gc_record(backend, trust, name).await? {
                GcHead::Valid(record) => {
                    gc = Some((name.to_owned(), record));
                    break;
                }
                GcHead::Missing => gc_missing.push(name.to_owned()),
                GcHead::Invalid => {}
            }
        }
        let gc_ref = gc.as_ref().map(|(_, r)| r);
        let records: HashMap<String, HeadRecord> = join_all(
            heads::pending_heads(names(), gc_ref)
                .into_iter()
                .map(|n| async move {
                    head_record(backend, trust, n)
                        .await
                        .map(|r| r.map(|r| (n.to_owned(), r)))
                }),
        )
        .await
        .into_iter()
        .filter_map(Result::transpose)
        .collect::<Result<_, _>>()?;
        let view = View::compute(gc_ref, &records);
        Ok(Heads {
            listed,
            gc,
            gc_missing,
            view,
        })
    }
}

impl Snapshot {
    pub async fn load(
        backend: Backend,
        trust: Trust,
        roots: &[String],
        previous: Option<&Snapshot>,
    ) -> Result<Snapshot, Error> {
        let Heads { view, .. } = Heads::load(&backend, &trust).await?;
        let loaded: HashMap<SegKey, Arc<Segment>> = previous
            .into_iter()
            .flat_map(|p| p.segments.iter().map(|s| (s.digest, s.clone())))
            .collect();
        let mut segments = Vec::new();
        for digest in roots
            .iter()
            .filter_map(|r| view.roots.get(r))
            .flat_map(|d| d.iter().rev())
        {
            if let Some(s) = loaded.get(digest) {
                segments.push(s.clone());
                continue;
            }
            // Evicted or corrupt: its paths miss and get pushed again.
            match fetch_meta(&backend, digest).await {
                Ok(meta) => segments.push(Arc::new(Segment {
                    digest: *digest,
                    meta,
                    tree: OnceCell::new(),
                    own: None,
                })),
                Err(err) => eprintln!("hestia: skipping segment {digest}: {err}"),
            }
        }
        // Read-your-writes under listing lag: same window as `View::compute`.
        for s in previous.into_iter().flat_map(|p| &p.segments) {
            if s.own.is_some_and(|b| b + 1 >= view.epoch)
                && !segments.iter().any(|x| x.digest == s.digest)
            {
                segments.insert(0, s.clone());
            }
        }
        let pack_indexes = previous
            .map(|p| p.pack_indexes.lock().unwrap().clone())
            .unwrap_or_default();
        Ok(Snapshot {
            backend,
            trust,
            roots: roots.to_vec(),
            view,
            segments,
            pack_indexes: Mutex::new(pack_indexes),
        })
    }

    /// The same roots against the store as it is now, reusing what is
    /// loaded. When the backend cannot be listed or read right now, the
    /// store as last seen: a drain then claims against a base at most one
    /// drain old instead of keeping its paths hostage to the listing.
    pub async fn reload(&self) -> Result<Snapshot, Error> {
        let fresh = Snapshot::load(
            self.backend.clone(),
            self.trust.clone(),
            &self.roots,
            Some(self),
        )
        .await;
        match fresh {
            Err(Error::Backend(e)) => {
                eprintln!("hestia: cannot refresh the view, using the last one: {e}");
                Ok(Snapshot {
                    backend: self.backend.clone(),
                    trust: self.trust.clone(),
                    roots: self.roots.clone(),
                    view: self.view.clone(),
                    segments: self.segments.clone(),
                    pack_indexes: Mutex::new(self.pack_indexes.lock().unwrap().clone()),
                })
            }
            other => other,
        }
    }

    /// Reload, then make sure `sealed` (just published under the first
    /// root) is served even if the listing does not show its head yet.
    pub async fn refresh_with(&self, sealed: &Sealed) -> Result<Snapshot, Error> {
        let mut next = self.reload().await?;
        let digest = sealed.key;
        if !next.segments.iter().any(|s| s.digest == digest) {
            let segment = Segment {
                digest,
                meta: Meta::open(&sealed.meta)?,
                tree: OnceCell::new_with(Some(Tree::open(&sealed.tree)?)),
                own: Some(self.view.epoch),
            };
            next.segments.insert(0, Arc::new(segment));
        }
        Ok(next)
    }

    /// Copy a stored entry into `writer`. `false` if no served segment has
    /// it or it is unservable as stored.
    pub async fn copy_entry(
        &self,
        hash: &PathHash,
        writer: &mut SegmentWriter,
    ) -> Result<bool, Error> {
        let Some((seg, i)) = self.find(hash) else {
            return Ok(false);
        };
        let tree = match self.tree(seg).await {
            Ok(tree) => tree,
            // Unservable as stored; nothing to carry over.
            Err(e) if e.is_absent() => return Ok(false),
            Err(e) => return Err(e),
        };
        let mut node = tree.node(i, &seg.meta.packs)?;
        node.map_chunks(&mut |c| {
            let row = &seg.meta.packs[c.pack as usize];
            ChunkRef {
                pack: writer.pack(row.hash, row.size, row.chunks),
                ..c
            }
        });
        writer.push(seg.meta.entry(i, node));
        Ok(true)
    }

    /// Those of `hashes` whose stored entry [`Self::copy_entry`] can carry
    /// over: found, and its segment's tree still loads.
    pub async fn copyable(&self, hashes: &[PathHash]) -> Result<BTreeSet<PathHash>, Error> {
        let mut ok = BTreeSet::new();
        let mut seen = BTreeSet::new();
        for seg in &self.segments {
            // First segment wins, as in `find`; one tree fetch per segment.
            let hits: Vec<PathHash> = hashes
                .iter()
                .filter(|h| !seen.contains(*h) && seg.meta.find(h).is_some())
                .copied()
                .collect();
            if hits.is_empty() {
                continue;
            }
            seen.extend(hits.iter().copied());
            match self.tree(seg).await {
                Ok(_) => ok.extend(hits),
                Err(e) if e.is_absent() => eprintln!("hestia: {e}; storing its paths anew"),
                Err(e) => return Err(e),
            }
        }
        Ok(ok)
    }

    /// Load the pack indexes behind stored entries with these names. An
    /// optimisation (dedup against the previous build), so evicted trees
    /// or packs are skipped.
    pub async fn load_indexes_for(&self, names: &BTreeSet<&str>) -> Result<(), Error> {
        for seg in &self.segments {
            let hits: Vec<usize> = (0..seg.meta.len())
                .filter(|&i| names.contains(seg.meta.name(i)))
                .collect();
            if hits.is_empty() {
                continue;
            }
            let tree = match self.tree(seg).await {
                Ok(tree) => tree,
                Err(e) if e.is_absent() => continue,
                Err(e) => return Err(e),
            };
            let mut packs = BTreeSet::new();
            for i in hits {
                tree.node(i, &seg.meta.packs)?
                    .for_each_chunk(&mut |c| _ = packs.insert(c.pack));
            }
            for p in packs {
                match self.pack_index(&seg.meta.packs[p as usize]).await {
                    Err(e) if !e.is_absent() => return Err(e),
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Chunks locatable without a fetch: every pack index loaded so far.
    pub fn known_chunks(&self) -> KnownChunks {
        let mut known = KnownChunks::default();
        for (pack, index) in self.pack_indexes.lock().unwrap().iter() {
            known.add(*pack, index);
        }
        known
    }

    /// Distinct paths served.
    pub fn path_count(&self) -> usize {
        self.path_hashes().len()
    }

    pub fn path_hashes(&self) -> BTreeSet<PathHash> {
        self.segments
            .iter()
            .flat_map(|s| (0..s.meta.len()).map(|i| s.meta.hash(i)))
            .collect()
    }

    pub fn pack_hashes(&self) -> BTreeSet<PackKey> {
        self.segments
            .iter()
            .flat_map(|s| s.meta.packs.iter().map(|p| p.hash))
            .collect()
    }

    pub fn by_nar_hash(&self, nar_hash: &Hash32) -> Option<PathHash> {
        self.segments.iter().find_map(|s| {
            (0..s.meta.len())
                .find_map(|i| (s.meta.body(i).nar_hash == *nar_hash).then(|| s.meta.hash(i)))
        })
    }

    /// Packs holding chunks of `hash` (empty if unknown).
    pub async fn packs_of(&self, hash: &PathHash) -> Result<BTreeSet<PackKey>, Error> {
        let mut packs = BTreeSet::new();
        if let Some((seg, i)) = self.find(hash) {
            self.tree(seg)
                .await?
                .node(i, &seg.meta.packs)?
                .for_each_chunk(&mut |c| _ = packs.insert(seg.meta.packs[c.pack as usize].hash));
        }
        Ok(packs)
    }

    fn find(&self, hash: &PathHash) -> Option<(&Segment, usize)> {
        self.segments
            .iter()
            .find_map(|s| s.meta.find(hash).map(|i| (&**s, i)))
    }

    pub fn contains(&self, hash: &PathHash) -> bool {
        self.find(hash).is_some()
    }

    /// Without the file tree: enough for narinfo.
    pub fn lookup(&self, hash: &PathHash) -> Option<PathEntry> {
        let (seg, i) = self.find(hash)?;
        let empty = Node::Directory {
            entries: BTreeMap::new(),
        };
        Some(path_entry(
            seg.meta.entry(i, empty.clone()),
            to_file_tree(&empty, &mut |_| unreachable!()),
        ))
    }

    async fn tree<'a>(&self, seg: &'a Segment) -> Result<&'a Tree, Error> {
        seg.tree
            .get_or_try_init(|| fetch_tree(&self.backend, &seg.meta.tree))
            .await
    }

    async fn pack_index(&self, row: &PackRow) -> Result<Arc<PackIndex>, Error> {
        if let Some(idx) = self.pack_indexes.lock().unwrap().get(&row.hash) {
            return Ok(idx.clone());
        }
        let idx = Arc::new(fetch_pack_index(&self.backend, row).await?);
        self.pack_indexes
            .lock()
            .unwrap()
            .insert(row.hash, idx.clone());
        Ok(idx)
    }

    pub async fn resolve(&self, hash: &PathHash) -> Result<Option<Resolved>, Error> {
        let Some((seg, i)) = self.find(hash) else {
            return Ok(None);
        };
        let node = self.tree(seg).await?.node(i, &seg.meta.packs)?;

        let mut rows = BTreeSet::new();
        node.for_each_chunk(&mut |c| _ = rows.insert(c));
        let mut map = ChunkMap::default();
        let mut indexes: HashMap<u32, (PackKey, Arc<PackIndex>)> = HashMap::new();
        for c in rows {
            let (pack, index) = match indexes.get(&c.pack) {
                Some(x) => x,
                None => {
                    let row = &seg.meta.packs[c.pack as usize];
                    let index = self.pack_index(row).await?;
                    map.packs.insert(row.hash, index.clone());
                    indexes.entry(c.pack).or_insert((row.hash, index))
                }
            };
            let e = index
                .get(c.chunk)
                .ok_or_else(|| Error::Missing(format!("chunk {} of pack {pack}", c.chunk)))?;
            map.chunks.entry(e.hash).or_insert(ChunkLocation {
                pack: *pack,
                offset: e.offset,
                compressed_size: e.compressed_size,
                uncompressed_size: e.uncompressed_size,
            });
        }
        let tree = to_file_tree(&node, &mut |c| {
            indexes[&c.pack].1.entries[c.chunk as usize].hash
        });
        Ok(Some(Resolved {
            entry: path_entry(seg.meta.entry(i, node), tree),
            map,
        }))
    }
}

fn path_entry(e: segment::Entry, tree: FileTree<ChunkList>) -> PathEntry {
    PathEntry {
        store_path: e.path,
        nar_hash: e.nar_hash,
        nar_size: e.nar_size,
        references: e.references,
        ca: e.ca,
        deriver: e.deriver,
        tree,
    }
}

fn to_file_tree(
    node: &Node,
    hash_of: &mut impl FnMut(ChunkRef) -> ChunkHash,
) -> FileTree<ChunkList> {
    FileTree(match node {
        Node::Regular {
            executable,
            chunks,
            rewrites,
        } => FileSystemObject::Regular(Regular {
            executable: *executable,
            contents: ChunkList {
                chunks: chunks.0.iter().map(|c| hash_of(*c)).collect(),
                rewrites: rewrites.clone(),
            },
        }),
        Node::Symlink { target } => FileSystemObject::Symlink(Symlink {
            target: target.clone(),
        }),
        Node::Directory { entries } => FileSystemObject::Directory(Directory {
            entries: entries
                .iter()
                .map(|(n, c)| (n.clone(), Box::new(to_file_tree(c, hash_of))))
                .collect(),
        }),
    })
}

/// Chunks locatable through a loaded pack index.
#[derive(Default)]
pub struct KnownChunks {
    chunks: HashMap<ChunkHash, (PackKey, u32)>,
    /// `(size, entries)` per pack.
    packs: HashMap<PackKey, (u64, u32)>,
}

impl KnownChunks {
    pub fn add(&mut self, pack: PackKey, index: &PackIndex) {
        self.packs
            .insert(pack, (index.size(), index.entries.len() as u32));
        for (i, e) in index.entries.iter().enumerate() {
            self.chunks.entry(e.hash).or_insert((pack, i as u32));
        }
    }

    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.chunks.contains_key(hash)
    }
}

fn from_file_tree(
    tree: &FileTree<ChunkList>,
    writer: &mut SegmentWriter,
    known: &KnownChunks,
) -> Option<Node> {
    Some(match &tree.0 {
        FileSystemObject::Regular(r) => {
            let mut chunks = Vec::with_capacity(r.contents.chunks.len());
            for h in &r.contents.chunks {
                let &(pack, chunk) = known.chunks.get(h)?;
                let (size, n) = known.packs[&pack];
                chunks.push(ChunkRef {
                    pack: writer.pack(pack, size, n),
                    chunk,
                });
            }
            Node::Regular {
                executable: r.executable,
                chunks: Chunks(chunks),
                rewrites: r.contents.rewrites.clone(),
            }
        }
        FileSystemObject::Symlink(l) => Node::Symlink {
            target: l.target.clone(),
        },
        FileSystemObject::Directory(d) => {
            let mut entries = BTreeMap::new();
            for (name, child) in &d.entries {
                entries.insert(name.clone(), from_file_tree(child, writer, known)?);
            }
            Node::Directory { entries }
        }
    })
}

/// Add a legacy-shaped entry to `writer`. `None` if a chunk cannot be located.
pub fn push_entry(
    writer: &mut SegmentWriter,
    entry: &PathEntry,
    known: &KnownChunks,
) -> Option<()> {
    let tree = from_file_tree(&entry.tree, writer, known)?;
    writer.push(segment::Entry {
        path: entry.store_path.clone(),
        nar_hash: entry.nar_hash,
        nar_size: entry.nar_size,
        references: entry.references.clone(),
        deriver: entry.deriver.clone(),
        ca: entry.ca.clone(),
        tree,
    });
    Some(())
}

async fn put_segment(backend: &Backend, sealed: &Sealed) -> Result<SegKey, Error> {
    backend
        .put(&tree_key(&sealed.tree_key), sealed.tree.clone().into())
        .await?;
    backend
        .put(&meta_key(&sealed.key), sealed.meta.clone().into())
        .await?;
    Ok(sealed.key)
}

pub async fn signed(trust: &Trust, record: Vec<u8>) -> Result<Vec<u8>, crate::trust::Error> {
    let proof = trust.sign(&record).await?;
    Ok(Signed { record, proof }.encode())
}

/// Upload a sealed segment and a signed `h-*` claiming it for `root`.
pub async fn publish(
    backend: &Backend,
    trust: &Trust,
    view: &View,
    root: &str,
    sealed: &Sealed,
    now: u64,
) -> Result<String, Error> {
    let record = HeadRecord {
        root: root.to_owned(),
        base_epoch: view.epoch,
        seg: put_segment(backend, sealed).await?,
        time: now,
    };
    let body = signed(trust, record.encode()).await?;
    let name = record.head_name(&body).to_string();
    backend.put(&name, body.into()).await?;
    Ok(name)
}
