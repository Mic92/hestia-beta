# Cache node (design draft)

Status: stage 2 is built, with a pack cache on the node; the rest is a
design. The numbers come from
`benches/cluster.rs`; what has not been measured is listed at the end.

## Problem

A build cluster shares one root. The bucket is durable but sits in
another datacenter: every request costs a cross-datacenter round trip
and all builders share one uplink. A cache node sits next to the
builders and takes those round trips off their path.

## What the bench says a builder pays

One root of a million paths in 50 segments, synthetic 1 KiB paths
(`HESTIA_BENCH_PATHS=1000000 HESTIA_BENCH_SEGMENTS=50 HESTIA_BENCH_SIZE=1024`).
The right column is the same fake store behind a datacenter-local link.
It stands for a node whose cache is warm, so it is the best a node can
do, not a measurement of one.

| Step | Bucket, 30 ms, 1 Gbit/s | Local, 1 ms, 10 Gbit/s |
|---|---|---|
| Daemon start (heads, every `.meta`) | 470 ms, 46 MB | 226 ms |
| Fetch 200 neighbouring paths | 165 ms, 5 requests | 56 ms |
| Fetch 200 scattered paths | 1313 ms, 231 requests | 493 ms |
| Drain 20 new paths, 2000 used | 344 ms, 7 requests | not rerun |
| Follow another writer's drain | 99 ms | 12 ms |
| Compact four small drains | 172 ms | not rerun |
| Follow that compaction | 100 ms | 18 ms |

The local column was measured before drains stopped copying the entries
of used paths; the two rows that change were not rerun.

What is left in the right column is CPU in the builder's own daemon
(opening segments, decoding pack indexes), which a blob cache cannot
remove. That is the argument for stage 2 below.

## Stage 1: a caching proxy for the store's own objects

The node speaks the directory layout hestia already reads and writes
(`pack/<xx>/`, `seg/<xx>/`, `heads/`), so a builder only points its store
URL at the node. No new client protocol.

- **Reads.** Packs, segments and trees are named by the hash of their
  bytes and never change, so the node keeps them on local disk without
  any invalidation. A range read of a pack the node lacks is passed
  through and the pack is fetched whole in the background.
- **Writes.** The node writes the object to its disk, acknowledges, and
  uploads to the bucket afterwards. The order hestia relies on is kept
  towards the bucket: a pack before the segment naming it, the segment
  before its head. A head is listed to other builders once its segment
  is on the node, so they see a drain after one local round trip.
- **Heads.** The node holds the head listing in memory, answers a
  listing in one request, and polls the bucket for heads written
  elsewhere (other nodes, GC).
- **Loss.** A node that dies loses what it acknowledged but had not
  uploaded. Nothing in the bucket then refers to it, so the cost is a
  rebuild, as with any cache miss.
- **Several nodes.** A builder picks the node by a hash of the object
  key, so a pack is cached on one node. Heads go to all.

## Stage 2: the node answers Nix

The node runs the substituter for the root, and builders use it as an
ordinary binary cache. A builder then loads no index at all, and the
pack reader's shared reads and read-ahead work across builders instead
of per builder.

The read half exists: `hestia serve --read-only --follow <SECONDS>
--listen <address>` serves a root to anyone and reloads it at that
interval. The bench plays 20 builders that start at once and each
fetch 200 paths, half of them common to all, from the same store as
above (30 ms, 1 Gbit/s):

| | Wall time | Requests | Downloaded |
|---|---|---|---|
| A daemon on every builder | 19.4 s | 3854 | 2384 MB |
| One shared daemon | 5.0 s | 1744 | 355 MB |

The shared daemon's index was loaded beforehand, as a running node's
is. Most of what the per-builder daemons download is the index, 46 MB
each. The link between builders and the shared daemon is not modelled.

### Drains through the node

A builder started with `--node <URL>` loads no index: it substitutes
from the node and its own daemon only pushes. A node
started with `--commit-interval <MILLISECONDS>` serves it two requests
(`src/node.rs`):

1. `POST /hestia/plan` with the hash and name of every path the builder
   is about to push. The node answers which are stored, and sends the
   indexes of the packs holding earlier paths of the same names. Those
   are where a rebuild's chunks repeat, so the builder uploads only
   chunks they lack.
2. The builder uploads its packs through the node (see below).
3. `POST /hestia/commit` with the sealed segment. The node waits until
   the bucket has the packs the segment names, merges what arrives
   within its commit interval into one segment, publishes one head and
   answers every builder with its name. A builder's drain has returned
   only once its paths are published, as before.

The node adds the paths it served since its last commit as used, so a
path that is fetched but never rebuilt stays alive across GC. A commit
planned before a GC moved chunks is refused and the builder drains
again.

The bench plays the store side of this: 20 builders that each built 20
paths and used 2000 stored ones finish at once.

| | Wall time | Requests | Uploaded | A follower then needs |
|---|---|---|---|---|
| A segment and head each | 1341 ms | 100 | 1.05 MB | 23 requests |
| One segment and head for all | 982 ms | 24 | 0.28 MB | 4 requests |

The first row leaves out what each builder pays to know the root
before it can drain: 471 ms and 46 MB to load it, or about 100 ms to
bring a loaded one up to date. Through the node that is one request on
the local link. The used lists are the same on every builder here,
which flatters the upload column.

Not done: the node authenticates nobody, so whoever reaches its port
can publish under its root. A commit of nothing but used paths waits
for the next drain.

### Packs on the node's disk

A node started with `--pack-cache <DIRECTORY>` keeps packs on local
disk, up to `--pack-cache-size` (`src/backend/cached.rs`):

- **Uploads.** A builder behind the node sends its packs to
  `PUT /hestia/pack/<key>`. The node checks the key is the hash of the
  body, writes the pack to its disk and answers. The bucket gets the
  pack afterwards, eight packs at a time. A builder needs no bucket
  credentials to upload.
- **Reads.** What a read is missing is fetched in 256 KiB blocks and
  kept. Blocks, not the ranges asked for, so a later read that overlaps
  an earlier one finds it. Whole packs are not fetched: a scattered
  fetch touches a small part of many packs. Readers that miss the same
  block at the same moment fetch it once.
- **Eviction.** The least recently used pack goes first. A pack the
  bucket does not have yet is never evicted. A pack GC deletes is
  dropped by the node that ran the GC at once.
- **Restart.** A clean exit records what is kept and the next start is
  warm. After a crash the disk cannot be trusted and the cache starts
  empty.

The substituter leaves reading ahead to the cache when there is one:
its own window changes with the order of requests, so the same paths
asked for twice would fetch different ranges.

Fetching 200 paths through a fresh daemon, same root and link as above:

| | Wall time | Bucket requests | Downloaded |
|---|---|---|---|
| Neighbouring paths, no pack cache | 215 ms | 6 | 2.8 MB |
| Neighbouring paths, node disk empty | 122 ms | 13 | 3.4 MB |
| Scattered paths, no pack cache | 1713 ms | 225 | 89.7 MB |
| Scattered paths, node disk empty | 1125 ms | 200 | 52.0 MB |
| Scattered paths again, node disk filled | 37 ms | 0 | 0 MB |

The block size was chosen on the scattered rows: 128 KiB downloads
26 MB and 1 MiB downloads 206 MB for the same 200 paths, at about the
same number of requests. The paths here are 1 KiB each, which favours
small blocks.

With the disk empty and 20 builders asking one node at once, half of
each builder's 200 paths the same on all of them:

| | Wall time | Bucket requests | Downloaded |
|---|---|---|---|
| No pack cache | 4604 ms | 1782 | 343.9 MB |
| Pack cache, every reader fetches what it misses | 4042 ms | 1616 | 422.1 MB |
| Pack cache, one fetch per block | 3442 ms | 1274 | 331.6 MB |

#### A head never names a pack the bucket lacks

The node answers a pack upload before the bucket has the pack, so
three rules keep a published segment whole:

- A commit waits for the packs its segment names. Only then does it
  join the group that is published.
- A pack the bucket refused fails every commit that names it, until a
  builder hands the pack in again. The builder's drain fails and its
  paths stay buffered for the next one, as with any failed drain.
- A node that died forgot which packs it still owed. The plan carries
  a number that names the node process, the commit sends it back, and a
  node refuses a commit planned with another process.

Packs waiting for the bucket may fill half the cache. Past that an
upload waits for room, and a pack larger than that half goes to the
bucket before the node answers. Waiting packs are read from disk when
their turn comes and cost no memory. A clean exit waits for all of
them.

20 builders hand in a 16 MiB pack each at once:

| | Builders answered after | Bucket has all after |
|---|---|---|
| Bucket first | 2796 ms | 2796 ms |
| Node's disk first | 105 ms | 2814 ms |

The drain as a whole still ends with the commit, which waits for the
bucket, so a builder that waits for its drain gains nothing from this
yet. What it gains is that chunking and packing the rest of a large
drain are no longer held up by the uplink. Not measured: a drain end
to end through a node, and a commit that returns before the bucket has
the packs.

### Which pack to evict

`examples/node_cache_sim.rs` replays a CI history against a node's
disk. The history is real: the 767 commits of 200 merges into git's
master (ten weeks, topic branches included), each built with a
derivation per compiled object (`examples/ci_trace/`). CI builds every
commit once on builders with empty stores; a step that runs reads its
inputs from the node and writes its output through it. Four test jobs
a run read the linked binary, which is an assumption. One run writes
45 MB and touches 82 MB.

Share of the bytes read that came from the bucket, evicting whole
packs (what one run wrote):

| Disk | LRU, uploads not kept | LRU | FIFO | SIEVE | S3-FIFO |
|---|---|---|---|---|---|
| 100 MB | 39.4% | 7.7% | 13.8% | 12.1% | 32.4% |
| 200 MB | 37.1% | 4.1% | 8.0% | 6.4% | 13.1% |
| 400 MB | 36.3% | 3.2% | 5.4% | 3.5% | 5.2% |
| 800 MB | 35.4% | 2.3% | 3.8% | 2.3% | 1.8% |
| 1600 MB | 34.6% | 1.5% | 2.6% | 1.6% | 1.4% |
| 3200 MB | 33.6% | 0.5% | 1.5% | 0.6% | 0.5% |
| 6400 MB | 33.4% | 0.3% | 0.8% | 0.3% | 0.2% |

- Not keeping uploads costs a third of all read bytes at any disk
  size: a fresh output is read by the next step before anyone fetched
  it.
- LRU is best or within a point of best everywhere. S3-FIFO, which
  holds new entries in a tenth of the disk until they are read, loses
  badly on small disks because here every output is read at least once,
  by the step that follows it.
- Evicting single outputs gives about the same numbers from 200 MB up,
  so packs are a fine unit.
- Disk matters more than the order: below about three runs' worth of
  writes every policy misses a lot, above about seventy there is little
  left to gain.

Without test jobs the linked binary is never read (13 GB of the 34 GB
written). LRU on packs then still matches or beats the others up to
400 MB and is within a point above.

Limits: one project, only compiled objects and no package outputs,
uncompressed sizes, and git stamps its version into one object, so the
archive and the binary are rebuilt for every commit and make up most
of the bytes.

## Stage 3: compiled objects

For a derivation per compiled object, the earlier measurements apply:
objects are stored whole from the builder, kept on nodes only, and
turned into deltas against the previous object of the same path after
the acknowledgement. This needs a rule that keeps a delta's base alive
as long as the delta, which is not designed yet.

## Not measured yet

- A node itself: acknowledgement latency with a disk sync, upload lag,
  cache hit rate under a real build's reads.
- Segment metadata of real store paths (references, derivers): the
  synthetic paths have none, so index sizes above are a lower bound.
- Packet loss and jitter. The fake network models a round trip and a
  shared link only, since the kernel here has no netem.
