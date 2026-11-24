# Cache node (design draft)

Status: stage 2 is built, the rest is a design. The numbers come from
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
2. The builder uploads its packs to the store itself.
3. `POST /hestia/commit` with the sealed segment. The node merges what
   arrives within its commit interval into one segment, publishes one
   head and answers every builder with its name. A builder's drain has
   returned only once its paths are published, as before.

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
can publish under its root. Packs still go from the builder to the
bucket, not through the node. A commit of nothing but used paths waits
for the next drain.

## Stage 3: compiled objects

For a derivation per compiled object, the earlier measurements apply:
objects are stored whole from the builder, kept on nodes only, and
turned into deltas against the previous object of the same path after
the acknowledgement. This needs a rule that keeps a delta's base alive
as long as the delta, which is not designed yet.

## Not measured yet

- A node itself: acknowledgement latency with a disk sync, upload lag,
  cache hit rate under a real build's reads.
- Whole-pack fetch on first touch against caching only the ranges read.
- Segment metadata of real store paths (references, derivers): the
  synthetic paths have none, so index sizes above are a lower bound.
- Packet loss and jitter. The fake network models a round trip and a
  shared link only, since the kernel here has no netem.
