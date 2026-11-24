# Cache node (design draft)

Status: a design, not code. The numbers come from `benches/cluster.rs`;
what has not been measured is listed at the end.

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
| Drain 20 new paths, 2000 used | 1369 ms, 32 requests | 354 ms |
| Follow another writer's drain | 102 ms | 12 ms |
| Compact four small drains | 80 ms | 19 ms |
| Follow that compaction | 106 ms | 18 ms |

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
ordinary binary cache. A builder then loads no index at all: no 46 MB
at start (more with real metadata), no following of other drains. The
pack reader's shared reads and read-ahead then work across builders
instead of per builder. Drains still chunk on the builder and upload
packs through stage 1; the node publishes the segment.

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
