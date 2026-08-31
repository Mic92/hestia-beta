# Architecture

hestia turns the GitHub Actions cache into a Nix binary cache. On paper
that is a poor fit: entries are write-once, keys can never be
overwritten, and anything idle for a week gets evicted. Most of the
design below exists to work around those three constraints.

## Runtime view

One daemon runs per job (`hestia serve`, started by the action's main
step). It speaks to Nix on two channels: a unix socket for paths the
job builds, and an HTTP listener serving the Nix binary cache protocol.

### Serving

![serving cached paths back to Nix](serve.svg)

The action puts the daemon first in `extra-substituters`, so Nix asks
it before cache.nixos.org. A narinfo hit answers straight from the
segments loaded at startup. A NAR request is more involved: the daemon fetches the path's
chunks from pack blobs with HTTP Range reads, reassembles the NAR, and
verifies its hash before the first byte leaves the process. Any failure
along the way (evicted pack, missing chunk, hash mismatch) becomes a
404 and Nix quietly falls through to the next substituter; the daemon
would rather serve nothing than something corrupt.

### Pushing

![caching built paths](push.svg)

Nix runs `hestia hook` after every successful build; the hook forwards
the built paths over the unix socket and the daemon buffers them in
memory. Uploads happen on drain: the action's post step, the idle
timeout, or SIGTERM. A drain takes the buffered paths plus everything
the substituter served, chunks the new ones, uploads packs, and
publishes a segment plus a head naming it.

## Storage view

![segments, heads and packs](segments.svg)

hestia creates three kinds of cache entry, all write-once: pack blobs
(`pack-<sha256>-<nonce>`, chunk frames followed by their index), segments
(`seg-<sha256>-<nonce>` for the `.meta` part, which names its
`tree-<sha256>-<nonce>`), and heads (`g-*`, `h-*`). Everything but a
head carries the SHA-256 of its bytes in its name and is verified
against it on every read; the nonce makes every upload a distinct key,
so a writer never inherits another writer's (possibly half-finished)
entry and a key always identifies exactly one claim. On OCI the blob
itself is stored by digest and the nonce lives in the manifest.

The same objects go into one of three stores: the Actions cache (the
default, evicts by LRU), an OCI registry (blobs plus one manifest each,
heads as tags) or an S3-compatible bucket (`pack/<xx>/`, `seg/`,
`heads/` under a prefix). The rest of this document is store-agnostic:
a store only needs put, ranged get, list by prefix and delete.

### Segments and heads

A segment is what one writer published for one root: `.meta` holds a
sorted path index, per-path narinfo fields and a pack table with a
live-chunk bitset per pack; `.tree` holds the file trees, where a
file's contents is a list of `(pack row, index in pack)` references
plus reference rewrites (see below). Nothing in a segment is ever
modified, so there is no merge conflict to resolve: concurrent drains
simply publish one segment each. Lengths, table indices
and nesting depth read from a segment are bounds-checked before use:
storage is untrusted input.

Which segments make up a root is decided by heads, small CBOR records:
`h-<epoch>-<root>-<time>-<sha256>` (a drain's claim: root name, the GC
epoch it read, the segment it added) and `g-<epoch>-<time>-<sha256>`
(GC's record: one base segment per root). The name is derived from the
body, so a body is valid under exactly one name. A reader lists the
heads, takes the newest GC record as the base and adds the segments of
drain heads of the roots it serves that are based on this epoch or the
one before. A drain therefore re-lists when it starts rather than
claiming against the view `serve` loaded hours earlier, and keeps
serving what it published itself for that window even if the listing
lags. Writers only ever append; nothing but GC merges or replaces
segments, so the view rule is a plain union and a busy root simply has
more segments to fetch in parallel until the next GC.
`docs/spec/segments.qnt` is the model of these rules (R1–R5 in its
header), `docs/spec/trust.qnt` the model of what a malicious writer can
do under each trust policy.

Where the store has no write scopes (registries, buckets) a head's body
carries a cosign bundle: over the name for `h-*`, over the record with
the proof field cleared for `c-*` and `g-*`. Readers verify pending heads
in parallel against a per-root policy before computing the view and take
the newest `g-*` that verifies, so a head from an unlisted signer is as
if never published. Content objects need no signature: they are only
reachable through a head and verified against their names.

### Packs

Store paths are not cached one entry each. NARs are split into
content-defined chunks (FastCDC, 16–256 KiB, 64 KiB average), each
chunk is zstd-compressed individually, and compressed chunks are
concatenated into pack blobs of about 64 MiB. The pack key is the
SHA-256 of the blob, so identical packs dedup naturally and a finalized
pack can be trusted to match its name. Chunk hashes use BLAKE3 (the hot
path, ~3x faster and unconstrained by any format).

This layout buys three things. Chunking dedups across paths and
versions: a rebuilt package shares most of its chunks with the previous
build, so only the changed chunks are new bytes. Per-chunk compression
keeps every chunk independently extractable: serving one path touches
only its chunks, fetched with Range reads, not whole packs. And packing
keeps the entry count low, which matters because both REST list
operations and the eviction clock work per entry.

### Reference normalization

Store paths embed the 32-character base32 hashes of their references
(and their own self-reference) in file contents. When a dependency is
rebuilt its hash changes, so every chunk covering an occurrence churns
even though nothing else in the file changed.

hestia rewrites those occurrences to zeros before chunking, so the
stored chunk stays identical across rebuilds. Each occurrence is
recorded in a per-file position table (`ChunkList::rewrites`: file
offset + reference index); on NAR reassembly the daemon copies the real
hash back into each span. The hashes are not stored twice -- they come
from the path's `references` in the `PathEntry`, and a reference's index
is its position in the sorted, deduplicated reference set, so write and
read derive identical indices.

Restoring from the position table is chosen over re-scanning each served
NAR: a benchmark measured 20-30 GB/s against under 1 GB/s, and it
restores losslessly with no chance of a sentinel colliding with genuine
content. The write side scans each file once to find the occurrences
regardless; recording their offsets during that scan is free.

Restoration keys off whether a file's `rewrites` is non-empty, so
reference-free files pass through untouched (and keep the single-chunk
zero-copy fast path). Correctness is checked, not assumed: the write
pipeline re-derives the NAR hash through the same restore path before
upload, and the substituter re-verifies the full NAR hash before
serving. Any disagreement is a 404, and Nix falls through.

### Roots and GC

What stays alive is decided by roots, one per branch and system (e.g.
`main-x86_64-linux`). Every drain publishes a segment naming what the
job pushed, found stored, or substituted. GC compacts each root to the
union of what drains named since its previous run, so matrix legs and
re-runs accumulate while closures no job uses any more die. GC is the
only deleter and the only rewriter: it repacks mostly-dead packs,
publishes the new GC head and only then deletes, from its own listing,
what is reachable from neither the new view nor the one it loaded and
is older than a drain could take. The previous `g-*` therefore survives
one epoch, and everything the new view reaches (heads, `.meta`, `.tree`,
packs) gets its LRU clock reset. A root whose claims cannot all be read
or merged is carried over unmerged (`RootRow.unmerged`) and retried next
run: a read error costs an epoch of compaction, never a path. A `g-*`
that is listed but cannot be fetched, or a verifier that cannot run,
aborts the run instead (`src/gc.rs`).

On the Actions cache GC lists, reads and deletes only the default
branch's scope: PR scopes are neither readable from there nor GC's to
manage, GitHub drops them with the branch. A drain re-lists `g-*` right
before publishing its head and reloads if GC ran twice since it started,
so a head is never published under a base readers already ignore.

### Crash safety

Order of operations does the heavy lifting. Packs are uploaded before
the segment that references them, the segment before its head, so a
head never points at a blob that was never finalized. A crash in
between leaves orphans (on the Actions cache possibly a reserved key
that will never hold data), which GC deletes once they are older than a
drain could take; thanks to the nonce no later writer ever lands on
such a key. The reverse hazard, GitHub evicting a pack a segment
still references, is handled at read time (404 → next substituter) and
by GC dropping the affected entries so the next job pushes them again.
