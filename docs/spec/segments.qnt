// -*- mode: Bluespec; -*-
//
// hestia segmented store: drains, GC (root reset, repack, listing sweep),
// head listing lag, GC crash, cache eviction and access scopes (gha).
// Two roots: writer `a` is a default-branch job (scope main, root main),
// writer `b` a PR job (scope pr, root pr). A PR job reads both scopes and
// writes its own; GC runs on the default branch and reads only main.
// Ids stand in for object keys. The only clock is the GC epoch.
//
//   quint run    docs/spec/segments.qnt --main segments_main      --invariant safety --max-steps 40 --max-samples 20000
//   quint verify docs/spec/segments.qnt --main segments_main      --invariant safety --max-steps 24 --backend tlc
//   quint run    docs/spec/segments.qnt --main segments_bad_span  --invariant safety   # counterexamples
//   quint run    docs/spec/segments.qnt --main segments_bad_scope --invariant safety
//   quint run    docs/spec/segments.qnt --main segments_bad_nonce --invariant safety
//
// Rules the implementation must share with this model:
//   R1  writers only append: a drain publishes one segment and one head naming
//       (root, base epoch, segment). Nothing but GC merges or replaces.
//   R2  object keys carry a writer nonce, so no two uploads share a key
//       (segments_bad_nonce: content keys + a crashed reservation = a hole
//       later writers dedup into).
//   R3  GC lists, reads and deletes only its own scope (segments_bad_scope).
//   R4  GC keeps reachable(new) and reachable(view it loaded) and deletes the
//       rest of its listing older than a drain; g-(N-1) survives one epoch.
//   R5  root reset: a root's next base is the union of segments named by
//       heads since the last base, or the old base if there are none.
// Assumptions:
//   A1  a drain publishes under a view at most MAX_JOB_SPAN epochs old: it
//       re-lists heads when the drain starts, not when the job did. What
//       remains assumed is that two GC runs do not fit inside one drain
//       (segments_bad_span).
//   A2  GC load sees every published head of its scope.

module segments {
  const WRITERS: Set[str]
  const MAX_JOB_SPAN: int
  const MAX_ID: int
  const MAX_EVICT: int
  const NONCE: bool
  const LIST_OWN_SCOPE: bool

  // ---------------------------------------------------------------- types

  type Seg   = { paths: Set[int], packs: Set[int] }
  type GHead = { epoch: int, roots: str -> int, folded: Set[int] }
  type WHead = { root: str, base: int, seg: int, visible: bool }
  type Store = {
    packs: Set[int], segs: int -> Seg, gheads: int -> GHead, wheads: int -> WHead,
    // access scope of every key ever created
    scope: int -> str,
    // keys reserved by a crashed writer: answer "exists" but hold nothing
    holes: int -> Seg,
  }
  type View  = { epoch: int, ghead: int, wheads: Set[int], segs: Set[int], packs: Set[int], paths: Set[int] }

  /// idle -> loaded -> staged -> idle
  type Writer = { phase: str, view: View, path: int, pack: int, seg: int }
  /// idle -> loaded -> published -> sweptObjs -> idle. May crash to idle after publish.
  type Gc     = { phase: str, view: View, listed: Set[int], young: Set[int], head: int, segs: Set[int], newpack: int, crashed: bool }

  type State = {
    store: Store, writers: str -> Writer, gc: Gc, next_id: int,
    // observation only
    committed: Set[(int, int, int)],   // (path, pack, epoch whose next GC must carry it)
    orphans: Set[int], evicted: Set[int], foreignDeleted: bool,
  }

  var s: State

  // ---------------------------------------------------------------- helpers

  pure def scopeOf(w: str): str = if (w == "a") "main" else "pr"
  pure def rootOf(w: str): str = scopeOf(w)
  pure def reads(scope: str): Set[str] = if (scope == "main") Set("main") else Set("main", "pr")

  pure def without(m: int -> a, ks: Set[int]): int -> a = m.keys().exclude(ks).mapBy(k => m.get(k))
  pure def maxId(ids: Set[int]): int = ids.fold(0, (a, b) => if (b > a) b else a)
  pure def vals(m: str -> int): Set[int] = m.keys().map(k => m.get(k))

  pure def inScope(st: Store, scopes: Set[str], k: int): bool =
    st.scope.keys().contains(k) and scopes.contains(st.scope.get(k))
  pure def segsIn(st: Store, scopes: Set[str], xs: Set[int]): Set[int] =
    xs.filter(x => st.segs.keys().contains(x) and inScope(st, scopes, x))
  pure def packsOf(st: Store, segs: Set[int]): Set[int] =
    segs.filter(x => st.segs.keys().contains(x)).map(x => st.segs.get(x).packs).flatten()
  pure def pathsOf(st: Store, segs: Set[int]): Set[int] =
    segs.filter(x => st.segs.keys().contains(x)).map(x => st.segs.get(x).paths).flatten()

  /// Newest GC head as base, plus writer heads readable in `scopes`, based
  /// on this epoch or the one before, not yet folded. R1: base ∪ added.
  pure def viewOf(st: Store, scopes: Set[str], onlyVisible: bool): View = {
    val gh    = maxId(st.gheads.keys())
    val epoch = if (gh == 0) 0 else st.gheads.get(gh).epoch
    val base  = if (gh == 0) Set() else vals(st.gheads.get(gh).roots)
    val done  = if (gh == 0) Set() else st.gheads.get(gh).folded
    val ws    = st.wheads.keys().filter(h => {
                  val wh = st.wheads.get(h)
                  (wh.visible or not(onlyVisible)) and inScope(st, scopes, h)
                  and wh.base >= epoch - 1 and not(done.contains(h)) })
    val segs  = segsIn(st, scopes, base.union(ws.map(h => st.wheads.get(h).seg)))
    { epoch: epoch, ghead: gh, wheads: ws, segs: segs, packs: packsOf(st, segs), paths: pathsOf(st, segs) }
  }

  val EmptyView: View = { epoch: 0, ghead: 0, wheads: Set(), segs: Set(), packs: Set(), paths: Set() }
  val IdleW: Writer = { phase: "idle", view: EmptyView, path: 0, pack: 0, seg: 0 }
  val IdleGc: Gc    = { phase: "idle", view: EmptyView, listed: Set(), young: Set(), head: 0, segs: Set(), newpack: 0, crashed: false }
  val ALL = Set("main", "pr")

  val store = s.store
  val gv    = viewOf(s.store, ALL, false)
  val mv    = viewOf(s.store, Set("main"), false)
  def room(n: int): bool = s.next_id + n <= MAX_ID + 1
  def allVisible: bool   = store.wheads.keys().forall(h => store.wheads.get(h).visible)
  def curEpoch: int      = gv.epoch

  /// A1
  def inTime(base: int): bool =
    curEpoch - base < MAX_JOB_SPAN or (curEpoch - base == MAX_JOB_SPAN and s.gc.phase != "loaded")

  /// Objects a non-idle writer uploaded but has not published yet are
  /// younger than GC's min_age.
  val youngObjs: Set[int] = WRITERS.filter(w => s.writers.get(w).phase == "staged")
                                   .map(w => Set(s.writers.get(w).pack, s.writers.get(w).seg)).flatten()

  def withScope(st: Store, ks: Set[int], sc: str): int -> str =
    ks.fold(st.scope, (m, k) => m.put(k, sc))

  // ------------------------------------------------------------- actions

  action init = s' = {
    store:   { packs: Set(), segs: Map(), gheads: Map(), wheads: Map(), scope: Map(), holes: Map() },
    writers: WRITERS.mapBy(_ => IdleW),
    gc:      IdleGc,
    next_id: 1, committed: Set(), orphans: Set(), evicted: Set(), foreignDeleted: false,
  }

  // -- drain --

  action wLoad(w: str): bool = {
    val st = s.writers.get(w)
    all {
      st.phase == "idle", room(1),
      s' = { ...s, next_id: s.next_id + 1,
             writers: s.writers.set(w, { ...st, phase: "loaded", view: viewOf(store, reads(scopeOf(w)), true), path: s.next_id }) },
    }
  }

  /// Upload pack + segment for a fresh path (dedup may reference one pack of
  /// the view), or re-claim the view without building anything (path 0).
  /// Without NONCE the segment key is its content: an equal stored segment
  /// or an equal hole is "already there" and nothing is uploaded.
  action wStage(w: str): bool = {
    val st = s.writers.get(w)
    all {
      st.phase == "loaded", room(2),
      nondet fresh = Set(true, false).oneOf()
      nondet r = st.view.packs.union(Set(0)).oneOf()
      val reuse = if (r == 0) Set() else Set(r)
      val pack  = if (fresh) s.next_id else 0
      val body  = if (fresh) { paths: Set(st.path), packs: reuse.union(Set(pack)) }
                  else       { paths: st.view.paths, packs: st.view.packs }
      val sameSeg  = store.segs.keys().filter(k => not(NONCE) and store.segs.get(k) == body)
      val sameHole = store.holes.keys().filter(k => not(NONCE) and store.holes.get(k) == body)
      val seg   = if (sameHole != Set()) maxId(sameHole) else if (sameSeg != Set()) maxId(sameSeg) else s.next_id + 1
      val isNew = sameHole == Set() and sameSeg == Set()
      val newKeys = (if (fresh) Set(pack) else Set()).union(if (isNew) Set(seg) else Set())
      all {
        fresh or st.view.paths != Set(),
        s' = { ...s, next_id: s.next_id + 2,
               store: { ...store,
                 packs: if (fresh) store.packs.union(Set(pack)) else store.packs,
                 segs:  if (isNew) store.segs.put(seg, body) else store.segs,
                 scope: withScope(store, newKeys, scopeOf(w)) },
               writers: s.writers.set(w, { ...st, phase: "staged", path: if (fresh) st.path else 0, pack: pack, seg: seg }) },
      }
    }
  }

  /// R2 counterexample only: the writer reserved its segment key and died
  /// before finalizing. The key answers "exists" from now on.
  action wCrashReserve(w: str): bool = {
    val st = s.writers.get(w)
    val body = { paths: st.view.paths, packs: st.view.packs }
    all {
      not(NONCE), st.phase == "loaded", st.view.paths != Set(), room(2),
      s' = { ...s, next_id: s.next_id + 2,
             store: { ...store, holes: store.holes.put(s.next_id + 1, body),
                      scope: withScope(store, Set(s.next_id + 1), scopeOf(w)) },
             writers: s.writers.set(w, IdleW) },
    }
  }

  action wCommit(w: str): bool = {
    val st = s.writers.get(w)
    val h  = { root: rootOf(w), base: st.view.epoch, seg: st.seg, visible: false }
    // The GC run that must carry this claim: the next one to load.
    val due = if (s.gc.phase == "loaded") curEpoch + 1 else curEpoch
    all {
      st.phase == "staged", room(1), inTime(st.view.epoch),
      s' = { ...s, next_id: s.next_id + 1,
             store: { ...store, wheads: store.wheads.put(s.next_id, h),
                      scope: withScope(store, Set(s.next_id), scopeOf(w)) },
             writers: s.writers.set(w, IdleW),
             committed: if (st.path != 0 and rootOf(w) == "main") s.committed.union(Set((st.path, st.pack, due))) else s.committed },
    }
  }

  /// View too old to publish under: reload and redo dedup/staging.
  action wRefresh(w: str): bool = {
    val st = s.writers.get(w)
    all {
      st.phase != "idle", not(inTime(st.view.epoch)),
      s' = { ...s, writers: s.writers.set(w, { ...st, phase: "loaded", view: viewOf(store, reads(scopeOf(w)), true), pack: 0, seg: 0 }),
             orphans: s.orphans.union(if (st.phase == "staged") Set(st.pack, st.seg).exclude(Set(0)) else Set()) },
    }
  }

  action becomeVisible = {
    val hidden = store.wheads.keys().filter(h => not(store.wheads.get(h).visible))
    all {
      hidden != Set(),
      nondet h = hidden.oneOf()
      s' = { ...s, store: { ...store, wheads: store.wheads.set(h, { ...store.wheads.get(h), visible: true }) } },
    }
  }

  // -- gc (scope main) --

  val gcScopes: Set[str] = if (LIST_OWN_SCOPE) Set("main") else ALL

  action gLoad = all {
    s.gc.phase == "idle", allVisible,   // A2
    s' = { ...s, gc: { ...s.gc, phase: "loaded", view: mv,
                       listed: store.wheads.keys().union(store.segs.keys()).union(store.packs).union(store.gheads.keys())
                                 .filter(k => inScope(store, gcScopes, k)),
                       young: youngObjs } },
  }

  /// R5: per root, the heads since the base decide; the base stands only
  /// when no head names the root. Optionally repack one live pack. Fold
  /// every listed head (also stale ones and older g-*).
  action gPublish = {
    val v     = s.gc.view
    val prevRoots: str -> int = if (store.gheads.keys().contains(v.ghead)) store.gheads.get(v.ghead).roots else Map()
    val whs   = v.wheads.filter(h => store.wheads.keys().contains(h))
    val roots = prevRoots.keys().union(whs.map(h => store.wheads.get(h).root))
    val inputsOf = roots.mapBy(r => {
      val hs = whs.filter(h => store.wheads.get(h).root == r)
      if (hs != Set()) segsIn(store, Set("main"), hs.map(h => store.wheads.get(h).seg))
      else segsIn(store, Set("main"), Set(prevRoots.get(r))) })
    val live  = roots.filter(r => inputsOf.get(r) != Set())
    val n     = live.size()
    all {
      s.gc.phase == "loaded", room(n + 2),
      nondet r = v.packs.union(Set(0)).oneOf()
      val hid     = s.next_id
      val newpack = if (r == 0) 0 else s.next_id + 1
      // deterministic seg id per root: sorted position after hid+1
      val order   = live.fold(([], hid + 2), (acc, root) => (acc._1.append((root, acc._2)), acc._2 + 1))._1
      val segId   = order.foldl(Map(), (m, p) => m.put(p._1, p._2))
      val bodyOf  = live.mapBy(root => {
        val ps = pathsOf(store, inputsOf.get(root))
        val ks = packsOf(store, inputsOf.get(root))
        { paths: ps, packs: if (r != 0 and ks.contains(r)) ks.exclude(Set(r)).union(Set(newpack)) else ks } })
      val newSegs = live.map(root => segId.get(root))
      val gh = { epoch: v.epoch + 1, roots: live.mapBy(root => segId.get(root)),
                 folded: s.gc.listed.filter(k => store.wheads.keys().contains(k) or store.gheads.keys().contains(k)) }
      s' = { ...s, next_id: s.next_id + n + 2,
             store: { ...store,
               packs:  if (r == 0) store.packs else store.packs.union(Set(newpack)),
               segs:   live.fold(store.segs, (m, root) => m.put(segId.get(root), bodyOf.get(root))),
               gheads: store.gheads.put(hid, gh),
               scope:  withScope(store, newSegs.union(Set(hid)).union(if (r == 0) Set() else Set(newpack)), "main") },
             gc: { ...s.gc, phase: "published", head: hid, segs: newSegs, newpack: newpack } },
    }
  }

  val gcMine: GHead = store.gheads.get(s.gc.head)

  /// R4: keep = reachable(new record) ∪ reachable(the view this run
  /// loaded: its g-*, its writer heads, their segments and packs);
  /// everything else GC listed and that is older than a drain goes.
  action gSweepObjs = {
    val v         = s.gc.view
    val keepSegs  = vals(gcMine.roots).union(v.segs)
    val keepPacks = packsOf(store, keepSegs).union(if (s.gc.newpack == 0) Set() else Set(s.gc.newpack))
    val keepHeads = v.wheads.union(Set(v.ghead, s.gc.head))
    val cand      = s.gc.listed.exclude(s.gc.young)
    val deadSegs  = cand.filter(k => store.segs.keys().contains(k)).exclude(keepSegs)
    val deadPacks = cand.filter(k => store.packs.contains(k)).exclude(keepPacks)
    val deadHeads = cand.filter(k => store.wheads.keys().contains(k) or store.gheads.keys().contains(k)).exclude(keepHeads)
    val dead      = deadSegs.union(deadPacks).union(deadHeads)
    all {
      s.gc.phase == "published", store.gheads.keys().contains(s.gc.head),
      s' = { ...s,
        store: { ...store, packs: store.packs.exclude(deadPacks), segs: without(store.segs, deadSegs),
                 wheads: without(store.wheads, deadHeads), gheads: without(store.gheads, deadHeads) },
        foreignDeleted: s.foreignDeleted or dead.exists(k => inScope(store, Set("pr"), k)),
        gc: IdleGc },
    }
  }

  action gCrash = all {
    s.gc.phase == "published",
    s' = { ...s, gc: { ...IdleGc, crashed: true } },
  }

  // -- environment (gha) --

  action evict = {
    val objs = store.packs.union(store.segs.keys()).union(store.gheads.keys()).union(store.wheads.keys())
    all {
      s.evicted.size() < MAX_EVICT, objs != Set(),
      nondet o = objs.oneOf()
      s' = { ...s, evicted: s.evicted.union(Set(o)),
             store: { ...store, packs: store.packs.exclude(Set(o)), segs: without(store.segs, Set(o)),
                      gheads: without(store.gheads, Set(o)), wheads: without(store.wheads, Set(o)) } },
    }
  }

  action step = any {
    nondet w = WRITERS.oneOf()
    any { wLoad(w), wStage(w), wCommit(w), wRefresh(w), wCrashReserve(w) },
    becomeVisible,
    gLoad, gPublish, gSweepObjs, gCrash,
    evict,
  }

  // ---------------------------------------------------------- properties

  val pristine = s.evicted == Set()
  def present(x: int): bool = store.segs.keys().contains(x) or store.packs.contains(x) or s.evicted.contains(x)

  /// Whatever any reader's view names exists. After an eviction GC may
  /// also sweep what only the evicted object referenced; both surface as a
  /// miss and a rebuild, so only checked pristine.
  val integrity = not(pristine) or gv.segs.forall(x => present(x)) and gv.packs.forall(p => present(p))
    and gv.wheads.forall(h => present(store.wheads.get(h).seg))

  /// R5: a fresh path published on main is served until the GC after the
  /// one that folded it (then it lives on only if re-claimed).
  val noLostWrite = not(pristine) or
    s.committed.forall(c => curEpoch > c._3 + 1 or (curEpoch < c._3 and not(allVisible)) or mv.paths.contains(c._1))

  /// An in-time drain never holds a reference to something GC deleted.
  val inflightRefsExist = not(pristine) or WRITERS.forall(w => {
    val st = s.writers.get(w)
    st.phase == "idle" or not(inTime(st.view.epoch)) or (
      st.view.segs.forall(x => present(x)) and st.view.packs.forall(p => present(p))
      and (st.phase != "staged" or ((st.pack == 0 or present(st.pack)) and present(st.seg))))
  })

  /// R3
  val foreignSafe = not(s.foreignDeleted)

  val core = all { integrity, noLostWrite, inflightRefsExist, foreignSafe }

  val safety = core

  val witnessRepackSwept = not(integrity and pristine and s.committed.exists(c => not(store.packs.contains(c._2))))
  val witnessReclaim     = not(store.wheads.keys().exists(h => store.gheads.keys().exists(g => vals(store.gheads.get(g).roots).contains(store.wheads.get(h).seg))))
  val witnessPrServed    = not(viewOf(store, ALL, true).paths != mv.paths and curEpoch > 0)
}

/// gha: one arbitrary eviction, lagged listings, scoped GC, nonce'd keys.
module segments_main {
  import segments(WRITERS = Set("a", "b"), MAX_JOB_SPAN = 1, MAX_ID = 12, MAX_EVICT = 1, NONCE = true, LIST_OWN_SCOPE = true).*
}

/// S3 / OCI: no eviction, no scopes (one writer kind).
module segments_s3 {
  import segments(WRITERS = Set("a"), MAX_JOB_SPAN = 1, MAX_ID = 13, MAX_EVICT = 0, NONCE = true, LIST_OWN_SCOPE = true).*
}

/// A1 violated: a drain publishes under a view two epochs old.
module segments_bad_span {
  import segments(WRITERS = Set("a"), MAX_JOB_SPAN = 2, MAX_ID = 13, MAX_EVICT = 0, NONCE = true, LIST_OWN_SCOPE = true).*
}

/// R3 violated: GC lists every scope but can read only its own (gha REST
/// without ref=): PR objects are swept as unreferenced.
module segments_bad_scope {
  import segments(WRITERS = Set("a", "b"), MAX_JOB_SPAN = 1, MAX_ID = 12, MAX_EVICT = 0, NONCE = true, LIST_OWN_SCOPE = false).*
}

/// R2 violated: content-addressed keys. A crashed reservation leaves a hole
/// that the next writer of the same content dedups into and publishes.
module segments_bad_nonce {
  import segments(WRITERS = Set("a"), MAX_JOB_SPAN = 1, MAX_ID = 12, MAX_EVICT = 0, NONCE = false, LIST_OWN_SCOPE = true).*
}
