// -*- mode: Bluespec; -*-
//
// hestia trust model: what an authorised but malicious writer can do.
// Companion to segments.qnt (which has no identities and no adversary, but
// lag, crash and eviction). Here GC is atomic and advances an epoch. What is
// shared with segments.qnt is the view rule: GC output as base plus the
// segments of accepted heads of the same root based on this epoch or the
// one before. Writers only append; nothing but GC replaces (R1).
//
//   quint run    docs/spec/trust.qnt --main trust_main --invariant safety
//   quint verify docs/spec/trust.qnt --main trust_main --invariant safety --max-steps 12 --backend tlc
//   quint run    docs/spec/trust.qnt --main trust_open        --invariant mainClean   # counterexamples
//   quint run    docs/spec/trust.qnt --main trust_same_repo   --invariant mainClean
//   quint run    docs/spec/trust.qnt --main trust_bad_aud      --invariant safety
//
// Proofs are abstract: { repo, ref, digest } stands for an OIDC token or a key signature alike.
// `digest` is the hash of the whole signed record (root, base epoch, segment), so a proof
// replayed on another record, root or epoch does not bind (BIND_AUD).
// Actors: `honest` builds on refs/heads/main and writes root "main" (protected).
//         `contrib` builds a PR and writes root "pr".
//         `mal` holds packages:write from refs/heads/feature (root "feat"). It may put any
//         well-typed object, claim any root, attach no proof, its own proof, or
//         replay a proof it has seen. It cannot forge a signature or collide ids.
//         `gc` runs from the default branch.

module trust {
  /// "strict": a root accepts heads proven for its own ref. "same_repo": any proven ref of this
  /// repository. "open": anything, proof optional.
  const POLICY: str
  /// the adversary pushes from a branch of this repo (true) or holds a token of another repo (false)
  const MAL_SAME_REPO: bool
  const BIND_AUD: bool           // a proof is only valid on the record it was issued for
  const MAX_ID: int

  type Entry = { path: int, nar: str }                 // nar: "good" | "bad"
  type Seg   = { entries: Set[Entry] }
  type Proof = { some: bool, repo: str, ref: str, digest: int }
  /// The signed record. `gc` heads carry every root's base.
  type Head  = { root: str, base: int, seg: int, proof: Proof, gc: bool, roots: str -> int }

  type State = {
    segs: int -> Seg,
    heads: int -> Head,
    epoch: int,
    next_id: int,
    // ground truth for the invariants
    committed: Set[(str, int, int)],   // (root, path, epoch) published by honest/contrib
  }
  var s: State

  val PATHS = Set(1)
  val ROOTS = Set("main", "pr", "feat")
  val HONEST_ROOTS = Set("main", "pr")
  pure def refOf(root: str): str =
    if (root == "main") "refs/heads/main" else if (root == "pr") "refs/pull/7/merge" else "refs/heads/feature"
  val GC_REF = "gc-workflow@refs/heads/main"
  val NoProof: Proof = { some: false, repo: "", ref: "", digest: 0 }
  val REPO = "owner/repo"

  def room(n: int): bool = s.next_id + n <= MAX_ID + 1

  // ---------------------------------------------------------------- policy

  def accepted(hid: int): bool = {
    val h = s.heads.get(hid)
    val pr = h.proof
    val bound = pr.some and pr.repo == REPO and (pr.digest == hid or not(BIND_AUD))
    if (POLICY == "open") true
    else if (POLICY == "same_repo") bound
    else bound and (pr.ref == GC_REF or (not(h.gc) and pr.ref == refOf(h.root)))
  }

  pure def maxId(ids: Set[int]): int = ids.fold(0, (a, b) => if (b > a) b else a)

  /// Newest accepted GC head.
  val gcHead: int = maxId(s.heads.keys().filter(k => s.heads.get(k).gc and accepted(k)))
  val gcEpoch: int = if (gcHead == 0) 0 else s.heads.get(gcHead).base

  /// Segments a reader of `root` uses: GC's base for the root plus accepted
  /// writer heads of that root based on this epoch or the one before.
  def view(root: str): Set[int] = {
    val base = if (gcHead != 0 and s.heads.get(gcHead).roots.keys().contains(root)) Set(s.heads.get(gcHead).roots.get(root)) else Set()
    val hs = s.heads.keys().filter(k => { val h = s.heads.get(k)
               not(h.gc) and accepted(k) and h.root == root and h.base >= gcEpoch - 1 and h.base <= gcEpoch })
    base.union(hs.map(k => s.heads.get(k).seg)).filter(x => s.segs.keys().contains(x))
  }
  def entriesOf(root: str): Set[Entry] = view(root).map(x => s.segs.get(x).entries).flatten()

  // --------------------------------------------------------------- actions

  action init = s' = { segs: Map(), heads: Map(), epoch: 0, next_id: 1, committed: Set() }

  def publish(st: State, root: str, base: int, entries: Set[Entry], proof: Proof): State = {
    val sid = st.next_id
    val hid = st.next_id + 1
    val pr  = if (proof.some and proof.digest == 0) { ...proof, digest: hid } else proof   // digest 0 = "sign this record"
    { ...st, next_id: st.next_id + 2,
      segs: st.segs.put(sid, { entries: entries }),
      heads: st.heads.put(hid, { root: root, base: base, seg: sid, proof: pr, gc: false, roots: Map() }) }
  }

  /// Honest drain on its own root, based on the epoch it read, with a proof for its own ref.
  action honestWrite(root: str): bool = all {
    room(2),
    nondet p = PATHS.oneOf()
    val st = publish(s, root, gcEpoch, Set({ path: p, nar: "good" }), { some: true, repo: REPO, ref: refOf(root), digest: 0 })
    s' = { ...st, committed: s.committed.union(Set((root, p, gcEpoch))) },
  }

  /// The adversary: any root, any base epoch, bad entries, any obtainable proof.
  /// A replayed proof keeps the digest of the record it was issued for.
  action malWrite = all {
    room(2),
    nondet root = ROOTS.oneOf()
    nondet p = PATHS.oneOf()
    nondet base = 0.to(gcEpoch + 1).oneOf()
    nondet proofKind = Set("none", "own", "replay").oneOf()
    val seen = s.heads.keys().map(k => s.heads.get(k).proof).filter(pr => pr.some)
    nondet replayed = seen.union(Set(NoProof)).oneOf()
    val proof = if (proofKind == "none") NoProof
                else if (proofKind == "own") { some: true, repo: if (MAL_SAME_REPO) REPO else "fork/repo", ref: "refs/heads/feature", digest: 0 }
                else replayed
    s' = publish(s, root, base, Set({ path: p, nar: "bad" }), proof),
  }

  /// The adversary forges a g-head that makes a bad segment every root's base.
  action malGc = all {
    room(2),
    nondet proofKind = Set("none", "own", "replay").oneOf()
    val seen = s.heads.keys().map(k => s.heads.get(k).proof).filter(pr => pr.some)
    nondet replayed = seen.union(Set(NoProof)).oneOf()
    val hid = s.next_id + 1
    val proof = if (proofKind == "none") NoProof
                else if (proofKind == "own") { some: true, repo: if (MAL_SAME_REPO) REPO else "fork/repo", ref: "refs/heads/feature", digest: hid }
                else replayed
    s' = { ...s, next_id: s.next_id + 2,
           segs: s.segs.put(s.next_id, { entries: Set({ path: 1, nar: "bad" }) }),
           heads: s.heads.put(hid, { root: "", base: gcEpoch + 1, seg: 0, proof: proof, gc: true, roots: ROOTS.mapBy(_ => s.next_id) }) },
  }

  /// The adversary re-publishes an honest head's exact record under its own
  /// id (another name for the same body, e.g. a different epoch in the key).
  /// With BIND_AUD the proof digest no longer matches.
  action malReplayHead = all {
    room(1), s.heads.keys() != Set(),
    nondet k = s.heads.keys().oneOf()
    nondet base = 0.to(gcEpoch + 1).oneOf()
    val h = s.heads.get(k)
    s' = { ...s, next_id: s.next_id + 1, heads: s.heads.put(s.next_id, { ...h, base: base, gc: false, roots: Map() }) },
  }

  /// GC: per root, fold the policy-filtered view into one segment, publish
  /// one signed g-head naming every root's base, delete everything else.
  val liveRoots = ROOTS.filter(r => view(r) != Set())
  action gc = all {
    room(liveRoots.size() + 1), liveRoots != Set(),
    val order = liveRoots.fold(([], s.next_id), (acc, r) => (acc._1.append((r, acc._2)), acc._2 + 1))._1
    val segId = order.foldl(Map(), (m, p) => m.put(p._1, p._2))
    val segs  = liveRoots.fold(Map(), (m, r) => m.put(segId.get(r), { entries: entriesOf(r) }))
    val hid   = s.next_id + liveRoots.size()
    val gh    = { root: "", base: gcEpoch + 1, seg: 0, proof: { some: true, repo: REPO, ref: GC_REF, digest: hid }, gc: true,
                  roots: liveRoots.mapBy(r => segId.get(r)) }
    s' = { ...s, next_id: hid + 1, epoch: gcEpoch + 1, segs: segs, heads: Map(hid -> gh) },
  }

  action step = any { honestWrite("main"), honestWrite("pr"), malWrite, malGc, malReplayHead, gc }

  // ------------------------------------------------------------ properties

  /// A root only ever serves what its own ref (or GC) published.
  val mainClean = HONEST_ROOTS.forall(r => entriesOf(r).forall(e => e.nar == "good"))

  /// The adversary cannot make honest paths disappear from someone else's
  /// root (within the epochs the root-reset rule keeps them).
  val noHide = s.committed.forall(c => gcEpoch > c._3 + 1 or entriesOf(c._1).exists(e => e.path == c._2))

  /// Nobody but GC moves the epoch.
  val gcOnlyClock = s.heads.keys().forall(k => not(s.heads.get(k).gc and accepted(k)) or s.heads.get(k).proof.ref == GC_REF or POLICY == "open")

  val safety = all { mainClean, noHide, gcOnlyClock }

  val witnessMalOnFeat = not(entriesOf("feat").exists(e => e.nar == "bad"))
  val witnessEpoch2    = not(gcEpoch >= 2 and entriesOf("main") != Set())
}

/// strict: a same-repo branch pusher can touch nothing but its own root.
module trust_main          { import trust(POLICY = "strict",    MAL_SAME_REPO = true,  BIND_AUD = true,  MAX_ID = 9).* }
/// same_repo: a fork / foreign token can touch nothing. (A same-repo pusher can: see trust_same_repo.)
module trust_same_repo_fork{ import trust(POLICY = "same_repo", MAL_SAME_REPO = false, BIND_AUD = true,  MAX_ID = 9).* }
/// expected violations, documenting what each setting gives up
module trust_same_repo     { import trust(POLICY = "same_repo", MAL_SAME_REPO = true,  BIND_AUD = true,  MAX_ID = 9).* }
module trust_open          { import trust(POLICY = "open",      MAL_SAME_REPO = false, BIND_AUD = true,  MAX_ID = 9).* }
/// proof not bound to the record: replaying an honest main proof on a bad
/// segment, or GC's proof on a forged g-head, or an honest record under
/// another epoch all verify.
module trust_bad_aud       { import trust(POLICY = "strict",    MAL_SAME_REPO = true,  BIND_AUD = false, MAX_ID = 9).* }
