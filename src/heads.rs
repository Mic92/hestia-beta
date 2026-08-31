//! Head names, head records, and the view rule.
//!
//! ```text
//! g-<epoch>-<time>-<d>              GC head,    body = Signed(GcRecord),   <d> = sha256(body)
//! h-<base_epoch>-<root>-<time>-<d>  drain head, body = Signed(HeadRecord), <d> = sha256(body)
//! ```
//!
//! All fields are fixed-width lowercase hex so listings sort by epoch and
//! names fit OCI tag and cache-key alphabets. `<time>` is the writer's
//! unix clock, so a head's age needs no backend metadata. `<root>` is a
//! 64-bit hash of the root name; the name itself is in the body. The name
//! is a function of the body, so a body (and the proof over it) is valid
//! under exactly one name: root, base epoch and time are all signed.
//!
//! Writers only append: a drain publishes one segment and one `h-*` naming
//! it. Nothing but GC merges or replaces segments, so a root's view is
//! GC's base plus the segments of its drain heads since (`docs/spec/segments.qnt` R1).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use minicbor::{Decode, Encode};

use crate::manifest::{SegDigest, SegKey};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("head record cbor: {0}")]
    Cbor(#[from] minicbor::decode::Error),
    #[error("head record: {0}")]
    Format(String),
}

// ---------------------------------------------------------------- names

pub type RootId = u64;

pub fn root_id(name: &str) -> RootId {
    u64::from_le_bytes(
        blake3::hash(name.as_bytes()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadName {
    Gc {
        epoch: u64,
        time: u64,
        digest: SegDigest,
    },
    Drain {
        base_epoch: u64,
        root: RootId,
        time: u64,
        digest: SegDigest,
    },
}

fn hex_u64(s: &str) -> Option<u64> {
    let canonical = s.len() == 16 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    canonical.then(|| u64::from_str_radix(s, 16).ok()).flatten()
}

impl HeadName {
    pub fn parse(s: &str) -> Option<HeadName> {
        let parts: Vec<&str> = s.split('-').collect();
        Some(match parts.as_slice() {
            ["g", e, t, d] => HeadName::Gc {
                epoch: hex_u64(e)?,
                time: hex_u64(t)?,
                digest: SegDigest::from_hex(d)?,
            },
            ["h", e, r, t, d] => HeadName::Drain {
                base_epoch: hex_u64(e)?,
                root: hex_u64(r)?,
                time: hex_u64(t)?,
                digest: SegDigest::from_hex(d)?,
            },
            _ => return None,
        })
    }

    pub fn time(&self) -> u64 {
        match self {
            HeadName::Gc { time, .. } | HeadName::Drain { time, .. } => *time,
        }
    }
}

impl fmt::Display for HeadName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HeadName::Gc {
                epoch,
                time,
                digest,
            } => write!(f, "g-{epoch:016x}-{time:016x}-{digest}"),
            HeadName::Drain {
                base_epoch,
                root,
                time,
                digest,
            } => write!(f, "h-{base_epoch:016x}-{root:016x}-{time:016x}-{digest}"),
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RootRow {
    #[n(0)]
    pub name: String,
    #[n(1)]
    pub seg: SegKey,
    /// Unix time GC last folded a writer head for this root.
    #[n(2)]
    pub stamp: u64,
    /// Claims GC could not fold into `seg` this run (a tree it could not
    /// fetch, a segment it could not parse). They stay part of the base so
    /// nothing is lost and the next run retries.
    #[n(3)]
    pub unmerged: Vec<SegKey>,
}

impl RootRow {
    pub fn segments(&self) -> impl Iterator<Item = SegKey> + '_ {
        std::iter::once(self.seg).chain(self.unmerged.iter().copied())
    }
}

/// Body of `g-*`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
pub struct GcRecord {
    #[n(0)]
    pub epoch: u64,
    /// One compacted segment per root, sorted by name.
    #[n(1)]
    pub roots: Vec<RootRow>,
    /// Heads this run consumed; readers skip them even while the listing
    /// still shows them.
    #[n(2)]
    pub folded: Vec<String>,
    /// Unix time written.
    #[n(3)]
    pub time: u64,
}

/// Body of `h-*`: one drain's claim on one root.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct HeadRecord {
    #[n(0)]
    pub root: String,
    /// Epoch of the `g-*` the writer's view was built on.
    #[n(1)]
    pub base_epoch: u64,
    #[n(2)]
    pub seg: SegKey,
    #[n(3)]
    pub time: u64,
}

/// Body of every head: the record and a proof over exactly its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Signed {
    #[cbor(n(0), with = "minicbor::bytes")]
    pub record: Vec<u8>,
    #[cbor(n(1), with = "minicbor::bytes")]
    pub proof: Vec<u8>,
}

impl Signed {
    pub fn unsigned(record: Vec<u8>) -> Vec<u8> {
        Signed {
            record,
            proof: vec![],
        }
        .encode()
    }
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }
    pub fn decode(bytes: &[u8]) -> Result<Signed, Error> {
        decode_record(bytes)
    }
}

const MAX_RECORD_BYTES: usize = 64 << 20;

fn decode_record<'a, T: Decode<'a, ()>>(bytes: &'a [u8]) -> Result<T, Error> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::Format("record exceeds cap".into()));
    }
    Ok(minicbor::decode(bytes)?)
}

impl GcRecord {
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }
    pub fn decode(bytes: &[u8]) -> Result<GcRecord, Error> {
        let r: GcRecord = decode_record(bytes)?;
        if r.roots.windows(2).any(|w| w[0].name >= w[1].name) {
            return Err(Error::Format("roots not sorted/unique".into()));
        }
        Ok(r)
    }
    pub fn head_name(&self, body: &[u8]) -> HeadName {
        HeadName::Gc {
            epoch: self.epoch,
            time: self.time,
            digest: SegDigest::digest(body),
        }
    }
}

impl HeadRecord {
    pub fn encode(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("Vec write")
    }
    pub fn decode(bytes: &[u8]) -> Result<HeadRecord, Error> {
        decode_record(bytes)
    }
    pub fn head_name(&self, body: &[u8]) -> HeadName {
        HeadName::Drain {
            base_epoch: self.base_epoch,
            root: root_id(&self.root),
            time: self.time,
            digest: SegDigest::digest(body),
        }
    }
}

// ---------------------------------------------------------------- view

/// `g-*` names, highest epoch first.
pub fn newest_gc<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut gcs: Vec<(u64, &str)> = names
        .into_iter()
        .filter_map(|n| match HeadName::parse(n)? {
            HeadName::Gc { epoch, .. } => Some((epoch, n)),
            HeadName::Drain { .. } => None,
        })
        .collect();
    gcs.sort_by(|a, b| b.cmp(a));
    gcs.into_iter().map(|(_, n)| n).collect()
}

/// Drain heads GC has not folded yet: based on this epoch or the one
/// before (published while GC ran), and not named in `folded`. These are
/// the heads whose bodies a reader fetches.
pub fn pending_heads<'a>(
    names: impl IntoIterator<Item = &'a str>,
    gc: Option<&GcRecord>,
) -> Vec<&'a str> {
    let epoch = gc.map_or(0, |g| g.epoch);
    let folded: BTreeSet<&str> = gc
        .into_iter()
        .flat_map(|g| g.folded.iter().map(String::as_str))
        .collect();
    let mut out: Vec<&str> = names
        .into_iter()
        .filter(|n| !folded.contains(n))
        .filter(|n| {
            matches!(HeadName::parse(n), Some(HeadName::Drain { base_epoch, .. })
                if base_epoch <= epoch && base_epoch + 1 >= epoch)
        })
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    pub epoch: u64,
    /// Live segments per root: GC's base first, then heads in name order.
    pub roots: BTreeMap<String, Vec<SegKey>>,
    /// Drain heads that contributed, with their record.
    pub heads: Vec<(String, HeadRecord)>,
}

impl View {
    /// `records` holds the fetched, verified bodies of [`pending_heads`]. A
    /// name missing there (fetch failed, proof rejected) counts as not listed.
    pub fn compute(gc: Option<&GcRecord>, records: &HashMap<String, HeadRecord>) -> View {
        let mut roots: BTreeMap<String, Vec<SegKey>> = gc
            .into_iter()
            .flat_map(|g| {
                g.roots
                    .iter()
                    .map(|r| (r.name.clone(), r.segments().collect()))
            })
            .collect();
        let mut heads: Vec<(String, HeadRecord)> = records
            .iter()
            .map(|(n, r)| (n.clone(), r.clone()))
            .collect();
        heads.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, r) in &heads {
            roots.entry(r.root.clone()).or_default().push(r.seg);
        }
        View {
            epoch: gc.map_or(0, |g| g.epoch),
            roots,
            heads,
        }
    }

    pub fn segments(&self) -> BTreeSet<SegKey> {
        self.roots.values().flatten().copied().collect()
    }

    pub fn has_head(&self, name: &str) -> bool {
        self.heads.iter().any(|(n, _)| n == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(b: u8) -> SegDigest {
        SegDigest([b; 32])
    }
    fn k(b: u8) -> SegKey {
        crate::manifest::ObjKey {
            digest: d(b),
            nonce: 0,
        }
    }

    fn gc(epoch: u64, roots: &[(&str, u8)], folded: &[&str]) -> GcRecord {
        let mut roots: Vec<RootRow> = roots
            .iter()
            .map(|(n, s)| RootRow {
                name: n.to_string(),
                seg: k(*s),
                stamp: 0,
                unmerged: vec![],
            })
            .collect();
        roots.sort_by(|a, b| a.name.cmp(&b.name));
        GcRecord {
            epoch,
            roots,
            folded: folded.iter().map(|s| s.to_string()).collect(),
            time: 0,
        }
    }

    fn rec(base: u64, root: &str, seg: u8) -> HeadRecord {
        HeadRecord {
            root: root.into(),
            base_epoch: base,
            seg: k(seg),
            time: 0,
        }
    }

    fn name(r: &HeadRecord) -> String {
        r.head_name(&Signed::unsigned(r.encode())).to_string()
    }

    #[test]
    fn names_roundtrip_and_reject_junk() {
        for h in [
            HeadName::Gc {
                epoch: 3,
                time: 0,
                digest: d(1),
            },
            HeadName::Drain {
                base_epoch: u64::MAX,
                root: 7,
                time: 0,
                digest: d(2),
            },
        ] {
            let s = h.to_string();
            assert!(
                s.len() <= 128 && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
                "{s}"
            );
            assert_eq!(HeadName::parse(&s), Some(h));
        }
        let ok = HeadName::Gc {
            epoch: 1,
            time: 0,
            digest: d(0xab),
        }
        .to_string();
        for junk in [
            "",
            "g-1-00",
            "x-0000000000000000-",
            "h-0000000000000000-0000000000000000-0000000000000000",
            "c-0000000000000000-0000000000000000-0000000000000000-00",
            &(ok.clone() + "-x"),
            &ok.to_uppercase(),
        ] {
            assert_eq!(HeadName::parse(junk), None, "{junk}");
        }
        // fixed width makes lexical = numeric order
        assert!(
            HeadName::Gc {
                epoch: 9,
                time: 0,
                digest: d(0xff)
            }
            .to_string()
                < HeadName::Gc {
                    epoch: 10,
                    time: 0,
                    digest: d(0)
                }
                .to_string()
        );
    }

    #[test]
    fn records_roundtrip_and_name_binds_body() {
        let g = gc(4, &[("main", 1), ("dev", 2)], &["h-x"]);
        assert_eq!(GcRecord::decode(&g.encode()).unwrap(), g);
        let body = Signed::unsigned(g.encode());
        assert_eq!(Signed::decode(&body).unwrap().record, g.encode());
        assert_eq!(
            g.head_name(&body),
            HeadName::Gc {
                epoch: 4,
                time: 0,
                digest: SegDigest::digest(&body)
            }
        );
        let mut unsorted = g.clone();
        unsorted.roots.swap(0, 1);
        assert!(GcRecord::decode(&unsorted.encode()).is_err());

        let h = rec(4, "main", 5);
        assert_eq!(HeadRecord::decode(&h.encode()).unwrap(), h);
        // Epoch and root are under the digest: the same body cannot be
        // renamed into another epoch or root.
        let mut other = h.clone();
        other.base_epoch = 5;
        assert_ne!(name(&h), name(&other));
        other = h.clone();
        other.root = "dev".into();
        assert_ne!(name(&h), name(&other));
    }

    proptest::proptest! {
        /// Head bodies come from other writers' scopes: junk is an `Err`.
        #[test]
        fn record_decoders_do_not_panic(
            which in 0usize..3,
            flips in proptest::collection::vec((0usize..512, 1u8..=255), 1..6),
            cut in 0usize..64,
        ) {
            let g = Signed::unsigned(gc(4, &[("main", 1), ("dev", 2)], &["h-x"]).encode());
            let h = Signed::unsigned(rec(4, "main", 5).encode());
            let mut bytes = [g.clone(), h, g][which].clone();
            let n = bytes.len();
            for (i, v) in &flips { bytes[i % n] ^= v; }
            bytes.truncate(n.saturating_sub(cut));
            if let Ok(s) = Signed::decode(&bytes) {
                match which {
                    0 => { let _ = GcRecord::decode(&s.record); }
                    1 => { let _ = HeadRecord::decode(&s.record); }
                    _ => { let _ = HeadName::parse(&String::from_utf8_lossy(&s.record)); }
                }
            }
        }
    }

    #[test]
    fn empty_store() {
        assert_eq!(View::compute(None, &HashMap::new()), View::default());
    }

    #[test]
    fn first_drain_names_its_root() {
        let h = rec(0, "main", 1);
        let v = View::compute(None, &HashMap::from([(name(&h), h)]));
        assert_eq!(v.roots["main"], vec![k(1)]);
    }

    #[test]
    fn view_is_base_plus_heads_per_root() {
        let g = gc(5, &[("main", 10), ("dev", 20)], &[]);
        let a = rec(5, "main", 11);
        let b = rec(4, "main", 12);
        let c = rec(5, "dev", 21);
        // A head naming the base again (a no-op drain) is just another claim.
        let same = rec(5, "main", 10);
        let recs: HashMap<String, HeadRecord> = [&a, &b, &c, &same]
            .into_iter()
            .map(|r| (name(r), r.clone()))
            .collect();
        let v = View::compute(Some(&g), &recs);
        assert_eq!(v.epoch, 5);
        assert_eq!(
            v.roots["main"].iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([k(10), k(11), k(12)])
        );
        assert_eq!(v.roots["main"][0], k(10), "base first");
        assert_eq!(v.roots["main"].len(), 4, "one entry per claim");
        assert_eq!(v.roots["dev"], vec![k(20), k(21)]);
        assert_eq!(v.heads.len(), 4);
    }

    #[test]
    fn pending_filters_by_epoch_and_folded() {
        let cur = name(&rec(5, "main", 11));
        let prev = name(&rec(4, "main", 12));
        let old = name(&rec(3, "main", 13));
        let future = name(&rec(6, "main", 14));
        let folded = name(&rec(5, "main", 16));
        let g = gc(5, &[("main", 10)], &[&folded]);
        let gn = g.head_name(&Signed::unsigned(g.encode())).to_string();
        let names = [&cur, &prev, &old, &future, &folded, &gn];
        let got: BTreeSet<&str> = pending_heads(names.iter().map(|s| s.as_str()), Some(&g))
            .into_iter()
            .collect();
        assert_eq!(got, BTreeSet::from([cur.as_str(), prev.as_str()]));
    }

    #[test]
    fn newest_gc_orders_by_epoch() {
        let g4 = gc(4, &[], &[]);
        let g5 = gc(5, &[], &[]);
        let (n4, n5) = (
            g4.head_name(&Signed::unsigned(g4.encode())).to_string(),
            g5.head_name(&Signed::unsigned(g5.encode())).to_string(),
        );
        let forged = HeadName::Gc {
            epoch: 5,
            time: 0,
            digest: d(0),
        }
        .to_string();
        let newest = newest_gc([n4.as_str(), n5.as_str(), forged.as_str(), "junk"]);
        assert_eq!(
            newest[..2].iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([n5.as_str(), forged.as_str()])
        );
        assert_eq!(newest[2], n4.as_str());
    }
}
