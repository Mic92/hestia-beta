//! Drains through a cache node.
//!
//! A builder that drains on its own loads the root's whole index to learn
//! what is stored, and publishes a segment and a head of its own. Through
//! a node it holds no index and publishes nothing:
//!
//! 1. `POST /hestia/plan` names the paths it is about to push. The node
//!    answers which of them are stored, and sends the indexes of the packs
//!    that hold earlier paths of the same names, so the builder uploads
//!    only chunks those lack.
//! 2. The builder uploads its packs to the store, as before.
//! 3. `POST /hestia/commit` hands the sealed segment to the node. The node
//!    merges what arrives within its commit interval into one segment,
//!    publishes one head for it and answers every builder with its name.
//!
//! The node trusts the builders it serves as the store trusts anyone who
//! can write to it: a segment naming packs that do not exist is published
//! as it is and its paths fail when fetched.

use std::sync::Arc;

use crate::manifest::{PackHash, PathHash};
use crate::segment::{self, PackIndex, Sealed};
use crate::store::KnownChunks;

const MAGIC_PLAN: &[u8; 4] = b"HNP1";
const MAGIC_COMMIT: &[u8; 4] = b"HNC1";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("malformed node message: {0}")]
    Malformed(&'static str),
    #[error("cache node unreachable: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cache node refused: {0}")]
    Refused(String),
    #[error(transparent)]
    Segment(#[from] segment::Error),
}

/// What a builder asks before a drain: `(path hash, name)` of every path
/// it would push.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PlanRequest {
    pub paths: Vec<(String, String)>,
}

/// The node's answer to a [`PlanRequest`].
pub struct Plan {
    /// The GC epoch the answer holds for. A commit made from it is
    /// refused once a GC has moved chunks.
    pub epoch: u64,
    /// Asked paths the node serves already.
    pub present: Vec<PathHash>,
    /// Indexes of the packs that hold stored paths of the asked names.
    pub packs: Vec<(PackHash, Arc<PackIndex>)>,
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.0.len() < n {
            return Err(Error::Malformed("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u32(&mut self) -> Result<usize, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize)
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn blob(&mut self) -> Result<&'a [u8], Error> {
        let n = self.u32()?;
        self.take(n)
    }
}

fn put_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
}

impl Plan {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC_PLAN.to_vec();
        out.extend(self.epoch.to_le_bytes());
        put_blob(&mut out, &segment::encode_uses(&self.present));
        out.extend((self.packs.len() as u32).to_le_bytes());
        for (hash, index) in &self.packs {
            out.extend(hash.0);
            put_blob(&mut out, &index.encode());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Plan, Error> {
        let mut r = Reader(bytes);
        if r.take(4)? != MAGIC_PLAN {
            return Err(Error::Malformed("not a plan"));
        }
        let epoch = r.u64()?;
        let present = segment::decode_uses(r.blob()?)?;
        let n = r.u32()?;
        let mut packs = Vec::new();
        for _ in 0..n {
            let hash = PackHash(r.take(PackHash::LEN)?.try_into().unwrap());
            packs.push((hash, Arc::new(PackIndex::decode(r.blob()?)?)));
        }
        Ok(Plan {
            epoch,
            present,
            packs,
        })
    }

    pub fn known_chunks(&self) -> KnownChunks {
        let mut known = KnownChunks::default();
        for (pack, index) in &self.packs {
            known.add(*pack, index);
        }
        known
    }
}

/// A sealed segment and the epoch of the plan it was made from.
pub fn encode_commit(epoch: u64, sealed: &Sealed) -> Vec<u8> {
    let mut out = MAGIC_COMMIT.to_vec();
    out.extend(epoch.to_le_bytes());
    put_blob(&mut out, &sealed.meta);
    put_blob(&mut out, &sealed.tree);
    put_blob(&mut out, sealed.uses.as_deref().unwrap_or(&[]));
    out
}

pub fn decode_commit(bytes: &[u8]) -> Result<(u64, Sealed), Error> {
    let mut r = Reader(bytes);
    if r.take(4)? != MAGIC_COMMIT {
        return Err(Error::Malformed("not a commit"));
    }
    let epoch = r.u64()?;
    let meta = r.blob()?.to_vec();
    let tree = r.blob()?.to_vec();
    let uses = r.blob()?;
    let uses = (!uses.is_empty()).then(|| uses.to_vec());
    Ok((epoch, Sealed { meta, tree, uses }))
}

/// The builder's side.
#[derive(Clone)]
pub struct NodeClient {
    http: reqwest::Client,
    base: String,
}

impl NodeClient {
    pub fn new(http: reqwest::Client, base: &str) -> NodeClient {
        NodeClient {
            http,
            base: base.trim_end_matches('/').to_owned(),
        }
    }

    async fn post(&self, what: &str, body: Vec<u8>) -> Result<bytes::Bytes, Error> {
        let response = self
            .http
            .post(format!("{}/hestia/{what}", self.base))
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(Error::Refused(format!(
                "{status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        Ok(body)
    }

    pub async fn plan(&self, paths: Vec<(PathHash, String)>) -> Result<Plan, Error> {
        let request = PlanRequest {
            paths: paths
                .into_iter()
                .map(|(hash, name)| (hash.to_string(), name))
                .collect(),
        };
        let body = serde_json::to_vec(&request).expect("plan request encodes");
        Plan::decode(&self.post("plan", body).await?)
    }

    /// Uploads a pack through the node, which keeps it for the builders
    /// that read it next. `false` when the bucket already had it.
    pub async fn put_pack(&self, key: &str, data: bytes::Bytes) -> Result<bool, Error> {
        let response = self
            .http
            .put(format!("{}/hestia/pack/{key}", self.base))
            .body(data)
            .send()
            .await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(Error::Refused(format!(
                "{status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        Ok(body.as_ref() == b"created")
    }

    /// Returns the head the node published for this and other drains.
    pub async fn commit(&self, epoch: u64, sealed: &Sealed) -> Result<String, Error> {
        let body = self.post("commit", encode_commit(epoch, sealed)).await?;
        Ok(String::from_utf8_lossy(&body).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::SegmentWriter;

    #[test]
    fn plan_and_commit_survive_the_wire() {
        let hash: PathHash = "00000000000000000000000000000000".parse().unwrap();
        let plan = Plan {
            epoch: 7,
            present: vec![hash],
            packs: vec![(PackHash([3; 32]), Arc::new(PackIndex::default()))],
        };
        let back = Plan::decode(&plan.encode()).unwrap();
        assert_eq!((back.epoch, back.present), (7, vec![hash]));
        assert_eq!(back.packs, plan.packs);
        assert!(Plan::decode(&plan.encode()[..20]).is_err());

        let mut writer = SegmentWriter::default();
        writer.use_path(&hash);
        let sealed = writer.seal().unwrap();
        let (epoch, back) = decode_commit(&encode_commit(9, &sealed)).unwrap();
        assert_eq!(epoch, 9);
        assert_eq!(
            (back.meta, back.tree, back.uses),
            (sealed.meta, sealed.tree, sealed.uses)
        );
    }
}
