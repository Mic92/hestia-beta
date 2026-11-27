//! Blob storage by string key. Every key but a head's is
//! `<kind>-<sha256 of the body>`, which the OCI backend relies on.

use std::ops::Range;

use bytes::Bytes;

pub use crate::gha::Error;

pub mod blobdir;
pub mod cached;
pub mod gha;
pub mod ghcr;
pub mod oci;
mod urls;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub created: Option<u64>,
    pub last_accessed: Option<u64>,
}

#[derive(Clone)]
pub enum Backend {
    Gha(gha::Gha),
    Oci(oci::Oci),
    /// S3, WebDAV, or the same tree read-only over plain HTTP.
    Dir(blobdir::BlobDir),
    /// Another backend with its packs kept on local disk (a cache node).
    Cached(std::sync::Arc<cached::Cached>),
}

pub const ENV_OCI: &str = "HESTIA_OCI";
pub const ENV_S3: &str = "HESTIA_S3";
pub const ENV_DAV: &str = "HESTIA_DAV";

impl Backend {
    /// `HESTIA_S3=s3://<bucket>/<prefix>` selects a bucket,
    /// `HESTIA_DAV=https://<host>/<prefix>` a WebDAV share,
    /// `HESTIA_OCI=<registry>/<repository>` a registry, else the Actions cache.
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        if let Some(url) = var(ENV_S3) {
            return Ok(Backend::Dir(blobdir::BlobDir::s3_from_env(&url, http)?));
        }
        if let Some(url) = var(ENV_DAV) {
            return Ok(Backend::Dir(blobdir::BlobDir::dav_from_env(&url, http)?));
        }
        if let Some(repo) = var(ENV_OCI) {
            return Ok(Backend::Oci(oci::Oci::from_env(&repo, http)?));
        }
        Ok(Backend::Gha(gha::Gha::from_env(http)?))
    }

    /// `false` if the key already existed (same content for content-addressed keys).
    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.put(key, data).await,
            Backend::Oci(b) => b.put(key, data).await,
            Backend::Dir(b) => b.put(key, data).await,
            Backend::Cached(b) => Box::pin(b.put(key, data)).await,
        }
    }

    /// `None` if the key does not exist.
    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        match self {
            Backend::Gha(b) => b.get(key, range).await,
            Backend::Oci(b) => b.get(key, range).await,
            Backend::Dir(b) => b.get(key, range).await,
            Backend::Cached(b) => Box::pin(b.get(key, range)).await,
        }
    }

    /// A range of a pack whose length the caller knows. A backend that
    /// keeps packs locally uses the length to fetch in whole blocks.
    pub async fn get_pack(
        &self,
        key: &str,
        range: Range<u64>,
        len: u64,
    ) -> Result<Option<Bytes>, Error> {
        match self {
            Backend::Cached(b) => Box::pin(b.get_pack(key, range, len)).await,
            _ => self.get(key, Some(range)).await,
        }
    }

    /// Whether pack reads are kept locally. Then reading ahead is this
    /// backend's business, not the reader's.
    pub fn keeps_packs(&self) -> bool {
        matches!(self, Backend::Cached(_))
    }

    /// Reset the key's LRU clock where there is one. `false` if the key is gone.
    pub async fn touch(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.touch(key).await,
            Backend::Oci(b) => b.exists(key).await,
            Backend::Dir(b) => b.exists(key).await,
            Backend::Cached(b) => Box::pin(b.inner.touch(key)).await,
        }
    }

    /// A writer found `key` stored and is about to publish a segment
    /// that names it: make it count as new to a GC, which deletes what
    /// no segment names once it is old. A bucket has no clock to reset,
    /// so the object is written again.
    pub async fn refresh(&self, key: &str, data: bytes::Bytes) -> Result<(), Error> {
        match self {
            Backend::Dir(b) => b.overwrite(key, data).await,
            Backend::Cached(b) => Box::pin(b.inner.refresh(key, data)).await,
            Backend::Gha(_) | Backend::Oci(_) => {
                if !self.touch(key).await? {
                    self.put(key, data).await?;
                }
                Ok(())
            }
        }
    }

    /// `Ok(None)` if there are more than `limit` entries: a partial listing
    /// proves presence but never absence.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        match self {
            Backend::Gha(b) => b.list(prefix, limit).await,
            Backend::Oci(b) => b.list(prefix, limit).await,
            Backend::Dir(b) => b.list(prefix, limit).await,
            Backend::Cached(b) => Box::pin(b.inner.list(prefix, limit)).await,
        }
    }

    /// Every `g-`/`h-`/`c-` key. One request where the backend can.
    pub async fn list_heads(&self) -> Result<Vec<Listed>, Error> {
        if let Backend::Cached(b) = self {
            return Box::pin(b.inner.list_heads()).await;
        }
        if let Backend::Gha(b) = self {
            let (g, h, c) = futures_util::future::try_join3(
                b.list("g-", None),
                b.list("h-", None),
                b.list("c-", None),
            )
            .await?;
            return Ok([g, h, c].into_iter().flatten().flatten().collect());
        }
        Ok(self.list("", None).await?.expect("unbounded"))
    }

    /// GC only: all `pack-`/`seg-`/`tree-`/`use-` objects with creation times.
    pub async fn list_objects(&self) -> Result<Vec<Listed>, Error> {
        if let Backend::Cached(b) = self {
            return Box::pin(b.inner.list_objects()).await;
        }
        if let Backend::Oci(b) = self {
            return b.list_objects().await;
        }
        let mut out = Vec::new();
        for prefix in ["pack-", "seg-", "tree-", "use-"] {
            let listed = self.list(prefix, None).await?.ok_or(Error::InvalidEnv {
                name: ENV_S3,
                reason: "an http(s):// store is read-only and cannot be collected".into(),
            })?;
            out.extend(listed);
        }
        Ok(out)
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.delete(key).await,
            Backend::Oci(b) => b.delete(key).await,
            Backend::Dir(b) => b.delete(key).await,
            Backend::Cached(b) => Box::pin(b.delete(key)).await,
        }
    }

    /// How many pack reads may be in flight across all NAR requests. A
    /// pack read on the Actions cache is a Twirp URL lookup plus Azure
    /// Range requests against a rate-limited API, so few. A bucket or a
    /// share takes far more, and a daemon many builders share is held up
    /// by nothing else (`benches/cluster.rs`).
    pub fn read_concurrency(&self) -> usize {
        match self {
            Backend::Gha(_) => 8,
            Backend::Oci(_) => 16,
            Backend::Dir(_) => 64,
            Backend::Cached(b) => b.inner.read_concurrency(),
        }
    }

    /// Why writes are refused, for the message a read-only serve prints.
    pub fn read_only_hint(&self) -> &'static str {
        match self {
            Backend::Gha(_) => {
                "the runtime token is read-only (expected for check_run and fork pull_request \
                 events)"
            }
            Backend::Oci(_) => "the registry credentials are missing or grant no push access",
            Backend::Dir(b) if !b.writable() => "an http(s):// store can only be read",
            Backend::Dir(_) => "the store credentials are missing or grant no write access",
            Backend::Cached(b) => b.inner.read_only_hint(),
        }
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.probe_writable().await,
            Backend::Oci(b) => b.probe_writable().await,
            Backend::Dir(b) => b.probe_writable().await,
            Backend::Cached(b) => Box::pin(b.inner.probe_writable()).await,
        }
    }

    /// Waits until the store has those of `keys` that were taken ahead
    /// of it ([`cached::Cached::put_behind`]).
    pub async fn settled(&self, keys: &[String]) -> Result<(), String> {
        match self {
            Backend::Cached(b) => b.settled(keys).await,
            _ => Ok(()),
        }
    }

    /// Persist backend bookkeeping: the GHCR ledger, the S3 head index.
    pub async fn flush(&self) -> Result<(), Error> {
        match self {
            Backend::Gha(_) => Ok(()),
            Backend::Dir(b) => b.flush().await,
            Backend::Cached(b) => Box::pin(b.inner.flush()).await,
            Backend::Oci(b) => b.flush().await,
        }
    }
}
