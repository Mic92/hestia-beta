//! Azure blob transfers over pre-signed SAS URLs.
//!
//! The Twirp API hands out pre-signed upload/download URLs pointing at Azure
//! Blob Storage. No Azure SDK is needed:
//!
//! * Upload: single `PUT` with `x-ms-blob-type: BlockBlob` (works for blobs
//!   up to 5000 MiB).
//! * Download: `GET`, optionally with a `Range` header for chunk reads.
//!
//! SAS URLs expire. On 401/403 the caller-provided refresh callback is asked
//! for a fresh URL (a new Twirp round-trip) and the transfer is retried once.
//! Transient failures (5xx, dropped connections) are retried with backoff.

use std::ops::Range;

use bytes::Bytes;

use crate::gha::Error;
use crate::gha::retry::{Backoff, is_transient};

/// `x-ms-blob-type` header value for single-shot uploads.
const BLOB_TYPE: &str = "BlockBlob";

/// Azure storage API version header (matches what actions/toolkit sends).
const API_VERSION: &str = "2020-04-08";

/// Format a half-open byte range as an HTTP `Range` header value
/// (inclusive on both ends per RFC 9110).
fn format_range(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

fn url_expired(status: u16) -> bool {
    status == 403 || status == 401
}

pub async fn status_error(url: &str, response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Error::Status {
        status,
        url: url.to_string(),
        body,
    }
}

/// Upload `data` to a pre-signed Azure URL with a single PUT.
pub async fn put(http: &reqwest::Client, url: &str, data: Bytes) -> Result<(), Error> {
    let response = http
        .put(url)
        .header("x-ms-blob-type", BLOB_TYPE)
        .header("x-ms-version", API_VERSION)
        .header(reqwest::header::CONTENT_LENGTH, data.len())
        .body(data)
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(status_error(url, response).await)
    }
}

/// Download a blob (or a byte range of it) from a pre-signed Azure URL.
///
/// When `range` is given, the response is validated to actually be that
/// range: the server must answer `206 Partial Content` with exactly
/// `range.end - range.start` bytes. Anything else (a server/proxy that
/// ignores `Range` and sends the whole blob with `200 OK`, or a blob that
/// is shorter than the caller believes) is an error — callers slice the
/// returned buffer at offsets relative to `range.start`, so handing them
/// different bytes silently corrupts every chunk they extract.
pub async fn get(
    http: &reqwest::Client,
    url: &str,
    range: Option<Range<u64>>,
) -> Result<Bytes, Error> {
    let mut request = http.get(url).header("x-ms-version", API_VERSION);
    if let Some(range) = &range {
        request = request.header(reqwest::header::RANGE, format_range(range));
    }
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(status_error(url, response).await);
    }
    let Some(range) = range else {
        return Ok(response.bytes().await?);
    };
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(Error::InvalidResponse(format!(
            "range request bytes {}..{} to {url} got HTTP {} instead of 206 Partial Content; \
             refusing to treat the response as the requested range",
            range.start,
            range.end,
            status.as_u16(),
        )));
    }
    let body = response.bytes().await?;
    let expected = range.end.saturating_sub(range.start);
    if body.len() as u64 != expected {
        return Err(Error::InvalidResponse(format!(
            "range request bytes {}..{} to {url} returned {} bytes instead of {expected}",
            range.start,
            range.end,
            body.len(),
        )));
    }
    Ok(body)
}

/// Like [`put`], but recovers from the two failure classes a pre-signed
/// transfer can hit: expired SAS URLs (401/403, ask `refresh` for a fresh
/// URL once) and transient failures (5xx, dropped connections; retried
/// with backoff).
pub async fn put_with_refresh<F>(
    http: &reqwest::Client,
    url: &str,
    data: Bytes,
    refresh: F,
) -> Result<(), Error>
where
    F: AsyncFnOnce() -> Result<String, Error>,
{
    let mut url = url.to_string();
    let mut refresh = Some(refresh);
    let mut backoff = Backoff::default();
    loop {
        match put(http, &url, data.clone()).await {
            Err(Error::Status { status, .. }) if url_expired(status) && refresh.is_some() => {
                url = refresh.take().expect("checked above")().await?;
            }
            Err(err) if backoff.retry(is_transient(&err)).await => {}
            result => return result,
        }
    }
}

/// Like [`get`], but with the same recovery as [`put_with_refresh`].
pub async fn get_with_refresh<F>(
    http: &reqwest::Client,
    url: &str,
    range: Option<Range<u64>>,
    refresh: F,
) -> Result<Bytes, Error>
where
    F: AsyncFnOnce() -> Result<String, Error>,
{
    let mut url = url.to_string();
    let mut refresh = Some(refresh);
    let mut backoff = Backoff::default();
    loop {
        match get(http, &url, range.clone()).await {
            Err(Error::Status { status, .. }) if url_expired(status) && refresh.is_some() => {
                url = refresh.take().expect("checked above")().await?;
            }
            Err(err) if backoff.retry(is_transient(&err)).await => {}
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_header_is_inclusive() {
        assert_eq!(format_range(&(0..1)), "bytes=0-0");
        assert_eq!(format_range(&(100..200)), "bytes=100-199");
        assert_eq!(format_range(&(0..0)), "bytes=0-0"); // degenerate, never sent
    }

    #[test]
    fn expiry_detection_only_matches_auth_failures() {
        assert!(url_expired(403));
        assert!(url_expired(401));
        assert!(!url_expired(404));
        assert!(!url_expired(500));
    }
}
