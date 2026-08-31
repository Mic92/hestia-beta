//! One backoff policy for every HTTP client here: network errors, 5xx and
//! 429 are retried a few times with doubling delay, nothing else is.

use std::time::Duration;

use super::Error;

const RETRIES: u32 = 4;
const FIRST_DELAY: Duration = Duration::from_millis(200);

/// Whether an error is worth retrying with the same request: network
/// failures (connection drops, timeouts, truncated bodies), 5xx and 429.
/// 401/403 are *not* transient: they need a fresh token or signed URL.
pub fn is_transient(error: &Error) -> bool {
    match error {
        // Builder and redirect-policy failures are deterministic.
        Error::Http(err) => !err.is_builder() && !err.is_redirect(),
        Error::Status { status, .. } => transient_status(*status),
        _ => false,
    }
}

pub fn transient_status(status: u16) -> bool {
    status >= 500 || status == 429
}

/// `is_transient` lifted to a response that has not been turned into an
/// error yet.
pub fn transient(result: &Result<reqwest::Response, Error>) -> bool {
    match result {
        Ok(r) => transient_status(r.status().as_u16()),
        Err(e) => is_transient(e),
    }
}

pub struct Backoff {
    left: u32,
    delay: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            left: RETRIES,
            delay: FIRST_DELAY,
        }
    }
}

impl Backoff {
    /// After a failed attempt: if it was `transient` and budget is left,
    /// sleep and answer `true` for another try.
    pub async fn retry(&mut self, transient: bool) -> bool {
        if !transient || self.left == 0 {
            return false;
        }
        self.left -= 1;
        tokio::time::sleep(self.delay).await;
        self.delay *= 2;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_errors_are_server_side_failures_not_auth_or_client_errors() {
        let status = |status| Error::Status {
            status,
            url: String::new(),
            body: String::new(),
        };
        assert!(is_transient(&status(500)));
        assert!(is_transient(&status(503)));
        assert!(is_transient(&status(429)));
        assert!(!is_transient(&status(401)));
        assert!(!is_transient(&status(403)));
        assert!(!is_transient(&status(404)));
        assert!(!is_transient(&Error::MissingEnv("X")));
        assert!(!is_transient(&Error::InvalidResponse("bad".into())));
    }
}
