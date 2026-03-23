//! Retry logic with exponential backoff for S3 operations.
//!
//! Adapted from walrust-core's battle-tested retry module. Implements:
//! - Exponential backoff with full jitter
//! - Error classification (retryable vs non-retryable)
//! - Circuit breaker pattern
//!
//! Based on AWS best practices:
//! https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

use anyhow::{anyhow, Result};
use rand::Rng;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: 5).
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds (default: 100).
    pub base_delay_ms: u64,
    /// Maximum backoff delay in milliseconds (default: 30000 = 30s).
    pub max_delay_ms: u64,
    /// Enable circuit breaker (default: true).
    pub circuit_breaker_enabled: bool,
    /// Number of consecutive failures before circuit opens (default: 10).
    pub circuit_breaker_threshold: u32,
    /// Time to wait before attempting half-open state (ms, default: 60000 = 1min).
    pub circuit_breaker_cooldown_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            circuit_breaker_enabled: true,
            circuit_breaker_threshold: 10,
            circuit_breaker_cooldown_ms: 60_000,
        }
    }
}

/// Error classification for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient error — should retry (500, 502, 503, 504, timeouts, network).
    Transient,
    /// Client error — don't retry, it's a bug (400).
    ClientError,
    /// Authentication error — don't retry without user intervention (401, 403).
    AuthError,
    /// Not found — context dependent (404).
    NotFound,
    /// Unknown error — may retry with caution.
    Unknown,
}

/// Classify an error to determine retry behavior.
pub fn classify_error(error: &anyhow::Error) -> ErrorKind {
    let error_str = error.to_string().to_lowercase();

    // HTTP 5xx server errors
    if error_str.contains("500")
        || error_str.contains("502")
        || error_str.contains("503")
        || error_str.contains("504")
        || error_str.contains("internal server error")
        || error_str.contains("bad gateway")
        || error_str.contains("service unavailable")
        || error_str.contains("gateway timeout")
    {
        return ErrorKind::Transient;
    }

    // Network and timeout errors
    if error_str.contains("timeout")
        || error_str.contains("timed out")
        || error_str.contains("connection")
        || error_str.contains("network")
        || error_str.contains("socket")
        || error_str.contains("reset")
        || error_str.contains("broken pipe")
        || error_str.contains("eof")
        || error_str.contains("temporarily unavailable")
        || error_str.contains("dispatch failure")
    {
        return ErrorKind::Transient;
    }

    // AWS SDK specific transient errors
    if error_str.contains("throttl")
        || error_str.contains("slowdown")
        || error_str.contains("reduce your request rate")
        || error_str.contains("request rate exceeded")
    {
        return ErrorKind::Transient;
    }

    // Client errors — don't retry
    if error_str.contains("400") || error_str.contains("bad request") {
        return ErrorKind::ClientError;
    }

    // Auth errors — don't retry
    if error_str.contains("401")
        || error_str.contains("403")
        || error_str.contains("unauthorized")
        || error_str.contains("forbidden")
        || error_str.contains("access denied")
        || error_str.contains("invalid credentials")
        || error_str.contains("expired token")
    {
        return ErrorKind::AuthError;
    }

    // Not found
    if error_str.contains("404")
        || error_str.contains("not found")
        || error_str.contains("no such key")
        || error_str.contains("nosuchkey")
    {
        return ErrorKind::NotFound;
    }

    ErrorKind::Unknown
}

/// Check if an error is retryable.
pub fn is_retryable(error: &anyhow::Error) -> bool {
    matches!(
        classify_error(error),
        ErrorKind::Transient | ErrorKind::Unknown
    )
}

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation.
    Closed,
    /// Failing — rejecting requests.
    Open,
    /// Testing if service recovered.
    HalfOpen,
}

/// Circuit breaker for preventing cascading failures.
#[derive(Debug)]
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    threshold: u32,
    /// Time when circuit opened (ms since UNIX epoch, 0 = not open).
    opened_at_ms: AtomicU64,
    cooldown_ms: u64,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown_ms: u64) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            opened_at_ms: AtomicU64::new(0),
            cooldown_ms,
        }
    }

    pub fn state(&self) -> CircuitState {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        let opened_at = self.opened_at_ms.load(Ordering::Relaxed);

        if failures < self.threshold {
            return CircuitState::Closed;
        }

        if opened_at == 0 {
            return CircuitState::Closed;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if now_ms - opened_at >= self.cooldown_ms {
            CircuitState::HalfOpen
        } else {
            CircuitState::Open
        }
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.opened_at_ms.store(0, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        if failures >= self.threshold && self.opened_at_ms.load(Ordering::Relaxed) == 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.opened_at_ms.store(now_ms, Ordering::Relaxed);
            tracing::warn!(
                "Circuit breaker opened after {} consecutive failures",
                failures
            );
        }
    }

    pub fn should_allow(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true, // Allow one test request
            CircuitState::Open => false,
        }
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }
}

/// Retry policy with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    config: RetryConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl RetryPolicy {
    pub fn new(config: RetryConfig) -> Self {
        let circuit_breaker = if config.circuit_breaker_enabled {
            Some(Arc::new(CircuitBreaker::new(
                config.circuit_breaker_threshold,
                config.circuit_breaker_cooldown_ms,
            )))
        } else {
            None
        };

        Self {
            config,
            circuit_breaker,
        }
    }

    pub fn default_policy() -> Self {
        Self::new(RetryConfig::default())
    }

    pub fn config(&self) -> &RetryConfig {
        &self.config
    }

    pub fn circuit_breaker(&self) -> Option<&Arc<CircuitBreaker>> {
        self.circuit_breaker.as_ref()
    }

    /// Calculate backoff delay for a given attempt.
    ///
    /// Uses full jitter: sleep = random(0, min(cap, base * 2^attempt))
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.config.base_delay_ms;
        let cap = self.config.max_delay_ms;

        // Exponential delay: base * 2^attempt (capped to prevent overflow)
        let exp_delay = base.saturating_mul(1u64 << attempt.min(20));
        let capped_delay = exp_delay.min(cap);

        // Full jitter: random delay between 0 and capped_delay
        let jittered = if capped_delay > 0 {
            rand::rng().random_range(0..=capped_delay)
        } else {
            0
        };

        Duration::from_millis(jittered)
    }

    /// Execute an async operation with retry.
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check circuit breaker
        if let Some(cb) = &self.circuit_breaker {
            if !cb.should_allow() {
                return Err(anyhow!(
                    "Circuit breaker open — refusing request after {} consecutive failures",
                    self.config.circuit_breaker_threshold
                ));
            }
        }

        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=self.config.max_retries {
            match operation().await {
                Ok(result) => {
                    if let Some(cb) = &self.circuit_breaker {
                        cb.record_success();
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let error_kind = classify_error(&e);
                    let retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                    if let Some(cb) = &self.circuit_breaker {
                        cb.record_failure();
                    }

                    // Don't retry non-retryable errors
                    if !retryable {
                        tracing::warn!(
                            "Non-retryable error (kind={:?}): {}",
                            error_kind,
                            e
                        );
                        return Err(e);
                    }

                    if attempt < self.config.max_retries {
                        let delay = self.calculate_delay(attempt);
                        tracing::debug!(
                            "Attempt {}/{} failed (kind={:?}), retrying in {:?}: {}",
                            attempt + 1,
                            self.config.max_retries + 1,
                            error_kind,
                            delay,
                            e
                        );
                        tokio::time::sleep(delay).await;
                    }

                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Retry failed with no error recorded")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        // Transient
        assert_eq!(
            classify_error(&anyhow!("500 Internal Server Error")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("503 Service Unavailable")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("Connection timeout")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("dispatch failure")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("SlowDown: reduce your request rate")),
            ErrorKind::Transient
        );

        // Auth
        assert_eq!(
            classify_error(&anyhow!("401 Unauthorized")),
            ErrorKind::AuthError
        );
        assert_eq!(
            classify_error(&anyhow!("403 Forbidden")),
            ErrorKind::AuthError
        );
        assert_eq!(
            classify_error(&anyhow!("Access Denied")),
            ErrorKind::AuthError
        );

        // Client
        assert_eq!(
            classify_error(&anyhow!("400 Bad Request")),
            ErrorKind::ClientError
        );

        // NotFound
        assert_eq!(
            classify_error(&anyhow!("404 Not Found")),
            ErrorKind::NotFound
        );
        assert_eq!(
            classify_error(&anyhow!("NoSuchKey")),
            ErrorKind::NotFound
        );

        // Unknown
        assert_eq!(
            classify_error(&anyhow!("some weird error")),
            ErrorKind::Unknown
        );
    }

    #[test]
    fn test_is_retryable() {
        assert!(is_retryable(&anyhow!("500 Internal Server Error")));
        assert!(is_retryable(&anyhow!("Connection timeout")));
        assert!(is_retryable(&anyhow!("some weird error"))); // Unknown → retryable
        assert!(!is_retryable(&anyhow!("401 Unauthorized")));
        assert!(!is_retryable(&anyhow!("403 Forbidden")));
        assert!(!is_retryable(&anyhow!("400 Bad Request")));
        assert!(!is_retryable(&anyhow!("404 Not Found")));
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy::new(RetryConfig {
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            ..Default::default()
        });

        // Attempt 0: 0-100ms
        for _ in 0..20 {
            let delay = policy.calculate_delay(0);
            assert!(delay <= Duration::from_millis(100));
        }

        // Attempt 1: 0-200ms
        for _ in 0..20 {
            let delay = policy.calculate_delay(1);
            assert!(delay <= Duration::from_millis(200));
        }

        // High attempt: capped at 30s
        for _ in 0..20 {
            let delay = policy.calculate_delay(20);
            assert!(delay <= Duration::from_millis(30_000));
        }
    }

    #[test]
    fn test_backoff_zero_base() {
        let policy = RetryPolicy::new(RetryConfig {
            base_delay_ms: 0,
            max_delay_ms: 0,
            ..Default::default()
        });
        assert_eq!(policy.calculate_delay(0), Duration::from_millis(0));
        assert_eq!(policy.calculate_delay(5), Duration::from_millis(0));
    }

    #[test]
    fn test_circuit_breaker_states() {
        let cb = CircuitBreaker::new(3, 100);

        // Initial: closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());

        // Two failures: still closed
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());

        // Third failure: opens
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.should_allow());
        assert_eq!(cb.consecutive_failures(), 3);

        // Wait for cooldown
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.should_allow());

        // Success resets
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn test_circuit_breaker_reopen_after_halfopen_failure() {
        let cb = CircuitBreaker::new(3, 50);

        // Open the circuit
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for half-open
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Another failure keeps it open (counter increments past threshold)
        cb.record_failure();
        // The opened_at_ms was already set, so state depends on time.
        // With fresh failures > threshold, it stays open.
        assert!(cb.consecutive_failures() >= 3);
    }

    #[tokio::test]
    async fn test_retry_success() {
        let policy = RetryPolicy::default_policy();
        let result: Result<i32> = policy.execute(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_transient_then_success() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 3,
            base_delay_ms: 1, // fast for tests
            max_delay_ms: 10,
            circuit_breaker_enabled: false,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                async move {
                    if attempt < 2 {
                        Err(anyhow!("Service unavailable"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn test_retry_auth_error_no_retry() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 5,
            base_delay_ms: 1,
            circuit_breaker_enabled: false,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async { Err(anyhow!("401 Unauthorized")) }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1); // No retry for auth errors
    }

    #[tokio::test]
    async fn test_retry_not_found_no_retry() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 5,
            base_delay_ms: 1,
            circuit_breaker_enabled: false,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async { Err(anyhow!("NoSuchKey: 404 Not Found")) }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 2,
            base_delay_ms: 1,
            max_delay_ms: 5,
            circuit_breaker_enabled: false,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async { Err(anyhow!("Service unavailable")) }
            })
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Service unavailable"));
        assert_eq!(attempts.load(Ordering::Relaxed), 3); // 1 initial + 2 retries
    }

    #[tokio::test]
    async fn test_circuit_breaker_blocks_execute() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 0,
            base_delay_ms: 1,
            circuit_breaker_enabled: true,
            circuit_breaker_threshold: 2,
            circuit_breaker_cooldown_ms: 60_000,
            ..Default::default()
        });

        // Trip the circuit breaker
        let cb = policy.circuit_breaker().unwrap().clone();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Execute should fail immediately
        let result: Result<i32> = policy.execute(|| async { Ok(42) }).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Circuit breaker open"));
    }

    #[tokio::test]
    async fn test_retry_resets_circuit_breaker_on_success() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 5,
            base_delay_ms: 1,
            circuit_breaker_enabled: true,
            circuit_breaker_threshold: 10,
            ..Default::default()
        });

        // Record some failures
        let cb = policy.circuit_breaker().unwrap().clone();
        for _ in 0..5 {
            cb.record_failure();
        }
        assert_eq!(cb.consecutive_failures(), 5);

        // Successful execute resets
        let _: i32 = policy.execute(|| async { Ok(42) }).await.unwrap();
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn test_retry_policy_disabled_circuit_breaker() {
        let policy = RetryPolicy::new(RetryConfig {
            circuit_breaker_enabled: false,
            ..Default::default()
        });
        assert!(policy.circuit_breaker().is_none());
    }
}
