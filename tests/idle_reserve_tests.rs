//! Regression tests for the idle-reserve bound and non-blocking validation.
//!
//! Before the fix the pool kept up to `max_size` *idle* connections and
//! validated each of them **while holding the pool mutex**, so a burst of idle
//! connections produced heavy "Connection validation failed" churn and
//! multi-second acquire stalls. These tests pin the bounded idle reserve.

use connection_pool::{CleanupConfig, ConnectionManager, ConnectionPool, MAX_IDLE_KEEP};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct TestError;
impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test error")
    }
}
impl std::error::Error for TestError {}

/// Minimal in-memory manager: every created connection is just an incrementing
/// id, and every connection is valid. Creation is instant so we can assert on
/// the pool bookkeeping without touching the network.
#[derive(Clone)]
struct MockManager {
    created: Arc<AtomicUsize>,
}

impl ConnectionManager for MockManager {
    type Connection = usize;
    type Error = TestError;
    type CreateFut = Pin<Box<dyn Future<Output = Result<usize, TestError>> + Send>>;
    type ValidFut<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

    fn create_connection(&self) -> Self::CreateFut {
        let id = self.created.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(id) })
    }

    fn is_valid<'a>(&'a self, _conn: &'a mut usize) -> Self::ValidFut<'a> {
        Box::pin(async move { true })
    }
}

#[derive(Clone)]
struct BlockingValidationManager {
    created: Arc<AtomicUsize>,
    block_validation: Arc<AtomicBool>,
    validation_started: Arc<Notify>,
}

impl ConnectionManager for BlockingValidationManager {
    type Connection = usize;
    type Error = TestError;
    type CreateFut = Pin<Box<dyn Future<Output = Result<usize, TestError>> + Send>>;
    type ValidFut<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

    fn create_connection(&self) -> Self::CreateFut {
        let id = self.created.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(id) })
    }

    fn is_valid<'a>(&'a self, _conn: &'a mut usize) -> Self::ValidFut<'a> {
        let block_validation = self.block_validation.clone();
        let validation_started = self.validation_started.clone();
        Box::pin(async move {
            if block_validation.load(Ordering::SeqCst) {
                validation_started.notify_one();
                std::future::pending::<()>().await;
            }
            true
        })
    }
}

fn new_pool(max_size: usize) -> Arc<ConnectionPool<MockManager>> {
    let manager = MockManager {
        created: Arc::new(AtomicUsize::new(0)),
    };
    ConnectionPool::new(Some(max_size), None, None, None, manager)
}

#[tokio::test]
async fn idle_reserve_is_bounded_by_max_idle_keep() {
    let pool = new_pool(50);

    // Acquire far more connections than the idle reserve (each acquisition
    // creates a fresh connection because the pool starts empty).
    let mut acquired = Vec::new();
    for _ in 0..30 {
        acquired.push(pool.clone().get_connection().await.expect("acquire"));
    }

    // Return them all: recycling is spawned per drop.
    drop(acquired);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let idle = pool.pool_size().await;
    assert!(
        idle <= MAX_IDLE_KEEP,
        "idle reserve must be bounded to {MAX_IDLE_KEEP}, but {idle} connections were kept"
    );
    assert_eq!(pool.outstanding_count(), 0, "no connection should remain outstanding");
}

#[tokio::test]
async fn acquire_still_works_after_idle_reserve_is_full() {
    let pool = new_pool(50);

    // Fill the idle reserve past its cap.
    let mut first = Vec::new();
    for _ in 0..20 {
        first.push(pool.clone().get_connection().await.expect("acquire"));
    }
    drop(first);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The pool can still hand out connections (reusing the bounded reserve or
    // creating new ones); this must not deadlock or error.
    let mut second = Vec::new();
    for _ in 0..20 {
        second.push(pool.clone().get_connection().await.expect("acquire after recycle"));
    }
    assert_eq!(second.len(), 20);
}

#[tokio::test]
async fn stopping_cleanup_cancels_validation_and_preserves_idle_connection() {
    let validation_started = Arc::new(Notify::new());
    let block_validation = Arc::new(AtomicBool::new(false));
    let manager = BlockingValidationManager {
        created: Arc::new(AtomicUsize::new(0)),
        block_validation: block_validation.clone(),
        validation_started: validation_started.clone(),
    };
    let pool = ConnectionPool::new(
        Some(2),
        None,
        None,
        Some(CleanupConfig {
            interval: Duration::from_secs(3600),
            enabled: false,
        }),
        manager,
    );

    let first = pool.clone().get_connection().await.expect("first acquire");
    let second = pool.clone().get_connection().await.expect("second acquire");
    drop((first, second));
    tokio::time::timeout(Duration::from_secs(1), async {
        while pool.pool_size().await != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection should be recycled");

    block_validation.store(true, Ordering::SeqCst);
    pool.restart_cleanup_task(CleanupConfig {
        interval: Duration::from_secs(3600),
        enabled: true,
    })
    .await;
    tokio::time::timeout(Duration::from_secs(1), validation_started.notified())
        .await
        .expect("cleanup should start validation");

    tokio::time::timeout(Duration::from_secs(1), pool.stop_cleanup_task())
        .await
        .expect("cleanup stop should cancel in-flight validation");
    assert_eq!(pool.pool_size().await, 1, "untouched idle connection should be preserved");
}
