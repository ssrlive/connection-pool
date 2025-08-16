use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout};

pub const DEFAULT_MAX_SIZE: usize = 10;
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60); // 5 minutes
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10); // 10 seconds
pub const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(30); // 30 seconds

/// Configuration for background cleanup task
#[derive(Clone)]
pub struct CleanupConfig {
    pub interval: Duration,
    pub enabled: bool,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_CLEANUP_INTERVAL,
            enabled: true,
        }
    }
}

/// Background cleanup task controller
struct CleanupTaskController {
    handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl CleanupTaskController {
    fn new() -> Self {
        Self {
            handle: None,
            shutdown_tx: None,
        }
    }

    fn start<T, M>(
        &mut self,
        connections: Arc<Mutex<VecDeque<InnerConnection<T>>>>,
        max_idle_time: Duration,
        cleanup_interval: Duration,
        manager: Arc<M>,
        max_size: usize,
    ) where
        T: Send + 'static,
        M: ConnectionManager<Connection = T> + Send + Sync + 'static,
    {
        if self.handle.is_some() {
            log::warn!("Cleanup task is already running");
            return;
        }
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);
        let handle = tokio::spawn(async move {
            let mut interval_timer = interval(cleanup_interval);
            log::info!("Background cleanup task started with interval: {cleanup_interval:?}");

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {}
                    _ = &mut shutdown_rx => {
                        log::info!("Received shutdown signal, exiting cleanup loop");
                        break;
                    }
                };

                let mut connections = connections.lock().await;
                let initial_count = connections.len();
                let now = Instant::now();

                // check both idle time and is_valid
                let mut valid_connections = VecDeque::new();
                for mut conn in connections.drain(..) {
                    let not_expired = now.duration_since(conn.created_at) < max_idle_time;
                    let is_valid = if not_expired {
                        manager.is_valid(&mut conn.connection).await
                    } else {
                        false
                    };
                    if not_expired && is_valid {
                        valid_connections.push_back(conn);
                    }
                }
                let removed_count = initial_count - valid_connections.len();
                *connections = valid_connections;

                if removed_count > 0 {
                    log::debug!("Background cleanup removed {removed_count} expired/invalid connections");
                }

                log::debug!("Current pool (remaining {}/{max_size}) after cleanup", connections.len());
            }
        });

        self.handle = Some(handle);
    }

    async fn stop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.await {
                log::error!("Error while stopping background cleanup task: {e}");
            }
            log::info!("Background cleanup task stopped in async stop");
        }
    }

    fn stop_sync(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            handle.abort();
            log::info!("Background cleanup task stopped in stop_sync");
        }
    }
}

impl Drop for CleanupTaskController {
    fn drop(&mut self) {
        self.stop_sync();
    }
}

/// Manager responsible for creating new [`ConnectionManager::Connection`]s or checking existing ones.
pub trait ConnectionManager: Sync + Send + Clone {
    /// Type of [`ConnectionManager::Connection`]s that this [`ConnectionManager`] creates and recycles.
    type Connection: Send;

    /// Error that this [`ConnectionManager`] can return when creating and/or recycling [`ConnectionManager::Connection`]s.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Future that resolves to a new [`ConnectionManager::Connection`] when created.
    type CreateFut: Future<Output = Result<Self::Connection, Self::Error>> + Send;

    /// Future that resolves to true if the connection is valid, false otherwise.
    type ValidFut<'a>: Future<Output = bool> + Send
    where
        Self: 'a;

    /// Create a new connection.
    fn create_connection(&self) -> Self::CreateFut;

    /// Check if a connection is valid.
    fn is_valid<'a>(&'a self, connection: &'a mut Self::Connection) -> Self::ValidFut<'a>;
}

/// Connection pool
pub struct ConnectionPool<M: ConnectionManager> {
    connections: Arc<Mutex<VecDeque<InnerConnection<M::Connection>>>>,
    semaphore: Arc<Semaphore>,
    max_size: usize,
    manager: M,
    max_idle_time: Duration,
    connection_timeout: Duration,
    cleanup_controller: Arc<Mutex<CleanupTaskController>>,
    outstanding_count: Arc<std::sync::atomic::AtomicUsize>,
}

/// Pooled inner connection, used within the connection pool
struct InnerConnection<T> {
    pub connection: T,
    pub created_at: Instant,
}

/// Pooled managed stream, provided for the outer world usage
pub struct ManagedConnection<M>
where
    M: ConnectionManager + Send + Sync + 'static,
{
    connection: Option<M::Connection>,
    pool: Arc<ConnectionPool<M>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<M: ConnectionManager> ManagedConnection<M> {
    /// Consume the managed connection and return the inner connection
    pub fn into_inner(mut self) -> M::Connection {
        self.connection.take().unwrap()
    }

    fn new(connection: M::Connection, pool: Arc<ConnectionPool<M>>, permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        pool.outstanding_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ManagedConnection {
            connection: Some(connection),
            pool,
            _permit: permit,
        }
    }
}

impl<M> ConnectionPool<M>
where
    M: ConnectionManager + Send + Sync + Clone + 'static,
{
    /// Create a new connection pool
    pub fn new(
        max_size: Option<usize>,
        max_idle_time: Option<Duration>,
        connection_timeout: Option<Duration>,
        cleanup_config: Option<CleanupConfig>,
        manager: M,
    ) -> Arc<Self> {
        let max_size = max_size.unwrap_or(DEFAULT_MAX_SIZE);
        let max_idle_time = max_idle_time.unwrap_or(DEFAULT_IDLE_TIMEOUT);
        let connection_timeout = connection_timeout.unwrap_or(DEFAULT_CONNECTION_TIMEOUT);
        let cleanup_config = cleanup_config.unwrap_or_default();

        log::info!(
            "Creating connection pool with max_size: {max_size}, idle_timeout: {max_idle_time:?}, connection_timeout: {connection_timeout:?}, cleanup_enabled: {}",
            cleanup_config.enabled
        );

        let connections = Arc::new(Mutex::new(VecDeque::new()));
        let cleanup_controller = Arc::new(Mutex::new(CleanupTaskController::new()));

        let pool = Arc::new(ConnectionPool {
            connections: connections.clone(),
            semaphore: Arc::new(Semaphore::new(max_size)),
            max_size,
            manager,
            max_idle_time,
            connection_timeout,
            cleanup_controller: cleanup_controller.clone(),
            outstanding_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });

        // Start background cleanup task if enabled
        if cleanup_config.enabled {
            let manager = Arc::new(pool.manager.clone());
            tokio::spawn(async move {
                let mut controller = cleanup_controller.lock().await;
                controller.start(connections, max_idle_time, cleanup_config.interval, manager, max_size);
            });
        }

        pool
    }

    /// Get a connection from the pool
    pub async fn get_connection(self: Arc<Self>) -> Result<ManagedConnection<M>, PoolError<M::Error>> {
        log::debug!("Attempting to get connection from pool");

        // Use semaphore to limit concurrent connections
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| PoolError::PoolClosed)?;

        // Try to get an existing connection from the pool
        {
            let mut connections = self.connections.lock().await;
            loop {
                let Some(mut pooled_conn) = connections.pop_front() else {
                    // No available connection, break the loop
                    break;
                };
                log::trace!("Found existing connection in pool, validating...");
                let age = Instant::now().duration_since(pooled_conn.created_at);
                let is_valid = if age < self.max_idle_time {
                    let r = self.manager.is_valid(&mut pooled_conn.connection).await;
                    if !r {
                        log::warn!("Connection validation failed, discarding invalid connection");
                    }
                    r
                } else {
                    log::debug!("Connection expired (age: {age:?}), discarding");
                    false
                };
                if is_valid {
                    let size = connections.len();
                    log::debug!("Reusing existing connection from pool (remaining: {size}/{})", self.max_size);
                    return Ok(ManagedConnection::new(pooled_conn.connection, self.clone(), permit));
                }
            }
        }

        log::trace!("No valid connection available, creating new connection...");
        // Create new connection
        match timeout(self.connection_timeout, self.manager.create_connection()).await {
            Ok(Ok(connection)) => {
                log::info!("Successfully created new connection");
                Ok(ManagedConnection::new(connection, self.clone(), permit))
            }
            Ok(Err(e)) => {
                log::error!("Failed to create new connection");
                Err(PoolError::Creation(e))
            }
            Err(_) => {
                log::warn!("Connection creation timed out after {:?}", self.connection_timeout);
                Err(PoolError::Timeout)
            }
        }
    }

    pub fn outstanding_count(&self) -> usize {
        self.outstanding_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn pool_size(&self) -> usize {
        self.connections.lock().await.len()
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Stop the background cleanup task
    pub async fn stop_cleanup_task(&self) {
        let mut controller = self.cleanup_controller.lock().await;
        controller.stop().await;
    }

    /// Restart the background cleanup task with new configuration
    pub async fn restart_cleanup_task(&self, cleanup_config: CleanupConfig) {
        self.stop_cleanup_task().await;

        if cleanup_config.enabled {
            let manager = Arc::new(self.manager.clone());
            let mut controller = self.cleanup_controller.lock().await;
            let m = self.max_size;
            controller.start(self.connections.clone(), self.max_idle_time, cleanup_config.interval, manager, m);
        }
    }
}

impl<M: ConnectionManager> ConnectionPool<M> {
    async fn recycle(&self, mut connection: M::Connection) {
        if !self.manager.is_valid(&mut connection).await {
            log::debug!("Invalid connection, dropping");
            return;
        }
        let mut connections = self.connections.lock().await;
        if connections.len() < self.max_size {
            connections.push_back(InnerConnection {
                connection,
                created_at: Instant::now(),
            });
            log::debug!("Connection recycled to pool (pool size: {}/{})", connections.len(), self.max_size);
        } else {
            log::debug!("Pool is full, dropping connection (pool max size: {})", self.max_size);
        }
        // If the pool is full, the connection will be dropped (automatically closed)
    }
}

impl<M: ConnectionManager> Drop for ManagedConnection<M> {
    fn drop(&mut self) {
        self.pool.outstanding_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(connection) = self.connection.take() {
            let pool = self.pool.clone();
            _ = tokio::spawn(async move {
                log::trace!("Recycling connection to pool on drop");
                pool.recycle(connection).await;
            });
        }
    }
}

// Generic implementations for AsRef and AsMut
impl<M: ConnectionManager> AsRef<M::Connection> for ManagedConnection<M> {
    fn as_ref(&self) -> &M::Connection {
        self.connection.as_ref().unwrap()
    }
}

impl<M: ConnectionManager> AsMut<M::Connection> for ManagedConnection<M> {
    fn as_mut(&mut self) -> &mut M::Connection {
        self.connection.as_mut().unwrap()
    }
}

// Implement Deref and DerefMut for PooledStream
impl<M: ConnectionManager> std::ops::Deref for ManagedConnection<M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl<M: ConnectionManager> std::ops::DerefMut for ManagedConnection<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}

/// Connection pool errors
#[derive(Debug)]
pub enum PoolError<E> {
    PoolClosed,
    Timeout,
    Creation(E),
}

impl<E: std::fmt::Display> std::fmt::Display for PoolError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::PoolClosed => write!(f, "Connection pool is closed"),
            PoolError::Timeout => write!(f, "Connection creation timeout"),
            PoolError::Creation(e) => write!(f, "Connection creation failed: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PoolError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PoolError::Creation(e) => Some(e),
            _ => None,
        }
    }
}
