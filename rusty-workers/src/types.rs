use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct WorkerConfiguration {
    pub executor: ExecutorConfiguration,
    pub fetch_service: SocketAddr,
    pub env: BTreeMap<String, String>,
    pub kv_namespaces: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ExecutorConfiguration {
    pub max_ab_memory_mb: u32,
    pub max_time_ms: u32,
    pub max_io_concurrency: u32,
    pub max_io_per_request: u32,

    #[serde(default)]
    pub max_detached_cpu_time_ms: u32,

    #[serde(default)]
    pub max_detached_wall_time_ms: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkerHandle {
    pub id: String,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct RequestObject {
    pub headers: BTreeMap<String, Vec<String>>,
    pub method: String,
    pub url: String,

    #[serde(default)]
    pub body: HttpBody,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct ResponseObject {
    pub headers: BTreeMap<String, Vec<String>>,
    pub status: u16,

    #[serde(default)]
    pub body: HttpBody,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HttpBody {
    Binary(Vec<u8>),
}

impl Default for HttpBody {
    fn default() -> Self {
        Self::Binary(vec![])
    }
}

/// An error that happens during worker execution.
#[derive(Serialize, Deserialize, Clone, Debug, Error)]
pub enum ExecutionError {
    /// The requested worker instance does not exist.
    ///
    /// Non-deterministic - maybe removed by LRU policy.
    #[error("no such worker")]
    NoSuchWorker,

    /// An unknown exception is thrown by the runtime.
    ///
    /// Worker terminated.
    #[error("runtime throws exception")]
    RuntimeThrowsException,

    /// Time limit exceeded.
    ///
    /// Worker terminated.
    #[error("time limit exceeded")]
    TimeLimitExceeded,

    /// Memory limit exceeded.
    ///
    /// Worker terminated.
    #[error("memory limit exceeded")]
    MemoryLimitExceeded,

    /// An I/O operation timed out.
    ///
    /// Worker terminated.
    #[error("I/O timed out")]
    IoTimeout,

    /// An exception is thrown by the script during task execution.
    ///
    /// This does not terminate the worker, and the same `WorkerHandle` is still valid.
    #[error("script throws exception")]
    ScriptThrowsException(String),
}
pub type ExecutionResult<T> = Result<T, ExecutionError>;

impl ExecutionError {
    pub fn terminates_worker(&self) -> bool {
        match self {
            ExecutionError::NoSuchWorker
            | ExecutionError::RuntimeThrowsException
            | ExecutionError::TimeLimitExceeded
            | ExecutionError::MemoryLimitExceeded
            | ExecutionError::IoTimeout => true,
            ExecutionError::ScriptThrowsException(_) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Error)]
pub enum GenericError {
    #[error("io: {0}")]
    Io(String),

    #[error("db: {0}")]
    Database(String),

    #[error("executor: {0}")]
    Executor(String),

    #[error("v8 unknown error")]
    V8Unknown,

    #[error("execution error: {0:?}")]
    Execution(ExecutionError),

    /// An exception is thrown by the script during worker initialization.
    #[error("script initialization exception")]
    ScriptInitException(String),

    /// I/O limit is exceeded.
    ///
    /// Worker terminated.
    #[error("I/O limit exceeded")]
    IoLimitExceeded,

    #[error("try again")]
    TryAgain,

    #[error("type conversion failed")]
    Conversion,

    #[error("type checking failed - expected {expected}")]
    Typeck { expected: String },

    #[error("other: {0}")]
    Other(String),
}

pub type GenericResult<T> = Result<T, GenericError>;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeId(pub String);

impl From<std::io::Error> for GenericError {
    fn from(other: std::io::Error) -> Self {
        Self::Io(format!("{:?}", other))
    }
}

impl From<mysql_async::Error> for GenericError {
    fn from(other: mysql_async::Error) -> Self {
        Self::Database(format!("{:?}", other))
    }
}

impl From<serde_json::Error> for GenericError {
    fn from(other: serde_json::Error) -> Self {
        Self::Database(format!("{:?}", other))
    }
}

impl WorkerHandle {
    pub fn generate() -> Self {
        Self {
            id: crate::util::rand_hex(16),
        }
    }
}

impl RuntimeId {
    pub fn generate() -> Self {
        Self(crate::util::rand_hex(16))
    }
}
