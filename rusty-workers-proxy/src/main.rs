#[macro_use]
extern crate log;

mod config;
mod sched;

use anyhow::Result;
use once_cell::sync::OnceCell;
use rusty_workers::types::*;
use std::net::SocketAddr;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::net::lookup_host;

use crate::config::*;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use sched::SchedError;

static SCHEDULER: OnceCell<Arc<sched::Scheduler>> = OnceCell::new();

#[derive(Debug, StructOpt)]
#[structopt(name = "rusty-workers-proxy", about = "Rusty Workers (frontend proxy)")]
struct Opt {
    /// HTTP listen address.
    #[structopt(short = "l", long)]
    http_listen: SocketAddr,

    #[structopt(long, env = "RW_FETCH_SERVICE")]
    fetch_service: String,

    /// Runtime service backends, comma-separated.
    #[structopt(long, env = "RUNTIMES")]
    runtimes: String,

    /// Max ArrayBuffer memory per worker, in MB
    #[structopt(long, env = "RW_MAX_AB_MEMORY_MB", default_value = "16")]
    max_ab_memory_mb: u32,

    /// Max CPU time, in milliseconds
    #[structopt(long, env = "RW_MAX_TIME_MS", default_value = "100")]
    max_time_ms: u32,

    /// Max number of concurrent I/O operations
    #[structopt(long, env = "RW_MAX_IO_CONCURRENCY", default_value = "10")]
    max_io_concurrency: u32,

    /// Max number of I/O operations per request
    #[structopt(long, env = "RW_MAX_IO_PER_REQUEST", default_value = "50")]
    max_io_per_request: u32,

    /// Max ready instances per app
    #[structopt(long, env = "RW_MAX_READY_INSTANCES_PER_APP", default_value = "50")]
    max_ready_instances_per_app: usize,

    /// Expiration time for ready instances
    #[structopt(
        long,
        env = "RW_READY_INSTANCE_EXPIRATION_MS",
        default_value = "120000"
    )]
    ready_instance_expiration_ms: u64,

    /// Request timeout in milliseconds.
    #[structopt(long, env = "RW_REQUEST_TIMEOUT_MS", default_value = "30000")]
    pub request_timeout_ms: u64,

    /// Max request body size in bytes.
    #[structopt(
        long,
        env = "RW_MAX_REQUEST_BODY_SIZE_BYTES",
        default_value = "2097152"
    )]
    pub max_request_body_size_bytes: u64,

    /// Probability of an instance being dropped out after a request. Valid values are 0 to 1.
    #[structopt(long, env = "RW_DROPOUT_RATE", default_value = "0.001")]
    pub dropout_rate: f32,

    /// Routing cache size.
    #[structopt(long, env = "RW_ROUTE_CACHE_SIZE", default_value = "1000")]
    pub route_cache_size: usize,

    /// MySQL-compatible database URL.
    #[structopt(long, env = "RW_DB_URL")]
    pub db_url: String,

    /// Size of app cache.
    #[structopt(long, env = "RW_APP_CACHE_SIZE", default_value = "100")]
    pub app_cache_size: usize,

    /// Max CPU time for detached execution.
    #[structopt(long, env = "RW_MAX_DETACHED_CPU_TIME_MS", default_value = "100")]
    pub max_detached_cpu_time_ms: u32,

    /// Max wall time for detached execution.
    #[structopt(long, env = "RW_MAX_DETACHED_WALL_TIME_MS", default_value = "15000")]
    pub max_detached_wall_time_ms: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();
    rusty_workers::init();

    let opt = Opt::from_args();

    let mut runtime_cluster: Vec<SocketAddr> = Vec::new();
    for elem in opt.runtimes.split(",") {
        let runtime_addr = lookup_host(elem)
            .await?
            .next()
            .unwrap_or_else(|| panic!("runtime address lookup failed: {}", elem));
        runtime_cluster.push(runtime_addr);
    }

    let kv_client = rusty_workers::db::DataClient::new(&opt.db_url).await?;

    let fetch_service = lookup_host(&opt.fetch_service)
        .await?
        .next()
        .expect("fetch service unresolved");

    SCHEDULER
        .set(sched::Scheduler::new(
            WorkerConfiguration {
                executor: ExecutorConfiguration {
                    max_ab_memory_mb: opt.max_ab_memory_mb,
                    max_time_ms: opt.max_time_ms,
                    max_io_concurrency: opt.max_io_concurrency,
                    max_io_per_request: opt.max_io_per_request,
                    max_detached_cpu_time_ms: opt.max_detached_cpu_time_ms,
                    max_detached_wall_time_ms: opt.max_detached_wall_time_ms,
                },
                fetch_service,
                env: Default::default(),
                kv_namespaces: Default::default(),
            },
            LocalConfig {
                max_ready_instances_per_app: opt.max_ready_instances_per_app,
                ready_instance_expiration_ms: opt.ready_instance_expiration_ms,
                request_timeout_ms: opt.request_timeout_ms,
                max_request_body_size_bytes: opt.max_request_body_size_bytes,
                dropout_rate: opt.dropout_rate,
                route_cache_size: opt.route_cache_size,
                app_cache_size: opt.app_cache_size,
                runtime_cluster,
            },
            kv_client,
        ))
        .unwrap_or_else(|_| panic!("cannot set scheduler"));

    tokio::spawn(async move {
        loop {
            let scheduler = SCHEDULER.get().unwrap();
            scheduler.discover_runtimes().await;
            scheduler.query_runtimes().await;
            tokio::time::sleep(std::time::Duration::from_secs(7)).await;
        }
    });

    let make_svc = make_service_fn(|_| async move {
        Ok::<_, hyper::Error>(service_fn(|req| async move {
            let scheduler = SCHEDULER.get().unwrap();
            match scheduler.handle_request(req).await {
                Ok(x) => Ok::<_, hyper::Error>(x),
                Err(e) => {
                    debug!("handle_request failed: {:?}", e);
                    let res = match e.downcast::<SchedError>() {
                        Ok(e) => e.build_response(),
                        Err(_) => {
                            let mut res = Response::new(Body::from("internal server error"));
                            *res.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                            res
                        }
                    };
                    Ok::<_, hyper::Error>(res)
                }
            }
        }))
    });
    info!("starting http server");

    Server::bind(&opt.http_listen).serve(make_svc).await?;
    Ok(())
}
