// mod cassandra_stress;
pub mod scylla_bench;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

use anyhow::Result;
use openssl::ssl::SslContext;

use scylla::Session;

use crate::distribution::DistributionContext;

pub struct BenchDescription {
    // pub mode: WorkloadMode,

    // pub typ: WorkloadType,
    pub nodes: Vec<String>,

    // pub consistency: Consistency,
    pub duration: Option<Duration>,
    pub credentials: Option<(String, String)>,
    pub compression: Option<scylla::transport::Compression>,
    pub ssl_context: Option<SslContext>,
    pub load_balancing_policy: Arc<dyn scylla::load_balancing::LoadBalancingPolicy>,
    pub concurrency: NonZeroUsize,
    pub rate_limit_per_second: Option<NonZeroU64>,
}

#[async_trait]
pub trait BenchStrategy {
    async fn prepare(&self, session: Arc<Session>) -> Result<Arc<dyn BenchOp>>;
}

#[async_trait]
pub trait BenchOp: Send + Sync {
    // TODO: Use std::ops::ControlFlow if it gets stabilized
    async fn execute(&self, ctx: DistributionContext, rows_processed: &AtomicU64) -> Result<bool>;
}
