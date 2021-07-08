// mod cassandra_stress;
pub mod scylla_bench;

use std::num::{NonZeroU64, NonZeroUsize};

use scylla::frame::types::Consistency;

pub struct BenchDescription {
    // pub mode: WorkloadMode,

    // pub typ: WorkloadType,
    pub nodes: Vec<String>,

    // pub consistency: Consistency,
    pub operation_count: u64,
    pub concurrency: NonZeroUsize,
    pub rate_limit_per_second: Option<NonZeroU64>,
}
