#[macro_use]
extern crate async_trait;

mod configuration;
mod distribution;
mod generator;
mod run;
mod sharded_histogram;
mod workload;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use configuration::{scylla_bench, BenchDescription};

#[tokio::main]
async fn main() {
    let (bench_desc, bench_strategy) =
        scylla_bench::parse_scylla_bench_args(std::env::args()).unwrap();

    // TODO: Support parsing cassandra-stress style arguments
    // let config = Arc::new(BenchDescription {
    //     operation_count: 10_000_000,
    //     concurrency: NonZeroUsize::new(1024).unwrap(),
    //     // rate_limit_per_second: Some(NonZeroU64::new(10000).unwrap()),
    //     rate_limit_per_second: None,
    //     nodes: vec!["127.0.0.1:9042".to_owned()],
    // });

    run::run(bench_desc, bench_strategy).await.unwrap();
}
