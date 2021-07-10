mod goflags;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use anyhow::Result;
use scylla::frame::types::Consistency;

use crate::configuration::BenchDescription;
use crate::distribution::{self, Distribution, FixedDistribution};

use goflags::{FlagValue, GoFlagSet};

pub fn parse_scylla_bench_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Arc<BenchDescription>> {
    // Skip the first arg
    args.next();

    // TODO: Implement all flags!

    let mut flag = GoFlagSet::new();

    let mode: FlagValue<Option<WorkloadMode>> = flag.var(
        "mode",
        None,
        "operating mode: write, read, counter_update, counter_read, scan",
        |s| Ok(Some(s.parse()?)),
    );
    let workload = flag.string_var("workload", "", "workload: sequential, uniform, timeseries");
    let consistency_level = flag.var(
        "consistency-level",
        Consistency::Quorum,
        "consistency level",
        parse_consistency,
    );
    let replication_factor = flag.u64_var("replication-factor", 1, "replication factor");
    // timeout

    let nodes = flag.string_var("nodes", "127.0.0.1:9042", "nodes");
    // clientCompression
    let concurrency = flag.u64_var("concurrency", 16, "maximum concurrent requests");
    // connectionCount
    let maximum_rate = flag.u64_var(
        "max-rate",
        0,
        "the maximum rate of outbound requests in op/s (0 for unlimited)",
    );
    // pageSize

    let partition_count = flag.u64_var("partition-count", 1000, "number of partitions");
    let clustering_row_count = flag.u64_var(
        "clustering-row-count",
        100,
        "number of clustering rows in a partition",
    );
    let default_dist: Arc<dyn Distribution> = Arc::new(FixedDistribution(4));
    let clustering_row_size_dist = flag.var(
        "clustering-row-size-dist",
        default_dist,
        "size of a single clustering row, can use random values",
        |s| Ok(distribution::parse_distribution(s)?.into()),
    );

    // rowsPerRequest
    // provideUpperBound
    // inRestriction
    // noLowerBound
    // rangeCount

    let test_duration = flag.i64_var(
        "duration",
        0,
        "duration of the test in seconds (0 for unlimited)",
    );
    let iterations = flag.u64_var(
        "iterations",
        1,
        "number of iterations to run (0 for unlimited, relevant only for workloads that have a defined number of ops to execute)",
    );

    // partitionOffset

    // measureLatency
    // validateData

    // writeRate
    // startTimestamp
    // distribution

    // keyspaceName
    // tableName
    // username
    // password

    // tlsEncryption
    // serverName
    // hostVerification
    // caCertFile
    // clientCertFile
    // clientKeyFile

    // hostSelectionPolicy

    flag.parse_args(args)?;

    let ret = Arc::new(BenchDescription {
        operation_count: partition_count.get() * clustering_row_count.get(),
        concurrency: NonZeroUsize::new(concurrency.get() as usize).unwrap(), // TODO: Fix unwrap
        rate_limit_per_second: NonZeroU64::new(maximum_rate.get()),
        nodes: nodes.get().split(',').map(str::to_owned).collect(),
    });

    Ok(ret)
}

fn parse_consistency(s: &str) -> Result<Consistency> {
    match s {
        "any" => Ok(Consistency::Any),
        "one" => Ok(Consistency::One),
        "two" => Ok(Consistency::Two),
        "three" => Ok(Consistency::Three),
        "quorum" => Ok(Consistency::Quorum),
        "all" => Ok(Consistency::All),
        "local_quorum" => Ok(Consistency::LocalQuorum),
        "each_quorum" => Ok(Consistency::EachQuorum),
        "local_one" => Ok(Consistency::LocalOne),
        _ => Err(anyhow::anyhow!("Invalid consistency: {:?}", s)),
    }
}

#[derive(Copy, Clone)]
pub enum WorkloadMode {
    Write,
    Read,
    CounterUpdate,
    CounterRead,
    Scan,
}

impl std::str::FromStr for WorkloadMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "write" => Ok(WorkloadMode::Write),
            "read" => Ok(WorkloadMode::Read),
            "counter_update" => Ok(WorkloadMode::CounterUpdate),
            "counter_read" => Ok(WorkloadMode::CounterRead),
            "scan" => Ok(WorkloadMode::Scan),
            _ => Err(anyhow::anyhow!("Invalid workload mode: {:?}", s)),
        }
    }
}
