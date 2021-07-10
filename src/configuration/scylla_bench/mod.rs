mod goflags;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use anyhow::Result;
use scylla::frame::types::Consistency;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::Session;

use crate::configuration::{BenchDescription, BenchOp, BenchStrategy};
use crate::distribution::{self, Distribution, DistributionContext, FixedDistribution};

use goflags::{FlagValue, GoFlagSet};

pub fn parse_scylla_bench_args(
    mut args: impl Iterator<Item = String>,
) -> Result<(Arc<BenchDescription>, Arc<dyn BenchStrategy>)> {
    // TODO: Implement all flags!

    let mut flag = GoFlagSet::new();

    let mode: FlagValue<Option<Mode>> = flag.var(
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
    let keyspace_name = flag.string_var("keyspace", "scylla_bench", "keyspace to use");
    let table_name = flag.string_var("table", "test", "table to use");
    // username
    // password

    // tlsEncryption
    // serverName
    // hostVerification
    // caCertFile
    // clientCertFile
    // clientKeyFile

    // hostSelectionPolicy

    // Skip the first arg
    args.next();
    flag.parse_args(args)?;

    let desc = Arc::new(BenchDescription {
        operation_count: partition_count.get() * clustering_row_count.get(),
        concurrency: NonZeroUsize::new(concurrency.get() as usize).unwrap(), // TODO: Fix unwrap
        rate_limit_per_second: NonZeroU64::new(maximum_rate.get()),
        nodes: nodes.get().split(',').map(str::to_owned).collect(),
    });

    let strategy = Arc::new(ScyllaBenchStrategy {
        keyspace: keyspace_name.get(),
        table: table_name.get(),
        replication_factor: replication_factor.get(),

        iterations: iterations.get(),
        partition_count: partition_count.get(),
        clustering_row_count: clustering_row_count.get(),

        clustering_row_size_dist: clustering_row_size_dist.get(),

        mode: mode.get().unwrap(),
    });

    Ok((desc, strategy))
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
enum Mode {
    Write,
    Read,
    CounterUpdate,
    CounterRead,
    Scan,
}

impl std::str::FromStr for Mode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "write" => Ok(Mode::Write),
            "read" => Ok(Mode::Read),
            "counter_update" => Ok(Mode::CounterUpdate),
            "counter_read" => Ok(Mode::CounterRead),
            "scan" => Ok(Mode::Scan),
            _ => Err(anyhow::anyhow!("Invalid mode: {:?}", s)),
        }
    }
}

struct ScyllaBenchStrategy {
    keyspace: String,
    table: String,
    replication_factor: u64, // TODO: NonZeroUsize?

    iterations: u64,
    partition_count: u64,
    clustering_row_count: u64,

    clustering_row_size_dist: Arc<dyn Distribution>,

    mode: Mode,
}

#[async_trait]
impl BenchStrategy for ScyllaBenchStrategy {
    async fn prepare(&self, session: Arc<Session>) -> Result<Arc<dyn BenchOp>> {
        let create_keyspace_query = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {}}}",
            &self.keyspace, &self.replication_factor,
        );
        session.query(create_keyspace_query, ()).await?;
        session.await_schema_agreement().await?;

        let create_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (pk bigint, ck bigint, v blob, PRIMARY KEY(pk, ck)) WITH compression = {{ }}",
            &self.keyspace, &self.table,
        );
        session.query(create_table_query, ()).await?;
        session.await_schema_agreement().await?;

        let insert_statement = format!(
            "INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)",
            &self.keyspace, &self.table,
        );
        let statement = session.prepare(insert_statement).await?;

        let workload = SequentialWorkload {
            iterations: self.iterations,
            pks: self.partition_count,
            cks_per_call: 1, // TODO: Support batches
            calls_per_pk: self.clustering_row_count,
        };
        let workload = Box::new(workload);

        // TODO: Support other workload modes
        let op = WriteOp {
            session,
            statement,
            workload,
            clustering_row_size_dist: self.clustering_row_size_dist.clone(),
        };
        let op = Arc::new(op);

        Ok(op)
    }
}

struct WriteOp {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: Box<dyn Workload>,
    clustering_row_size_dist: Arc<dyn Distribution>,
}

#[async_trait]
impl BenchOp for WriteOp {
    async fn execute(&self, mut ctx: DistributionContext) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        if cks.len() == 1 {
            let clustering_row_len = self.clustering_row_size_dist.get_u64(&mut ctx) as usize;
            let data = generate_row_data(pk, cks[0], clustering_row_len);
            self.session
                .execute(&self.statement, (pk, cks[0], data))
                .await?;
        } else {
            // TODO: Support batches
            unimplemented!()
        }

        Ok(true)
    }
}

trait Workload: Sync + Send {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)>;
}

struct SequentialWorkload {
    iterations: u64,
    pks: u64,
    cks_per_call: u64,
    calls_per_pk: u64,
}

impl Workload for SequentialWorkload {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)> {
        let seq = ctx.get_seq();
        let pk = seq / self.calls_per_pk;
        let ck_base = (seq % self.calls_per_pk) * self.cks_per_call;

        if pk >= self.iterations * self.pks {
            return None;
        }

        let pk = pk % self.pks;
        let v = (ck_base..ck_base + self.cks_per_call)
            .into_iter()
            .map(|x| x as i64)
            .collect();
        Some((pk as i64, v))
    }
}

// This algorithm has some differences from the scylla-bench's GenerateData:
// - It always returns a result whose length == `size`
// - It uses crc32 instead of sha256 for checksum - this is a benchmark,
//   not a bitcoin miner :)
// TODO: Implement the checksum!
fn generate_row_data(pk: i64, ck: i64, size: usize) -> Vec<u8> {
    const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
    const FULL_HEADER_SIZE: usize = 24;
    const FULL_HEADER_WITH_CHECKSUM_SIZE: usize = FULL_HEADER_SIZE + CHECKSUM_SIZE + 1;

    use bytes::BufMut;

    let mut v = Vec::new();

    if size <= FULL_HEADER_SIZE {
        v.reserve(std::cmp::max(FULL_HEADER_SIZE, size));
        v.put_u8(size as u8);
        v.put_i64_le(pk ^ ck);
    } else {
        v.reserve(std::cmp::max(FULL_HEADER_WITH_CHECKSUM_SIZE, size));
        v.put_u64_le(size as u64);
        v.put_i64_le(pk);
        v.put_i64_le(ck);
    }

    // TODO: Implement writing random data and checksumming it
    v.resize(size, 0);
    v
}

// TODO: generated data validation?
