mod goflags;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand_distr::{Distribution as _, Standard, StandardNormal, Uniform};
use scylla::frame::types::Consistency;
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::transport::Compression;
use scylla::Session;

use crate::configuration::{BenchDescription, BenchOp, BenchStrategy};
use crate::distribution::{self, Distribution, DistributionContext, FixedDistribution};

use goflags::{FlagValue, GoFlagSet};

pub fn parse_scylla_bench_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<(Arc<BenchDescription>, Arc<dyn BenchStrategy>)>> {
    // TODO: Implement all flags!

    let mut flag = GoFlagSet::new();

    let mode: FlagValue<Option<Mode>> = flag.var(
        "mode",
        None,
        "operating mode: write, read, counter_update, counter_read, scan",
        |s| Ok(Some(s.parse()?)),
    );
    let workload_type: FlagValue<Option<WorkloadType>> = flag.var(
        "workload",
        None,
        "workload: sequential, uniform, timeseries",
        |s| Ok(Some(s.parse()?)),
    );
    let consistency_level = flag.var(
        "consistency-level",
        Consistency::Quorum,
        "consistency level",
        parse_consistency,
    );
    let replication_factor = flag.u64_var("replication-factor", 1, "replication factor");
    // timeout

    let nodes = flag.string_var("nodes", "127.0.0.1:9042", "cluster contact nodes");
    let client_compression = flag.bool_var(
        "client-compression",
        true,
        "use snappy compression for client-coordinator communication",
    );
    let concurrency = flag.u64_var("concurrency", 16, "maximum concurrent requests");
    let _ = flag.u64_var(
        "connection-count",
        4,
        "number of connections to use (ignored, the driver establishes one connection per shard)",
    );
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
        "clustering-row-size",
        default_dist,
        "size of a single clustering row, can use random values",
        |s| Ok(distribution::parse_distribution(s)?.into()),
    );

    // rowsPerRequest
    // provideUpperBound
    // inRestriction
    // noLowerBound
    // rangeCount

    let test_duration = flag.duration_var(
        "duration",
        Duration::ZERO,
        "duration of the test (0 for unlimited)",
    );
    let iterations = flag.u64_var(
        "iterations",
        1,
        "number of iterations to run (0 for unlimited, relevant only for workloads that have a defined number of ops to execute)",
    );

    // partitionOffset

    let _ = flag.bool_var(
        "measure-latency",
        true,
        "measure request latency (ignored, the latency is always measured)",
    );
    // validateData

    // writeRate
    // startTimestamp
    // distribution

    let keyspace_name = flag.string_var("keyspace", "scylla_bench", "keyspace to use");
    let table_name = flag.string_var("table", "test", "table to use");
    let username = flag.string_var("username", "", "cql username for authentication");
    let password = flag.string_var("password", "", "cql password for authentication");

    let show_help = flag.bool_var("help", false, "show this help message and exit");

    // tlsEncryption
    // serverName
    // hostVerification
    // caCertFile
    // clientCertFile
    // clientKeyFile

    // hostSelectionPolicy

    // Skip the first arg
    let prog_name = args.next().unwrap();
    flag.parse_args(args)?;

    if show_help.get() {
        flag.print_help(&prog_name);
        return Ok(None);
    }

    let mode = mode
        .get()
        .ok_or_else(|| anyhow::anyhow!("Test mode needs to be specified"))?;

    let workload_type = workload_type
        .get()
        .ok_or_else(|| anyhow::anyhow!("Workload needs to be specified"))?;

    let duration = if test_duration.get() == Duration::ZERO {
        None
    } else {
        Some(test_duration.get()) // TODO: Handle underflow
    };

    let credentials = if username.get() != "" && password.get() != "" {
        Some((username.get(), password.get()))
    } else {
        None
    };

    let compression = if client_compression.get() {
        Some(Compression::Snappy)
    } else {
        None
    };

    let desc = Arc::new(BenchDescription {
        nodes: nodes.get().split(',').map(str::to_owned).collect(),
        duration,
        credentials,
        compression,
        concurrency: NonZeroUsize::new(concurrency.get() as usize).unwrap(), // TODO: Fix unwrap
        rate_limit_per_second: NonZeroU64::new(maximum_rate.get()),
    });

    let strategy = Arc::new(ScyllaBenchStrategy {
        keyspace: keyspace_name.get(),
        table: table_name.get(),
        replication_factor: replication_factor.get(),
        consistency_level: consistency_level.get(),

        iterations: iterations.get(),
        partition_count: partition_count.get(),
        clustering_row_count: clustering_row_count.get(),

        clustering_row_size_dist: clustering_row_size_dist.get(),

        max_rate: NonZeroU64::new(maximum_rate.get()),

        mode,
        workload_type,
    });

    Ok(Some((desc, strategy)))
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

#[derive(Copy, Clone)]
enum WorkloadType {
    Sequential,
    Uniform,
    TimeSeries,
    Scan,
}

impl std::str::FromStr for WorkloadType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sequential" => Ok(WorkloadType::Sequential),
            "uniform" => Ok(WorkloadType::Uniform),
            "timeseries" => Ok(WorkloadType::TimeSeries),
            "scan" => Ok(WorkloadType::Scan),
            _ => Err(anyhow::anyhow!("Invalid workload type: {:?}", s)),
        }
    }
}

struct ScyllaBenchStrategy {
    keyspace: String,
    table: String,
    replication_factor: u64, // TODO: NonZeroUsize?
    consistency_level: Consistency,

    iterations: u64,
    partition_count: u64,
    clustering_row_count: u64,

    clustering_row_size_dist: Arc<dyn Distribution>,

    max_rate: Option<NonZeroU64>,

    mode: Mode,
    workload_type: WorkloadType,
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
        let mut statement = session.prepare(insert_statement).await?;
        statement.set_consistency(self.consistency_level);
        statement.set_is_idempotent(true);

        let workload: Box<dyn Workload> = match self.workload_type {
            WorkloadType::Sequential => Box::new(SequentialWorkload {
                iterations: self.iterations,
                pks: self.partition_count,
                cks_per_call: 1, // TODO: Support batches
                calls_per_pk: self.clustering_row_count,
            }),

            WorkloadType::Uniform => Box::new(UniformWorkload {
                pk_distribution: Uniform::new(0, self.partition_count as i64),
                ck_distribution: Uniform::new(0, self.clustering_row_count as i64),
                cks_per_call: 1, // TODO: Support batches
            }),

            WorkloadType::TimeSeries => Box::new(TimeSeriesWriteWorkload {
                cks_per_pk: self.clustering_row_count,
                writes_per_generation: self.partition_count * self.clustering_row_count,
                start_time: 0, // TODO: Support start-time
                period: (1_000_000 * self.partition_count) / self.max_rate.unwrap(),
            }),

            _ => todo!("Scan workload is not implemented"),
        };

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

impl WriteOp {
    fn new(
        session: Arc<Session>,
        keyspace: &str,
        table: &str,
        consistency_level: Consistency,
        workload: Box<dyn Workload>,
        clustering_row_size_dist: Arc<dyn Distribution>,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let insert_statement = format!(
            "INSERT INTO {}.{} (pk, ck, v) VALUES (?, ?, ?)",
            keyspace, table,
        );
        async move {
            let mut statement = session.prepare(insert_statement).await?;
            statement.set_consistency(consistency_level);
            statement.set_is_idempotent(true);

            Ok(Self {
                session,
                statement,
                workload,
                clustering_row_size_dist,
            })
        }
    }
}

#[async_trait]
impl BenchOp for WriteOp {
    async fn execute(&self, mut ctx: DistributionContext) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        debug_assert!(!cks.is_empty());
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

struct CounterUpdateOp {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: Box<dyn Workload>,
}

impl CounterUpdateOp {
    fn new(
        session: Arc<Session>,
        keyspace: &str,
        table: &str,
        consistency_level: Consistency,
        workload: Box<dyn Workload>,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let insert_statement = format!(
            "UPDATE {}.{} SET c1 = c1 + ?, c2 = c2 + ?, c3 = c3 + ?, c4 = c4 + ?, c5 = c5 + ? WHERE pk = ? AND ck = ?",
            keyspace, table,
        );
        async move {
            let mut statement = session.prepare(insert_statement).await?;
            statement.set_consistency(consistency_level);
            statement.set_is_idempotent(true);

            Ok(Self {
                session,
                statement,
                workload,
            })
        }
    }
}

#[async_trait]
impl BenchOp for CounterUpdateOp {
    async fn execute(&self, mut ctx: DistributionContext) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        debug_assert!(!cks.is_empty());
        if cks.len() == 1 {
            let ck = cks[0];
            self.session
                .execute(
                    &self.statement,
                    (ck, ck + 1, ck + 2, ck + 3, ck + 4, pk, ck),
                )
                .await?;
        } else {
            // TODO: Support batches
            unimplemented!()
        }

        Ok(true)
    }
}

#[derive(Copy, Clone)]
enum ReadRestrictionKind {
    InRestriction,
    BothBounds,
    OnlyLowerBound { limit: u64 },
    NoBounds { limit: u64 },
}

impl ReadRestrictionKind {
    fn to_query_string(&self, rows_per_request: NonZeroU64) -> String {
        match self {
            ReadRestrictionKind::InRestriction => {
                let mut ins = String::with_capacity(rows_per_request.get() as usize * 3 - 2);
                ins.push('?');
                for _ in 1..rows_per_request.get() {
                    ins.push_str(", ?");
                }
                format!("AND ck IN ({})", ins)
            }

            ReadRestrictionKind::BothBounds => "AND ck >= ? AND ck < ?".to_owned(),
            ReadRestrictionKind::OnlyLowerBound { limit } => {
                format!("AND ck >= ? LIMIT {}", limit)
            }
            ReadRestrictionKind::NoBounds { limit } => {
                format!("LIMIT {}", limit)
            }
        }
    }
}

struct ReadOp {
    session: Arc<Session>,
    statement: PreparedStatement,
    workload: Box<dyn Workload>,
    rows_per_request: NonZeroU64,

    read_restriction: ReadRestrictionKind,
    is_counter: bool,
}

impl ReadOp {
    // The function cannot be async due to this bug:
    // https://github.com/rust-lang/rust/issues/63033
    fn new(
        session: Arc<Session>,
        keyspace: &str,
        table: &str,
        consistency_level: Consistency,
        workload: Box<dyn Workload>,
        rows_per_request: NonZeroU64,
        read_restriction: ReadRestrictionKind,
        is_counter: bool,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let ck_restriction = read_restriction.to_query_string(rows_per_request);

        let statement_text = if !is_counter {
            format!(
                "SELECT ck, v FROM {}.{} WHERE pk = ? {}",
                keyspace, table, ck_restriction,
            )
        } else {
            format!(
                "SELECT ck, c1, c2, c3, c4, c5 FROM {}.{} WHERE pk = ? {}",
                keyspace, table, ck_restriction,
            )
        };

        async move {
            let mut statement = session.prepare(statement_text).await?;
            statement.set_consistency(consistency_level);
            statement.set_is_idempotent(true);
            Ok(ReadOp {
                session,
                statement,
                workload,
                rows_per_request,
                read_restriction,
                is_counter,
            })
        }
    }
}

#[async_trait]
impl BenchOp for ReadOp {
    async fn execute(&self, mut ctx: DistributionContext) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        // TODO: Consider executing with iterator
        let result = match self.read_restriction {
            ReadRestrictionKind::InRestriction => {
                use scylla::frame::value::SerializedValues;
                let col_size = std::mem::size_of::<i64>();
                let mut s = SerializedValues::with_capacity(col_size * (1 + cks.len()));
                s.add_value(&pk)?;
                for x in cks {
                    s.add_value(&x)?;
                }
                self.session.execute(&self.statement, s).await?
            }
            ReadRestrictionKind::BothBounds => {
                debug_assert_eq!(cks.len(), 1);
                self.session
                    .execute(
                        &self.statement,
                        (pk, cks[0], cks[0] + self.rows_per_request.get() as i64),
                    )
                    .await?
            }
            ReadRestrictionKind::OnlyLowerBound { .. } => {
                debug_assert_eq!(cks.len(), 1);
                self.session.execute(&self.statement, (pk, cks[0])).await?
            }
            ReadRestrictionKind::NoBounds { .. } => {
                debug_assert_eq!(cks.len(), 0);
                self.session.execute(&self.statement, (pk,)).await?
            }
        };

        use scylla::IntoTypedRows;

        if !self.is_counter {
            let rows = result
                .rows
                .ok_or_else(|| anyhow::anyhow!("Expected rows in response, but got nothing"))?
                .into_typed::<(i64, Vec<u8>)>();

            for r in rows {
                let (ck, v) = r?;
                if let Err(err) = validate_row_data(pk, ck, &v) {
                    // TODO: Tracing?
                    println!("{:?}", err);
                }
            }
        } else {
            let rows = result
                .rows
                .ok_or_else(|| anyhow::anyhow!("Expected rows in response, but got nothing"))?
                .into_typed::<(i64, i64, i64, i64, i64, i64)>();

            for r in rows {
                let (ck, c1, c2, c3, c4, c5) = r?;
                if let Err(err) = validate_counters(pk, ck, c1, c2, c3, c4, c5) {
                    // TODO: Tracing?
                    println!("{:?}", err);
                }
            }
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

struct UniformWorkload {
    pk_distribution: Uniform<i64>,
    ck_distribution: Uniform<i64>,
    cks_per_call: u64,
}

impl Workload for UniformWorkload {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)> {
        let pk = self.pk_distribution.sample(ctx.get_gen_mut());
        let mut v = Vec::with_capacity(self.cks_per_call as usize);
        for _ in 0..self.cks_per_call {
            v.push(self.ck_distribution.sample(ctx.get_gen_mut()));
        }
        Some((pk, v))
    }
}

struct TimeSeriesWriteWorkload {
    cks_per_pk: u64,
    writes_per_generation: u64,
    start_time: u64, // Nanos since unix epoch
    period: u64,     // Nanos
}

impl Workload for TimeSeriesWriteWorkload {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)> {
        let seq = ctx.get_seq();
        let pk_generation = seq / self.writes_per_generation;
        let write_id = seq % self.writes_per_generation;
        let ck_position = write_id / self.cks_per_pk;
        let pk_position = write_id % self.cks_per_pk;

        let pk = pk_position << 32 | pk_generation;

        let pos = ck_position + pk_generation * self.cks_per_pk;
        let ck = self.start_time + self.period * pos;

        Some((pk as i64, vec![-(ck as i64)]))
    }
}

struct TimeSeriesReadWorkload {
    cks_per_pk: u64,
    cks_per_call: u64,
    reads_per_generation: u64,
    start_time: Instant,
    period: u64, // Nanos

    use_half_normal_dist: bool,
    ck_distribution: Uniform<i64>,
}

impl TimeSeriesReadWorkload {
    fn get_random_pk(&self, ctx: &mut DistributionContext, max: u64) -> u64 {
        if self.use_half_normal_dist {
            let x = self.get_half_normal_f64(ctx);
            (x * max as f64) as u64
        } else {
            use rand::Rng;
            ctx.get_gen_mut().gen_range(0..max)
        }
    }

    fn get_random_ck(&self, ctx: &mut DistributionContext) -> i64 {
        if self.use_half_normal_dist {
            let x = self.get_half_normal_f64(ctx);
            (x * self.cks_per_pk as f64) as i64
        } else {
            self.ck_distribution.sample(ctx.get_gen_mut())
        }
    }

    fn get_half_normal_f64(&self, ctx: &mut DistributionContext) -> f64 {
        use rand_distr::Distribution;
        let mut base =
            <StandardNormal as Distribution<f64>>::sample(&StandardNormal, ctx.get_gen_mut()).abs();
        if base > 4.0 {
            base = 4.0;
        }
        1.0 - base / 4.0
    }
}

impl Workload for TimeSeriesReadWorkload {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)> {
        let now = Instant::now();
        let max_generation = (now - self.start_time).as_nanos() as u64
            / (self.period * self.reads_per_generation)
            + 1;
        let pk_generation = self.get_random_pk(ctx, max_generation);

        let seq = ctx.get_seq();
        let pk_position = seq % self.reads_per_generation;
        let pk = pk_position << 32 | pk_generation;

        // We are OK with ck duplicates - at least scylla-bench is
        let mut cks = Vec::with_capacity(self.cks_per_call as usize);
        for _ in 0..self.cks_per_call {
            cks.push(self.get_random_ck(ctx));
        }

        Some((pk as i64, cks))
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

    if size == 0 {
        return v;
    } else if size <= FULL_HEADER_SIZE {
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

fn validate_row_data(pk: i64, ck: i64, data: &[u8]) -> Result<()> {
    let expected = generate_row_data(pk, ck, data.len());
    if data != expected {
        return Err(anyhow::anyhow!(
            "Row data mismatch for pk={}, ck={}: expected {:x?}, got {:x?}",
            pk,
            ck,
            expected,
            data,
        ));
    }

    Ok(())
}

fn validate_counters(pk: i64, ck: i64, c1: i64, c2: i64, c3: i64, c4: i64, c5: i64) -> Result<()> {
    let update_count = if ck == 0 { c2 } else { c1 / ck };

    if c1 != ck * update_count
        || c2 != c1 + update_count
        || c3 != c2 + update_count
        || c4 != c3 + update_count
        || c5 != c4 + update_count
    {
        return Err(anyhow::anyhow!(
            "Invalid counter values for pk={}, ck={}: c1={}. c2={}, c3={}, c4={}, c5={}",
            pk,
            ck,
            c1,
            c2,
            c3,
            c4,
            c5,
        ));
    }

    Ok(())
}
