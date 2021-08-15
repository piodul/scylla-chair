mod goflags;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use futures::StreamExt;
use rand_distr::{Distribution as _, StandardNormal, Uniform};
use scylla::batch::{Batch, BatchType};
use scylla::frame::{types::Consistency, value::Counter};
use scylla::load_balancing::{LoadBalancingPolicy, RoundRobinPolicy, TokenAwarePolicy};
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::transport::Compression;
use scylla::Session;

use crate::configuration::{BenchDescription, BenchOp, BenchStrategy};
use crate::distribution::{self, Distribution, DistributionContext, FixedDistribution};

use goflags::{FlagValue, GoFlagSet};

#[allow(clippy::type_complexity)]
pub fn parse_scylla_bench_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<(Arc<BenchDescription>, Arc<dyn BenchStrategy>)>> {
    // TODO: Implement all flags!

    let mut flag = GoFlagSet::new();

    let mode: FlagValue<Option<Mode>> = flag.var(
        "mode",
        None,
        "operating mode: write, read, counter_update, counter_read, scan",
        |s| Ok(Some(parse_enum("mode", MODE_VALUES, s)?)),
    );
    let workload_type: FlagValue<Option<WorkloadType>> = flag.var(
        "workload",
        None,
        "workload: sequential, uniform, timeseries",
        |s| Ok(Some(parse_enum("workload type", WORKLOAD_TYPE_VALUES, s)?)),
    );
    let consistency_level = flag.var(
        "consistency-level",
        Consistency::Quorum,
        "consistency level",
        |s| parse_enum("consistency", CONSISTENCY_VALUES, s),
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
    let page_size = flag.u64_var("page-size", 1000, "page size");

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

    let rows_per_request =
        flag.u64_var("rows-per-request", 1, "clustering rows per single request");
    let provide_upper_bound = flag.bool_var(
        "provide-upper-bound",
        false,
        "whether read requests should provide an upper bound",
    );
    let in_restriction = flag.bool_var(
        "in-restriction",
        false,
        "use IN restriction in read requests",
    );
    let no_lower_bound = flag.bool_var(
        "no-lower-bound",
        false,
        "do not provide lower bound in read requests",
    );
    let range_count = flag.u64_var(
        "range-count",
        1,
        "number of ranges to split the token space into (relevant only for scan mode)",
    );

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
        "measure request latency (ignored, the latency is always measured)", // TODO: Should we implement this?
    );
    // validateData

    let write_rate = flag.u64_var(
        "write-rate",
        0,
        "rate of writes (relevant only for time series reads)",
    );
    let start_timestamp = flag.u64_var(
        "start-timestamp",
        0,
        "start timestamp of the write load (relevant only for time series reads)",
    );
    let use_hnormal = flag.var(
        "distribution",
        false,
        "distribution of keys (relevant only for time series reads): uniform, hnormal",
        |s| {
            parse_enum(
                "time series distribution",
                TIMESERIES_DISTRIBUTION_VALUES,
                s,
            )
        },
    );

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

    let host_selection_policy = flag.var(
        "host-selection-policy",
        Arc::new(TokenAwarePolicy::new(Box::new(RoundRobinPolicy::new())))
            as Arc<dyn LoadBalancingPolicy>,
        "set the driver host selection policy (round-robin, token-aware), default is 'token-aware'",
        parse_host_selection_policy,
    );

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

    let read_mode_tweak_count = in_restriction.get() as u32
        + provide_upper_bound.get() as u32
        + no_lower_bound.get() as u32;
    if mode != Mode::Read && mode != Mode::CounterRead {
        if read_mode_tweak_count > 0 {
            return Err(anyhow::anyhow!("in-restriction, no-lower-bound and provide-uppder-bound flags make sense only in read mode"));
        }
    } else if read_mode_tweak_count > 1 {
        return Err(anyhow::anyhow!(
            "in-restriction, no-lower-bound and provide-uppder-bound flags are mutually exclusive"
        ));
    }

    let read_restriction = if in_restriction.get() {
        ReadRestrictionKind::InRestriction
    } else if provide_upper_bound.get() {
        ReadRestrictionKind::BothBounds
    } else if no_lower_bound.get() {
        ReadRestrictionKind::NoBounds {
            limit: rows_per_request.get(),
        }
    } else {
        ReadRestrictionKind::OnlyLowerBound {
            limit: rows_per_request.get(),
        }
    };

    let start_timestamp = if start_timestamp.get() == 0 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    } else {
        start_timestamp.get()
    };

    let desc = Arc::new(BenchDescription {
        nodes: nodes.get().split(',').map(str::to_owned).collect(),
        duration,
        credentials,
        compression,
        load_balancing_policy: host_selection_policy.get(),
        concurrency: NonZeroUsize::new(concurrency.get() as usize).unwrap(), // TODO: Fix unwrap
        rate_limit_per_second: NonZeroU64::new(maximum_rate.get()),
    });

    println!("Configuration");
    println!("Mode:\t\t\t{}", enum_to_str(MODE_VALUES, mode));
    println!(
        "Workload:\t\t{}",
        enum_to_str(WORKLOAD_TYPE_VALUES, workload_type),
    );
    // println!("Timeout:\t\t{}", timeout);
    println!(
        "Consistency level:\t{}",
        enum_to_str(CONSISTENCY_VALUES, consistency_level.get()),
    );
    println!("Partition count:\t{}", partition_count.get());
    // if workload_type == WorkloadType::Sequential && partition_offset.get() != 0 {
    //     println!("Partition offset:\t{}", partition_offset.get());
    // }
    println!("Clustering rows:\t{}", clustering_row_count.get());
    // println!("Clustering row size:\t{}", clustering_row_size_dist.get());
    println!("Rows per request:\t{}", rows_per_request.get());
    if mode == Mode::Read {
        println!("Provide upper bound:\t{}", provide_upper_bound.get());
        println!("IN queries:\t{}", in_restriction.get());
        println!("No lower bound:\t{}", no_lower_bound.get());
    }
    // println!("Page size:\t{}", page_size.get());
    println!("Concurrency:\t\t{}", concurrency.get());
    // println!("Connections:\t{}", connection_count.get());
    if maximum_rate.get() > 0 {
        println!("Maximum rate:\t\t{}", maximum_rate.get());
    } else {
        println!("Maximum rate:\t\t unlimited");
    }
    println!("Client compression:\t{}", client_compression.get());
    if workload_type == WorkloadType::TimeSeries {
        println!("Start timestamp:\t{}", start_timestamp);
        println!(
            "Write rate:\t\t{}",
            maximum_rate.get() / partition_count.get()
        );
    }

    let strategy = Arc::new(ScyllaBenchStrategy {
        keyspace: keyspace_name.get(),
        table: table_name.get(),
        replication_factor: replication_factor.get(),
        consistency_level: consistency_level.get(),

        iterations: iterations.get(),
        partition_count: partition_count.get(),
        clustering_row_count: clustering_row_count.get(),
        total_ranges: NonZeroU64::new(range_count.get())
            .ok_or_else(|| anyhow::anyhow!("The 'range-count' parameter cannot be zero"))?,
        rows_per_request: NonZeroU64::new(rows_per_request.get())
            .ok_or_else(|| anyhow::anyhow!("The 'rows-per-request' parameter cannot be zero"))?,
        read_restriction,
        start_timestamp,
        use_hnormal: use_hnormal.get(),
        write_rate: NonZeroU64::new(write_rate.get()),
        page_size: page_size.get() as i32,

        clustering_row_size_dist: clustering_row_size_dist.get(),

        max_rate: NonZeroU64::new(maximum_rate.get()),

        mode,
        workload_type,
    });

    Ok(Some((desc, strategy)))
}

// gocql's host selection policy is analogous to rust driver's load balancing policy
fn parse_host_selection_policy(s: &str) -> Result<Arc<dyn LoadBalancingPolicy>> {
    match s {
        "round-robin" => Ok(Arc::new(RoundRobinPolicy::new())),
        "token-aware" => Ok(Arc::new(TokenAwarePolicy::new(Box::new(
            RoundRobinPolicy::new(),
        )))),
        "host-pool" => Err(anyhow::anyhow!(
            "host-pool selection policy is not supported"
        )),
        other => Err(anyhow::anyhow!("unknown host selection policy: {}", other)),
    }
}

fn parse_enum<E: Clone + Copy>(e_name: &str, e_values: &[(&str, E)], to_parse: &str) -> Result<E> {
    e_values
        .iter()
        .find(|(name, _)| to_parse == *name)
        .map(|(_, value)| *value)
        .ok_or_else(|| anyhow::anyhow!("Invalid {}: {}", e_name, to_parse))
}

fn enum_to_str<'s, E: PartialEq>(e_values: &[(&'s str, E)], e: E) -> &'s str {
    e_values
        .iter()
        .find(|(_, v)| e == *v)
        .map(|(label, _)| *label)
        .unwrap()
}

static CONSISTENCY_VALUES: &[(&str, Consistency)] = &[
    ("any", Consistency::Any),
    ("one", Consistency::One),
    ("two", Consistency::Two),
    ("three", Consistency::Three),
    ("quorum", Consistency::Quorum),
    ("all", Consistency::All),
    ("local_quorum", Consistency::LocalQuorum),
    ("each_quorum", Consistency::EachQuorum),
    ("local_one", Consistency::LocalOne),
];

static MODE_VALUES: &[(&str, Mode)] = &[
    ("write", Mode::Write),
    ("read", Mode::Read),
    ("counter_update", Mode::CounterUpdate),
    ("counter_read", Mode::CounterRead),
    ("scan", Mode::Scan),
];

static WORKLOAD_TYPE_VALUES: &[(&str, WorkloadType)] = &[
    ("sequential", WorkloadType::Sequential),
    ("uniform", WorkloadType::Uniform),
    ("timeseries", WorkloadType::TimeSeries),
    ("scan", WorkloadType::Scan),
];

static TIMESERIES_DISTRIBUTION_VALUES: &[(&str, bool)] = &[("uniform", false), ("hnormal", true)];

#[derive(Copy, Clone, PartialEq, Eq)]
enum Mode {
    Write,
    Read,
    CounterUpdate,
    CounterRead,
    Scan,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum WorkloadType {
    Sequential,
    Uniform,
    TimeSeries,
    Scan,
}

struct ScyllaBenchStrategy {
    keyspace: String,
    table: String,
    replication_factor: u64, // TODO: NonZeroUsize?
    consistency_level: Consistency,

    iterations: u64,
    partition_count: u64,
    clustering_row_count: u64,
    total_ranges: NonZeroU64,
    rows_per_request: NonZeroU64,
    read_restriction: ReadRestrictionKind,
    start_timestamp: u64,
    use_hnormal: bool,
    write_rate: Option<NonZeroU64>,
    page_size: i32,

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
        let create_counter_table_query = format!(
            "CREATE TABLE IF NOT EXISTS {}.test_counters (pk bigint, ck bigint, c1 counter, c2 counter, c3 counter, c4 counter, c5 counter, PRIMARY KEY(pk, ck)) WITH compression = {{ }}",
            &self.keyspace,
        );
        futures::try_join!(
            session.query(create_table_query, ()),
            session.query(create_counter_table_query, ()),
        )?;
        session.await_schema_agreement().await?;

        let op: Arc<dyn BenchOp> = match self.mode {
            Mode::Write => Arc::new(
                WriteOp::new(
                    session,
                    &self.keyspace,
                    &self.table,
                    self.consistency_level,
                    self.create_standard_workload().await?,
                    self.clustering_row_size_dist.clone(),
                )
                .await?,
            ),

            Mode::Read => Arc::new(
                ReadOp::new(
                    session,
                    &self.keyspace,
                    &self.table,
                    self.consistency_level,
                    self.page_size,
                    self.create_standard_workload().await?,
                    self.rows_per_request,
                    self.read_restriction,
                    false,
                )
                .await?,
            ),

            Mode::CounterUpdate => Arc::new(
                CounterUpdateOp::new(
                    session,
                    &self.keyspace,
                    self.consistency_level,
                    self.create_standard_workload().await?,
                )
                .await?,
            ),

            Mode::CounterRead => Arc::new(
                ReadOp::new(
                    session,
                    &self.keyspace,
                    &self.table,
                    self.consistency_level,
                    self.page_size,
                    self.create_standard_workload().await?,
                    self.rows_per_request,
                    self.read_restriction,
                    true,
                )
                .await?,
            ),

            Mode::Scan => Arc::new(
                ScanOp::new(
                    session,
                    &self.keyspace,
                    &self.table,
                    self.consistency_level,
                    self.page_size,
                    self.iterations,
                    self.total_ranges,
                )
                .await?,
            ),
        };
        Ok(op)
    }
}

impl ScyllaBenchStrategy {
    async fn create_standard_workload(&self) -> Result<Box<dyn Workload>> {
        match (self.workload_type, self.mode) {
            (WorkloadType::Sequential, _) => Ok(Box::new(SequentialWorkload {
                iterations: self.iterations,
                pks: self.partition_count,
                cks_per_call: self.rows_per_request.get(),
                calls_per_pk: self.clustering_row_count,
            })),

            (WorkloadType::Uniform, _) => Ok(Box::new(UniformWorkload {
                pk_distribution: Uniform::new(0, self.partition_count as i64),
                ck_distribution: Uniform::new(0, self.clustering_row_count as i64),
                cks_per_call: self.rows_per_request.get(),
            })),

            (WorkloadType::TimeSeries, Mode::Write) => Ok(Box::new(TimeSeriesWriteWorkload::new(
                self.partition_count,
                self.clustering_row_count,
                self.start_timestamp,
                self.max_rate.unwrap().get(), // TODO: Error reporting
            ))),

            (WorkloadType::TimeSeries, Mode::Read) => Ok(Box::new(TimeSeriesReadWorkload::new(
                self.partition_count,
                self.clustering_row_count,
                self.rows_per_request.get(),
                self.start_timestamp,
                self.write_rate.unwrap().get(), // TODO: Error reporting
                self.use_hnormal,
            ))),

            (WorkloadType::TimeSeries, _) => Err(anyhow::anyhow!(
                "the 'time_series' workload is only works with either 'read' or 'write' modes",
            )),

            (WorkloadType::Scan, Mode::Scan) => {
                panic!("create_standard_workload is not intended to be used in 'scan' mode")
            }
            (WorkloadType::Scan, _) => Err(anyhow::anyhow!(
                "the 'scan' workload is allowed only with the 'scan' mode",
            )),
        }
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
    async fn execute(
        &self,
        mut ctx: DistributionContext,
        rows_processed: &AtomicU64,
    ) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        debug_assert!(!cks.is_empty());
        let cks_count = cks.len();
        if cks_count == 1 {
            let clustering_row_len = self.clustering_row_size_dist.get_u64(&mut ctx) as usize;
            let data = generate_row_data(pk, cks[0], clustering_row_len);
            self.session
                .execute(&self.statement, (pk, cks[0], data))
                .await?;
        } else {
            let mut batch = Batch::new(BatchType::Unlogged);
            batch.set_is_idempotent(true);
            batch.set_consistency(self.statement.get_consistency());
            let mut vals = Vec::with_capacity(cks_count);
            for ck in cks {
                let clustering_row_len = self.clustering_row_size_dist.get_u64(&mut ctx) as usize;
                let data = generate_row_data(pk, ck, clustering_row_len);
                batch.append_statement(self.statement.clone());
                vals.push((pk, ck, data));
            }
            self.session.batch(&batch, vals).await?;
        }
        rows_processed.fetch_add(cks_count as u64, Ordering::Relaxed);

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
        consistency_level: Consistency,
        workload: Box<dyn Workload>,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let insert_statement = format!(
            "UPDATE {}.test_counters SET c1 = c1 + ?, c2 = c2 + ?, c3 = c3 + ?, c4 = c4 + ?, c5 = c5 + ? WHERE pk = ? AND ck = ?",
            keyspace,
        );
        async move {
            let mut statement = session.prepare(insert_statement).await?;
            statement.set_consistency(consistency_level);

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
    async fn execute(
        &self,
        mut ctx: DistributionContext,
        rows_processed: &AtomicU64,
    ) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        debug_assert!(!cks.is_empty());
        let cks_count = cks.len();
        if cks_count == 1 {
            let ck = cks[0];
            self.session
                .execute(
                    &self.statement,
                    (
                        Counter(ck),
                        Counter(ck + 1),
                        Counter(ck + 2),
                        Counter(ck + 3),
                        Counter(ck + 4),
                        pk,
                        ck,
                    ),
                )
                .await?;
        } else {
            let mut batch = Batch::new(BatchType::Counter);
            batch.set_consistency(self.statement.get_consistency());
            let mut vals = Vec::with_capacity(cks_count);
            for ck in cks {
                batch.append_statement(self.statement.clone());
                vals.push((ck, ck + 1, ck + 2, ck + 3, ck + 4, pk, ck));
            }
            self.session.batch(&batch, vals).await?;
        }
        rows_processed.fetch_add(cks_count as u64, Ordering::Relaxed);

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
    fn as_query_string(&self, rows_per_request: NonZeroU64) -> String {
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
    #[allow(clippy::too_many_arguments)]
    fn new(
        session: Arc<Session>,
        keyspace: &str,
        table: &str,
        consistency_level: Consistency,
        page_size: i32,
        workload: Box<dyn Workload>,
        rows_per_request: NonZeroU64,
        read_restriction: ReadRestrictionKind,
        is_counter: bool,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let ck_restriction = read_restriction.as_query_string(rows_per_request);

        let statement_text = if !is_counter {
            format!(
                "SELECT ck, v FROM {}.{} WHERE pk = ? {}",
                keyspace, table, ck_restriction,
            )
        } else {
            format!(
                "SELECT ck, c1, c2, c3, c4, c5 FROM {}.test_counters WHERE pk = ? {}",
                keyspace, ck_restriction,
            )
        };

        async move {
            let mut statement = session.prepare(statement_text).await?;
            statement.set_consistency(consistency_level);
            statement.set_is_idempotent(true);
            statement.set_page_size(page_size);
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
    async fn execute(
        &self,
        mut ctx: DistributionContext,
        rows_processed: &AtomicU64,
    ) -> Result<bool> {
        let (pk, cks) = match self.workload.get(&mut ctx) {
            None => return Ok(false),
            Some(x) => x,
        };

        // TODO: Consider executing with iterator
        let iter = match self.read_restriction {
            ReadRestrictionKind::InRestriction => {
                use scylla::frame::value::SerializedValues;
                let col_size = std::mem::size_of::<i64>();
                let mut s = SerializedValues::with_capacity(col_size * (1 + cks.len()));
                s.add_value(&pk)?;
                for x in cks {
                    s.add_value(&x)?;
                }
                self.session.execute_iter(self.statement.clone(), s).await?
            }
            ReadRestrictionKind::BothBounds => {
                debug_assert_eq!(cks.len(), 1);
                self.session
                    .execute_iter(
                        self.statement.clone(),
                        (pk, cks[0], cks[0] + self.rows_per_request.get() as i64),
                    )
                    .await?
            }
            ReadRestrictionKind::OnlyLowerBound { .. } => {
                debug_assert_eq!(cks.len(), 1);
                self.session
                    .execute_iter(self.statement.clone(), (pk, cks[0]))
                    .await?
            }
            ReadRestrictionKind::NoBounds { .. } => {
                debug_assert_eq!(cks.len(), 0);
                self.session
                    .execute_iter(self.statement.clone(), (pk,))
                    .await?
            }
        };

        use futures::StreamExt;

        if !self.is_counter {
            let mut iter = iter.into_typed::<(i64, Vec<u8>)>();

            while let Some(r) = iter.next().await {
                let (ck, v) = r?;
                if let Err(err) = validate_row_data(pk, ck, &v) {
                    // TODO: Tracing?
                    println!("{:?}", err);
                }
                rows_processed.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let mut iter = iter.into_typed::<(i64, Counter, Counter, Counter, Counter, Counter)>();

            while let Some(r) = iter.next().await {
                let (ck, c1, c2, c3, c4, c5) = r?;
                if let Err(err) = validate_counters(pk, ck, c1.0, c2.0, c3.0, c4.0, c5.0) {
                    // TODO: Tracing?
                    println!("{:?}", err);
                }
                rows_processed.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(true)
    }
}

struct ScanOp {
    session: Arc<Session>,
    statement: PreparedStatement,
    iterations: u64,
    total_ranges: NonZeroU64,
}

impl ScanOp {
    // The function cannot be async due to this bug:
    // https://github.com/rust-lang/rust/issues/63033
    fn new(
        session: Arc<Session>,
        keyspace: &str,
        table: &str,
        consistency_level: Consistency,
        page_size: i32,
        iterations: u64,
        total_ranges: NonZeroU64,
    ) -> impl std::future::Future<Output = Result<Self>> {
        let statement_text = format!(
            "SELECT * FROM {}.{} WHERE token(pk) >= ? AND token(pk) <= ?",
            keyspace, table
        );
        async move {
            let mut statement = session.prepare(statement_text).await?;
            statement.set_consistency(consistency_level);
            statement.set_is_idempotent(true);
            statement.set_page_size(page_size);
            Ok(Self {
                session,
                statement,
                iterations,
                total_ranges,
            })
        }
    }
}

#[async_trait]
impl BenchOp for ScanOp {
    async fn execute(&self, ctx: DistributionContext, rows_processed: &AtomicU64) -> Result<bool> {
        let seq = ctx.get_seq();
        let iteration_id = seq / self.total_ranges.get();
        let range_id = seq % self.total_ranges.get();

        if iteration_id >= self.iterations {
            return Ok(false);
        }

        let token_adjust = 1i128 << 63;
        let first_token =
            (((range_id as i128) << 64) / self.total_ranges.get() as i128 - token_adjust) as i64;
        let last_token = (((range_id as i128 + 1) << 64) / self.total_ranges.get() as i128
            - 1
            - token_adjust) as i64;

        let mut iter = self
            .session
            .execute_iter(self.statement.clone(), (first_token, last_token))
            .await?;

        // TODO: How should we calculate latency? per-page?
        // In order to do that, a ScanOp needs to be able to calculate the latency itself

        while let Some(row) = iter.next().await {
            row?;
            rows_processed.fetch_add(1, Ordering::Relaxed);
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
    partition_count: u64,
    writes_per_generation: u64,
    start_time: u64, // Nanos since unix epoch
    period: u64,     // Nanos
}

impl TimeSeriesWriteWorkload {
    fn new(
        partition_count: u64,
        clustering_row_count: u64,
        start_timestamp: u64,
        max_rate: u64,
    ) -> Self {
        Self {
            partition_count,
            writes_per_generation: partition_count * clustering_row_count,
            start_time: start_timestamp,
            period: (1_000_000_000 * partition_count) / max_rate,
        }
    }
}

impl Workload for TimeSeriesWriteWorkload {
    fn get(&self, ctx: &mut DistributionContext) -> Option<(i64, Vec<i64>)> {
        let seq = ctx.get_seq();
        let pk_generation = seq / self.writes_per_generation;
        let write_id = seq % self.writes_per_generation;
        let ck_position = write_id / self.partition_count;
        let pk_position = write_id % self.partition_count;

        let pk = pk_position << 32 | pk_generation;

        let pos = ck_position + pk_generation * self.partition_count;
        let ck = self.start_time + self.period * pos;

        Some((pk as i64, vec![-(ck as i64)]))
    }
}

struct TimeSeriesReadWorkload {
    cks_per_pk: u64,
    cks_per_call: u64,
    partition_count: u64,
    start_time: u64,
    period: u64, // Nanos

    use_half_normal_dist: bool,
}

impl TimeSeriesReadWorkload {
    fn new(
        partition_count: u64,
        clustering_row_count: u64,
        rows_per_request: u64,
        start_timestamp: u64,
        max_rate: u64,
        use_half_normal_dist: bool,
    ) -> Self {
        Self {
            cks_per_pk: clustering_row_count,
            cks_per_call: rows_per_request,
            start_time: start_timestamp,
            partition_count,
            period: (1_000_000_000 * partition_count) / max_rate,

            use_half_normal_dist,
        }
    }

    fn get_random_pk_generation(&self, ctx: &mut DistributionContext, max: u64) -> u64 {
        if self.use_half_normal_dist {
            let x = self.get_half_normal_f64(ctx);
            (x * max as f64) as u64
        } else {
            use rand::Rng;
            ctx.get_gen_mut().gen_range(0..max)
        }
    }

    fn get_random_ck_position(&self, ctx: &mut DistributionContext) -> f64 {
        if self.use_half_normal_dist {
            self.get_half_normal_f64(ctx)
        } else {
            use rand::Rng;
            ctx.get_gen_mut().gen_range(0.0..1.0)
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
        let now_nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let max_generation = (now_nanos - self.start_time) / (self.period * self.cks_per_pk) + 1;
        let pk_generation = self.get_random_pk_generation(ctx, max_generation);

        let seq = ctx.get_seq();
        let pk_position = seq % self.partition_count;
        let pk = pk_position << 32 | pk_generation;

        let max_range =
            (now_nanos - self.start_time) / self.period - pk_generation * self.cks_per_pk + 1;
        let max_range = std::cmp::min(max_range, self.cks_per_pk);

        // We are OK with ck duplicates - at least scylla-bench is
        let mut cks = Vec::with_capacity(self.cks_per_call as usize);
        for _ in 0..self.cks_per_call {
            let ck_position = (self.get_random_ck_position(ctx) * max_range as f64) as u64;
            let timestamp_delta = (pk_generation * self.cks_per_pk + ck_position) * self.period;
            cks.push(-((timestamp_delta + self.start_time) as i64));
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
