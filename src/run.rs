use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use histogram::Histogram;
use scylla::{Session, SessionBuilder};
use tokio::time::Instant;

use crate::configuration::{BenchDescription, BenchStrategy};
use crate::distribution::{DistributionContext, RngGen};
use crate::sharded_histogram::ShardedHistogram;

const REPORT_INTERVAL: Duration = Duration::from_secs(1);
const CATCH_UPPER_INTERVAL: Duration = Duration::from_millis(100);

struct WorkerContext {
    config: Arc<BenchDescription>,
    next_operation_id: AtomicU64,

    rate_limiter: Option<RateLimiter>,
    histogram: ShardedHistogram,

    start_time: Instant,
    rows_written: AtomicU64,
}

struct RateLimiter {
    base: Instant,
    increment: u64,
    nanoseconds: AtomicU64,
}

impl RateLimiter {
    pub fn new(base: Instant, ops_per_sec: u64) -> Self {
        Self {
            nanoseconds: 0.into(),
            base,
            increment: 1_000_000_000 / ops_per_sec,
        }
    }

    pub async fn wait(&self) -> Instant {
        let nanos = self
            .nanoseconds
            .fetch_add(self.increment, Ordering::Relaxed);
        let start_at = self.base + Duration::from_nanos(nanos);

        tokio::time::sleep_until(start_at).await;

        let now = Instant::now();
        std::cmp::max(start_at, now)
    }

    // Runs an asynchronous process which will add nanoseconds
    // if it observes that we are lagging too much behind.
    // This should help in situations when Cassandra/Scylla is overloaded
    // for some time and we can't achieve configured rate, but then it stops
    // being overloaded - in that case, if we didn't run the catch-upper,
    // we would try to catch up with no regard to the limit.
    pub async fn run_catch_upper(&self) {
        // TODO: We should calculate max lag based on the concurrency and op rate,
        // not have a constant. This will behave badly for low op rates
        // and concurrencies.
        let max_allowed_lag = Duration::from_millis(250);

        loop {
            tokio::time::sleep(CATCH_UPPER_INTERVAL).await;
            let now = Instant::now();
            let nanos = self.nanoseconds.load(Ordering::Relaxed);
            let status = self.base + Duration::from_nanos(nanos);
            let lag = now
                .checked_duration_since(status)
                .unwrap_or_else(|| Duration::from_secs(0));

            if lag > max_allowed_lag {
                let adjustment = (lag - max_allowed_lag).as_nanos() as u64;
                self.nanoseconds.fetch_add(adjustment, Ordering::Relaxed);
            }
        }
    }
}

const INVALID_OP_BIT: u64 = 1 << 63;

impl WorkerContext {
    pub fn new(config: Arc<BenchDescription>) -> Self {
        let now = Instant::now();

        let rate_limiter = config
            .rate_limit_per_second
            .map(|limit| RateLimiter::new(now, limit.get()));

        Self {
            config,
            next_operation_id: 0.into(),
            rate_limiter,
            histogram: ShardedHistogram::new(Default::default()),
            start_time: now,
            rows_written: 0.into(),
        }
    }

    pub fn get_start_time(&self) -> Instant {
        self.start_time
    }

    pub fn issue_operation_id(&self) -> Option<u64> {
        let id = self.next_operation_id.fetch_add(1, Ordering::Relaxed);
        if id >= INVALID_OP_BIT {
            return None;
        }
        Some(id)
    }

    pub fn get_operations_done_count(&self) -> u64 {
        // `next_operation_id` does not represent the number of the operations
        // done, only the number of the operations issued. Subtract the
        // concurrency to get a good estimate.
        // Clear the highest bit - the highest bit indicates that the bench
        // was stopped due to timeout
        let id = self.next_operation_id.load(Ordering::Relaxed) & !INVALID_OP_BIT;
        id.saturating_sub(self.config.concurrency.get() as u64)
    }

    pub fn get_rows_written_count_ref(&self) -> &AtomicU64 {
        &self.rows_written
    }

    pub async fn rate_limit(&self) -> Instant {
        if let Some(limiter) = &self.rate_limiter {
            limiter.wait().await
        } else {
            Instant::now()
        }
    }

    pub async fn run_catch_upper(self: Arc<Self>) {
        if let Some(limiter) = &self.rate_limiter {
            limiter.run_catch_upper().await
        }
    }

    pub fn mark_latency(&self, latency: Duration) {
        let _ = self
            .histogram
            .get_shard_mut()
            .increment(latency.as_micros() as u64);
    }

    pub fn get_combined_histogram_and_clear(&self) -> Histogram {
        self.histogram.get_combined_and_clear()
    }

    /// Prevents more operations from being performed.
    pub fn stop(&self) {
        self.next_operation_id
            .fetch_or(INVALID_OP_BIT, Ordering::Relaxed);
    }
}

struct ProgressReporter {
    context: Arc<WorkerContext>,
    start_time: Instant,

    previous_ops: u64,
    previous_rows: u64,
    previous_report_time: Instant,
    total_histogram: Histogram,
}

impl ProgressReporter {
    pub fn new(context: Arc<WorkerContext>) -> Self {
        let now = Instant::now();
        Self {
            context,
            start_time: now,

            previous_ops: 0,
            previous_rows: 0,
            previous_report_time: now,
            total_histogram: Histogram::new(),
        }
    }

    pub async fn wait_and_print_partial_report(&mut self) {
        let next = self.previous_report_time + REPORT_INTERVAL;
        tokio::time::sleep_until(next).await;
        self.print_report(next, false);
    }

    pub fn print_full_report(&mut self) {
        self.print_report(Instant::now(), true);
    }

    fn print_report(&mut self, now: Instant, full: bool) {
        let elapsed = now - self.start_time;
        let ops_done = self.context.get_operations_done_count();
        let rows_done = self
            .context
            .get_rows_written_count_ref()
            .load(Ordering::Relaxed);

        let ops_delta = if full {
            ops_done
        } else {
            ops_done - self.previous_ops
        };
        let rows_delta = if full {
            rows_done
        } else {
            rows_done - self.previous_rows
        };

        let hist = self.context.get_combined_histogram_and_clear();
        self.total_histogram.merge(&hist);
        let hist = if full { &self.total_histogram } else { &hist };
        ResultPrinter.print_partial_results(elapsed, ops_delta, rows_delta, 0, hist);

        self.previous_ops = ops_done;
        self.previous_rows = rows_done;
        self.previous_report_time = now;
    }
}

pub async fn run(config: Arc<BenchDescription>, strategy: Arc<dyn BenchStrategy>) -> Result<()> {
    let mut session_builder = SessionBuilder::new()
        .known_nodes(&config.nodes)
        .tcp_nodelay(false) // TODO: Make configurable
        .compression(config.compression)
        .ssl_context(config.ssl_context.clone())
        .load_balancing(config.load_balancing_policy.clone());
    // .disallow_shard_aware_port(true)
    // .pool_size(scylla::transport::session::PoolSize::PerShard(
    //     std::num::NonZeroUsize::new(2).unwrap(),
    // ));

    if let Some((username, password)) = &config.credentials {
        session_builder = session_builder.user(username, password);
    }

    let session = session_builder.build().await?;
    let session = Arc::new(session);

    let op = strategy.prepare(session.clone()).await?;

    let context = Arc::new(WorkerContext::new(config.clone()));
    let mut handles = FuturesUnordered::new();

    for _ in 0usize..config.concurrency.into() {
        let context = context.clone();
        let op = op.clone();

        handles.push({
            let res: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                let mut prev_op_id = 0;
                let mut gen = RngGen::new(0); // TODO: Support seeding

                while let Some(op_id) = context.issue_operation_id() {
                    let start = context.rate_limit().await;

                    gen.advance(((op_id - prev_op_id) * 1024) as u128);
                    prev_op_id = op_id;

                    let ctx = DistributionContext::new(op_id, gen.clone());

                    match op.execute(ctx, context.get_rows_written_count_ref()).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(err) => {
                            println!("Failed to perform an operation: {:?}", err);
                        }
                    }

                    let end = Instant::now();
                    context.mark_latency(end - start);
                }
                Ok(())
            });
            res
        });
    }

    let mut progress_reporter = ProgressReporter::new(context.clone());
    let _catch_upper_handle = tokio::spawn(context.clone().run_catch_upper()).remote_handle();

    let start_time = context.get_start_time();
    let _stopper_handle = config.duration.map(move |duration| {
        tokio::spawn(async move {
            tokio::time::sleep_until(start_time + duration).await;
            context.stop();
        })
        .remote_handle()
    });

    ResultPrinter.print_header();

    while !handles.is_empty() {
        tokio::select! {
            biased;

            res = &mut handles.next() => {
                res.unwrap()??;
                break;
            }

            _ = progress_reporter.wait_and_print_partial_report() => {}
        }
    }

    progress_reporter.print_full_report();

    Ok(())
}

struct ResultPrinter;

impl ResultPrinter {
    fn print_header(&self) {
        println!(
            "{:10} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
            "time",
            "ops/s",
            "rows/s",
            "errors",
            "max",
            "99.9th",
            "99th",
            "95th",
            "90th",
            "median",
            "mean",
        );
    }

    fn print_partial_results(
        &self,
        elapsed: Duration,
        ops_delta: u64,
        rows_delta: u64,
        errors_delta: u64,
        hist: &Histogram,
    ) {
        let p50 = Duration::from_micros(hist.percentile(50.0).unwrap_or(0));
        let p90 = Duration::from_micros(hist.percentile(90.0).unwrap_or(0));
        let p95 = Duration::from_micros(hist.percentile(95.0).unwrap_or(0));
        let p99 = Duration::from_micros(hist.percentile(99.0).unwrap_or(0));
        let p999 = Duration::from_micros(hist.percentile(99.9).unwrap_or(0));
        let max = Duration::from_micros(hist.maximum().unwrap_or(0));
        let mean = Duration::from_micros(hist.mean().unwrap_or(0));

        println!(
            "{:10} {:>7} {:>7} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
            format_duration(elapsed),
            ops_delta,
            rows_delta,
            errors_delta,
            format_duration(max),
            format_duration(p999),
            format_duration(p99),
            format_duration(p95),
            format_duration(p90),
            format_duration(p50),
            format_duration(mean),
        );
    }
}

// TODO: Move to utils
// TODO: This could be realized as a custom formatter, without any allocations
fn format_duration(d: Duration) -> String {
    let mut ret = String::new();

    if d == Duration::ZERO {
        return "0".into();
    }

    if d >= Duration::from_secs(1) {
        let minutes = (d.as_secs() / 60) % 60;
        let hours = d.as_secs() / 3600;

        if hours > 0 {
            ret += &format!("{}h", hours);
        }
        if minutes > 0 {
            ret += &format!("{}m", minutes);
        }

        // Print one digit of precision
        let secs = d.as_secs_f64() % 60.0;
        ret += &format!("{:.1}s", secs);
    } else {
        // Diverge a bit from scylla-bench: we will keep at most 3 digits of precision, always
        let total_nanos = d.subsec_nanos();
        let mut first_digit = 1_000_000_000;
        let mut first_offset = 9;
        while first_digit > 1 && total_nanos < first_digit {
            first_digit /= 10;
            first_offset -= 1;
        }

        let round_unit = std::cmp::max(first_digit / 100, 1);
        let total_nanos = total_nanos - (total_nanos % round_unit);

        if first_digit >= 1_000_000 {
            let prec = 8 - first_offset;
            ret = format!("{:.*}ms", prec, total_nanos as f64 / 1_000_000.0);
        } else if first_digit >= 1_000 {
            let prec = 5 - first_offset;
            ret = format!("{:.*}μs", prec, total_nanos as f64 / 1_000.0);
        } else {
            ret = format!("{}ns", total_nanos);
        }
    }

    ret
}
