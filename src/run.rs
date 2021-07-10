use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use futures::FutureExt;
use histogram::Histogram;
use scylla::{Session, SessionBuilder};
use tokio::task::JoinHandle;
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
}

struct RateLimiter {
    base: Instant,
    increment: u64,
    microseconds: AtomicU64,
}

impl RateLimiter {
    pub fn new(base: Instant, ops_per_sec: u64) -> Self {
        Self {
            microseconds: 0.into(),
            base,
            increment: 1_000_000 / ops_per_sec,
        }
    }

    pub async fn wait(&self) -> Instant {
        let micros = self
            .microseconds
            .fetch_add(self.increment, Ordering::Relaxed);
        let start_at = self.base + Duration::from_micros(micros);

        tokio::time::sleep_until(start_at).await;
        start_at
    }

    // Runs an asynchronous process which will add microseconds
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
            let micros = self.microseconds.load(Ordering::Relaxed);
            let status = self.base + Duration::from_micros(micros);
            let lag = now
                .checked_duration_since(status)
                .unwrap_or_else(|| Duration::from_secs(0));

            if lag > max_allowed_lag {
                let adjustment = (lag - max_allowed_lag).as_micros() as u64;
                self.microseconds.fetch_add(adjustment, Ordering::Relaxed);
            }
        }
    }
}

impl WorkerContext {
    pub fn new(config: Arc<BenchDescription>) -> Self {
        let rate_limiter = config
            .rate_limit_per_second
            .map(|limit| RateLimiter::new(Instant::now(), limit.get()));

        Self {
            config,
            next_operation_id: 0.into(),
            rate_limiter,
            histogram: ShardedHistogram::new(Default::default()),
        }
    }

    pub fn issue_operation_id(&self) -> Option<u64> {
        let id = self.next_operation_id.fetch_add(1, Ordering::Relaxed);
        if id >= self.config.operation_count {
            return None;
        }
        Some(id)
    }

    pub fn get_operations_done_count(&self) -> u64 {
        // `next_operation_id` does not represent the number of the operations
        // done, only the number of the operations issued. Subtract the
        // concurrency to get a good estimate.
        let id = self.next_operation_id.load(Ordering::Relaxed);
        id.saturating_sub(self.config.concurrency.get() as u64)
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
}

struct ProgressReporter {
    context: Arc<WorkerContext>,
    start_time: Instant,

    previous_ops: u64,
    previous_report_time: Instant,
}

impl ProgressReporter {
    pub fn new(context: Arc<WorkerContext>) -> Self {
        let now = Instant::now();
        Self {
            context,
            start_time: now,

            previous_ops: 0,
            previous_report_time: now,
        }
    }

    pub async fn wait_and_print_report(&mut self) {
        let next = self.previous_report_time + REPORT_INTERVAL;
        tokio::time::sleep_until(next).await;
        self.print_report(next);
    }

    pub fn print_report_now(&mut self) {
        self.print_report(Instant::now());
    }

    fn print_report(&mut self, now: Instant) {
        let elapsed = now - self.start_time;
        let ops_done = self.context.get_operations_done_count();

        let ops_delta = ops_done - self.previous_ops;
        let time_delta = now - self.previous_report_time;

        let ops_per_sec = ops_delta as f64 / time_delta.as_secs_f64();

        let hist = self.context.get_combined_histogram_and_clear();
        let p50 = hist.percentile(50.0).unwrap_or(0) as f64 / 1000.0;
        let p95 = hist.percentile(95.0).unwrap_or(0) as f64 / 1000.0;
        let p99 = hist.percentile(99.0).unwrap_or(0) as f64 / 1000.0;

        println!(
            "{:?}: {} {}ops/s {:.3}ms {:.3}ms {:.3}ms",
            elapsed, ops_done, ops_per_sec, p50, p95, p99
        );

        self.previous_ops = ops_done;
        self.previous_report_time = now;
    }
}

pub async fn run(config: Arc<BenchDescription>, strategy: Arc<dyn BenchStrategy>) -> Result<()> {
    let session = SessionBuilder::new()
        .known_nodes(&config.nodes)
        .tcp_nodelay(false) // TODO: Make configurable
        .build()
        .await?;

    let session = Arc::new(session);

    let op = strategy.prepare(session.clone()).await?;

    let context = Arc::new(WorkerContext::new(config.clone()));
    let mut handles = Vec::with_capacity(config.concurrency.into());

    for _ in 0usize..config.concurrency.into() {
        let context = context.clone();
        let op = op.clone();

        handles.push(async move {
            let res: Result<Result<()>, tokio::task::JoinError> = tokio::spawn(async move {
                let mut prev_op_id = 0;
                let mut gen = RngGen::new(0); // TODO: Support seeding

                while let Some(op_id) = context.issue_operation_id() {
                    let start = context.rate_limit().await;

                    gen.advance(((op_id - prev_op_id) * 1024) as u128);
                    prev_op_id = op_id;

                    let ctx = DistributionContext::new(op_id, gen.clone());

                    match op.execute(ctx).await {
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
            })
            .await;
            res.map_err(|err| err.into()).and_then(|res| res)
        });
    }

    let wait_for_handles = futures::future::try_join_all(handles);
    tokio::pin!(wait_for_handles);

    let mut progress_reporter = ProgressReporter::new(context.clone());
    let _catch_upper_handle = tokio::spawn(context.clone().run_catch_upper()).remote_handle();

    loop {
        tokio::select! {
            biased;

            res = &mut wait_for_handles => {
                res?;
                break;
            }

            _ = progress_reporter.wait_and_print_report() => {}
        }
    }

    progress_reporter.print_report_now();

    Ok(())
}
