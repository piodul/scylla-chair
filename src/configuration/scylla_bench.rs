use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::rc::Rc;
use std::sync::Arc;

use anyhow::Result;

use crate::configuration::BenchDescription;
use crate::distribution::{self, Distribution, FixedDistribution};

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
    let consistency_level = flag.string_var("consistency-level", "", "consistency level");
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

struct GoFlag {
    pub name: &'static str,
    pub desc: &'static str,
    pub is_boolean: bool,
    pub setter: Box<dyn Fn(&str) -> Result<()>>,
}

struct FlagValue<T> {
    r: Rc<RefCell<T>>,
}

impl<T: Clone> FlagValue<T> {
    fn new(r: Rc<RefCell<T>>) -> Self {
        Self { r }
    }

    fn get(&self) -> T {
        self.r.borrow().clone()
    }
}

struct GoFlagSet {
    flags: HashMap<&'static str, GoFlag>,
}

impl GoFlagSet {
    pub fn new() -> Self {
        GoFlagSet {
            flags: HashMap::new(),
        }
    }

    pub fn parse_args(&self, mut args: impl Iterator<Item = String>) -> Result<()> {
        let mut parsed_flags = HashSet::new();

        while let Some(arg) = args.next() {
            // Trim dashes at the beginning
            let arg = arg
                .as_str()
                .strip_prefix("--")
                .or_else(|| arg.strip_prefix('-'))
                .ok_or_else(|| anyhow::anyhow!("Expected an option, but got {}", arg))?;

            // Get the name of the flag, and - if it has form '-name=value' - its value
            let (name, maybe_value) = match arg.split_once('=') {
                Some((name, value)) => (name, Some(value)),
                None => (arg, None),
            };

            // Ensure that the flag was not present
            anyhow::ensure!(
                parsed_flags.insert(name.to_owned()),
                "The flag {} was provided twice",
                name,
            );

            // Get the flag object
            let flag = self
                .flags
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("Unknown flag: {}", name))?;

            // If the flag was not of form '-name=value`, get the value from the next arg
            let value = match maybe_value {
                Some(value) => value.to_owned(),
                None if flag.is_boolean => "1".to_owned(),
                None => args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("Value is missing for flag {}", name))?,
            };

            (flag.setter)(value.as_ref())?;
        }

        Ok(())
    }

    pub fn bool_var(
        &mut self,
        name: &'static str,
        default: bool,
        desc: &'static str,
    ) -> FlagValue<bool> {
        self.add_flag(name, true, default, desc, move |s| match s {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
            _ => Err(anyhow::anyhow!("Invalid value for bool flag: {}", s)),
        })
    }

    pub fn string_var(
        &mut self,
        name: &'static str,
        default: impl ToString,
        desc: &'static str,
    ) -> FlagValue<String> {
        self.add_flag(
            name,
            false,
            default.to_string(),
            desc,
            |s| Ok(s.to_string()),
        )
    }

    pub fn i64_var(
        &mut self,
        name: &'static str,
        default: i64,
        desc: &'static str,
    ) -> FlagValue<i64> {
        self.add_flag(name, false, default, desc, |s| Ok(s.parse()?))
    }

    pub fn u64_var(
        &mut self,
        name: &'static str,
        default: u64,
        desc: &'static str,
    ) -> FlagValue<u64> {
        self.add_flag(name, false, default, desc, |s| Ok(s.parse()?))
    }

    pub fn var<T: Clone + 'static>(
        &mut self,
        name: &'static str,
        default: T,
        desc: &'static str,
        converter: impl Fn(&str) -> Result<T> + 'static,
    ) -> FlagValue<T> {
        self.add_flag(name, false, default, desc, converter)
    }

    fn add_flag<T: Clone + 'static>(
        &mut self,
        name: &'static str,
        is_boolean: bool,
        default: T,
        desc: &'static str,
        parser: impl Fn(&str) -> Result<T> + 'static,
    ) -> FlagValue<T> {
        let target = Rc::new(RefCell::new(default));
        let target_for_parser = target.clone();
        self.flags.insert(
            name,
            GoFlag {
                name,
                desc,
                is_boolean,
                setter: Box::new(move |s| {
                    target_for_parser.replace(parser(s)?);
                    Ok(())
                }),
            },
        );
        FlagValue::new(target)
    }
}
