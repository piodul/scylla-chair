use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;

use anyhow::Result;

pub struct GoFlag {
    pub name: &'static str,
    pub desc: &'static str,
    pub is_boolean: bool,
    pub setter: Box<dyn Fn(&str) -> Result<()>>,
}

pub struct FlagValue<T> {
    r: Rc<RefCell<T>>,
}

impl<T: Clone> FlagValue<T> {
    fn new(r: Rc<RefCell<T>>) -> Self {
        Self { r }
    }

    pub fn get(&self) -> T {
        self.r.borrow().clone()
    }
}

pub struct GoFlagSet {
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

    pub fn print_help(&self, program_name: &str) {
        println!("Usage of {}:", program_name);
        let mut flag_names: Vec<&str> = self.flags.keys().copied().collect();
        flag_names.sort_unstable();

        for fname in flag_names {
            let flag = self.flags.get(&fname).unwrap();
            println!("  -{}", fname);
            println!("        {}", flag.desc);
        }
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

    pub fn duration_var(
        &mut self,
        name: &'static str,
        default: Duration,
        desc: &'static str,
    ) -> FlagValue<Duration> {
        self.add_flag(name, false, default, desc, parse_duration)
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

static UNIT_MULTIPLICANDS: &[(&str, f64)] = &[
    ("ns", 1.0),
    ("us", 1_000.0),
    ("µs", 1_000.0), // U+00B5 = micro symbol
    ("μs", 1_000.0), // U+03BC = Greek letter mu
    ("ms", 1_000_000.0),
    ("s", 1_000_000_000.0),
    ("m", 60.0 * 1_000_000_000.0),
    ("h", 60.0 * 60.0 * 1_000_000_000.0),
];

// Reimplementation of Go's time.ParseDuration
// Not exactly the same, but should be enough
// TODO: There might be some precision issues with hour fractions
fn parse_duration(mut s: &str) -> Result<Duration> {
    let original = s;
    let mut nanos = 0u128;

    // We don't support negative durations! We don't need them, and Rust's duration
    // does not permit negative durations either.
    if s.starts_with('-') {
        return Err(anyhow::anyhow!("negative durations are not supported"));
    } else if s.starts_with('+') {
        s = s.get(1..).unwrap();
    }

    // Special case for unitless 0
    if s == "0" {
        return Ok(Duration::ZERO);
    }

    if s.is_empty() {
        return Err(anyhow::anyhow!("invalid duration: {}", original));
    }

    while !s.is_empty() {
        // Consume a number (possibly floating point)
        let number_end = s
            .find(|c: char| c != '.' && !c.is_digit(10))
            .unwrap_or(s.len());
        let (number_s, rest) = s.split_at(number_end);
        let number = number_s.parse::<f64>()?;
        s = rest;

        // Consume a unit
        let unit_end = s
            .find(|c: char| c == '.' || c.is_digit(10))
            .unwrap_or(s.len());
        let (unit, rest) = s.split_at(unit_end);
        let unit_multiplicand = UNIT_MULTIPLICANDS
            .iter()
            .find_map(|(uname, mult)| if &unit == uname { Some(mult) } else { None })
            .ok_or_else(|| anyhow::anyhow!("invalid duration unit: {}", unit))?;
        s = rest;

        // TODO: Handle overflow here
        nanos += (number * unit_multiplicand) as u128;
    }

    if nanos == 0 {
        return Ok(Duration::ZERO);
    }

    // Rust's API does not permit constructing durations from u128 numbers, only u64.
    // Duration holds 96 bits internally, so by providing only the lowest 64 bits
    // we might lose some information.
    // Therefore, here we construct the duration manually.

    let mask = (1u128 << 64) - 1;
    let mut ret = Duration::from_nanos((nanos & mask) as u64);
    nanos >>= 64;

    if nanos > 0 {
        // The duration did not fit in 64 bits
        let mut high_part = Duration::from_nanos(nanos as u64);

        // We have to shift the high part by 64 bits, but we can multiply
        // by u32 numbers only, therefore we have to take three steps

        for step in [31, 31, 2] {
            high_part = high_part
                .checked_mul(1 << step)
                .ok_or_else(|| anyhow::anyhow!("duration overflow"))?;
        }

        ret = ret
            .checked_add(high_part)
            .ok_or_else(|| anyhow::anyhow!("duration overflow"))?;
    }

    Ok(ret)
}
