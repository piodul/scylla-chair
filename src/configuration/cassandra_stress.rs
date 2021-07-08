use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use rand_distr::uniform::SampleUniform;

use crate::configuration::{BenchDescription, WorkloadMode, WorkloadType};
use crate::distribution::{self, Distribution};

// TODO: This whole module is work in progress

struct CsConfig {
    op_count: Option<u64>,
}

// Parses command line in the cassandra-stress format.
pub fn parse(opts: &[&str]) -> Result<Option<BenchDescription>> {
    anyhow::ensure!(!opts.is_empty(), "Missing command");

    match opts[0].to_lowercase().as_ref() {
        "write" => {
            todo!();
        }
        "read" => {
            todo!();
        }
        "counter_read" => {
            return Err(anyhow::anyhow!("Counter read mode is not supported"));
        }
        "counter_write" => {
            return Err(anyhow::anyhow!("Counter write mode is not supported"));
        }
        "mixed" => {
            todo!();
        }
        "user" => {
            todo!();
        }
        "help" => {
            todo!();
        }
        "version" => {
            todo!();
        }
        "print" => {
            todo!();
        }
        "legacy" => {
            return Err(anyhow::anyhow!("Legacy mode is not supported"));
        }
        other => {
            return Err(anyhow::anyhow!(
                "Unknown cassandra-stress command: {}",
                other
            ));
        }
    }

    let (main_options, suboptions) = parse_opts_map(&opts[1..])?;

    todo!()
}

#[allow(clippy::type_complexity)]
fn parse_opts_map<'a, 'b>(
    opts: &'b [&'a str],
) -> Result<(&'b [&'a str], HashMap<String, &'b [&'a str]>)> {
    let scan_opts = |pos: &mut usize| -> Result<&'b [&'a str]> {
        let start = *pos;
        while *pos < opts.len() && !opts[*pos].starts_with('-') {
            *pos += 1;
        }

        Ok(&opts[start..*pos])
    };

    let mut pos = 0;
    let mut subopts_map = HashMap::new();

    let main_opts = scan_opts(&mut pos).context("Failed to parse the main options")?;

    while pos < opts.len() {
        let opt_name = opts[pos][1..].to_lowercase();
        if subopts_map.contains_key(opt_name.as_str()) {
            return Err(anyhow::anyhow!(
                "The option {:?} was provided more than once",
                opt_name
            ));
        }

        pos += 1;
        let subopts = scan_opts(&mut pos)
            .with_context(|| format!("Failed to parse sub-options for option {:?}", opt_name))?;
        subopts_map.insert(opt_name, subopts);
    }

    Ok((main_opts, subopts_map))
}

type SubOptDesc<'target> = Box<dyn FnMut(&str) -> Result<bool> + 'target>;

fn parse_subopts<'target, 'opts>(
    defs: &mut [SubOptDesc<'target>],
    raw_opts: &[&'opts str],
) -> Result<()> {
    let mut used_opts = HashMap::new();

    'outer: for opt in raw_opts {
        for (id, def) in defs.iter_mut().enumerate() {
            if !(*def)(opt)? {
                continue;
            }

            match used_opts.get(&id) {
                Some(previous) => {
                    return Err(anyhow::anyhow!(
                        "The sub-option {:?} would override previous sub-option {:?}",
                        opt,
                        previous
                    ));
                }
                None => {
                    used_opts.insert(id, opt);
                    continue 'outer;
                }
            }
        }

        return Err(anyhow::anyhow!("Unrecognized option: {:?}", opt));
    }

    Ok(())
}

fn sub_opt_flag<'target>(name: &'static str, target: &'target mut bool) -> SubOptDesc<'target> {
    Box::new(move |s| {
        if s == name {
            *target = true;
            Ok(true)
        } else {
            Ok(false)
        }
    })
}

fn sub_opt_pfx_from_str<'target, T: FromStr>(
    pfx: &'static str,
    target: &'target mut T,
) -> SubOptDesc<'target>
where
    T::Err: std::error::Error + Sync + Send + 'static,
{
    Box::new(move |s| {
        if !s.starts_with(pfx) {
            return Ok(false);
        }
        *target = s[pfx.len()..]
            .parse()
            .with_context(|| format!("Cannot parse option {:?}", s))?;
        Ok(true)
    })
}

fn sub_opt_pfx_str<'target>(pfx: &'static str, target: &'target mut String) -> SubOptDesc<'target> {
    Box::new(move |s| {
        if !s.starts_with(pfx) {
            return Ok(false);
        }
        *target = s[pfx.len()..].to_string();
        Ok(true)
    })
}

fn sub_opt_pfx_conversion<'target, T>(
    pfx: &'static str,
    target: &'target mut T,
    conv: impl Fn(&str) -> Result<T> + 'target,
) -> SubOptDesc<'target> {
    Box::new(move |s| {
        if !s.starts_with(pfx) {
            return Ok(false);
        }
        *target = conv(&s[pfx.len()..])?;
        Ok(true)
    })
}
