use std::ops::RangeInclusive;

use anyhow::Result;
use rand::distributions::Distribution as RandDistribution;
use rand_distr::Uniform;
use rand_pcg::Pcg64Mcg;

pub type RngGen = Pcg64Mcg;

pub struct DistributionContext {
    seq: u64,
    gen: RngGen,
}

impl DistributionContext {
    pub fn new(seq: u64, gen: RngGen) -> Self {
        DistributionContext { seq, gen }
    }

    pub fn get_seq(&self) -> u64 {
        self.seq
    }

    pub fn get_gen_mut(&mut self) -> &mut RngGen {
        &mut self.gen
    }
}

pub trait Distribution: Send + Sync {
    fn get_u64(&self, ctx: &mut DistributionContext) -> u64;
    fn get_f64(&self, ctx: &mut DistributionContext) -> f64 {
        self.get_u64(ctx) as f64
    }
}

pub struct FixedDistribution(pub u64);

impl Distribution for FixedDistribution {
    fn get_u64(&self, _ctx: &mut DistributionContext) -> u64 {
        self.0
    }
}

pub struct UniformDistribution(pub Uniform<u64>);

impl Distribution for UniformDistribution {
    fn get_u64(&self, ctx: &mut DistributionContext) -> u64 {
        self.0.sample(ctx.get_gen_mut())
    }
}

pub struct SequentialDistribution {
    base: u64,
    width: u64,
    divisor: u64,
}

impl SequentialDistribution {
    fn new(range: RangeInclusive<u64>) -> Self {
        Self::new_with_divisor(range, 1)
    }

    fn new_with_divisor(range: RangeInclusive<u64>, divisor: u64) -> Self {
        let width = range.end() - range.start();
        Self {
            base: *range.start(),
            width: width.wrapping_add(1),
            divisor,
        }
    }
}

impl Distribution for SequentialDistribution {
    fn get_u64(&self, ctx: &mut DistributionContext) -> u64 {
        let mut idx = ctx.get_seq() / self.divisor;
        if self.width > 0 {
            idx %= self.width;
        }
        self.base + idx
    }
}

// Parse distribution description in c-s or s-b style
pub fn parse_distribution(mut s: &str) -> Result<Box<dyn Distribution>> {
    // TODO: Use the inverted attribute
    let is_inverted = s.starts_with('~');
    if is_inverted {
        s = &s[1..];
    }

    // Locate the argument list and the distribution name
    let (dist_s, args_s) = if let Some(left_paren) = s.find('(') {
        anyhow::ensure!(
            s.ends_with(')'),
            "Missing closing parenthesis ')' for the distribution parameter list"
        );
        (&s[..left_paren], &s[left_paren + 1..s.len() - 1])
    } else if let Some(colon) = s.find(':') {
        (&s[..colon], &s[colon + 1..])
    } else {
        return Err(anyhow::anyhow!("Missing parameter list"));
    };

    let args: Vec<_> = args_s.split(',').collect();

    let ensure_exact_arg_count = |count: usize| {
        anyhow::ensure!(
            args.len() == count,
            "Expected exactly {} parameters for the fixed distribution, got {}",
            count,
            args.len()
        );
        Ok(())
    };

    match dist_s.trim().to_lowercase().as_str() {
        "fixed" => {
            ensure_exact_arg_count(1)?;
            let num = parse_number(args[0])?;
            Ok(Box::new(FixedDistribution(num)))
        }
        "uniform" => {
            ensure_exact_arg_count(1)?;
            let range = parse_range(args[0])?;
            Ok(Box::new(UniformDistribution(Uniform::new_inclusive(
                range.start(),
                range.end(),
            ))))
        }
        "seq" => {
            ensure_exact_arg_count(1)?;
            let range = parse_range(args[0])?;
            Ok(Box::new(SequentialDistribution::new(range)))
        }
        _ => unimplemented!(),
    }
}

fn parse_range(s: &str) -> Result<RangeInclusive<u64>> {
    match s.split_once("..") {
        Some((left, right)) => {
            let left = parse_number(left)?;
            let right = parse_number(right)?;
            Ok(left..=right)
        }
        None => Err(anyhow::anyhow!(
            "The distribution parameter is missing the '..' separator"
        )),
    }
}

fn parse_number(s: &str) -> Result<u64> {
    let last_char = s
        .trim()
        .chars()
        .rev()
        .next()
        .map(|c| c.to_ascii_lowercase());

    let mult = match last_char {
        Some('b') => Some(1_000_000_000),
        Some('m') => Some(1_000_000),
        Some('k') => Some(1_000),
        _ => None,
    };
    match mult {
        Some(mult) => {
            let s = &s[..s.len() - 1];
            let num: u64 = s.parse()?;
            Ok(mult * num)
        }
        None => Ok(s.parse()?),
    }
}
