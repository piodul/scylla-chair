use std::sync::Arc;

use scylla::frame::response::result::CqlValue;

use crate::distribution::{Distribution, DistributionContext};

pub trait ValueGenerator: Send + Sync {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue;
}

pub struct BigIntGenerator {
    value_dist: Arc<dyn Distribution>,
}

impl ValueGenerator for BigIntGenerator {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue {
        CqlValue::BigInt(self.value_dist.get_u64(ctx) as i64)
    }
}

pub struct IntGenerator {
    value_dist: Arc<dyn Distribution>,
}

impl ValueGenerator for IntGenerator {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue {
        CqlValue::Int(self.value_dist.get_u64(ctx) as i32)
    }
}

pub struct SmallIntGenerator {
    value_dist: Arc<dyn Distribution>,
}

impl ValueGenerator for SmallIntGenerator {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue {
        CqlValue::SmallInt(self.value_dist.get_u64(ctx) as i16)
    }
}

pub struct TinyIntGenerator {
    value_dist: Arc<dyn Distribution>,
}

impl ValueGenerator for TinyIntGenerator {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue {
        CqlValue::TinyInt(self.value_dist.get_u64(ctx) as i8)
    }
}

pub struct BlobGenerator {
    value_dist: Arc<dyn Distribution>,
    length_dist: Arc<dyn Distribution>,
}

impl ValueGenerator for BlobGenerator {
    fn generate(&self, ctx: &mut DistributionContext) -> CqlValue {
        let dest_len = self.length_dist.get_u64(ctx) as usize;
        let mut ret = Vec::with_capacity((dest_len + 7) / 8 * 8);
        while ret.len() < dest_len {
            ret.extend_from_slice(&self.value_dist.get_u64(ctx).to_le_bytes());
        }
        ret.truncate(dest_len);
        CqlValue::Blob(ret)
    }
}
