use anyhow::Result;
use scylla::{statement::prepared_statement::PreparedStatement, Session};

use crate::distribution::{Distribution, DistributionContext};
use crate::generator::ValueGenerator;

// TODO: Move the schema elsewhere

pub struct Schema {
    keyspace: String,
    table: String,

    replication_class: String,
    replication_factor: u32,

    pk_count: usize,
    ck_count: usize,

    types: Vec<String>,
}

// impl Schema {
//     fn to_create_statement(&self) -> String {}
// }

pub trait Workload: Sync {
    fn create_schema(&self, session: &Session) -> Result<()>;
    fn prepare(&mut self, session: &Session) -> Result<()>;
    fn work(&self, session: &Session, ctx: DistributionContext) -> Result<()>;
}

pub struct WriteWorkload {
    generators: Vec<Box<dyn ValueGenerator>>,
    stmt: Option<PreparedStatement>,

    schema: Schema,
}

// impl Workload for WriteWorkload {

// }
