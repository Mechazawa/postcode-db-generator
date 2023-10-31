use sea_orm_migration::prelude::*;
use sea_orm_migration::MigratorTrait;

mod m20231101_000000_create_nodes_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20231101_000000_create_nodes_table::Migration),
        ]
    }
}