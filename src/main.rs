mod migrator;
mod entities;

use clap::{Command, arg};
use futures::executor::block_on;
use sea_orm::{ConnectionTrait, Database, DbBackend, DbErr, Statement};
use sea_orm_migration::MigratorTrait;
use crate::migrator::Migrator;

fn cli() -> Command {
    Command::new("OSM postcode data importer")
        .about("Parses OSM XML metadata file and extracts postcodes to be stored in a database")
        .arg_required_else_help(true)
        .arg(arg!(--xml <XML>))
        .arg(arg!(--uri <DATABASE_URI>).default_value("sqlite://output.db"))
}

async fn run() -> Result<(), DbErr> {
    let matches = cli().get_matches();

    let db_url = matches.get_one::<String>("uri").expect("defaulted in clap");
    let db = Database::connect(db_url).await?;

    Migrator::refresh(&db).await?;

    let schema_manager = sea_orm_migration::SchemaManager::new(&db); // To investigate the schema
    assert!(schema_manager.has_table("node").await?);

    Ok(())
}

fn main() {
    if let Err(err) = block_on(run()) {
        panic!("{}", err);
    }
}