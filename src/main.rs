use std::default::Default;
use std::sync::Arc;

use clap::{arg, Command};
use osmpbf::{DenseNode, Element, ElementReader};
use sea_orm::{ActiveValue, ConnectionTrait, ConnectOptions, Database, DatabaseConnection, DbErr};
use sea_orm_migration::MigratorTrait;
use tokio::time::Duration;

use crate::batch_insert::BatchInsert;
use crate::entities::*;
use crate::migrator::Migrator;

mod migrator;
mod entities;

mod batch_insert;

fn cli() -> Command {
    Command::new("OSM postcode data importer")
        .about("Parses OSM XML metadata file and extracts postcodes to be stored in a database\npipe the xml into stdin to process it. You can use tools like `pv` to monitor progress.")
        // .arg(arg!(--xml <XML>))
        .arg(arg!(--fresh))
        .arg(arg!(--country <COUNTRY>)).about("Default country")
        .arg(arg!(--db <DATABASE_URI>).default_value("sqlite://output.db"))
}

async fn build_db(db: Arc<DatabaseConnection>, fresh: bool) -> Result<(), DbErr> {
    let schema_manager = sea_orm_migration::SchemaManager::new(db.as_ref());

    if fresh {
        println!("Recreating database!");
        Migrator::refresh(db.as_ref()).await?;
    } else {
        Migrator::up(db.as_ref(), None).await?;
    }

    // To investigate the schema
    assert!(schema_manager.has_table("node").await?);

    if schema_manager.has_table("node_uniq").await? {
        db.execute_unprepared("DROP TABLE `node_uniq`").await?;
    }

    Ok(())
}

fn node_ready(node: &node::ActiveModel) -> bool {
    node.id.is_set() && node.postcode.is_set() && node.street.is_set()
}

impl From<DenseNode<'_>> for node::ActiveModel {
    fn from(value: DenseNode<'_>) -> Self {
        let mut result = node::ActiveModel {
            id: ActiveValue::set(value.id()),
            lat: ActiveValue::set(value.lat()),
            lon: ActiveValue::set(value.lon()),
            city: ActiveValue::Set(None),
            country: ActiveValue::NotSet,
            province: ActiveValue::Set(None),
            state: ActiveValue::Set(None),
            house_number: ActiveValue::Set(None),
            house_name: ActiveValue::Set(None),
            source: ActiveValue::Set(None),
            source_date: ActiveValue::Set(None),
            updated_at: ActiveValue::Set(None),
            created_at: ActiveValue::Set(None),
            ..node::ActiveModel::default()
        };

        for tag in value.tags() {
            match tag {
                ("addr:city", value) => result.city = ActiveValue::set(Some(value.into())),
                ("addr:country", value) => result.country = ActiveValue::set(Some(value.into())),
                ("addr:postcode", value) => result.postcode = ActiveValue::set(value.replace(" ", "").to_uppercase()),
                ("addr:street", value) => result.street = ActiveValue::set(Some(value.into())),
                ("addr:province", value) => result.province = ActiveValue::set(Some(value.into())),
                ("addr:housenumber", value) => result.house_number = ActiveValue::set(Some(value.replace(" ", ""))),
                ("addr:state", value) => result.state = ActiveValue::Set(Some(value.into())),
                ("addr:housename", value) => result.house_name = ActiveValue::Set(Some(value.into())),
                _ => {},
            }
        }

        result
    }
}

async fn parse_file(db: Arc<DatabaseConnection>, default_country: Option<String>) -> std::io::Result<()> {
    let reader = ElementReader::from_path("/dev/stdin")?;
    let mut batcher = BatchInsert::new(db.clone(), 2000, 4);

    reader.for_each(
        |element| {
            if let Some(mut model) = match element {
                Element::DenseNode(data) => Some(node::ActiveModel::from(data)),
                // Element::Node(data) => Some(node::ActiveModel::from(data)),
                _ => None,
            } {
                if node_ready(&model) {
                    if model.country.is_not_set() {
                        model.country = ActiveValue::set(default_country.clone());
                    }

                    batcher.insert(model);
                }
            }
        }
    )?;

    println!("Waiting for writes to finish...");
    batcher.flush();

    Ok(())
}

async fn process_data(db: Arc<DatabaseConnection>) -> Result<(), DbErr> {
    println!("Build uniq table");
    db.execute_unprepared("CREATE TABLE node_uniq AS SELECT id, AVG(lat) as lat, AVG(lon) as lon, city, country, postcode, province, street, source, source_date, updated_at, version FROM node GROUP BY postcode HAVING count(distinct street) = 1").await?;

    println!("Index uniq table");
    db.execute_unprepared("CREATE INDEX idx_node_uniq_postcode ON node_uniq(postcode)").await?;

    println!("Remove duplicates");
    db.execute_unprepared("DELETE FROM node WHERE postcode IN (SELECT postcode FROM node_uniq)").await?;

    println!("Re-insert normalized unique postcodes");
    db.execute_unprepared("INSERT INTO node (id, lat, lon, city, country, postcode, province, street, house_number, source, source_date, updated_at, version) SELECT id, lat, lon, city, country, postcode, province, street, null, source, source_date, updated_at, version FROM node_uniq").await?;

    println!("Cleanup, removing node_uniq");
    db.execute_unprepared("DROP TABLE node_uniq").await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    let matches = cli().get_matches();
    let db_uri = matches.get_one::<String>("db").expect("defaulted in clap");

    let mut db_opt = ConnectOptions::new(db_uri);

    db_opt.max_connections(128)
        .acquire_timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10));

    let db = Arc::new(Database::connect(db_opt).await.unwrap());

    println!("Building database");
    build_db(db.clone(), matches.get_flag("fresh")).await.unwrap();

    println!("Parsing file");
    parse_file(db.clone(), matches.get_one::<String>("country").cloned()).await.unwrap();

    println!("Processing data");
    process_data(db.clone()).await.unwrap();
}
