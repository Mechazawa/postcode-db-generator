use std::collections::{BTreeMap, HashMap};
use std::default::Default;
use std::io;
use std::str::FromStr;
use std::time::Duration;

use clap::{arg, Command};
use sea_orm::{ActiveModelTrait, ActiveValue, ConnectionTrait, ConnectOptions, Database, DbErr};
use sea_orm::prelude::DateTime;
use sea_orm_migration::MigratorTrait;
use tokio;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};

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
        .arg(arg!(--db <DATABASE_URI>).default_value("sqlite://output.db"))
}

async fn build_db(db_uri: &str) -> Result<(), DbErr> {
    let db = Database::connect(db_uri).await?;
    let schema_manager = sea_orm_migration::SchemaManager::new(&db);

    // Migrator::refresh(&db).await?;
    // Migrator::up(&db, None).await?;

    // To investigate the schema
    assert!(schema_manager.has_table("node").await?);

    if schema_manager.has_table("node_uniq").await? {
        db.execute_unprepared("DROP TABLE `node_uniq`").await?;
    }

    Ok(())
}

fn find_attr<'a>(name: &str, attributes: &'a [OwnedAttribute]) -> Option<&'a OwnedAttribute> {
    attributes.iter()
        .find(|attr| attr.name.to_string() == name)
}

fn map_attr(attributes: &[OwnedAttribute]) -> BTreeMap<&str, &OwnedAttribute> {
    BTreeMap::from_iter(attributes.iter().map(|attr| (attr.name.local_name.as_str(), attr)))
}

fn node_ready(node: &node::ActiveModel) -> bool {
    node.id.is_set() &&
        node.lat.is_set() &&
        node.lon.is_set() &&
        node.postcode.is_set() &&
        node.version.is_set() &&
        node.updated_at.is_set()
}

async fn parse_file(db_uri: &str) -> std::io::Result<()> {
    // let parser = match path {
    //     Some(path) => EventReader::new(BufReader::new(File::open(path)?)),
    //     None => {
    //         let stdin = io::stdin();
    //
    //         EventReader::new(stdin.lock())
    //     }
    // };

    let stdin = io::stdin();

    let parser = EventReader::new(stdin.lock());

    let mut db_opt = ConnectOptions::new(db_uri);

    db_opt.max_connections(128)
        .acquire_timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(10));

    let db = Database::connect(db_opt).await.unwrap();
    tokio::s

    let mut current_node: node::ActiveModel = Default::default();

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                match name.to_string().as_str() {
                    "node" => {
                        if node_ready(&current_node) {

                        }

                        let attribute_map = map_attr(&attributes);

                        current_node = node::ActiveModel {
                            id: attribute_map.get(&"id").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            lat: attribute_map.get(&"lat").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            lon: attribute_map.get(&"lon").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            version: attribute_map.get(&"version").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            // updated_at: ActiveValue::Set(DateTime::default()),
                            updated_at: attribute_map.get(&"timestamp").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(DateTime::from_str(&attr.value.to_string()).unwrap_or_default())),
                            city: ActiveValue::Set(None),
                            country: ActiveValue::Set(Some("NL".to_string())),
                            postcode: ActiveValue::NotSet,
                            house_number: ActiveValue::Set(None),
                            street: ActiveValue::Set(None),
                            province: ActiveValue::Set(None),
                            source: ActiveValue::Set(None),
                            source_date: ActiveValue::Set(None),
                        };
                    }
                    "tag" => {
                        let tag_key = find_attr("k", &attributes)
                            .map(|attr| attr.value.as_str())
                            .expect("tags have keys");

                        let value = find_attr("v", &attributes);

                        match tag_key {
                            "addr:city" => current_node.city = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "addr:country" => current_node.country = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "addr:housenumber" => current_node.house_number = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string().to_uppercase()))),
                            "addr:postcode" => current_node.postcode = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.to_string().to_uppercase().replace(" ", ""))),
                            "addr:street" => current_node.street = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "addr:province" => current_node.province = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "province" => current_node.province = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "source" => current_node.source = value.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            // "source:date" => current_node.source_date = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            _ => (),
                        }
                    }
                    _ => {}
                };
            }
            // Ok(XmlEvent::EndElement { name }) => {
            //     depth -= 1;
            //     println!("{:spaces$}-{name}", "", spaces = depth * 2);
            // }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
            // There's more: https://docs.rs/xml-rs/latest/xml/reader/enum.XmlEvent.html
            _ => {}
        }
    }

    if node_ready(&current_node) {
        batcher.insert(current_node).await;
    }

    batcher.flush().await;

    println!("Waiting for writes to finish...");

    Ok(())
}

async fn process_data(db_uri: &str) -> Result<(), DbErr> {
    let db = Database::connect(db_uri).await?;

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

    println!("Building database");
    build_db(db_uri).await.unwrap();

    println!("Parsing file");
    parse_file(db_uri).await.unwrap();

    println!("Processing data");
    process_data(db_uri).await.unwrap();
}