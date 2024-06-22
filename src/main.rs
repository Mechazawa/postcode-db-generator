use std::cmp::max;
use std::collections::HashMap;
use std::default::Default;
use std::io;
use std::str::FromStr;
use std::time::Duration;

use clap::{arg, Command};
use sea_orm::{ActiveValue, ConnectionTrait, ConnectOptions, Database, DbErr, EntityTrait, Iterable};
use sea_orm::prelude::DateTime;
use sea_orm::sea_query::OnConflict;
use sea_orm_migration::MigratorTrait;
use tokio;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};

use crate::entities::*;
use crate::migrator::Migrator;

mod migrator;
mod entities;

fn cli() -> Command {
    Command::new("OSM postcode data importer")
        .about("Parses OSM XML metadata file and extracts postcodes to be stored in a database\npipe the xml into stdin to process it. You can use tools like `pv` to monitor progress.")
        // .arg(arg!(--xml <XML>))
        .arg(arg!(--fresh))
        .arg(arg!(--db <DATABASE_URI>).default_value("sqlite://output.db"))
}

async fn build_db(db_uri: &str, fresh: bool) -> Result<(), DbErr> {
    let db = Database::connect(db_uri).await?;
    let schema_manager = sea_orm_migration::SchemaManager::new(&db);

    if fresh {
        println!("Recreating database!");
        Migrator::refresh(&db).await?;
    } else {
        Migrator::up(&db, None).await?;
    }

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

fn node_ready(node: &node::ActiveModel) -> bool {
    node.id.is_set() && node.postcode.is_set() && node.street.is_set()
}

#[derive(Debug, Clone)]
enum ParsedElementEvent {
    Node(HashMap<String, String>),
    Tag(String, String),
}
unsafe impl Send for ParsedElementEvent {}

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

    let mut current_node: node::ActiveModel = Default::default();

    const BUFFER_SIZE: usize = 500;
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);

    let mut current_province = None;

    for raw_event in parser {
        if let Ok(XmlEvent::StartElement { name, attributes, .. }) = raw_event {
            if buffer.len() >= BUFFER_SIZE {
                node::Entity::insert_many(buffer.drain(..))
                    .on_conflict(OnConflict::column(node::Column::Id).update_columns(node::Column::iter()).to_owned())
                    .exec(&db).await.unwrap();

                buffer.reserve(max(BUFFER_SIZE - buffer.capacity(), 0));
            }

            let event = match name.to_string().as_str() {
                "node" => ParsedElementEvent::Node(
                    HashMap::from_iter(attributes.iter().map(|attr| (attr.name.local_name.clone(), attr.value.clone())))
                ),
                "tag" => {
                    let tag_key = find_attr("k", &attributes)
                        .map(|attr| attr.value.to_string())
                        .expect("tags have keys");

                    let value = find_attr("v", &attributes);

                    if value.is_none() {
                        continue;
                    }

                    ParsedElementEvent::Tag(tag_key, value.unwrap().to_string())
                },
                _ => continue,
            };

            match event {
                ParsedElementEvent::Node(attribute_map) => {
                    if node_ready(&current_node) {
                        buffer.push(current_node);
                    }

                    current_node = node::ActiveModel {
                        id: attribute_map.get("id").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.parse().unwrap())),
                        lat: attribute_map.get("lat").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.parse().unwrap())),
                        lon: attribute_map.get("lon").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.parse().unwrap())),
                        version: attribute_map.get("version").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.parse().unwrap())),
                        // updated_at: ActiveValue::Set(DateTime::default()),
                        updated_at: attribute_map.get("timestamp").map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(DateTime::from_str(&attr.to_string()).unwrap_or_default())),
                        city: ActiveValue::Set(None),
                        country: ActiveValue::Set(Some("NL".to_string())),
                        postcode: ActiveValue::NotSet,
                        house_number: ActiveValue::Set(None),
                        street: ActiveValue::Set(None),
                        province: ActiveValue::Set(current_province.clone()),
                        source: ActiveValue::Set(None),
                        source_date: ActiveValue::Set(None),
                    };
                }
                ParsedElementEvent::Tag(tag_key, value) => {
                    match tag_key.as_str() {
                        "addr:city" | "city" => current_node.city = ActiveValue::Set(Some(value.to_string())),
                        "addr:country" | "country" => current_node.country = ActiveValue::Set(Some(value.to_string())),
                        "addr:housenumber" | "housenumber" => current_node.house_number = ActiveValue::Set(Some(value.to_string().to_uppercase())),
                        "addr:postcode" | "postcode" => current_node.postcode = ActiveValue::Set(value.to_string().to_uppercase().replace(" ", "")),
                        "addr:street" | "street" => current_node.street = ActiveValue::Set(Some(value.to_string())),
                        "addr:province" | "province" => {
                            let province = ActiveValue::Set(Some(value.clone()));

                            current_node.province = province.clone();
                            current_province = province.unwrap();
                        },
                        "source" => current_node.source = ActiveValue::Set(Some(value.clone())),
                        // "source:date" => current_node.source_date = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                        _ => (),
                    }
                }
            }
        }
    }

    if node_ready(&current_node) {
        buffer.push(current_node);
    }

    println!("Waiting for writes to finish...");
    node::Entity::insert_many(buffer.drain(..))
        .on_conflict(OnConflict::column(node::Column::Id).update_columns(node::Column::iter()).to_owned())
        .exec(&db)
        .await
        .unwrap();

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
    build_db(db_uri, matches.get_flag("fresh")).await.unwrap();

    println!("Parsing file");
    parse_file(db_uri).await.unwrap();

    println!("Processing data");
    process_data(db_uri).await.unwrap();
}