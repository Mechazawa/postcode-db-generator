use std::default::Default;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::{arg, Command};
use futures::future::join_all;
use sea_orm::{ActiveValue, ConnectionTrait, ConnectOptions, Database, DatabaseConnection, DbErr, EntityTrait, Iterable};
use sea_orm::prelude::DateTime;
use sea_orm::sea_query::OnConflict;
use sea_orm_migration::MigratorTrait;
use tokio;
use tokio::io::BufReader;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, ParserConfig2, XmlEvent};
use regex::Regex;

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

#[derive(Debug, Clone)]
enum ParsedElementEvent {
    Node(ParsedAttributeMap),
    Tag(String, String),
}
unsafe impl Send for ParsedElementEvent {}

#[derive(Default, Debug, Clone, Copy)]
struct ParsedAttributeMap {
    id: Option<u64>,
    lat: Option<f64>,
    lon: Option<f64>,
    version: Option<i32>,
    timestamp: Option<DateTime>,
}

async fn parse_file(db: Arc<DatabaseConnection>) -> std::io::Result<()> {
    // let parser = match path {
    //     Some(path) => EventReader::new(BufReader::new(File::open(path)?)),
    //     None => {
    //         let stdin = io::stdin();
    //
    //         EventReader::new(stdin.lock())
    //     }
    // };
    let now = chrono::offset::Local::now().naive_local();
    let re_addr = Regex::new("^addr:").unwrap();

    let parser_config = ParserConfig2::new()
        .trim_whitespace(true)
        .ignore_comments(true)
        .cdata_to_characters(false);

    let parser_buffer = std::io::BufReader::with_capacity(10_000_000, std::io::stdin());
    let parser = EventReader::new_with_config(parser_buffer, parser_config);

    let mut current_node: node::ActiveModel = Default::default();

    const BUFFER_SIZE: usize = 256;
    let mut buffer = Vec::with_capacity(BUFFER_SIZE);
    let mut futures = Vec::new();

    let mut current_province = None;
    let mut current_country = None;

    for raw_event in parser {
        if let Ok(XmlEvent::StartElement { name, attributes, .. }) = raw_event {
            if buffer.len() >= BUFFER_SIZE {
                let my_db = db.clone();

                let future = async move {
                    node::Entity::insert_many(buffer)
                        .on_conflict(
                            OnConflict::column(node::Column::Id)
                                .update_columns(node::Column::iter())
                                .to_owned()
                        ).exec::<DatabaseConnection>(my_db.as_ref()).await
                };

                futures.push(tokio::spawn(future));

                buffer = Vec::with_capacity(BUFFER_SIZE);
            }

            if futures.len() >= 128 {
                println!("Draining write queue...");
                join_all(futures.drain(..)).await;

                futures = Vec::new();
            }

            let event = match name.to_string().as_str() {
                "node" => ParsedElementEvent::Node({
                    let mut parsed = ParsedAttributeMap::default();

                    for OwnedAttribute { name, value } in &attributes {
                        match name.local_name.to_string().as_str() {
                            "id" => {parsed.id = Some(value.parse().unwrap())},
                            "lat" => {parsed.lat = Some(value.parse().unwrap())},
                            "lon" => {parsed.lon = Some(value.parse().unwrap())},
                            "version" => {parsed.version = Some(value.parse().unwrap())},
                            "timestamp" => {parsed.timestamp = Some(DateTime::from_str(&value.to_string()).unwrap_or_default())},
                            _ => {},
                            // v => {println!("Warning: skipped node key: {}", v);}
                        }
                    }

                    parsed
                }),
                "tag" => {
                    let mut tag_key = None;
                    let mut tag_value = None;

                    for OwnedAttribute{name, value} in &attributes {
                        match name.local_name.to_string().as_str() {
                            "k" => tag_key = Some(value.clone()),
                            "v" => tag_value = Some(value.clone()),
                            v => {println!("Warning: malformed tag key: {}", v);}
                        };
                    }

                    if tag_key.is_none() || tag_value.is_none() {
                        continue;
                    }

                    ParsedElementEvent::Tag(tag_key.unwrap(), tag_value.unwrap())
                },
                _ => continue,
            };

            match event {
                ParsedElementEvent::Node(attribute_map) => {
                    if node_ready(&current_node) {
                        buffer.push(current_node);
                    }

                    current_node = node::ActiveModel {
                        id: attribute_map.id.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr)),
                        lat: attribute_map.lat.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr)),
                        lon: attribute_map.lon.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr)),
                        version: attribute_map.version.map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr)),
                        updated_at: attribute_map.timestamp.map_or(ActiveValue::Set(now.clone()), |attr| ActiveValue::Set(attr)),
                        city: ActiveValue::Set(None),
                        country: ActiveValue::Set(current_country.clone()),
                        postcode: ActiveValue::NotSet,
                        house_number: ActiveValue::Set(None),
                        street: ActiveValue::Set(None),
                        province: ActiveValue::Set(current_province.clone()),
                        source: ActiveValue::Set(None),
                        source_date: ActiveValue::Set(None),
                    };
                }
                ParsedElementEvent::Tag(tag_key, value) => {
                    match re_addr.replace(tag_key.as_str(), "").to_string().as_str() {
                        "city" => current_node.city = ActiveValue::Set(Some(value.to_string())),
                        "country" => {
                            current_country = Some(value.to_string());

                            current_node.country = ActiveValue::Set(current_country.clone())
                        },
                        "housenumber" => current_node.house_number = ActiveValue::Set(Some(value.to_string().to_uppercase())),
                        "postcode" => current_node.postcode = ActiveValue::Set(value.to_string().to_uppercase().replace(" ", "")),
                        "street" => current_node.street = ActiveValue::Set(Some(value.to_string())),
                        "province" => {
                            current_province = Some(value.to_string());

                            current_node.province = ActiveValue::Set(current_province.clone());
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
        .exec(db.as_ref())
        .await
        .unwrap();

    join_all(futures.drain(..)).await;

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
    parse_file(db.clone()).await.unwrap();

    println!("Processing data");
    process_data(db.clone()).await.unwrap();
}
