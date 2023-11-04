use std::default::Default;
use std::io;
use std::str::FromStr;

use clap::{arg, Command};
use sea_orm::{ActiveValue, Database, DbErr};
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

    Migrator::refresh(&db).await?;
    // Migrator::up(&db).await?;

    // To investigate the schema
    let schema_manager = sea_orm_migration::SchemaManager::new(&db);
    assert!(schema_manager.has_table("node").await?);

    Ok(())
}

fn find_attr<'a>(name: &str, attributes: &'a [OwnedAttribute]) -> Option<&'a OwnedAttribute> {
    attributes.iter()
        .find(|attr| attr.name.to_string() == name)
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

    let db = Database::connect(db_uri).await.unwrap();

    let mut current_node: node::ActiveModel = Default::default();

    let mut batcher = BatchInsert::new(db.clone(), 100);

    for e in parser {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) => {
                match name.to_string().as_str() {
                    "node" => {
                        if node_ready(&current_node) {
                            batcher.insert(current_node).await;
                        }

                        current_node = node::ActiveModel {
                            id: find_attr("id", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            lat: find_attr("lat", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            lon: find_attr("lon", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            version: find_attr("version", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.parse().unwrap())),
                            // updated_at: ActiveValue::Set(DateTime::default()),
                            updated_at: find_attr("timestamp", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(DateTime::from_str(&attr.value.to_string()).unwrap_or_default())),
                            city: ActiveValue::Set(None),
                            country: ActiveValue::Set(Some("NL".to_string())),
                            postcode: ActiveValue::NotSet,
                            house_number: ActiveValue::Set(None),
                            street: ActiveValue::Set(None),
                            source: ActiveValue::Set(None),
                            source_date: ActiveValue::Set(None),
                        };
                    }
                    "tag" => {
                        let tag_key = find_attr("k", &attributes)
                            .map(|attr| attr.value.as_str())
                            .expect("tags have keys");

                        match tag_key {
                            "addr:city" => current_node.city = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "addr:country" => current_node.country = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "addr:housenumber" => current_node.house_number = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string().to_uppercase()))),
                            "addr:postcode" => current_node.postcode = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(attr.value.to_string().to_uppercase())),
                            "addr:street" => current_node.street = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
                            "source" => current_node.source = find_attr("v", &attributes).map_or(ActiveValue::NotSet, |attr| ActiveValue::Set(Some(attr.value.to_string()))),
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
    batcher.join().await;

    Ok(())
}

#[tokio::main]
async fn main() {
    let matches = cli().get_matches();
    let db_uri = matches.get_one::<String>("db").expect("defaulted in clap");

    build_db(db_uri).await.unwrap();

    // let xml_path = matches.get_one::<String>("xml");

    parse_file(db_uri).await.unwrap();
}