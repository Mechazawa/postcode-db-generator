use sea_orm::entity::prelude::*;

/// See https://wiki.openstreetmap.org/wiki/Key:addr
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "node")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Double")]
    pub lat: f64,
    #[sea_orm(column_type = "Double")]
    pub lon: f64,
    pub city: Option<String>,
    pub country: Option<String>,
    pub postcode: String,
    pub street: Option<String>,
    pub province: Option<String>,
    pub state: Option<String>,
    pub house_number: Option<String>,
    pub house_name: Option<String>,
    pub source: Option<String>,
    pub source_date: Option<Date>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
