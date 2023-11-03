use sea_orm::DbBackend;
use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20231101_000000_create_nodes_table"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.create_table(Table::create()
            .table(Node::Table)
            .col(
                ColumnDef::new(Node::Id)
                    .big_unsigned()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Node::Lat).double().not_null())
            .col(ColumnDef::new(Node::Lon).double().not_null())
            .col(ColumnDef::new(Node::City).string())
            .col(ColumnDef::new(Node::Country).string())
            .col(ColumnDef::new(Node::Postcode).not_null().string())
            .col(ColumnDef::new(Node::Street).string())
            .col(ColumnDef::new(Node::HouseNumber).string())
            .col(ColumnDef::new(Node::Source).string())
            .col(ColumnDef::new(Node::SourceDate).date())
            .col(ColumnDef::new(Node::UpdatedAt).date_time().not_null())
            .col(ColumnDef::new(Node::Version).integer().not_null())
            .to_owned()).await?;

        if manager.get_database_backend() != DbBackend::Sqlite {
            manager.create_index(Index::create().if_not_exists().clone().name("idx-lat").table(Node::Table).col(Node::Lat).to_owned()).await?;
            manager.create_index(Index::create().if_not_exists().clone().name("idx-lon").table(Node::Table).col(Node::Lon).to_owned()).await?;
            manager.create_index(Index::create().if_not_exists().clone().name("idx-postcode").table(Node::Table).col(Node::Postcode).to_owned()).await?;
            manager.create_index(Index::create().if_not_exists().clone().name("idx-house_number").table(Node::Table).col(Node::HouseNumber).to_owned()).await?;
        }

        Ok(())
    }

    // Define how to rollback this migration: Drop the Bakery table.
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Node::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Node {
    Table,
    Id,
    Lat,
    Lon,
    City,
    Country,
    HouseNumber,
    Postcode,
    Street,
    Source,
    SourceDate,
    UpdatedAt,
    Version,
}
