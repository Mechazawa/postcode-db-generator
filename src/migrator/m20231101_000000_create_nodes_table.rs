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
    // Define how to apply this migration: Create the Bakery table.
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let mut table_definition = Table::create()
            .table(Node::Table)
            .col(
                ColumnDef::new(Node::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(Node::Lat).double().not_null())
            .col(ColumnDef::new(Node::Lon).double().not_null())
            .col(ColumnDef::new(Node::City).string())
            .col(ColumnDef::new(Node::Country).string())
            .col(ColumnDef::new(Node::Postcode).string())
            .col(ColumnDef::new(Node::Street).string())
            .col(ColumnDef::new(Node::HouseNumber).string())
            .col(ColumnDef::new(Node::Source).string())
            .col(ColumnDef::new(Node::SourceDate).date())
            .col(ColumnDef::new(Node::UpdatedAt).date_time().not_null())
            .col(ColumnDef::new(Node::Version).integer().not_null());

        if manager.get_database_backend() != DbBackend::Sqlite {
            table_definition
                .index(Index::create().col(Node::Lat))
                .index(Index::create().col(Node::Lon))
                .index(Index::create().col(Node::Postcode))
                .index(Index::create().col(Node::HouseNumber));
        }

        manager.create_table(table_definition.to_owned()).await
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
