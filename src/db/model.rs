use async_trait::async_trait;
use chrono::{DateTime, Utc};
use snafu::Snafu;
use std::convert::TryFrom;
use uuid::Uuid;

pub type EntityId = Uuid;

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(rename = "index_status")]
#[sqlx(rename_all = "snake_case")]
pub enum IndexStatus {
    NotAvailable,
    DownloadingInProgress,
    DownloadingError,
    Downloaded,
    ProcessingInProgress,
    ProcessingError,
    Processed,
    IndexingInProgress,
    IndexingError,
    Indexed,
    ValidationInProgress,
    ValidationError,
    Available,
}

/// An environment stored in the database
#[derive(Debug, Clone)]
pub struct EnvironmentEntity {
    pub id: EntityId,
    pub name: String,
    pub signature: String,
    pub port: i32,
    pub indexes: Vec<IndexEntity>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The input data necessary to create an environment.
#[derive(Debug, Clone)]
pub struct InputEnvironmentEntity {
    pub name: String,
    pub port: i32,
}

/// An index stored in the database
#[derive(Debug, Clone)]
pub struct IndexEntity {
    pub id: EntityId,
    pub index_type: String,
    pub data_source: String,
    pub regions: Vec<String>,
    pub signature: String,
    pub status: IndexStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The input data necessary to create an index.
#[derive(Debug, Clone)]
pub struct InputIndexEntity {
    pub environment: EntityId,
    pub index_type: String,
    pub data_source: String,
    pub regions: Vec<String>,
}

// From sqlx realworld example
#[async_trait]
pub trait ProvideData {
    async fn get_all_environments(&mut self) -> ProvideResult<Vec<EnvironmentEntity>>;

    async fn get_environment_indexes(
        &mut self,
        environment: &Uuid,
    ) -> ProvideResult<Vec<IndexEntity>>;

    async fn create_environment(
        &mut self,
        environment: &InputEnvironmentEntity,
    ) -> ProvideResult<EnvironmentEntity>;

    async fn delete_environment(&mut self, environment: &Uuid) -> ProvideResult<EnvironmentEntity>;

    async fn create_index(&mut self, index: &InputIndexEntity) -> ProvideResult<IndexEntity>;
}

pub type ProvideResult<T> = Result<T, ProvideError>;

/// An error returned by a provider
#[derive(Debug, Snafu)]
pub enum ProvideError {
    /// The requested entity does not exist
    #[snafu(display("Entity does not exist"))]
    #[snafu(visibility(pub))]
    NotFound,

    /// The operation violates a uniqueness constraint
    #[snafu(display("Operation violates uniqueness constraint: {}", details))]
    #[snafu(visibility(pub))]
    UniqueViolation { details: String },

    /// The requested operation violates the data model
    #[snafu(display("Operation violates model: {}", details))]
    #[snafu(visibility(pub))]
    ModelViolation { details: String },

    /// The requested operation violates the data model
    #[snafu(display("UnHandled Error: {}", source))]
    #[snafu(visibility(pub))]
    UnHandledError { source: sqlx::Error },
}

impl From<sqlx::Error> for ProvideError {
    /// Convert a SQLx error into a provider error
    ///
    /// For Database errors we attempt to downcast
    ///
    /// FIXME(RFC): I have no idea if this is sane
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::RowNotFound => ProvideError::NotFound,
            sqlx::Error::Database(db_err) => {
                if let Some(pg_err) = db_err.try_downcast_ref::<sqlx::postgres::PgError>() {
                    if let Ok(provide_err) = ProvideError::try_from(pg_err) {
                        provide_err
                    } else {
                        ProvideError::UnHandledError {
                            source: sqlx::Error::Database(db_err),
                        }
                    }
                } else {
                    ProvideError::UnHandledError {
                        source: sqlx::Error::Database(db_err),
                    }
                }
            }
            _ => ProvideError::UnHandledError { source: e },
        }
    }
}
