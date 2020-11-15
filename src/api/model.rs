use chrono::{DateTime, Utc};
use futures::stream::{self, TryStreamExt};
use juniper::futures::TryFutureExt;
use juniper::{GraphQLEnum, GraphQLInputObject, GraphQLObject};
use serde::{Deserialize, Serialize};
use slog::debug;
use snafu::ResultExt;
use sqlx::Connection;
use std::convert::TryFrom;
use uuid::Uuid;

use crate::api::gql::Context;
use crate::db::model as db;
use crate::db::model::ProvideData;
use crate::db::Db;
use crate::docker;
use crate::error;

#[derive(Debug, PartialEq, Serialize, Deserialize, GraphQLEnum)]
#[serde(rename_all = "camelCase")]
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

impl From<IndexStatus> for db::IndexStatus {
    fn from(status: IndexStatus) -> Self {
        match status {
            IndexStatus::NotAvailable => db::IndexStatus::NotAvailable,
            IndexStatus::DownloadingInProgress => db::IndexStatus::DownloadingInProgress,
            IndexStatus::DownloadingError => db::IndexStatus::DownloadingError,
            IndexStatus::Downloaded => db::IndexStatus::Downloaded,
            IndexStatus::ProcessingInProgress => db::IndexStatus::ProcessingInProgress,
            IndexStatus::ProcessingError => db::IndexStatus::ProcessingError,
            IndexStatus::Processed => db::IndexStatus::Processed,
            IndexStatus::IndexingInProgress => db::IndexStatus::IndexingInProgress,
            IndexStatus::IndexingError => db::IndexStatus::IndexingError,
            IndexStatus::Indexed => db::IndexStatus::Indexed,
            IndexStatus::ValidationInProgress => db::IndexStatus::ValidationInProgress,
            IndexStatus::ValidationError => db::IndexStatus::ValidationError,
            IndexStatus::Available => db::IndexStatus::Available,
        }
    }
}

impl From<db::IndexStatus> for IndexStatus {
    fn from(status: db::IndexStatus) -> Self {
        match status {
            db::IndexStatus::NotAvailable => IndexStatus::NotAvailable,
            db::IndexStatus::DownloadingInProgress => IndexStatus::DownloadingInProgress,
            db::IndexStatus::DownloadingError => IndexStatus::DownloadingError,
            db::IndexStatus::Downloaded => IndexStatus::Downloaded,
            db::IndexStatus::ProcessingInProgress => IndexStatus::ProcessingInProgress,
            db::IndexStatus::ProcessingError => IndexStatus::ProcessingError,
            db::IndexStatus::Processed => IndexStatus::Processed,
            db::IndexStatus::IndexingInProgress => IndexStatus::IndexingInProgress,
            db::IndexStatus::IndexingError => IndexStatus::IndexingError,
            db::IndexStatus::Indexed => IndexStatus::Indexed,
            db::IndexStatus::ValidationInProgress => IndexStatus::ValidationInProgress,
            db::IndexStatus::ValidationError => IndexStatus::ValidationError,
            db::IndexStatus::Available => IndexStatus::Available,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, GraphQLObject)]
#[serde(rename_all = "camelCase")]
pub struct Environment {
    pub id: Uuid,
    pub name: String,
    pub signature: String,
    pub port: i32, // We should use u16, but it does not implement GraphQLType
    pub indexes: Vec<Index>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<db::EnvironmentEntity> for Environment {
    fn from(entity: db::EnvironmentEntity) -> Self {
        let db::EnvironmentEntity {
            id,
            name,
            signature,
            port,
            indexes,
            created_at,
            updated_at,
            ..
        } = entity;

        let indexes = indexes.into_iter().map(Index::from).collect::<Vec<Index>>();
        // let port = u16::try_from(port).expect("casting port");
        Environment {
            id,
            name,
            signature,
            port,
            indexes,
            created_at,
            updated_at,
        }
    }
}

pub fn default_status() -> IndexStatus {
    IndexStatus::NotAvailable
}

#[derive(Debug, PartialEq, Serialize, Deserialize, GraphQLObject)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub id: Uuid,
    pub index_type: String,
    pub data_source: String,
    pub regions: Vec<String>,
    pub signature: String,
    #[serde(default = "default_status")]
    pub status: IndexStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<db::IndexEntity> for Index {
    fn from(entity: db::IndexEntity) -> Self {
        let db::IndexEntity {
            id,
            index_type,
            data_source,
            regions,
            signature,
            status,
            created_at,
            updated_at,
            ..
        } = entity;

        Index {
            id,
            index_type,
            data_source,
            regions,
            signature,
            status: IndexStatus::from(status),
            created_at,
            updated_at,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, GraphQLObject)]
#[serde(rename_all = "camelCase")]
pub struct SingleEnvironmentResponseBody {
    pub env: Option<Environment>,
}

impl From<Environment> for SingleEnvironmentResponseBody {
    fn from(env: Environment) -> Self {
        Self { env: Some(env) }
    }
}

/// The response body for multiple environments
#[derive(Debug, Deserialize, Serialize, GraphQLObject)]
#[serde(rename_all = "camelCase")]
pub struct MultiEnvironmentsResponseBody {
    pub envs: Vec<Environment>,
    pub envs_count: i32,
}

impl From<Vec<Environment>> for MultiEnvironmentsResponseBody {
    fn from(envs: Vec<Environment>) -> Self {
        let envs_count = i32::try_from(envs.len()).unwrap();
        Self { envs, envs_count }
    }
}

#[derive(Debug, Deserialize, Serialize, GraphQLInputObject)]
pub struct EnvironmentRequestBody {
    pub name: String,
}

impl From<EnvironmentRequestBody> for db::InputEnvironmentEntity {
    fn from(request: EnvironmentRequestBody) -> Self {
        let EnvironmentRequestBody { name, .. } = request;

        // At this stage, when the user request an environment, the
        // port is not known. So we set it to 0, and it will be assigned
        // a value before it beeing used.
        db::InputEnvironmentEntity { name, port: 0i32 }
    }
}

#[derive(Debug, Deserialize, Serialize, GraphQLInputObject)]
pub struct EnvironmentIdBody {
    pub id: Uuid,
}

#[derive(Debug, Deserialize, Serialize, GraphQLObject)]
#[serde(rename_all = "camelCase")]
pub struct SingleIndexResponseBody {
    pub index: Option<Index>,
}

impl From<Index> for SingleIndexResponseBody {
    fn from(index: Index) -> Self {
        Self { index: Some(index) }
    }
}

#[derive(Debug, Deserialize, Serialize, GraphQLInputObject)]
pub struct IndexRequestBody {
    pub environment: Uuid,
    pub index_type: String,
    pub data_source: String,
    pub regions: Vec<String>,
}

impl From<IndexRequestBody> for db::InputIndexEntity {
    fn from(request: IndexRequestBody) -> Self {
        let IndexRequestBody {
            environment,
            index_type,
            data_source,
            regions,
            ..
        } = request;

        db::InputIndexEntity {
            environment,
            index_type,
            data_source,
            regions,
        }
    }
}

/// Retrieve all environments
pub async fn list_environments(
    context: &Context,
) -> Result<MultiEnvironmentsResponseBody, error::Error> {
    async move {
        let pool = &context.state.pool;

        let mut tx = pool
            .conn()
            .and_then(Connection::begin)
            .await
            .context(error::DBError {
                msg: "could not initiate transaction",
            })?;

        let entities = tx
            .get_all_environments()
            .await
            .context(error::DBProvideError {
                msg: "Could not get all them environments",
            })?;

        // FIXME. Not sure this is the most efficient.... we open as many connections
        // as there are environments, each of the connection retrieves the indexes for an
        // environment. It is, at least, not very scalable.
        let entities: Vec<db::EnvironmentEntity> =
            stream::iter(entities.into_iter().map(|env| Ok(env)))
                .and_then(|mut env| async move {
                    let mut tx =
                        pool.conn()
                            .and_then(Connection::begin)
                            .await
                            .context(error::DBError {
                                msg: "could not initiate transaction",
                            })?;

                    let indexes = tx.get_environment_indexes(&env.id).await.context(
                        error::DBProvideError {
                            msg: "Could not get all them environments",
                        },
                    )?;

                    tx.commit().await.context(error::DBError {
                        msg: "could not commit transaction",
                    })?;

                    env.indexes = indexes;
                    Ok(env)
                })
                .try_collect()
                .await?;

        let environments = entities
            .into_iter()
            .map(Environment::from)
            .collect::<Vec<_>>();

        tx.commit().await.context(error::DBError {
            msg: "could not commit transaction",
        })?;

        Ok(MultiEnvironmentsResponseBody::from(environments))
    }
    .await
}

/// Create a new environment
pub async fn create_environment(
    request: EnvironmentRequestBody,
    context: &Context,
) -> Result<SingleEnvironmentResponseBody, error::Error> {
    async move {
        let mut input = db::InputEnvironmentEntity::from(request);

        let port =
            docker::create_twerg(&input.name, &context.state.settings, &context.state.logger)
                .await?;

        input.port = port as i32;
        debug!(context.state.logger, "Created Twerg at port {}", input.port);

        let pool = &context.state.pool;

        let mut tx = pool
            .conn()
            .and_then(Connection::begin)
            .await
            .context(error::DBError {
                msg: "could not initiate transaction",
            })?;

        let resp = ProvideData::create_environment(&mut tx as &mut sqlx::PgConnection, &input)
            .await
            .context(error::DBProvideError {
                msg: "Could not create environment",
            })?;

        tx.commit().await.context(error::DBError {
            msg: "could not commit create environment transaction.",
        })?;

        let environment = Environment::from(resp);
        Ok(SingleEnvironmentResponseBody::from(environment))
    }
    .await
}

/// Delete an environment. Return the deleted environment.
pub async fn delete_environment(
    id: EnvironmentIdBody,
    context: &Context,
) -> Result<SingleEnvironmentResponseBody, error::Error> {
    async move {
        let pool = &context.state.pool;

        let mut tx = pool
            .conn()
            .and_then(Connection::begin)
            .await
            .context(error::DBError {
                msg: "could not initiate transaction",
            })?;

        let resp = ProvideData::delete_environment(&mut tx as &mut sqlx::PgConnection, &id.id)
            .await
            .context(error::DBProvideError {
                msg: "Could not delete environment",
            })?;

        tx.commit().await.context(error::DBError {
            msg: "could not commit delete environment transaction.",
        })?;

        let environment = Environment::from(resp);
        Ok(SingleEnvironmentResponseBody::from(environment))
    }
    .await
}

/// Create a new index
pub async fn create_index(
    request: IndexRequestBody,
    context: &Context,
) -> Result<SingleIndexResponseBody, error::Error> {
    async move {
        let input = db::InputIndexEntity::from(request);

        let pool = &context.state.pool;

        let mut tx = pool
            .conn()
            .and_then(Connection::begin)
            .await
            .context(error::DBError {
                msg: "could not initiate transaction",
            })?;

        let resp = ProvideData::create_index(&mut tx as &mut sqlx::PgConnection, &input)
            .await
            .context(error::DBProvideError {
                msg: "Could not create environment",
            })?;

        tx.commit().await.context(error::DBError {
            msg: "could not commit create environment transaction.",
        })?;

        let environment = Index::from(resp);
        Ok(SingleIndexResponseBody::from(environment))
    }
    .await
}
