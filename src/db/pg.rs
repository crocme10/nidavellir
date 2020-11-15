use async_trait::async_trait;
// use chrono::{DateTime, Utc};
use slog::{debug, info, o, Logger};
use snafu::ResultExt;
use sqlx::error::DatabaseError;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgError, PgQueryAs, PgRow};
use sqlx::row::{FromRow, Row};
use sqlx::{PgConnection, PgPool};
use std::convert::TryFrom;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use super::model;
use super::Db;
use crate::error;

/// The row here should match the information in the return_environment_type
impl<'c> FromRow<'c, PgRow<'c>> for model::EnvironmentEntity {
    fn from_row(row: &PgRow<'c>) -> Result<Self, sqlx::Error> {
        Ok(model::EnvironmentEntity {
            id: row.get(0),
            name: row.get(1),
            indexes: Vec::new(),
            signature: row.get(2),
            port: row.get(3),
            created_at: row.get(4),
            updated_at: row.get(5),
        })
    }
}

/// The row here should match the information in the return_index_type
impl<'c> FromRow<'c, PgRow<'c>> for model::IndexEntity {
    fn from_row(row: &PgRow<'c>) -> Result<Self, sqlx::Error> {
        Ok(model::IndexEntity {
            id: row.get(0),
            index_type: row.get(1),
            data_source: row.get(2),
            regions: row.get(3),
            signature: row.get(4),
            status: row.get(5),
            created_at: row.get(6),
            updated_at: row.get(7),
        })
    }
}

/// Open a connection to a database
pub async fn connect(db_url: &str) -> sqlx::Result<PgPool> {
    let pool = PgPool::new(db_url).await?;
    Ok(pool)
}

impl TryFrom<&PgError> for model::ProvideError {
    type Error = ();

    /// Attempt to convert a Postgres error into a generic ProvideError
    ///
    /// Unexpected cases will be bounced back to the caller for handling
    ///
    /// * [Postgres Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)
    fn try_from(pg_err: &PgError) -> Result<Self, Self::Error> {
        let provider_err = match pg_err.code().unwrap() {
            "23505" => model::ProvideError::UniqueViolation {
                details: pg_err.details().unwrap().to_owned(),
            },
            code if code.starts_with("23") => model::ProvideError::ModelViolation {
                details: pg_err.message().to_owned(),
            },
            _ => return Err(()),
        };

        Ok(provider_err)
    }
}

#[async_trait]
impl Db for PgPool {
    type Conn = PoolConnection<PgConnection>;

    async fn conn(&self) -> Result<Self::Conn, sqlx::Error> {
        self.acquire().await
    }
}

#[async_trait]
impl model::ProvideData for PgConnection {
    async fn get_all_environments(
        &mut self,
    ) -> model::ProvideResult<Vec<model::EnvironmentEntity>> {
        let environments: Vec<model::EnvironmentEntity> =
            sqlx::query_as(r#"SELECT * FROM list_environments()"#)
                .fetch_all(self)
                .await?;

        Ok(environments)
    }

    async fn get_environment_indexes(
        &mut self,
        environment: &model::EntityId,
    ) -> model::ProvideResult<Vec<model::IndexEntity>> {
        let indexes: Vec<model::IndexEntity> =
            sqlx::query_as(r#"SELECT * FROM list_environment_indexes($1)"#)
                .bind(environment)
                .fetch_all(self)
                .await?;

        Ok(indexes)
    }

    async fn create_environment(
        &mut self,
        env: &model::InputEnvironmentEntity,
    ) -> model::ProvideResult<model::EnvironmentEntity> {
        let environment: model::EnvironmentEntity =
            sqlx::query_as("SELECT * FROM create_environment($1::TEXT, $2::INTEGER)")
                .bind(&env.name)
                .bind(&env.port)
                .fetch_one(self)
                .await?;

        Ok(environment)
    }

    async fn delete_environment(
        &mut self,
        env: &model::EntityId,
    ) -> model::ProvideResult<model::EnvironmentEntity> {
        let environment: model::EnvironmentEntity =
            sqlx::query_as("SELECT * FROM delete_environment($1::UUID)")
                .bind(&env)
                .fetch_one(self)
                .await?;

        Ok(environment)
    }

    async fn create_index(
        &mut self,
        index: &model::InputIndexEntity,
    ) -> model::ProvideResult<model::IndexEntity> {
        let index: model::IndexEntity =
            sqlx::query_as("SELECT * FROM create_index($1::UUID, $2::TEXT, $3::TEXT, $4::TEXT[])")
                .bind(&index.environment)
                .bind(&index.index_type)
                .bind(&index.data_source)
                .bind(&index.regions)
                .fetch_one(self)
                .await?;
        Ok(index)
    }

    async fn get_environment_by_id(
        &mut self,
        id: &model::EntityId,
    ) -> model::ProvideResult<model::EnvironmentEntity> {
        let environment: model::EnvironmentEntity =
            sqlx::query_as("SELECT * FROM get_environment_by_id($1::UUID)")
                .bind(&id)
                .fetch_one(self)
                .await?;

        Ok(environment)
    }
}

pub async fn init_db(conn_str: &str, logger: Logger) -> Result<(), error::Error> {
    info!(logger, "Initializing  DB @ {}", conn_str);
    migration_down(conn_str, &logger).await?;
    migration_up(conn_str, &logger).await?;
    Ok(())
}

pub async fn migration_up(conn_str: &str, logger: &Logger) -> Result<(), error::Error> {
    let clogger = logger.new(o!("database" => String::from(conn_str)));
    debug!(clogger, "Movine Up");
    // This is essentially running 'psql $DATABASE_URL < db/init.sql', and logging the
    // psql output.
    // FIXME This relies on a command psql, which is not desibable.
    // We could alternatively try to use sqlx...
    // There may be a tool for doing migrations.
    let mut cmd = Command::new("movine");
    cmd.env("DATABASE_URL", conn_str);
    cmd.arg("up");
    cmd.stdout(Stdio::piped());

    let mut child = cmd.spawn().context(error::TokioIOError {
        msg: String::from("Failed to execute movine"),
    })?;

    let stdout = child.stdout.take().ok_or(error::Error::MiscError {
        msg: String::from("child did not have a handle to stdout"),
    })?;

    let mut reader = BufReader::new(stdout).lines();

    // Ensure the child process is spawned in the runtime so it can
    // make progress on its own while we await for any output.
    tokio::spawn(async {
        // FIXME Need to do something about logging this and returning an error.
        let _status = child.await.expect("child process encountered an error");
        // println!("child status was: {}", status);
    });
    debug!(clogger, "Spawned migration up");

    while let Some(line) = reader.next_line().await.context(error::TokioIOError {
        msg: String::from("Could not read from piped output"),
    })? {
        debug!(clogger, "movine: {}", line);
    }

    Ok(())
}

pub async fn migration_down(conn_str: &str, logger: &Logger) -> Result<(), error::Error> {
    let clogger = logger.new(o!("database" => String::from(conn_str)));
    debug!(clogger, "Movine Down");
    // This is essentially running 'psql $DATABASE_URL < db/init.sql', and logging the
    // psql output.
    // FIXME This relies on a command psql, which is not desibable.
    // We could alternatively try to use sqlx...
    // There may be a tool for doing migrations.
    let mut cmd = Command::new("movine");
    cmd.env("DATABASE_URL", conn_str);
    cmd.arg("down");
    cmd.stdout(Stdio::piped());

    let mut child = cmd.spawn().context(error::TokioIOError {
        msg: String::from("Failed to execute movine"),
    })?;

    let stdout = child.stdout.take().ok_or(error::Error::MiscError {
        msg: String::from("child did not have a handle to stdout"),
    })?;

    let mut reader = BufReader::new(stdout).lines();

    // Ensure the child process is spawned in the runtime so it can
    // make progress on its own while we await for any output.
    tokio::spawn(async {
        // FIXME Need to do something about logging this and returning an error.
        let _status = child.await.expect("child process encountered an error");
        // println!("child status was: {}", status);
    });
    debug!(clogger, "Spawned migration down");

    while let Some(line) = reader.next_line().await.context(error::TokioIOError {
        msg: String::from("Could not read from piped output"),
    })? {
        debug!(clogger, "movine: {}", line);
    }

    Ok(())
}
