use juniper::{graphql_value, FieldError, IntoFieldError};
use snafu::Snafu;

use crate::db::model::ProvideError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not identify environment {}", env))]
    #[snafu(visibility(pub))]
    Environment { env: String },

    #[snafu(display("Config Error: {} [{}]", msg, source))]
    #[snafu(visibility(pub))]
    ConfigError {
        msg: String,
        source: config::ConfigError,
    },

    #[snafu(display("Env Var Error: {}", msg))]
    #[snafu(visibility(pub))]
    EnvVarError {
        msg: String,
        source: std::env::VarError,
    },

    #[snafu(display("Miscellaneous Error: {}", msg))]
    #[snafu(visibility(pub))]
    MiscError { msg: String },

    #[snafu(display("Tokio IO Error: {}", msg))]
    #[snafu(visibility(pub))]
    TokioIOError {
        msg: String,
        source: tokio::io::Error,
    },

    #[snafu(display("Std IO Error: {}", msg))]
    #[snafu(visibility(pub))]
    IOError { msg: String, source: std::io::Error },

    #[snafu(display("JSON Error: {} - {}", msg, source))]
    #[snafu(visibility(pub))]
    JSONError {
        msg: String,
        source: serde_json::Error,
    },

    #[snafu(display("DB Error: {} - {}", msg, source))]
    #[snafu(visibility(pub))]
    DBError { msg: String, source: sqlx::Error },

    #[snafu(display("DB Provide Error: {} - {}", msg, source))]
    #[snafu(visibility(pub))]
    DBProvideError { msg: String, source: ProvideError },

    #[snafu(display("Reqwest Error: {} - {}", msg, source))]
    #[snafu(visibility(pub))]
    ReqwestError { msg: String, source: reqwest::Error },
}

impl IntoFieldError for Error {
    fn into_field_error(self) -> FieldError {
        match self {
            err @ Error::Environment { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Environment Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }

            err @ Error::ConfigError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new("Config Error", graphql_value!({ "internal_error": errmsg }))
            }

            err @ Error::EnvVarError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Environment Variable Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }

            err @ Error::MiscError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Miscellaneous Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }

            err @ Error::TokioIOError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Tokio IO Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }

            err @ Error::IOError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new("IO Error", graphql_value!({ "internal_error": errmsg }))
            }

            err @ Error::JSONError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new("JSON Error", graphql_value!({ "internal_error": errmsg }))
            }

            err @ Error::DBError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new("DB Error", graphql_value!({ "internal_error": errmsg }))
            }

            err @ Error::DBProvideError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Provide Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }

            err @ Error::ReqwestError { .. } => {
                let errmsg = format!("{}", err);
                FieldError::new(
                    "Reqwest Error",
                    graphql_value!({ "internal_error": errmsg }),
                )
            }
        }
    }
}
