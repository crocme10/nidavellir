use juniper::{EmptySubscription, FieldResult, IntoFieldError, RootNode};
use slog::info;

use crate::api::model;
use crate::state::State;

#[derive(Debug, Clone)]
pub struct Context {
    pub state: State,
}

impl juniper::Context for Context {}

pub struct Query;

#[juniper::graphql_object(
    Context = Context
)]
impl Query {
    /// Returns a list of documents
    async fn environments(
        &self,
        context: &Context,
    ) -> FieldResult<model::MultiEnvironmentsResponseBody> {
        info!(context.state.logger, "Request for environments");
        model::list_environments(context)
            .await
            .map_err(IntoFieldError::into_field_error)
            .into()
    }
}

pub struct Mutation;

#[juniper::graphql_object(
    Context = Context
)]
impl Mutation {
    async fn create_environment(
        &self,
        env: model::EnvironmentRequestBody,
        context: &Context,
    ) -> FieldResult<model::SingleEnvironmentResponseBody> {
        info!(
            context.state.logger,
            "Request for environment '{}' creation", env.name
        );
        model::create_environment(env, context)
            .await
            .map_err(IntoFieldError::into_field_error)
    }

    async fn create_index(
        &self,
        index: model::IndexRequestBody,
        context: &Context,
    ) -> FieldResult<model::SingleIndexResponseBody> {
        info!(context.state.logger, "Request for index creation");
        model::create_index(index, context)
            .await
            .map_err(IntoFieldError::into_field_error)
    }
}

type Schema = RootNode<'static, Query, Mutation, EmptySubscription<Context>>;

pub fn schema() -> Schema {
    Schema::new(Query, Mutation, EmptySubscription::new())
}
