use futures::future::TryFutureExt;
use slog::{info, Logger};
use snafu::futures::try_future::TryFutureExt as SnafuTryFutureExt;
use snafu::ResultExt;
use uuid::Uuid;

use crate::api::model;
use crate::error;
use crate::utils::{construct_headers, get_service_url};

// FIXME I'm not exactly sure what to retrieve from this function for Nidavellir...
pub async fn create_index(
    index: &model::IndexRequestBody,
    port: i32,
    logger: &Logger,
) -> Result<i32, error::Error> {
    let data = get_graphql_create_index(&index);
    let url = get_service_url(port);
    let client = reqwest::Client::new();
    client
        .post(&url)
        .headers(construct_headers())
        .body(data)
        .send()
        .context(error::ReqwestError {
            msg: String::from("Could not request SingleIndexResponseBody"),
        })
        .and_then(|resp| {
            resp.json::<serde_json::Value>()
                .context(error::ReqwestError {
                    msg: String::from("Could not deserialize SingleUserResponseBody"),
                })
        })
        .and_then(|json| {
            async move {
                // This JSON contains two fields, data, and errors.
                // So we test if data is null,
                //   in which case we return the first error in the errors array,
                // otherwise
                //   we return the expected singleuserresponse
                if json["data"].is_null() {
                    let errors = json["errors"].as_array().expect("errors");
                    let error = &errors.first().expect("at least one error");
                    Err(error::Error::MiscError {
                        msg: format!("{}", error),
                    })
                } else {
                    let res = &json["data"]["createIndex"]["index"]["indexId"];
                    let res = res.clone();
                    serde_json::from_value(res).context(error::JSONError {
                        msg: String::from("Cannot deserialize id from index creation"),
                    })
                }
            }
        })
        .await
}

// This is a helper function which generates the GraphQL query for creating an index.
pub fn get_graphql_create_index(index: &model::IndexRequestBody) -> String {
    let query = r#" "mutation createIndex($index: IndexRequestBody!) { createIndex(index: $index) { index { indexId } } }" "#;
    // FIXME Here we have a problem Houston.... Twerg has for now been designed for a single
    // region (ie a String), whereas Nidavellir been done for Vec<String>.
    let variables = format!(
        r#"{{ "indexType": "{indexType}", "dataSource": "{dataSource}", "region": "{region}" }}"#,
        indexType = index.index_type,
        dataSource = index.data_source,
        region = index.regions[0]
    );
    format!(
        r#"{{ "query": {query}, "variables": {{ "index": {variables} }} }}"#,
        query = query,
        variables = variables
    )
}
