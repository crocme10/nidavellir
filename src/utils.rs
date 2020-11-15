use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use std::env;

pub fn construct_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers
}

pub fn get_service_url(port: i32) -> String {
    format!("http://localhost:{}/mimir/graphql", port)
}

pub fn get_database_url() -> String {
    let mode = env::var("RUN_MODE").expect("RUN_MODE should be set");
    match mode.as_str() {
        "testing" => env::var("SQLITE_TEST_FILE").expect("SQLITE_TEST_FILE should be set"),
        _ => env::var("SQLITE_FILE").expect("SQLITE_FILE should be set"),
    }
}
