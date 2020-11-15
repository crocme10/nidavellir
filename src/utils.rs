use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use std::env;

pub fn construct_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers
}

pub fn get_service_url() -> String {
    // FIXME Uncommenting the following will lead to linking error
    // So I hardcode the port...
    // Here we pass 'None' to settings, because we don't have any command line argument to
    // overwrite what's in the file settings.
    // let settings = Settings::new(None).expect("Settings");
    // let port = settings.service.port;
    let port = 5000;
    let mode = env::var("RUN_MODE").expect("RUN_MODE should be set");
    match mode.as_str() {
        "testing" => format!("http://localhost:{}/graphql", port),
        _ => format!("http://journal:{}/graphql", port),
    }
}

pub fn get_database_url() -> String {
    let mode = env::var("RUN_MODE").expect("RUN_MODE should be set");
    match mode.as_str() {
        "testing" => env::var("SQLITE_TEST_FILE").expect("SQLITE_TEST_FILE should be set"),
        _ => env::var("SQLITE_FILE").expect("SQLITE_FILE should be set"),
    }
}
