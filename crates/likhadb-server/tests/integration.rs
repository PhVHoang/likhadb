// A single integration-test crate avoids linking the server and its large
// dependency graph once per test file.
mod integration {
    mod auth;
    mod hybrid;
    mod metrics;
    mod parquet_paths;
    mod query_limits;
}
