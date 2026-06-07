use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tower::{Layer, Service};

/// Tower [`Layer`] that records per-method request duration and total count
/// for every gRPC call passing through the tonic transport server.
///
/// Emits:
/// - `likhadb_grpc_request_duration_seconds{method}` — histogram
/// - `likhadb_grpc_requests_total{method, status}` — counter (status: "ok" | "error")
#[derive(Clone, Default)]
pub struct GrpcMetricsLayer;

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetricsService { inner }
    }
}

#[derive(Clone)]
pub struct GrpcMetricsService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for GrpcMetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let method = extract_method(req.uri().path());
        let start = Instant::now();
        // Clone so the future owns its own ready service handle.
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.call(req).await;
            let elapsed = start.elapsed().as_secs_f64();

            let status = match &result {
                Ok(resp) => {
                    if resp.status().is_success() {
                        "ok"
                    } else {
                        "error"
                    }
                }
                Err(_) => "error",
            };

            metrics::histogram!(
                "likhadb_grpc_request_duration_seconds",
                "method" => method.clone()
            )
            .record(elapsed);

            metrics::counter!(
                "likhadb_grpc_requests_total",
                "method" => method,
                "status" => status
            )
            .increment(1);

            result
        })
    }
}

/// Extract the bare method name from a gRPC URI path.
/// `/likhadb.LikhaDb/Insert` → `Insert`
fn extract_method(path: &str) -> String {
    path.rsplit('/').next().unwrap_or("unknown").to_owned()
}
