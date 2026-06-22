use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};
use tonic::Status;

/// Shared bearer secret from `LIKHADB_API_TOKEN`.
///
/// `None` means auth is disabled (intended for local dev only; `main` logs a
/// loud warning at startup). The check lives at the application layer, so
/// enabling transport-level mTLS later is additive — no change to this type.
#[derive(Clone)]
pub struct ApiToken(Arc<Option<String>>);

impl ApiToken {
    /// Construct directly. `None` disables auth. Prefer [`ApiToken::from_env`]
    /// in production; this is handy for tests and explicit wiring.
    pub fn new(token: Option<String>) -> Self {
        Self(Arc::new(token.filter(|s| !s.is_empty())))
    }

    pub fn from_env() -> Self {
        Self::new(std::env::var("LIKHADB_API_TOKEN").ok())
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    fn verify(&self, presented: &str) -> bool {
        match self.0.as_deref() {
            None => true,
            Some(expected) => constant_time_eq(expected.as_bytes(), presented.as_bytes()),
        }
    }
}

/// Compares without short-circuiting on content, so timing does not reveal how
/// many leading bytes matched. Only leaks length, which is fixed per deploy.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn extract_bearer(raw: Option<&str>) -> Option<&str> {
    raw?.strip_prefix("Bearer ")
}

/// REST middleware. Apply with `route_layer` so public routes (`/health`)
/// are not gated.
pub async fn require_bearer(
    State(token): State<ApiToken>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !token.is_enabled() {
        return Ok(next.run(req).await);
    }
    let provided = extract_bearer(
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    match provided {
        Some(t) if token.verify(t) => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// gRPC interceptor. Gates every method on the service.
pub fn grpc_interceptor(
    token: ApiToken,
) -> impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, Status> + Clone {
    move |req| {
        if !token.is_enabled() {
            return Ok(req);
        }
        let provided = extract_bearer(
            req.metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
        );
        match provided {
            Some(t) if token.verify(t) => Ok(req),
            _ => Err(Status::unauthenticated("missing or invalid bearer token")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_only_identical() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secret-longer"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn extract_bearer_strips_prefix() {
        assert_eq!(extract_bearer(Some("Bearer abc")), Some("abc"));
        assert_eq!(extract_bearer(Some("bearer abc")), None);
        assert_eq!(extract_bearer(Some("abc")), None);
        assert_eq!(extract_bearer(None), None);
    }

    #[test]
    fn disabled_token_accepts_anything() {
        let t = ApiToken::new(None);
        assert!(!t.is_enabled());
        assert!(t.verify("whatever"));
    }

    #[test]
    fn enabled_token_requires_exact_match() {
        let t = ApiToken::new(Some("s3cr3t".to_string()));
        assert!(t.is_enabled());
        assert!(t.verify("s3cr3t"));
        assert!(!t.verify("wrong"));
    }
}
