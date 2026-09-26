use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use subtle::ConstantTimeEq;

const LOCAL_AUTH_FORMAT_VERSION: u32 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretFile {
    format_version: u32,
    server_bearer: String,
}

pub(crate) struct LocalAuth {
    server_bearer: String,
}

impl LocalAuth {
    pub(crate) fn read(path: &Path) -> Result<Arc<Self>> {
        let bytes = crate::private_file::read(path, "local authentication", 16 * 1024)?;
        let secrets: SecretFile =
            serde_json::from_slice(&bytes).context("parse local authentication file")?;
        ensure!(
            secrets.format_version == LOCAL_AUTH_FORMAT_VERSION,
            "unsupported local authentication format"
        );
        ensure!(
            valid_token(&secrets.server_bearer),
            "server_bearer must be a 43-character base64url token"
        );
        Ok(Arc::new(Self {
            server_bearer: secrets.server_bearer,
        }))
    }

    pub(crate) fn authorize_server_headers(&self, headers: &HeaderMap) -> bool {
        authorize(headers, &self.server_bearer)
    }

    pub(crate) fn is_server_bearer(&self, token: &str) -> bool {
        secret_eq(token, &self.server_bearer)
    }

    #[cfg(test)]
    pub(crate) fn testing(server: &str) -> Arc<Self> {
        Arc::new(Self {
            server_bearer: server.to_owned(),
        })
    }
}

pub(crate) fn valid_token(value: &str) -> bool {
    // A 32-byte value encodes to 43 unpadded base64url characters; only these final sextets have the required two zero padding bits, which rejects non-canonical aliases.
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| b"AEIMQUYcgkosw048".contains(byte))
}

fn secret_eq(left: &str, right: &str) -> bool {
    left.as_bytes().ct_eq(right.as_bytes()).into()
}

/// Exactly one `Bearer` value, matching in constant time.
fn single_bearer<'a>(mut values: impl Iterator<Item = Option<&'a str>>, expected: &str) -> bool {
    values
        .next()
        .flatten()
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| secret_eq(token, expected))
        && values.next().is_none()
}

fn authorize(headers: &HeaderMap, expected: &str) -> bool {
    single_bearer(
        headers
            .get_all(header::AUTHORIZATION)
            .iter()
            .map(|value| value.to_str().ok()),
        expected,
    )
}

pub(crate) fn authorize_metadata(metadata: &tonic::metadata::MetadataMap, expected: &str) -> bool {
    single_bearer(
        metadata
            .get_all("authorization")
            .iter()
            .map(|value| value.to_str().ok()),
        expected,
    )
}

#[derive(Clone)]
pub(crate) struct UploadAdmission {
    auth: Arc<LocalAuth>,
    activity: Arc<crate::activity::ActivityTracker>,
}

impl UploadAdmission {
    pub(crate) fn new(
        auth: Arc<LocalAuth>,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Self {
        Self { auth, activity }
    }
}

pub(crate) async fn require_upload_bearer(
    State(admission): State<UploadAdmission>,
    request: Request,
    next: Next,
) -> Response {
    if request.headers().contains_key(header::ORIGIN) {
        return private_error(StatusCode::FORBIDDEN, "browser Origin is not allowed");
    }
    if !admission.auth.authorize_server_headers(request.headers()) {
        return private_error(StatusCode::UNAUTHORIZED, "authentication required");
    }
    // Middleware owns the guard before the body extractor buffers the upload.
    let _work = admission.activity.begin_work();
    private_no_store(next.run(request).await)
}

fn private_no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        http::HeaderValue::from_static("private, no-store"),
    );
    response
}

pub(crate) fn private_error(status: StatusCode, message: &'static str) -> Response {
    private_no_store((status, message).into_response())
}

pub(crate) fn grpc_interceptor(auth: Arc<LocalAuth>) -> impl tonic::service::Interceptor + Clone {
    move |request: tonic::Request<()>| {
        if authorize_metadata(request.metadata(), &auth.server_bearer) {
            Ok(request)
        } else {
            Err(tonic::Status::unauthenticated("authentication required"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use axum::body::Body;
    use axum::routing::get;
    use http::Request;
    use tower::ServiceExt;

    use super::*;

    const SERVER: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const OTHER: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBE";

    fn auth() -> LocalAuth {
        LocalAuth {
            server_bearer: SERVER.to_owned(),
        }
    }

    #[test]
    fn secret_file_requires_one_private_base64url_token() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("auth.json");
        std::fs::write(
            &path,
            format!(r#"{{"format_version":1,"server_bearer":"{SERVER}"}}"#),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(LocalAuth::read(&path).is_ok());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(LocalAuth::read(&path).is_err());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(
            &path,
            format!(r#"{{"format_version":2,"server_bearer":"{SERVER}"}}"#),
        )
        .unwrap();
        assert!(LocalAuth::read(&path).is_err());

        std::fs::write(
            &path,
            format!(
                r#"{{"format_version":1,"server_bearer":"{SERVER}","dashboard_bearer":"{OTHER}"}}"#
            ),
        )
        .unwrap();
        assert!(LocalAuth::read(&path).is_err());

        let symlink = temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("auth-link.json");
        std::os::unix::fs::symlink(&path, &symlink).unwrap();
        assert!(LocalAuth::read(&symlink).is_err());

        std::fs::write(
            &path,
            format!(r#"{{"format_version":1,"server_bearer":"{SERVER}"}}"#),
        )
        .unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(LocalAuth::read(&path).is_err());
    }

    #[test]
    fn authorization_requires_one_exact_scoped_bearer() {
        let auth = auth();
        assert!(!valid_token("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"));
        let mut headers = HeaderMap::new();
        assert!(!auth.authorize_server_headers(&headers));
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {SERVER}").parse().unwrap(),
        );
        assert!(auth.authorize_server_headers(&headers));
        assert!(auth.is_server_bearer(SERVER));
        assert!(!auth.is_server_bearer(OTHER));
    }

    #[tokio::test]
    async fn upload_middleware_rejects_browsers_and_other_credentials() {
        let app = axum::Router::new()
            .route("/protected", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn_with_state(
                UploadAdmission::new(
                    LocalAuth::testing(SERVER),
                    crate::activity::ActivityTracker::new_local(),
                ),
                require_upload_bearer,
            ));
        for (origin, token, expected) in [
            (None, None, StatusCode::UNAUTHORIZED),
            (None, Some(OTHER), StatusCode::UNAUTHORIZED),
            (None, Some(SERVER), StatusCode::OK),
            (
                Some("http://127.0.0.1:1"),
                Some(SERVER),
                StatusCode::FORBIDDEN,
            ),
        ] {
            let mut request = Request::get("/protected").body(Body::empty()).unwrap();
            if let Some(origin) = origin {
                request
                    .headers_mut()
                    .insert(header::ORIGIN, origin.parse().unwrap());
            }
            if let Some(token) = token {
                request.headers_mut().insert(
                    header::AUTHORIZATION,
                    format!("Bearer {token}").parse().unwrap(),
                );
            }
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected);
        }
    }

    #[test]
    fn grpc_interceptor_requires_the_server_scope() {
        use tonic::service::Interceptor;

        let mut interceptor = grpc_interceptor(LocalAuth::testing(SERVER));
        let mut request = tonic::Request::new(());
        assert_eq!(
            interceptor.call(request).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        request = tonic::Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {OTHER}").parse().unwrap());
        assert_eq!(
            interceptor.call(request).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        request = tonic::Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {SERVER}").parse().unwrap());
        interceptor.call(request).unwrap();

        let mut request = tonic::Request::new(());
        request
            .metadata_mut()
            .append("authorization", format!("Bearer {SERVER}").parse().unwrap());
        request
            .metadata_mut()
            .append("authorization", format!("Bearer {SERVER}").parse().unwrap());
        assert_eq!(
            interceptor.call(request).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
}
