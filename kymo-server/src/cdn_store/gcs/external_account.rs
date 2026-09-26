//! Workload Identity Federation for the GCS store (docs/cdn-gcs-migration.md § Credentials): each refresh re-reads the subject-token file, which the kubelet rotates, and exchanges it at Google STS for a federated access token used directly, with no service-account impersonation. `object_store` has no native support.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use object_store::gcp::GcpCredential;

/// Refresh once less than this remains, the same margin the crate's own token cache uses.
const REFRESH_BEFORE: Duration = Duration::from_secs(300);
/// A cached token with less than this left is never handed out, even when a refresh fails.
const USABLE_MARGIN: Duration = Duration::from_secs(60);
/// How long one failed exchange answers every caller before STS is tried again.
const FAILURE_BACKOFF: Duration = Duration::from_secs(5);
const SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// The supported subset of Google's external-account file: a file-sourced, text-format subject exchanged directly at STS. Unknown fields fail startup, so an unsupported feature (impersonation, a URL or executable source, a JSON-format subject) is refused instead of silently ignored.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    audience: String,
    subject_token_type: String,
    token_url: String,
    credential_source: CredentialSource,
    // `gcloud iam workload-identity-pools create-cred-config` writes these two; the GCS endpoints are fixed, so only the default universe works.
    universe_domain: Option<String>,
    #[serde(rename = "token_info_url")]
    _token_info_url: Option<serde::de::IgnoredAny>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialSource {
    file: String,
    #[serde(rename = "format")]
    _format: Option<TextFormat>,
}

/// `{"type": "text"}`, the only subject format (and the default when absent).
#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TextFormat {
    Text,
}

pub(super) struct ExternalAccount {
    audience: String,
    subject_token_type: String,
    token_url: String,
    subject_token_file: String,
    http: reqwest::Client,
    cached: Mutex<Option<(Arc<GcpCredential>, Instant)>>,
    /// Held across an exchange; holds the last failed one.
    refresh: tokio::sync::Mutex<Option<(Instant, String)>>,
}

// Hand-written so the cached bearer never reaches a log.
impl std::fmt::Debug for ExternalAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalAccount")
            .field("audience", &self.audience)
            .finish_non_exhaustive()
    }
}

impl ExternalAccount {
    pub(super) fn new(config: Config) -> anyhow::Result<Self> {
        url::Url::parse(&config.token_url).context("token_url")?;
        if let Some(universe) = &config.universe_domain {
            anyhow::ensure!(
                universe == "googleapis.com",
                "universe_domain {universe:?} is unsupported"
            );
        }
        Ok(Self {
            audience: config.audience,
            subject_token_type: config.subject_token_type,
            token_url: config.token_url,
            subject_token_file: config.credential_source.file,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            cached: Mutex::new(None),
            refresh: tokio::sync::Mutex::new(None),
        })
    }

    pub(super) fn audience(&self) -> &str {
        &self.audience
    }

    fn cached_with(&self, margin: Duration) -> Option<Arc<GcpCredential>> {
        let cached = self.cached.lock().unwrap();
        let (token, expiry) = cached.as_ref()?;
        (expiry.saturating_duration_since(Instant::now()) > margin).then(|| token.clone())
    }

    async fn exchange(&self) -> anyhow::Result<(Arc<GcpCredential>, Instant)> {
        let subject = tokio::fs::read_to_string(&self.subject_token_file)
            .await
            .with_context(|| format!("reading subject token {}", self.subject_token_file))?;
        let subject = subject.trim();
        anyhow::ensure!(
            !subject.is_empty(),
            "subject token {} is empty",
            self.subject_token_file
        );
        let requested_at = Instant::now();
        let response = self
            .http
            .post(&self.token_url)
            .form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:token-exchange",
                ),
                ("audience", &self.audience),
                ("scope", SCOPE),
                (
                    "requested_token_type",
                    "urn:ietf:params:oauth:token-type:access_token",
                ),
                ("subject_token_type", &self.subject_token_type),
                ("subject_token", subject),
            ])
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        anyhow::ensure!(
            status.is_success(),
            "STS answered {status}: {}",
            super::truncate(&body, 600)
        );
        #[derive(serde::Deserialize)]
        struct Token {
            access_token: String,
            expires_in: u64,
        }
        let token: Token = serde_json::from_str(&body).context("unexpected STS response body")?;
        Ok((
            Arc::new(GcpCredential {
                bearer: token.access_token,
            }),
            requested_at + Duration::from_secs(token.expires_in),
        ))
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for ExternalAccount {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
        if let Some(token) = self.cached_with(REFRESH_BEFORE) {
            return Ok(token);
        }
        // One exchange at a time; while one is in flight, a caller with a still-usable token doesn't queue behind it.
        let mut last_failure = match self.refresh.try_lock() {
            Ok(guard) => guard,
            Err(_) => match self.cached_with(USABLE_MARGIN) {
                Some(token) => return Ok(token),
                None => self.refresh.lock().await,
            },
        };
        if let Some(token) = self.cached_with(REFRESH_BEFORE) {
            return Ok(token);
        }
        // A recent failure answers instead of a new exchange, so an STS outage costs one attempt per backoff, not one per queued caller.
        let error = match &*last_failure {
            Some((at, error)) if at.elapsed() < FAILURE_BACKOFF => error.clone(),
            _ => match self.exchange().await {
                Ok((token, expiry)) => {
                    *self.cached.lock().unwrap() = Some((token.clone(), expiry));
                    return Ok(token);
                }
                Err(e) => {
                    let error = format!("STS token exchange failed: {e:#}");
                    tracing::warn!(error = %error, "GCS federated token refresh failed");
                    *last_failure = Some((Instant::now(), error.clone()));
                    error
                }
            },
        };
        // STS is a network dependency, so a failed refresh rides the cached token while it's usable.
        self.cached_with(USABLE_MARGIN)
            .ok_or_else(|| object_store::Error::Generic {
                store: "GCS",
                source: error.into(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use object_store::CredentialProvider as _;
    use std::collections::HashMap;

    /// The chart's credential file, minus the `type` tag that `GcsStore::new` dispatches on.
    fn config(token_url: &str, subject_file: &str) -> String {
        serde_json::json!({
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/k",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": token_url,
            "credential_source": { "file": subject_file, "format": { "type": "text" } },
        })
        .to_string()
    }

    /// Scripted STS: each exchange pops `(status, expires_in)` and records the form it received.
    struct Sts {
        script: Mutex<Vec<(u16, u64)>>,
        forms: Mutex<Vec<HashMap<String, String>>>,
    }

    async fn sts(script: Vec<(u16, u64)>) -> (Arc<Sts>, String) {
        let state = Arc::new(Sts {
            script: Mutex::new(script),
            forms: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route(
                "/v1/token",
                post(|State(s): State<Arc<Sts>>, body: String| async move {
                    let form = url::form_urlencoded::parse(body.as_bytes())
                        .into_owned()
                        .collect();
                    let n = {
                        let mut forms = s.forms.lock().unwrap();
                        forms.push(form);
                        forms.len()
                    };
                    let (status, expires_in) = s.script.lock().unwrap().remove(0);
                    let body = serde_json::json!({
                        "access_token": format!("federated-{n}"),
                        "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
                        "token_type": "Bearer",
                        "expires_in": expires_in,
                    });
                    (StatusCode::from_u16(status).unwrap(), body.to_string())
                }),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/token", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (state, url)
    }

    fn parse(json: &str) -> anyhow::Result<ExternalAccount> {
        ExternalAccount::new(serde_json::from_str(json)?)
    }

    fn federated(token_url: &str, subject: &tempfile::NamedTempFile) -> ExternalAccount {
        parse(&config(token_url, subject.path().to_str().unwrap())).unwrap()
    }

    #[tokio::test]
    async fn exchanges_the_subject_token_and_caches_the_result() {
        let (sts, url) = sts(vec![(200, 3600)]).await;
        let subject = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(subject.path(), "ksa-jwt\n").unwrap();
        let provider = federated(&url, &subject);

        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        let forms = sts.forms.lock().unwrap();
        assert_eq!(forms.len(), 1, "a fresh token is cached");
        let form = &forms[0];
        assert_eq!(
            form["grant_type"],
            "urn:ietf:params:oauth:grant-type:token-exchange"
        );
        assert_eq!(form["audience"], provider.audience());
        assert_eq!(form["scope"], SCOPE);
        assert_eq!(
            form["requested_token_type"],
            "urn:ietf:params:oauth:token-type:access_token"
        );
        assert_eq!(
            form["subject_token_type"],
            "urn:ietf:params:oauth:token-type:jwt"
        );
        assert_eq!(form["subject_token"], "ksa-jwt", "trimmed file contents");
        assert!(
            !format!("{provider:?}").contains("federated"),
            "Debug must not print the bearer"
        );
    }

    #[tokio::test]
    async fn a_token_near_expiry_refreshes_from_the_rotated_file() {
        let (sts, url) = sts(vec![(200, 0), (200, 3600)]).await;
        let subject = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(subject.path(), "first").unwrap();
        let provider = federated(&url, &subject);

        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        std::fs::write(subject.path(), "rotated").unwrap();
        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-2"
        );
        let forms = sts.forms.lock().unwrap();
        assert_eq!(forms[1]["subject_token"], "rotated");
    }

    #[tokio::test]
    async fn a_failed_refresh_uses_the_cached_token_only_while_usable() {
        // Inside the refresh window but still usable: the failure is ridden out, and callers within the backoff ride it without another exchange.
        let (sts_a, url) = sts(vec![(200, 240), (503, 0)]).await;
        let subject = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(subject.path(), "jwt").unwrap();
        let provider = federated(&url, &subject);
        for _ in 0..3 {
            assert_eq!(
                provider.get_credential().await.unwrap().bearer,
                "federated-1"
            );
        }
        assert_eq!(sts_a.forms.lock().unwrap().len(), 2);

        // Past the usable margin: the failure surfaces, and callers within the backoff share it.
        let (sts_b, url) = sts(vec![(200, 30), (503, 0)]).await;
        let provider = federated(&url, &subject);
        provider.get_credential().await.unwrap();
        for _ in 0..2 {
            let err = provider.get_credential().await.unwrap_err();
            assert!(
                err.to_string()
                    .contains("STS token exchange failed: STS answered 503"),
                "{err}"
            );
        }
        assert_eq!(sts_b.forms.lock().unwrap().len(), 2);
    }

    #[test]
    fn unsupported_shapes_are_refused() {
        let good: serde_json::Value =
            serde_json::from_str(&config("https://sts.googleapis.com/v1/token", "/t")).unwrap();
        for (pointer, value) in [
            (
                "/service_account_impersonation_url",
                serde_json::json!("https://impersonation.example/x"),
            ),
            (
                "/credential_source/url",
                serde_json::json!("http://metadata.example/token"),
            ),
            ("/credential_source/format/type", serde_json::json!("json")),
            ("/token_url", serde_json::json!("sts.googleapis.com")),
            ("/universe_domain", serde_json::json!("example.com")),
        ] {
            let mut bad = good.clone();
            let (parent, field) = pointer.rsplit_once('/').unwrap();
            bad.pointer_mut(parent).unwrap()[field] = value;
            assert!(
                parse(&bad.to_string()).is_err(),
                "{pointer} must be refused"
            );
        }
        assert!(parse(&good.to_string()).is_ok());
    }
}
