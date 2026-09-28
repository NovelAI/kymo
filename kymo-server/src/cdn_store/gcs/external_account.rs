//! Workload Identity Federation for GCS (docs/cdn-gcs-migration.md § Credentials): each refresh re-reads the subject-token file, which the kubelet rotates, and exchanges it at Google STS for a federated access token. The store uses that token directly; an impersonating file (the CDN collector's) trades it at IAM Credentials for a service account's. `object_store` has no native support.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use object_store::gcp::GcpCredential;

/// Refresh once less than this remains, the same margin the crate's own token cache uses.
const REFRESH_BEFORE: Duration = Duration::from_secs(300);
/// A cached token with less than this left is never handed out, even when a refresh fails.
const USABLE_MARGIN: Duration = Duration::from_secs(60);
/// How long one failed exchange answers every caller before another is tried.
const FAILURE_BACKOFF: Duration = Duration::from_secs(5);
/// An impersonated token's lifetime: IAM Credentials' default and, without an org policy, its maximum.
const IMPERSONATED_LIFETIME: Duration = Duration::from_secs(3600);
/// The scope of every token that reaches GCS. IAM Credentials rejects it, so a leaked store token can't impersonate.
const STORAGE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
/// What `generateAccessToken` requires of the federated token that calls it: only the collector's STS leg asks for it.
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
// Both endpoints are pinned, since each is sent a credential: STS the subject token, IAM Credentials (`{IMPERSONATION_URL_PREFIX}<account>:generateAccessToken`) the federated one.
const STS_TOKEN_URL: &str = "https://sts.googleapis.com/v1/token";
const IMPERSONATION_URL_PREFIX: &str =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";

/// The supported subset of Google's external-account file: a file-sourced, text-format subject exchanged at STS, optionally impersonating a service account. Unknown fields fail startup, so an unsupported feature (a URL or executable source, a JSON-format subject, impersonation options such as a token lifetime) is refused instead of silently ignored.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    audience: String,
    subject_token_type: String,
    token_url: String,
    credential_source: CredentialSource,
    /// Written by `create-cred-config --service-account`: the IAM Credentials `generateAccessToken` URL of the account to impersonate.
    service_account_impersonation_url: Option<String>,
    // `gcloud iam workload-identity-pools create-cred-config` writes these two; the GCS endpoints are fixed, so only the default universe works.
    universe_domain: Option<String>,
    #[serde(rename = "token_info_url")]
    _token_info_url: Option<serde::de::IgnoredAny>,
}

impl Config {
    pub(super) fn impersonates(&self) -> bool {
        self.service_account_impersonation_url.is_some()
    }
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
    config: Config,
    http: reqwest::Client,
    cached: Mutex<Option<(Arc<GcpCredential>, Instant)>>,
    /// Held across an exchange; holds the last failed one.
    refresh: tokio::sync::Mutex<Option<(Instant, String)>>,
}

// Hand-written so the cached bearer never reaches a log.
impl std::fmt::Debug for ExternalAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalAccount")
            .field("audience", &self.config.audience)
            .finish_non_exhaustive()
    }
}

impl ExternalAccount {
    pub(super) fn new(config: Config) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.token_url == STS_TOKEN_URL,
            "token_url {:?} is not {STS_TOKEN_URL}",
            config.token_url
        );
        if let Some(url) = &config.service_account_impersonation_url {
            let account = url
                .strip_prefix(IMPERSONATION_URL_PREFIX)
                .and_then(|rest| rest.strip_suffix(":generateAccessToken"));
            anyhow::ensure!(
                account.is_some_and(|account| !account.is_empty()
                    && account.bytes().all(|b| b.is_ascii_alphanumeric() || b"@.-".contains(&b))),
                "service_account_impersonation_url {url:?} is not {IMPERSONATION_URL_PREFIX}<account>:generateAccessToken"
            );
        }
        if let Some(universe) = &config.universe_domain {
            anyhow::ensure!(
                universe == "googleapis.com",
                "universe_domain {universe:?} is unsupported"
            );
        }
        Ok(Self {
            config,
            // A redirect would re-send the form (with the subject token) or the bearer elsewhere.
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            cached: Mutex::new(None),
            refresh: tokio::sync::Mutex::new(None),
        })
    }

    pub(super) fn audience(&self) -> &str {
        &self.config.audience
    }

    fn cached_with(&self, margin: Duration) -> Option<Arc<GcpCredential>> {
        let cached = self.cached.lock().unwrap();
        let (token, expiry) = cached.as_ref()?;
        (expiry.saturating_duration_since(Instant::now()) > margin).then(|| token.clone())
    }

    async fn exchange(&self) -> anyhow::Result<(Arc<GcpCredential>, Instant)> {
        let file = &self.config.credential_source.file;
        let subject = tokio::fs::read_to_string(file)
            .await
            .with_context(|| format!("reading subject token {file}"))?;
        let subject = subject.trim();
        anyhow::ensure!(!subject.is_empty(), "subject token {file} is empty");
        // Lifetimes count from before the STS call, so the cache retires a token early rather than late.
        let requested_at = Instant::now();
        #[derive(serde::Deserialize)]
        struct Federated {
            access_token: String,
            expires_in: u64,
        }
        let federated: Federated = call(
            "STS",
            self.http.post(&self.config.token_url).form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:token-exchange",
                ),
                ("audience", &self.config.audience),
                (
                    "scope",
                    match self.config.service_account_impersonation_url {
                        Some(_) => CLOUD_PLATFORM_SCOPE,
                        None => STORAGE_SCOPE,
                    },
                ),
                (
                    "requested_token_type",
                    "urn:ietf:params:oauth:token-type:access_token",
                ),
                ("subject_token_type", &self.config.subject_token_type),
                ("subject_token", subject),
            ]),
        )
        .await?;
        let (bearer, lifetime) = match &self.config.service_account_impersonation_url {
            None => (
                federated.access_token,
                Duration::from_secs(federated.expires_in),
            ),
            Some(url) => {
                #[derive(serde::Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Impersonated {
                    access_token: String,
                }
                let request = serde_json::json!({
                    "scope": [STORAGE_SCOPE],
                    "lifetime": format!("{}s", IMPERSONATED_LIFETIME.as_secs()),
                });
                let impersonated: Impersonated = call(
                    "IAM Credentials",
                    self.http
                        .post(url)
                        .bearer_auth(&federated.access_token)
                        .header(reqwest::header::CONTENT_TYPE, "application/json")
                        .body(request.to_string()),
                )
                .await?;
                (impersonated.access_token, IMPERSONATED_LIFETIME)
            }
        };
        Ok((Arc::new(GcpCredential { bearer }), requested_at + lifetime))
    }
}

/// Sends `request` and parses a successful answer's JSON; `service` names the endpoint in errors.
async fn call<T: serde::de::DeserializeOwned>(
    service: &str,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<T> {
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "{service} answered {status}: {}",
        super::truncate(&body, 600)
    );
    serde_json::from_str(&body).with_context(|| format!("unexpected {service} response body"))
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
        // A recent failure answers instead of a new exchange, so an outage costs one attempt per backoff, not one per queued caller.
        let error = match &*last_failure {
            Some((at, error)) if at.elapsed() < FAILURE_BACKOFF => error.clone(),
            _ => match self.exchange().await {
                Ok((token, expiry)) => {
                    *self.cached.lock().unwrap() = Some((token.clone(), expiry));
                    return Ok(token);
                }
                Err(e) => {
                    let error = format!("federated token refresh failed: {e:#}");
                    tracing::warn!(error = %error, "GCS federated token refresh failed");
                    *last_failure = Some((Instant::now(), error.clone()));
                    error
                }
            },
        };
        // The exchange is a network dependency, so a failed refresh rides the cached token while it's usable.
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
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::Router;
    use object_store::CredentialProvider as _;
    use std::collections::HashMap;

    /// The chart's credential file, minus the `type` tag that `credentialed_builder` dispatches on.
    fn config(subject_file: &str) -> serde_json::Value {
        serde_json::json!({
            "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/k",
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": STS_TOKEN_URL,
            "credential_source": { "file": subject_file, "format": { "type": "text" } },
        })
    }

    const IMPERSONATION_PATH: &str =
        "/v1/projects/-/serviceAccounts/gc@sa.example:generateAccessToken";

    /// Scripted Google: each STS exchange pops `(status, expires_in)` and records the form it received; each generateAccessToken records its bearer and body, and answers `impersonation_status`.
    struct Google {
        script: Mutex<Vec<(u16, u64)>>,
        forms: Mutex<Vec<HashMap<String, String>>>,
        impersonations: Mutex<Vec<(String, serde_json::Value)>>,
        impersonation_status: Mutex<u16>,
    }

    async fn token(State(g): State<Arc<Google>>, body: String) -> (StatusCode, String) {
        let form = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        let n = {
            let mut forms = g.forms.lock().unwrap();
            forms.push(form);
            forms.len()
        };
        let (status, expires_in) = g.script.lock().unwrap().remove(0);
        let body = serde_json::json!({
            "access_token": format!("federated-{n}"),
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "token_type": "Bearer",
            "expires_in": expires_in,
        });
        (StatusCode::from_u16(status).unwrap(), body.to_string())
    }

    async fn generate_access_token(
        State(g): State<Arc<Google>>,
        headers: HeaderMap,
        body: String,
    ) -> (StatusCode, String) {
        let bearer = headers["authorization"].to_str().unwrap().to_owned();
        let n = {
            let mut impersonations = g.impersonations.lock().unwrap();
            impersonations.push((bearer, serde_json::from_str(&body).unwrap()));
            impersonations.len()
        };
        let status = *g.impersonation_status.lock().unwrap();
        let body = serde_json::json!({ "accessToken": format!("impersonated-{n}") });
        (StatusCode::from_u16(status).unwrap(), body.to_string())
    }

    /// Serves both endpoints on one local origin, which it returns.
    async fn google(script: Vec<(u16, u64)>) -> (Arc<Google>, String) {
        let state = Arc::new(Google {
            script: Mutex::new(script),
            forms: Mutex::new(Vec::new()),
            impersonations: Mutex::new(Vec::new()),
            impersonation_status: Mutex::new(200),
        });
        let app = Router::new()
            .route("/v1/token", post(token))
            .route(IMPERSONATION_PATH, post(generate_access_token))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (state, origin)
    }

    fn parse(file: &serde_json::Value) -> anyhow::Result<ExternalAccount> {
        ExternalAccount::new(serde_json::from_value(file.clone())?)
    }

    fn subject(contents: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), contents).unwrap();
        file
    }

    /// A provider parsed from `file`, then pointed at the mock `origin`: the file itself must name Google's endpoints.
    fn retargeted(origin: &str, file: &serde_json::Value) -> ExternalAccount {
        let mut provider = parse(file).unwrap();
        provider.config.token_url = provider
            .config
            .token_url
            .replace("https://sts.googleapis.com", origin);
        if let Some(url) = &mut provider.config.service_account_impersonation_url {
            *url = url.replace("https://iamcredentials.googleapis.com", origin);
        }
        provider
    }

    fn federated(origin: &str, subject: &tempfile::NamedTempFile) -> ExternalAccount {
        retargeted(origin, &config(subject.path().to_str().unwrap()))
    }

    /// The CDN collector's file: the same, plus the account to impersonate.
    fn impersonating(origin: &str, subject: &tempfile::NamedTempFile) -> ExternalAccount {
        let mut file = config(subject.path().to_str().unwrap());
        file["service_account_impersonation_url"] =
            format!("https://iamcredentials.googleapis.com{IMPERSONATION_PATH}").into();
        retargeted(origin, &file)
    }

    #[tokio::test]
    async fn exchanges_the_subject_token_and_caches_the_result() {
        let (google, origin) = google(vec![(200, 3600)]).await;
        let subject = subject("ksa-jwt\n");
        let provider = federated(&origin, &subject);

        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        let forms = google.forms.lock().unwrap();
        assert_eq!(forms.len(), 1, "a fresh token is cached");
        let form = &forms[0];
        assert_eq!(
            form["grant_type"],
            "urn:ietf:params:oauth:grant-type:token-exchange"
        );
        assert_eq!(form["audience"], provider.audience());
        // Literal, not the constant: the scope is the property under test.
        assert_eq!(
            form["scope"], "https://www.googleapis.com/auth/devstorage.read_write",
            "the store's token can't impersonate"
        );
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
        let (google, origin) = google(vec![(200, 0), (200, 3600)]).await;
        let subject = subject("first");
        let provider = federated(&origin, &subject);

        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-1"
        );
        std::fs::write(subject.path(), "rotated").unwrap();
        assert_eq!(
            provider.get_credential().await.unwrap().bearer,
            "federated-2"
        );
        let forms = google.forms.lock().unwrap();
        assert_eq!(forms[1]["subject_token"], "rotated");
    }

    #[tokio::test]
    async fn a_failed_refresh_uses_the_cached_token_only_while_usable() {
        // Inside the refresh window but still usable: the failure is ridden out, and callers within the backoff ride it without another exchange.
        let (google_a, origin) = google(vec![(200, 240), (503, 0)]).await;
        let subject = subject("jwt");
        let provider = federated(&origin, &subject);
        for _ in 0..3 {
            assert_eq!(
                provider.get_credential().await.unwrap().bearer,
                "federated-1"
            );
        }
        assert_eq!(google_a.forms.lock().unwrap().len(), 2);

        // Past the usable margin: the failure surfaces, and callers within the backoff share it.
        let (google_b, origin) = google(vec![(200, 30), (503, 0)]).await;
        let provider = federated(&origin, &subject);
        provider.get_credential().await.unwrap();
        for _ in 0..2 {
            let err = provider.get_credential().await.unwrap_err();
            assert!(
                err.to_string()
                    .contains("federated token refresh failed: STS answered 503"),
                "{err}"
            );
        }
        assert_eq!(google_b.forms.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn impersonation_trades_the_federated_token_for_the_service_accounts() {
        // The federated token expires at once, so the second call is served from the cache only if it keeps the impersonated token's own lifetime.
        let (google, origin) = google(vec![(200, 0)]).await;
        let subject = subject("ksa-jwt");
        let provider = impersonating(&origin, &subject);

        for _ in 0..2 {
            assert_eq!(
                provider.get_credential().await.unwrap().bearer,
                "impersonated-1"
            );
        }
        let forms = google.forms.lock().unwrap();
        assert_eq!(forms.len(), 1);
        assert_eq!(forms[0]["scope"], CLOUD_PLATFORM_SCOPE);
        let impersonations = google.impersonations.lock().unwrap();
        assert_eq!(impersonations.len(), 1, "the impersonated token is cached");
        let (bearer, body) = &impersonations[0];
        assert_eq!(bearer, "Bearer federated-1");
        assert_eq!(
            body,
            &serde_json::json!({ "scope": ["https://www.googleapis.com/auth/devstorage.read_write"], "lifetime": "3600s" })
        );
    }

    #[tokio::test]
    async fn a_refused_impersonation_fails_closed() {
        let (google, origin) = google(vec![(200, 3600)]).await;
        *google.impersonation_status.lock().unwrap() = 403;
        let subject = subject("ksa-jwt");
        let err = impersonating(&origin, &subject)
            .get_credential()
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("federated token refresh failed: IAM Credentials answered 403"),
            "{err}"
        );
    }

    #[test]
    fn unsupported_shapes_are_refused() {
        let good = config("/t");
        let generate = format!("https://iamcredentials.googleapis.com{IMPERSONATION_PATH}");
        for (pointer, value) in [
            (
                "/service_account_impersonation",
                serde_json::json!({ "token_lifetime_seconds": 600 }),
            ),
            (
                "/credential_source/url",
                serde_json::json!("http://metadata.example/token"),
            ),
            ("/credential_source/format/type", serde_json::json!("json")),
            (
                "/token_url",
                serde_json::json!("http://sts.googleapis.com/v1/token"),
            ),
            (
                "/token_url",
                serde_json::json!("https://sts.example/v1/token"),
            ),
            (
                "/service_account_impersonation_url",
                generate.replace("https:", "http:").into(),
            ),
            (
                "/service_account_impersonation_url",
                generate
                    .replace("googleapis.com", "googleapis.com.example")
                    .into(),
            ),
            (
                "/service_account_impersonation_url",
                format!("{generate}?x=1").into(),
            ),
            (
                "/service_account_impersonation_url",
                generate.replace("https://", "https://user@").into(),
            ),
            (
                "/service_account_impersonation_url",
                generate.replace("gc@", "gc?x=1#").into(),
            ),
            ("/universe_domain", serde_json::json!("example.com")),
        ] {
            let mut bad = good.clone();
            let (parent, field) = pointer.rsplit_once('/').unwrap();
            bad.pointer_mut(parent).unwrap()[field] = value;
            assert!(parse(&bad).is_err(), "{pointer} must be refused");
        }
        assert!(parse(&good).is_ok());
        let mut impersonating = good;
        impersonating["service_account_impersonation_url"] = generate.into();
        assert!(parse(&impersonating).is_ok());
    }
}
