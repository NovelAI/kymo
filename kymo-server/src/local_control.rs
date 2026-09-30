use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use tonic::{Request, Response, Status};

use crate::ingest::BumpCoalescer;
use crate::local_auth;
use crate::local_proto::local_runtime_control_server::LocalRuntimeControl;
use crate::local_proto::{
    GetActivityRequest, GetActivityResponse, ShutdownLocalRequest, ShutdownLocalResponse,
};

const LIFECYCLE_AUTH_FORMAT_VERSION: u32 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretFile {
    format_version: u32,
    lifecycle_bearer: String,
}

pub(crate) struct LifecycleAuth {
    bearer: String,
}

impl LifecycleAuth {
    pub(crate) fn read(path: &Path) -> Result<Arc<Self>> {
        let bytes = crate::private_file::read(path, "local lifecycle authentication", 4096)?;
        let secret: SecretFile =
            serde_json::from_slice(&bytes).context("parse local lifecycle authentication file")?;
        ensure!(
            secret.format_version == LIFECYCLE_AUTH_FORMAT_VERSION,
            "unsupported local lifecycle authentication format"
        );
        ensure!(
            local_auth::valid_token(&secret.lifecycle_bearer),
            "lifecycle_bearer must be a 43-character base64url token"
        );
        Ok(Arc::new(Self {
            bearer: secret.lifecycle_bearer,
        }))
    }

    pub(crate) fn conflicts_with(&self, auth: &local_auth::LocalAuth) -> bool {
        auth.is_server_bearer(&self.bearer)
    }

    pub(crate) fn interceptor(self: Arc<Self>) -> impl tonic::service::Interceptor + Clone {
        move |request: Request<()>| {
            if local_auth::authorize_metadata(request.metadata(), &self.bearer) {
                Ok(request)
            } else {
                Err(Status::unauthenticated("lifecycle authentication required"))
            }
        }
    }

    #[cfg(test)]
    fn testing(bearer: &str) -> Arc<Self> {
        Arc::new(Self {
            bearer: bearer.to_owned(),
        })
    }
}

pub(crate) struct LocalRuntimeControlService {
    bumps: Arc<BumpCoalescer>,
    activity: Arc<crate::activity::ActivityTracker>,
}

impl LocalRuntimeControlService {
    pub(crate) fn new(
        bumps: Arc<BumpCoalescer>,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Self {
        Self { bumps, activity }
    }
}

#[tonic::async_trait]
impl LocalRuntimeControl for LocalRuntimeControlService {
    async fn shutdown_local(
        &self,
        _request: Request<ShutdownLocalRequest>,
    ) -> Result<Response<ShutdownLocalResponse>, Status> {
        self.bumps.request_shutdown();
        Ok(Response::new(ShutdownLocalResponse {}))
    }

    async fn get_activity(
        &self,
        _request: Request<GetActivityRequest>,
    ) -> Result<Response<GetActivityResponse>, Status> {
        // Snapshot before constructing the response and do not acquire an application-work guard: supervisor observation must not observe or prolong itself.
        let snapshot = self.activity.snapshot();
        Ok(Response::new(GetActivityResponse {
            keepalive_idle_for_ms: snapshot.keepalive_idle_for_ms,
            last_committed_ingest_ago_ms: snapshot.last_committed_ingest_ago_ms,
            frontend_connections: snapshot.frontend_connections,
            in_flight_work: snapshot.in_flight_work,
            ingest_bookkeeping_draining: self.bumps.is_draining(),
            fulfilled_hold_ids: snapshot.fulfilled_hold_ids,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use tonic::service::Interceptor;

    use super::*;

    const LIFECYCLE: &str = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDU";
    const CLIENT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn request(token: &str) -> Request<()> {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    }

    #[test]
    fn lifecycle_secret_requires_private_canonical_token() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("lifecycle.json");
        std::fs::write(
            &path,
            format!(r#"{{"format_version":1,"lifecycle_bearer":"{LIFECYCLE}"}}"#),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(LifecycleAuth::read(&path).is_ok());

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(LifecycleAuth::read(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(
            &path,
            format!(r#"{{"format_version":2,"lifecycle_bearer":"{LIFECYCLE}"}}"#),
        )
        .unwrap();
        assert!(LifecycleAuth::read(&path).is_err());
    }

    #[tokio::test]
    async fn lifecycle_scope_alone_starts_the_one_way_drain() {
        let auth = LifecycleAuth::testing(LIFECYCLE);
        let mut interceptor = auth.interceptor();
        assert!(interceptor.call(request(CLIENT)).is_err());
        assert!(interceptor.call(request(LIFECYCLE)).is_ok());

        let client_auth = local_auth::LocalAuth::testing(LIFECYCLE);
        assert!(LifecycleAuth::testing(LIFECYCLE).conflicts_with(&client_auth));

        let bumps = BumpCoalescer::empty_for_test();
        let activity = crate::activity::ActivityTracker::new_local();
        let service = LocalRuntimeControlService::new(bumps.clone(), activity);
        service
            .shutdown_local(Request::new(ShutdownLocalRequest {}))
            .await
            .unwrap();
        assert!(bumps.is_draining());
    }

    #[tokio::test]
    async fn activity_monitoring_does_not_observe_itself_as_application_work() {
        let bumps = BumpCoalescer::empty_for_test();
        let activity = crate::activity::ActivityTracker::new_local();
        let service = LocalRuntimeControlService::new(bumps, activity.clone());

        for _ in 0..3 {
            let response = service
                .get_activity(Request::new(GetActivityRequest {}))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.in_flight_work, 0);
        }
        assert_eq!(activity.snapshot().in_flight_work, 0);
    }
}
