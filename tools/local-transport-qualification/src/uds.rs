use std::convert::Infallible;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

pub mod proto {
    tonic::include_proto!("kymo.transport.v1");
}

use proto::Payload;
use proto::probe_client::ProbeClient;
use proto::probe_server::{Probe, ProbeServer};

const MAX_MESSAGE_BYTES: usize = 64 * 1024;

#[derive(Clone)]
struct ProbeService {
    cancellations: Arc<AtomicUsize>,
}

struct CancellationGuard(Arc<AtomicUsize>);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl Probe for ProbeService {
    async fn echo(&self, request: Request<Payload>) -> Result<Response<Payload>, Status> {
        Ok(Response::new(request.into_inner()))
    }

    type BidiStream = Pin<Box<dyn Stream<Item = Result<Payload, Status>> + Send>>;

    async fn bidi(
        &self,
        request: Request<Streaming<Payload>>,
    ) -> Result<Response<Self::BidiStream>, Status> {
        let mut input = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            while let Some(item) = input.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn slow(&self, request: Request<Payload>) -> Result<Response<Payload>, Status> {
        let guard = CancellationGuard(self.cancellations.clone());
        tokio::time::sleep(Duration::from_secs(30)).await;
        std::mem::forget(guard);
        Ok(Response::new(request.into_inner()))
    }
}

pub async fn qualify(root: &Path, python: &Path) -> Result<()> {
    make_private_dir(root)?;
    reject_overlong_socket_path()?;
    let grpc_path = root.join("grpc.sock");
    let http_path = root.join("http.sock");
    let grpc_listener = private_listener(&grpc_path)?;
    let http_listener = private_listener(&http_path)?;
    let cancellations = Arc::new(AtomicUsize::new(0));
    let service = ProbeService {
        cancellations: cancellations.clone(),
    };
    let (grpc_stop_tx, grpc_stop_rx) = oneshot::channel();
    let grpc_task = tokio::spawn(async move {
        Server::builder()
            .add_service(
                ProbeServer::new(service)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming_shutdown(UnixListenerStream::new(grpc_listener), async {
                let _ = grpc_stop_rx.await;
            })
            .await
    });
    let app = Router::new()
        .route("/echo", post(|body: Bytes| async move { body }))
        .route(
            "/stream",
            get(|| async {
                Body::from_stream(stream::iter([
                    Ok::<_, Infallible>(Bytes::from_static(b"stream-")),
                    Ok::<_, Infallible>(Bytes::from_static(b"works")),
                ]))
            }),
        )
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "too late"
            }),
        )
        .layer(DefaultBodyLimit::max(MAX_MESSAGE_BYTES));
    let (http_stop_tx, http_stop_rx) = oneshot::channel();
    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, app)
            .with_graceful_shutdown(async {
                let _ = http_stop_rx.await;
            })
            .await
    });

    let result = exercise_clients(&grpc_path, &http_path, python, &cancellations).await;
    let _ = grpc_stop_tx.send(());
    let _ = http_stop_tx.send(());
    let grpc_result = grpc_task
        .await
        .context("join tonic Unix-socket server")
        .and_then(|result| result.context("serve tonic over Unix socket"));
    let http_result = http_task
        .await
        .context("join Axum Unix-socket server")
        .and_then(|result| result.context("serve Axum over Unix socket"));
    let grpc_cleanup = remove_socket(&grpc_path);
    let http_cleanup = remove_socket(&http_path);
    result?;
    grpc_result?;
    http_result?;
    grpc_cleanup?;
    http_cleanup?;
    Ok(())
}

async fn exercise_clients(
    grpc_path: &Path,
    http_path: &Path,
    python: &Path,
    cancellations: &AtomicUsize,
) -> Result<()> {
    let endpoint = Endpoint::from_shared(format!("unix:{}", grpc_path.display()))?;
    let mut client = ProbeClient::new(endpoint.connect().await?);
    let payload = Payload {
        data: b"rust-tonic-uds".to_vec(),
    };
    let echoed = client.echo(payload.clone()).await?.into_inner();
    ensure!(
        echoed == payload,
        "Rust tonic Unix-socket unary echo changed payload"
    );
    let outbound = stream::iter([
        Payload {
            data: b"one".to_vec(),
        },
        Payload {
            data: b"two".to_vec(),
        },
    ]);
    let inbound = client
        .bidi(outbound)
        .await?
        .into_inner()
        .collect::<Vec<_>>()
        .await;
    let data = inbound
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|payload| payload.data)
        .collect::<Vec<_>>();
    ensure!(
        data == [b"one".to_vec(), b"two".to_vec()],
        "Rust tonic Unix-socket bidi echo changed payloads"
    );

    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("qualify_python_uds.py");
    let output = Command::new(python)
        .arg(script)
        .arg("--grpc-socket")
        .arg(grpc_path)
        .arg("--http-socket")
        .arg(http_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("run Python UDS probe with {}", python.display()))?;
    ensure!(
        output.status.success(),
        "Python UDS probe failed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for _ in 0..40 {
        if cancellations.load(Ordering::SeqCst) > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(
        false,
        "Python gRPC cancellation did not cancel the tonic handler"
    );
    Ok(())
}

pub fn make_private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("create private directory {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    ensure!(
        mode == 0o700,
        "private directory {} has mode {mode:o}",
        path.display()
    );
    Ok(())
}

fn private_listener(path: &Path) -> Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => std::fs::remove_file(path)?,
        Ok(_) => anyhow::bail!(
            "refusing to replace non-socket Unix endpoint {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener =
        UnixListener::bind(path).with_context(|| format!("bind Unix socket {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    ensure!(
        mode == 0o600,
        "Unix socket {} has mode {mode:o}",
        path.display()
    );
    Ok(listener)
}

fn remove_socket(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect Unix socket before cleanup {}", path.display()))?;
    ensure!(
        metadata.file_type().is_socket(),
        "refusing to remove non-socket Unix endpoint {}",
        path.display()
    );
    std::fs::remove_file(path)?;
    Ok(())
}

fn reject_overlong_socket_path() -> Result<()> {
    let path = PathBuf::from("/tmp").join(format!("m2q-{}", "x".repeat(180)));
    ensure!(
        UnixListener::bind(&path).is_err(),
        "platform accepted a 189-byte Unix-socket path; update the documented path bound"
    );
    Ok(())
}
