// The shared schema necessarily generates ingest and watchdog messages that the browser never constructs itself.
#[allow(dead_code)]
pub mod proto {
    tonic::include_proto!("kymo");
}
