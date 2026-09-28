pub mod client;
// Wire contract shared verbatim with the server (lives next to kymo.proto; both crates include it by path). Each side exercises its half — the server slices, the client splices — so the other half is dead code by design.
#[path = "../../../proto/chart_delta.rs"]
#[allow(dead_code)]
pub mod chart_delta;
mod gen;
mod routes;
mod ws;
#[path = "../../../proto/ws_rpc.rs"]
#[allow(dead_code)]
mod ws_rpc;

pub use client::GrpcClient;
pub use gen::proto;
pub use ws::{
    connection, connection_changed, is_stale, merge_versions, raise, set_page_visibility,
    subscribe_push, wait_until_page_visible,
};
pub use ws_rpc::RELOAD_REQUIRED;
