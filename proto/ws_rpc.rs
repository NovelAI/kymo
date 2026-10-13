//! Shared method paths and framing for the frozen browser WebSocket transport (kymo-server's ws_proxy.rs, kymo-frontend's grpc/ws.rs).
//!
//! The server dispatcher and frontend wrappers include this file directly so
//! a path rename cannot compile on only one side. The manifest test also
//! checks that every active browser path still names an RPC in kymo.proto.
//!
//! Framing (binary, little-endian):
//!   request:  [u32 id][u16 path_len][path utf8][protobuf request bytes]
//!   response: [u32 id][u8 grpc code, 0 = OK][protobuf response bytes, or utf8 error message when code != 0]
//! The format is frozen: open dashboard tabs keep their WASM bundle (and this framing) until reload, so changing it in place strands them however the deploys are ordered. Evolve by mounting a new route beside it.
//!
//! /grpc-ws2 (hosted only) is such a route: the same framing plus one flag bit per direction marking a zlib-compressed (RFC 1950) body, REQUEST_DEFLATED in path_len and RESPONSE_DEFLATED in the code byte. Either side compresses a body of at least DEFLATE_MIN_BYTES and sends it compressed only when that is smaller. /grpc-ws and the local route never set either bit.

pub const LIST_PROJECTS: &str = "/kymo.Kymo/ListProjects";
pub const LIST_RUNS: &str = "/kymo.Kymo/ListRuns";
pub const LIST_METRICS: &str = "/kymo.Kymo/ListMetrics";
pub const LIST_RUN_SET_METRICS: &str = "/kymo.Kymo/ListRunSetMetrics";
pub const RENAME_RUN: &str = "/kymo.Kymo/RenameRun";
pub const TRASH_RUNS: &str = "/kymo.Kymo/TrashRuns";
pub const RESTORE_RUN: &str = "/kymo.Kymo/RestoreRun";
pub const LIST_TRASH: &str = "/kymo.Kymo/ListTrash";
pub const GET_RUN: &str = "/kymo.Kymo/GetRun";
pub const QUERY_CHART: &str = "/kymo.Kymo/QueryChart";
pub const QUERY_CDN_KEYS: &str = "/kymo.Kymo/QueryCdnKeys";
pub const QUERY_TEXT_WINDOW: &str = "/kymo.Kymo/QueryTextWindow";
pub const POLL_VERSIONS: &str = "/kymo.Kymo/PollVersions";

pub const PUSH_CONTROL: &str = "/kymo.push/control";

/// The local dashboard's socket, on the loopback listener that serves its page: /grpc-ws's framing, never compressed. `trash` is the one project ID the shared server already reserves; nesting local transport there steals no hosted-valid project/run URL.
pub const LOCAL_WS_PATH: &str = "/trash/_kymo-grpc-ws-local-v1";

/// /grpc-ws2: set in a request's path-length field when its body is compressed (paths are far shorter than 32 KiB).
pub const REQUEST_DEFLATED: u16 = 0x8000;
/// /grpc-ws2: set in a response's code byte when its payload is compressed (gRPC codes stop at 16).
pub const RESPONSE_DEFLATED: u8 = 0x80;
/// Smaller bodies go raw on /grpc-ws2 in both directions, and with them the ordinary chart polls of charts up to about 20 runs: compressing those would add a codec pass at both ends of every poll to save a kilobyte or so. With 25-character run ids and names, poll replies compress from 21-24 runs and requests from 30-34; longer ids or names lower those counts.
pub const DEFLATE_MIN_BYTES: usize = 4096;

/// /grpc-ws2: how the Internal error starts that a server answers to a compressed request body it cannot inflate (corrupt, truncated, or with bytes after the stream). The frontend counts it as a failure of the route, as it does a reply that fails to inflate.
pub const REQUEST_INFLATE_FAILED: &str = "request body inflate failed";

/// `deflated` (/grpc-ws2 only) marks `body` as compressed.
pub fn encode_request(id: u32, path: &str, body: &[u8], deflated: bool) -> Vec<u8> {
    debug_assert!(path.len() < usize::from(REQUEST_DEFLATED));
    let mut path_len = path.len() as u16;
    if deflated {
        path_len |= REQUEST_DEFLATED;
    }
    let mut frame = Vec::with_capacity(6 + path.len() + body.len());
    frame.extend_from_slice(&id.to_le_bytes());
    frame.extend_from_slice(&path_len.to_le_bytes());
    frame.extend_from_slice(path.as_bytes());
    frame.extend_from_slice(body);
    frame
}

/// `(id, path, body, deflated)`, reading the compression bit when `ws2`. None on malformed input: a client that can't frame correctly can't be answered by id either.
pub fn decode_request(buf: &[u8], ws2: bool) -> Option<(u32, &str, &[u8], bool)> {
    let id = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let path_len = u16::from_le_bytes(buf.get(4..6)?.try_into().ok()?);
    let flag = if ws2 { REQUEST_DEFLATED } else { 0 };
    let deflated = path_len & flag != 0;
    let path_len = usize::from(path_len & !flag);
    let path = std::str::from_utf8(buf.get(6..6 + path_len)?).ok()?;
    let body = buf.get(6 + path_len..)?;
    Some((id, path, body, deflated))
}

/// `code` 0 carries a protobuf payload, any other a UTF-8 error message.
pub fn encode_response(id: u32, code: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.extend_from_slice(&id.to_le_bytes());
    frame.push(code);
    frame.extend_from_slice(payload);
    frame
}

/// A response frame; `payload` is still compressed when `deflated`.
#[derive(Debug, PartialEq)]
pub struct ResponseFrame {
    pub id: u32,
    pub code: u8,
    pub deflated: bool,
    pub payload: Vec<u8>,
}

/// Reads the code byte's compression bit when `ws2`, and clears it from `code` on either route: no gRPC code reaches it.
pub fn decode_response(mut buf: Vec<u8>, ws2: bool) -> Option<ResponseFrame> {
    let id = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let code = *buf.get(4)?;
    let deflated = ws2 && code & RESPONSE_DEFLATED != 0;
    let code = code & !RESPONSE_DEFLATED;
    // Shift the 5-byte header off in place: chart payloads run to megabytes, and a second allocation+copy per frame is real work.
    buf.drain(..5);
    Some(ResponseFrame {
        id,
        code,
        deflated,
        payload: buf,
    })
}

/// The Close code the server sends every dashboard socket when it shuts down (RFC 6455's "going away"). The frontend reads it (grpc/ws.rs): no strike against /grpc-ws2, and it drops the origin's /grpc-ws2 refusal record.
pub const SHUTDOWN_CLOSE_CODE: u16 = 1001;

/// Sent as `?rev=` on the socket URL. Bump it in the frontend change that stops using a wire shape the server will later drop.
pub const FRONTEND_WIRE_REVISION: u32 = 2;
/// Oldest frontend revision the server serves. Raise it only after every served frontend has reached that revision.
pub const MIN_FRONTEND_WIRE_REVISION: u32 = 2;
// A server must serve the bundle built from its own commit.
const _: () = assert!(MIN_FRONTEND_WIRE_REVISION <= FRONTEND_WIRE_REVISION);
/// Frozen: current bundles match this exact message, stop connecting and show it. Sent as Unavailable to every request from a frontend below the floor; older bundles retry that and keep what they show. Never send it as InvalidArgument: dashboards 20260928-67e9241 through 20260928-a7ae00e reload on that, and pre-`?rev=` bundles replace charts and logs with it.
pub const RELOAD_REQUIRED: &str = "kymo was updated; reload this tab";

pub const ALL_FROZEN_WIRE_PATHS: &[&str] = &[
    LIST_PROJECTS,
    LIST_RUNS,
    LIST_METRICS,
    LIST_RUN_SET_METRICS,
    RENAME_RUN,
    TRASH_RUNS,
    RESTORE_RUN,
    LIST_TRASH,
    GET_RUN,
    QUERY_CHART,
    QUERY_CDN_KEYS,
    QUERY_TEXT_WINDOW,
    POLL_VERSIONS,
    PUSH_CONTROL,
];

/// One typed source for both the frontend route declarations and server
/// dispatcher. The callback receives `(constant, request, response, method)`
/// tuples and decides how to render them in its crate.
macro_rules! browser_rpc_routes {
    ($callback:ident) => {
        $callback! {
            (LIST_PROJECTS, ListProjectsRequest, ListProjectsResponse, list_projects),
            (LIST_RUNS, ListRunsRequest, ListRunsResponse, list_runs),
            (LIST_METRICS, ListMetricsRequest, ListMetricsResponse, list_metrics),
            (LIST_RUN_SET_METRICS, ListRunSetMetricsRequest, ListMetricsResponse, list_run_set_metrics),
            (RENAME_RUN, RenameRunRequest, RenameRunResponse, rename_run),
            (TRASH_RUNS, TrashRunsRequest, TrashRunsResponse, trash_runs),
            (RESTORE_RUN, RestoreRunRequest, RestoreRunResponse, restore_run),
            (LIST_TRASH, ListTrashRequest, ListTrashResponse, list_trash),
            (GET_RUN, GetRunRequest, GetRunResponse, get_run),
            (QUERY_CHART, ChartRequest, ChartResponse, query_chart),
            (QUERY_CDN_KEYS, QueryCdnKeysRequest, QueryCdnKeysResponse, query_cdn_keys),
            (QUERY_TEXT_WINDOW, QueryTextWindowRequest, QueryTextWindowResponse, query_text_window),
            (POLL_VERSIONS, PollVersionsRequest, PollVersionsResponse, poll_versions),
        }
    };
}
pub(crate) use browser_rpc_routes;

macro_rules! declare_browser_rpc_paths {
    ($(($name:ident, $request:ident, $response:ident, $method:ident)),* $(,)?) => {
        pub const BROWSER_RPC_PATHS: &[&str] = &[$($name),*];
    };
}
browser_rpc_routes!(declare_browser_rpc_paths);

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    struct BrowserRpcAbi {
        path: &'static str,
        request: &'static str,
        response: &'static str,
        method: &'static str,
    }

    macro_rules! declare_browser_rpc_abi {
        ($(($name:ident, $request:ident, $response:ident, $method:ident)),* $(,)?) => {
            const BROWSER_RPC_ABI: &[BrowserRpcAbi] = &[
                $(BrowserRpcAbi {
                    path: $name,
                    request: stringify!($request),
                    response: stringify!($response),
                    method: stringify!($method),
                }),*
            ];
        };
    }
    browser_rpc_routes!(declare_browser_rpc_abi);

    #[test]
    fn active_browser_routes_match_the_proto_service_and_frozen_abi() {
        let proto = include_str!("kymo.proto");
        let mut frozen = String::new();
        for route in BROWSER_RPC_ABI {
            let rpc_name = route.path.rsplit('/').next().expect("path has method name");
            let declaration = format!(
                "rpc {rpc_name} ({}) returns ({});",
                route.request, route.response
            );
            assert!(
                proto.contains(&declaration),
                "browser RPC route {} has no matching kymo.proto declaration {declaration:?}",
                route.path
            );
            use std::fmt::Write;
            writeln!(
                frozen,
                "{}|{}|{}|{}",
                route.path, route.request, route.response, route.method
            )
            .unwrap();
        }
        assert_eq!(frozen, include_str!("ws_rpc_routes.golden"));
    }

    #[test]
    fn every_frozen_wire_path_is_unique_and_matches_the_golden_abi() {
        let mut seen = HashSet::new();
        for path in ALL_FROZEN_WIRE_PATHS {
            assert!(seen.insert(*path), "duplicate frozen wire path {path}");
        }
        assert_eq!(ALL_FROZEN_WIRE_PATHS.len(), BROWSER_RPC_PATHS.len() + 1);
        for path in BROWSER_RPC_PATHS {
            assert!(
                ALL_FROZEN_WIRE_PATHS.contains(path),
                "active browser RPC path {path} is missing from the frozen ABI"
            );
        }
        let actual = ALL_FROZEN_WIRE_PATHS.join("\n") + "\n";
        assert_eq!(actual, include_str!("ws_rpc_paths.golden"));
    }

    #[test]
    fn request_frames_carry_the_compression_bit_only_on_grpc_ws2() {
        let path = "/kymo.Kymo/PollVersions";
        let wire = |length_high: u8, body: &[u8]| {
            let mut frame = vec![7, 0, 0, 0, path.len() as u8, length_high];
            frame.extend_from_slice(path.as_bytes());
            frame.extend_from_slice(body);
            frame
        };
        let raw = encode_request(7, path, b"body", false);
        assert_eq!(raw, wire(0, b"body"));
        let deflated = encode_request(7, path, b"zlib", true);
        assert_eq!(deflated, wire(0x80, b"zlib"));
        for ws2 in [false, true] {
            assert_eq!(
                decode_request(&raw, ws2),
                Some((7, path, &b"body"[..], false))
            );
        }
        assert_eq!(
            decode_request(&deflated, true),
            Some((7, path, &b"zlib"[..], true))
        );
        // /grpc-ws reads the bit as length, so the same frame is malformed there and dropped.
        assert_eq!(decode_request(&deflated, false), None);
    }

    #[test]
    fn response_frames_carry_the_compression_bit_only_on_grpc_ws2() {
        assert_eq!(encode_response(7, 0, &[9, 9]), [7, 0, 0, 0, 0, 9, 9]);
        let ok = |deflated, payload: &[u8]| {
            Some(ResponseFrame {
                id: 9,
                code: 0,
                deflated,
                payload: payload.to_vec(),
            })
        };
        let flagged = encode_response(9, RESPONSE_DEFLATED, b"zlib");
        assert_eq!(decode_response(flagged.clone(), true), ok(true, b"zlib"));
        // /grpc-ws never sets it: a frame that has it anyway is not read as compressed, and the flag is cleared from its code byte, as on /grpc-ws2.
        assert_eq!(decode_response(flagged, false), ok(false, b"zlib"));
        // A raw frame decodes the same on either socket.
        for ws2 in [false, true] {
            assert_eq!(
                decode_response(encode_response(9, 0, b"ok"), ws2),
                ok(false, b"ok")
            );
        }
    }

    #[test]
    fn malformed_frames_are_rejected_not_panicked() {
        for ws2 in [false, true] {
            assert_eq!(decode_request(&[], ws2), None);
            assert_eq!(decode_request(&[1, 2, 3], ws2), None);
            // path_len pointing past the buffer, with and without the flag
            for length_flags in [0, REQUEST_DEFLATED] {
                let mut frame = encode_request(1, "", b"short", false);
                frame[4..6].copy_from_slice(&(500 | length_flags).to_le_bytes());
                assert_eq!(decode_request(&frame, ws2), None);
            }
            // non-utf8 path
            assert_eq!(decode_request(&[1, 0, 0, 0, 2, 0, 0xff, 0xfe], ws2), None);
            for short in [&[][..], &[1, 0, 0, 0][..]] {
                assert_eq!(decode_response(short.to_vec(), ws2), None);
            }
        }
    }
}
