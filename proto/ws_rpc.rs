//! Shared method paths for the frozen browser WebSocket transport.
//!
//! The server dispatcher and frontend wrappers include this file directly so
//! a path rename cannot compile on only one side. The manifest test also
//! checks that every active browser path still names an RPC in kymo.proto.

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

/// Sent as `?rev=` on the socket URL. Bump it in the frontend change that stops using a wire shape the server will later drop.
pub const FRONTEND_WIRE_REVISION: u32 = 2;
/// Oldest frontend revision the server serves. Raise it only after every served frontend has reached that revision.
pub const MIN_FRONTEND_WIRE_REVISION: u32 = 1;
// A server must serve the bundle built from its own commit.
const _: () = assert!(MIN_FRONTEND_WIRE_REVISION <= FRONTEND_WIRE_REVISION);
/// Sent with InvalidArgument to frontends below the floor, which stop connecting and reload. Frozen: shipped bundles match it exactly.
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
}
