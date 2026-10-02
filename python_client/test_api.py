"""Offline tests for kymo.Api. The gRPC layer is faked; nothing connects."""

import concurrent.futures
import json
import math
import multiprocessing
import sys
import threading
import time
import unittest
from types import SimpleNamespace as NS
from unittest import mock

import grpc
import httpx

import kymo
from kymo import api as kymo_api
from kymo._generated import kymo_pb2 as pb


def offline_api(stub, local=False):
    # Construction opens nothing, so a stub swapped in before the first call is all the fake there is.
    api = (
        kymo_api.Api(mode="local")
        if local
        else kymo_api.Api("h.example:1", mode="hosted")
    )
    api._stub = stub
    return api


class RpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


def chart(x_values, *series, banded=False):
    resp = pb.ChartResponse(x_values=x_values, banded=banded)
    for label, starts, lens, values, nan_indices, nan_kinds in series:
        resp.series.add(
            label=label,
            run_id="r",
            seg_starts=starts,
            seg_lens=lens,
            values=values,
            nan_indices=nan_indices,
            nan_kinds=nan_kinds,
        )
    return resp


class HistoryTests(unittest.TestCase):
    def history(self, resp, tagged=False, run_id="r"):
        # Mirrors query.rs: the untagged filter answers nothing for a metric with tagged rows, and an untagged series ignores the filter.
        seen = []

        def query_chart(req, timeout):
            seen.append(req)
            filtered = list(req.y_series[0].tags) == [""]
            return pb.ChartResponse() if filtered and tagged else resp

        out = offline_api(NS(QueryChart=query_chart)).history(
            "p", run_id, "loss", step_max=9
        )
        self.assertEqual(list(seen[0].y_series[0].tags), [""])
        self.requests = len(seen)
        for req in seen:
            self.assertEqual(req.target_resolution, 0)
            self.assertFalse(req.use_timestamp_axis)
            self.assertFalse(req.HasField("step_min"))
            self.assertEqual(req.step_max, 9)
        return out

    def test_untagged_series_restores_markers_at_their_steps(self):
        resp = chart([0, 1, 2, 3], ("r", [0, 3], [1, 1], [1.5, 2.5], [1, 2], [2, 3]))
        self.assertEqual(
            self.history(resp),
            {"": [(0, 1.5), (1, math.inf), (2, -math.inf), (3, 2.5)]},
        )
        self.assertEqual(self.requests, 1)

    def test_empty_kinds_mean_nan_and_kind4_is_not_a_value(self):
        resp = chart([0, 1], ("r", [0], [1], [4.0], [1, 0], []))
        (points,) = self.history(resp).values()
        self.assertEqual(points[0], (0, 4.0))
        self.assertTrue(math.isnan(points[1][1]) and points[2][0] == 1)
        resp = chart([0], ("r", [0], [1], [4.0], [0], [4]))
        self.assertEqual(self.history(resp)[""], [(0, 4.0)])

    def test_duplicate_x_collision_keeps_value_then_marker(self):
        resp = chart([5], ("r", [0], [1], [7.0], [0], [1]))
        (value, marker) = self.history(resp)[""]
        self.assertEqual(value, (5, 7.0))
        self.assertEqual(marker[0], 5)
        self.assertTrue(math.isnan(marker[1]))

    def test_tagged_series_key_by_tag(self):
        resp = chart(
            [0], ("0", [0], [1], [1.0], [], []), ("1", [0], [1], [2.0], [], [])
        )
        self.assertEqual(
            self.history(resp, tagged=True), {"0": [(0, 1.0)], "1": [(0, 2.0)]}
        )
        self.assertEqual(self.requests, 2)

    def test_lone_tag_equal_to_the_run_id_stays_a_tag(self):
        # init(run_id="0") plus a one-element list: the server labels the series "0", exactly as it would label an untagged one
        resp = chart([7], ("0", [0], [1], [1.0], [], []))
        self.assertEqual(self.history(resp, tagged=True, run_id="0"), {"0": [(7, 1.0)]})
        self.assertEqual(self.requests, 3)

    def test_untagged_series_arriving_between_requests_stays_untagged(self):
        # the first point lands after the filtered request answered empty: the unfiltered answer is a lone series labelled with the run id
        resp = chart([1], ("r", [0], [1], [1.25], [], []))
        answers = [pb.ChartResponse(), resp, resp]
        api = offline_api(NS(QueryChart=lambda req, timeout: answers.pop(0)))
        self.assertEqual(api.history("p", "r", "loss"), {"": [(1, 1.25)]})
        self.assertEqual(answers, [])

    def test_lone_tagged_series_keys_by_its_tag(self):
        resp = chart([0], ("0", [0], [1], [1.0], [], []))
        self.assertEqual(self.history(resp, tagged=True), {"0": [(0, 1.0)]})
        self.assertEqual(self.requests, 2)

    def test_steps_beyond_f64_precision_are_refused(self):
        ok = chart([2**53 - 1], ("r", [0], [1], [1.0], [], []))
        self.assertEqual(self.history(ok), {"": [(2**53 - 1, 1.0)]})
        for x in (2**53, -(2**53)):
            with self.assertRaisesRegex(ValueError, "cannot return exactly"):
                self.history(chart([x], ("r", [0], [1], [1.0], [], [])))

    def test_banded_answer_is_refused(self):
        # bucket means must never pass for samples
        with self.assertRaises(RuntimeError):
            self.history(chart([0], ("r", [0], [1], [1.0], [], []), banded=True))

    def test_missing_metric_is_empty(self):
        self.assertEqual(self.history(chart([])), {})


class RecordTests(unittest.TestCase):
    def test_runs_map_status_and_optional_times(self):
        resp = pb.ListRunsResponse()
        resp.runs.add(
            run_id="a",
            run_name="A",
            ordinal=2,
            status=pb.RUN_STATUS_PRESUMED_DEAD,
            created_at_ms=5,
            last_ingested_at_ms=7,
        )
        resp.runs.add(
            run_id="b", created_at_ms=1, status=99
        )  # a state this client predates
        runs = offline_api(NS(ListRuns=lambda req, timeout: resp)).runs("p")
        self.assertEqual(
            runs[0], kymo_api.Run("a", "A", 2, "presumed_dead", 5, 7, None)
        )
        self.assertEqual(runs[1].status, "unknown")

    def test_metric_kinds(self):
        resp = pb.ListMetricsResponse()
        resp.metrics.add(metric_name="loss", metric_type=pb.MetricInfo.NUMERIC)
        resp.metrics.add(
            metric_name="logs/std_out", metric_type=pb.MetricInfo.TEXT_STREAM
        )
        resp.metrics.add(metric_name="eval/images", metric_type=pb.MetricInfo.CDN)
        kinds = offline_api(NS(ListMetrics=lambda req, timeout: resp)).metrics("p", "r")
        # sorted by name, an order the CLI's listing and default summary rely on
        self.assertEqual(
            list(kinds.items()),
            [
                ("eval/images", "cdn"),
                ("logs/std_out", "text_stream"),
                ("loss", "numeric"),
            ],
        )

    def test_media_marks_pending_uploads(self):
        series = NS(
            entries=[NS(step=0, cdn_key="a.png"), NS(step=1, cdn_key="pending:x")]
        )
        api = offline_api(NS(QueryCdnKeys=lambda req, timeout: NS(series=[series])))
        self.assertEqual(api.media("p", "r", "img"), [(0, "a.png"), (1, None)])


class LogsTests(unittest.TestCase):
    def read(self, total, pages, **kw):
        # pages: indexed-line lists the windows return in order after the size probe
        calls = []

        def qtw(req, timeout):
            calls.append(
                (req.line_offset, req.line_limit, list(req.metric_names), req.search)
            )
            page = [] if req.line_offset == 2**63 or not pages else pages.pop(0)
            lines = [
                NS(line_index=i, metric_name="logs/std_out", step=10 + i, text=t)
                for i, t in page
            ]
            return NS(total_lines=total, lines=lines)

        logs = offline_api(NS(QueryTextWindow=qtw)).logs("p", "r", **kw)
        self.assertEqual(logs.total_lines, total)
        return logs.lines, calls

    def test_probe_then_tail_window_forwarding_the_query(self):
        page = [(9950 + i, f"T{i}") for i in range(50)]
        lines, calls = self.read(
            10000,
            [page],
            streams=["logs/std_out", "logs/std_err"],
            search="oom",
            limit=50,
        )
        # the probe sits past every real stream, so it reads no payload; the default window is the tail
        self.assertEqual([c[:2] for c in calls], [(2**63, 1), (9950, 50)])
        for _, _, names, search in calls:
            self.assertEqual((names, search), (["logs/std_out", "logs/std_err"], "oom"))
        self.assertEqual(lines[0], kymo_api.LogLine(9950, "logs/std_out", 9960, "T0"))

    def test_short_page_is_not_the_end(self):
        pages = [
            [(i, f"L{i}") for i in range(a, b)]
            for a, b in ((0, 1500), (1500, 3500), (3500, 4000))
        ]
        lines, calls = self.read(4000, pages, limit=4000)
        self.assertEqual([ln.text for ln in lines], [f"L{i}" for i in range(4000)])
        # the server clamps a window and can answer short: the cursor follows line_index, not request arithmetic
        self.assertEqual(
            [c[:2] for c in calls[1:]], [(0, 2000), (1500, 2000), (3500, 500)]
        )

    def test_skip_ahead_page_moves_the_cursor_without_duplicates(self):
        pages = [
            [(100, "S100"), (101, "S101")],
            [(102 + i, f"S{102 + i}") for i in range(98)],
        ]
        lines, calls = self.read(200, pages, limit=200)
        self.assertEqual([ln.index for ln in lines], list(range(100, 200)))
        self.assertEqual([c[:2] for c in calls[1:]], [(0, 200), (102, 98)])

    def test_explicit_window_ends_at_its_end_across_a_gap(self):
        # lines 100-119 vanished mid-read: the window keeps its gap rather than read lines past index 149
        page = [(120 + i, f"L{120 + i}") for i in range(30)]
        lines, calls = self.read(4000, [page], offset=100, limit=50)
        self.assertEqual([c[:2] for c in calls[1:]], [(100, 50)])
        self.assertEqual([ln.index for ln in lines], list(range(120, 150)))

    def test_empty_page_is_a_gap(self):
        # every line of the middle page vanished mid-read: the read steps over it rather than end there
        pages = [[(i, f"L{i}") for i in range(a, a + 2000)] for a in (0, 4000)]
        pages.insert(1, [])
        lines, calls = self.read(6000, pages)
        self.assertEqual(
            [ln.index for ln in lines], [*range(0, 2000), *range(4000, 6000)]
        )
        self.assertEqual(
            [c[:2] for c in calls[1:]], [(0, 2000), (2000, 2000), (4000, 2000)]
        )

    def test_no_progress_stops(self):
        # a page that re-anchored behind the cursor cannot advance it
        lines, calls = self.read(100, [[(40, "L40")]], offset=50, limit=50)
        self.assertEqual(([ln.index for ln in lines], len(calls)), ([40], 2))

    def test_whole_stream_and_offset_only_window(self):
        lines, _ = self.read(3, [[(i, f"L{i}") for i in range(3)]])
        self.assertEqual([ln.text for ln in lines], ["L0", "L1", "L2"])
        # with no limit the window ends at the probed total, as Logs.total_lines reports, even while the stream grows
        _, calls = self.read(100, [[(i, f"L{i}") for i in range(40, 100)]], offset=40)
        self.assertEqual(calls[1][:2], (40, 60))

    def stream(self, total, seen, refuse):
        # a stream that holds still; the server refuses the windows `refuse` picks for its 8 MiB cap, which 2000 long lines can exceed
        def qtw(req, timeout):
            seen.append((req.line_offset, req.line_limit))
            if req.line_offset == 2**63:
                return NS(total_lines=total, lines=[])
            if refuse(req):
                raise RpcError(grpc.StatusCode.RESOURCE_EXHAUSTED)
            end = min(total, req.line_offset + req.line_limit)
            lines = [
                NS(line_index=i, metric_name="logs/std_out", step=0, text=f"L{i}")
                for i in range(req.line_offset, end)
            ]
            return NS(total_lines=total, lines=lines)

        return offline_api(NS(QueryTextWindow=qtw)).logs("p", "r")

    def test_oversized_window_halves_its_page(self):
        seen = []
        logs = self.stream(600, seen, lambda req: req.line_limit > 500)
        self.assertEqual(len(logs.lines), 600)
        self.assertEqual([limit for _, limit in seen], [1, 600, 300, 300])
        with self.assertRaises(RpcError):
            self.stream(1, [], lambda req: True)

    def test_page_regrows_after_the_long_lines(self):
        # only the window over the long lines is refused; later pages grow back toward the server's cap
        seen = []
        logs = self.stream(
            3000, seen, lambda req: req.line_offset == 0 and req.line_limit > 500
        )
        self.assertEqual(len(logs.lines), 3000)
        self.assertEqual(
            seen[1:], [(0, 2000), (0, 1000), (0, 500), (500, 1000), (1500, 1500)]
        )

    def test_bad_arguments_are_refused_before_any_request(self):
        # a bare string: protobuf would split it into one-letter stream names and the server would answer an empty stream
        for kw, error in (
            ({"offset": -5, "limit": 3}, ValueError),
            ({"limit": -1}, ValueError),
            ({"streams": ()}, ValueError),
            ({"streams": "logs/std_out"}, TypeError),
        ):
            with self.assertRaises(error):
                offline_api(NS()).logs("p", "r", **kw)

    def test_huge_limit_pages_within_the_window_cap(self):
        lines, calls = self.read(
            3, [[(i, f"L{i}") for i in range(3)]], limit=sys.maxsize
        )
        self.assertEqual(calls[1][:2], (0, 2000))
        self.assertEqual(len(lines), 3)


class FetchTests(unittest.TestCase):
    def cdn_api(self, handler, local=False):
        api = offline_api(NS(), local=local)
        api._cdn = "http://c.example:8080"
        api._http = httpx.Client(transport=httpx.MockTransport(handler))
        self.addCleanup(api._http.close)
        return api

    def test_key_is_requested_encoded_whole(self):
        seen = []

        def handler(request):
            seen.append(request.url.raw_path)
            return httpx.Response(200, content=b"x")

        self.assertEqual(self.cdn_api(handler).fetch("a?b/c.png"), b"x")
        self.assertEqual(seen, [b"/cdn/a%3Fb%2Fc.png"])

    def test_local_fetch_first_connects_for_the_origin(self):
        # the local CDN origin comes from the endpoint, so a fetch before any RPC connects first
        def connect(api):
            api._cdn, api._stub = "http://c.example:9", NS()

        seen = []

        def handler(request):
            seen.append(request.url.port)
            return httpx.Response(200, content=b"x")

        api = self.cdn_api(handler, local=True)
        api._stub = None
        with mock.patch.object(kymo_api.Api, "_connect", connect):
            self.assertEqual(api.fetch("k.png"), b"x")
        self.assertEqual(seen, [9])

    def test_local_retry_follows_a_moved_origin(self):
        seen = []

        def handler(request):
            seen.append(request.url.port)
            if request.url.port == 8080:
                raise httpx.ConnectError("stack stopped", request=request)
            return httpx.Response(200, content=b"x")

        api = self.cdn_api(handler, local=True)
        with mock.patch.object(
            kymo_api.Api,
            "_connect",
            lambda self: setattr(self, "_cdn", "http://c.example:9"),
        ):
            self.assertEqual(api.fetch("k.png"), b"x")
        self.assertEqual(seen, [8080, 9])

    def test_run_info_reads_the_uploaded_manifest(self):
        manifest = {"v": 1, "class": "metadata", "data": {"config": {"lr": 2}}}
        api = self.cdn_api(
            lambda request: httpx.Response(200, content=json.dumps(manifest).encode())
        )
        # kymo writes info/run_info at step 0, and the server keeps one entry per step: its latest manifest, or a pending placeholder
        for key, expected in (("run.json", {"config": {"lr": 2}}), ("pending:x", None)):
            api._stub = NS(
                QueryCdnKeys=lambda req, timeout: NS(
                    series=[NS(entries=[NS(step=0, cdn_key=key)])]
                )
            )
            self.assertEqual(api.run_info("p", "r"), expected)


class LocalReconnectTests(unittest.TestCase):
    def test_local_retries_once_after_reconnect(self):
        # UNAVAILABLE: the stack stopped; UNAUTHENTICATED: it restarted on the same socket with a new bearer
        for code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNAUTHENTICATED):
            calls = []

            def projects(req, timeout):
                calls.append("old")
                raise RpcError(code)

            def retried(req, timeout, wait_for_ready):
                # the new channel shares gRPC's connection to the socket, which may still be backing off from the stop
                calls.append(("new", wait_for_ready))
                return NS(project_ids=["p"])

            api = offline_api(NS(ListProjects=projects), local=True)
            fresh = NS(ListProjects=retried)
            with mock.patch.object(
                kymo_api.Api, "_connect", lambda self: setattr(self, "_stub", fresh)
            ):
                self.assertEqual(api.projects(), ["p"])
            self.assertEqual(calls, ["old", ("new", True)])

    def test_reconnect_keeps_the_channel_while_the_stack_did_not_restart(self):
        # a server-side UNAVAILABLE re-ensures the same generation: a new channel could not help, and every one stays open until close()
        g1 = NS(endpoint_generation="g1", cdn_origin="http://c.example:9")
        endpoints = [
            g1,
            g1,
            NS(endpoint_generation="g2", cdn_origin="http://c.example:9"),
        ]
        api = offline_api(None, local=True)
        with (
            mock.patch(
                "kymo._local_runtime.ensure_local_endpoint",
                side_effect=endpoints,
            ),
            mock.patch(
                "kymo._local_runtime.grpc_channel", side_effect=lambda *a: mock.Mock()
            ),
            mock.patch.object(
                kymo_api.kymo_pb2_grpc,
                "KymoStub",
                side_effect=lambda channel: mock.Mock(),
            ),
            mock.patch.object(kymo_api, "_channel_opened", False),
        ):
            first = api._live_stub()
            self.assertIs(api._live_stub(first), first)
            second = api._live_stub(first)
            self.assertIsNot(second, first)
            # another thread that failed on `first` takes the new stub without a fourth ensure, which would exhaust `endpoints`
            self.assertIs(api._live_stub(first), second)
        # the restart's new channel leaves the old one open: another thread may still be calling through it
        self.assertEqual([c.close.called for c in api._channels], [False, False])
        api.close()
        self.assertEqual([c.close.called for c in api._channels], [True, True])

    def test_hosted_does_not_retry(self):
        def projects(req, timeout):
            raise RpcError(grpc.StatusCode.UNAVAILABLE)

        api = offline_api(NS(ListProjects=projects))
        with mock.patch.object(kymo_api.Api, "_connect", side_effect=AssertionError):
            with self.assertRaises(RpcError):
                api.projects()


class ChannelLifecycleTests(unittest.TestCase):
    def test_failed_local_start_leaves_init_free(self):
        # no channel opened, so falling back to hosted logging must not be refused by the fork guard
        api = offline_api(None, local=True)
        with (
            mock.patch.object(kymo_api, "_channel_opened", False),
            mock.patch(
                "kymo._local_runtime.ensure_local_endpoint",
                side_effect=RuntimeError("stack failed to start"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "failed to start"):
                api.projects()
            self.assertFalse(kymo_api._channel_opened)

    def test_concurrent_first_calls_connect_once(self):
        # racing first calls share one connect: unlocked, each would open its own channel (and in local mode run ensure_local_endpoint)
        connects = []
        started = threading.Barrier(8)

        def connect(api):
            connects.append(True)
            time.sleep(0.05)
            api._stub = NS(ListProjects=lambda req, timeout: NS(project_ids=["p"]))

        def call(_):
            started.wait()
            return api.projects()

        api = offline_api(None)
        with (
            mock.patch.object(kymo_api.Api, "_connect", connect),
            concurrent.futures.ThreadPoolExecutor(8) as pool,
        ):
            results = list(pool.map(call, range(8)))
        self.assertEqual((results, len(connects)), ([["p"]] * 8, 1))

    def test_hosted_channel_raises_the_receive_limit(self):
        with (
            mock.patch("grpc.insecure_channel") as channel,
            mock.patch.object(kymo_api, "_channel_opened", False),
        ):
            api = kymo_api.Api("h.example:1", mode="hosted")
            api._connect()
            api.close()
        self.assertEqual(
            channel.call_args.kwargs["options"],
            (("grpc.max_receive_message_length", 256 * 1024 * 1024),),
        )

    def test_close_in_a_forked_child_leaves_the_parent_alone(self):
        closed = []
        api = offline_api(NS())
        api._channels = [NS(close=lambda: closed.append(True))]
        with mock.patch("os.getpid", return_value=api._pid + 1):
            api.close()
        self.assertEqual((closed, api._http.is_closed), ([], False))
        api.close()
        self.assertEqual((closed, api._http.is_closed), ([True], True))

    def test_closed_or_forked_api_refuses_before_any_socket(self):
        # after close() the channel is closed (a call through it can crash the process), and a forked child shares its parent's sockets
        api = offline_api(NS(), local=True)
        with mock.patch.object(kymo_api.Api, "_connect", side_effect=AssertionError):
            with mock.patch("os.getpid", return_value=api._pid + 1):
                for call in (api.projects, lambda: api.fetch("k.png")):
                    with self.assertRaisesRegex(RuntimeError, "across fork"):
                        call()
            api.close()
            for call in (api.projects, lambda: api.fetch("k.png")):
                with self.assertRaisesRegex(RuntimeError, "after close"):
                    call()


@mock.patch.object(kymo_api, "_channel_opened", True)
class ForkGuardTests(unittest.TestCase):
    def guard(self, explicit, default):
        with (
            mock.patch.object(
                multiprocessing, "get_start_method", return_value=explicit
            ) as get,
            mock.patch.object(
                multiprocessing,
                "get_all_start_methods",
                return_value=[default, "spawn"],
            ),
        ):
            kymo_api._check_fork_safe()
        # allow_none=False would pin the process's start method as a side effect
        get.assert_called_once_with(allow_none=True)

    def test_fork_default_refuses(self):
        with self.assertRaisesRegex(RuntimeError, "forks its upload worker"):
            self.guard(None, "fork")

    def test_explicit_spawn_and_forkserver_default_pass(self):
        self.guard("spawn", "fork")
        self.guard(None, "forkserver")

    def test_init_refuses_before_any_side_effect(self):
        with (
            mock.patch.object(multiprocessing, "get_start_method", return_value="fork"),
            mock.patch(
                "kymo.client._run_control_rpc", side_effect=AssertionError("RPC ran")
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "forks its upload worker"):
                kymo.init(
                    server_address="h.example:1",
                    project_id="p",
                    run_name="n",
                    mode="hosted",
                )
        self.assertFalse(kymo.is_initialized())

    def test_unused_api_never_refuses(self):
        with mock.patch.object(kymo_api, "_channel_opened", False):
            kymo_api._check_fork_safe()


class ConstructionTests(unittest.TestCase):
    def test_hosted_needs_a_server_and_derives_the_cdn(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "kymo.Api: hosted mode needs"):
                kymo_api.Api()
            api = kymo_api.Api("h.example:50051")
            self.assertEqual(api._cdn, "http://h.example:8080")
            api.close()
            api = kymo_api.Api("h.example:50051", cdn_address="http://c.example:9/")
            self.assertEqual(api._cdn, "http://c.example:9")
            api.close()

    def test_cdn_client_follows_redirects_and_keeps_loopback_off_proxies(self):
        for api, trusts_env in (
            (offline_api(NS()), True),
            (offline_api(NS(), local=True), False),
        ):
            self.assertTrue(api._http.follow_redirects)
            self.assertEqual(api._http.trust_env, trusts_env)
            api.close()

    def test_construction_opens_no_channel(self):
        # a trainer may build its Api before kymo.init() and read after it
        with mock.patch.object(kymo_api, "_channel_opened", False):
            kymo_api.Api("h.example:50051", mode="hosted").close()
            self.assertFalse(kymo_api._channel_opened)

    def test_local_rejects_endpoint_overrides(self):
        with self.assertRaisesRegex(ValueError, "cannot override local"):
            kymo_api.Api("h.example:1", mode="local")
        with self.assertRaisesRegex(ValueError, "kymo.Api: mode must be"):
            kymo_api.Api(mode="remote")


if __name__ == "__main__":
    unittest.main()
