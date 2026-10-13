use std::collections::HashMap;

use dioxus::prelude::*;

use crate::components::icons::TrashIcon;
use crate::components::theme_toggle::ThemeToggle;
use crate::components::user_settings::{UserSettingsButton, UserSettingsPanel};
use crate::grpc::proto::ListProjectsResponse;
use crate::grpc::{merge_versions, GrpcClient};
use crate::route::Route;
use crate::state::app_state::request_refresh;
use crate::state::trash::{extrapolated_now_ms, monotonic_now_ms};
use crate::util::{is_app_escape, local_storage, local_time, primary, use_clock_tick};

const SORT_KEY: &str = "kymo_projects_sort";

/// How far the labels' clock trails the server's. A project logging through a clock minute rolls its value forward early in the minute, but the page gets it only after the ingest flush (2 s), the heartbeat (2 s), the socket's coalescing (1 s) and their statements; on the server's own clock every active project would read "1m ago" for those seconds of each minute. An idle project's label moves this much late, which a minute grid doesn't show.
const LABEL_LAG_MS: i64 = 10_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    Name,
    LastLogged,
}

/// Coarse on purpose: the row's tooltip has the exact time. Counted in clock minutes, the Last-logged sort's grid.
fn last_logged_label(at_ms: Option<i64>, now_ms: i64) -> String {
    let Some(at_ms) = at_ms else {
        return "—".to_string();
    };
    let minutes = now_ms.div_euclid(60_000) - at_ms.div_euclid(60_000);
    // A month is a twelfth of a year, so "12mo" never shows.
    for (size, unit) in [
        (525_600, "y"),
        (43_800, "mo"),
        (1_440, "d"),
        (60, "h"),
        (1, "m"),
    ] {
        if minutes >= size {
            return format!("{}{unit} ago", minutes / size);
        }
    }
    "just now".to_string()
}

/// ListProjects' rows with the heartbeat's pushed activity folded in.
#[derive(Default)]
struct Listing {
    loaded: bool,
    /// The server's order, by name.
    project_ids: Vec<String>,
    /// Max-merged from ListProjects and pushes, so a push racing a refetch is never lost; the server never lowers a value while it serves.
    last_logged_at_ms: HashMap<String, i64>,
    /// Row order under the Last-logged sort, set on page entry and by a click on its header only: activity and refetches never move a row, so a row the user may be about to click stays put. Projects listed later go last.
    by_last_logged: Vec<String>,
    server_now_ms: i64,
    observed_monotonic_ms: f64,
}

impl Listing {
    /// The labels' clock: the server's, extrapolated as on the Trash page, less [`LABEL_LAG_MS`].
    fn now_ms(&self) -> i64 {
        extrapolated_now_ms(self.server_now_ms, self.observed_monotonic_ms) - LABEL_LAG_MS
    }

    fn replace(&mut self, response: ListProjectsResponse) {
        merge_versions(&mut self.last_logged_at_ms, response.last_logged_at_ms);
        self.server_now_ms = response.server_now_ms;
        self.observed_monotonic_ms = monotonic_now_ms();
        self.project_ids = response.project_ids;
        if std::mem::replace(&mut self.loaded, true) {
            // A refetch keeps each row's place; new projects go last, in name order.
            let rank: HashMap<&String, usize> = self
                .by_last_logged
                .iter()
                .enumerate()
                .map(|(i, id)| (id, i))
                .collect();
            let mut order = self.project_ids.clone();
            order.sort_by_key(|id| rank.get(id).copied().unwrap_or(usize::MAX));
            self.by_last_logged = order;
        } else {
            self.sort_by_last_logged();
        }
    }

    /// Newest clock minute first, stable over the server's name order: live runs flushing within one minute keep name order, and never-logged projects go last.
    fn sort_by_last_logged(&mut self) {
        let at = &self.last_logged_at_ms;
        self.by_last_logged = self.project_ids.clone();
        self.by_last_logged
            .sort_by_key(|id| std::cmp::Reverse(at.get(id).map(|at| at.div_euclid(60_000))));
    }
}

#[component]
pub fn ProjectsPage() -> Element {
    // Generation counter bumped when the pushed global version moves or the
    // connection resynced — forces the ListProjects fetch to re-run. Trash
    // lifecycle changes also move this shared generation; most leave the
    // projects list unchanged.
    let generation = use_signal(|| 0u64);
    let mut projects = use_signal(Listing::default);

    use_future(move || async move {
        let mut subscription = crate::grpc::subscribe_push();
        loop {
            let update = subscription.next_visible().await;
            // The first seed (resync_gen >= 1 once the socket is up)
            // drives the INITIAL fetch: the resource below never settles
            // before generation moves, so a tab opened in the background
            // fetches nothing until it's first shown.
            if update.global.is_some() || update.resync_gen.is_some() {
                request_refresh(generation);
            }
            // The heartbeat's pushed activity patches rows in place: no query, and the page never polls.
            if !update.project_last_logged_at_ms.is_empty() {
                merge_versions(
                    &mut projects.write().last_logged_at_ms,
                    update.project_last_logged_at_ms,
                );
            }
        }
    });

    let _fetch = use_resource(move || async move {
        let n = *generation.read(); // pre-await read: subscribes to re-runs
        if n == 0 {
            return std::future::pending().await;
        }
        let response = crate::state::visibility::retry_visible("projects", async || {
            GrpcClient::new().list_projects().await
        })
        .await;
        projects.write().replace(response);
    });

    // Labels age by the labels' clock (Listing::now_ms), one render per minute of it, their grid. A pushed value restarts the sleep, which aims at the same minute again.
    use_clock_tick(move || (60_000 - projects.read().now_ms().rem_euclid(60_000)) as u64);

    let mut filter = use_signal(String::new);
    let settings_open = use_signal(|| false);
    let mut sort = use_signal(|| match local_storage::get(SORT_KEY).as_deref() {
        Some("name") => Sort::Name,
        _ => Sort::LastLogged,
    });
    let sort_button = move |label: &'static str, by: Sort| {
        rsx! {
            button {
                class: "project-sort",
                r#type: "button",
                aria_label: "Sort by {label}",
                aria_pressed: *sort.read() == by,
                onmousedown: primary(move |_| {
                    sort.set(by);
                    if by == Sort::LastLogged {
                        projects.write().sort_by_last_logged();
                    }
                    local_storage::set(SORT_KEY, if by == Sort::Name { "name" } else { "last_logged" });
                }),
                "{label}"
            }
        }
    };

    rsx! {
        // Restore the default title (document.title persists across SPA
        // navigation, so coming back from a dashboard would keep its name).
        document::Title { "kymo — Metrics Dashboard" }
        // The Settings panel docks beside the main landmark, not inside it.
        div { class: "projects-page",
            main { class: "projects-page-inner",
                div { class: "projects-title-row",
                    h1 { "kymo" }
                    div { class: "projects-page-actions",
                        Link {
                            to: Route::TrashPage {},
                            class: "trash-nav-link page-action",
                            title: "View runs in Trash",
                            aria_label: "View runs in Trash",
                            TrashIcon {}
                            span { "Trash" }
                        }
                        UserSettingsButton { open: settings_open }
                        ThemeToggle { class: "page-action" }
                    }
                }

                match &*projects.read() {
                    listing if !listing.loaded => rsx! {
                        p { class: "text-disabled", "Loading..." }
                    },
                    listing if listing.project_ids.is_empty() => rsx! {
                        p { class: "text-disabled", "No projects found. Ingest some metrics to get started." }
                    },
                    listing => {
                        let query = filter.read().trim().to_owned();
                        let needle = query.to_lowercase();
                        let now_ms = listing.now_ms();
                        let order = if *sort.read() == Sort::LastLogged {
                            &listing.by_last_logged
                        } else {
                            &listing.project_ids
                        };
                        let shown: Vec<(&String, Option<i64>)> = order
                            .iter()
                            .filter(|pid| pid.to_lowercase().contains(&needle))
                            .map(|pid| (pid, listing.last_logged_at_ms.get(pid).copied()))
                            .collect();
                        rsx! {
                            input {
                                class: "projects-filter",
                                r#type: "text",
                                placeholder: "Filter projects",
                                aria_label: "Filter projects",
                                value: "{filter}",
                                oninput: move |e: Event<FormData>| filter.set(e.value()),
                                onkeydown: move |e: Event<KeyboardData>| {
                                    if is_app_escape(&e) && !filter.peek().is_empty() {
                                        e.prevent_default();
                                        filter.set(String::new());
                                    }
                                },
                            }
                            if shown.is_empty() {
                                p { class: "text-disabled", "No projects match \"{query}\"." }
                            } else {
                                div { class: "project-list",
                                    div { class: "project-list-header",
                                        {sort_button("Project", Sort::Name)}
                                        {sort_button("Last logged", Sort::LastLogged)}
                                    }
                                    for (pid, at) in shown {
                                        Link {
                                            key: "{pid}",
                                            to: Route::ProjectPage { project_id: pid.clone(), chart: None.into() },
                                            class: "project-row",
                                            span { class: "project-row-name", "{pid}" }
                                            span {
                                                class: "project-row-logged",
                                                title: at.map(local_time),
                                                {last_logged_label(at, now_ms)}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if *settings_open.read() {
                UserSettingsPanel { open: settings_open }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{last_logged_label, Listing, LABEL_LAG_MS};
    use crate::grpc::proto::ListProjectsResponse;

    fn response(rows: &[(&str, Option<i64>)]) -> ListProjectsResponse {
        ListProjectsResponse {
            project_ids: rows.iter().map(|(id, _)| id.to_string()).collect(),
            last_logged_at_ms: rows
                .iter()
                .filter_map(|(id, at)| Some((id.to_string(), (*at)?)))
                .collect(),
            server_now_ms: 10 * 60_000,
        }
    }

    #[test]
    fn last_logged_order_moves_only_on_entry_and_header_click() {
        let mut listing = Listing::default();
        listing.replace(response(&[
            ("a", Some(60_000)),
            ("b", Some(5 * 60_000)),
            ("c", None),
        ]));
        assert_eq!(listing.by_last_logged, ["b", "a", "c"]);
        // A push patches values, never the order.
        crate::grpc::merge_versions(
            &mut listing.last_logged_at_ms,
            [("a".to_string(), 9 * 60_000)],
        );
        assert_eq!(listing.by_last_logged, ["b", "a", "c"]);
        // A refetch keeps listed rows in place, drops a delisted one, appends a new one, and never lowers a value.
        listing.replace(response(&[
            ("a", Some(2 * 60_000)),
            ("c", Some(9 * 60_000)),
            ("d", None),
            ("e", Some(1)),
        ]));
        assert_eq!(listing.by_last_logged, ["a", "c", "d", "e"]);
        assert_eq!(listing.last_logged_at_ms["a"], 9 * 60_000);
        // The header click re-sorts: newest clock minute first, then name order.
        listing.sort_by_last_logged();
        assert_eq!(listing.by_last_logged, ["a", "c", "e", "d"]);
    }

    #[test]
    fn labels_trail_the_server_clock_past_the_push_path() {
        let minute = 1_000 * 60_000;
        // A monotonic stamp past now pins the extrapolation to server_now_ms.
        let mut listing = Listing {
            server_now_ms: minute + 5_000,
            observed_monotonic_ms: f64::MAX,
            ..Default::default()
        };
        // Through the flush, heartbeat and coalescing seconds of a new minute, the previous minute's value reads "just now".
        assert_eq!(
            last_logged_label(Some(minute - 1_000), listing.now_ms()),
            "just now"
        );
        listing.server_now_ms = minute + LABEL_LAG_MS;
        assert_eq!(
            last_logged_label(Some(minute - 1_000), listing.now_ms()),
            "1m ago"
        );
    }

    #[test]
    fn last_logged_label_keeps_one_coarse_unit() {
        const DAY: i64 = 86_400_000;
        let now = 1_000 * DAY;
        assert_eq!(last_logged_label(None, now), "—");
        assert_eq!(last_logged_label(Some(now + 5_000), now), "just now");
        assert_eq!(last_logged_label(Some(now + 120_000), now), "just now");
        // Same clock minute, same label, whatever the elapsed seconds.
        assert_eq!(
            last_logged_label(Some(now + 10_000), now + 80_000),
            "1m ago"
        );
        assert_eq!(
            last_logged_label(Some(now + 50_000), now + 80_000),
            "1m ago"
        );
        assert_eq!(last_logged_label(Some(now - 59 * 60_000), now), "59m ago");
        assert_eq!(last_logged_label(Some(now - 3_600_000), now), "1h ago");
        assert_eq!(last_logged_label(Some(now - 29 * DAY), now), "29d ago");
        assert_eq!(last_logged_label(Some(now - 30 * DAY), now), "30d ago");
        assert_eq!(last_logged_label(Some(now - 45 * DAY), now), "1mo ago");
        assert_eq!(last_logged_label(Some(now - 364 * DAY), now), "11mo ago");
        assert_eq!(last_logged_label(Some(now - 400 * DAY), now), "1y ago");
    }
}
