use dioxus::prelude::*;

use crate::components::icons::TrashIcon;
use crate::components::theme_toggle::ThemeToggle;
use crate::components::user_settings::{UserSettingsButton, UserSettingsPanel};
use crate::grpc::GrpcClient;
use crate::route::Route;
use crate::state::app_state::request_refresh;
use crate::util::{is_app_escape, local_storage, local_time, primary};

const SORT_KEY: &str = "kymo_projects_sort";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sort {
    Name,
    LastLogged,
}

/// Coarse on purpose: the row's tooltip has the exact time. Counted in clock minutes, the Last-logged sort's grid, so a label never contradicts the order.
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

#[component]
pub fn ProjectsPage() -> Element {
    // Generation counter bumped when the pushed global version moves or the
    // connection resynced — forces `projects` use_resource to re-run. Trash
    // lifecycle changes also move this shared generation; most leave the
    // projects list unchanged.
    let generation = use_signal(|| 0u64);

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
        }
    });

    let projects = use_resource(move || async move {
        let n = *generation.read(); // pre-await read: subscribes to re-runs
        if n == 0 {
            return std::future::pending().await;
        }
        crate::state::visibility::retry_visible("projects", async || {
            GrpcClient::new().list_projects().await
        })
        .await
    });

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
        main { class: "projects-page",
            div { class: "projects-page-inner",
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
                    Some(listing) if listing.project_ids.is_empty() => rsx! {
                        p { class: "text-disabled", "No projects found. Ingest some metrics to get started." }
                    },
                    Some(listing) => {
                        let query = filter.read().trim().to_owned();
                        let needle = query.to_lowercase();
                        let mut shown: Vec<(&String, Option<i64>)> = listing
                            .project_ids
                            .iter()
                            .filter(|pid| pid.to_lowercase().contains(&needle))
                            .map(|pid| (pid, listing.last_logged_at_ms.get(pid).copied()))
                            .collect();
                        // Newest clock minute first, stable over the server's name order: live runs flushing within one minute keep name order, and never-logged projects go last.
                        if *sort.read() == Sort::LastLogged {
                            shown.sort_by_key(|&(_, at)| std::cmp::Reverse(at.map(|at| at.div_euclid(60_000))));
                        }
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
                                                {last_logged_label(at, listing.server_now_ms)}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None => rsx! {
                        p { class: "text-disabled", "Loading..." }
                    },
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
    use super::last_logged_label;

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
