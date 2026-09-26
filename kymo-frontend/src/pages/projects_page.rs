use dioxus::prelude::*;

use crate::components::icons::TrashIcon;
use crate::components::theme_toggle::ThemeToggle;
use crate::components::user_settings::UserSettingsButton;
use crate::grpc::GrpcClient;
use crate::route::Route;

#[component]
pub fn ProjectsPage() -> Element {
    // Generation counter bumped when the pushed global version moves or the
    // connection resynced — forces `projects` use_resource to re-run. Trash
    // lifecycle changes also move this shared generation, so some refetches
    // are intentionally no-ops for the projects list.
    let mut generation = use_signal(|| 0u64);

    use_future(move || async move {
        let mut subscription = crate::grpc::subscribe_push();
        loop {
            let update = subscription.next_visible().await;
            // The first seed (resync_gen >= 1 once the socket is up)
            // drives the INITIAL fetch: the resource below never settles
            // before generation moves, so a tab opened in the background
            // fetches nothing until it's first shown.
            if update.initial || update.global.is_some() || update.resync_gen.is_some() {
                let g = *generation.peek();
                generation.set(g + 1);
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
                        UserSettingsButton {}
                        ThemeToggle { class: "page-action" }
                    }
                }

                match &*projects.read() {
                    Some(ids) if ids.is_empty() => rsx! {
                        p { class: "text-disabled", "No projects found. Ingest some metrics to get started." }
                    },
                    Some(ids) => {
                        let query = filter.read().trim().to_owned();
                        let needle = query.to_lowercase();
                        let shown: Vec<&String> =
                            ids.iter().filter(|pid| pid.to_lowercase().contains(&needle)).collect();
                        rsx! {
                            input {
                                class: "projects-filter",
                                r#type: "text",
                                placeholder: "Filter projects",
                                aria_label: "Filter projects",
                                value: "{filter}",
                                oninput: move |e: Event<FormData>| filter.set(e.value()),
                                onkeydown: move |e: Event<KeyboardData>| {
                                    if e.key() == Key::Escape && !filter.peek().is_empty() {
                                        filter.set(String::new());
                                    }
                                },
                            }
                            if shown.is_empty() {
                                p { class: "text-disabled", "No projects match \"{query}\"." }
                            }
                            div { class: "project-list",
                                for pid in shown {
                                    Link {
                                        to: Route::ProjectPage { project_id: pid.clone(), chart: None.into() },
                                        class: "project-card",
                                        div { class: "project-card-name", "{pid}" }
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
        }
    }
}
