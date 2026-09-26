use dioxus::prelude::*;

use crate::route::Route;
use crate::state::LayoutDiff;
use crate::util::primary;

/// Reload retries the saved layout with the current bundle. Reset is a
/// separate destructive fallback for a layout that cannot be recovered.
#[component]
pub fn SettingsErrorPage(project_id: String) -> Element {
    let pid_for_reset = project_id.clone();
    let reload_route = Route::ProjectPage {
        project_id: project_id.clone(),
        chart: None.into(),
    }
    .to_string();
    let pid_display = project_id.clone();

    rsx! {
        document::Title { "Saved layout error — kymo" }
        main { class: "settings-error",
            div { class: "settings-error-inner",
                h1 { "Saved layout could not be loaded" }
                p {
                    "Your saved layout for project "
                    span { class: "settings-error-pid", "{pid_display}" }
                    " uses a format this build does not support. This can happen \
                     after upgrading or rolling back across a layout-format change."
                }
                p {
                    "Reload this tab to get the current build and retry your saved layout. "
                    "Reset to discard the saved layout and regenerate a fresh \
                     one from your current metrics. Other settings (color \
                     overrides, theme, shown runs) are unaffected."
                }
                div { class: "settings-error-actions",
                    button {
                        class: "btn btn-primary",
                        onmousedown: primary(move |_| {
                            if let Some(window) = web_sys::window() {
                                // A full navigation fetches the current bundle;
                                // router navigation would reuse this one.
                                let _ = window.location().replace(&reload_route);
                            }
                        }),
                        "Reload this tab"
                    }
                    button {
                        class: "btn btn-ghost",
                        onmousedown: primary(move |_| {
                            LayoutDiff::clear(&pid_for_reset);
                            // `replace`, like the redirect that lands here:
                            // Back must not return to a stale error page
                            // after the reset cleared the corrupt state.
                            navigator().replace(Route::ProjectPage {
                                project_id: pid_for_reset.clone(),
                                chart: None.into(),
                            });
                        }),
                        "Reset layout"
                    }
                    Link {
                        to: Route::ProjectsPage {},
                        class: "btn btn-ghost",
                        "Back to projects"
                    }
                }
            }
        }
    }
}
