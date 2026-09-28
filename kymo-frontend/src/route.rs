use dioxus::prelude::*;

use crate::components::dashboard_layout::DashboardLayout;
use crate::pages::project_page::ProjectPage;
use crate::pages::projects_page::ProjectsPage;
use crate::pages::run_page::RunPage;
use crate::pages::settings_error::SettingsErrorPage;
use crate::pages::trash_page::TrashPage;

/// The dashboard's sole query parameter. Dioxus decodes a query before it
/// splits named arguments, so a full-query field is required for valid chart
/// ids containing `&` or a literal percent escape.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChartQuery(Option<String>);

impl ChartQuery {
    fn value(&self) -> Option<String> {
        self.0.clone()
    }

    fn is_some(&self) -> bool {
        self.0.is_some()
    }
}

impl From<Option<String>> for ChartQuery {
    fn from(value: Option<String>) -> Self {
        Self(value)
    }
}

impl From<&str> for ChartQuery {
    fn from(query: &str) -> Self {
        Self(query.strip_prefix("chart=").map(ToOwned::to_owned))
    }
}

impl std::fmt::Display for ChartQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use dioxus::prelude::dioxus_router::exports::percent_encoding::{
            utf8_percent_encode, AsciiSet, CONTROLS,
        };

        const CHART_VALUE: &AsciiSet = &CONTROLS.add(b'%').add(b'&').add(b'=');
        if let Some(value) = &self.0 {
            write!(f, "chart={}", utf8_percent_encode(value, CHART_VALUE))?;
        }
        Ok(())
    }
}

#[derive(Routable, Clone, PartialEq, Debug)]
pub enum Route {
    #[route("/")]
    ProjectsPage {},

    // `trash` is reserved at project admission, so this global route can stay
    // short without ambiguity.
    #[route("/trash")]
    TrashPage {},

    // Sits outside DashboardLayout so it can render even when DashboardLayout
    // would itself fail (corrupt saved layout for that project).
    #[route("/trash/settings-error/:project_id")]
    SettingsErrorPage { project_id: String },

    #[nest("/:project_id")]
    #[layout(DashboardLayout)]
    #[route("/?:..chart")]
    ProjectPage {
        project_id: String,
        chart: ChartQuery,
    },
    #[route("/:run_id?:..chart")]
    RunPage {
        project_id: String,
        run_id: String,
        chart: ChartQuery,
    },
    #[end_layout]
    #[end_nest]
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}

impl Route {
    /// The focused-chart `?chart=` param of the current page, if any.
    pub fn chart_param(&self) -> Option<String> {
        match self {
            Route::ProjectPage { chart, .. } | Route::RunPage { chart, .. } => chart.value(),
            _ => None,
        }
    }

    /// The same page with the `?chart=` param set or cleared; identity on routes that have no chart param.
    pub fn with_chart(&self, chart: Option<String>) -> Route {
        match self.clone() {
            Route::ProjectPage { project_id, .. } => Route::ProjectPage {
                project_id,
                chart: chart.into(),
            },
            Route::RunPage {
                project_id, run_id, ..
            } => Route::RunPage {
                project_id,
                run_id,
                chart: chart.into(),
            },
            other => other,
        }
    }
}

// The route the last focus push created. It proves "the current history entry is the one we pushed" only while the URL has continuously remained exactly this route — `note_route` clears it the moment anything else becomes current, because a *different* entry with the same route can only be reached by passing through some other route first (identical consecutive pushes are deduped by the router). While the proof holds, the entry below ours is necessarily the pre-open page (entries below an existing entry are immutable), so popping is safe.
thread_local! {
    static FOCUS_PUSH: std::cell::RefCell<Option<Route>> = const { std::cell::RefCell::new(None) };
}

/// Called by DashboardLayout on every render (it re-renders on each route change); any current route other than the recorded one invalidates the pop-on-dismiss record above.
pub fn note_route(route: &Route) {
    FOCUS_PUSH.with_borrow_mut(|rec| {
        if rec.as_ref().is_some_and(|r| r != route) {
            *rec = None;
        }
    });
}

/// Focus or unfocus a chart by rewriting the current URL's `?chart=` param — the maximize overlay's source of truth (synced by DashboardLayout) and a shareable link to the focused chart.
/// Focusing pushes, so Back dismisses the overlay. Dismissing pops the entry that push created when it's provably still the current one (see `FOCUS_PUSH`), so open→Esc leaves no duplicate entry for Back to eat; otherwise — direct chart link, or the URL moved under the overlay — it rewrites in place.
pub fn focus_chart(chart: Option<String>) {
    let current: Route = router().current();
    let target = current.with_chart(chart);
    if target == current {
        return;
    }
    if matches!(
        &target,
        Route::ProjectPage { chart, .. } | Route::RunPage { chart, .. } if chart.is_some()
    ) {
        navigator().push(target.clone());
        FOCUS_PUSH.set(Some(target));
    } else if FOCUS_PUSH.take().is_some_and(|route| route == current) {
        navigator().go_back();
    } else {
        navigator().replace(target);
    }
}

#[component]
fn NotFound(segments: Vec<String>) -> Element {
    rsx! {
        document::Title { "Page not found — kymo" }
        main { class: "not-found",
            div { class: "not-found-inner",
                h1 { "404" }
                p { "Page not found" }
                Link { to: Route::ProjectsPage {}, "Back to projects" }
            }
        }
    }
}

#[cfg(test)]
mod route_tests {
    use std::str::FromStr;

    use super::{ChartQuery, Route};

    #[test]
    fn trash_route_reserves_the_trash_project_id() {
        assert_eq!(Route::from_str("/trash").unwrap(), Route::TrashPage {});
        assert_eq!(
            Route::from_str("/a-project").unwrap(),
            Route::ProjectPage {
                project_id: "a-project".to_string(),
                chart: None.into(),
            }
        );
    }

    #[test]
    fn settings_error_project_keeps_its_run_routes() {
        assert_eq!(
            Route::from_str("/settings-error/a-run").unwrap(),
            Route::RunPage {
                project_id: "settings-error".to_string(),
                run_id: "a-run".to_string(),
                chart: None.into(),
            }
        );
        assert_eq!(
            Route::from_str("/trash/settings-error/a-project").unwrap(),
            Route::SettingsErrorPage {
                project_id: "a-project".to_string(),
            }
        );
    }

    // Fails if the patched router macro (see Cargo.toml) stops applying.
    #[test]
    fn unfocused_routes_have_no_bare_query_mark() {
        let project = Route::ProjectPage {
            project_id: "p".to_string(),
            chart: None.into(),
        };
        let run = Route::RunPage {
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            chart: None.into(),
        };
        assert_eq!(project.to_string(), "/p/");
        assert_eq!(run.to_string(), "/p/r");
        assert_eq!(
            Route::ProjectPage {
                project_id: "p".to_string(),
                chart: Some("x".to_string()).into(),
            }
            .to_string(),
            "/p/?chart=x"
        );
        // Older links end in a bare `?` and must open the same page.
        assert_eq!(Route::from_str("/p/?").unwrap(), project);
        assert_eq!(Route::from_str("/p/r?").unwrap(), run);
    }

    #[test]
    fn focused_chart_query_roundtrips_every_valid_identifier_shape() {
        for value in [
            "loss&aux",
            "rate%26raw",
            "a=b",
            "café",
            "space #?+tab\t",
            "",
        ] {
            for route in [
                Route::ProjectPage {
                    project_id: "project".to_string(),
                    chart: Some(value.to_string()).into(),
                },
                Route::RunPage {
                    project_id: "project".to_string(),
                    run_id: "run".to_string(),
                    chart: Some(value.to_string()).into(),
                },
            ] {
                let encoded = route.to_string();
                let decoded = Route::from_str(&encoded).unwrap();
                assert_eq!(decoded.chart_param().as_deref(), Some(value), "{encoded}");
            }
        }

        let none = Route::ProjectPage {
            project_id: "project".to_string(),
            chart: ChartQuery::default(),
        };
        assert_eq!(
            Route::from_str(&none.to_string()).unwrap().chart_param(),
            None
        );

        assert_eq!(
            Route::from_str("/project?chart=loss%26aux")
                .unwrap()
                .chart_param()
                .as_deref(),
            Some("loss&aux")
        );
        assert!(Route::ProjectPage {
            project_id: "project".to_string(),
            chart: Some("loss&aux".to_string()).into(),
        }
        .to_string()
        .contains("loss%26aux"));
        assert!(Route::RunPage {
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            chart: Some("rate%26raw".to_string()).into(),
        }
        .to_string()
        .contains("rate%2526raw"));
    }
}
