use dioxus::prelude::*;

use crate::components::options_editor::{ChartOptionsForm, EditorSection, OPTIONS_ID_PREFIX};
use crate::components::options_panel::OptionsPanel;
use crate::state::layout_config::{RectOptions, MAX_SECTION_COLUMNS};
use crate::state::{DashboardState, SectionConfig};
use crate::util::use_live_apply;

/// The section settings this editor owns, as one draft.
#[derive(Clone, PartialEq)]
struct SectionDraft {
    display_name: String,
    max_columns: u32,
    rows_per_page: u32,
    chart_opts: RectOptions,
}

/// The `display_name` a typed name stores: trimmed, and empty when it matches the id (a rename to the id is no rename at all).
fn stored_display_name(typed: &str, name: &str) -> String {
    let display = typed.trim();
    if display == name {
        String::new()
    } else {
        display.to_string()
    }
}

/// Edits section settings and chart defaults live. Renames update `display_name`; `name` stays the identity.
/// Chart defaults start at the section's resolved options and are stored as a sparse patch over the project level (`OptionsBaseline::write_fields`).
/// Each change writes just the settings and chart-default fields it changed, Revert's restore included, so a collapse or chart-height drag beside the panel, or another tab's edit to anything else, survives.
#[component]
pub fn SectionEditor(return_focus_id: String, config: SectionConfig) -> Element {
    let state = use_context::<DashboardState>();
    // Resolved once: nothing else in this tab edits the project's or this section's defaults while the panel is open.
    let chart_baseline = use_hook(|| state.chart_defaults_baseline(Some(&config.name)));
    let initial = use_hook(|| SectionDraft {
        display_name: stored_display_name(&config.display_name, &config.name),
        max_columns: config.max_columns.clamp(1, MAX_SECTION_COLUMNS),
        rows_per_page: config.rows_per_page,
        chart_opts: chart_baseline.opened.clone(),
    });
    let mut draft_display_name = use_signal(|| initial.display_name.clone());
    let mut draft_max_columns = use_signal(|| initial.max_columns);
    let mut draft_rows_per_page = use_signal(|| initial.rows_per_page);
    let mut draft_chart_opts = use_signal(|| initial.chart_opts.clone());

    let name = config.name.clone();
    // Holds what would be stored, so a name typed back to what it was leaves nothing to revert.
    let current = {
        let name = name.clone();
        move || SectionDraft {
            display_name: stored_display_name(&draft_display_name.read(), &name),
            max_columns: *draft_max_columns.read(),
            rows_per_page: *draft_rows_per_page.read(),
            chart_opts: draft_chart_opts.read().clone(),
        }
    };
    let mut cleared = use_signal(Vec::new);
    let unchanged = current() == initial && cleared.read().is_empty();
    use_live_apply(current, {
        let chart_baseline = chart_baseline.clone();
        move |previous: &SectionDraft, draft: SectionDraft| {
            state.edit_section_settings(&name, |section| {
                if draft.display_name != previous.display_name {
                    section.display_name = draft.display_name;
                }
                if draft.max_columns != previous.max_columns {
                    section.max_columns = draft.max_columns;
                }
                if draft.rows_per_page != previous.rows_per_page {
                    section.rows_per_page = draft.rows_per_page;
                }
                chart_baseline.write_fields(
                    &mut section.chart_defaults,
                    &previous.chart_opts,
                    &draft.chart_opts,
                );
            });
        }
    });

    rsx! {
        OptionsPanel {
            title: "Configure section",
            // The catch-all section of unprefixed metrics has no name to show.
            target: Some(config.display_name()).filter(|name| !name.is_empty()).map(str::to_string),
            return_focus_ids: vec![return_focus_id],
            revert_disabled: unchanged,
            on_revert: {
                let initial = initial.clone();
                move |_| {
                    draft_display_name.set(initial.display_name.clone());
                    draft_max_columns.set(initial.max_columns);
                    draft_rows_per_page.set(initial.rows_per_page);
                    draft_chart_opts.set(initial.chart_opts.clone());
                    state.restore_overrides(&std::mem::take(&mut *cleared.write()));
                }
            },
            on_close: move |_| state.close_options_panel(),

            div { class: "editor-options",
                div { class: "binding-field",
                    label { r#for: "{OPTIONS_ID_PREFIX}-name", "Name" }
                    input {
                        id: "{OPTIONS_ID_PREFIX}-name",
                        r#type: "text",
                        value: "{draft_display_name}",
                        placeholder: "{config.name}",
                        oninput: move |e: Event<FormData>| {
                            draft_display_name.set(e.value());
                        },
                    }
                }

                div { class: "binding-field",
                    label { r#for: "{OPTIONS_ID_PREFIX}-columns", "Columns" }
                    input {
                        id: "{OPTIONS_ID_PREFIX}-columns",
                        r#type: "number",
                        min: "1",
                        max: "{MAX_SECTION_COLUMNS}",
                        value: "{draft_max_columns}",
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<u32>() {
                                draft_max_columns.set(v.clamp(1, MAX_SECTION_COLUMNS));
                            }
                        },
                    }
                }

                div { class: "binding-field",
                    label { r#for: "{OPTIONS_ID_PREFIX}-rows-per-page", "Rows / page" }
                    input {
                        id: "{OPTIONS_ID_PREFIX}-rows-per-page",
                        r#type: "number",
                        min: "0",
                        value: "{draft_rows_per_page}",
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<u32>() {
                                draft_rows_per_page.set(v);
                            }
                        },
                    }
                }

                EditorSection {
                    title: "Chart defaults",

                    ChartOptionsForm {
                        draft: draft_chart_opts,
                        anchor: chart_baseline.anchor.clone(),
                        section: config.name.clone(),
                        cleared,
                    }
                }
            }
        }
    }
}
