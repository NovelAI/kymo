use dioxus::prelude::*;

use crate::components::editor_dialog::EditorDialog;
use crate::components::options_editor::{ChartOptionsForm, EditorSection};
use crate::state::layout_config::{options_patch_between, RectOptions};
use crate::state::SectionConfig;
use crate::util::use_live_apply;

/// Edits section settings and chart defaults live. Cancel restores the initial
/// config; Save closes. Renames update `display_name`; `name` stays the identity.
/// Defaults start at `chart_current` and emit their diff from `chart_anchor`.
/// The payload retains original `rects`: use `update_section_settings`, not rect edits.
#[component]
pub fn SectionEditor(
    return_focus_id: String,
    config: SectionConfig,
    chart_anchor: RectOptions,
    chart_current: RectOptions,
    on_change: EventHandler<SectionConfig>,
    on_close: EventHandler<()>,
) -> Element {
    let id_prefix = format!("{return_focus_id}-dialog");
    // Live edits update `config`; build and restore from the open-time snapshot.
    let original = use_hook(|| config.clone());
    let mut draft_display_name = use_signal(|| config.display_name.clone());
    let mut draft_max_columns = use_signal(|| config.max_columns.max(1));
    let mut draft_rows_per_page = use_signal(|| config.rows_per_page);
    let draft_chart_opts = use_signal(|| chart_current.clone());
    let chart_open = use_signal(|| true);

    let updated_from_drafts = {
        let original = original.clone();
        let chart_anchor = chart_anchor.clone();
        move || {
            let mut updated = original.clone();
            let display = draft_display_name.read().trim().to_string();
            // A rename matching the id is no rename at all.
            updated.display_name = if display == updated.name {
                String::new()
            } else {
                display
            };
            updated.max_columns = (*draft_max_columns.read()).max(1);
            updated.rows_per_page = *draft_rows_per_page.read();
            updated.chart_defaults = options_patch_between(&chart_anchor, &draft_chart_opts.read());
            updated
        }
    };

    let cancel_live = use_live_apply(
        original.clone(),
        updated_from_drafts,
        move |updated| on_change.call(updated),
        move || on_close.call(()),
    );

    rsx! {
        EditorDialog {
            return_focus_id,
            title: "Configure section",
            on_cancel: move |_| cancel_live(),
            on_save: move |_| on_close.call(()),

            div { class: "editor-options",
                div { class: "binding-field",
                    label { r#for: "{id_prefix}-name", "Name" }
                    input {
                        id: "{id_prefix}-name",
                        r#type: "text",
                        value: "{draft_display_name}",
                        placeholder: "{config.name}",
                        oninput: move |e: Event<FormData>| {
                            draft_display_name.set(e.value());
                        },
                    }
                }

                div { class: "binding-field",
                    label { r#for: "{id_prefix}-columns", "Columns" }
                    input {
                        id: "{id_prefix}-columns",
                        r#type: "number",
                        min: "1",
                        max: "12",
                        value: "{draft_max_columns}",
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<u32>() {
                                draft_max_columns.set(v.clamp(1, 12));
                            }
                        },
                    }
                }

                div { class: "binding-field",
                    label { r#for: "{id_prefix}-rows-per-page", "Rows / page" }
                    input {
                        id: "{id_prefix}-rows-per-page",
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
                    open: chart_open,

                    ChartOptionsForm {
                        id_prefix: id_prefix.clone(),
                        draft: draft_chart_opts,
                        anchor: chart_anchor.clone(),
                        section: config.name.clone(),
                    }
                }
            }
        }
    }
}
