use dioxus::prelude::*;
use dioxus::web::WebEventExt;

use crate::components::icons::{
    CaretLeftIcon, CaretRightIcon, GearIcon, GripIcon, PlusIcon, TrashIcon,
};
use crate::components::metric_rect::MetricRect;
use crate::components::section_drag::SectionDrag;
use crate::state::layout_config::{MetricBinding, ProjectRef, RectOptions, RunRef};
use crate::state::{
    DashboardState, DisplayType, PanelTarget, RectConfig, SectionConfig, UserConfigState,
};
use crate::util::{
    blur_target, confirm, editor_trigger_id, focus_later, focus_on_mount, is_app_escape, primary,
    refocus_if_dropped, reveal_renamed_section,
};

/// Greedy row-pack: each rect consumes its `column_span` units of the current
/// row; overflow wraps to the next row; `rows_per_page` rows = one page.
/// Returns the global rect indices grouped per page (always at least one page).
fn pack_into_pages(rects: &[RectConfig], max_columns: u32, rows_per_page: u32) -> Vec<Vec<usize>> {
    if rects.is_empty() {
        return vec![Vec::new()];
    }
    let max_columns = max_columns.max(1);
    let mut pages: Vec<Vec<usize>> = vec![Vec::new()];
    let mut units_in_row: u32 = 0;
    let mut rows_in_page: u32 = 0;

    for (i, rect) in rects.iter().enumerate() {
        let span = rect.options.column_span.clamp(1, max_columns);
        if units_in_row + span > max_columns {
            units_in_row = 0;
            rows_in_page += 1;
            if rows_per_page > 0 && rows_in_page >= rows_per_page {
                pages.push(Vec::new());
                rows_in_page = 0;
            }
        }
        pages.last_mut().unwrap().push(i);
        units_in_row += span;
    }
    pages
}

/// Parse the pager's one-based display value into the zero-based index used
/// by `current_page`. Numeric overshoots go to the nearest real page; an
/// empty or otherwise invalid value leaves the current page alone.
fn page_index_from_input(input: &str, total_pages: usize) -> Option<usize> {
    let page = input.trim().parse::<usize>().ok()?;
    Some(page.clamp(1, total_pages.max(1)) - 1)
}

#[component]
pub fn Section(
    config: SectionConfig,
    /// Search needle from `MetricGrid` (lowercased; empty = show all). Filters rects for display only.
    filter: String,
) -> Element {
    // Immutable id — keys rect ids; the label is what the user sees.
    let section_id = config.name.clone();
    let trigger_id = editor_trigger_id("section", &section_id);
    let body_id = format!("{trigger_id}-body");
    let add_trigger_id = editor_trigger_id("section-add", &section_id);
    let add_panel_id = editor_trigger_id("section-add-panel", &section_id);
    let section_label = config.display_name().to_string();
    let state = use_context::<DashboardState>();
    let mut panel_filter = state.panel_filter;
    let mut current_page = use_signal(|| 0usize);
    let naming = use_memo({
        let section_id = section_id.clone();
        move || state.naming_section.read().as_deref() == Some(section_id.as_str())
    });
    let drag = use_context::<SectionDrag>();
    let (insert_before, insert_after) = drag.marker_on(&section_id);
    let dragging = drag.is_source(&section_id);
    // Outlines the section the options panel edits; a memo, so opening the panel re-renders only that section.
    let options_target = use_memo({
        let section_id = section_id.clone();
        move || {
            state.options_panel.read().as_ref().is_some_and(
                |p| matches!(&p.target, PanelTarget::Section(name) if *name == section_id),
            )
        }
    });

    let display_rects: Vec<RectConfig> = config
        .rects
        .iter()
        .filter(|r| r.matches_filter(&filter))
        .cloned()
        .collect();
    let sections_visible = use_context::<UserConfigState>().sections_visible();
    let collapsed = config.is_collapsed(sections_visible);

    let pages = pack_into_pages(&display_rects, config.max_columns, config.rows_per_page);
    // Clamp the page signal so config changes (rows_per_page, max_columns,
    // rect deletions) can't leave us pointing past the end.
    let page = (*current_page.read()).min(pages.len().saturating_sub(1));
    let total_pages = pages.len();
    let show_pagination = config.rows_per_page > 0 && total_pages > 1;
    let visible_indices: Vec<usize> = pages[page].clone();
    let page_input_style = format!("width: calc({}ch + 6px);", total_pages.to_string().len());

    // Every panel's content sizes itself by the section's chart height (kymo.css), so a height change changes no panel props.
    let grid_style = format!(
        "grid-template-columns: repeat({}, 1fr); --kymo-chart-height: {}px;",
        config.max_columns.max(1),
        config.chart_height
    );

    // The header's Add metric and an empty section's Add panel.
    let add_panel = {
        let section_id = section_id.clone();
        let add_trigger_id = add_trigger_id.clone();
        move |e: Event<MouseData>| {
            e.stop_propagation();
            // As Configure does: the new chart's Close returns focus here, so the page follows this section if its completed name moved it.
            let naming_this = *naming.peek();
            if state.finish_section_name(true) && naming_this {
                reveal_renamed_section(&section_id);
            }
            // An active filter would hide the new blank rect; clear it first. Guarded — set on an already-empty filter still notifies subscribers.
            if !panel_filter.peek().is_empty() {
                panel_filter.set(String::new());
            }
            // Expand before adding so the new rect is visible.
            if collapsed {
                state.edit_section_settings(&section_id, |s| {
                    s.set_collapsed(false, sections_visible)
                });
            }
            // Generated id, not a position: the layout base regenerates
            // as metrics appear/disappear, so a length-based id could
            // collide with an earlier user-added rect in the saved diff.
            let id = crate::util::unique_id(&format!("{}-new", section_id));
            state.add_rect(
                &section_id,
                RectConfig {
                    id: id.clone(),
                    label: String::new(),
                    bindings: vec![MetricBinding {
                        project: ProjectRef::Current,
                        runs: RunRef::Selected,
                        metric_name: String::new(),
                    }],
                    display_type: DisplayType::Numeric,
                    options: RectOptions::default(),
                },
            );
            // The new rect appends to the end; overshoot the page
            // signal and let the render-time clamp resolve it to
            // whatever the last page becomes.
            current_page.set(usize::MAX);
            // Configure's flow: the new chart maximizes beside its editor, which opens on its first metric picker (`BindingEditor`); closing the editor returns to that last page.
            state.open_options_panel(PanelTarget::Chart, add_trigger_id.clone());
            crate::route::focus_chart(Some(id));
        }
    };

    rsx! {
        div {
            class: "section",
            "data-section-id": "{section_id}",
            "data-dragging": "{dragging}",
            "data-insert-before": "{insert_before}",
            "data-insert-after": "{insert_after}",
            "data-options-target": "{options_target}",
            // The whole bar toggles collapse; the action buttons inside stop
            // propagation so they don't double as a toggle.
            div {
                class: "section-header",
                onmousedown: primary({
                    let section_id = section_id.clone();
                    move |_| {
                        // While naming, a press on the bar only leaves the field, whose blur saves the name, and one in the field bubbles here: collapsing would hide the new section's Add panel.
                        if *naming.peek() {
                            return;
                        }
                        state.edit_section_settings(&section_id, |s| {
                            s.set_collapsed(!collapsed, sections_visible)
                        });
                    }
                }),
                span {
                    class: "section-drag-handle",
                    draggable: "true",
                    title: "Drag to reorder",
                    aria_hidden: "true",
                    // Pressing the handle must not toggle collapse.
                    onmousedown: move |e: Event<MouseData>| e.stop_propagation(),
                    ondragstart: {
                        let source = section_id.clone();
                        move |event: DragEvent| {
                            let ids = state
                                .layout_config
                                .peek()
                                .as_ref()
                                .map(|layout| layout.sections.iter().map(|s| s.name.clone()).collect())
                                .unwrap_or_default();
                            drag.start(&event, source.clone(), ids);
                        }
                    },
                    ondragend: move |_| drag.end(state),
                    GripIcon {}
                }
                // The name field takes the toggle's place while naming: a button can't hold an input.
                if naming() {
                    {
                        // Enter and Escape hand focus to the section's Add panel.
                        let add_panel_id = add_panel_id.clone();
                        let section_id = section_id.clone();
                        rsx! {
                            input {
                                class: "section-name-input",
                                aria_label: "Section name",
                                placeholder: "{section_label}",
                                onmounted: move |e: MountedEvent| {
                                    // Reveal the whole section by the least distance, so its Add panel shows too, not just the input at the viewport edge.
                                    if let Some(element) = e.data().try_as_web_event() {
                                        if let Ok(Some(section)) = element.closest(".section") {
                                            let options = web_sys::ScrollIntoViewOptions::new();
                                            options.set_block(web_sys::ScrollLogicalPosition::Nearest);
                                            section.scroll_into_view_with_scroll_into_view_options(&options);
                                        }
                                    }
                                    focus_on_mount(e);
                                },
                                // Uncontrolled: the typed name lives in the field and the untracked draft, so typing re-renders nothing.
                                oninput: move |e: Event<FormData>| {
                                    let mut draft = state.naming_draft;
                                    draft.set(e.value());
                                },
                                onkeydown: {
                                    let section_id = section_id.clone();
                                    move |e: Event<KeyboardData>| {
                                        if e.key() == Key::Enter && !e.is_composing() {
                                            e.prevent_default();
                                            if state.finish_section_name(true) {
                                                reveal_renamed_section(&section_id);
                                            }
                                            focus_later(&[&add_panel_id]);
                                        } else if is_app_escape(&e) {
                                            e.prevent_default();
                                            state.finish_section_name(false);
                                            focus_later(&[&add_panel_id]);
                                        }
                                    }
                                },
                                onblur: move |e: Event<FocusData>| {
                                    let reached = blur_target(&e);
                                    let stored = state.finish_section_name(true);
                                    match reached {
                                        // Another control took focus and keeps it: the page stays, unless that control moved with the section.
                                        Some(reached) => refocus_if_dropped(&reached),
                                        // A press in neutral space, or a blur with no target at all (switching app or tab): the page follows the section.
                                        None if stored => reveal_renamed_section(&section_id),
                                        None => {}
                                    }
                                },
                            }
                        }
                    }
                    span { class: "section-count", "{display_rects.len()}" }
                } else {
                    // The toggle's keyboard face, with no handler of its own: a press bubbles to the bar's mousedown, and Enter or Space on it reaches the same handler through KB_ACTIVATE_JS (main.rs).
                    button {
                        r#type: "button",
                        class: "section-toggle",
                        aria_expanded: !collapsed,
                        // A collapsed section renders no body, so there is nothing to point at then.
                        aria_controls: (!collapsed).then(|| body_id.clone()),
                        span { class: "section-name", "{section_label}" }
                        span { class: "section-count", "{display_rects.len()}" }
                    }
                }
                button {
                    id: "{add_trigger_id}",
                    class: "section-action icon-button",
                    title: "Add metric",
                    onmousedown: primary(add_panel.clone()),
                    PlusIcon {}
                }
                button {
                    id: "{trigger_id}",
                    class: "section-action icon-button",
                    title: "Configure",
                    aria_expanded: options_target(),
                    onmousedown: primary({
                        let section_id = section_id.clone();
                        move |e: Event<MouseData>| {
                            e.stop_propagation();
                            // Seed the settings editor from the completed inline name; the panel takes focus, so the page follows this section if the name moved it.
                            let naming_this = *naming.peek();
                            if state.finish_section_name(true) && naming_this {
                                reveal_renamed_section(&section_id);
                            }
                            state.open_options_panel(
                                PanelTarget::Section(section_id.clone()),
                                trigger_id.clone(),
                            );
                        }
                    }),
                    GearIcon {}
                }
                button {
                    class: "section-action delete-action icon-button",
                    title: "Delete section",
                    onmousedown: primary({
                        let section_label = section_label.clone();
                        let section_id = section_id.clone();
                        move |e| {
                            e.stop_propagation();
                            if confirm(&format!("Delete section \"{section_label}\"?")) {
                                state.delete_section(&section_id);
                            }
                        }
                    }),
                    TrashIcon {}
                }
                // Pagination only makes sense for content you can see.
                if show_pagination && !collapsed {
                    div {
                        class: "section-pagination",
                        onmousedown: move |e: Event<MouseData>| e.stop_propagation(),
                        // Step from the render-clamped `page`, not the raw
                        // signal — adds park the signal past the end and rely
                        // on the clamp, so raw reads would step from nowhere.
                        button {
                            class: "section-action icon-button",
                            title: "Previous page",
                            onmousedown: primary(move |_| {
                                if page > 0 {
                                    current_page.set(page - 1);
                                }
                            }),
                            CaretLeftIcon {}
                        }
                        span { class: "section-page-label",
                            input {
                                class: "section-page-input",
                                style: "{page_input_style}",
                                r#type: "number",
                                min: "1",
                                max: "{total_pages}",
                                value: "{page + 1}",
                                title: "Jump to page",
                                aria_label: "Jump to page, {total_pages} pages total",
                                // Commit on Enter, stepping, or blur. Dioxus
                                // rewrites this volatile value even when the
                                // signal is set to itself, so an empty/invalid
                                // edit reliably restores the committed page.
                                onchange: move |e: Event<FormData>| {
                                    match page_index_from_input(&e.value(), total_pages) {
                                        Some(target) => current_page.set(target),
                                        None => {
                                            let committed = *current_page.peek();
                                            current_page.set(committed);
                                        }
                                    }
                                },
                            }
                            span { aria_hidden: "true", "/{total_pages}" }
                        }
                        button {
                            class: "section-action icon-button",
                            title: "Next page",
                            onmousedown: primary(move |_| {
                                if page + 1 < total_pages {
                                    current_page.set(page + 1);
                                }
                            }),
                            CaretRightIcon {}
                        }
                    }
                }
            }

            if !collapsed {
                div { id: "{body_id}", class: "section-grid", style: "{grid_style}",
                    if config.rects.is_empty() {
                        div { class: "section-empty",
                            button {
                                id: "{add_panel_id}",
                                class: "btn btn-ghost",
                                onmousedown: primary(add_panel.clone()),
                                PlusIcon {}
                                "Add panel"
                            }
                        }
                    }
                    for i in visible_indices.iter().copied() {
                        {
                            let rect = display_rects[i].clone();
                            let span = rect.options.column_span.clamp(1, config.max_columns.max(1));
                            let span_style = format!("grid-column: span {};", span);
                            rsx! {
                                div {
                                    key: "{rect.id}",
                                    style: "{span_style}",
                                    MetricRect { config: rect.clone() }
                                }
                            }
                        }
                    }
                }
            }

        }
    }
}

#[cfg(test)]
mod tests {
    use super::page_index_from_input;

    #[test]
    fn page_input_is_one_based_and_clamped() {
        assert_eq!(page_index_from_input("1", 12), Some(0));
        assert_eq!(page_index_from_input(" 7 ", 12), Some(6));
        assert_eq!(page_index_from_input("12", 12), Some(11));
        assert_eq!(page_index_from_input("0", 12), Some(0));
        assert_eq!(page_index_from_input("99", 12), Some(11));
    }

    #[test]
    fn invalid_page_input_is_rejected() {
        assert_eq!(page_index_from_input("", 12), None);
        assert_eq!(page_index_from_input("page 3", 12), None);
        assert_eq!(page_index_from_input("-1", 12), None);
        assert_eq!(page_index_from_input(&format!("{}0", usize::MAX), 12), None);
    }
}
