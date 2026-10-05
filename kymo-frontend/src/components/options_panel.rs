use std::cell::Cell;

use dioxus::prelude::*;

use crate::components::binding_editor::BindingEditor;
use crate::components::icons::CloseIcon;
use crate::components::metric_rect::rect_title;
use crate::components::options_editor::ProjectDefaultsEditor;
use crate::components::section_editor::SectionEditor;
use crate::state::layout_config::MAX_SECTION_COLUMNS;
use crate::state::{DashboardState, PanelTarget};
use crate::util::{
    editor_trigger_id, focus_is_within, focus_later, focus_on_mount, is_app_escape, primary,
    MAXIMIZE_OVERLAY_ID, OPTIONS_PANEL_ID,
};

thread_local! {
    /// Whether the panel that dropped last held focus, for the one replacing it.
    static HELD_FOCUS: Cell<bool> = const { Cell::new(false) };
}

/// Docked shell for every options editor: a header (title, Revert, close) over a scrolling body, with a width handle on its left edge. Edits apply live, so closing keeps them; Revert puts back the values the panel opened with and stays open.
#[component]
pub fn OptionsPanel(
    title: String,
    /// What the panel edits, when the title alone doesn't say.
    target: Option<String>,
    /// The control that opened the panel. However the panel goes (closed, deleted, un-maximized, replaced), focus it takes along returns here.
    return_focus_id: String,
    /// Where that focus goes when the opener can't take it: a maximized chart's Configure, gone with the panel, falls back to its grid copy's.
    fallback_focus_id: Option<String>,
    /// Whether the panel takes focus as it mounts, as an opening panel does; one that remounts to follow the maximized chart (←/→) takes it only from the panel it replaced.
    #[props(default = true)]
    take_focus: bool,
    revert_disabled: bool,
    on_revert: EventHandler<()>,
    on_close: EventHandler<()>,
    children: Element,
) -> Element {
    use_drop(move || {
        // Components drop before the DOM changes, so the panel is still there to ask.
        HELD_FOCUS.set(focus_is_within(OPTIONS_PANEL_ID));
        let fallback = fallback_focus_id.as_deref().unwrap_or(&return_focus_id);
        // A maximized chart makes the grid inert, so an opener there can't take focus; the overlay can.
        focus_later(&[&return_focus_id, fallback, MAXIMIZE_OVERLAY_ID], true);
    });
    let has_target = target.is_some();
    rsx! {
        aside {
            id: OPTIONS_PANEL_ID,
            class: "options-panel",
            aria_labelledby: "kymo-panel-title",
            aria_describedby: has_target.then_some("kymo-panel-target"),
            tabindex: "-1",
            onmounted: move |e| {
                let held = HELD_FOCUS.take();
                if take_focus || held {
                    focus_on_mount(e);
                }
            },
            onkeydown: move |e: Event<KeyboardData>| {
                // A control inside may have used this Esc already (a filter clearing itself).
                if is_app_escape(&e) && e.default_action_enabled() {
                    e.prevent_default();
                    on_close.call(());
                }
            },
            // Dragged by util/width_drag.js.
            div { class: "options-panel-resize" }
            div { class: "options-panel-header",
                div { class: "options-panel-heading",
                    h3 { id: "kymo-panel-title", "{title}" }
                    if let Some(target) = target {
                        div { id: "kymo-panel-target", class: "options-panel-target fade-overflow", title: "{target}",
                            span { "{target}" }
                        }
                    }
                }
                button {
                    r#type: "button",
                    class: "btn btn-ghost",
                    disabled: revert_disabled,
                    title: "Put back the values this panel opened with",
                    onmousedown: primary(move |_| {
                        on_revert.call(());
                        // Revert disables itself, which drops keyboard focus on the page (Chromium) or strands it on the disabled button (WebKit).
                        focus_later(&[OPTIONS_PANEL_ID], false);
                    }),
                    "Revert"
                }
                button {
                    r#type: "button",
                    class: "options-panel-close icon-button",
                    title: "Close",
                    onmousedown: primary(move |_| on_close.call(())),
                    CloseIcon {}
                }
            }
            div { class: "options-panel-body", {children} }
        }
    }
}

/// The dashboard's options panel: the editor for `DashboardState::options_panel`'s target, docked after the main column.
#[component]
pub fn DashboardOptionsPanel() -> Element {
    let state = use_context::<DashboardState>();
    let close = move |_| state.close_options_panel();

    // A target that leaves the layout takes its panel with it: deleted here or in another tab (seen at this tab's next edit), or gone from a regenerated base (its runs hidden). A chart panel waits for the layout to load (a chart link ahead of the sweep), and its chart stays maximized on its snapshot, as it does with no panel open.
    use_effect(move || {
        let Some(target) = state
            .options_panel
            .read()
            .as_ref()
            .map(|p| p.target.clone())
        else {
            return;
        };
        let layout = state.layout_config.read();
        let gone = match target {
            PanelTarget::ProjectDefaults => false,
            PanelTarget::Section(name) => layout
                .as_ref()
                .is_none_or(|l| l.find_section(&name).is_none()),
            PanelTarget::Chart => match (layout.as_ref(), state.maximized.read().as_ref()) {
                (Some(l), Some(chart)) => l.find_rect(&chart.id).is_none(),
                _ => false,
            },
        };
        if gone {
            let mut panel = state.options_panel;
            panel.set(None);
        }
    });

    let Some(open) = state.options_panel.read().clone() else {
        return rsx! {};
    };
    let return_focus_id = open.return_focus_id.clone();
    // Each target mounts its own editor, seeding its drafts: the editor kind changes with the arm, and the keyed arms remount when the section or chart does.
    match open.target {
        PanelTarget::ProjectDefaults => rsx! {
            ProjectDefaultsEditor { return_focus_id, on_close: close }
        },
        PanelTarget::Section(name) => {
            let section = state
                .layout_config
                .read()
                .as_ref()
                .and_then(|l| l.find_section(&name).cloned());
            let Some(section) = section else {
                return rsx! {};
            };
            rsx! {
                for k in [name] {
                    SectionEditor {
                        key: "{k}",
                        return_focus_id: return_focus_id.clone(),
                        config: section.clone(),
                        on_close: close,
                    }
                }
            }
        }
        PanelTarget::Chart => {
            let Some(view) = state.maximized.read().clone() else {
                return rsx! {};
            };
            // The live rect and its section's columns, like the overlay's. Before the layout holds the rect (a chart link ahead of the sweep), the snapshot shows with the most columns a section can have; an edit then closes the chart anyway.
            let (rect, max_columns) = state
                .layout_config
                .read()
                .as_ref()
                .and_then(|l| l.resolve_rect(&view.id))
                .unwrap_or((view, MAX_SECTION_COLUMNS));
            let (cdn_class, display_type) = state.chart_facts(&rect);
            let display_type = display_type.unwrap_or(rect.display_type);
            rsx! {
                for k in [rect.id.clone()] {
                    BindingEditor {
                        key: "{k}",
                        return_focus_id: return_focus_id.clone(),
                        fallback_focus_id: editor_trigger_id("rect-grid", &rect.id),
                        take_focus: !open.follows_chart,
                        rect_id: rect.id.clone(),
                        target: rect_title(&rect),
                        bindings: rect.bindings.clone(),
                        display_type,
                        cdn_class: cdn_class.clone(),
                        max_columns,
                        on_close: close,
                    }
                }
            }
        }
    }
}
