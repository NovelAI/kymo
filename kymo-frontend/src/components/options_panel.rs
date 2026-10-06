use dioxus::prelude::*;

use crate::components::binding_editor::BindingEditor;
use crate::components::icons::CloseIcon;
use crate::components::options_editor::ProjectDefaultsEditor;
use crate::components::section_editor::SectionEditor;
use crate::state::{DashboardState, PanelTarget};
use crate::util::{
    focus_later, focus_on_mount, is_app_escape, primary, MAXIMIZE_OVERLAY_ID, OPTIONS_PANEL_ID,
};

/// The panel's scrolling body, which takes the panel's focus so the keyboard scrolls it.
const OPTIONS_PANEL_BODY_ID: &str = "kymo-panel-body";

/// Docked shell for every options editor: a header (title, Revert, close) over a scrolling body, with a width handle on its left edge. Edits apply live, so closing keeps them; Revert puts back the values the panel opened with and stays open.
#[component]
pub fn OptionsPanel(
    title: String,
    /// The line under the title: what the panel edits, or where it saves.
    target: Option<String>,
    /// However the panel goes (closed, deleted, un-maximized, replaced), focus it takes along returns to the first of these that can take it: the control that opened the panel, then any stand-ins for it (a chart panel's opener can be a maximized chart's Configure that a ←/→ follow or the un-maximize took away).
    return_focus_ids: Vec<String>,
    /// Whether the panel takes focus as it mounts (see `OpenPanel::take_focus`).
    #[props(default = true)]
    take_focus: bool,
    revert_disabled: bool,
    on_revert: EventHandler<()>,
    on_close: EventHandler<()>,
    children: Element,
) -> Element {
    use_drop(move || {
        // A maximized chart makes the grid inert, so an opener there can't take focus; the overlay can.
        let ids: Vec<&str> = return_focus_ids
            .iter()
            .map(String::as_str)
            .chain([MAXIMIZE_OVERLAY_ID])
            .collect();
        focus_later(&ids, true);
    });
    let has_target = target.is_some();
    rsx! {
        aside {
            id: OPTIONS_PANEL_ID,
            class: "options-panel",
            aria_labelledby: "kymo-panel-title",
            aria_describedby: has_target.then_some("kymo-panel-target"),
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
                        focus_later(&[OPTIONS_PANEL_BODY_ID], false);
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
            div {
                id: OPTIONS_PANEL_BODY_ID,
                class: "options-panel-body",
                role: "group",
                aria_labelledby: "kymo-panel-title",
                tabindex: "-1",
                onmounted: move |e| {
                    if take_focus {
                        focus_on_mount(e);
                    }
                },
                {children}
            }
        }
    }
}

/// The dashboard's options panel: the editor for `DashboardState::options_panel`'s target, docked after the main column.
#[component]
pub fn DashboardOptionsPanel() -> Element {
    let state = use_context::<DashboardState>();

    // A target the layout no longer holds (deleted, here or in another tab, or its runs hidden) takes its panel with it. A chart panel waits for the layout to load (a chart link ahead of the sweep), and its chart stays maximized on its snapshot, unless the panel's own edit is what finds it gone (`DashboardState::edit_rect` closes the chart then).
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
            ProjectDefaultsEditor { return_focus_id }
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
                    }
                }
            }
        }
        PanelTarget::Chart => {
            let Some((rect, max_columns)) = state.maximized_rect() else {
                return rsx! {};
            };
            rsx! {
                for k in [rect.id.clone()] {
                    BindingEditor {
                        key: "{k}",
                        return_focus_id: return_focus_id.clone(),
                        take_focus: open.take_focus,
                        rect: rect.clone(),
                        max_columns,
                    }
                }
            }
        }
    }
}
