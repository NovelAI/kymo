use dioxus::prelude::*;

use crate::components::icons::GearIcon;
use crate::components::options_panel::OptionsPanel;
use crate::state::{FontSize, UserConfig, UserConfigState};
use crate::util::{focus_later, primary};

/// The Settings button's id, which focus returns to when the panel closes.
const TRIGGER_ID: &str = "kymo-user-settings-trigger";

#[component]
pub fn UserSettingsButton(mut open: Signal<bool>) -> Element {
    rsx! {
        button {
            id: TRIGGER_ID,
            class: "page-action",
            r#type: "button",
            title: "User settings",
            // Shown pressed while the panel is open, and closes it.
            aria_expanded: *open.read(),
            onmousedown: primary(move |_| {
                // The docked panel narrows the page under the pointer as it opens and closes.
                crate::util::panel_moved();
                open.toggle()
            }),
            GearIcon {}
            span { "Settings" }
        }
    }
}

type Toggle = fn(&mut UserConfig) -> &mut bool;

/// The panel's checkboxes: element id, label, and the setting each one edits.
const TOGGLES: [(&str, &str, Toggle); 4] = [
    (
        "kymo-single-click-unzoom",
        "Single-click to exit chart zoom",
        |c| &mut c.single_click_unzoom,
    ),
    (
        "kymo-highlight-same-name",
        "Highlight all runs with the same name",
        |c| &mut c.highlight_same_name,
    ),
    (
        "kymo-show-nearest-point",
        "Show each run’s nearest point when hovering a gap",
        |c| &mut c.show_nearest_point,
    ),
    (
        "kymo-sections-visible",
        "Chart sections visible by default (instead of collapsed)",
        |c| &mut c.sections_visible,
    ),
];

/// Browser-local settings, each change saved as it's made. A change the browser refuses to store doesn't apply.
#[component]
pub fn UserSettingsPanel(mut open: Signal<bool>) -> Element {
    let state = use_context::<UserConfigState>();
    let initial = use_hook(|| state.peek_config());
    let mut save_failed = use_signal(|| false);
    // A refused change must not stay on screen: the controls remount from the stored settings (a controlled input's DOM state survives a render whose value didn't change).
    let mut refusals = use_signal(|| 0u32);
    let mut save = move |next: UserConfig| {
        let saved = state.commit(next);
        save_failed.set(!saved);
        if !saved {
            *refusals.write() += 1;
            // The remount drops the control the user was on; take them back to it.
            if let Some(id) = web_sys::window()
                .and_then(|w| w.document())
                .and_then(|d| d.active_element())
                .map(|e| e.id())
                .filter(|id| !id.is_empty())
            {
                focus_later(&[&id], false);
            }
        }
    };
    let mut current = state.config();
    let font_size = current.font_size;

    rsx! {
        OptionsPanel {
            title: "Settings",
            target: "Saved in this browser.",
            return_focus_ids: vec![TRIGGER_ID.to_string()],
            revert_disabled: current == initial,
            on_revert: move |_| save(initial),
            on_close: move |_| {
                crate::util::panel_moved();
                open.set(false)
            },

            for r in [*refusals.read()] {
                div { key: "{r}",
                    div { class: "user-settings-field",
                        label { r#for: "kymo-user-font-size", "Font size" }
                        div { class: "user-settings-font-control",
                            input {
                                id: "kymo-user-font-size",
                                r#type: "range",
                                min: "{FontSize::MIN}",
                                max: "{FontSize::MAX}",
                                step: "1",
                                value: "{font_size.pixels()}",
                                aria_valuetext: "{font_size.pixels()} pixels",
                                oninput: move |event: Event<FormData>| {
                                    if let Some(font_size) = event.value().parse().ok().and_then(FontSize::new) {
                                        save(UserConfig { font_size, ..state.peek_config() });
                                    }
                                }
                            }
                            output { "for": "kymo-user-font-size", "{font_size.pixels()} px" }
                        }
                    }

                    for (id, text, field) in TOGGLES {
                        label { key: "{id}", class: "checkbox-label user-settings-toggle",
                            input {
                                id,
                                r#type: "checkbox",
                                checked: *field(&mut current),
                                onchange: move |event: Event<FormData>| {
                                    let mut next = state.peek_config();
                                    *field(&mut next) = event.checked();
                                    save(next);
                                },
                            }
                            span { "{text}" }
                        }
                    }
                }
            }

            if *save_failed.read() {
                p {
                    class: "user-settings-save-error",
                    role: "alert",
                    "Could not save settings in this browser. Check browser storage permissions and try again."
                }
            }
        }
    }
}
