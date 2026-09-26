use dioxus::prelude::*;

use crate::components::editor_dialog::EditorDialog;
use crate::components::icons::GearIcon;
use crate::state::{FontSize, UserConfig, UserConfigState};
use crate::util::{editor_trigger_id, primary};

#[component]
pub fn UserSettingsButton() -> Element {
    let mut editing = use_signal(|| false);
    let trigger_id = editor_trigger_id("user-settings", "global");

    rsx! {
        button {
            id: "{trigger_id}",
            class: "page-user-settings-trigger page-action",
            r#type: "button",
            title: "User settings",
            onmousedown: primary(move |_| editing.set(true)),
            GearIcon {}
            span { "Settings" }
        }
        if *editing.read() {
            UserSettingsDialog {
                return_focus_id: trigger_id.clone(),
                on_close: move |_| editing.set(false),
            }
        }
    }
}

type Toggle = fn(&mut UserConfig) -> &mut bool;

/// The dialog's checkboxes: element id, label, and the setting each one edits.
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

#[component]
fn UserSettingsDialog(return_focus_id: String, on_close: EventHandler<()>) -> Element {
    let state = use_context::<UserConfigState>();
    let initial = use_hook(|| state.current());
    let mut draft = use_signal(|| initial);
    let mut save_failed = use_signal(|| false);
    use_effect(move || draft.read().apply_to_document());
    // Preview is document-only. Any unmount reapplies canonical state, which
    // a successful Save updates synchronously before closing.
    use_drop(move || state.current().apply_to_document());
    let mut current = *draft.read();
    let selected = current.font_size;
    let is_dirty = current != initial;
    let show_save_error = *save_failed.read();

    rsx! {
        EditorDialog {
            return_focus_id,
            title: "Settings",
            panel_class: "user-settings-modal",
            save_disabled: !is_dirty,
            on_cancel: move |_| on_close.call(()),
            on_save: move |_| {
                let draft = *draft.peek();
                if state.commit(draft) {
                    on_close.call(());
                } else {
                    save_failed.set(true);
                }
            },

            p { class: "user-settings-scope", "Saved in this browser." }

            div { class: "user-settings-field",
                label { r#for: "kymo-user-font-size", "Font size" }
                div { class: "user-settings-font-control",
                    input {
                        id: "kymo-user-font-size",
                        r#type: "range",
                        min: "{FontSize::MIN}",
                        max: "{FontSize::MAX}",
                        step: "1",
                        value: "{selected.pixels()}",
                        aria_valuetext: "{selected.pixels()} pixels",
                        oninput: move |event: Event<FormData>| {
                            if let Some(font_size) = event.value().parse().ok().and_then(FontSize::new) {
                                save_failed.set(false);
                                draft.write().font_size = font_size;
                            }
                        }
                    }
                    output { "for": "kymo-user-font-size", "{selected.pixels()} px" }
                }
            }

            for (id, text, field) in TOGGLES {
                label { key: "{id}", class: "checkbox-label user-settings-toggle",
                    input {
                        id,
                        r#type: "checkbox",
                        checked: *field(&mut current),
                        onchange: move |event: Event<FormData>| {
                            save_failed.set(false);
                            *field(&mut draft.write()) = event.checked();
                        },
                    }
                    span { "{text}" }
                }
            }

            if show_save_error {
                p {
                    class: "user-settings-save-error",
                    role: "alert",
                    "Could not save settings in this browser. Check browser storage permissions and try again."
                }
            }
        }
    }
}
