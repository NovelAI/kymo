use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;

use crate::components::uplot_chart::PALETTE;
use crate::util::{local_storage, primary};

#[component]
pub fn ColorPicker(
    run_id: String,
    run_label: String,
    current_color: String,
    anchor_ordinal: u64,
    on_close: EventHandler<()>,
) -> Element {
    let storage_key = format!("kymo_color_{}", run_id);
    let legacy_storage_key = format!("mkdb2_color_{}", run_id);
    let dialog_label = format!("Color for {run_label}");

    rsx! {
        div {
            class: "color-picker",
            style: "position-anchor: --run-overflow-{anchor_ordinal};",
            popover: "auto",
            role: "dialog",
            aria_label: "{dialog_label}",
            tabindex: "-1",
            onmounted: move |e| {
                let picker = e.as_web_event().unchecked_into::<web_sys::HtmlElement>();
                let _ = picker.show_popover();
                let _ = picker.focus();
            },
            ontoggle: move |e: Event<ToggleData>| {
                let event = e.as_web_event().unchecked_into::<web_sys::ToggleEvent>();
                if event.new_state() == "closed" {
                    on_close.call(());
                }
            },

            div { class: "color-picker-title fade-overflow", title: "{run_label}",
                span { "{dialog_label}" }
            }

            div { class: "color-picker-palette",
                for color in PALETTE.iter().copied() {
                    {
                        let key = storage_key.clone();
                        let legacy_key = legacy_storage_key.clone();
                        let active = current_color == color;
                        rsx! {
                            button {
                                class: if active { "color-swatch color-swatch-active" } else { "color-swatch" },
                                style: "background: {color};",
                                title: "Use {color}",
                                aria_label: "Use color {color}",
                                aria_pressed: active,
                                onmousedown: primary(move |_| {
                                    local_storage::set_migrating(&key, &legacy_key, color);
                                    on_close.call(());
                                }),
                            }
                        }
                    }
                }
            }

            div { class: "color-picker-custom",
                label { class: "color-picker-label", "Custom" }
                input {
                    r#type: "color",
                    value: "{current_color}",
                    aria_label: "Custom color for {run_label}",
                    onchange: {
                        let key = storage_key.clone();
                        let legacy_key = legacy_storage_key.clone();
                        move |e: Event<FormData>| {
                            local_storage::set_migrating(&key, &legacy_key, &e.value());
                            on_close.call(());
                        }
                    },
                }
                button {
                    class: "btn-link",
                    onmousedown: primary({
                        let key = storage_key.clone();
                        let legacy_key = legacy_storage_key.clone();
                        move |_| {
                            local_storage::remove_migrating(&key, &legacy_key);
                            on_close.call(());
                        }
                    }),
                    "Reset"
                }
            }
        }
    }
}
