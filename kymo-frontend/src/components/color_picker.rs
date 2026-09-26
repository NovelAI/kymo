use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsValue;

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
    let picker_id = format!("run-color-picker-{anchor_ordinal}");
    let dialog_label = format!("Color for {run_label}");

    rsx! {
        div {
            id: "{picker_id}",
            class: "color-picker",
            style: "position-anchor: --run-overflow-{anchor_ordinal};",
            popover: "auto",
            role: "dialog",
            aria_label: "{dialog_label}",
            tabindex: "-1",
            onmounted: {
                let picker_id = picker_id.clone();
                move |_| {
                    let picker_id = serde_json::to_string(&picker_id)
                        .unwrap_or_else(|_| "\"\"".to_string());
                    spawn(async move {
                        let _ = document::eval(&format!(
                            "const picker=document.getElementById({picker_id});\
                             picker?.showPopover();\
                             picker?.focus();"
                        ))
                        .await;
                    });
                }
            },
            ontoggle: move |e: Event<ToggleData>| {
                let event = e.data().as_web_event();
                let new_state = js_sys::Reflect::get(
                    event.as_ref(),
                    &JsValue::from_str("newState"),
                )
                .ok()
                .and_then(|value| value.as_string());
                if new_state.as_deref() == Some("closed") {
                    on_close.call(());
                }
            },

            div { class: "color-picker-title fade-overflow", title: "{run_label}",
                span { "{dialog_label}" }
            }

            div { class: "color-picker-palette",
                for color in PALETTE.iter() {
                    {
                        let c = (*color).to_string();
                        let c2 = c.clone();
                        let key = storage_key.clone();
                        let legacy_key = legacy_storage_key.clone();
                        let active = current_color == c;
                        rsx! {
                            button {
                                class: if active { "color-swatch color-swatch-active" } else { "color-swatch" },
                                style: "background: {c};",
                                title: "Use {c}",
                                aria_label: "Use color {c}",
                                aria_pressed: active,
                                onmousedown: primary(move |_| {
                                    local_storage::set_migrating(&key, &legacy_key, &c2);
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
