use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;

use crate::util::{js_bridge::js_string, primary};

/// Native modal shell. Cancel must unmount it; Save may keep it open when persistence fails.
#[component]
pub fn EditorDialog(
    return_focus_id: String,
    title: String,
    #[props(default)] panel_class: String,
    on_cancel: EventHandler<()>,
    on_save: EventHandler<()>,
    #[props(default)] save_disabled: bool,
    children: Element,
) -> Element {
    let heading_id = format!("{return_focus_id}-dialog-title");
    let mut element = use_hook(|| CopyValue::new(None::<web_sys::HtmlDialogElement>));
    use_drop({
        let target_id = js_string(&return_focus_id);
        move || {
            // Native close restores too early when live edits move the opener.
            // Restore by ID after the DOM update, on the root because this
            // scope is ending. Leave any open dialog focused.
            let js = format!(
                "requestAnimationFrame(()=>{{if(!document.querySelector('dialog:modal'))document.getElementById({target_id})?.focus()}});"
            );
            dioxus::core::spawn_forever(async move {
                let _ = document::eval(&js).await;
            });
        }
    });

    rsx! {
        dialog {
            class: "editor-dialog",
            aria_labelledby: "{heading_id}",
            onmounted: move |event| {
                let dialog = event.as_web_event().unchecked_into::<web_sys::HtmlDialogElement>();
                element.set(Some(dialog.clone()));
                spawn(async move {
                    // Yield past the opening mousedown's default action;
                    // even a microtask is too early to preserve native focus.
                    gloo_timers::future::TimeoutFuture::new(0).await;
                    if dialog.is_connected() && !dialog.open() {
                        dialog.show_modal().expect("mounted editor dialog can be shown modally");
                    }
                });
            },
            oncancel: move |event| {
                // The owner reverts live edits before unmounting.
                event.prevent_default();
                on_cancel.call(());
            },
            onmousedown: primary(move |event| {
                let Some(dialog) = element.cloned() else { return; };
                let mouse = event.as_web_event();
                if mouse.target().as_ref() != Some(dialog.as_ref()) {
                    return;
                }
                let bounds = dialog.get_bounding_client_rect();
                let point = event.client_coordinates();
                // Backdrop events target the dialog; keep any future shell padding or border non-dismissive.
                if point.x < bounds.left() || point.x >= bounds.right()
                    || point.y < bounds.top() || point.y >= bounds.bottom()
                {
                    event.prevent_default();
                    on_cancel.call(());
                }
            }),
            div {
                class: "modal {panel_class}",
                // Initial Enter must not activate a Remove or Reset button.
                tabindex: "-1",
                autofocus: true,
                h3 { id: "{heading_id}", "{title}" }
                {children}
                div { class: "modal-actions",
                    button {
                        r#type: "button",
                        class: "btn btn-ghost",
                        onmousedown: primary(move |_| on_cancel.call(())),
                        "Cancel"
                    }
                    button {
                        r#type: "button",
                        class: "btn btn-primary",
                        disabled: save_disabled,
                        onmousedown: primary(move |_| on_save.call(())),
                        "Save"
                    }
                }
            }
        }
    }
}
