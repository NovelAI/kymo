use dioxus::prelude::*;
use dioxus::web::WebEventExt;

use crate::util::clipboard;

fn text_is_selected() -> bool {
    web_sys::window()
        .and_then(|window| window.get_selection().ok().flatten())
        .is_some_and(|selection| !selection.is_collapsed())
}

/// Chromium and WebKit keep a selection until after a click on its text dismisses it.
fn press_hits_selection(x: f64, y: f64) -> bool {
    let Some(selection) =
        web_sys::window().and_then(|window| window.get_selection().ok().flatten())
    else {
        return false;
    };
    !selection.is_collapsed()
        && (0..selection.range_count())
            .filter_map(|i| selection.get_range_at(i).ok())
            .filter_map(|range| range.get_client_rects())
            .any(|rects| {
                (0..rects.length()).filter_map(|i| rects.item(i)).any(|r| {
                    (r.left()..=r.right()).contains(&x) && (r.top()..=r.bottom()).contains(&y)
                })
            })
}

#[component]
pub(crate) fn CopyText(text: String, class: &'static str, display: Option<String>) -> Element {
    let display = display.as_deref().unwrap_or(&text);
    let mut pressed_on_selection = use_signal(|| false);
    let keyboard_text = text.clone();
    rsx! {
        span {
            role: "button",
            tabindex: 0,
            class: "copy-text {class}",
            title: "{display}",
            aria_label: display.trim().is_empty().then_some("Copy empty text"),
            onmousedown: move |event: Event<MouseData>| {
                // Shift-clicks and multi-clicks adjust the selection rather than dismiss it.
                let plain = !event.modifiers().contains(Modifiers::SHIFT)
                    && event.data().try_as_web_event().is_some_and(|web_event| web_event.detail() == 1);
                let point = event.client_coordinates();
                pressed_on_selection.set(plain && press_hits_selection(point.x, point.y));
            },
            // Release activation lets text-selection gestures finish without replacing the clipboard; a click that dismisses a selection still copies.
            onclick: move |_| {
                // Dismiss first so the copy fallback cannot restore the selection.
                if pressed_on_selection.take() {
                    if let Some(selection) = web_sys::window().and_then(|window| window.get_selection().ok().flatten()) {
                        let _ = selection.remove_all_ranges();
                    }
                }
                if !text_is_selected() {
                    clipboard::write_text(&text);
                }
            },
            onkeydown: move |event: Event<KeyboardData>| {
                if event.key() == Key::Enter || event.key() == Key::Character(" ".to_owned()) {
                    event.prevent_default();
                    event.stop_propagation();
                    if !event.is_auto_repeating() {
                        clipboard::write_text(&keyboard_text);
                    }
                }
            },
            span { class: "fade-overflow", span { "{display}" } }
        }
    }
}
