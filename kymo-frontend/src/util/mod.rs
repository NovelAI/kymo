pub(crate) mod clipboard;
pub(crate) mod js_bridge;
mod live_apply;
pub mod local_storage;
pub(crate) mod resize_observer;
pub mod sections;

pub use live_apply::use_live_apply;

use std::cmp::Ordering;
use std::fmt::Write as _;

use dioxus::html::input_data::MouseButton;
use dioxus::prelude::{
    spawn, Event, Key, KeyboardData, Modifiers, ModifiersInteraction, MountedEvent, MouseData,
    PointerInteraction,
};

pub(crate) const TOP_LAYER_SELECTOR: &str = ":popover-open, dialog:modal";

/// Format epoch milliseconds in the viewer's local timezone.
pub fn local_time(ms: i64) -> String {
    js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(ms as f64))
        .to_locale_string("en-US", &wasm_bindgen::JsValue::UNDEFINED)
        .as_string()
        .unwrap_or_else(|| ms.to_string())
}

/// Esc that an in-app layer may consume, marking it handled with `prevent_default`: Dioxus applies that to the native event, where the page-level Esc layers check it, while `stop_propagation` never leaves Dioxus. Esc cancelling an IME composition, or one that an open native dialog or popover closes on after keydown, is left alone; `prevent_default` would cancel that close.
pub(crate) fn is_app_escape(e: &Event<KeyboardData>) -> bool {
    e.key() == Key::Escape
        && !e.is_composing()
        && web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.query_selector(TOP_LAYER_SELECTOR).ok().flatten())
            .is_none()
}

/// Compare two strings "naturally": maximal runs of ASCII digits compare by
/// numeric value, everything else byte-for-byte (which, for UTF-8, equals
/// Unicode scalar order). So `block_2` sorts before `block_10`, where plain
/// lexical order puts `block_10` first. A digit run is ranked by length then
/// bytes, so for un-padded numbers this is exactly numeric value. Leading
/// zeros are not normalized: differently-padded spellings of one value (`007`
/// vs `10`) sort by width, not magnitude — acceptable because mixing padding
/// widths within a namespace is a naming error, not ours to paper over. The
/// order stays total, so it's a drop-in `cmp` for sort keys.
///
/// Only *unsigned integer* runs are read numerically: `-` and `.` are ordinary
/// separator bytes, not a sign or decimal point. So an embedded negative sorts
/// by magnitude (`x-2` < `x-10`) and a decimal's fraction compares as a second
/// integer (`t1.5` < `t1.10`). That's the conventional natural-sort rule and is
/// fine for the metric/section *names* this orders; it is not a numeric value
/// comparator.
pub fn natural_cmp(a: &str, b: &str) -> Ordering {
    let (mut a, mut b) = (a.as_bytes(), b.as_bytes());
    loop {
        match (a.first(), b.first()) {
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some(x), Some(y)) if x.is_ascii_digit() && y.is_ascii_digit() => {
                let (ra, a_rest) = split_digits(a);
                let (rb, b_rest) = split_digits(b);
                match cmp_digit_runs(ra, rb) {
                    Ordering::Equal => {
                        a = a_rest;
                        b = b_rest;
                    }
                    other => return other,
                }
            }
            (Some(x), Some(y)) => match x.cmp(y) {
                Ordering::Equal => {
                    a = &a[1..];
                    b = &b[1..];
                }
                other => return other,
            },
        }
    }
}

/// Split off the maximal leading run of ASCII digits, returning `(run, rest)`.
fn split_digits(s: &[u8]) -> (&[u8], &[u8]) {
    let end = s
        .iter()
        .position(|c| !c.is_ascii_digit())
        .unwrap_or(s.len());
    s.split_at(end)
}

/// Order two ASCII-digit runs: longer runs sort later, equal lengths compare
/// lexically. For numbers without leading zeros that is exactly numeric order,
/// and comparing structurally (never parsing into a fixed-width integer) means
/// arbitrarily long runs can't overflow. Leading zeros are not normalized, so
/// padding width participates in the order — see `natural_cmp`.
fn cmp_digit_runs(a: &[u8], b: &[u8]) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// Collision-safe id for user-created layout elements (sections, rects).
/// Wall-clock milliseconds alone collide on a fast double-click when the
/// browser coarsens timers (e.g. Firefox resistFingerprinting rounds
/// Date.now() to as much as 100ms) — and `LayoutDiff` silently merges
/// duplicate ids — so mix in a random suffix.
pub fn unique_id(prefix: &str) -> String {
    let ms = js_sys::Date::now() as u64;
    let rand = (js_sys::Math::random() * f64::from(u32::MAX)) as u32;
    format!("{prefix}-{ms}-{rand:08x}")
}

/// Browser-console warning.
pub fn warn(msg: &str) {
    web_sys::console::warn_1(&wasm_bindgen::JsValue::from_str(msg));
}

/// Stable, collision-free DOM identity for the control that opens an editor.
/// Hex keeps arbitrary layout IDs out of HTML's whitespace-sensitive `id`
/// syntax; `kind` distinguishes a grid chart from its maximized copy.
pub fn editor_trigger_id(kind: &str, identity: &str) -> String {
    let mut id = format!("kymo-editor-{kind}-");
    for byte in identity.as_bytes() {
        write!(id, "{byte:02x}").expect("writing to a String cannot fail");
    }
    id
}

/// `onmounted` handler that focuses its element at once, so keys typed right after the mount land in it (Chromium runs queued input before timers), and again one task later, because an element mounted by a mousedown handler loses focus to that press's default focus action.
pub fn focus_on_mount(e: MountedEvent) {
    spawn(async move {
        let _ = e.data().set_focus(true).await;
        gloo_timers::future::TimeoutFuture::new(0).await;
        let _ = e.data().set_focus(true).await;
    });
}

/// Instant actions run on primary press (AI-1418), excluding right/middle buttons and macOS Control-click context menus.
pub fn primary<F>(mut f: F) -> impl FnMut(Event<MouseData>) + Clone + 'static
where
    F: FnMut(Event<MouseData>) + Clone + 'static,
{
    move |e| {
        if e.trigger_button() != Some(MouseButton::Primary) {
            return;
        }
        if e.modifiers().contains(Modifiers::CONTROL)
            && web_sys::window()
                .and_then(|window| window.navigator().platform().ok())
                .is_some_and(|platform| platform.starts_with("Mac"))
        {
            return;
        }
        f(e);
    }
}

#[cfg(test)]
mod natural_cmp_tests {
    use super::*;

    fn sorted(mut v: Vec<&str>) -> Vec<&str> {
        v.sort_by(|a, b| natural_cmp(a, b));
        v
    }

    #[test]
    fn digits_compare_numerically_not_lexically() {
        assert_eq!(
            sorted(vec!["block_10", "block_2", "block_1"]),
            vec!["block_1", "block_2", "block_10"]
        );
    }

    #[test]
    fn handles_multiple_numeric_fields_and_text_tails() {
        assert_eq!(
            sorted(vec![
                "loss/layer10/head2",
                "loss/layer2/head10",
                "loss/layer2/head2"
            ]),
            vec![
                "loss/layer2/head2",
                "loss/layer2/head10",
                "loss/layer10/head2"
            ]
        );
    }

    #[test]
    fn leading_zeros_order_by_width_not_value() {
        // Digit runs aren't zero-normalized: a run is ranked by length first,
        // so differently-padded spellings of one value sort by width. Accepted
        // quirk — mixing padding widths within a namespace is malformed input.
        assert_eq!(natural_cmp("v1", "v01"), Ordering::Less); // shorter run first
        assert_eq!(natural_cmp("v007", "v7"), Ordering::Greater); // wider run later
                                                                  // So a smaller padded value can sort after a larger un-padded one:
        assert_eq!(natural_cmp("a007", "a10"), Ordering::Greater);
        // Equal width compares lexically, which at equal width equals numeric.
        assert_eq!(natural_cmp("v02", "v10"), Ordering::Less);
    }

    #[test]
    fn no_panic_on_large_numbers_or_all_zeros() {
        assert_eq!(
            natural_cmp("a99999999999999999999", "a100000000000000000000"),
            Ordering::Less
        );
        assert_eq!(natural_cmp("x0", "x000"), Ordering::Less);
        assert_eq!(natural_cmp("0", "0"), Ordering::Equal);
    }

    #[test]
    fn prefix_is_less_than_extension() {
        assert_eq!(natural_cmp("loss", "loss/eval"), Ordering::Less);
        assert_eq!(natural_cmp("loss", "loss"), Ordering::Equal);
    }

    #[test]
    fn minus_and_dot_are_separators_not_sign_or_decimal() {
        // Documented, intentional: only unsigned integer runs are numeric, so
        // `-` sorts by magnitude (not signed value) and `.`'s fraction is read
        // as a second integer. Matches the natsort default (unsigned int).
        assert_eq!(natural_cmp("x-2", "x-10"), Ordering::Less);
        assert_eq!(natural_cmp("t1.5", "t1.10"), Ordering::Less);
    }
}

#[cfg(test)]
mod editor_focus_tests {
    use super::*;

    #[test]
    fn trigger_ids_encode_arbitrary_identities_and_mount_roles() {
        let grid = editor_trigger_id("rect-grid", "a b\n-'\"");
        assert_eq!(grid, "kymo-editor-rect-grid-6120620a2d2722");
        assert_ne!(grid, editor_trigger_id("rect-max", "a b\n-'\""));
        assert_ne!(
            editor_trigger_id("section", "ab"),
            editor_trigger_id("section", "a-b")
        );
    }
}
