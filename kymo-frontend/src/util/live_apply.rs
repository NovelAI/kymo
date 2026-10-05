use std::cell::RefCell;
use std::rc::Rc;

use dioxus::prelude::*;

/// Live-apply a reactive draft without emitting its open-time value: `current` reads every draft signal, the first effect run only subscribes, and each later change (Revert setting the drafts back included) calls `apply` with the value it replaces and the new one, so an editor can write just what changed. A write that leaves the draft as it was (a clamped input, a Revert with nothing to put back) applies nothing.
pub fn use_live_apply<T: Clone + PartialEq + 'static>(
    current: impl Fn() -> T + 'static,
    apply: impl Fn(&T, T) + 'static,
) {
    let last = use_hook(|| Rc::new(RefCell::new(None::<T>)));
    use_effect(move || {
        let value = current();
        let previous = last.replace(Some(value.clone()));
        if let Some(previous) = previous.filter(|previous| *previous != value) {
            apply(&previous, value);
        }
    });
}
