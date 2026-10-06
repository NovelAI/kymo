use dioxus::prelude::*;

/// Live-apply a reactive draft: `current` reads every draft signal; each change against the value last applied (the one at mount first), a Revert's included, calls `apply` with the previous value and the new one, so an editor can write just what changed. Setting a draft signal to the value it already has applies nothing.
pub fn use_live_apply<T: Clone + PartialEq + 'static>(
    current: impl Fn() -> T + 'static,
    apply: impl Fn(&T, T) + 'static,
) {
    let mut last = use_hook(|| CopyValue::new(current()));
    use_effect(move || {
        let value = current();
        let previous = std::mem::replace(&mut *last.write(), value.clone());
        if previous != value {
            apply(&previous, value);
        }
    });
}
