use dioxus::prelude::*;

/// Live-apply a reactive draft: `current` reads every draft signal; each change against the value last applied (the one at mount first), a Revert's included, calls `apply` with the previous value and the new one, so an editor can write just what changed. Setting a draft signal to the value it already has applies nothing.
/// `apply` returns whether the change took. One that didn't (the browser refused to store it) leaves the previous value in force, and `restore` sets the draft back to it, so picking the refused value again tries it again.
pub fn use_live_apply<T: Clone + PartialEq + 'static>(
    current: impl Fn() -> T + 'static,
    mut restore: impl FnMut(T) + 'static,
    apply: impl Fn(&T, T) -> bool + 'static,
) {
    let mut last = use_hook(|| CopyValue::new(current()));
    use_effect(move || {
        let value = current();
        if *last.peek() == value {
            return;
        }
        let previous = std::mem::replace(&mut *last.write(), value.clone());
        if !apply(&previous, value) {
            *last.write() = previous.clone();
            restore(previous);
        }
    });
}
