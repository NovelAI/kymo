use dioxus::prelude::*;

/// Live-apply a reactive draft without emitting its open-time value.
///
/// `current` must read every draft signal that should trigger an apply. The
/// first effect run performs those reads only; subsequent runs call `apply`.
/// The returned Cancel callback restores `initial` after at least one live
/// apply, then calls `close`. Untouched editors therefore close without a
/// no-op restore.
pub fn use_live_apply<T>(
    initial: T,
    current: impl Fn() -> T + 'static,
    apply: impl Fn(T) + Clone + 'static,
    close: impl Fn() + Clone + 'static,
) -> impl Fn() + Clone + 'static
where
    T: Clone + 'static,
{
    let initial = use_hook(|| initial);
    let mut armed = use_signal(|| false);
    let mut dirty = use_signal(|| false);
    let apply_live = apply.clone();

    use_effect(move || {
        let value = current();
        if !*armed.peek() {
            armed.set(true);
            return;
        }
        dirty.set(true);
        apply_live(value);
    });

    move || {
        if *dirty.peek() {
            apply(initial.clone());
        }
        close();
    }
}
