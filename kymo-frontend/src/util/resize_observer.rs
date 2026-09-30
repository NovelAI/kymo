use wasm_bindgen::{closure::Closure, JsCast};

/// A ResizeObserver on one element, owned by Rust: dropping it disconnects the observer, so no callback outlives its component. Dioxus 0.7.9's onresize does not unobserve on component removal.
pub(crate) struct ElementResizeObserver {
    observer: web_sys::ResizeObserver,
    _callback: Closure<dyn FnMut(js_sys::Array)>,
}

impl ElementResizeObserver {
    /// Create it synchronously in `onmounted`, never in a render-spawned task. The browser delivers one entry on observe, which is the initial measurement.
    pub(crate) fn new(
        element: &web_sys::Element,
        mut on_resize: impl FnMut(&web_sys::ResizeObserverEntry) + 'static,
    ) -> Self {
        let callback = Closure::<dyn FnMut(js_sys::Array)>::new(move |entries: js_sys::Array| {
            for entry in entries.iter() {
                on_resize(entry.unchecked_ref());
            }
        });
        let observer = web_sys::ResizeObserver::new(callback.as_ref().unchecked_ref())
            .expect("browser supports ResizeObserver");
        observer.observe(element);
        Self {
            observer,
            _callback: callback,
        }
    }
}

impl Drop for ElementResizeObserver {
    fn drop(&mut self) {
        self.observer.disconnect();
    }
}
