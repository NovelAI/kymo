//! Test-only allocator probe: the peak heap bytes a closure holds live on its own thread. Stage 2's build charge (budget.rs) is measured with it and checked against it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Probe;

#[global_allocator]
static PROBE: Probe = Probe;

thread_local! {
    /// (probing, live bytes since the probe started, their peak). Const-initialized and drop-free, so the allocator can read it without allocating.
    static STATE: Cell<(bool, isize, isize)> = const { Cell::new((false, 0, 0)) };
}

fn note(delta: isize) {
    let _ = STATE.try_with(|state| {
        let (probing, live, peak) = state.get();
        if probing {
            let live = live + delta;
            state.set((true, live, peak.max(live)));
        }
    });
}

unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            note(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            note(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        note(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if new == ptr {
            note(new_size as isize - layout.size() as isize);
        } else if !new.is_null() {
            // Moved: allocated, copied and freed, so both buffers were live at once.
            note(new_size as isize);
            note(-(layout.size() as isize));
        }
        new
    }
}

/// Run `f` and return its output with the peak bytes it held live on this thread (its output included while it is built) and the bytes still live when it returned (what it left allocated).
pub(super) fn peak_and_live_bytes<T>(f: impl FnOnce() -> T) -> (T, u64, i64) {
    STATE.with(|state| state.set((true, 0, 0)));
    let out = f();
    let (_, live, peak) = STATE.with(|state| state.replace((false, 0, 0)));
    (out, peak.max(0) as u64, live as i64)
}
