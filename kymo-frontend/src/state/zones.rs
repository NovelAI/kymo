//! Page-wide viewport-zone tracking: TWO IntersectionObservers total (plus MutationObservers for `.metric-slot` auto-registration and the grid's `inert` under the maximize overlay), one rAF-batched eval channel, and ZoneBridge fanning changes out to per-slot signals. Replaces per-rect observer pairs and eval channels — at thousands of panels those dominated idle cost. A slot that never changes zone costs nothing after its initial classification.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use dioxus::prelude::*;
use gloo_timers::future::sleep;

use crate::state::visibility::Zone;

/// The JS half. Element state lives on a Map keyed by element; slots carry their rect id in `data-slot-id`. Zone codes match the old per-rect protocol: 2 = Visible, 1 = Near, 0 = Far. `js_bridge` owns predecessor eviction and late-drop fencing; this body owns only observer cleanup and messages.
const BOOTSTRAP_JS: &str = r#"(()=>{
const st=new Map();
const dirty=new Map();
let scheduled=false;
function flush(){scheduled=false;if(!dirty.size)return;const b=[...dirty];dirty.clear();try{dioxus.send(b);}catch(_){td();}}
function queue(s){if(s.v===null||s.n===null)return;dirty.set(s.id,s.v?(covered?1:2):(s.n?1:0));if(!scheduled){scheduled=true;requestAnimationFrame(flush);}}
function cb(k){return function(es){for(const e of es){const s=st.get(e.target);if(s){s[k]=e.isIntersecting;queue(s);}}};}
// Root at the SCROLL CONTAINER, not the viewport: targets are clipped by
// every scrollable ancestor BEFORE intersecting the (margin-expanded)
// root, so with root:null the margin was a no-op — below-the-fold slots
// stayed clipped-empty until actually scrolled into .main-content, and
// Near fired at the same instant as Visible.
const sc=document.querySelector('main.main-content');
const vo=new IntersectionObserver(cb('v'),{root:sc});
const no=new IntersectionObserver(cb('n'),{root:sc,rootMargin:'100% 0px 100% 0px'});
// Occlusion: IntersectionObserver is geometry-only — z-order is not an input, so the maximize overlay covering the grid fires no events. The scroll container is inert exactly while the overlay is up (dashboard_layout.rs), and intersecting slots then cap at Near: canvases unmount (leaving the cursor-sync and highlight-redraw loops to the overlay chart) but data stays warm for an instant close. Only intersecting slots' codes depend on `covered`, so a flip re-queues exactly those — every queued entry is a real change.
let covered=!!sc&&sc.hasAttribute('inert');
const lo=new MutationObserver(function(){const c=sc.hasAttribute('inert');if(c!==covered){covered=c;for(const s of st.values())if(s.v)queue(s);}});
if(sc)lo.observe(sc,{attributes:true,attributeFilter:['inert']});
function add(el){if(st.has(el))return;st.set(el,{id:el.dataset.slotId,v:null,n:null});vo.observe(el);no.observe(el);}
function rm(el){if(st.delete(el)){vo.unobserve(el);no.unobserve(el);}}
function scan(n,f){if(!(n instanceof Element))return;if(n.matches('.metric-slot'))f(n);for(const el of n.querySelectorAll('.metric-slot'))f(el);}
const mo=new MutationObserver(function(ms){for(const m of ms){for(const n of m.addedNodes)scan(n,add);for(const n of m.removedNodes)scan(n,rm);}});
function cleanup(){mo.disconnect();vo.disconnect();no.disconnect();lo.disconnect();st.clear();dirty.clear();}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup);
mo.observe(document.body,{childList:true,subtree:true});
for(const el of document.querySelectorAll('.metric-slot'))add(el);
})()"#;

/// rect id -> that slot's zone signal. Slots insert on mount and remove in use_drop; removal precedes the signal's own drop, so the pump can never write a dead signal (wasm is single-threaded — no interleaving between the lookup and the write).
#[derive(Clone, Default)]
pub struct ZoneRegistry(Rc<RefCell<HashMap<String, Signal<Zone>>>>);

impl ZoneRegistry {
    pub fn register(&self, id: String, zone: Signal<Zone>) {
        self.0.borrow_mut().insert(id, zone);
    }

    /// Remove `id` — but only while it still maps to `zone`: if a new slot with the same id registered before this (unmounting) one dropped, the entry is the successor's and must survive.
    pub fn unregister(&self, id: &str, zone: Signal<Zone>) {
        let mut map = self.0.borrow_mut();
        if map.get(id).is_some_and(|s| s.id() == zone.id()) {
            map.remove(id);
        }
    }

    fn apply(&self, id: &str, zone: Zone) {
        // Copy the signal out before writing — the write queues renders that may re-enter the registry.
        let sig = self.0.borrow().get(id).copied();
        if let Some(mut sig) = sig {
            // This cross-scope use is sound by the registry discipline above (entries leave in use_drop before their signal drops, and single-threaded wasm can't interleave the pump with a teardown), but dioxus's hoisted-value lint can't see that and prints its full help page per zone write — acknowledge it at this one site so the lint stays live everywhere else.
            use dioxus::warnings::Warning as _;
            dioxus::signals::warnings::copy_value_hoisted::allow(|| {
                if *sig.peek() != zone {
                    sig.set(zone);
                }
            });
        }
    }
}

/// Invisible component owning the JS bootstrap and the pump that folds batched zone changes into slot signals — the only place observer output becomes signal writes. Mount once per dashboard, next to PushBridge.
#[component]
pub fn ZoneBridge() -> Element {
    let registry = use_context::<ZoneRegistry>();
    let bridge = crate::util::js_bridge::use_bridge("zones");

    // use_effect, NOT use_future: a render-spawned task can first-poll BEFORE its own pass's mutations reach the DOM (dioxus interleaves task polls into render_immediate whenever another dirty task drains the scheduler channel mid-pass — the bug that killed the chart zoom listeners), and this bootstrap's querySelector('main.main-content') would then silently degrade every observer to root:null: the clipped-empty / Near-fires-with-Visible pathology described on BOOTSTRAP_JS. Effects run only when no scope is dirty, never inside a render pass, and always after the queuing pass's flush — so the scroll container exists by contract when this runs, and the bootstrap's full .metric-slot scan means starting post-flush misses nothing. The closure reads no signals, so it runs exactly once; later re-bootstraps (channel loss) happen long after mount and are safe at any time.
    use_effect({
        let registry = registry.clone();
        let bridge = bridge.clone();
        move || {
            let registry = registry.clone();
            let js = bridge.script(BOOTSTRAP_JS);
            spawn(async move {
                loop {
                    let mut eval = document::eval(&js);
                    while let Ok(batch) = eval.recv::<Vec<(String, u8)>>().await {
                        for (id, z) in batch {
                            let zone = match z {
                                2 => Zone::Visible,
                                1 => Zone::Near,
                                _ => Zone::Far,
                            };
                            registry.apply(&id, zone);
                        }
                    }
                    // Channel died (shouldn't happen while this page lives): zones gate every panel's mount and fetches, so re-bootstrap rather than leave them frozen — the bootstrap tears down its predecessor.
                    crate::util::warn("[zones] bridge channel lost; re-registering");
                    sleep(Duration::from_secs(1)).await;
                }
            });
        }
    });

    rsx! {}
}

#[cfg(test)]
mod bridge_template_tests {
    #[test]
    fn zones_use_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::BOOTSTRAP_JS);
    }
}
