mod components;
mod grpc;
mod pages;
mod route;
mod runtime;
mod state;
mod util;

use dioxus::prelude::*;
use route::Route;

// Static-head assets apply before first paint, so a cold load is never unstyled or wrongly themed. THEME_BOOT must stay a classic script after the kymo.css link (dx orders static-head assets by source path): it waits for the stylesheet and blocks the parser, holding back the app's loader in <body>.
const STYLE: Asset = asset!(
    "/assets/kymo.css",
    AssetOptions::css().with_static_head(true)
);
const THEME_BOOT: Asset = asset!(
    "/assets/theme_boot.js",
    AssetOptions::js().with_static_head(true)
);
// Vendored uPlot draws _focus series last; uplot_chart/highlight.js sets that flag.
// See assets/vendor/README.md for the patch's provenance.
const UPLOT_JS: Asset = asset!("/assets/vendor/uPlot.iife.min.js");
// Static head, not a runtime link: while a sheet is still loading, WebKit reports new elements to ResizeObserver at 0×0, and a callback that reads layout then raises a "ResizeObserver loop" notice.
const UPLOT_CSS: Asset = asset!(
    "/assets/vendor/uPlot.min.css",
    AssetOptions::css().with_static_head(true)
);
const SOURCE_SANS_400: Asset = asset!("/assets/vendor/source-sans-pro-400.ttf");
const SOURCE_SANS_600: Asset = asset!("/assets/vendor/source-sans-pro-600.ttf");
const SOURCE_CODE_400: Asset = asset!("/assets/vendor/source-code-pro-400.ttf");
const UPLOT_LICENSE: Asset = asset!("/assets/vendor/LICENSE-uPlot.txt");
const SOURCE_SANS_LICENSE: Asset = asset!("/assets/vendor/LICENSE-source-sans.md");
const SOURCE_CODE_LICENSE: Asset = asset!("/assets/vendor/LICENSE-source-code.md");
const TWEMOJI_LICENSE: Asset = asset!("/assets/vendor/LICENSE-twemoji.md");
const BOOTSTRAP_ICONS_LICENSE: Asset = asset!("/assets/vendor/LICENSE-bootstrap-icons.txt");
const MATERIAL_ICONS_LICENSE: Asset = asset!("/assets/vendor/LICENSE-material-icons.txt");

// Keyboard/assistive-tech activation arrives as a click with detail 0 (pointer clicks always have detail >= 1); replay it as the primary mousedown that controls activate on (util::primary, AI-1418). ARIA checkboxes also receive a matching mouseup so virtual activation cannot leave drag-paint armed. Physical pointer gestures never enter this listener. Guarded so a re-mount can't stack a second listener.
const KB_ACTIVATE_JS: &str = r#"if(!window.__kymo_kbActivate){window.__kymo_kbActivate=1;document.addEventListener('click',(e)=>{if(e.detail!==0)return;const c=e.target.closest?.('[role="checkbox"]');if(c){c.dispatchEvent(new MouseEvent('mousedown',{bubbles:true,button:0}));c.dispatchEvent(new MouseEvent('mouseup',{bubbles:true,button:0}));return}const b=e.target.closest?.('button');if(b)b.dispatchEvent(new MouseEvent('mousedown',{bubbles:true,button:0}));});}"#;
// One gallery player at a time: starting one pauses the others. In Chromium a player holds one of the CDN origin's six HTTP/1.1 connections while it plays and for 10-20 s after it pauses, and six held connections stall image loads from that origin. `play` doesn't bubble, hence capture. Guarded like KB_ACTIVATE_JS.
const MEDIA_SOLO_JS: &str = r#"if(!window.__kymo_mediaSolo){window.__kymo_mediaSolo=1;document.addEventListener('play',(e)=>{for(const m of document.querySelectorAll('video,audio'))if(m!==e.target)m.pause();},true);}"#;
const CHART_COPY_JS: &str = include_str!("components/uplot_chart/copy.js");
const CHART_HIGHLIGHT_JS: &str = include_str!("components/uplot_chart/highlight.js");
const CHART_HOVER_JS: &str = include_str!("components/uplot_chart/hover_points.js");
const OVERFLOW_FADE_JS: &str = include_str!("util/overflow_fade.js");

const VISIBILITY_BRIDGE_JS: &str = r#"(()=>{
function send(){try{dioxus.send(document.hidden)}catch(_){td();}}
function cleanup(){document.removeEventListener('visibilitychange',send);}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup);
document.addEventListener('visibilitychange',send);
send();
})()"#;

fn font_faces() -> String {
    format!(
        r#"@font-face{{font-family:"Source Sans Pro";font-style:normal;font-weight:400;font-display:swap;src:url("{SOURCE_SANS_400}") format("truetype")}}@font-face{{font-family:"Source Sans Pro";font-style:normal;font-weight:600;font-display:swap;src:url("{SOURCE_SANS_600}") format("truetype")}}@font-face{{font-family:"Source Code Pro";font-style:normal;font-weight:400;font-display:swap;src:url("{SOURCE_CODE_400}") format("truetype")}}"#
    )
}

#[allow(non_snake_case)]
fn App() -> Element {
    // Referencing the notices makes Dioxus copy them beside the corresponding vendored code/fonts in both hosted bundles and the embedded local wheel.
    let _third_party_notices = (
        UPLOT_LICENSE,
        SOURCE_SANS_LICENSE,
        SOURCE_CODE_LICENSE,
        TWEMOJI_LICENSE,
        BOOTSTRAP_ICONS_LICENSE,
        MATERIAL_ICONS_LICENSE,
    );
    // Likewise for the static-head assets, which dx links from index.html.
    let _static_head = (STYLE, THEME_BOOT, UPLOT_CSS);
    // Above the router so every page resolves the same preferences; construction applies the font size and theme (stored, else the OS's) before descendants mount.
    let user_config = use_context_provider(state::UserConfigState::new);
    state::use_os_theme(user_config);

    let visibility_bridge = util::js_bridge::use_bridge("visibility");

    // Page Visibility → the transport's hidden flag (see grpc/ws.rs for
    // what parks on it). Seeded SYNCHRONOUSLY (js_sys::eval) so a tab
    // opened in the background is hidden before the socket can connect and
    // trigger the initial resync.
    use_hook({
        let visibility_bridge = visibility_bridge.clone();
        move || {
            if let Ok(v) = js_sys::eval("document.hidden") {
                grpc::set_page_visibility(v.as_bool().unwrap_or(false));
            }
            // Bootstrap the WebSocket unconditionally: every page's data flow
            // starts with a pushed resync, and awaiting a push subscription can't
            // spawn the connection it waits on (a cold "/" load would
            // otherwise deadlock on "Loading...").
            grpc::GrpcClient::new();
            let js = visibility_bridge.script(VISIBILITY_BRIDGE_JS);
            spawn(async move {
                loop {
                    // The listener re-sends the CURRENT state right after registering, catching any flip inside the seed → registration gap (or across a re-registration).
                    let mut eval = document::eval(&js);
                    while let Ok(hidden) = eval.recv::<bool>().await {
                        grpc::set_page_visibility(hidden);
                    }
                    // Channel died (shouldn't happen at the root scope): the flag gates ALL traffic, so re-register rather than leave it frozen; the shared registry evicts the predecessor.
                    util::warn("[visibility] listener channel lost; re-registering");
                    gloo_timers::future::sleep(std::time::Duration::from_secs(1)).await;
                }
            });
        }
    });

    rsx! {
        document::Script { src: UPLOT_JS }
        document::Script { {KB_ACTIVATE_JS} }
        document::Script { {MEDIA_SOLO_JS} }
        document::Style { {font_faces()} }
        // The notice bar sits above the router so it survives navigation and covers every page
        // (sizing: kymo.css App Shell).
        div { class: "app-root",
            components::notice_bar::NoticeBar {}
            Router::<Route> {}
        }
    }
}

#[allow(non_snake_case)]
fn Root() -> Element {
    match use_hook(runtime::initialize) {
        Ok(()) => rsx! { App {} },
        Err(error) => rsx! {
            main {
                h1 { "kymo is unavailable" }
                p { "{error}" }
                if cfg!(feature = "local-runtime") {
                    p { "Run `kymo open` again to restart the local dashboard." }
                }
            }
        },
    }
}

fn main() {
    js_sys::eval(CHART_COPY_JS).expect("failed to install the chart copy bridge");
    js_sys::eval(CHART_HIGHLIGHT_JS).expect("failed to install chart highlighting");
    js_sys::eval(CHART_HOVER_JS).expect("failed to install chart hover lookup");
    js_sys::eval(OVERFLOW_FADE_JS).expect("failed to install the overflow fade observer");
    js_sys::eval(util::js_bridge::LIFECYCLE_JS)
        .expect("failed to install the JavaScript bridge registry");
    dioxus::launch(Root);
}

#[cfg(test)]
mod bridge_template_tests {
    #[test]
    fn visibility_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::VISIBILITY_BRIDGE_JS);
    }
}

#[cfg(test)]
mod stylesheet_brand_tests {
    const CSS: &str = include_str!("../assets/kymo.css");

    #[test]
    fn runtime_selectors_and_sidebar_variable_have_kymo_styles() {
        for expected in [
            "--kymo-sidebar-w",
            ".kymo-tip",
            ".kymo-tip-row",
            ".kymo-hotpt",
        ] {
            assert!(
                CSS.contains(expected),
                "missing stylesheet contract {expected}"
            );
        }
        assert!(!CSS.contains("mkdb2"));
    }
}
