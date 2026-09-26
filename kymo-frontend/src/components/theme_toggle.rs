use dioxus::prelude::*;

use crate::components::icons::{MoonIcon, SunIcon};
use crate::state::UserConfigState;
use crate::util::primary;

/// Light/dark switch; `class` styles it for its host (navbar icon or page button).
#[component]
pub fn ThemeToggle(class: &'static str) -> Element {
    let user_config = use_context::<UserConfigState>();
    let light = user_config.is_light();
    rsx! {
        button {
            class: "{class} icon-button",
            title: if light { "Switch to dark mode" } else { "Switch to light mode" },
            onmousedown: primary(move |_| user_config.toggle_theme()),
            if light { MoonIcon {} } else { SunIcon {} }
        }
    }
}
