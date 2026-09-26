use std::collections::BTreeMap;

use dioxus::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wasm_bindgen::JsCast;

use crate::util::local_storage;

// The key carries the incompatible-format version. The raw envelope below
// preserves additive fields; an incompatible schema must use a new key.
const STORAGE_KEY: &str = "kymo_user_config_v1";
// Pre-rename key; read as a fallback and retired on the next confirmed write (see local_storage).
const LEGACY_STORAGE_KEY: &str = "mkdb2_user_config_v1";
// The theme keeps its own raw "light"/"dark" key, outside the config envelope; assets/theme_boot.js reads it (and the legacy key) before the wasm loads.
const THEME_KEY: &str = "kymo_theme";
const LEGACY_THEME_KEY: &str = "mkdb2_theme";

/// Root font size in CSS pixels. Canvas and virtualized rows share its scale.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct FontSize(u8);

impl Default for FontSize {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl FontSize {
    pub const MIN: u8 = 12;
    pub const MAX: u8 = 24;
    pub const DEFAULT: Self = Self(16);

    pub const fn new(pixels: u8) -> Option<Self> {
        if pixels >= Self::MIN && pixels <= Self::MAX {
            Some(Self(pixels))
        } else {
            None
        }
    }

    pub const fn pixels(self) -> u8 {
        self.0
    }

    fn from_stored(value: &Value) -> Option<Self> {
        // Accept the prototype's named presets without rewriting untouched values.
        let pixels = match value.as_str() {
            Some("small") => 13,
            Some("default") => 15,
            Some("large") => 17,
            Some("extra_large") => 20,
            _ => u8::try_from(value.as_u64()?).ok()?,
        };
        Self::new(pixels)
    }

    /// Match the CSS typography scale's fixed 15px design base, not the user default.
    pub const fn scale_px(self, base_px: u32) -> u32 {
        (base_px * self.pixels() as u32 + 7) / 15
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct UserConfig {
    pub font_size: FontSize,
    pub single_click_unzoom: bool,
    pub highlight_same_name: bool,
    pub show_nearest_point: bool,
    /// Default for sections the user hasn't toggled; see `SectionConfig::is_collapsed`.
    pub sections_visible: bool,
}

impl Default for UserConfig {
    fn default() -> Self {
        Self {
            font_size: FontSize::default(),
            single_click_unzoom: true,
            highlight_same_name: false,
            show_nearest_point: false,
            sections_visible: true,
        }
    }
}

impl UserConfig {
    pub(crate) fn apply_to_document(&self) {
        let Some(root) = web_sys::window()
            .and_then(|window| window.document())
            .and_then(|document| document.document_element())
            .and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
        else {
            return;
        };
        let value = format!("{}px", self.font_size.pixels());
        let _ = root.style().set_property("--kymo-user-font-size", &value);
        for (name, enabled) in [
            ("data-kymo-single-click-unzoom", self.single_click_unzoom),
            ("data-kymo-highlight-same-name", self.highlight_same_name),
            ("data-kymo-show-nearest-point", self.show_nearest_point),
        ] {
            let _ = root.set_attribute(name, if enabled { "true" } else { "false" });
        }
        let _ = js_sys::eval("window.__kymo_refreshHl()");
    }
}

/// A raw envelope lets older builds retain unknown keys and newer values.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct StoredUserConfig(BTreeMap<String, Value>);

impl StoredUserConfig {
    fn load() -> Self {
        local_storage::get_migrating(STORAGE_KEY, LEGACY_STORAGE_KEY)
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default()
    }

    fn effective(&self) -> UserConfig {
        let defaults = UserConfig::default();
        UserConfig {
            font_size: self
                .0
                .get("font_size")
                .and_then(FontSize::from_stored)
                .unwrap_or_default(),
            single_click_unzoom: self
                .bool_value("single_click_unzoom", defaults.single_click_unzoom),
            highlight_same_name: self
                .bool_value("highlight_same_name", defaults.highlight_same_name),
            show_nearest_point: self.bool_value("show_nearest_point", defaults.show_nearest_point),
            sections_visible: self.bool_value("sections_visible", defaults.sections_visible),
        }
    }

    fn bool_value(&self, key: &str, default: bool) -> bool {
        self.0.get(key).and_then(Value::as_bool).unwrap_or(default)
    }

    /// Store each field `draft` changed from `initial`, removing one that lands on the product default so it follows future default changes.
    fn record(&mut self, initial: UserConfig, draft: UserConfig) {
        let [Ok(Value::Object(initial)), Ok(Value::Object(draft)), Ok(Value::Object(defaults))] =
            [initial, draft, UserConfig::default()].map(serde_json::to_value)
        else {
            unreachable!("UserConfig serializes to a JSON object");
        };
        for (key, value) in draft {
            if value == initial[&key] {
                continue;
            }
            if value == defaults[&key] {
                self.0.remove(&key);
            } else {
                self.0.insert(key, value);
            }
        }
    }

    fn persist(&self) -> bool {
        if self.0.is_empty() {
            local_storage::remove_migrating(STORAGE_KEY, LEGACY_STORAGE_KEY)
        } else {
            serde_json::to_string(self).is_ok_and(|json| {
                local_storage::set_migrating(STORAGE_KEY, LEGACY_STORAGE_KEY, &json)
            })
        }
    }
}

/// Reactive app-root owner for browser-local user preferences. Per-project
/// layout state deliberately remains in `DashboardState`/`LayoutDiff`.
#[derive(Clone, Copy)]
pub struct UserConfigState {
    config: Signal<UserConfig>,
    light: Signal<bool>,
}

fn apply_theme(light: bool) {
    if let Some(root) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.document_element())
    {
        let _ = root.set_attribute("data-theme", if light { "light" } else { "dark" });
    }
}

/// The toggle's explicit choice; without one the theme follows the OS.
fn stored_light() -> Option<bool> {
    match local_storage::get_migrating(THEME_KEY, LEGACY_THEME_KEY).as_deref() {
        Some("light") => Some(true),
        Some("dark") => Some(false),
        _ => None,
    }
}

fn os_prefers_light() -> bool {
    web_sys::window()
        .and_then(|window| {
            window
                .match_media("(prefers-color-scheme: light)")
                .ok()
                .flatten()
        })
        .is_some_and(|query| query.matches())
}

// Sends the OS preference once registered, then on every change.
const COLOR_SCHEME_BRIDGE_JS: &str = r#"(()=>{
const query=matchMedia('(prefers-color-scheme: light)');
function send(){try{dioxus.send(query.matches)}catch(_){td();}}
function cleanup(){query.removeEventListener('change',send);}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup);
query.addEventListener('change',send);
send();
})()"#;

/// Follows the OS preference until the toggle stores a choice; call once, beside the `UserConfigState` provider.
pub fn use_os_theme(user_config: UserConfigState) {
    let bridge = crate::util::js_bridge::use_bridge("color-scheme");
    use_hook(move || {
        let js = bridge.script(COLOR_SCHEME_BRIDGE_JS);
        spawn(async move {
            let mut eval = document::eval(&js);
            while let Ok(light) = eval.recv::<bool>().await {
                user_config.follow_os(light);
            }
            crate::util::warn("[color-scheme] listener channel lost");
        });
    });
}

impl UserConfigState {
    pub fn new() -> Self {
        let config = StoredUserConfig::load().effective();
        config.apply_to_document();
        let light = stored_light().unwrap_or_else(os_prefers_light);
        apply_theme(light);
        Self {
            config: Signal::new(config),
            light: Signal::new(light),
        }
    }

    pub fn is_light(&self) -> bool {
        *self.light.read()
    }

    /// Stores the new theme as an explicit choice, or clears the choice when the new theme is the OS's, so the page follows the OS again.
    pub fn toggle_theme(&self) {
        let light = !*self.light.peek();
        if light == os_prefers_light() {
            local_storage::remove_migrating(THEME_KEY, LEGACY_THEME_KEY);
        } else {
            local_storage::set_migrating(
                THEME_KEY,
                LEGACY_THEME_KEY,
                if light { "light" } else { "dark" },
            );
        }
        self.set_light(light);
    }

    fn follow_os(&self, os_light: bool) {
        let light = stored_light().unwrap_or(os_light);
        if light != *self.light.peek() {
            self.set_light(light);
        }
    }

    fn set_light(&self, light: bool) {
        apply_theme(light);
        // Charts resolve axis/grid colors per draw, so a repaint retints them.
        let _ = js_sys::eval(
            "for(const c of Object.values(window.__kymo_charts||{})){try{c.redraw(false)}catch(_){}}",
        );
        let mut signal = self.light;
        signal.set(light);
    }

    pub fn current(&self) -> UserConfig {
        *self.config.peek()
    }

    pub fn font_size(&self) -> FontSize {
        self.config.read().font_size
    }

    pub fn sections_visible(&self) -> bool {
        self.config.read().sections_visible
    }

    /// Re-read before writing so only edited fields replace stored values;
    /// unknown/newer fields continue to round-trip.
    pub fn commit(&self, draft: UserConfig) -> bool {
        let initial = *self.config.peek();
        if draft == initial {
            return true;
        }
        let mut stored = StoredUserConfig::load();
        stored.record(initial, draft);
        if !stored.persist() {
            return false;
        }
        draft.apply_to_document();
        let mut signal = self.config;
        signal.set(draft);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_scheme_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(COLOR_SCHEME_BRIDGE_JS);
    }

    fn parse(json: &str) -> StoredUserConfig {
        serde_json::from_str(json).expect("valid stored config")
    }

    /// A single-tab Save: `change` applied to the stored settings' effective config.
    fn edit(stored: &mut StoredUserConfig, change: impl FnOnce(&mut UserConfig)) {
        let initial = stored.effective();
        let mut draft = initial;
        change(&mut draft);
        stored.record(initial, draft);
    }

    #[test]
    fn missing_override_uses_the_product_default() {
        let stored = parse("{}");
        assert_eq!(stored.effective(), UserConfig::default());
        assert_eq!(stored.effective().font_size.pixels(), 16);
        assert!(stored.effective().single_click_unzoom);
        assert!(!stored.effective().highlight_same_name);
        assert!(!stored.effective().show_nearest_point);
        assert!(stored.effective().sections_visible);
        assert_eq!(stored.effective().font_size.scale_px(12), 13);
    }

    #[test]
    fn explicit_overrides_survive_default_changes() {
        let stored = parse(r#"{"font_size":15,"single_click_unzoom":false}"#);
        assert_eq!(stored.effective().font_size.pixels(), 15);
        assert!(!stored.effective().single_click_unzoom);
    }

    #[test]
    fn font_sizes_accept_each_pixel_and_legacy_presets() {
        for pixels in FontSize::MIN..=FontSize::MAX {
            let size = FontSize::new(pixels).unwrap();
            assert_eq!(FontSize::from_stored(&Value::from(pixels)), Some(size));
            assert_eq!(size.scale_px(15), u32::from(pixels));
        }
        for (legacy, pixels) in [
            ("small", 13),
            ("default", 15),
            ("large", 17),
            ("extra_large", 20),
        ] {
            assert_eq!(
                FontSize::from_stored(&Value::from(legacy)),
                FontSize::new(pixels)
            );
        }
        for invalid in [
            serde_json::json!(11),
            serde_json::json!(25),
            serde_json::json!(18.5),
            serde_json::json!("huge"),
        ] {
            assert_eq!(FontSize::from_stored(&invalid), None);
        }
        assert_eq!(FontSize::new(20).unwrap().scale_px(16), 21);
    }

    #[test]
    fn default_is_a_sparse_override() {
        let mut stored = parse(r#"{"font_size":"large"}"#);
        edit(&mut stored, |c| c.font_size = FontSize::default());
        assert!(!stored.0.contains_key("font_size"));
        assert_eq!(serde_json::to_value(stored).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn newer_fields_and_values_round_trip() {
        let mut stored = parse(r#"{"font_size":"huge","future_contrast":"high"}"#);
        assert_eq!(stored.effective().font_size, FontSize::default());

        edit(&mut stored, |c| c.font_size = FontSize::new(18).unwrap());
        let value = serde_json::to_value(stored).unwrap();
        assert_eq!(value["font_size"], 18);
        assert_eq!(value["future_contrast"], "high");
    }

    #[test]
    fn chart_gesture_override_is_sparse_and_field_local() {
        let mut stored =
            parse(r#"{"font_size":"huge","single_click_unzoom":false,"future_contrast":"high"}"#);
        assert!(!stored.effective().single_click_unzoom);

        edit(&mut stored, |c| c.single_click_unzoom = true);
        let value = serde_json::to_value(stored).unwrap();
        assert_eq!(value["font_size"], "huge");
        assert!(value.get("single_click_unzoom").is_none());
        assert_eq!(value["future_contrast"], "high");
    }

    #[test]
    fn hover_overrides_are_opt_in_and_preserve_unrelated_fields() {
        let mut stored =
            parse(r#"{"font_size":"huge","single_click_unzoom":false,"future_contrast":"high"}"#);
        edit(&mut stored, |c| {
            c.highlight_same_name = true;
            c.show_nearest_point = true;
        });
        let encoded = serde_json::to_string(&stored).unwrap();
        let restored = parse(&encoded).effective();
        assert!(restored.highlight_same_name);
        assert!(restored.show_nearest_point);

        edit(&mut stored, |c| c.highlight_same_name = false);
        assert!(stored.effective().show_nearest_point);
        edit(&mut stored, |c| c.show_nearest_point = false);
        assert_eq!(
            serde_json::to_value(stored).unwrap(),
            serde_json::json!({
                "font_size": "huge",
                "single_click_unzoom": false,
                "future_contrast": "high",
            }),
        );
    }

    #[test]
    fn malformed_hover_overrides_stay_off() {
        for invalid in [serde_json::json!("true"), serde_json::json!(1), Value::Null] {
            let stored = parse(
                &serde_json::json!({
                    "highlight_same_name": invalid,
                    "show_nearest_point": invalid,
                })
                .to_string(),
            );
            assert!(!stored.effective().highlight_same_name);
            assert!(!stored.effective().show_nearest_point);
        }
    }
}
