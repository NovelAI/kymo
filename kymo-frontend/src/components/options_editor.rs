use dioxus::prelude::*;
use serde_json::Value;

use crate::components::editor_dialog::EditorDialog;
use crate::components::icons::{CaretDownIcon, CaretRightIcon, CloseIcon};
use crate::state::layout_config::{
    ema_time_constant, finer_overrides, OptionOverride, OverrideTarget, RectOptions,
    SmoothingAlgorithm,
};
use crate::state::DashboardState;
use crate::util::{primary, use_live_apply};

fn clamp_max_runs(value: u32) -> u32 {
    value.min(64)
}

/// Reset an overridden field to its inherited value; hidden when `show` is false.
#[component]
fn ResetDot(show: bool, onreset: EventHandler<()>) -> Element {
    if !show {
        return rsx! {};
    }
    rsx! {
        button {
            class: "field-reset-dot",
            title: "Reset to inherited value",
            onmousedown: primary(move |_| onreset.call(())),
            "●"
        }
    }
}

const TRIANGULAR_POLYFIT_HINT: &str = "Polynomial fit where weight ~ (step + 1). Causal.";

const EMA_POLYFIT_HINT: &str =
    "Local polynomial fit on (causal) EMA. Order 0 is typical EMA smoothing.";

const BIWEIGHT_POLYFIT_HINT: &str =
    "Local polynomial fit in a symmetric window. Only compare runs when they have the same x values in the window. For example, do not use this to compare the last point of an incomplete run to other runs; use an EMA instead.\nTechnical details: This is Savitzky-Golay on a biweight window, which approximates a Gaussian. Higher powers are closer to Gaussian, and we chose the second power.";
/// UI cap for the biweight-polyfit smoothing window (the server clamps at 4096).
const BIWEIGHT_WINDOW_MAX: u32 = 500;

/// Format override values using the field's UI representation.
fn pretty_option_value(field: &str, v: &Value) -> String {
    match v {
        Value::Bool(b) => if *b { "on" } else { "off" }.to_string(),
        Value::String(s) => match s.as_str() {
            "TriangularPolyfit" => "Triangular polyfit",
            "EmaPolyfit" => "EMA polyfit",
            "BiweightPolyfit" => "(1−x²)² polyfit",
            "None" => "none",
            other => other,
        }
        .to_string(),
        Value::Number(n) if field == "smoothing_alpha" => {
            format!(
                "τ {}",
                ema_time_constant(n.as_f64().unwrap_or(0.0)).round() as u32
            )
        }
        other => other.to_string(),
    }
}

/// Clears finer-level pins so they inherit this field again.
fn override_chips(
    state: DashboardState,
    field: &'static str,
    overrides: &[OptionOverride],
) -> Element {
    let hits: Vec<OptionOverride> = overrides
        .iter()
        .filter(|o| o.field == field)
        .cloned()
        .collect();
    if hits.is_empty() {
        return rsx! {};
    }
    rsx! {
        div { class: "override-chips",
            span { class: "override-chips-label", "overrides:" }
            for o in hits {
                {
                    let (prefix, kind) = match &o.target {
                        OverrideTarget::Section(_) => ("§ ", "Section"),
                        OverrideTarget::Rect(_) => ("", "Chart"),
                    };
                    let text = format!("{prefix}{} = {}", o.label, pretty_option_value(field, &o.value));
                    let title = format!("{kind} \"{}\" overrides this field — click to clear so it re-inherits", o.label);
                    rsx! {
                        button {
                            class: "override-chip",
                            title: "{title}",
                            onmousedown: primary(move |_| match &o.target {
                                OverrideTarget::Section(name) => state.clear_section_chart_default(name, &o.field),
                                OverrideTarget::Rect(id) => state.clear_rect_option(id, &o.field),
                            }),
                            "{text}"
                            span { class: "override-chip-x", CloseIcon {} }
                        }
                    }
                }
            }
        }
    }
}

/// Edits project chart defaults live, storing their diff from library defaults. Cancel restores the open-time values; Save closes.
#[component]
pub fn ProjectDefaultsEditor(return_focus_id: String, on_close: EventHandler<()>) -> Element {
    let id_prefix = format!("{return_focus_id}-dialog");
    let state = use_context::<DashboardState>();
    let initial = use_hook(|| state.project_level_options());
    let draft = use_signal(|| initial.clone());

    let cancel_live = use_live_apply(
        initial.clone(),
        move || draft.read().clone(),
        move |opts| state.set_project_chart_defaults(opts),
        move || on_close.call(()),
    );

    rsx! {
        EditorDialog {
            return_focus_id,
            title: "Project chart defaults",
            on_cancel: move |_| cancel_live(),
            on_save: move |_| on_close.call(()),

            div { class: "editor-options",
                ChartOptionsForm { id_prefix, draft, anchor: RectOptions::default() }
            }
        }
    }
}

/// Collapsible editor content; the caller owns expansion state across visibility changes.
#[component]
pub fn EditorSection(title: String, open: Signal<bool>, children: Element) -> Element {
    rsx! {
        button {
            r#type: "button",
            class: "editor-section-label editor-section-toggle",
            aria_expanded: *open.read(),
            onmousedown: primary(move |_| open.toggle()),
            span { class: "editor-section-arrow",
                aria_hidden: "true",
                if *open.read() { CaretDownIcon {} } else { CaretRightIcon {} }
            }
            "{title}"
        }
        if *open.read() {
            {children}
        }
    }
}

fn field_class(overridden: bool) -> &'static str {
    if overridden {
        "binding-field field-overridden"
    } else {
        "binding-field"
    }
}

/// Smoothing controls shared by chart and defaults editors; optional chips expose finer-level overrides.
#[component]
pub fn SmoothingFields(
    id_prefix: String,
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    #[props(default)] overrides: Vec<OptionOverride>,
) -> Element {
    let state = use_context::<DashboardState>();
    let chips = |field| override_chips(state, field, &overrides);
    let o = draft.read().clone();
    let smoothing_value = match &o.smoothing {
        SmoothingAlgorithm::None => "none",
        SmoothingAlgorithm::TriangularPolyfit => "triangular",
        SmoothingAlgorithm::EmaPolyfit => "ema-polyfit",
        SmoothingAlgorithm::BiweightPolyfit => "savgol",
    };
    let hint = match o.smoothing {
        SmoothingAlgorithm::None => None,
        SmoothingAlgorithm::TriangularPolyfit => Some(TRIANGULAR_POLYFIT_HINT),
        SmoothingAlgorithm::EmaPolyfit => Some(EMA_POLYFIT_HINT),
        SmoothingAlgorithm::BiweightPolyfit => Some(BIWEIGHT_POLYFIT_HINT),
    };
    let is_ema = matches!(o.smoothing, SmoothingAlgorithm::EmaPolyfit);
    let is_polyfit = matches!(
        o.smoothing,
        SmoothingAlgorithm::TriangularPolyfit
            | SmoothingAlgorithm::EmaPolyfit
            | SmoothingAlgorithm::BiweightPolyfit
    );
    let needs_window = matches!(o.smoothing, SmoothingAlgorithm::BiweightPolyfit);
    let window = o.smoothing_window;
    let poly_order = o.smoothing_poly_order;
    // The UI edits the e-folding time constant, while the draft stores alpha.
    let tc_display = ema_time_constant(o.smoothing_alpha).round() as u32;
    let inherited_smoothing = anchor.smoothing.clone();
    let inherited_order = anchor.smoothing_poly_order;
    let inherited_window = anchor.smoothing_window;
    let inherited_alpha = anchor.smoothing_alpha;
    let set_window = move |event: Event<FormData>| {
        if let Ok(value) = event.value().parse::<u32>() {
            draft.write().smoothing_window = value.clamp(3, BIWEIGHT_WINDOW_MAX);
        }
    };
    let set_time_constant = move |event: Event<FormData>| {
        if let Ok(value) = event.value().parse::<f64>() {
            let value = value.clamp(1.0, 1000.0);
            draft.write().smoothing_alpha = 1.0 - (-1.0 / value).exp();
        }
    };

    rsx! {
        div { class: field_class(o.smoothing != anchor.smoothing),
            ResetDot { show: o.smoothing != anchor.smoothing, onreset: move |_| draft.write().smoothing = inherited_smoothing.clone() }
            label { r#for: "{id_prefix}-smoothing", "Smoothing" }
            select {
                id: "{id_prefix}-smoothing",
                class: "smoothing-select",
                value: "{smoothing_value}",
                onchange: move |e: Event<FormData>| {
                    let s = match e.value().as_str() {
                        "triangular" => SmoothingAlgorithm::TriangularPolyfit,
                        "ema-polyfit" => SmoothingAlgorithm::EmaPolyfit,
                        "savgol" => SmoothingAlgorithm::BiweightPolyfit,
                        _ => SmoothingAlgorithm::None,
                    };
                    let mut d = draft.write();
                    // Normalize a retained legacy/inherited value when the windowed algorithm becomes active.
                    if matches!(s, SmoothingAlgorithm::BiweightPolyfit) {
                        d.smoothing_window = d.smoothing_window.min(BIWEIGHT_WINDOW_MAX);
                    }
                    d.smoothing = s;
                },
                option { value: "none", "None" }
                option { value: "triangular", "Triangular polyfit" }
                option { value: "ema-polyfit", "EMA polyfit" }
                option { value: "savgol", "(1−x²)² polyfit" }
            }
        }
        {chips("smoothing")}
        if is_polyfit {
            div { class: field_class(o.smoothing_poly_order != anchor.smoothing_poly_order),
                ResetDot { show: o.smoothing_poly_order != anchor.smoothing_poly_order, onreset: move |_| draft.write().smoothing_poly_order = inherited_order }
                label { r#for: "{id_prefix}-fit-order", "Fit order" }
                select {
                    id: "{id_prefix}-fit-order",
                    value: "{poly_order}",
                    onchange: move |e: Event<FormData>| {
                        if let Ok(v) = e.value().parse::<u32>() {
                            draft.write().smoothing_poly_order = v.min(2);
                        }
                    },
                    option { value: "0", "0 — average" }
                    option { value: "1", "1 — linear" }
                    option { value: "2", "2 — quadratic" }
                }
            }
            {chips("smoothing_poly_order")}
        }
        if needs_window {
            div { class: field_class(o.smoothing_window != anchor.smoothing_window),
                ResetDot { show: o.smoothing_window != anchor.smoothing_window, onreset: move |_| draft.write().smoothing_window = inherited_window }
                label { id: "{id_prefix}-window-label", r#for: "{id_prefix}-window", "Window" }
                div { class: "slider-input-row",
                    input {
                        aria_labelledby: "{id_prefix}-window-label",
                        r#type: "range",
                        min: "3",
                        max: "{BIWEIGHT_WINDOW_MAX}",
                        value: "{window}",
                        oninput: set_window,
                    }
                    input {
                        id: "{id_prefix}-window",
                        r#type: "number",
                        class: "slider-input-number",
                        min: "3",
                        max: "{BIWEIGHT_WINDOW_MAX}",
                        value: "{window}",
                        oninput: set_window,
                    }
                }
            }
            {chips("smoothing_window")}
        }
        if is_ema {
            div { class: field_class(o.smoothing_alpha != anchor.smoothing_alpha),
                ResetDot { show: o.smoothing_alpha != anchor.smoothing_alpha, onreset: move |_| draft.write().smoothing_alpha = inherited_alpha }
                label { id: "{id_prefix}-time-constant-label", r#for: "{id_prefix}-time-constant", "Time constant (steps)" }
                div { class: "slider-input-row",
                    input {
                        aria_labelledby: "{id_prefix}-time-constant-label",
                        r#type: "range",
                        min: "1",
                        max: "500",
                        value: "{tc_display}",
                        oninput: set_time_constant,
                    }
                    input {
                        id: "{id_prefix}-time-constant",
                        r#type: "number",
                        class: "slider-input-number",
                        min: "1",
                        max: "1000",
                        value: "{tc_display}",
                        oninput: set_time_constant,
                    }
                }
            }
            {chips("smoothing_alpha")}
        }
        if let Some(hint) = hint {
            p { class: "editor-hint smoothing-hover-hint", "{hint}" }
        }
    }
}

/// Axis scales and run cap shared by chart and defaults editors.
#[component]
pub fn AxisFields(
    id_prefix: String,
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    #[props(default)] overrides: Vec<OptionOverride>,
) -> Element {
    let state = use_context::<DashboardState>();
    let chips = |field| override_chips(state, field, &overrides);
    let o = draft.read().clone();
    let inherited_max_runs = anchor.max_runs;
    let log_field = |axis: &str, value: bool, inherited: bool, set: fn(&mut RectOptions, bool)| {
        let input_id = format!("{id_prefix}-log-{}", axis.to_lowercase());
        rsx! {
            div { class: field_class(value != inherited),
                ResetDot {
                    show: value != inherited,
                    onreset: move |_| draft.with_mut(|options| set(options, inherited)),
                }
                label { r#for: "{input_id}", "Log {axis}" }
                input {
                    id: "{input_id}",
                    r#type: "checkbox",
                    checked: value,
                    onchange: move |event: Event<FormData>| {
                        draft.with_mut(|options| set(options, event.checked()));
                    },
                }
            }
        }
    };

    rsx! {
        {log_field("X", o.log_x, anchor.log_x, |options, value| options.log_x = value)}
        {chips("log_x")}
        {log_field("Y", o.log_y, anchor.log_y, |options, value| options.log_y = value)}
        {chips("log_y")}
        div { class: field_class(o.max_runs != anchor.max_runs),
            ResetDot { show: o.max_runs != anchor.max_runs, onreset: move |_| draft.write().max_runs = inherited_max_runs }
            label { r#for: "{id_prefix}-max-runs", "Max runs" }
            input {
                id: "{id_prefix}-max-runs",
                r#type: "number",
                min: "0",
                max: "64",
                value: "{o.max_runs}",
                oninput: move |event: Event<FormData>| {
                    if let Ok(value) = event.value().parse::<u32>() {
                        draft.write().max_runs = clamp_max_runs(value);
                    }
                },
            }
        }
        {chips("max_runs")}
    }
}

/// Project/section chart defaults. `anchor` marks inherited values; live apply stores their diff.
/// `section` scopes finer-level override chips, or is omitted for project defaults.
#[component]
pub fn ChartOptionsForm(
    id_prefix: String,
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    section: Option<String>,
) -> Element {
    let state = use_context::<DashboardState>();
    // Recomputed on every layout write, so a cleared chip disappears as the intent lands.
    let overrides = use_memo(move || {
        let shown = state.layout_config.read();
        let Some(shown) = shown.as_ref() else {
            return Vec::new();
        };
        finer_overrides(&state.peek_diff(), shown, section.as_deref())
    });
    let overrides = overrides();
    rsx! {
        SmoothingFields {
            id_prefix: id_prefix.clone(),
            draft,
            anchor: anchor.clone(),
            overrides: overrides.clone(),
        }
        AxisFields { id_prefix, draft, anchor, overrides }
    }
}

#[cfg(test)]
mod tests {
    use super::clamp_max_runs;

    #[test]
    fn max_runs_editor_preserves_unlimited_and_caps_large_values() {
        assert_eq!(clamp_max_runs(0), 0);
        assert_eq!(clamp_max_runs(1), 1);
        assert_eq!(clamp_max_runs(65), 64);
    }
}
