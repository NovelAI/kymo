use std::time::Duration;

use dioxus::core::{needs_update, Task};
use dioxus::prelude::*;
use gloo_timers::future::sleep;
use serde_json::Value;

use crate::components::icons::{CaretDownIcon, CaretRightIcon, CloseIcon};
use crate::components::options_panel::OptionsPanel;
use crate::state::layout_config::{
    ema_time_constant, finer_overrides, OptionOverride, OverrideTarget, RectOptions,
    SmoothingAlgorithm,
};
use crate::state::DashboardState;
use crate::util::{primary, use_live_apply};

/// Field-id prefix for every options editor; one panel is open at a time.
pub const OPTIONS_ID_PREFIX: &str = "kymo-options";

/// A field's row, marked when `overridden`, with a dot that resets it to its inherited value.
#[component]
fn Field(overridden: bool, onreset: EventHandler<()>, children: Element) -> Element {
    rsx! {
        div { class: if overridden { "binding-field field-overridden" } else { "binding-field" },
            if overridden {
                button {
                    class: "field-reset-dot",
                    title: "Reset to inherited value",
                    onmousedown: primary(move |_| onreset.call(())),
                    "●"
                }
            }
            {children}
        }
    }
}

const TRIANGULAR_POLYFIT_HINT: &str = "Polynomial fit where weight ~ (step + 1). Causal.";

const EMA_POLYFIT_HINT: &str =
    "Local polynomial fit on (causal) EMA. Order 0 is typical EMA smoothing.";

const BIWEIGHT_POLYFIT_HINT: &str =
    "Local polynomial fit in a symmetric window. Only compare runs when they have the same x values in the window. For example, do not use this to compare the last point of an incomplete run to other runs; use an EMA instead.\nTechnical details: This is Savitzky-Golay on a biweight window, which approximates a Gaussian. Higher powers are closer to Gaussian, and we chose the second power.";
/// UI cap for the biweight-polyfit smoothing window and the EMA time constant (the server clamps windows at 4096).
const SMOOTHING_MAX: u32 = 2000;
/// The smallest biweight-polyfit window; the server floors smaller ones to it.
const BIWEIGHT_WINDOW_MIN: u32 = 3;
/// The smallest EMA time constant.
const TIME_CONSTANT_MIN: u32 = 1;
/// Steps on the exponential smoothing sliders, whose positions run 0..=SLIDER_STEPS: each step moves the value by the same ratio.
const SLIDER_STEPS: u32 = 100;

/// The value at `position` on an exponential slider from `min` to [`SMOOTHING_MAX`].
fn slider_value(min: u32, position: u32) -> u32 {
    let ratio = f64::from(SMOOTHING_MAX) / f64::from(min);
    let fraction = f64::from(position) / f64::from(SLIDER_STEPS);
    (f64::from(min) * ratio.powf(fraction)).round() as u32
}

/// The position nearest `value` on an exponential slider from `min`; values outside the slider's range, or within half a step of one end (window 1937 and up, time constant 1926 and up), sit at that end, where its Home or End key writes nothing.
fn slider_position(min: u32, value: f64) -> u32 {
    let ratio = f64::from(SMOOTHING_MAX) / f64::from(min);
    let position = (value / f64::from(min)).log(ratio) * f64::from(SLIDER_STEPS);
    position.round().clamp(0.0, f64::from(SLIDER_STEPS)) as u32
}

/// Where a slider from `min` draws `value`, and the value its number box shows: `thumb` and the value it reads while a drag's rest is `pending` or that value is `value` rounded, else the position nearest `value` and `value` rounded.
fn display(min: u32, thumb: Option<u32>, pending: bool, value: f64) -> (u32, u32) {
    let shown = value.round() as u32;
    match thumb.map(|thumb| (thumb, slider_value(min, thumb))) {
        Some((thumb, reads)) if pending || reads == shown => (thumb, reads),
        _ => (slider_position(min, value), shown),
    }
}

/// How long a mouse drag rests before its slider writes. Each write re-renders the dashboard and starts full chart builds the server never cancels (measured in `docs/chart-repaint-performance.md`, 2026-10-28), so a drag writes only when it rests or lets go, while arrow keys and typed values write at once. A drag the panel closes under (Escape under a held button) loses its moves since it last rested.
const SLIDER_REST: Duration = Duration::from_millis(250);

/// An exponential slider's state across renders.
#[derive(Clone, Copy)]
struct ExpSlider {
    min: u32,
    /// Where the thumb last moved. Every re-render rewrites the volatile `value`, and neighboring low-end positions round to one value: drawn at the value's nearest position, the thumb would jump under the pointer within such a run, and arrow keys could never leave it.
    thumb: Signal<Option<u32>>,
    /// The timer writing a drag's position once it rests; the thumb's value shows meanwhile.
    rest: Signal<Option<Task>>,
}

fn use_exp_slider(min: u32) -> ExpSlider {
    ExpSlider {
        min,
        thumb: use_signal(|| None),
        rest: use_signal(|| None),
    }
}

impl ExpSlider {
    /// `input`: move the thumb, and write its value once it rests.
    fn slide(mut self, event: Event<FormData>, mut write: impl FnMut(u32) + 'static) {
        let Ok(position) = event.value().parse() else {
            return;
        };
        self.thumb.set(Some(position));
        if let Some(task) = self.rest.take() {
            task.cancel();
        }
        let value = slider_value(self.min, position);
        self.rest.set(Some(spawn(async move {
            sleep(SLIDER_REST).await;
            self.rest.set(None);
            write(value);
        })));
    }

    /// `change` (a release, or each arrow key after its `input`) and `pointerup` (a release back where the drag started fires no `change`): write a pending rest now, so arrow keys write at once and no rest can undo a Reset or Revert made just after a release.
    fn flush(mut self, mut write: impl FnMut(u32)) {
        if let (Some(task), Some(position)) = (self.rest.take(), *self.thumb.peek()) {
            task.cancel();
            write(slider_value(self.min, position));
        }
    }

    /// The label, slider and number box editing `value`, with ids from `name`. The box writes typed whole values at once, capped at [`SMOOTHING_MAX`], and leaves those below the minimum for its commit to raise, since each write re-renders it: raised on its first digit, a typed 1500 would end as 2000, and a typed 0150 as 1150.
    fn inputs(
        self,
        name: &str,
        text: &str,
        value: f64,
        mut write: impl FnMut(u32) + Copy + 'static,
    ) -> Element {
        let pending = self.rest.read().is_some();
        let (position, shown) = display(self.min, *self.thumb.read(), pending, value);
        let min = self.min;
        rsx! {
            label { id: "{OPTIONS_ID_PREFIX}-{name}-label", r#for: "{OPTIONS_ID_PREFIX}-{name}", "{text}" }
            div { class: "slider-input-row",
                input {
                    aria_labelledby: "{OPTIONS_ID_PREFIX}-{name}-label",
                    aria_valuetext: "{shown}",
                    r#type: "range",
                    min: "0",
                    max: "{SLIDER_STEPS}",
                    value: "{position}",
                    oninput: move |event| self.slide(event, write),
                    onchange: move |_| self.flush(write),
                    onpointerup: move |_| self.flush(write),
                }
                input {
                    id: "{OPTIONS_ID_PREFIX}-{name}",
                    r#type: "number",
                    class: "slider-input-number",
                    min: "{min}",
                    max: "{SMOOTHING_MAX}",
                    value: "{shown}",
                    oninput: move |event: Event<FormData>| match event.value().parse::<u32>() {
                        Ok(value) if value >= min => write(value.min(SMOOTHING_MAX)),
                        _ => {}
                    },
                    // A commit rounds and clamps what typing couldn't take (2.5, -5, 1e4) and redraws a box left empty, so the box never keeps a value the chart doesn't use.
                    onchange: move |event: Event<FormData>| match event.value().parse::<f64>() {
                        Ok(value) => write(value.round().clamp(f64::from(min), f64::from(SMOOTHING_MAX)) as u32),
                        Err(_) => needs_update(),
                    },
                }
            }
        }
    }
}

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

/// The finer-level pins a defaults editor lists under its fields, and the record of those it has cleared, which its Revert puts back.
#[derive(Clone, Copy, PartialEq)]
pub struct OverrideChips {
    pins: Memo<Vec<OptionOverride>>,
    cleared: Signal<Vec<OptionOverride>>,
}

/// The chips listing `field`'s finer-level pins; clicking one clears it so the field inherits again.
fn override_chips(
    state: DashboardState,
    field: &'static str,
    chips: Option<OverrideChips>,
) -> Element {
    let Some(chips) = chips else {
        return rsx! {};
    };
    let mut cleared = chips.cleared;
    let hits: Vec<OptionOverride> = chips
        .pins
        .read()
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
                            onmousedown: primary(move |_| {
                                state.clear_option_pin(&o);
                                cleared.write().push(o.clone());
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

/// Edits project chart defaults live, as a sparse patch over the library defaults, written field by field (`OptionsBaseline::write_fields`).
#[component]
pub fn ProjectDefaultsEditor(return_focus_id: String) -> Element {
    let state = use_context::<DashboardState>();
    let baseline = use_hook(|| state.chart_defaults_baseline(None));
    let mut draft = use_signal(|| baseline.opened.clone());
    let mut cleared = use_signal(Vec::new);
    use_live_apply(move || draft.read().clone(), {
        let baseline = baseline.clone();
        move |previous: &RectOptions, opts: RectOptions| {
            state.edit_project_chart_defaults(|stored| {
                baseline.write_fields(stored, previous, &opts)
            });
        }
    });
    let unchanged = *draft.read() == baseline.opened && cleared.read().is_empty();

    rsx! {
        OptionsPanel {
            title: "Project settings",
            return_focus_ids: vec![return_focus_id],
            revert_disabled: unchanged,
            on_revert: {
                let opened = baseline.opened.clone();
                move |_| {
                    draft.set(opened.clone());
                    state.restore_overrides(&std::mem::take(&mut *cleared.write()));
                }
            },
            on_close: move |_| state.close_options_panel(),

            div { class: "editor-options",
                ChartOptionsForm { draft, anchor: baseline.anchor.clone(), cleared }
            }
        }
    }
}

/// Collapsible editor content, open until the user collapses it.
#[component]
pub fn EditorSection(title: String, children: Element) -> Element {
    let mut open = use_signal(|| true);
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

/// Smoothing controls shared by chart and defaults editors; optional chips expose finer-level overrides.
#[component]
pub fn SmoothingFields(
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    #[props(default)] chips: Option<OverrideChips>,
) -> Element {
    let state = use_context::<DashboardState>();
    let chips_for = |field| override_chips(state, field, chips);
    let o = draft.read().clone();
    let (smoothing_value, hint) = match o.smoothing {
        SmoothingAlgorithm::None => ("none", None),
        SmoothingAlgorithm::TriangularPolyfit => ("triangular", Some(TRIANGULAR_POLYFIT_HINT)),
        SmoothingAlgorithm::EmaPolyfit => ("ema-polyfit", Some(EMA_POLYFIT_HINT)),
        SmoothingAlgorithm::BiweightPolyfit => ("savgol", Some(BIWEIGHT_POLYFIT_HINT)),
    };
    let window_slider = use_exp_slider(BIWEIGHT_WINDOW_MIN);
    let tc_slider = use_exp_slider(TIME_CONSTANT_MIN);
    let write_window = move |value: u32| draft.write().smoothing_window = value;
    // The UI edits the e-folding time constant, while the draft stores alpha.
    let write_time_constant = move |value: u32| {
        draft.write().smoothing_alpha = 1.0 - (-1.0 / f64::from(value)).exp();
    };

    rsx! {
        Field { overridden: o.smoothing != anchor.smoothing, onreset: move |_| draft.write().smoothing = anchor.smoothing,
            label { r#for: "{OPTIONS_ID_PREFIX}-smoothing", "Smoothing" }
            select {
                id: "{OPTIONS_ID_PREFIX}-smoothing",
                class: "smoothing-select",
                value: "{smoothing_value}",
                onchange: move |e: Event<FormData>| {
                    draft.write().smoothing = match e.value().as_str() {
                        "triangular" => SmoothingAlgorithm::TriangularPolyfit,
                        "ema-polyfit" => SmoothingAlgorithm::EmaPolyfit,
                        "savgol" => SmoothingAlgorithm::BiweightPolyfit,
                        _ => SmoothingAlgorithm::None,
                    };
                },
                option { value: "none", "None" }
                option { value: "triangular", "Triangular polyfit" }
                option { value: "ema-polyfit", "EMA polyfit" }
                option { value: "savgol", "(1−x²)² polyfit" }
            }
        }
        {chips_for("smoothing")}
        if o.smoothing != SmoothingAlgorithm::None {
            Field { overridden: o.smoothing_poly_order != anchor.smoothing_poly_order, onreset: move |_| draft.write().smoothing_poly_order = anchor.smoothing_poly_order,
                label { r#for: "{OPTIONS_ID_PREFIX}-fit-order", "Fit order" }
                select {
                    id: "{OPTIONS_ID_PREFIX}-fit-order",
                    value: "{o.smoothing_poly_order}",
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
            {chips_for("smoothing_poly_order")}
        }
        if o.smoothing == SmoothingAlgorithm::BiweightPolyfit {
            Field { overridden: o.smoothing_window != anchor.smoothing_window, onreset: move |_| draft.write().smoothing_window = anchor.smoothing_window,
                {window_slider.inputs("window", "Window", f64::from(o.smoothing_window), write_window)}
            }
            {chips_for("smoothing_window")}
        }
        if o.smoothing == SmoothingAlgorithm::EmaPolyfit {
            Field { overridden: o.smoothing_alpha != anchor.smoothing_alpha, onreset: move |_| draft.write().smoothing_alpha = anchor.smoothing_alpha,
                {tc_slider.inputs("time-constant", "Time constant (steps)", ema_time_constant(o.smoothing_alpha), write_time_constant)}
            }
            {chips_for("smoothing_alpha")}
        }
        if let Some(hint) = hint {
            p { class: "smoothing-hover-hint", "{hint}" }
        }
    }
}

/// Axis scales and run cap shared by chart and defaults editors.
#[component]
pub fn AxisFields(
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    #[props(default)] chips: Option<OverrideChips>,
) -> Element {
    let state = use_context::<DashboardState>();
    let chips_for = |field| override_chips(state, field, chips);
    let o = draft.read().clone();
    let log_field = |axis: &str, value: bool, inherited: bool, set: fn(&mut RectOptions, bool)| {
        let input_id = format!("{OPTIONS_ID_PREFIX}-log-{}", axis.to_lowercase());
        rsx! {
            Field { overridden: value != inherited, onreset: move |_| draft.with_mut(|options| set(options, inherited)),
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
        {chips_for("log_x")}
        {log_field("Y", o.log_y, anchor.log_y, |options, value| options.log_y = value)}
        {chips_for("log_y")}
        Field { overridden: o.max_runs != anchor.max_runs, onreset: move |_| draft.write().max_runs = anchor.max_runs,
            label { r#for: "{OPTIONS_ID_PREFIX}-max-runs", "Max runs" }
            input {
                id: "{OPTIONS_ID_PREFIX}-max-runs",
                r#type: "number",
                min: "0",
                max: "64",
                value: "{o.max_runs}",
                oninput: move |event: Event<FormData>| {
                    if let Ok(value) = event.value().parse::<u32>() {
                        draft.write().max_runs = value.min(64);
                    }
                },
            }
        }
        {chips_for("max_runs")}
    }
}

/// Project/section chart defaults. `anchor` marks inherited values; live apply stores them as a sparse patch over it.
/// `section` scopes finer-level override chips, or is omitted for project defaults; `cleared` records the chips clicked.
#[component]
pub fn ChartOptionsForm(
    draft: Signal<RectOptions>,
    anchor: RectOptions,
    section: Option<String>,
    cleared: Signal<Vec<OptionOverride>>,
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
    let chips = OverrideChips {
        pins: overrides,
        cleared,
    };
    rsx! {
        SmoothingFields { draft, anchor: anchor.clone(), chips }
        AxisFields { draft, anchor, chips }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoothing_sliders_are_exponential_from_their_minimum_to_the_cap() {
        for min in [TIME_CONSTANT_MIN, BIWEIGHT_WINDOW_MIN] {
            assert_eq!(slider_value(min, 0), min);
            assert_eq!(slider_value(min, SLIDER_STEPS), SMOOTHING_MAX);
            for position in 0..SLIDER_STEPS {
                assert!(slider_value(min, position) <= slider_value(min, position + 1));
            }
        }
        // Halfway is the geometric mean of the ends.
        assert_eq!(slider_value(TIME_CONSTANT_MIN, SLIDER_STEPS / 2), 45);
        assert_eq!(slider_value(BIWEIGHT_WINDOW_MIN, SLIDER_STEPS / 2), 77);
    }

    #[test]
    fn a_slider_value_sits_at_a_position_showing_it() {
        for min in [TIME_CONSTANT_MIN, BIWEIGHT_WINDOW_MIN] {
            for position in 0..=SLIDER_STEPS {
                let value = slider_value(min, position);
                let shown = slider_position(min, f64::from(value));
                assert_eq!(
                    slider_value(min, shown),
                    value,
                    "min {min}, position {position}"
                );
            }
            assert_eq!(slider_position(min, 0.0), 0);
            assert_eq!(slider_position(min, 4096.0), SLIDER_STEPS);
        }
    }

    #[test]
    fn the_thumb_stays_where_it_moved_while_it_reads_the_value() {
        // Positions 0-2 all read window 3, so an arrow press from 1 to 2 must leave the thumb at 2.
        assert_eq!(display(BIWEIGHT_WINDOW_MIN, Some(2), false, 3.0), (2, 3));
        // A typed 4 moves it to 4's own position.
        assert_eq!(display(BIWEIGHT_WINDOW_MIN, Some(2), false, 4.0), (4, 4));
        // A time constant of 1.4, shown as 1: the thumb a slide left reading 1 stays; untouched, it sits at 1.4's position, so Home still moves it and writes 1.
        assert_eq!(display(TIME_CONSTANT_MIN, Some(2), false, 1.4), (2, 1));
        assert_eq!(display(TIME_CONSTANT_MIN, None, false, 1.4), (4, 1));
        // A drag shows its own position and value until its rest writes them.
        assert_eq!(display(BIWEIGHT_WINDOW_MIN, Some(50), true, 3.0), (50, 77));
    }
}
