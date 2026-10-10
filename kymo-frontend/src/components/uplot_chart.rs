mod create_js;

use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use dioxus::prelude::*;
use js_sys::{Array, Float64Array, Object, Reflect};
use wasm_bindgen::JsValue;

use crate::grpc::chart_delta::DenseChart;
use crate::state::UserConfigState;
use create_js::{build_create_js, esc_js, ChartJsConfig};

static CHART_COUNTER: AtomicU32 = AtomicU32::new(0);

// Unsmoothed downsampled runs draw faint min–max envelopes, or one ordinary line if no bucket has spread; bucket means stay data-only.
// Smoothing adds a full-strength values line over faint raw evidence. Passthrough charts draw values directly, plus raw scatter when smoothed.
// Column order is draw order (uPlot strokes series by index): all raw/envelope blocks first, all values lines after. A collapsed unsmoothed line remains in its raw block; the vendored _focus patch lifts the highlighted run's group on top.

fn next_chart_id() -> String {
    let n = CHART_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("uplot-{n}")
}

/// A curated palette of 16 visually distinct colors.
pub const PALETTE: &[&str] = &[
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
    "#9c755f", "#bab0ac", "#af7aa1", "#86bcb6", "#d37295", "#fabfd2", "#b6992d", "#499894",
];

const FALLBACK_COLOR: &str = "#808080";

fn is_hex_color(color: &str) -> bool {
    color.len() == 7
        && color.starts_with('#')
        && color[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn stored_color(storage_key: &str) -> Option<String> {
    let legacy_key = storage_key.replacen("kymo_color_", "mkdb2_color_", 1);
    crate::util::local_storage::get_migrating(storage_key, &legacy_key)
        .filter(|color| is_hex_color(color))
}

fn normalized_hex_color(color: &str) -> &str {
    if is_hex_color(color) {
        color
    } else {
        FALLBACK_COLOR
    }
}

/// Pick a stable color from the palette based on a string key.
pub fn hash_color(key: &str) -> String {
    let mut hash: u32 = 5381;
    for b in key.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    PALETTE[(hash as usize) % PALETTE.len()].to_string()
}

/// Pick a color for a run. User override (keyed on run_id) takes precedence;
/// otherwise color is derived from the run's per-project ordinal so sequential
/// runs are visually distinct and stable across reloads.
pub fn run_color(run_id: &str, ordinal: u64) -> String {
    let storage_key = format!("kymo_color_{}", run_id);
    if let Some(custom) = stored_color(&storage_key) {
        return custom;
    }
    PALETTE[(ordinal as usize) % PALETTE.len()].to_string()
}

fn parse_hex_color(hex: &str) -> (u8, u8, u8) {
    let hex = normalized_hex_color(hex);
    let r = u8::from_str_radix(&hex[1..3], 16).expect("validated hex color");
    let g = u8::from_str_radix(&hex[3..5], 16).expect("validated hex color");
    let b = u8::from_str_radix(&hex[5..7], 16).expect("validated hex color");
    (r, g, b)
}

/// Push columnar data to `window.__kymo_data[id]` as typed arrays — one
/// bulk copy per column across the wasm boundary. Gap slots stay NaN
/// here; the chart-create JS converts series columns to plain arrays
/// with NULL gaps in a single JS-side pass (per-element JsValue pushes
/// from wasm cost a boundary call each — hundreds of thousands per
/// render of a wide chart).
fn push_data_to_js(id: &str, x: &[f64], series: &[Vec<f64>]) {
    let window = web_sys::window().unwrap();
    let key = JsValue::from_str("__kymo_data");
    let data_store = match Reflect::get(&window, &key) {
        Ok(v) if !v.is_undefined() && !v.is_null() => v,
        _ => {
            let obj = Object::new();
            let _ = Reflect::set(&window, &key, &obj);
            obj.into()
        }
    };
    let data_array = Array::new();
    data_array.push(&Float64Array::from(x));
    for s in series {
        data_array.push(&Float64Array::from(&s[..]));
    }
    let _ = Reflect::set(&data_store, &JsValue::from_str(id), &data_array);
}

fn destroy_js(id: &str) -> String {
    let id = esc_js(id);
    format!(
        r#"(()=>{{
if(window.__kymo_ro&&window.__kymo_ro['{id}']){{
  window.__kymo_ro['{id}'].disconnect();
  delete window.__kymo_ro['{id}'];
}}
if(window.__kymo_zg&&window.__kymo_zg.srcId==='{id}'&&window.__kymo_zg.cancel)window.__kymo_zg.cancel();
if(window.__kymo_charts&&window.__kymo_charts['{id}']){{
  // An unmounting hover source gets no mouseleave: clear its cursor through the normal path so synced readouts and the highlight go with it.
  if(window.__kymo_hoversrc==='{id}')window.__kymo_charts['{id}'].setCursor({{left:-10,top:-10}},true,true);
  window.__kymo_charts['{id}'].destroy();
  delete window.__kymo_charts['{id}'];
}}
if(window.__kymo_hoversrc==='{id}')window.__kymo_hoversrc=null;
document.getElementById('{id}-tip')?.remove();
if(window.__kymo_data) delete window.__kymo_data['{id}'];
}})();"#,
        id = id,
    )
}

fn set_data_js(id: &str) -> String {
    format!(
        r#"return(()=>{{
try{{
let u=window.__kymo_charts&&window.__kymo_charts['{id}'];
let data=window.__kymo_data&&window.__kymo_data['{id}'];
if(!u||!u.root||!u.root.isConnected||!data||!data.length)return false;
data=data.map((c,i)=>{{if(i==0)return c;let n=c.length,o=new Array(n);for(let j=0;j<n;j++){{let v=c[j];o[j]=v!==v?null:v;}}return o;}});
u.setData(data);
return true;
}}catch(_error){{
return false;
}}finally{{
dioxus.close();
}}
}})()"#,
        id = esc_js(id)
    )
}

/// The x-axis semantics a chart renders under: scale kind (step / time / custom metric), wall vs relative, and log bucketing.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct ShownAxis {
    pub log_x: bool,
    pub time: bool,
    pub wall: bool,
    /// Custom-x metric name; empty = none.
    pub x_metric: String,
}

impl ShownAxis {
    /// Step-based x: zoom refetches through the step store.
    pub fn step_axis(&self) -> bool {
        !self.time && self.x_metric.is_empty()
    }

    /// Axis label above a copied text table: step, time, or the custom metric's full path.
    pub fn x_label(&self) -> &str {
        if self.time {
            "time"
        } else if !self.x_metric.is_empty() {
            &self.x_metric
        } else {
            "step"
        }
    }
}

/// A chart model, compared by allocation, never by its columns: models are immutable once built, so only a new Rc changes content. Comparing columns would cost real time on fat charts (~10^5 floats) on every parent render and never match across a NaN; a new Rc with equal content costs one redundant data push and setData. Sound only while the comparing side holds the previous model alive, as UPlotChart's props and use_reactive do, so a new model can never reuse its address.
#[derive(Clone)]
pub struct ChartModel(pub Rc<DenseChart>);

impl PartialEq for ChartModel {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

#[component]
pub fn UPlotChart(
    /// The response as received, time axes still in ms (converted below).
    chart: ChartModel,
    /// Display labels and hex colors, one per `chart.series`.
    labels: Vec<String>,
    colors: Vec<String>,
    /// Exact owning run names, separate from decorated metric/tag labels.
    /// None means the run's metadata is not available for name grouping.
    run_names: Vec<Option<String>>,
    axis: ShownAxis,
    log_y: bool,
    /// From the request options: stroke smoothed values over raw evidence. Unsmoothed envelope charts render min/max data instead of bucket means.
    smoothed: bool,
) -> Element {
    let user_config = use_context::<UserConfigState>();
    let chart_id = use_hook(next_chart_id);

    // The structural config last sent for this chart: an equal config sends only uPlot.setData, skipping the create script (see build_create_js).
    let mut last_create_config = use_signal(|| Option::<ChartJsConfig>::None);

    // Push every new data model; rebuild the uPlot instance only when its structural config changes. use_reactive reruns the effect when any prop differs and holds the last props, keeping the previous chart alive (see ChartModel).
    use_effect(use_reactive(
        (
            &chart, &labels, &colors, &run_names, &axis, &log_y, &smoothed,
        ),
        {
            let id = chart_id.clone();
            move |(ChartModel(chart), labels, colors, run_names, axis, log_y, smoothed)| {
                let log_x = axis.log_x;
                let font_size = user_config.font_size();

                // ONE shared axis in chart.x_values; every series column aligns to it index-for-index (NaN where a run has no data in a slot).
                if chart.series.is_empty() || chart.x_values.is_empty() {
                    return;
                }
                // Zero-present log x on a step or time axis renders log(x+1) (ChartJsConfig::log_shift), the rule the server's bucket ladder applies to the same model, so x = 0 stays on the chart. Zero-less charts keep plain log(x); custom-x never shifts.
                let log_shift =
                    log_x && axis.x_metric.is_empty() && chart.x_values.first() == Some(&0.0);
                // Time axes render seconds.
                let unit = if axis.time { 1000.0 } else { 1.0 };
                let shift = if log_shift { 1.0 } else { 0.0 };
                let mut x_values: Vec<f64> =
                    chart.x_values.iter().map(|&x| (x + shift) / unit).collect();
                let n_axis = x_values.len();

                let has_range = chart.series.iter().any(|s| !s.min_values.is_empty());
                let has_raw = chart.series.iter().any(|s| !s.raw_values.is_empty());

                // Columns arrive axis-length from the server; the resize is a shape guard (pads short with NaN, truncates long), not a join.
                let col = |src: &[f64]| -> Vec<f64> {
                    let mut c = src.to_vec();
                    c.resize(n_axis, f64::NAN);
                    c
                };
                // Envelope columns ship as wire truth, including mins <= 0 on log-y charts: those CLIP at the bottom border instead of gapping (uPlot's log scale clamps non-positive values a decade under the scale min, so the band visibly runs into the border), yNiceLog keeps them from driving the y-range, and the tooltip prints the real number. The wire never carries a non-finite value other than NaN; logged ±inf render as marker circles at the border they exceed.
                // Raw/envelope blocks first, values lines after, matching build_create_js's series opts (see the top-of-file comment).
                let mut all_series = Vec::new();
                // Slots where the log axis clips a run's lowest drawn evidence get a synthesized kind-5 border circle below.
                let mut synth_marks: Vec<Vec<u32>> = Vec::with_capacity(chart.series.len());
                for s in chart.series.iter() {
                    let samples = if s.raw_values.len() == s.values.len() {
                        &s.raw_values
                    } else {
                        &s.values
                    };
                    if has_raw {
                        all_series.push(col(samples));
                    }
                    // The run's lowest drawn evidence: its envelope min, else its samples.
                    let mut floor = samples;
                    if has_range {
                        // A missing envelope on an enveloped chart means the run has no finite sample at all (the server ships envelopes dense, never sparse or all-NaN): NaN columns, nothing to band.
                        if s.min_values.len() == s.values.len()
                            && s.max_values.len() == s.values.len()
                        {
                            all_series.push(col(&s.min_values));
                            all_series.push(col(&s.max_values));
                            floor = &s.min_values;
                        } else {
                            all_series.push(vec![f64::NAN; n_axis]);
                            all_series.push(vec![f64::NAN; n_axis]);
                        }
                    }
                    // NaN gap slots fail the comparison and stay unmarked.
                    synth_marks.push(if log_y {
                        (0..floor.len())
                            .filter(|&i| floor[i] <= 0.0)
                            .map(|i| i as u32)
                            .collect()
                    } else {
                        Vec::new()
                    });
                }
                for s in chart.series.iter() {
                    all_series.push(col(&s.values));
                }

                // Append one NaN-marker column per real series, marking where the run LOGGED a non-finite value (server-computed nan_indices, indexing the shared axis) or where the log axis clipped its envelope or sample. Only when a marker exists somewhere, so healthy charts pay nothing. The value is the marker KIND (1 NaN, 2 +inf, 3 -inf, 4 unplottable x, 5 log-clipped envelope or sample — frontend-only, never on the wire): markers live on a dummy scale and the draw hook pins their circles to the plot's border (top for +inf, bottom for the rest), so the kind value never plots.
                let needs_markers = chart.series.iter().any(|s| !s.nan_indices.is_empty())
                    || synth_marks.iter().any(|v| !v.is_empty());
                let nan_markers = if needs_markers {
                    for (si, s) in chart.series.iter().enumerate() {
                        let mut marker = vec![f64::NAN; n_axis];
                        for &i in &synth_marks[si] {
                            if let Some(slot) = marker.get_mut(i as usize) {
                                *slot = 5.0;
                            }
                        }
                        // A server marker at the same slot says strictly more than the synthesized clip circle and wins.
                        for (mi, &i) in s.nan_indices.iter().enumerate() {
                            if let Some(slot) = marker.get_mut(i as usize) {
                                *slot = s.nan_kinds.get(mi).copied().unwrap_or(1) as f64;
                            }
                        }
                        all_series.push(marker);
                    }
                    chart.series.len()
                } else {
                    0
                };

                // Two trailing data-only columns (no series entries — uPlot never touches data past its series list): the chart-level bucket x extents, in the axis's units (seconds on a time axis), read by the tooltip's x-range header and whole-bucket zoom. They hold REAL x: no log shift.
                let has_xrange = chart.xr_min.iter().any(|v| !v.is_nan());
                if has_xrange {
                    for extent in [&chart.xr_min, &chart.xr_max] {
                        all_series.push(col(extent).into_iter().map(|x| x / unit).collect());
                    }
                }

                // Custom-x's nonpositive x (exceptional markers server-side) trims defensively.
                if log_x && x_values[0] <= 0.0 {
                    let start = x_values
                        .iter()
                        .position(|&s| s > 0.0)
                        .unwrap_or(x_values.len());
                    x_values = x_values[start..].to_vec();
                    all_series = all_series.iter().map(|s| s[start..].to_vec()).collect();
                }

                push_data_to_js(&id, &x_values, &all_series);

                // Per run: envelope has area somewhere (see ChartJsConfig::banded). Straight off the wire columns: NaN slots and absent envelopes fail the < and count as degenerate.
                let banded: Vec<bool> = chart
                    .series
                    .iter()
                    .map(|s| {
                        s.min_values
                            .iter()
                            .zip(s.max_values.iter())
                            .any(|(&a, &b)| a < b)
                    })
                    .collect();
                let cfg = ChartJsConfig {
                    labels,
                    colors,
                    run_ids: chart.series.iter().map(|s| s.run_id.clone()).collect(),
                    run_names,
                    has_range,
                    banded,
                    has_raw,
                    smoothed,
                    has_xrange,
                    nan_markers,
                    xnan_counts: chart.series.iter().map(|s| s.xnan_count).collect(),
                    axis,
                    log_shift,
                    log_y,
                    font_size,
                };
                // Structure-unchanged refreshes swap the arrays in place:
                // live runs tick on every pushed update, and a full destroy/
                // recreate per tick reset cursor and tooltip mid-hover. setData
                // re-ranges x per the scale's auto policy: full extent normally
                // (zoom-refetch charts carry the zoom window in the data
                // itself), current window while a client-side zoom is active
                // (__kymo_userzoom) — either way it commits, so new points
                // paint without wiping the zoom.
                let structure_unchanged = last_create_config.peek().as_ref() == Some(&cfg);
                if !structure_unchanged {
                    last_create_config.set(Some(cfg.clone()));
                }
                let id = id.clone();
                spawn(async move {
                    // The chart can be missing (an earlier create failed, the DOM detached) or setData can reject malformed browser state: fall through to a full create.
                    if structure_unchanged
                        && matches!(
                            document::eval(&set_data_js(&id)).join::<bool>().await,
                            Ok(true)
                        )
                    {
                        return;
                    }
                    let failure = match document::eval(&build_create_js(&id, &cfg))
                        .join::<String>()
                        .await
                    {
                        Ok(message) if message.is_empty() => return,
                        Ok(message) => message,
                        Err(error) => error.to_string(),
                    };
                    crate::util::warn(&format!("[chart:{id}] uPlot create failed: {failure}"));
                });
            }
        },
    ));

    use_drop({
        let id = chart_id.clone();
        move || {
            let js = destroy_js(&id);
            // spawn() would park this on the scope being torn down: dioxus
            // drains the dying scope's tasks BEFORE dropping hooks, so a
            // task spawned from a drop hook is never polled and the eval
            // never runs. spawn_forever parks it on the root scope, which
            // exists precisely for post-unmount work like this.
            dioxus::core::spawn_forever(async move {
                let _ = document::eval(&js).await;
            });
        }
    });

    rsx! {
        // Sized by CSS (`--kymo-chart-height`, set by the section or the maximize overlay); create.js and its ResizeObserver follow the box.
        div { id: "{chart_id}", class: "chart-container" }
    }
}

/// Invisible page half of the zoom protocol: ONE document-level listener funnels every chart's bubbling `kymo-zoom` dispatch into the shared step store — zoom is a page-global concern (one store, every step chart), so it gets one listener, not one per chart. Attaching to `document` makes the dioxus pre-flush task-poll hazard structurally impossible (no element whose render timing matters), which is why this replaced per-element listeners; use_future's any-time first poll is therefore fine. Mount once per dashboard, next to ZoneBridge.
///
/// `js_bridge` synchronously evicts a predecessor and owner-fences the Rust-side drop, so route transitions cannot leave an orphan or let an old scope remove its successor.
const ZOOM_BRIDGE_JS: &str = r#"(()=>{
function h(e){try{dioxus.send(e.detail);}catch(_){td();}}
function cleanup(){document.removeEventListener('kymo-zoom',h);}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup);
document.addEventListener('kymo-zoom',h);
})()"#;

#[component]
pub fn ZoomBridge() -> Element {
    let state = use_context::<crate::state::DashboardState>();
    let bridge = crate::util::js_bridge::use_bridge("zoom");
    use_future(move || {
        let js = bridge.script(ZOOM_BRIDGE_JS);
        async move {
            let mut eval = document::eval(&js);
            loop {
                match eval.recv::<serde_json::Value>().await {
                    Ok(val) => {
                        let xmin = val.get("xmin").and_then(|v| v.as_f64());
                        let xmax = val.get("xmax").and_then(|v| v.as_f64());
                        // The physical gesture source dispatches one real-x range. Quantize it outward to inclusive whole steps; the peek gate keeps an idempotent whole-bucket selection from refetching.
                        let z = xmin
                            .zip(xmax)
                            .map(|(a, b)| (a.floor() as i64, b.ceil() as i64));
                        let mut store = state.step_zoom;
                        if *store.peek() != z {
                            store.set(z);
                        }
                    }
                    Err(_) => {
                        // Structurally near-impossible while the listener is attached (it pins the channel); if it ever fires, zoom is dead until the next dashboard mount — say so.
                        crate::util::warn("[zoom] bridge channel lost");
                        break;
                    }
                }
            }
        }
    });
    rsx! {}
}

#[cfg(test)]
mod bridge_template_tests {
    #[test]
    fn zoom_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::ZOOM_BRIDGE_JS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chart_destroy_clears_every_template_owned_per_chart_global() {
        const ID: &str = "__KYMO_ID__";
        const ASSIGNMENT: &str = "['__KYMO_ID__']=";
        let create_template = include_str!("uplot_chart/create.js");
        let destroy = destroy_js(ID);
        let template_owned: Vec<&str> = create_template
            .lines()
            .filter_map(|line| {
                line.trim_start()
                    .strip_prefix("window.__kymo_")?
                    .split_once(ASSIGNMENT)
                    .map(|(registry, _)| registry)
            })
            .collect();

        assert!(
            !template_owned.is_empty(),
            "per-chart assignment scan became vacuous"
        );
        for registry in template_owned {
            assert!(
                destroy.contains(&format!("delete window.__kymo_{registry}['{ID}']")),
                "destroy is missing the create.js-owned {registry} registry"
            );
        }
        // The readout lives outside the chart, under <body> (create.js), so teardown removes it by id.
        assert!(destroy.contains("document.getElementById('__KYMO_ID__-tip')?.remove();"));
        // Data is populated through Reflect in Rust rather than an assignment in create.js, so it remains an explicit part of the teardown contract.
        assert!(destroy.contains("delete window.__kymo_data['__KYMO_ID__']"));
        assert!(destroy.contains("window.__kymo_zg.srcId==='__KYMO_ID__'"));
        assert!(destroy.contains("window.__kymo_zg.cancel()"));
        // An unmounting hover source publishes its clear before destroy, so synced readouts don't outlive it.
        assert!(destroy.contains(
            "if(window.__kymo_hoversrc==='__KYMO_ID__')window.__kymo_charts['__KYMO_ID__'].setCursor({left:-10,top:-10},true,true);\n  window.__kymo_charts['__KYMO_ID__'].destroy();"
        ));
    }

    #[test]
    fn malformed_colors_use_the_safe_fallback() {
        assert_eq!(normalized_hex_color("#123aBC"), "#123aBC");
        assert_eq!(normalized_hex_color("red';alert(1)//"), FALLBACK_COLOR);
        assert_eq!(parse_hex_color("not-a-color"), (128, 128, 128));
    }

    #[test]
    fn set_data_reports_failures_and_always_closes_its_eval_channel() {
        let js = set_data_js("chart'7");

        assert!(js.starts_with("return(()=>{"));
        assert!(js.contains("window.__kymo_charts['chart\\'7']"));
        assert!(js.contains("!u.root||!u.root.isConnected"));
        assert!(js.contains("catch(_error)"));
        assert!(js.contains("finally{"));
        assert!(js.contains("dioxus.close();"));
        assert!(!js.contains("dioxus.send"));
    }
}
