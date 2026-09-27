use super::{normalized_hex_color, parse_hex_color};
use crate::state::FontSize;

const TEMPLATE: &str = include_str!("create.js");
const HOVER: &str = include_str!("hover.js");
const ZOOM_MATH: &str = include_str!("zoom_math.js");
const TOKEN_PREFIX: &str = "__KYMO_";

#[derive(Clone, Hash, PartialEq, Eq)]
pub(super) struct ChartJsConfig {
    pub(super) labels: Vec<String>,
    /// Pre-computed hex colors per series.
    pub(super) colors: Vec<String>,
    /// Owning run per series — keys the cross-chart run highlight.
    pub(super) run_ids: Vec<String>,
    /// Exact run names for grouping resumed runs; missing metadata stays None.
    pub(super) run_names: Vec<Option<String>>,
    pub(super) has_range: bool,
    /// Per run: min < max at some slot. Skip zero-area bands because Firefox rasterizes a clip mask per band on each redraw. Including this in the config rebuilds when spread changes.
    pub(super) banded: Vec<bool>,
    pub(super) has_raw: bool,
    /// Smoothing is on: the `values` column is a smoothed curve rather than a bucket mean. From the request options, NOT raw_values presence — downsampled smoothed charts ship no raw column.
    pub(super) smoothed: bool,
    /// Two trailing data-only columns (after the NaN-marker columns) hold
    /// the x extent of the raw points behind each union slot, unioned
    /// across series. No series entries — uPlot ignores data columns past
    /// its series list — they exist for the tooltip's x readout only.
    pub(super) has_xrange: bool,
    /// Number of trailing "NaN marker" columns appended after all real series
    /// columns (one per real series, same order/colors). Each marks where the
    /// run logged a non-finite value with a hollow circle pinned to the
    /// bottom of the chart, like wandb.
    pub(super) nan_markers: usize,
    /// Per series: how many samples sit behind its kind-4 (unplottable-x)
    /// markers — the tooltip appends "×N" on those rows when N > 1.
    pub(super) xnan_counts: Vec<u32>,
    pub(super) log_x: bool,
    /// Step-axis log charts render in log(x+1), matching the server's bucket ladder: the data is shifted +1 before uPlot, and the splits/formatters/zoom dispatches convert back to real steps.
    pub(super) log_shift: bool,
    /// Tooltip and copy readouts undo the +0.001s used to render zero-present log-time charts.
    pub(super) time_log_shift: bool,
    pub(super) log_y: bool,
    pub(super) height: u32,
    /// Canvas font/gutter input included in the structural config.
    pub(super) font_size: FontSize,
    /// Whether zoom should refetch (step-based X) or be client-side only
    pub(super) zoom_refetch: bool,
    /// X-axis shows time values (seconds for relative, epoch for wall)
    pub(super) is_time_axis: bool,
    /// Wall-clock time (format as HH:MM:SS) vs relative (format as elapsed)
    pub(super) is_wall_time: bool,
    /// Cursor-sync group: charts sharing an x-axis kind (step / rel-time /
    /// wall-time / same custom metric) mirror each other's crosshair and
    /// show compact tooltips at the synced x.
    pub(super) sync_key: String,
    /// Human-readable x-axis label shown above the copied text table.
    pub(super) x_label: String,
}

/// Escape the contents of a single-quoted JS literal; unlike `util::js_bridge::js_string`, this does not add the surrounding quotes. Besides the
/// quote and backslash, newlines and the JS line separators U+2028/U+2029
/// are syntax errors inside a literal and would kill the whole create
/// script if a label ever contained one.
pub(super) fn esc_js(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn esc_html_text(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

fn render_template(replacements: &[(&str, String)]) -> String {
    let template = TEMPLATE.strip_suffix('\n').unwrap_or(TEMPLATE);
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    let mut used = vec![false; replacements.len()];

    while let Some(start) = rest.find(TOKEN_PREFIX) {
        rendered.push_str(&rest[..start]);
        let token_and_rest = &rest[start..];
        let offset = template.len() - rest.len() + start;
        // Line numbers exist only for malformed-template diagnostics; keep the ordinary render path from rescanning the 50-KiB prefix once per token.
        let line = || {
            template[..offset]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count()
                + 1
        };
        let suffix = token_and_rest[TOKEN_PREFIX.len()..]
            .find("__")
            .unwrap_or_else(|| panic!("unterminated create.js token on line {}", line()));
        let token_len = TOKEN_PREFIX.len() + suffix + 2;
        let token = &token_and_rest[..token_len];
        let index = replacements
            .iter()
            .position(|(candidate, _)| *candidate == token)
            .unwrap_or_else(|| panic!("unknown create.js token {token} on line {}", line()));
        rendered.push_str(&replacements[index].1);
        used[index] = true;
        rest = &token_and_rest[token_len..];
    }
    rendered.push_str(rest);

    for ((token, _), was_used) in replacements.iter().zip(used) {
        assert!(was_used, "create.js is missing {token}");
    }
    rendered
}

/// Build the JS that creates a uPlot chart. Rust's `ChartJsConfig` equality gate keeps ordinary data-only refreshes out of this roughly 52-KiB renderer entirely; the embedded config hash is a second, browser-side race guard so overlapping same-config create evaluations coalesce through `setData` instead of destroying and recreating the chart. A genuinely different config still falls through to destroy + recreate.
pub(super) fn build_create_js(id: &str, cfg: &ChartJsConfig) -> String {
    // This hash is an ephemeral same-page equality token only; it is never persisted or sent over the wire, so cross-process stability is deliberately unnecessary.
    let cfg_hash = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        cfg.hash(&mut h);
        h.finish()
    };
    let mut series_opts: Vec<String> = vec!["{}".to_string()];
    let mut bands_js = Vec::new();
    // Props and local storage normally supply #RRGGBB, but this is the final JavaScript boundary: normalize malformed persisted or caller-provided values so they cannot break the generated script.
    let colors: Vec<&str> = cfg
        .colors
        .iter()
        .map(|color| normalized_hex_color(color))
        .collect();

    // Block width: run i's raw/envelope block at 1 + i*raw_stride, its values line at 1 + n*raw_stride + i. Data assembly in the component mirrors this order.
    let raw_stride = (if cfg.has_raw { 1 } else { 0 }) + (if cfg.has_range { 2 } else { 0 });

    for (i, color) in colors.iter().copied().enumerate() {
        let (r, g, b) = parse_hex_color(color);

        if cfg.has_raw {
            // Raw scatter behind a smoothed passthrough line.
            series_opts.push(format!("{{'stroke':'rgba({},{},{},0.35)','width':1,'spanGaps':true,'points':{{show:false}}}}", r, g, b));
        }

        if cfg.has_range {
            // Use faint edges under a band or smoothed curve; otherwise the min edge is the line, since bounds can stay finite beside a NaN value.
            let faint = cfg.banded[i] || cfg.smoothed;
            let (edge_stroke, edge_width) = if faint {
                (format!("rgba({r},{g},{b},0.2)"), 1.0)
            } else {
                (color.to_string(), 1.5)
            };
            let edge_opt = format!(
                "{{'stroke':'{edge_stroke}','width':{edge_width},'spanGaps':true,'points':{{show:false}}}}"
            );
            series_opts.push(edge_opt.clone());
            series_opts.push(if faint {
                edge_opt
            } else {
                "{'stroke':'transparent','width':0,'points':{show:false},'paths':()=>null}"
                    .to_string()
            });
            // Fill max-to-min in the raw block, below value lines. Keep log-clipped bounds as data; uPlot clamps their paths while yNiceLog excludes them from the range.
            if cfg.banded[i] {
                let min_idx = 1 + i * raw_stride + (if cfg.has_raw { 1 } else { 0 });
                let max_idx = min_idx + 1;
                bands_js.push(format!(
                    "{{series:[{},{}],fill:'rgba({},{},{},0.12)'}}",
                    max_idx, min_idx, r, g, b
                ));
            }
        }
    }

    for (label, color) in cfg.labels.iter().zip(colors.iter().copied()) {
        let escaped = esc_js(label);

        // spanGaps: each series only has values at its own x positions on
        // the joined axis — bridge the slots owned by other series. Logged
        // NaNs still surface via the hollow markers.
        // Unsmoothed bucket means are data-only; skip building their invisible paths.
        let (line_stroke, line_paths) = if cfg.has_range && !cfg.smoothed {
            ("transparent", ",'paths':()=>null")
        } else {
            (color, "")
        };
        series_opts.push(format!(
            "{{'label':'{}','stroke':'{}','width':1.5,'spanGaps':true,'points':{{show:false}}{}}}",
            escaped, line_stroke, line_paths
        ));
    }

    // NaN markers: appended AFTER all real series + bands so the block/band
    // index math above is untouched. One marker column per real series; the
    // column holds the marker KIND where the run logged a non-finite value
    // (NaN elsewhere). Nothing renders through the series machinery —
    // each marker series sits on the dummy 'nan' scale so it can never
    // stretch the y range, and the draw hook paints its hollow circles
    // pinned to the plot's bottom edge.
    for _ in 0..cfg.nan_markers {
        series_opts.push(
            "{'scale':'nan','stroke':'transparent','width':0,'points':{show:false},'paths':()=>null}"
                .to_string(),
        );
    }

    let colors_js: Vec<String> = colors.iter().map(|color| format!("'{color}'")).collect();

    let labels_js: Vec<String> = cfg
        .labels
        .iter()
        // create.js uses these strings as tooltip innerHTML; escape once here rather than on every hover. The separate uPlot series labels above remain plain text.
        .map(|label| format!("'{}'", esc_js(&esc_html_text(label))))
        .collect();

    let run_ids_js: Vec<String> = cfg
        .run_ids
        .iter()
        .map(|r| format!("'{}'", esc_js(r)))
        .collect();

    let run_names_js: Vec<String> = cfg
        .run_names
        .iter()
        .map(|name| match name {
            Some(name) => format!("'{}'", esc_js(name)),
            None => "null".to_string(),
        })
        .collect();

    let x_splits = if cfg.log_shift {
        "{splits:xSplits,filter:function(u,s){return s}}"
    } else {
        "{}"
    };
    // uPlot's default log range snaps to powers of 10; pin to the zoom's own endpoints, but via logSafeX so an extrapolated <=0/non-finite bound can't freeze the per-decade tick walk.
    let x_range = if cfg.log_x { "{range:logSafeX}" } else { "{}" };
    // yNice/yNiceLog: nice bounds under a 10% slack cap, measured linearly or in decades to match the scale's geometry.
    let y_range = if cfg.log_y {
        "{range:yNiceLog}"
    } else {
        "{range:yNice}"
    };
    // Vendored uPlot's font parser accepts integer `Npx` only. Round each
    // size here rather than emitting a decimal that it would mis-parse.
    let axis_font_size = cfg.font_size.scale_px(12);
    // Tick and gap geometry stays fixed; only the label contribution grows.
    let x_axis_size = 11 + axis_font_size;
    let y_axis_min_size = 11 + cfg.font_size.scale_px(24);
    // Cursor sync exchanges real x: every step chart defines a derived syncX scale over original steps whose fwd/bwd reproduce its plotted pixel geometry, so linear, plain-log, and log(x+1) charts meet in one native sync group.
    let sync_scale = if cfg.zoom_refetch {
        if cfg.log_shift {
            "{syncX:{from:'x',distr:100,fwd:v=>Math.log10(v+1),bwd:v=>Math.pow(10,v)-1,range:(u,mn,mx)=>[mn-1,mx-1]}}"
        } else if cfg.log_x {
            // Identity domain, but the geometry must match the plotted distr-3 log scale — a bare `from` scale defaults to linear and would misplace the mirrored crosshair.
            "{syncX:{from:'x',distr:100,fwd:v=>Math.log10(v),bwd:v=>Math.pow(10,v),range:(u,mn,mx)=>[mn,mx]}}"
        } else {
            "{syncX:{from:'x',range:(u,mn,mx)=>[mn,mx]}}"
        }
    } else {
        "{}"
    };
    let replacements = [
        ("__KYMO_HOVER__", HOVER.to_string()),
        (
            "__KYMO_ZOOM_MATH__",
            ZOOM_MATH
                .strip_suffix('\n')
                .unwrap_or(ZOOM_MATH)
                .to_string(),
        ),
        ("__KYMO_ID__", esc_js(id)),
        ("__KYMO_CHART_HEIGHT__", cfg.height.to_string()),
        ("__KYMO_AXIS_FONT_SIZE__", axis_font_size.to_string()),
        ("__KYMO_X_AXIS_SIZE__", x_axis_size.to_string()),
        ("__KYMO_Y_AXIS_MIN_SIZE__", y_axis_min_size.to_string()),
        ("__KYMO_RAW_STRIDE__", raw_stride.to_string()),
        ("__KYMO_HAS_RAW__", cfg.has_raw.to_string()),
        ("__KYMO_HAS_RANGE__", cfg.has_range.to_string()),
        ("__KYMO_SMOOTHED__", cfg.smoothed.to_string()),
        ("__KYMO_HAS_XRANGE__", cfg.has_xrange.to_string()),
        ("__KYMO_NAN_MARKERS__", cfg.nan_markers.to_string()),
        ("__KYMO_ZOOM_REFETCH__", cfg.zoom_refetch.to_string()),
        ("__KYMO_X_SHIFT__", u8::from(cfg.log_shift).to_string()),
        (
            "__KYMO_READOUT_X_SHIFT__",
            if cfg.log_shift {
                "1"
            } else if cfg.time_log_shift {
                "0.001"
            } else {
                "0"
            }
            .to_string(),
        ),
        ("__KYMO_X_SPLITS__", x_splits.to_string()),
        (
            "__KYMO_X_DISTR__",
            if cfg.log_x { "3" } else { "1" }.to_string(),
        ),
        ("__KYMO_X_RANGE__", x_range.to_string()),
        (
            "__KYMO_Y_DISTR__",
            if cfg.log_y { "3" } else { "1" }.to_string(),
        ),
        ("__KYMO_Y_RANGE__", y_range.to_string()),
        ("__KYMO_SERIES_LIST__", series_opts.join(",")),
        ("__KYMO_LABELS__", labels_js.join(",")),
        ("__KYMO_COLORS__", colors_js.join(",")),
        ("__KYMO_RUN_IDS__", run_ids_js.join(",")),
        ("__KYMO_RUN_NAMES__", run_names_js.join(",")),
        (
            "__KYMO_XNAN_COUNTS__",
            cfg.xnan_counts
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        ("__KYMO_BANDS_LIST__", bands_js.join(",")),
        ("__KYMO_CFG_HASH__", format!("{cfg_hash:x}")),
        ("__KYMO_IS_TIME__", cfg.is_time_axis.to_string()),
        ("__KYMO_IS_WALL__", cfg.is_wall_time.to_string()),
        ("__KYMO_X_LABEL__", esc_js(&cfg.x_label)),
        ("__KYMO_SYNC_KEY__", esc_js(&cfg.sync_key)),
        ("__KYMO_SYNC_SCALE__", sync_scale.to_string()),
        (
            "__KYMO_SYNC_SCALE_KEY__",
            if cfg.zoom_refetch { "syncX" } else { "x" }.to_string(),
        ),
    ];
    render_template(&replacements)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_config() -> ChartJsConfig {
        ChartJsConfig {
            labels: vec!["first'line".into(), "second\nline".into()],
            colors: vec!["#123456".into(), "#abcdef".into()],
            run_ids: vec!["run-a".into(), "run-b".into()],
            run_names: vec![Some("resumed 'run'\n/tag & <name>".into()), None],
            has_range: true,
            banded: vec![true, false],
            has_raw: true,
            smoothed: true,
            has_xrange: true,
            nan_markers: 2,
            xnan_counts: vec![0, 3],
            log_x: true,
            log_shift: true,
            time_log_shift: false,
            log_y: true,
            height: 321,
            font_size: FontSize::default(),
            zoom_refetch: true,
            is_time_axis: false,
            is_wall_time: false,
            sync_key: "step-sync".into(),
            x_label: "step".into(),
        }
    }

    fn minimal_config() -> ChartJsConfig {
        ChartJsConfig {
            labels: vec!["metric".into()],
            colors: vec!["#010203".into()],
            run_ids: vec!["run".into()],
            run_names: vec![Some("".into())],
            has_range: false,
            banded: vec![false],
            has_raw: false,
            smoothed: false,
            has_xrange: false,
            nan_markers: 0,
            xnan_counts: vec![0],
            log_x: false,
            log_shift: false,
            time_log_shift: false,
            log_y: false,
            height: 200,
            font_size: FontSize::default(),
            zoom_refetch: false,
            is_time_axis: true,
            is_wall_time: true,
            sync_key: "wall".into(),
            x_label: "time".into(),
        }
    }

    #[test]
    fn envelope_borders_stay_faint_and_single_lines_keep_normal_strength() {
        let mut cfg = full_config();
        cfg.has_raw = false;
        cfg.nan_markers = 0;
        for smoothed in [false, true] {
            cfg.smoothed = smoothed;
            let js = build_create_js("envelopes", &cfg);
            assert_eq!(
                js.matches("'stroke':'rgba(18,52,86,0.2)','width':1,")
                    .count(),
                2
            );
            assert_eq!(
                js.matches("'stroke':'rgba(171,205,239,0.2)','width':1,")
                    .count(),
                if smoothed { 2 } else { 0 }
            );
            assert!(js.contains("series:[2,1],fill:'rgba(18,52,86,0.12)'"));
            assert!(!js.contains("series:[4,3]"));
            assert_eq!(
                js.matches("'paths':()=>null").count(),
                if smoothed { 0 } else { 3 }
            );
            assert_eq!(
                js.matches("'stroke':'#123456','width':1.5,").count(),
                usize::from(smoothed)
            );
            assert_eq!(js.matches("'stroke':'#abcdef','width':1.5,").count(), 1);
            let values_stroke = if smoothed { "#abcdef" } else { "transparent" };
            assert!(js.contains(&format!(
                "'label':'second\\nline','stroke':'{values_stroke}','width':1.5,"
            )));
        }
    }

    #[test]
    fn renders_every_template_token_and_escapes_dynamic_strings() {
        let js = build_create_js("chart-7", &full_config());

        assert!(!js.contains("__KYMO_"));
        assert!(js.contains("setCursor:[function updateHover(u){"));
        assert!(js.starts_with("return(()=>{"));
        assert!(js.ends_with("})();"));
        assert!(js.contains("finally{\n  // This script returns"));
        assert!(js.contains("dioxus.close();"));
        assert!(js.contains("'first\\'line'"));
        assert!(js.contains("'second\\nline'"));
        assert!(js.contains("'run-b'"));
        assert!(js.contains("let runNames=['resumed \\'run\\'\\n/tag & <name>',null];"));
    }

    #[test]
    fn renders_the_opposite_feature_branches() {
        let js = build_create_js("chart-7", &minimal_config());

        assert!(!js.contains("__KYMO_"));
        assert!(js.contains("distr:1"));
        assert!(js.contains("height:200"));
        assert!(js.contains("font:'13px sans-serif'"));
        assert!(js.contains("!sameWallDay(s.min,s.max)"));
        assert!(js.contains("if(isWallTime)return fmtWall(v,true)"));
    }

    #[test]
    fn user_font_size_scales_canvas_axes_and_gutters() {
        let mut config = minimal_config();
        config.font_size = FontSize::new(20).unwrap();
        let js = build_create_js("chart-7", &config);

        assert!(js.contains("font:'16px sans-serif'"));
        assert!(js.contains("gap:6,size:27"));
        assert!(js.contains("Math.max(43"));
    }

    #[test]
    fn custom_x_tooltips_keep_sub_cent_numeric_precision() {
        let js = build_create_js("chart-7", &minimal_config());

        let custom_x = js
            .find("if(!zoomRefetch)return v.toPrecision(4)")
            .expect("custom X tooltip formatter");
        let step = js
            .find("return v.toLocaleString(undefined,{maximumFractionDigits:2})")
            .expect("step tooltip formatter");
        assert!(custom_x < step, "custom X must bypass step rounding");
    }

    #[test]
    fn hover_cursor_publishes_to_the_sync_group() {
        let js = build_create_js("chart-7", &minimal_config());
        // Gutter positions and clears must reach synced readouts too (AI-1405).
        assert!(js.contains("c.setCursor(pos||{left:-10,top:-10},true,true)"));
        // A leaving or rebuilt source publishes its clear while it is still the source, so its hook drops the highlight and synced readouts clear.
        assert!(js.contains(
            "if(window.__kymo_hoversrc!=='chart-7')return;\n    let c=window.__kymo_charts['chart-7'];\n    if(c)c.setCursor({left:-10,top:-10},true,true);\n    window.__kymo_hoversrc=null;"
        ));
        assert!(js.contains(
            "if(window.__kymo_hoversrc==='chart-7')prev.setCursor({left:-10,top:-10},true,true);\n  prev.destroy();"
        ));
    }

    #[test]
    fn kymo_owns_selection_gestures_and_commits_from_one_path() {
        let js = build_create_js("chart-7", &minimal_config());

        assert!(js.contains(
            "bind:{mousedown:()=>null,mouseenter:()=>null,mousemove:()=>null,mouseleave:()=>null,dblclick:()=>null}"
        ));
        assert!(js.contains("drag:{click:()=>{}}"));
        assert!(!js.contains("setSelect:["));
        assert!(!js.contains("__kymo_dragpx"));
    }

    #[test]
    fn zero_present_log_time_keeps_zoom_unshifted_and_restores_readout_x() {
        let mut config = minimal_config();
        config.log_x = true;
        config.time_log_shift = true;
        let js = build_create_js("chart-7", &config);

        assert!(js.contains("let xShift=0;"));
        assert!(js.contains("let readoutXShift=0.001;"));
    }

    #[test]
    fn does_not_interpret_tokens_from_dynamic_values() {
        let mut config = minimal_config();
        config.labels[0] = "__KYMO_COLORS__".to_string();
        config.sync_key = "sync'key\n".to_string();
        let js = build_create_js("chart'7\n", &config);

        assert!(js.contains("'__KYMO_COLORS__'"));
        assert!(js.contains("chart\\'7\\n"));
        assert!(js.contains("sync\\'key\\n"));
    }

    #[test]
    fn malformed_colors_cannot_break_the_generated_script() {
        let mut config = minimal_config();
        config.colors[0] = "red';globalThis.injected=true;//".to_string();
        let js = build_create_js("chart-7", &config);

        assert!(!js.contains("globalThis.injected"));
        assert!(js.contains("'#808080'"));
        assert!(js.contains("'stroke':'#808080'"));
    }

    #[test]
    fn tooltip_escapes_user_controlled_labels_before_using_inner_html() {
        let mut config = minimal_config();
        config.labels[0] = "<img src=x onerror='globalThis.injected=true'> &".to_string();
        let js = build_create_js("chart-7", &config);

        assert!(!js.contains("let labels=['<img src=x"));
        assert!(js.contains(
            "let labels=['&lt;img src=x onerror=&#39;globalThis.injected=true&#39;&gt; &amp;']"
        ));
    }

    #[test]
    fn chart_copy_metadata_uses_the_x_label_and_live_column_layout() {
        let mut config = minimal_config();
        config.x_label = "custom'x\n".to_string();
        let js = build_create_js("chart-7", &config);

        assert!(js.contains("xLabel:'custom\\'x\\n'"));
        assert!(js.contains(
            "lineBase:lineBase,seriesCount:labels.length,nanBase:nanBase,nanCols:nanCols,xShift:readoutXShift"
        ));
    }
}
