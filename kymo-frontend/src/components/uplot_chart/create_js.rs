use super::{normalized_hex_color, parse_hex_color, ShownAxis};
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
    /// its series list — they feed the tooltip's x-range header and whole-bucket zoom.
    pub(super) has_xrange: bool,
    /// Number of trailing "NaN marker" columns appended after all real series
    /// columns (one per real series, same order/colors). Each marks where the
    /// run logged a non-finite value or an unplottable x, or where the log y
    /// axis clips it, with a hollow circle pinned to the plot's border (top
    /// for +inf, bottom for the rest), like wandb.
    pub(super) nan_markers: usize,
    /// Per series: how many samples sit behind its kind-4 (unplottable-x)
    /// markers — the tooltip appends "×N" on those rows when N > 1.
    pub(super) xnan_counts: Vec<u32>,
    pub(super) axis: ShownAxis,
    /// Zero-present log x on a step or time axis renders log(x+1) in the wire's units (steps, ms), matching the server's bucket ladder: the data is shifted before uPlot. Its ticks fall at real x and its labels read it (steps, or seconds on a time axis), and a step chart's zoom dispatches to real steps; tooltip and copy readouts undo the shift (+1 step, +0.001 s).
    pub(super) log_shift: bool,
    pub(super) log_y: bool,
    /// Canvas font/gutter input included in the structural config.
    pub(super) font_size: FontSize,
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
        // Line numbers exist only for malformed-template diagnostics; keep the ordinary render path from rescanning the template's prefix once per token.
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

/// Cursor-sync group: charts on the same x axis (step, relative time, wall time, or one custom metric) mirror each other's crosshair, compact tooltips, zoom and selection boxes.
fn sync_key(axis: &ShownAxis) -> String {
    if axis.step_axis() {
        "step".to_string()
    } else if axis.wall {
        "wall".to_string()
    } else if axis.time {
        "rel".to_string()
    } else {
        // The full metric name: esc_js escapes it, and a lossy rewrite could map two metrics to one key.
        // The m- prefix keeps a metric named step, wall or rel out of those built-in groups.
        format!("m-{}", axis.x_metric)
    }
}

/// A line's builders (create.js): the line over the samples it strokes, and a dot on a lone sample, which no segment would show.
const LINE: &str = "'paths':linePaths,'points':lonePoints()";

/// A series that draws nothing, holding a column for the tooltip or copy: hidden, so uPlot builds it no path and leaves it out of the y range, which follows what is drawn. The drawn series of its run span its values, except on log-y, where uPlot's range skips a bucket's nonpositive min but would count its positive mean.
const HIDDEN: &str = "'show':false";

/// Build the JS that creates a uPlot chart. Rust's `ChartJsConfig` equality gate keeps ordinary data-only refreshes out of this renderer entirely; the embedded config hash is a second, browser-side race guard so overlapping same-config create evaluations coalesce through `setData` instead of destroying and recreating the chart. A genuinely different config still falls through to destroy + recreate.
pub(super) fn build_create_js(id: &str, cfg: &ChartJsConfig) -> String {
    let log_x = cfg.axis.log_x;
    let zoom_refetch = cfg.axis.step_axis();
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
            series_opts.push(format!(
                "{{'stroke':'rgba({r},{g},{b},0.35)','width':1,'spanGaps':true,{LINE}}}"
            ));
        }

        if cfg.has_range {
            // Use faint edges under a band or smoothed curve; otherwise the min edge is the line, since bounds can stay finite beside a NaN value.
            let faint = cfg.banded[i] || cfg.smoothed;
            let (edge_stroke, edge_width) = if faint {
                (format!("rgba({r},{g},{b},0.2)"), 1.0)
            } else {
                (color.to_string(), 1.5)
            };
            let edge = format!(
                "'stroke':'{edge_stroke}','width':{edge_width},'spanGaps':true,'paths':linePaths"
            );
            // A lone bucket's mark rides the min edge: on unsmoothed charts in the run's color at its line's weight, banded or not (one bucket's band has no width); behind a smoothed curve in the faint edge color and width, as evidence.
            let (mark_fill, mark_bar) = if cfg.smoothed {
                (edge_stroke.as_str(), edge_width)
            } else {
                (color, 1.5)
            };
            series_opts.push(format!(
                "{{{edge},'points':lonePoints('{mark_fill}',{mark_bar})}}"
            ));
            series_opts.push(if faint {
                format!("{{{edge},'points':{{show:false}}}}")
            } else {
                format!("{{{HIDDEN}}}")
            });
            // Fill max-to-min in the raw block, below value lines. Keep log-clipped bounds as data; uPlot clamps their paths and leaves them out of its log range.
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
        // Unsmoothed bucket means are data-only.
        series_opts.push(if cfg.has_range && !cfg.smoothed {
            format!("{{'label':'{escaped}',{HIDDEN}}}")
        } else {
            format!("{{'label':'{escaped}','stroke':'{color}','width':1.5,'spanGaps':true,{LINE}}}")
        });
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

    // Vendored uPlot's font parser accepts integer `Npx` only. Round each
    // size here rather than emitting a decimal that it would mis-parse.
    let axis_font_size = cfg.font_size.scale_px(12);
    // Tick and gap geometry stays fixed; only the label contribution grows.
    let x_axis_size = 11 + axis_font_size;
    let y_axis_min_size = 11 + cfg.font_size.scale_px(24);
    // Cursor sync exchanges real x: every step chart defines a derived syncX scale over original steps whose fwd/bwd reproduce its plotted pixel geometry, so linear, plain-log, and log(x+1) charts meet in one native sync group.
    let sync_scale = if zoom_refetch {
        if cfg.log_shift {
            "{syncX:{from:'x',distr:100,fwd:v=>Math.log10(v+1),bwd:v=>Math.pow(10,v)-1,range:(u,mn,mx)=>[mn-1,mx-1]}}"
        } else if log_x {
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
        ("__KYMO_AXIS_FONT_SIZE__", axis_font_size.to_string()),
        ("__KYMO_X_AXIS_SIZE__", x_axis_size.to_string()),
        ("__KYMO_Y_AXIS_MIN_SIZE__", y_axis_min_size.to_string()),
        ("__KYMO_RAW_STRIDE__", raw_stride.to_string()),
        ("__KYMO_HAS_RAW__", cfg.has_raw.to_string()),
        ("__KYMO_HAS_RANGE__", cfg.has_range.to_string()),
        ("__KYMO_SMOOTHED__", cfg.smoothed.to_string()),
        ("__KYMO_HAS_XRANGE__", cfg.has_xrange.to_string()),
        ("__KYMO_NAN_MARKERS__", cfg.nan_markers.to_string()),
        ("__KYMO_ZOOM_REFETCH__", zoom_refetch.to_string()),
        (
            "__KYMO_READOUT_X_SHIFT__",
            if !cfg.log_shift {
                "0"
            } else if cfg.axis.time {
                "0.001"
            } else {
                "1"
            }
            .to_string(),
        ),
        (
            "__KYMO_X_DISTR__",
            if log_x { "3" } else { "1" }.to_string(),
        ),
        (
            "__KYMO_Y_DISTR__",
            if cfg.log_y { "3" } else { "1" }.to_string(),
        ),
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
        ("__KYMO_IS_TIME__", cfg.axis.time.to_string()),
        ("__KYMO_IS_WALL__", cfg.axis.wall.to_string()),
        ("__KYMO_X_LABEL__", esc_js(cfg.axis.x_label())),
        ("__KYMO_SYNC_KEY__", esc_js(&sync_key(&cfg.axis))),
        ("__KYMO_SYNC_SCALE__", sync_scale.to_string()),
        (
            "__KYMO_SYNC_SCALE_KEY__",
            if zoom_refetch { "syncX" } else { "x" }.to_string(),
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
            axis: ShownAxis {
                log_x: true,
                time: false,
                wall: false,
                x_metric: String::new(),
            },
            log_shift: true,
            log_y: true,
            font_size: FontSize::default(),
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
            axis: ShownAxis {
                log_x: false,
                time: true,
                wall: true,
                x_metric: String::new(),
            },
            log_shift: false,
            log_y: false,
            font_size: FontSize::default(),
        }
    }

    /// Every series after x's, by position: envelope edges are faint under a band or a smoothed curve, and otherwise the min edge is the run's line. A lone sample's mark: a dot on each line, in its stroke (the run's color on a values line, the faint evidence color on the raw scatter); on an envelope, a bar on the min edge, in the run's color at 1.5 px on unsmoothed charts, in the faint edge color and width behind a smoothed curve. Max edges carry none, and undrawn series are hidden.
    #[test]
    fn each_series_strokes_and_marks_its_run_or_is_hidden() {
        // Each series: "<stroke> <width>", then " dot" or " bar <width> <fill>" for its mark; or "hidden". Every shown series spans its gaps through linePaths: one without linePaths would get uPlot's linear builder, which returns null on an empty column and so rebuilds on every repaint, and one that broke its line at gaps would leave samples between gaps unmarked.
        fn series(js: &str) -> String {
            let list = js.split_once("series:[{},{").unwrap().1;
            let list = list.split_once("}],\n  bands:").unwrap().0;
            list.split("},{")
                .map(|entry| {
                    if entry.contains(HIDDEN) {
                        return "hidden".to_string();
                    }
                    let (line, points) = entry
                        .split_once(",'spanGaps':true,'paths':linePaths,'points':")
                        .expect(entry);
                    let line = line.split_once("'stroke':'").unwrap().1;
                    let line = line.replace("','width':", " ");
                    match points {
                        "{show:false}" => line,
                        "lonePoints()" => format!("{line} dot"),
                        bar => {
                            let bar = bar.strip_prefix("lonePoints('").unwrap();
                            let (fill, width) =
                                bar.strip_suffix(')').unwrap().split_once("',").unwrap();
                            format!("{line} bar {width} {fill}")
                        }
                    }
                })
                .collect::<Vec<_>>()
                .join("|")
        }
        assert_eq!(
            series(&build_create_js("plain", &minimal_config())),
            "#010203 1.5 dot"
        );

        let (a, b) = ("#123456", "#abcdef");
        let (fa, fb) = ("rgba(18,52,86,0.2)", "rgba(171,205,239,0.2)");
        let (ra, rb) = ("rgba(18,52,86,0.35)", "rgba(171,205,239,0.35)");
        // Per run: [raw], min edge, max edge; then the values lines. Marker columns have no series entries. Run 0 is banded; run 1's max edge is hidden unless smoothing makes its edges faint.
        let mut cfg = full_config();
        for (has_range, has_raw, smoothed, expected) in [
            (
                true,
                false,
                false,
                format!("{fa} 1 bar 1.5 {a}|{fa} 1|{b} 1.5 bar 1.5 {b}|hidden|hidden|hidden"),
            ),
            (
                true,
                false,
                true,
                format!("{fa} 1 bar 1 {fa}|{fa} 1|{fb} 1 bar 1 {fb}|{fb} 1|{a} 1.5 dot|{b} 1.5 dot"),
            ),
            (
                true,
                true,
                true,
                format!("{ra} 1 dot|{fa} 1 bar 1 {fa}|{fa} 1|{rb} 1 dot|{fb} 1 bar 1 {fb}|{fb} 1|{a} 1.5 dot|{b} 1.5 dot"),
            ),
            (
                false,
                true,
                true,
                format!("{ra} 1 dot|{rb} 1 dot|{a} 1.5 dot|{b} 1.5 dot"),
            ),
        ] {
            cfg.has_range = has_range;
            cfg.has_raw = has_raw;
            cfg.smoothed = smoothed;
            assert_eq!(
                series(&build_create_js("ranges", &cfg)),
                expected,
                "range={has_range} raw={has_raw} smoothed={smoothed}"
            );
        }
    }

    #[test]
    fn only_a_banded_run_fills_and_a_hidden_values_line_keeps_its_label() {
        let mut cfg = full_config();
        cfg.has_raw = false;
        for smoothed in [false, true] {
            cfg.smoothed = smoothed;
            let js = build_create_js("envelopes", &cfg);
            assert!(js.contains("series:[2,1],fill:'rgba(18,52,86,0.12)'"));
            assert!(!js.contains("series:[4,3]"));
            assert!(js.contains(if smoothed {
                "'label':'second\\nline','stroke':'#abcdef','width':1.5,"
            } else {
                "'label':'second\\nline','show':false}"
            }));
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
            .find("return fmtNum(v,true,2)")
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
    fn zero_present_log_time_restores_readout_x() {
        let mut config = minimal_config();
        // Only relative time starts at zero.
        config.axis.wall = false;
        config.axis.log_x = true;
        config.log_shift = true;
        let js = build_create_js("chart-7", &config);

        assert!(js.contains("let readoutXShift=0.001;"));
    }

    /// AI-1429: a custom axis syncs only with its own metric, never with one differing only in punctuation nor, when named "step", with the step charts.
    #[test]
    fn custom_axes_sync_only_with_the_same_metric() {
        let key = |metric: &str| {
            sync_key(&ShownAxis {
                x_metric: metric.to_string(),
                ..full_config().axis
            })
        };
        assert_ne!(key("train/epoch"), key("train_epoch"));
        // A custom metric named step against the step axis.
        assert_ne!(key("step"), key(""));
    }

    #[test]
    fn does_not_interpret_tokens_from_dynamic_values() {
        let mut config = minimal_config();
        config.labels[0] = "__KYMO_COLORS__".to_string();
        config.axis.time = false;
        config.axis.wall = false;
        config.axis.x_metric = "sync'key\n".to_string();
        let js = build_create_js("chart'7\n", &config);

        assert!(js.contains("'__KYMO_COLORS__'"));
        assert!(js.contains("chart\\'7\\n"));
        assert!(js.contains("key:'kymo-m-sync\\'key\\n'"));
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
        config.axis.time = false;
        config.axis.wall = false;
        config.axis.x_metric = "custom'x\n".to_string();
        let js = build_create_js("chart-7", &config);

        assert!(js.contains("xLabel:'custom\\'x\\n'"));
        assert!(js.contains(
            "lineBase:lineBase,seriesCount:labels.length,nanBase:nanBase,nanCols:nanCols,xShift:readoutXShift"
        ));
    }
}
