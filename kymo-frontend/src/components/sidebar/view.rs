use super::*;
use crate::util::{focus_on_mount, is_app_escape, TOP_LAYER_SELECTOR};
use dioxus::core::{current_scope_id, Runtime};

/// A run's liveness glyph: shape tells the kind of state, CSS color its severity.
#[component]
fn RunStatusIcon(status: RunStatus) -> Element {
    const DOT: &str = "M5 1.5a3.5 3.5 0 1 1 0 7 3.5 3.5 0 1 1 0-7z";
    let (state, title, path, stroked) = match status {
        RunStatus::Running => ("running", "running", DOT, false),
        RunStatus::Stuck => (
            "stuck",
            "stuck (main metrics stalled)",
            "M1.8 1.2h2.3v7.6H1.8zM5.9 1.2h2.3v7.6H5.9z",
            false,
        ),
        // Warning triangle with the "!" knocked out (evenodd).
        RunStatus::Unresponsive => (
            "unresponsive",
            "unresponsive (no system metrics)",
            "M5 .6 9.7 9.2H.3zM4.35 3.4h1.3v3.1h-1.3zM4.35 7.1h1.3v1.2h-1.3z",
            false,
        ),
        RunStatus::PresumedDead => (
            "presumed-dead",
            "presumed dead (>10min silent)",
            "M5 1.3a3.7 3.7 0 1 1 0 7.4 3.7 3.7 0 1 1 0-7.4zM2.4 7.6l5.2-5.2",
            true,
        ),
        RunStatus::Crashed => (
            "crashed",
            "crashed (non-zero exit)",
            "M2 2l6 6M8 2 2 8",
            true,
        ),
        RunStatus::Finished => (
            "finished",
            "finished (clean exit)",
            "M1.5 5.4 4 7.8l4.6-5.4",
            true,
        ),
        RunStatus::Unknown => ("unknown", "unknown", DOT, false),
    };
    rsx! {
        span { class: "run-status {state}", role: "img", aria_label: title, title,
            svg {
                view_box: "0 0 10 10",
                fill: "currentColor",
                fill_rule: "evenodd",
                stroke_width: "1.6",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                "aria-hidden": "true",
                path {
                    d: path,
                    fill: if stroked { "none" },
                    stroke: if stroked { "currentColor" },
                }
            }
        }
    }
}

#[component]
pub fn Sidebar() -> Element {
    let mut state = use_context::<DashboardState>();
    let project_id = state.project_id.read().clone();
    // Run links carry the focused chart along, so a run click under an open maximize overlay lands with the same chart focused.
    // Only the links read it, through a memo that notifies them when the chart changes.
    let chart_focus = use_memo(|| router().current::<Route>().chart_param());
    let runs = state.runs.read();
    let mut filter = use_signal(String::new);
    let mut picking_color_for = use_signal(|| None::<ColorPickerTarget>);
    // Deletion selection is deliberately transient and completely separate
    // from `selected_runs`, whose filled/hollow markers mean "visible on charts".
    let mut trash_pick = use_signal(|| None::<HashSet<String>>);
    // One drag-paint gesture at a time, whichever mode armed it: bulk mode
    // paints the deletion selection, normal mode paints `selected_runs`.
    let paint = use_signal(|| None::<SelectionPaint>);
    let trash_busy = use_signal(|| false);
    let action_feedback = use_signal(String::new);
    let action_feedback_timer = use_signal(|| None::<Task>);
    let mut trash_feedback = use_signal(String::new);
    let mut rename_target = use_signal(|| None::<RenameRunTarget>);
    let mutation_ui = TrashMutationUi {
        dashboard: state,
        picker: trash_pick,
        paint,
        busy: trash_busy,
        feedback: trash_feedback,
    };
    let sidebar_pick_bridge = crate::util::js_bridge::use_bridge("sidebar_pick");

    // Filtering and run updates replace rows; Trash mode changes their classes.
    // Reapply JS hover classes after those Dioxus renders.
    use_effect(move || {
        let _ = filter.read();
        let _ = state.runs.read();
        let _ = trash_pick.read();
        let _ = js_sys::eval("window.__kymo_refreshHl()");
    });

    use_future(move || {
        let js = sidebar_pick_bridge
            .script(SIDEBAR_PICK_BRIDGE_JS)
            .replace("__TOP_LAYER_SELECTOR__", &js_string(TOP_LAYER_SELECTOR));
        async move {
            let mut eval = document::eval(&js);
            while let Ok(action) = eval.recv::<String>().await {
                end_paint_if_active(paint);
                if action == "cancel" && trash_pick.peek().is_some() && !*trash_busy.peek() {
                    trash_feedback.set(String::new());
                    close_trash_picker(trash_pick, paint);
                }
            }
        }
    });
    // Restore selected runs from localStorage on first load
    let mut loaded = use_signal(|| false);
    let mut skip_initial_persist = use_signal(|| true);
    if !*loaded.read() && !runs.is_empty() {
        loaded.set(true);
        let key_v2 = selected_runs_key(&project_id);
        let selection = if let Some(saved) = local_storage::get(&key_v2) {
            restored_json_run_selection(&saved, &runs)
        } else if let Some(saved) = local_storage::get(&legacy_selected_runs_v2_key(&project_id))
            .filter(|value| is_json_run_selection(value))
        {
            restored_json_run_selection(&saved, &runs)
        } else {
            let legacy_key = legacy_selected_runs_v1_key(&project_id);
            local_storage::get(&legacy_key).and_then(|saved| restored_run_selection(&saved, &runs))
        };
        if let Some(selection) = selection {
            // An encoded empty selection shows no runs, distinct from missing
            // or stale data (which keeps first-load auto-selection).
            state.selected_runs.set(selection);
        }
    }

    // Once the list has loaded, an empty project forgets its saved selection, however its runs left (this tab, another tab, or before this visit).
    use_effect({
        let project_id = project_id.clone();
        move || {
            if *state.runs_loaded.read() && state.runs.read().is_empty() {
                forget_run_selection(&project_id);
            }
        }
    });

    // Persist selected runs to localStorage on change (only after initial load)
    use_effect({
        let project_id = project_id.clone();
        move || {
            let sel = state.selected_runs.read();
            if !*loaded.read() {
                return;
            }
            // Restoring state is a pure read. Persist only after the next state
            // change so an ordinary visit leaves rollback state untouched.
            if *skip_initial_persist.peek() {
                skip_initial_persist.set(false);
                return;
            }
            // An empty project keeps no saved selection (the effect above).
            if state.runs.peek().is_empty() {
                return;
            }
            let key = selected_runs_key(&project_id);
            if local_storage::set(&key, &persisted_run_selection(&sel)) {
                remove_owned_legacy_run_selections(&project_id);
            }
        }
    });

    let listed = use_memo(move || {
        let filter_text = filter.read().to_lowercase();
        let runs: Vec<Rc<RunInfo>> = state
            .runs
            .read()
            .iter()
            .filter(|r| {
                filter_text.is_empty()
                    || r.run_name.to_lowercase().contains(&filter_text)
                    || r.run_id.to_lowercase().contains(&filter_text)
                    || r.ordinal.to_string().contains(&filter_text)
            })
            .map(|run| Rc::new(run.clone()))
            .collect();
        let run_ids = runs.iter().map(|run| run.run_id.clone()).collect();
        ListedRuns { runs, run_ids }
    });
    use_context_provider(|| RowContext {
        listed,
        mutation_ui,
        rename_target,
        picking_color_for,
        action_feedback,
        action_feedback_timer,
        chart_focus,
        sidebar: current_scope_id(),
    });
    let show_run_ordinals =
        *use_memo(move || sidebar_needs_run_ordinals(&state.runs.read())).read();
    let listed_now = listed.read();
    let filtered_runs = &listed_now.runs;
    let filtered_run_ids = &listed_now.run_ids;

    let in_trash_mode = trash_pick.read().is_some();
    let mut pending_trash = trash_pick.read().clone().unwrap_or_default();
    // A push or a locally confirmed partial mutation can remove runs while
    // bulk mode is open. Never leave those stale identities commit-able.
    if !pending_trash.is_empty() {
        let active_ids: HashSet<&str> = runs.iter().map(|run| run.run_id.as_str()).collect();
        let picked = pending_trash.len();
        pending_trash.retain(|run_id| active_ids.contains(run_id.as_str()));
        if pending_trash.len() < picked {
            end_paint_if_active(paint);
            trash_pick.set(Some(pending_trash.clone()));
        }
    }

    // Close the picker when its anchor, the row's ⋯ trigger, unmounts (filtering, a run-list change, Trash mode); merely skipping its render would reopen it with the row.
    let row_gone = |run_id: &String| in_trash_mode || !filtered_run_ids.contains(run_id);
    let mut color_picker_target = picking_color_for.read().clone();
    if let Some(orphan) = color_picker_target.take_if(|target| row_gone(&target.run_id)) {
        picking_color_for.set(None);
        // The picker may own focus; fall back like any other close.
        focus_run_overflow_trigger(orphan.ordinal);
    }
    // The rename editor closes the same way, including one armed after its row unmounted.
    let mut active_rename = rename_target.read().clone();
    if let Some(orphan) = active_rename.take_if(|target| row_gone(&target.run_id)) {
        rename_target.set(None);
        focus_run_overflow_trigger(orphan.ordinal);
    }
    let selected = state.selected_runs.read();
    let pending_count = pending_trash.len();
    let active_selected_count = if in_trash_mode {
        pending_count
    } else {
        selected.len()
    };
    let action_feedback_text = action_feedback.read().clone();
    let trash_feedback_text = trash_feedback.read().clone();
    let toast_text = if in_trash_mode {
        ""
    } else if !trash_feedback_text.is_empty() {
        trash_feedback_text.as_str()
    } else {
        action_feedback_text.as_str()
    };
    // The Trash footer's text; nothing else reads it.
    let (pending_shown, live_warning) = if in_trash_mode {
        let shown = filtered_runs
            .iter()
            .filter(|run| pending_trash.contains(&run.run_id))
            .count();
        (shown, live_trash_warning(&runs, &pending_trash))
    } else {
        (0, String::new())
    };
    rsx! {
        div {
            class: if in_trash_mode && *trash_busy.read() {
                "sidebar sidebar-trash-mode sidebar-trash-busy"
            } else if in_trash_mode {
                "sidebar sidebar-trash-mode"
            } else {
                "sidebar"
            },
            onmouseleave: move |_| end_paint_if_active(paint),
            div { class: "sidebar-actions",
                button {
                    class: "btn-link",
                    disabled: (active_selected_count == 0 && filtered_run_ids.is_empty())
                        || (in_trash_mode && *trash_busy.read()),
                    onmousedown: primary(move |_| {
                        end_paint_if_active(paint);
                        let listed_run_ids = &listed.read().run_ids;
                        if in_trash_mode {
                            if let Some(pick) = trash_pick.write().as_mut() {
                                all_or_none_selection(pick, listed_run_ids);
                            }
                        } else {
                            all_or_none_selection(&mut state.selected_runs.write(), listed_run_ids);
                        }
                    }),
                    {selection_action_label(active_selected_count, !filter.read().is_empty(), in_trash_mode)}
                }
                div { class: "sidebar-actions-end",
                    span {
                        class: "sidebar-meta",
                        aria_live: "polite",
                        aria_atomic: "true",
                        "{active_selected_count}/{runs.len()}"
                    }
                    if in_trash_mode {
                        button {
                            class: "sidebar-trash-cancel icon-button",
                            disabled: *trash_busy.read(),
                            title: "Cancel trash selection",
                            aria_label: "Cancel trash selection",
                            onmounted: focus_on_mount,
                            onmousedown: primary(move |_| {
                                trash_feedback.set(String::new());
                                close_trash_picker(trash_pick, paint);
                            }),
                            CloseIcon {}
                        }
                    } else {
                        button {
                            id: "sidebar-trash-trigger",
                            class: "sidebar-trash-trigger icon-button",
                            title: "Select runs to move to Trash",
                            aria_label: "Select runs to move to Trash",
                            disabled: *trash_busy.read(),
                            onmousedown: primary(move |_| {
                                if *trash_busy.peek() {
                                    return;
                                }
                                trash_feedback.set(String::new());
                                end_paint_if_active(paint);
                                trash_pick.set(Some(HashSet::new()));
                            }),
                            TrashIcon {}
                        }
                    }
                }
            }

            div { class: "sidebar-filter-row",
                input {
                    class: "sidebar-filter",
                    placeholder: "Filter runs...",
                    disabled: in_trash_mode && *trash_busy.read(),
                    value: "{filter}",
                    oninput: move |e: Event<FormData>| {
                        end_paint_if_active(paint);
                        filter.set(e.value());
                    },
                    // Esc clears, like the navbar filter.
                    onkeydown: move |e: Event<KeyboardData>| {
                        if is_app_escape(&e) && !filter.peek().is_empty() {
                            e.prevent_default();
                            end_paint_if_active(paint);
                            filter.set(String::new());
                        }
                    },
                }
            }

            div { class: "sidebar-body",
                if let Some(message) = run_list_empty_message(runs.len(), filtered_runs.len()) {
                    p { class: "sidebar-empty", "{message}" }
                }
                for run in filtered_runs.iter() {
                    RunRow {
                        key: "{run.run_id}",
                        run: run.clone(),
                        is_selected: selected.contains(&run.run_id),
                        is_pending_trash: pending_trash.contains(&run.run_id),
                        in_trash_mode,
                        show_run_ordinals,
                        renaming: active_rename.as_ref().filter(|target| target.run_id == run.run_id).cloned(),
                    }
                }
            }

            if in_trash_mode {
                div { class: "sidebar-trash-footer",
                    div { class: "sidebar-trash-footer-meta",
                        span { "Click or drag across runs" }
                        if pending_count > pending_shown {
                            span { "{pending_count - pending_shown} selected outside filter" }
                        }
                    }
                    // Always mounted so the live region exists before its text changes.
                    div { class: "sidebar-trash-warning", role: "status", "{live_warning}" }
                    if !trash_feedback_text.is_empty() {
                        div {
                            class: "sidebar-trash-feedback",
                            role: "status",
                            aria_live: "polite",
                            aria_atomic: "true",
                            "{trash_feedback_text}"
                        }
                    }
                    button {
                        class: "btn btn-primary sidebar-trash-commit",
                        disabled: pending_count == 0 || *trash_busy.read(),
                        onmousedown: primary({
                            let project_id = project_id.clone();
                            let mut requested = pending_trash.iter().cloned().collect::<Vec<_>>();
                            requested.sort_unstable();
                            move |_| submit_trash_runs(
                                project_id.clone(),
                                requested.clone(),
                                mutation_ui,
                            )
                        }),
                        TrashIcon {}
                        if *trash_busy.read() {
                            "Moving to Trash…"
                        } else if pending_count == 0 {
                            "Move to Trash"
                        } else if pending_count == 1 {
                            "Move 1 run to Trash"
                        } else {
                            "Move {pending_count} runs to Trash"
                        }
                    }
                }
            }

            // Dragged by util/width_drag.js.
            div { class: "sidebar-resize" }
        }

        if let Some(ColorPickerTarget { run_id, run_label, ordinal, opened }) = color_picker_target {
            {
                let color = run_color(&run_id, ordinal);
                let (color_run_id, opened_color) = (run_id.clone(), color.clone());
                rsx! {
                    ColorPicker {
                        key: "{opened}",
                        run_id,
                        run_label,
                        current_color: color,
                        anchor_ordinal: ordinal,
                        on_close: move |_| {
                            picking_color_for.set(None);
                            // Only a changed color repaints the run markers and charts.
                            if run_color(&color_run_id, ordinal) != opened_color {
                                let v = *state.color_version.read();
                                state.color_version.set(v + 1);
                            }
                            focus_run_overflow_trigger(ordinal);
                        },
                    }
                }
            }
        }

        div {
            class: "run-copy-feedback",
            role: "status",
            aria_live: "polite",
            aria_atomic: "true",
            title: "{toast_text}",
            // The toast paints a background, so the fade goes on an inner span, rendered only for non-empty text so :empty still matches.
            if !toast_text.is_empty() {
                span { class: "fade-overflow", span { "{toast_text}" } }
            }
        }

    }
}

/// The filtered run list, shared by the rendered rows and the drag-paint handlers.
#[derive(PartialEq)]
struct ListedRuns {
    runs: Vec<Rc<RunInfo>>,
    run_ids: Vec<String>,
}

/// Sidebar state the rows share, provided once instead of passed to each row.
#[derive(Clone, Copy)]
struct RowContext {
    listed: Memo<ListedRuns>,
    mutation_ui: TrashMutationUi,
    rename_target: Signal<Option<RenameRunTarget>>,
    picking_color_for: Signal<Option<ColorPickerTarget>>,
    action_feedback: Signal<String>,
    action_feedback_timer: Signal<Option<Task>>,
    chart_focus: Memo<Option<String>>,
    sidebar: ScopeId,
}

impl RowContext {
    /// Run `f` in the sidebar's scope, so the tasks it spawns survive their row's removal (a trashed run's row unmounts at once).
    fn in_sidebar(self, f: impl FnOnce()) {
        Runtime::current().in_scope(self.sidebar, f)
    }
}

/// Hide a row's overflow menu, which hands focus back to its ⋯ when focus was inside.
fn hide_run_menu(ordinal: u64) {
    if let Some(menu) = web_sys::window().and_then(|window| {
        window
            .document()?
            .get_element_by_id(&format!("run-overflow-menu-{ordinal}"))
    }) {
        let _ = menu.unchecked_into::<web_sys::HtmlElement>().hide_popover();
    }
}

/// A row's link to its run page, carrying the focused chart.
/// It renders the anchor itself and reads only the chart-focus memo, since the router's `Link` reads the whole route and so re-renders on every navigation, a maximize included.
#[component]
fn RunLink(project_id: String, run_id: String, children: Element) -> Element {
    let RowContext { chart_focus, .. } = use_context();
    let route = Route::RunPage {
        project_id,
        run_id,
        chart: chart_focus().into(),
    };
    let href = router().prefix().unwrap_or_default() + &route.to_string();
    rsx! {
        a {
            class: "run-details run-details-link",
            href,
            // Like `Link`: a plain primary click navigates in place; modified and other clicks keep the browser's behavior.
            onclick: move |e: MouseEvent| {
                if e.modifiers().is_empty() && e.trigger_button() == Some(MouseButton::Primary) {
                    e.prevent_default();
                    navigator().push(route.clone());
                }
            },
            {children}
        }
    }
}

/// One run row, its own component so a selection change re-renders only the rows whose props it changes.
#[component]
fn RunRow(
    run: Rc<RunInfo>,
    is_selected: bool,
    is_pending_trash: bool,
    in_trash_mode: bool,
    show_run_ordinals: bool,
    renaming: Option<RenameRunTarget>,
) -> Element {
    let row = use_context::<RowContext>();
    let RowContext {
        listed,
        mutation_ui,
        mut rename_target,
        mut picking_color_for,
        mut action_feedback,
        action_feedback_timer,
        ..
    } = row;
    let TrashMutationUi {
        dashboard: mut state,
        picker: mut trash_pick,
        mut paint,
        busy: trash_busy,
        feedback: mut trash_feedback,
    } = mutation_ui;
    // Marker colors come from localStorage; a changed color bumps this version to repaint them.
    let _ = state.color_version.read();
    let run_id_toggle = run.run_id.clone();
    let run_id_visibility_start = run.run_id.clone();
    let run_id_hover = run.run_id.clone();
    let run_id_select = run.run_id.clone();
    let run_id_key = run.run_id.clone();
    let ordinal = run.ordinal;
    let menu_id = format!("run-overflow-menu-{ordinal}");
    let color = run_color(&run.run_id, ordinal);
    let hover_name = run.run_name.clone();
    let display_name = sidebar_run_label(&run, show_run_ordinals);
    let visibility_title = if is_selected {
        format!("Hide {display_name} from charts")
    } else {
        format!("Show {display_name} on charts")
    };
    let visibility_label = format!("{display_name} visible on charts");
    let status = run.status();
    let row_class = if in_trash_mode && is_pending_trash {
        "sidebar-run sidebar-run-trash sidebar-run-trash-selected"
    } else if in_trash_mode {
        "sidebar-run sidebar-run-trash"
    } else {
        "sidebar-run"
    };
    rsx! {
        div {
            class: "{row_class}",
            // Chart hover matches the raw run ID or name.
            "data-run-id": "{run.run_id}",
            "data-run-name": "{run.run_name}",
            role: in_trash_mode.then_some("checkbox"),
            tabindex: (in_trash_mode && !*trash_busy.read()).then_some("0"),
            aria_checked: if in_trash_mode { Some(is_pending_trash) } else { None },
            aria_disabled: if in_trash_mode { Some(*trash_busy.read()) } else { None },
            aria_label: if in_trash_mode {
                Some(format!("Select {display_name} for Trash"))
            } else {
                None
            },
            // main.rs's detail-zero activation bridge sends ARIA checkboxes a paired down/up.
            // Physical presses come here directly and keep paint armed.
            onmousedown: primary(move |_| {
                if !in_trash_mode || *trash_busy.peek() {
                    return;
                }
                let mut trash = trash_pick.write();
                let Some(pick) = trash.as_mut() else {
                    return;
                };
                paint.set(Some(SelectionPaint::begin(pick, &run_id_select)));
            }),
            onkeydown: move |e: Event<KeyboardData>| {
                if in_trash_mode
                    && !*trash_busy.peek()
                    && !e.is_auto_repeating()
                    && (e.key() == Key::Enter
                        || e.key() == Key::Character(" ".to_string()))
                {
                    e.prevent_default();
                    end_paint_if_active(paint);
                    let mut trash = trash_pick.write();
                    let Some(pick) = trash.as_mut() else {
                        return;
                    };
                    toggle_membership(pick, &run_id_key);
                }
            },
            // In normal mode hover highlights this run on every chart.
            // Trash mode keeps hover local to the deletion-selection row.
            onmouseenter: move |e: Event<MouseData>| {
                if !e.held_buttons().contains(MouseButton::Primary)
                    || paint.peek().is_none()
                {
                    // The window bridge delivers release asynchronously; hover without a held primary button clears stale paint.
                    end_paint_if_active(paint);
                } else if in_trash_mode {
                    if let Some(pick) = trash_pick.write().as_mut() {
                        continue_selection_paint(
                            &mut paint.write(),
                            pick,
                            &listed.read().run_ids,
                            &run_id_hover,
                        );
                    }
                } else {
                    continue_selection_paint(
                        &mut paint.write(),
                        &mut state.selected_runs.write(),
                        &listed.read().run_ids,
                        &run_id_hover,
                    );
                }
                if in_trash_mode {
                    return;
                }
                let rid = js_string(&run_id_hover);
                let name = js_string(&hover_name);
                let _ = js_sys::eval(&format!("window.__kymo_setHl({rid},{name})"));
            },
            onmouseleave: move |_| {
                if !in_trash_mode {
                    let _ = js_sys::eval("window.__kymo_setHl(null)");
                }
            },
            if in_trash_mode {
                span { class: "run-marker-toggle",
                    "aria-hidden": "true",
                    span {
                        // Keep chart visibility visible in Trash mode; the selected row class adds deletion fill and its warning halo without a second state check.
                        class: if is_selected {
                            "run-marker run-marker-filled"
                        } else {
                            "run-marker"
                        },
                        style: "--run-color: {color};",
                    }
                }
            } else {
                // Native checkbox semantics stay intact while the specialized CSS presents the run color as a filled (visible) or hollow (hidden) circle.
                // The label owns the full-height hit target and the row's left padding.
                label {
                    class: "run-marker-toggle",
                    onmousedown: primary(move |e: Event<MouseData>| {
                        // Paint visibility without taking keyboard focus from another control.
                        e.prevent_default();
                        paint.set(Some(SelectionPaint::begin(
                            &mut state.selected_runs.write(),
                            &run_id_visibility_start,
                        )));
                    }),
                    // Cancel the originating pointer click on the label: WebKit forwards it to the input with detail 0, indistinguishable from keyboard activation. This prevents a second toggle after painting on press.
                    onclick: move |e: Event<MouseData>| {
                        if e.data().as_web_event().detail() != 0 {
                            e.prevent_default();
                        }
                    },
                    // WebKit also activates native checkboxes on auxiliary clicks.
                    onauxclick: move |e: Event<PointerData>| e.prevent_default(),
                    input {
                        r#type: "checkbox",
                        class: "run-marker run-marker-input",
                        checked: is_selected,
                        style: "--run-color: {color};",
                        title: "{visibility_title}",
                        aria_label: "{visibility_label}",
                        onchange: move |_| {
                            toggle_membership(&mut state.selected_runs.write(), &run_id_toggle);
                        },
                    }
                }
            }
            if in_trash_mode {
                span { class: "run-details",
                    RunStatusIcon { status }
                    span { class: "run-name-host fade-overflow",
                        span { class: "run-name", "{display_name}" }
                    }
                }
            } else {
                if let Some(target) = renaming {
                    span { class: "run-details",
                        RunStatusIcon { status }
                        InlineRunRename {
                            target,
                            on_close: move |restore_focus: bool| {
                                rename_target.set(None);
                                if restore_focus {
                                    focus_run_overflow_trigger(ordinal);
                                }
                            },
                            on_renamed: move |run_name: String| {
                                row.in_sidebar(|| {
                                    flash_feedback(
                                        action_feedback,
                                        action_feedback_timer,
                                        format!("Renamed to “{run_name}”"),
                                    )
                                });
                            },
                            on_error: move |message: String| {
                                action_feedback.set(message);
                            },
                        }
                    }
                } else {
                    RunLink { project_id: run.project_id.clone(), run_id: run.run_id.clone(),
                        RunStatusIcon { status }
                        // The shared host stays in flow while its pointer-transparent name pops out on hover.
                        span { class: "run-name-host fade-overflow",
                            span { class: "run-name", "{display_name}" }
                        }
                    }
                }
                button {
                    id: "run-overflow-trigger-{ordinal}",
                    class: "run-overflow-trigger icon-button",
                    style: "anchor-name: --run-overflow-{ordinal};",
                    title: "More actions for {display_name}",
                    aria_label: "More actions for {display_name}",
                    aria_haspopup: "dialog",
                    aria_controls: "{menu_id}",
                    popovertarget: "{menu_id}",
                    popovertargetaction: "toggle",
                    MoreIcon {}
                }
                div {
                    id: "{menu_id}",
                    class: "run-overflow-menu",
                    style: "position-anchor: --run-overflow-{ordinal};",
                    popover: "auto",
                    role: "dialog",
                    aria_label: "Actions for {display_name}",
                    // A press's default action focuses what it lands on, or <body> when that can't take focus (the divider, a button in WebKit, or an item, which hides this menu on press). Cancelling every press here keeps focus where it was or where the item put it: the picker, the rename input, or the ⋯ the hiding menu hands it back to. Anything added here must let its press bubble to this handler, and a field here could not be focused by clicking it.
                    onmousedown: |e| e.prevent_default(),
                    button {
                        autofocus: true,
                        onmousedown: primary({
                            let copied_name = run.run_name.clone();
                            move |_| {
                                crate::util::clipboard::write_text(&copied_name);
                                hide_run_menu(ordinal);
                            }
                        }),
                        "Copy run name"
                    }
                    button {
                        onmousedown: primary({
                            let target = RenameRunTarget {
                                project_id: run.project_id.clone(),
                                run_id: run.run_id.clone(),
                                current_name: run.run_name.clone(),
                                display_label: display_name.clone(),
                                ordinal,
                            };
                            move |_| {
                                trash_feedback.set(String::new());
                                action_feedback.set(String::new());
                                hide_run_menu(ordinal);
                                rename_target.set(Some(target.clone()));
                            }
                        }),
                        "Rename"
                    }
                    button {
                        onmousedown: primary({
                            let (run_id, run_label) = (run.run_id.clone(), display_name.clone());
                            move |_| {
                                action_feedback.set(String::new());
                                // The picker is anchored to the row trigger, so pointer and keyboard activation place it identically after the menu exits.
                                hide_run_menu(ordinal);
                                let opened = picking_color_for.peek().as_ref().map_or(0, |target| target.opened + 1);
                                picking_color_for.set(Some(ColorPickerTarget {
                                    run_id: run_id.clone(),
                                    run_label: run_label.clone(),
                                    ordinal,
                                    opened,
                                }));
                            }
                        }),
                        span {
                            class: "run-menu-color-swatch",
                            style: "background: {color};",
                            aria_hidden: "true",
                        }
                        "Change color…"
                    }
                    div { class: "run-overflow-divider" }
                    button {
                        onmousedown: primary({
                            let project_id = run.project_id.clone();
                            let run_id = run.run_id.clone();
                            move |_| {
                                hide_run_menu(ordinal);
                                row.in_sidebar(|| submit_trash_runs(project_id.clone(), vec![run_id.clone()], mutation_ui));
                            }
                        }),
                        TrashIcon {}
                        "Trash"
                    }
                }
            }
        }
    }
}
