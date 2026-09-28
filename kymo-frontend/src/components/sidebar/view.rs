use super::*;
use crate::util::{js_bridge::js_string, TOP_LAYER_SELECTOR};

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
    let chart_focus = use_route::<Route>().chart_param();
    let runs = state.runs.read().clone();
    let selected = state.selected_runs.read().clone();
    let mut filter = use_signal(String::new);
    let mut picking_color_for = use_signal(|| None::<ColorPickerTarget>);
    // Deletion selection is deliberately transient and completely separate
    // from `selected_runs`, whose filled/hollow markers mean "visible on charts".
    let mut trash_pick = use_signal(|| None::<HashSet<String>>);
    // One drag-paint gesture at a time, whichever mode armed it: bulk mode
    // paints the deletion selection, normal mode paints `selected_runs`.
    let mut paint = use_signal(|| None::<SelectionPaint>);
    let trash_busy = use_signal(|| false);
    let mut action_feedback = use_signal(String::new);
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

    use_future({
        let bridge = sidebar_pick_bridge.clone();
        move || {
            let bridge = bridge.clone();
            async move {
                let js = bridge
                    .script(SIDEBAR_PICK_BRIDGE_JS)
                    .replace("__TOP_LAYER_SELECTOR__", &js_string(TOP_LAYER_SELECTOR));
                let mut eval = document::eval(&js);
                while let Ok(action) = eval.recv::<String>().await {
                    end_paint_if_active(paint);
                    if action == "cancel" && trash_pick.peek().is_some() && !*trash_busy.peek() {
                        trash_feedback.set(String::new());
                        close_trash_picker(trash_pick, paint);
                    }
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

    let filter_text = filter.read().to_lowercase();
    let show_run_ordinals = sidebar_needs_run_ordinals(&runs);
    let filtered_runs: Vec<crate::grpc::proto::RunInfo> = if filter_text.is_empty() {
        runs.clone()
    } else {
        runs.iter()
            .filter(|r| {
                r.run_name.to_lowercase().contains(&filter_text)
                    || r.run_id.to_lowercase().contains(&filter_text)
                    || r.ordinal.to_string().contains(&filter_text)
            })
            .cloned()
            .collect()
    };
    let filtered_run_ids: Rc<[String]> = filtered_runs
        .iter()
        .map(|run| run.run_id.clone())
        .collect::<Vec<_>>()
        .into();

    // A push or a locally confirmed partial mutation can remove runs while
    // bulk mode is open. Never leave those stale identities commit-able.
    let active_ids: HashSet<&str> = runs.iter().map(|run| run.run_id.as_str()).collect();
    let pending_needs_prune = trash_pick.peek().as_ref().is_some_and(|pick| {
        pick.iter()
            .any(|run_id| !active_ids.contains(run_id.as_str()))
    });
    if pending_needs_prune {
        end_paint_if_active(paint);
        if let Some(pick) = trash_pick.write().as_mut() {
            pick.retain(|run_id| active_ids.contains(run_id.as_str()));
        }
    }

    let in_trash_mode = trash_pick.read().is_some();
    let pending_trash = trash_pick.read().clone().unwrap_or_default();
    // Close the picker when its anchor, the row's ⋯ trigger, unmounts (filtering, a run-list change, Trash mode); merely skipping its render would reopen it with the row.
    let mut color_picker_target = picking_color_for.read().clone();
    if let Some(orphan) = color_picker_target
        .take_if(|target| in_trash_mode || !filtered_run_ids.contains(&target.run_id))
    {
        picking_color_for.set(None);
        // The picker may own focus; fall back like any other close.
        focus_run_overflow_trigger(orphan.ordinal, true);
    }
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
    let active_rename = rename_target.read().clone();
    let pending_shown = filtered_runs
        .iter()
        .filter(|run| pending_trash.contains(&run.run_id))
        .count();
    let live_warning = live_trash_warning(&runs, &pending_trash);
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
            onkeydown: move |e: Event<KeyboardData>| {
                // Bulk-mode Escape is window-scoped by SIDEBAR_PICK_BRIDGE_JS.
                // Outside bulk mode, retain the existing clear-filter behavior.
                if e.key() == Key::Escape
                    && trash_pick.peek().is_none()
                    && !filter.peek().is_empty()
                {
                    e.stop_propagation();
                    end_paint_if_active(paint);
                    filter.set(String::new());
                }
            },
            div { class: "sidebar-actions",
                button {
                    class: "btn-link",
                    disabled: (active_selected_count == 0 && filtered_run_ids.is_empty())
                        || (in_trash_mode && *trash_busy.read()),
                    onmousedown: primary({
                        let listed_run_ids = filtered_run_ids.clone();
                        move |_| {
                            end_paint_if_active(paint);
                            if in_trash_mode {
                                if let Some(pick) = trash_pick.write().as_mut() {
                                    all_or_none_selection(pick, &listed_run_ids);
                                }
                            } else {
                                all_or_none_selection(&mut state.selected_runs.write(), &listed_run_ids);
                            }
                        }
                    }),
                    {selection_action_label(active_selected_count, !filter_text.is_empty(), in_trash_mode)}
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
                            onmounted: move |e| {
                                spawn(async move {
                                    let _ = e.data().set_focus(true).await;
                                });
                            },
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
                }
            }

            div { class: "sidebar-body",
                if let Some(message) = run_list_empty_message(runs.len(), filtered_runs.len()) {
                    p { class: "sidebar-empty", "{message}" }
                }
                for (visible_index, run) in filtered_runs.iter().enumerate() {
                    {
                        let is_selected = selected.contains(&run.run_id);
                        let is_pending_trash = pending_trash.contains(&run.run_id);
                        let run_id_toggle = run.run_id.clone();
                        let run_id_visibility_start = run.run_id.clone();
                        let run_id_hover = run.run_id.clone();
                        let run_id_link = run.run_id.clone();
                        let run_id_select = run.run_id.clone();
                        let run_id_key = run.run_id.clone();
                        let visible_run_ids_start = filtered_run_ids.clone();
                        let visible_run_ids_paint = filtered_run_ids.clone();
                        let run_id_menu = run.run_id.clone();
                        let ordinal_menu = run.ordinal;
                        let menu_id = format!("run-overflow-menu-{ordinal_menu}");
                        let color = run_color(&run.run_id, run.ordinal);
                        let hover_name = run.run_name.clone();
                        let run_name = run.run_name.clone();
                        let display_name = sidebar_run_label(run, show_run_ordinals);
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
                                key: "{run.run_id}",
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
                                // main.rs's detail-zero activation bridge sends
                                // ARIA checkboxes a paired down/up. Physical
                                // presses come here directly and keep paint armed.
                                onmousedown: primary(move |_| {
                                    if !in_trash_mode || *trash_busy.peek() {
                                        return;
                                    }
                                    let mut trash = trash_pick.write();
                                    let Some(pick) = trash.as_mut() else {
                                        return;
                                    };
                                    paint.set(SelectionPaint::begin(
                                        pick,
                                        &run_id_select,
                                        visible_run_ids_start.clone(),
                                        visible_index,
                                    ));
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
                                // In normal mode hover highlights this run on
                                // every chart. Trash mode keeps hover local to
                                // the deletion-selection row.
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
                                                visible_run_ids_paint.as_ref(),
                                                visible_index,
                                            );
                                        }
                                    } else {
                                        continue_selection_paint(
                                            &mut paint.write(),
                                            &mut state.selected_runs.write(),
                                            visible_run_ids_paint.as_ref(),
                                            visible_index,
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
                                            // Keep chart visibility visible in Trash mode;
                                            // the selected row class adds deletion fill and
                                            // its warning halo without a second state check.
                                            class: if is_selected {
                                                "run-marker run-marker-filled"
                                            } else {
                                                "run-marker"
                                            },
                                            style: "--run-color: {color};",
                                        }
                                    }
                                } else {
                                    // Native checkbox semantics stay intact while the
                                    // specialized CSS presents the run color as a filled
                                    // (visible) or hollow (hidden) circle. The label owns
                                    // the full-height hit target and the row's left padding.
                                    label {
                                        class: "run-marker-toggle",
                                        onmousedown: primary({
                                            let visible_run_ids = filtered_run_ids.clone();
                                            move |e: Event<MouseData>| {
                                                // Paint visibility without taking keyboard focus from another control.
                                                e.prevent_default();
                                                paint.set(SelectionPaint::begin(
                                                    &mut state.selected_runs.write(),
                                                    &run_id_visibility_start,
                                                    visible_run_ids.clone(),
                                                    visible_index,
                                                ));
                                            }
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
                                    if let Some(target) = active_rename
                                        .as_ref()
                                        .filter(|target| target.run_id == run.run_id)
                                        .cloned()
                                    {
                                        {
                                            let return_focus_ordinal = target.ordinal;
                                            rsx! {
                                                span { class: "run-details",
                                                    RunStatusIcon { status }
                                                    InlineRunRename {
                                                        key: "{target.run_id}",
                                                        target,
                                                        on_close: move |restore_focus: bool| {
                                                            rename_target.set(None);
                                                            if restore_focus {
                                                                focus_run_overflow_trigger(return_focus_ordinal, false);
                                                            }
                                                        },
                                                        on_renamed: move |run_name: String| {
                                                            flash_feedback(
                                                                action_feedback,
                                                                action_feedback_timer,
                                                                format!("Renamed to “{run_name}”"),
                                                            );
                                                        },
                                                        on_error: move |message: String| {
                                                            action_feedback.set(message);
                                                        },
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        Link {
                                            class: "run-details run-details-link",
                                            to: Route::RunPage {
                                                project_id: project_id.clone(),
                                                run_id: run_id_link.clone(),
                                                chart: chart_focus.clone().into(),
                                            },
                                            RunStatusIcon { status }
                                            // The shared host stays in flow while its
                                            // pointer-transparent name pops out on hover.
                                            span { class: "run-name-host fade-overflow",
                                                span { class: "run-name", "{display_name}" }
                                            }
                                        }
                                    }
                                    button {
                                        id: "run-overflow-trigger-{ordinal_menu}",
                                        class: "run-overflow-trigger icon-button",
                                        style: "anchor-name: --run-overflow-{ordinal_menu};",
                                        title: "More actions for {display_name}",
                                        aria_label: "More actions for {display_name}",
                                        aria_haspopup: "dialog",
                                        aria_controls: "{menu_id}",
                                        popovertarget: "{menu_id}",
                                        popovertargetaction: "toggle",
                                        onmousedown: primary(move |e: Event<MouseData>| {
                                            e.stop_propagation();
                                        }),
                                        MoreIcon {}
                                    }
                                    div {
                                        id: "{menu_id}",
                                        class: "run-overflow-menu",
                                        style: "position-anchor: --run-overflow-{ordinal_menu};",
                                        popover: "auto",
                                        role: "dialog",
                                        aria_label: "Actions for {display_name}",
                                        button {
                                            autofocus: true,
                                            popovertarget: "{menu_id}",
                                            popovertargetaction: "hide",
                                            onmousedown: primary({
                                                let copied_name = run_name.clone();
                                                move |_| crate::util::clipboard::write_text(&copied_name)
                                            }),
                                            "Copy run name"
                                        }
                                        button {
                                            onmousedown: primary({
                                                let target = RenameRunTarget {
                                                    project_id: project_id.clone(),
                                                    run_id: run_id_menu.clone(),
                                                    current_name: run_name.clone(),
                                                    display_label: display_name.clone(),
                                                    ordinal: ordinal_menu,
                                                };
                                                let menu_id = menu_id.clone();
                                                move |_| {
                                                    trash_feedback.set(String::new());
                                                    action_feedback.set(String::new());
                                                    let target = target.clone();
                                                    let menu_id = menu_id.clone();
                                                    spawn(async move {
                                                        // Finish native popover
                                                        // dismissal before replacing
                                                        // the row label with its input.
                                                        hide_run_popover(&menu_id).await;
                                                        rename_target.set(Some(target));
                                                    });
                                                }
                                            }),
                                            "Rename"
                                        }
                                        button {
                                            onmousedown: primary({
                                                let target = ColorPickerTarget {
                                                    run_id: run_id_menu.clone(),
                                                    run_label: display_name.clone(),
                                                    ordinal: ordinal_menu,
                                                };
                                                let menu_id = menu_id.clone();
                                                move |_| {
                                                    action_feedback.set(String::new());
                                                    let target = target.clone();
                                                    let menu_id = menu_id.clone();
                                                    spawn(async move {
                                                        // The picker is anchored to the
                                                        // row trigger, so pointer and
                                                        // keyboard activation place it
                                                        // identically after the menu exits.
                                                        hide_run_popover(&menu_id).await;
                                                        picking_color_for.set(Some(target));
                                                    });
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
                                            disabled: *trash_busy.read(),
                                            popovertarget: "{menu_id}",
                                            popovertargetaction: "hide",
                                            onmousedown: primary({
                                                let project_id = project_id.clone();
                                                let run_id = run_id_menu.clone();
                                                let menu_id = menu_id.clone();
                                                move |_| {
                                                    let project_id = project_id.clone();
                                                    let run_id = run_id.clone();
                                                    let menu_id = menu_id.clone();
                                                    spawn(async move {
                                                        hide_run_popover(&menu_id).await;
                                                        submit_trash_runs(
                                                            project_id,
                                                            vec![run_id],
                                                            mutation_ui,
                                                        );
                                                    });
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
                        class: "sidebar-trash-commit",
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

            div {
                class: "sidebar-resize",
                onmounted: move |_| {
                    spawn(async move {
                        let _ = document::eval(SIDEBAR_RESIZE_JS).await;
                    });
                },
            }
        }

        if let Some(ColorPickerTarget { run_id, run_label, ordinal }) = color_picker_target {
            {
                let color = run_color(&run_id, ordinal);
                rsx! {
                    ColorPicker {
                        run_id,
                        run_label,
                        current_color: color,
                        anchor_ordinal: ordinal,
                        on_close: move |_| {
                            picking_color_for.set(None);
                            let v = *state.color_version.read();
                            state.color_version.set(v + 1);
                            // Pointer light-dismiss may already have moved focus
                            // to another control; do not steal it back.
                            focus_run_overflow_trigger(ordinal, true);
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
