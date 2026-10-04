"""Exercise native editor dismissal, browser modality, and live-edit rollback.

It only changes browser-local layout/settings; it never mutates runs.
"""

import argparse
import json
import math
import re
from urllib.parse import unquote, urlsplit

from playwright.sync_api import Locator, Page, expect, sync_playwright

from user_settings_fences import (
    VIEWPORT,
    assert_native_focus,
    backdrop_cancel,
    native_dialog,
    open_editor,
)


def pin_trigger(page: Page, trigger: Locator) -> Locator:
    identifier = trigger.get_attribute("id")
    assert identifier, "editor trigger needs a stable identity for focus restoration"
    return page.locator(f"[id={json.dumps(identifier)}]")


def closed(page: Page, trigger: Locator) -> None:
    expect(page.locator("dialog:modal")).to_have_count(0)
    expect(page.locator("dialog.editor-dialog")).to_have_count(0)
    expect(trigger).to_be_focused()


def dismiss(page: Page, dialog: Locator, trigger: Locator, how: str) -> None:
    if how == "blurred Escape":
        page.evaluate("document.activeElement.blur()")
        assert page.evaluate("document.activeElement === document.body")
        page.keyboard.press("Escape")
    elif how == "Escape":
        assert dialog.evaluate("element => element.contains(document.activeElement)"), (
            "focused Escape needs focus inside the editor"
        )
        page.keyboard.press("Escape")
    elif how == "backdrop":
        backdrop_cancel(page, dialog)
    else:
        assert how in ("Cancel", "Save"), f"unknown dismissal: {how}"
        dialog.get_by_role("button", name=how, exact=True).click()
    closed(page, trigger)


def open_and_escape(page: Page, trigger: Locator, name: str) -> None:
    # A genuine down/up sequence catches mouseup moving focus back to the
    # trigger after an editor opened on mousedown. Do not repair focus here.
    trigger.hover()
    page.mouse.down()
    dialog = native_dialog(page, name)
    panel = dialog.locator(".modal")
    expect(panel).to_be_focused()
    page.mouse.up()
    expect(panel).to_be_focused()
    dismiss(page, dialog, trigger, "Escape")

    for key in ("Enter", "Space"):
        dialog = open_editor(page, trigger, name, key=key)
        panel = dialog.locator(".modal")
        expect(panel).to_be_focused()
        # A second/repeated Enter must not activate Remove source or a reset
        # button just because it happened to be the first editor control.
        page.keyboard.press("Enter")
        expect(panel).to_be_focused()
        if key == "Enter":
            assert_native_focus(page, dialog, trigger)
        dismiss(page, dialog, trigger, "blurred Escape")


def log_y_control(dialog: Locator) -> Locator:
    return dialog.get_by_role("checkbox", name="Log Y", exact=True)


def toggle_live(page: Page, control: Locator, storage_key: str) -> bool:
    before = page.evaluate("key => localStorage.getItem(key)", storage_key)
    original = control.is_checked()
    identifier = control.get_attribute("id")
    assert identifier
    page.locator(f"label[for={json.dumps(identifier)}]").click()
    expect(control).to_be_checked(checked=not original)
    expect(control).to_be_focused()
    page.wait_for_function(
        "([key, before]) => localStorage.getItem(key) !== before",
        arg=[storage_key, before],
    )
    return original


def live_edit_paths(page: Page, trigger: Locator, name: str, storage_key: str) -> None:
    for method in ("Escape", "Cancel", "backdrop"):
        print(f"{name}: rollback via {method}", flush=True)
        dialog = open_editor(page, trigger, name)
        original = toggle_live(page, log_y_control(dialog), storage_key)
        dismiss(page, dialog, trigger, method)
        dialog = open_editor(page, trigger, name)
        expect(log_y_control(dialog)).to_be_checked(checked=original)
        dismiss(page, dialog, trigger, "Escape")

    dialog = open_editor(page, trigger, name)
    original = toggle_live(page, log_y_control(dialog), storage_key)
    dialog.get_by_role("button", name="Save", exact=True).press("Enter")
    closed(page, trigger)
    dialog = open_editor(page, trigger, name)
    expect(log_y_control(dialog)).to_be_checked(checked=not original)
    toggle_live(page, log_y_control(dialog), storage_key)
    dismiss(page, dialog, trigger, "Save")


def smoothing_hint(page: Page, trigger: Locator, name: str) -> None:
    dialog = open_editor(page, trigger, name)
    selector = dialog.get_by_role("combobox", name="Smoothing", exact=True)
    selector.select_option("savgol")
    actions = dialog.locator(".modal-actions")
    hint = dialog.locator(".smoothing-hover-hint")
    try:
        for width in (VIEWPORT["width"], 900):
            page.set_viewport_size({"width": width, "height": VIEWPORT["height"]})
            selector.scroll_into_view_if_needed()
            before = actions.bounding_box()
            selector.hover()
            expect(hint).to_be_visible()
            assert hint.evaluate("element => !!element.closest('dialog:modal')")
            bounds = hint.bounding_box()
            assert bounds is not None
            assert 0 <= bounds["x"] < bounds["x"] + bounds["width"] <= width
            assert (
                0 <= bounds["y"] < bounds["y"] + bounds["height"] <= VIEWPORT["height"]
            )
            if width < 1000:
                selector_bounds = selector.bounding_box()
                assert selector_bounds is not None
                assert bounds["y"] + bounds["height"] <= selector_bounds["y"], (
                    "narrow smoothing hint covers the smoothing select"
                )
            assert actions.bounding_box() == before, (
                "hover help moved the editor controls"
            )
    finally:
        page.set_viewport_size(VIEWPORT)
    dismiss(page, dialog, trigger, "Escape")


def chart_option_fields(
    page: Page, trigger: Locator, name: str, storage_key: str
) -> None:
    print(f"{name}: shared chart options", flush=True)
    dialog = open_editor(page, trigger, name)
    algorithm = dialog.get_by_role("combobox", name="Smoothing", exact=True)
    order = dialog.get_by_role("combobox", name="Fit order", exact=True)
    window = dialog.get_by_role("spinbutton", name="Window", exact=True)
    window_slider = dialog.get_by_role("slider", name="Window", exact=True)
    tau = dialog.get_by_role("spinbutton", name="Time constant (steps)", exact=True)
    tau_slider = dialog.get_by_role("slider", name="Time constant (steps)", exact=True)
    log_x = dialog.get_by_role("checkbox", name="Log X", exact=True)
    max_runs = dialog.get_by_role("spinbutton", name="Max runs", exact=True)
    scope, identity = "project", None
    if name == "Configure section":
        scope = "section"
        identity = dialog.get_by_role("textbox", name="Name", exact=True).get_attribute(
            "placeholder"
        )
    elif name == "Configure Metric":
        scope = "rect"
        identity = trigger.locator("xpath=ancestor::*[@data-slot-id][1]").get_attribute(
            "data-slot-id"
        )
    read_options = """([key, scope, identity]) => {
        const diff = JSON.parse(localStorage.getItem(key) || '{}');
        if (scope === 'project') return diff.project_chart_defaults || {};
        const patch = (diff[scope + '_overrides'] || [])
            .find(entry => entry.key === identity)?.patch || {};
        return patch[scope === 'section' ? 'chart_defaults' : 'options'] || {};
    }"""
    storage_args = [storage_key, scope, identity]

    def stored_options() -> dict:
        return page.evaluate(read_options, storage_args)

    def expect_option(field: str, value: float | bool | str) -> None:
        page.wait_for_function(
            f"""args => {{
                const options = ({read_options})(args.slice(0, 3));
                const actual = options[args[3]], expected = args[4];
                return typeof expected === 'number'
                    ? typeof actual === 'number' && Math.abs(actual - expected) < 1e-12
                    : actual === expected;
            }}""",
            arg=[*storage_args, field, value],
        )

    def reset_to_inherited(control: Locator) -> str | bool:
        reset = control.locator(
            "xpath=ancestor::div[contains(@class, 'binding-field')][1]"
        ).get_by_title("Reset to inherited value", exact=True)
        if reset.count():
            reset.click()
        expect(reset).to_have_count(0)
        return (
            control.is_checked()
            if control.get_attribute("type") == "checkbox"
            else control.input_value()
        )

    inherited_log_x = reset_to_inherited(log_x)
    saved_log_x = not inherited_log_x
    dialog.locator("label").filter(has_text=re.compile(r"^Log X$")).click()
    expect(log_x).to_be_checked(checked=saved_log_x)
    expect(log_x).to_be_focused()
    expect_option("log_x", saved_log_x)
    assert reset_to_inherited(log_x) == inherited_log_x
    log_x.set_checked(saved_log_x)
    inherited_max_runs = reset_to_inherited(max_runs)
    if scope == "project":
        for value, clamped in (("65", "64"), ("0", "0")):
            max_runs.fill(value)
            expect(max_runs).to_have_value(clamped)
            if clamped != inherited_max_runs:
                expect_option("max_runs", int(clamped))
    saved_max_runs = "17" if inherited_max_runs != "17" else "23"
    max_runs.fill(saved_max_runs)
    expect_option("max_runs", int(saved_max_runs))
    assert reset_to_inherited(max_runs) == inherited_max_runs
    max_runs.fill(saved_max_runs)
    inherited_algorithm = reset_to_inherited(algorithm)
    dialog.locator("label").filter(has_text=re.compile(r"^Smoothing$")).click()
    expect(algorithm).to_be_focused()
    before = page.evaluate("key => localStorage.getItem(key)", storage_key)
    algorithm.select_option("none" if inherited_algorithm != "none" else "triangular")
    page.wait_for_function(
        "([key, before]) => localStorage.getItem(key) !== before",
        arg=[storage_key, before],
    )
    assert reset_to_inherited(algorithm) == inherited_algorithm
    if scope == "project":
        for value in ("none", "triangular", "ema-polyfit", "savgol"):
            algorithm.select_option(value)
            expect(order).to_have_count(int(value != "none"))
            expect(window).to_have_count(int(value == "savgol"))
            expect(window_slider).to_have_count(int(value == "savgol"))
            expect(tau).to_have_count(int(value == "ema-polyfit"))
            expect(tau_slider).to_have_count(int(value == "ema-polyfit"))
    algorithm.select_option("savgol")

    inherited_order = reset_to_inherited(order)
    saved_order = "2" if inherited_order != "2" else "0"
    order.select_option(saved_order)
    expect_option("smoothing_poly_order", int(saved_order))
    assert reset_to_inherited(order) == inherited_order
    order.select_option(saved_order)

    inherited_window = reset_to_inherited(window)
    if scope == "project":
        for key, value in (
            ("Home", "3"),
            ("ArrowLeft", "3"),
            ("End", "500"),
            ("ArrowRight", "500"),
        ):
            window_slider.press(key)
            expect(window).to_have_value(value)
        for value, clamped in (("1", "3"), ("999", "500")):
            window.fill(value)
            expect(window).to_have_value(clamped)
            expect(window_slider).to_have_value(clamped)
    saved_window = "43" if inherited_window != "43" else "47"
    window.fill(saved_window)
    expect_option("smoothing_window", int(saved_window))
    assert reset_to_inherited(window) == inherited_window
    window.fill(saved_window)

    algorithm.select_option("ema-polyfit")
    inherited_tau = reset_to_inherited(tau)
    if scope == "project":
        for key, value in (("Home", "1"), ("End", "500")):
            tau_slider.press(key)
            expect(tau).to_have_value(value)
        for value, clamped, slider_value in (("0", "1", "1"), ("1001", "1000", "500")):
            tau.fill(value)
            expect(tau).to_have_value(clamped)
            expect(tau_slider).to_have_value(slider_value)
    saved_tau = "37" if inherited_tau != "37" else "41"
    tau.fill(saved_tau)
    expect_option("smoothing_alpha", 1.0 - math.exp(-1.0 / int(saved_tau)))
    assert reset_to_inherited(tau) == inherited_tau
    tau.fill(saved_tau)
    algorithm.select_option("savgol")
    expect(window).to_have_value(saved_window)
    expect(order).to_have_value(saved_order)

    if scope != "project":
        toggle = dialog.get_by_role(
            "button",
            name="Smoothing" if scope == "rect" else "Chart defaults",
            exact=True,
        )
        toggle.press("Enter")
        expect(toggle).to_have_attribute("aria-expanded", "false")
        expect(algorithm).to_have_count(0)
        toggle.press("Space")
        expect(toggle).to_have_attribute("aria-expanded", "true")
        expect(algorithm).to_have_value("savgol")
        expect(window).to_have_value(saved_window)
        expect(order).to_have_value(saved_order)

    dismiss(page, dialog, trigger, "Save")
    saved_options = stored_options()
    dialog = open_editor(page, trigger, name)
    expect(log_x).to_be_checked(checked=saved_log_x)
    expect(max_runs).to_have_value(saved_max_runs)
    expect(algorithm).to_have_value("savgol")
    expect(window).to_have_value(saved_window)
    expect(order).to_have_value(saved_order)
    algorithm.select_option("ema-polyfit")
    expect(tau).to_have_value(saved_tau)
    expect_option("smoothing", "EmaPolyfit")
    dismiss(page, dialog, trigger, "Cancel")
    restored_options = stored_options()
    assert restored_options.keys() == saved_options.keys(), (
        f"Cancel retained smoothing edits: saved={saved_options}, restored={restored_options}"
    )
    # Reopening round-trips alpha through JSON; use the live-edit numeric tolerance.
    for field, value in saved_options.items():
        expect_option(field, value)


def chart_option_override_chips(
    page: Page, metric: Locator, section: Locator, defaults: Locator
) -> None:
    dialog = open_editor(page, section, "Configure section")
    inherited_window = dialog.get_by_role(
        "spinbutton", name="Window", exact=True
    ).input_value()
    inherited_max_runs = dialog.get_by_role(
        "spinbutton", name="Max runs", exact=True
    ).input_value()
    dismiss(page, dialog, section, "Cancel")
    for trigger, name, pinned, limit in (
        (section, "Configure section", "137", "19"),
        (defaults, "Project chart defaults", "149", "29"),
    ):
        dialog = open_editor(page, metric, "Configure Metric")
        dialog.get_by_role("spinbutton", name="Window", exact=True).fill(pinned)
        dialog.get_by_role("spinbutton", name="Max runs", exact=True).fill(limit)
        dismiss(page, dialog, metric, "Save")
        dialog = open_editor(page, trigger, name)
        for value in (pinned, limit):
            chip = dialog.get_by_role("button").filter(
                has_text=re.compile(rf" = {value}\s*$")
            )
            expect(chip).to_have_count(1)
            chip.click()
            expect(chip).to_have_count(0)
        dismiss(page, dialog, trigger, "Save")
        dialog = open_editor(page, metric, "Configure Metric")
        expect(
            dialog.get_by_role("spinbutton", name="Window", exact=True)
        ).to_have_value(inherited_window)
        expect(
            dialog.get_by_role("spinbutton", name="Max runs", exact=True)
        ).to_have_value(inherited_max_runs)
        dismiss(page, dialog, metric, "Cancel")


def metric_picker(page: Page, trigger: Locator, storage_key: str) -> None:
    """Sorted typed groups, substring filter, type gating, and a multi-run specific source, all rolled back by Cancel."""
    print("Configure Metric: metric picker", flush=True)
    identity = trigger.locator("xpath=ancestor::*[@data-slot-id][1]").get_attribute(
        "data-slot-id"
    )
    read_bindings = """([key, identity]) => {
        const diff = JSON.parse(localStorage.getItem(key) || '{}');
        return (diff.rect_overrides || []).find(entry => entry.key === identity)
            ?.patch?.bindings ?? null;
    }"""
    saved_bindings = page.evaluate(read_bindings, [storage_key, identity])
    dialog = open_editor(page, trigger, "Configure Metric")
    metric = dialog.get_by_role("combobox", name="Source 1 metric", exact=True)
    groups = metric.locator("optgroup")
    expect(groups.first).to_be_attached()
    current = metric.input_value()
    assert current, "the fence chart needs a bound metric"
    labels = groups.evaluate_all("groups => groups.map(group => group.label)")
    assert len(labels) >= 2, "type gating needs metrics of different types"
    assert labels == [
        label for label in ("Numeric", "Media", "Text logs") if label in labels
    ], f"metric groups out of order: {labels}"
    listed = groups.evaluate_all(
        "groups => groups.map(group => [...group.children].map(option => option.value))"
    )
    for names in listed:
        assert names == sorted(names), f"metric group is not sorted: {names[:8]}"
    catalog = [name for names in listed for name in names]

    # Case-insensitive substring filter; the saved metric stays selected even when filtered out.
    other = next(
        (n for n in catalog if n != current and len(n) >= 4 and " " not in n), None
    )
    if other is not None:
        needle = other[1:4].upper()
        box = dialog.get_by_role("textbox", name="Source 1 metric filter", exact=True)
        box.fill(needle)
        matches = [name for name in catalog if needle.lower() in name.lower()]
        expect(dialog.locator(".binding-filter-count")).to_have_text(
            f"{len(matches)} of {len(catalog)}"
        )
        shown = groups.locator("option").evaluate_all(
            "options => options.map(option => option.value)"
        )
        assert set(shown) == set(matches) | {current}, (
            f"filter {needle!r} listed {shown[:8]}"
        )
        expect(metric).to_have_value(current)
        box.fill("")
        expect(groups.locator("option")).to_have_count(len(catalog))

    # A second source may pick only the first source's type.
    kind = metric.evaluate("select => select.selectedOptions[0].parentElement.label")
    dialog.get_by_role("button", name="+ Add Source", exact=True).click()
    second = dialog.get_by_role("combobox", name="Source 2 metric", exact=True)
    expect(second.locator("optgroup")).to_have_count(len(labels))
    expect(second.locator("optgroup:not([disabled])")).to_have_attribute("label", kind)
    for group in second.locator("optgroup[disabled]").all():
        expect(group).to_have_attribute(
            "label", re.compile(rf" — other sources are {kind.lower()}$")
        )
    dialog.get_by_role("button", name="Remove source 2", exact=True).click()
    expect(second).to_have_count(0)

    # Specific Runs: several checked at once, live-applied in pick order.
    dialog.get_by_role("combobox", name="Source 1 runs", exact=True).select_option(
        label="Specific Runs"
    )
    boxes = dialog.get_by_role(
        "group", name="Source 1 specific runs", exact=True
    ).get_by_role("checkbox")
    expect(boxes.first).to_be_attached()
    assert boxes.count() >= 2, "multi-run picker needs two active runs"
    rows = dialog.locator(".binding-run-list label")
    checked = dialog.locator(".binding-run-list input:checked")
    while checked.count():
        checked.first.uncheck()
    picked = [rows.nth(index).get_attribute("title") for index in (1, 0)]
    boxes.nth(1).check()
    boxes.nth(0).focus()
    page.keyboard.press("Space")
    expect(boxes.nth(0)).to_be_checked()
    page.wait_for_function(
        f"""args => {{
            const bindings = ({read_bindings})(args.slice(0, 2));
            return JSON.stringify(bindings?.[0]?.runs) === JSON.stringify(args[2]);
        }}""",
        arg=[storage_key, identity, {"Specific": picked}],
    )

    # Per-source state stays with its source when an earlier source is removed.
    dialog.get_by_role("button", name="+ Add Source", exact=True).click()
    first, second = (
        dialog.get_by_role("textbox", name=f"Source {n} metric filter", exact=True)
        for n in (1, 2)
    )
    first.fill("first source")
    second.fill("second source")
    dialog.get_by_role("button", name="Remove source 1", exact=True).click()
    expect(second).to_have_count(0)
    expect(first).to_have_value("second source")
    expect(
        dialog.get_by_role("combobox", name="Source 1 metric", exact=True)
    ).to_have_value("")
    dismiss(page, dialog, trigger, "Cancel")
    page.wait_for_function(
        f"""args => JSON.stringify(({read_bindings})(args.slice(0, 2)))
            === JSON.stringify(args[2])""",
        arg=[storage_key, identity, saved_bindings],
    )
    dialog = open_editor(page, trigger, "Configure Metric")
    expect(
        dialog.get_by_role("group", name="Source 1 specific runs", exact=True)
    ).to_have_count(0)
    expect(
        dialog.get_by_role("combobox", name="Source 1 metric", exact=True)
    ).to_have_value(current)
    dismiss(page, dialog, trigger, "Escape")


def section_fields(page: Page, section: Locator, storage_key: str) -> None:
    dialog = open_editor(page, section, "Configure section")
    fields = (
        dialog.get_by_role("textbox", name="Name", exact=True),
        dialog.get_by_role("spinbutton", name="Columns", exact=True),
        dialog.get_by_role("spinbutton", name="Rows / page", exact=True),
    )
    original = [field.input_value() for field in fields]
    edited = [
        f"Edited {original[0] or 'section'}",
        "1" if original[1] != "1" else "2",
        "0" if original[2] != "0" else "1",
    ]
    dismiss(page, dialog, section, "Escape")
    for method in ("Escape", "Save"):
        dialog = open_editor(page, section, "Configure section")
        for field, value in zip(fields, edited):
            before = page.evaluate("key => localStorage.getItem(key)", storage_key)
            field.fill(value)
            page.wait_for_function(
                "([key, before]) => localStorage.getItem(key) !== before",
                arg=[storage_key, before],
            )
        dismiss(page, dialog, section, method)
        dialog = open_editor(page, section, "Configure section")
        for field, value in zip(fields, original if method == "Escape" else edited):
            expect(field).to_have_value(value)
        dismiss(page, dialog, section, "Cancel")


def open_color_picker(page: Page, picker: Locator) -> None:
    page.locator(".run-overflow-trigger").first.press("Enter")
    page.locator(".run-overflow-menu:popover-open").get_by_role(
        "button", name="Change color…", exact=True
    ).press("Enter")
    expect(picker).to_be_focused()


def color_picker_dismissal(page: Page) -> None:
    picker = page.locator(".color-picker")
    open_color_picker(page, picker)
    page.keyboard.press("Escape")
    expect(picker).to_have_count(0)
    expect(page.locator(".run-overflow-trigger").first).to_be_focused()

    # Filtering out the picker's row while the picker keeps focus closes it and returns focus to the filter.
    open_color_picker(page, picker)
    run_filter = page.locator(".sidebar-filter")
    run_filter.evaluate(
        """element => {
            element.value = 'no run matches this filter';
            element.dispatchEvent(new Event('input', {bubbles: true}));
        }"""
    )
    expect(picker).to_have_count(0)
    expect(run_filter).to_be_focused()
    page.keyboard.press("Escape")
    expect(run_filter).to_have_value("")

    open_color_picker(page, picker)
    trash = page.locator("#sidebar-trash-trigger")
    for _ in range(page.locator("button,input,a[href]").count() + 3):
        if trash.evaluate("element => element === document.activeElement"):
            break
        page.keyboard.press("Shift+Tab")
        expect(picker).to_be_visible()
    expect(trash).to_be_focused()
    page.keyboard.press("Enter")
    bulk = page.locator(".sidebar-trash-mode")
    expect(bulk).to_be_visible()
    expect(picker).to_have_count(0)
    page.keyboard.press("Escape")
    expect(bulk).to_have_count(0)
    expect(trash).to_be_focused()


def run_fences(page: Page) -> None:
    page.set_viewport_size(VIEWPORT)
    page.locator(".chart-container").first.wait_for(timeout=20_000)
    expect(
        page.locator(".run-overflow-trigger").first,
        "editor dialog fences require an active sidebar run for the colour-picker case",
    ).to_be_attached()
    parsed = urlsplit(page.url)
    project_id = unquote(parsed.path.split("/", 2)[1])
    storage_key = "kymo_layout_diff_" + project_id
    chart = (
        page.locator(".metric-rect").filter(has=page.locator(".chart-container")).first
    )
    metric = pin_trigger(page, chart.locator('button[title="Configure"]'))
    section = pin_trigger(
        page,
        chart.locator(
            "xpath=ancestor::div[contains(concat(' ', @class, ' '), ' section ')][1]"
        ).locator('.section-header button[title="Configure"]'),
    )
    defaults = pin_trigger(
        page, page.get_by_title("Project chart defaults (inherited by every chart)")
    )
    editors = (
        (defaults, "Project chart defaults"),
        (section, "Configure section"),
        (metric, "Configure Metric"),
    )
    for trigger, name in editors:
        open_and_escape(page, trigger, name)
        live_edit_paths(page, trigger, name, storage_key)
        smoothing_hint(page, trigger, name)

    saved_layout = page.evaluate("key => localStorage.getItem(key)", storage_key)
    try:
        for trigger, name in editors:
            chart_option_fields(page, trigger, name, storage_key)
        chart_option_override_chips(page, metric, section, defaults)
        section_fields(page, section, storage_key)
        metric_picker(page, metric, storage_key)
    finally:
        # Restore saved pins and section settings, including when an assertion fails.
        page.evaluate(
            """([key, value]) => value === null
                ? localStorage.removeItem(key) : localStorage.setItem(key, value)""",
            [storage_key, saved_layout],
        )
        page.reload(wait_until="domcontentloaded")
        page.locator(".chart-container").first.wait_for(timeout=20_000)

    # The sidebar's window-capture Esc listener must leave bulk selection active when the modal cancels.
    bulk = page.locator("#sidebar-trash-trigger")
    bulk.press("Enter")
    expect(page.locator(".sidebar-trash-mode")).to_be_visible()
    dialog = open_editor(page, defaults, "Project chart defaults")
    dismiss(page, dialog, defaults, "Escape")
    expect(page.locator(".sidebar-trash-mode")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator(".sidebar-trash-mode")).to_have_count(0)
    # Focus outside the sidebar (back on the dialog's trigger) is not the picker's to move.
    expect(defaults).to_be_focused()

    color_picker_dismissal(page)

    chart.locator('button[title="Maximize"]').click()
    overlay = page.locator(".maximize-overlay")
    expect(overlay).to_be_visible()
    maximized_trigger = pin_trigger(page, overlay.locator('button[title="Configure"]'))
    for method in ("backdrop", "Cancel", "Escape", "blurred Escape"):
        dialog = open_editor(page, maximized_trigger, "Configure Metric")
        if method == "Escape":
            expect(dialog.locator(".modal")).to_be_focused()
        dismiss(page, dialog, maximized_trigger, method)
        expect(overlay).to_be_visible()
    dialog = open_editor(page, defaults, "Project chart defaults")
    expect(dialog.locator(".modal")).to_be_focused()
    dismiss(page, dialog, defaults, "Escape")
    expect(overlay).to_be_visible()
    page.keyboard.press("Escape")
    expect(overlay).to_have_count(0)

    # Inline header rename keeps its own Esc behavior; it is not an editor
    # modal and must not be caught by the new native cancellation path.
    chart.locator('button[title="Maximize"]').click()
    title = overlay.locator(".rect-title")
    original_title = title.inner_text()
    title.dblclick()
    rename = overlay.locator(".rect-title-input")
    rename.fill("cancelled dialog fence rename")
    rename.press("Escape")
    expect(rename).to_have_count(0)
    expect(overlay).to_be_visible()
    expect(title).to_have_text(original_title)
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)

    page.goto(f"{parsed.scheme}://{parsed.netloc}/", wait_until="domcontentloaded")
    settings = pin_trigger(
        page, page.get_by_role("button", name="Settings", exact=True)
    )
    open_and_escape(page, settings, "Settings")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="dashboard project URL")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        try:
            page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1)
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("native editor dialog fences passed")


if __name__ == "__main__":
    main()
