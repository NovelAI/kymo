"""Exercise the docked options panel: dismissal and focus, live edits that Esc and Close keep, Revert, and the editors' fields.

It only changes browser-local layout/settings; it never mutates runs.
"""

import argparse
import json
import math
import re
from contextlib import contextmanager
from urllib.parse import unquote, urlencode, urlsplit

from playwright.sync_api import Locator, Page, expect, sync_playwright

from fences_common import drag_handle, request_frame

from user_settings_fences import (
    VIEWPORT,
    close_panel,
    count_presses,
    double_click_acts_once,
    open_editor,
    options_panel,
    panel_body,
    settings_trigger,
)


# The section element around a locator.
SECTION_OF = "xpath=ancestor::div[contains(concat(' ', @class, ' '), ' section ')][1]"


def pin_trigger(page: Page, trigger: Locator) -> Locator:
    identifier = trigger.get_attribute("id")
    assert identifier, "editor trigger needs a stable identity for focus restoration"
    return page.locator(f"[id={json.dumps(identifier)}]")


def configure_maximizes(trigger: Locator) -> bool:
    """Whether `trigger` is a grid chart's Configure, which maximizes the chart beside its panel."""
    return trigger.evaluate(
        "element => !!element.closest('.metric-rect') && !element.closest('.maximize-overlay')"
    )


def saving(page: Page, storage_key: str, action) -> None:
    """Run `action` and wait until it has changed the stored layout."""
    before = page.evaluate("key => localStorage.getItem(key)", storage_key)
    action()
    page.wait_for_function(
        "([key, before]) => localStorage.getItem(key) !== before",
        arg=[storage_key, before],
    )


def dismiss(page: Page, panel: Locator, trigger: Locator, how: str) -> None:
    close_panel(page, panel, trigger, how)
    if configure_maximizes(trigger):
        # Closing returns to the grid; wait for its history pop too, or the next step's Configure can land before it and be closed by it.
        expect(page.locator(".maximize-overlay")).to_have_count(0)
        expect(page).not_to_have_url(re.compile(r"[?&]chart="))


def open_and_escape(page: Page, trigger: Locator, name: str) -> None:
    # A genuine down/up sequence catches mouseup moving focus back to the trigger after a panel opened on mousedown. Do not repair focus here.
    trigger.hover()
    page.mouse.down()
    panel = options_panel(page, name)
    expect(panel_body(panel)).to_be_focused()
    page.mouse.up()
    expect(panel_body(panel)).to_be_focused()
    dismiss(page, panel, trigger, "Escape")

    for key in ("Enter", "Space"):
        panel = open_editor(page, trigger, name, key=key)
        # A second/repeated Enter must not activate Remove source or a reset button just because it happened to be the first editor control.
        page.keyboard.press("Enter")
        expect(panel_body(panel)).to_be_focused()
        dismiss(page, panel, trigger, "Escape" if key == "Enter" else "Close")

    # Esc from outside the panel keeps its page meaning: on a chart that Configure maximized, it closes the chart and the chart's panel with it.
    panel = open_editor(page, trigger, name)
    page.evaluate("document.activeElement.blur()")
    page.keyboard.press("Escape")
    if configure_maximizes(trigger):
        expect(page.locator(".maximize-overlay")).to_have_count(0)
        expect(panel).to_have_count(0)
    else:
        expect(panel).to_be_visible()
        dismiss(page, panel, trigger, "Close")


def log_y_control(panel: Locator) -> Locator:
    return panel.get_by_role("checkbox", name="Log Y", exact=True)


def expect_focused_by_label(page: Page, control: Locator) -> None:
    """A label click focuses its control, except a checkbox in WebKit."""
    if (
        page.context.browser.browser_type.name != "webkit"
        or control.get_attribute("type") != "checkbox"
    ):
        expect(control).to_be_focused()


def toggle_live(page: Page, control: Locator, storage_key: str) -> bool:
    original = control.is_checked()
    identifier = control.get_attribute("id")
    assert identifier
    saving(
        page,
        storage_key,
        lambda: page.locator(f"label[for={json.dumps(identifier)}]").click(),
    )
    expect(control).to_be_checked(checked=not original)
    expect_focused_by_label(page, control)
    return original


def keep_and_revert(page: Page, trigger: Locator, name: str, storage_key: str) -> None:
    print(f"{name}: Escape keeps the edit", flush=True)
    panel = open_editor(page, trigger, name)
    original = toggle_live(page, log_y_control(panel), storage_key)
    dismiss(page, panel, trigger, "Escape")
    panel = open_editor(page, trigger, name)
    expect(log_y_control(panel)).to_be_checked(checked=not original)
    toggle_live(page, log_y_control(panel), storage_key)
    dismiss(page, panel, trigger, "Escape")

    print(f"{name}: Revert restores and stays open", flush=True)
    panel = open_editor(page, trigger, name)
    revert = panel.get_by_role("button", name="Revert", exact=True)
    expect(revert).to_be_disabled()
    original = toggle_live(page, log_y_control(panel), storage_key)
    expect(revert).to_be_enabled()
    saving(page, storage_key, revert.click)
    expect(log_y_control(panel)).to_be_checked(checked=original)
    expect(revert).to_be_disabled()
    expect(panel).to_be_visible()
    dismiss(page, panel, trigger, "Close")
    panel = open_editor(page, trigger, name)
    expect(log_y_control(panel)).to_be_checked(checked=original)

    # Revert disables itself; from the keyboard, focus stays in the panel so Esc still closes it.
    toggle_live(page, log_y_control(panel), storage_key)
    revert.focus()
    page.keyboard.press("Enter")
    expect(log_y_control(panel)).to_be_checked(checked=original)
    expect(panel_body(panel)).to_be_focused()
    dismiss(page, panel, trigger, "Escape")


def assert_on_top(element: Locator, what: str) -> None:
    """Hit-test `element`'s corners and centre, with pointer events on for the probe: nothing may paint over it."""
    covering = element.evaluate(
        """element => {
            const saved = element.style.pointerEvents;
            element.style.pointerEvents = "auto";
            const box = element.getBoundingClientRect();
            const points = [
                [box.left + 2, box.top + 2],
                [box.left + box.width / 2, box.top + box.height / 2],
                [box.right - 2, box.bottom - 2],
            ];
            const covering = points
                .map(([x, y]) => document.elementFromPoint(x, y))
                .filter(hit => !element.contains(hit))
                .map(hit => hit && (hit.className || hit.tagName));
            element.style.pointerEvents = saved;
            return covering;
        }"""
    )
    assert not covering, f"{what} is painted over by {covering}"


def smoothing_hint(page: Page, trigger: Locator, name: str) -> None:
    panel = open_editor(page, trigger, name)
    selector = panel.get_by_role("combobox", name="Smoothing", exact=True)
    selector.select_option("savgol")
    header = panel.locator(".options-panel-header")
    hint = panel.locator(".smoothing-hover-hint")
    try:
        # Under 54rem (864px at the default font) the help spans the window's foot.
        for width in (VIEWPORT["width"], 800):
            page.set_viewport_size({"width": width, "height": VIEWPORT["height"]})
            selector.scroll_into_view_if_needed()
            before = header.bounding_box()
            selector.hover()
            expect(hint).to_be_visible()
            # Configure on a grid chart maximizes it, and the hint must stay above it.
            assert_on_top(hint, f"{width}px smoothing hint")
            bounds = hint.bounding_box()
            select = selector.bounding_box()
            assert bounds is not None and select is not None
            assert 0 <= bounds["x"] < bounds["x"] + bounds["width"] <= width
            assert (
                0 <= bounds["y"] < bounds["y"] + bounds["height"] <= VIEWPORT["height"]
            )
            if width == VIEWPORT["width"]:
                covers = (
                    bounds["x"] < select["x"] + select["width"]
                    and select["x"] < bounds["x"] + bounds["width"]
                    and bounds["y"] < select["y"] + select["height"]
                    and select["y"] < bounds["y"] + bounds["height"]
                )
                assert not covers, "the smoothing hint covers the smoothing select"
                docked = panel.bounding_box()
                assert docked is not None
                assert bounds["x"] + bounds["width"] <= docked["x"], (
                    "the hint should sit beside the docked panel"
                )
            else:
                assert bounds["width"] > width / 2, bounds
                assert VIEWPORT["height"] - (bounds["y"] + bounds["height"]) < 40, (
                    bounds
                )
            assert header.bounding_box() == before, (
                "hover help moved the panel controls"
            )
    finally:
        page.set_viewport_size(VIEWPORT)
    dismiss(page, panel, trigger, "Escape")


# A chart-options level's own patch as stored: ([layout key, 'project' | 'section' | 'rect', section name or rect id]) => patch.
READ_OPTIONS = """([key, scope, identity]) => {
    const diff = JSON.parse(localStorage.getItem(key) || '{}');
    if (scope === 'project') return diff.project_chart_defaults || {};
    const patch = (diff[scope + '_overrides'] || [])
        .find(entry => entry.key === identity)?.patch || {};
    return patch[scope === 'section' ? 'chart_defaults' : 'options'] || {};
}"""


def chart_option_fields(
    page: Page, trigger: Locator, name: str, storage_key: str
) -> None:
    print(f"{name}: shared chart options", flush=True)
    panel = open_editor(page, trigger, name)
    algorithm = panel.get_by_role("combobox", name="Smoothing", exact=True)
    order = panel.get_by_role("combobox", name="Fit order", exact=True)
    window = panel.get_by_role("spinbutton", name="Window", exact=True)
    window_slider = panel.get_by_role("slider", name="Window", exact=True)
    tau = panel.get_by_role("spinbutton", name="Time constant (steps)", exact=True)
    tau_slider = panel.get_by_role("slider", name="Time constant (steps)", exact=True)
    log_x = panel.get_by_role("checkbox", name="Log X", exact=True)
    max_runs = panel.get_by_role("spinbutton", name="Max runs", exact=True)
    scope, identity = "project", None
    if name == "Configure section":
        scope = "section"
        identity = panel.get_by_role("textbox", name="Name", exact=True).get_attribute(
            "placeholder"
        )
    elif name == "Configure Metric":
        scope = "rect"
        identity = trigger.locator("xpath=ancestor::*[@data-slot-id][1]").get_attribute(
            "data-slot-id"
        )
    storage_args = [storage_key, scope, identity]

    def stored_options() -> dict:
        return page.evaluate(READ_OPTIONS, storage_args)

    def expect_option(field: str, value: float | bool | str) -> None:
        page.wait_for_function(
            f"""args => {{
                const options = ({READ_OPTIONS})(args.slice(0, 3));
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
    panel.locator("label").filter(has_text=re.compile(r"^Log X$")).click()
    expect(log_x).to_be_checked(checked=saved_log_x)
    expect_focused_by_label(page, log_x)
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
    panel.locator("label").filter(has_text=re.compile(r"^Smoothing$")).click()
    expect_focused_by_label(page, algorithm)
    saving(
        page,
        storage_key,
        lambda: algorithm.select_option(
            "none" if inherited_algorithm != "none" else "triangular"
        ),
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
        toggle = panel.get_by_role(
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

    dismiss(page, panel, trigger, "Close")
    saved_options = stored_options()
    panel = open_editor(page, trigger, name)
    expect(log_x).to_be_checked(checked=saved_log_x)
    expect(max_runs).to_have_value(saved_max_runs)
    expect(algorithm).to_have_value("savgol")
    expect(window).to_have_value(saved_window)
    expect(order).to_have_value(saved_order)
    algorithm.select_option("ema-polyfit")
    expect(tau).to_have_value(saved_tau)
    expect_option("smoothing", "EmaPolyfit")
    panel.get_by_role("button", name="Revert", exact=True).click()
    expect(algorithm).to_have_value("savgol")
    dismiss(page, panel, trigger, "Close")
    restored_options = stored_options()
    assert restored_options == saved_options, (
        f"Revert retained smoothing edits: saved={saved_options}, restored={restored_options}"
    )


def chart_option_override_chips(
    page: Page, metric: Locator, section: Locator, defaults: Locator, storage_key: str
) -> None:
    panel = open_editor(page, section, "Configure section")
    inherited_window = panel.get_by_role(
        "spinbutton", name="Window", exact=True
    ).input_value()
    inherited_max_runs = panel.get_by_role(
        "spinbutton", name="Max runs", exact=True
    ).input_value()
    dismiss(page, panel, section, "Close")
    for trigger, name, pinned, limit in (
        (section, "Configure section", "137", "19"),
        (defaults, "Project settings", "149", "29"),
    ):
        panel = open_editor(page, metric, "Configure Metric")
        panel.get_by_role("spinbutton", name="Window", exact=True).fill(pinned)
        panel.get_by_role("spinbutton", name="Max runs", exact=True).fill(limit)
        dismiss(page, panel, metric, "Close")
        panel = open_editor(page, trigger, name)
        revert = panel.get_by_role("button", name="Revert", exact=True)
        chips = [
            panel.get_by_role("button").filter(has_text=re.compile(rf" = {value}\s*$"))
            for value in (pinned, limit)
        ]
        # Revert puts back the pins the chips cleared, then the chips clear them for good.
        for undo in (True, False):
            expect(revert).to_be_disabled()
            for chip in chips:
                expect(chip).to_have_count(1)
                chip.click()
                expect(chip).to_have_count(0)
            if undo:
                saving(page, storage_key, revert.click)
        dismiss(page, panel, trigger, "Close")
        panel = open_editor(page, metric, "Configure Metric")
        expect(
            panel.get_by_role("spinbutton", name="Window", exact=True)
        ).to_have_value(inherited_window)
        expect(
            panel.get_by_role("spinbutton", name="Max runs", exact=True)
        ).to_have_value(inherited_max_runs)
        dismiss(page, panel, metric, "Close")


def metric_picker(page: Page, trigger: Locator, storage_key: str) -> None:
    """Sorted typed groups, substring filter, type gating, and a multi-run specific source, all rolled back by Revert."""
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
    panel = open_editor(page, trigger, "Configure Metric")
    metric = panel.get_by_role("combobox", name="Source 1 metric", exact=True)
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
        box = panel.get_by_role("textbox", name="Source 1 metric filter", exact=True)
        box.fill(needle)
        matches = [name for name in catalog if needle.lower() in name.lower()]
        expect(panel.locator(".binding-filter-count")).to_have_text(
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
    panel.get_by_role("button", name="+ Add Source", exact=True).click()
    second = panel.get_by_role("combobox", name="Source 2 metric", exact=True)
    expect(second.locator("optgroup")).to_have_count(len(labels))
    expect(second.locator("optgroup:not([disabled])")).to_have_attribute("label", kind)
    for group in second.locator("optgroup[disabled]").all():
        expect(group).to_have_attribute(
            "label", re.compile(rf" — other sources are {kind.lower()}$")
        )
    panel.get_by_role("button", name="Remove source 2", exact=True).click()
    expect(second).to_have_count(0)

    # Specific Runs: several checked at once, live-applied in pick order.
    panel.get_by_role("combobox", name="Source 1 runs", exact=True).select_option(
        label="Specific Runs"
    )
    boxes = panel.get_by_role(
        "group", name="Source 1 specific runs", exact=True
    ).get_by_role("checkbox")
    expect(boxes.first).to_be_attached()
    assert boxes.count() >= 2, "multi-run picker needs two active runs"
    rows = panel.locator(".binding-run-list label")
    checked = panel.locator(".binding-run-list input:checked")
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
    panel.get_by_role("button", name="+ Add Source", exact=True).click()
    first, second = (
        panel.get_by_role("textbox", name=f"Source {n} metric filter", exact=True)
        for n in (1, 2)
    )
    first.fill("first source")
    second.fill("second source")
    panel.get_by_role("button", name="Remove source 1", exact=True).click()
    expect(second).to_have_count(0)
    expect(first).to_have_value("second source")
    expect(
        panel.get_by_role("combobox", name="Source 1 metric", exact=True)
    ).to_have_value("")
    panel.get_by_role("button", name="Revert", exact=True).click()
    dismiss(page, panel, trigger, "Close")
    page.wait_for_function(
        f"""args => JSON.stringify(({read_bindings})(args.slice(0, 2)))
            === JSON.stringify(args[2])""",
        arg=[storage_key, identity, saved_bindings],
    )
    panel = open_editor(page, trigger, "Configure Metric")
    expect(
        panel.get_by_role("group", name="Source 1 specific runs", exact=True)
    ).to_have_count(0)
    expect(
        panel.get_by_role("combobox", name="Source 1 metric", exact=True)
    ).to_have_value(current)
    dismiss(page, panel, trigger, "Escape")


def section_fields(page: Page, section: Locator, storage_key: str) -> None:
    panel = open_editor(page, section, "Configure section")
    fields = (
        panel.get_by_role("textbox", name="Name", exact=True),
        panel.get_by_role("spinbutton", name="Columns", exact=True),
        panel.get_by_role("spinbutton", name="Rows / page", exact=True),
    )
    original = [field.input_value() for field in fields]
    edited = [
        f"Edited {original[0] or 'section'}",
        "1" if original[1] != "1" else "2",
        "0" if original[2] != "0" else "1",
    ]
    for field, value in zip(fields, edited):
        saving(page, storage_key, lambda: field.fill(value))
    # Revert restores every field and keeps the panel open.
    panel.get_by_role("button", name="Revert", exact=True).click()
    for field, value in zip(fields, original):
        expect(field).to_have_value(value)
    expect(panel).to_be_visible()
    for field, value in zip(fields, edited):
        saving(page, storage_key, lambda: field.fill(value))
    dismiss(page, panel, section, "Escape")
    panel = open_editor(page, section, "Configure section")
    for field, value in zip(fields, edited):
        expect(field).to_have_value(value)
    for field, value in zip(fields, original):
        field.fill(value)
    dismiss(page, panel, section, "Close")


def close_keeps_scroll(page: Page, section: Locator) -> None:
    """Closing a panel returns focus to its opener without scrolling the grid back to it: scrolling beside the docked panel is the point."""
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 500})
    content = page.locator("main.main-content")
    try:
        content.evaluate("e => e.scrollTo(0, 0)")
        panel = open_editor(page, section, "Configure section")
        content.evaluate("e => e.scrollTo(0, e.scrollHeight)")
        scrolled = content.evaluate("e => e.scrollTop")
        assert scrolled > 0
        dismiss(page, panel, section, "Escape")
        assert content.evaluate("e => e.scrollTop") == scrolled
    finally:
        content.evaluate("e => e.scrollTo(0, 0)")
        page.set_viewport_size(VIEWPORT)


def settings_beside_the_panel(page: Page, section: Locator) -> None:
    """The panel stays open beside the grid, so a collapse made there must survive the panel's edits and its Revert."""
    container = section.locator(SECTION_OF)
    panel = open_editor(page, section, "Configure section")
    expect(container).to_have_attribute("data-options-target", "true")
    name = container.locator(".section-name")
    grid = container.locator(".section-grid")
    # Clicked in its middle, Playwright's default: its name may be empty, and its buttons don't toggle.
    header = container.locator(".section-header")

    # The first chart's section is the catch-all of unprefixed metrics: it has no name, and its panel no empty target line.
    assert not name.inner_text(), name.inner_text()
    expect(page.locator("#kymo-panel-target")).to_have_count(0)
    expect(grid).to_have_count(1)
    header.click()
    expect(grid).to_have_count(0)
    box = panel.get_by_role("textbox", name="Name", exact=True)
    box.fill("Renamed beside a collapse")
    expect(name).to_have_text("Renamed beside a collapse")
    expect(grid).to_have_count(0)
    panel.get_by_role("button", name="Revert", exact=True).click()
    expect(name).to_have_text("")
    expect(grid).to_have_count(0)
    header.click()
    expect(grid).to_have_count(1)

    # So must a chart-height drag.
    rect = container.locator(".metric-rect").first
    dragged = drag_chart_height(page, rect, 60)
    box.fill("Renamed beside a drag")
    expect(name).to_have_text("Renamed beside a drag")
    panel.get_by_role("button", name="Revert", exact=True).click()
    expect(name).to_have_text("")
    assert rect.evaluate(CHART_HEIGHT) == dragged, (
        "a panel edit undid the chart-height drag"
    )
    dismiss(page, panel, section, "Close")
    expect(container).to_have_attribute("data-options-target", "false")


CHART_HEIGHT = "e => getComputedStyle(e).getPropertyValue('--kymo-chart-height')"


def drag_chart_height(page: Page, rect: Locator, dy: float) -> str:
    """Drag `rect`'s resize grip `dy` pixels vertically, wait for its section's chart height to change, and return the new height."""
    before = rect.evaluate(CHART_HEIGHT)
    rect.hover()
    grip = rect.locator(".rect-resize-handle").bounding_box()
    assert grip is not None
    x, y = grip["x"] + grip["width"] / 2, grip["y"] + grip["height"] / 2
    page.mouse.move(x, y)
    page.mouse.down()
    page.mouse.move(x, y + dy, steps=6)
    page.mouse.up()
    page.wait_for_function(
        f"([e, before]) => ({CHART_HEIGHT})(e) !== before",
        arg=[rect.element_handle(), before],
    )
    return rect.evaluate(CHART_HEIGHT)


def double_presses_act_once(
    page: Page, defaults: Locator, metric: Locator, storage_key: str
) -> None:
    """A double-click on a control that opens or closes the panel acts once: its second press doesn't land on what the panel moved under the pointer (the section header beside the panel's Close collapses on a press)."""
    stored = "key => localStorage.getItem(key)"
    before = page.evaluate(stored, storage_key)
    panel = open_editor(page, defaults, "Project settings")
    double_click_acts_once(page, panel.get_by_role("button", name="Close", exact=True))
    expect(panel).to_have_count(0)
    double_click_acts_once(page, defaults)
    panel = options_panel(page, "Project settings")
    expect(panel).to_be_visible()
    dismiss(page, panel, defaults, "Escape")

    # Un-maximizing the chart closes its panel too, by the chart's Close or a press on the overlay's backdrop (the section header comes back under it); presses paced as a person's land after the grid is back.
    overlay = page.locator(".maximize-overlay")
    for backdrop in (False, True):
        open_editor(page, metric, "Configure Metric")
        box = (
            overlay if backdrop else overlay.locator('button[title="Close"]')
        ).bounding_box()
        assert box is not None
        count_presses(page)
        page.mouse.move(
            box["x"] + box["width"] / 2,
            box["y"] + (6 if backdrop else box["height"] / 2),
        )
        for count in (1, 2):
            page.mouse.down(click_count=count)
            page.mouse.up(click_count=count)
            page.wait_for_timeout(150)
        expect(overlay).to_have_count(0)
        assert page.evaluate("window.__kymo_presses") == 1, (
            f"both presses on the {'backdrop' if backdrop else 'Close'} reached the page"
        )
    assert page.evaluate(stored, storage_key) == before, (
        "a double-click that un-maximized the chart also pressed what moved under it"
    )


def docking(page: Page, defaults: Locator, section: Locator) -> None:
    """The panel docks: the main column narrows by its width. Another target replaces it."""
    main = page.locator(".main-wrap")
    before = main.bounding_box()
    panel = open_editor(page, defaults, "Project settings")
    docked = panel.bounding_box()
    after = main.bounding_box()
    assert before is not None and docked is not None and after is not None
    assert abs(docked["x"] + docked["width"] - VIEWPORT["width"]) < 0.5, docked
    assert abs(before["width"] - after["width"] - docked["width"]) < 1, (
        before,
        after,
        docked,
    )
    replacement = open_editor(page, section, "Configure section")
    expect(panel).to_have_count(0)
    dismiss(page, replacement, section, "Escape")
    restored = main.bounding_box()
    assert restored is not None and abs(restored["width"] - before["width"]) < 0.5

    # Fixed-position content in the chart area still positions against the viewport, and the docked panel paints over it (hover readouts, under <body>, paint over the panel instead).
    panel = open_editor(page, defaults, "Project settings")
    probe = page.evaluate(
        """() => {
            const d = document.createElement('div');
            d.style.cssText = 'position:fixed;top:0;left:0;width:100vw;height:100vh';
            document.querySelector('.main-content').appendChild(d);
            const box = d.getBoundingClientRect();
            const p = document.getElementById('kymo-panel').getBoundingClientRect();
            const hit = document.elementFromPoint(p.left + p.width / 2, p.top + p.height / 2);
            d.remove();
            return {top: box.top, left: box.left, panel_on_top: !!hit.closest('#kymo-panel')};
        }"""
    )
    assert probe == {"top": 0, "left": 0, "panel_on_top": True}, probe
    dismiss(page, panel, defaults, "Escape")

    # Beside a run list dragged wide, the panel gives way first, down to 14rem; then the run list and the charts shrink together, each losing the same share of its width (the charts' counted as 20rem).
    page.evaluate(
        "document.documentElement.style.setProperty('--kymo-sidebar-w', '75vw')"
    )
    try:
        panel = open_editor(page, defaults, "Project settings")
        docked = panel.bounding_box()
        after = main.bounding_box()
        run_list = page.locator(".sidebar").bounding_box()
        rem = page.evaluate(
            "parseFloat(getComputedStyle(document.documentElement).fontSize)"
        )
        assert docked is not None and after is not None and run_list is not None
        assert abs(docked["x"] + docked["width"] - VIEWPORT["width"]) < 0.5, docked
        assert abs(docked["width"] - 14 * rem) < 0.5, docked
        run_list_kept = run_list["width"] / (0.75 * VIEWPORT["width"])
        charts_kept = after["width"] / (20 * rem)
        assert charts_kept < 1 and abs(run_list_kept - charts_kept) < 0.01, (
            run_list,
            after,
        )
        dismiss(page, panel, defaults, "Escape")
    finally:
        page.evaluate(
            "document.documentElement.style.removeProperty('--kymo-sidebar-w')"
        )

    # Dragging the run list wide beside the panel saves the width dragged to (at most 75% of the window): the panel squeezes it only while open.
    panel = open_editor(page, defaults, "Project settings")
    try:
        drag_handle(page, page.locator(".sidebar-resize"), VIEWPORT["width"] - 40)
        squeezed = page.locator(".sidebar").bounding_box()
        stored = float(page.evaluate("localStorage.getItem('kymo_sidebar_w')"))
        assert squeezed is not None and stored == 75, (stored, squeezed)
        assert squeezed["width"] < 0.75 * VIEWPORT["width"], squeezed
        dismiss(page, panel, defaults, "Escape")
        after = page.locator(".sidebar").bounding_box()
        assert (
            after is not None and abs(after["width"] - 0.75 * VIEWPORT["width"]) < 1
        ), after
    finally:
        page.evaluate(
            """() => {
                localStorage.removeItem('kymo_sidebar_w');
                document.documentElement.style.removeProperty('--kymo-sidebar-w');
            }"""
        )


def panel_resize(page: Page, defaults: Locator) -> None:
    """The panel's left edge drags its width like the run list's: a share of the window, saved as dragged and restored before the next load lays anything out."""
    handle = page.locator(".options-panel-resize")
    rem = page.evaluate(
        "parseFloat(getComputedStyle(document.documentElement).fontSize)"
    )

    def stored() -> float:
        """The saved share of the window, in pixels at the current width."""
        return float(
            page.evaluate("localStorage.getItem('kymo_options_w') * innerWidth / 100")
        )

    try:
        panel = open_editor(page, defaults, "Project settings")
        start = panel.bounding_box()
        assert start is not None
        drag_handle(page, handle, start["x"] - 120)
        widened = panel.bounding_box()
        assert (
            widened is not None and abs(widened["width"] - start["width"] - 120) < 2
        ), (start, widened)
        assert abs(stored() - widened["width"]) < 1, (stored(), widened)
        # A press without a drag saves nothing.
        before = stored()
        box = handle.bounding_box()
        assert box is not None
        page.mouse.move(box["x"] + 2, box["y"] + 100)
        page.mouse.down()
        page.mouse.up()
        assert stored() == before
        # A wider window widens the panel by the same share.
        page.set_viewport_size({"width": 1600, "height": VIEWPORT["height"]})
        followed = panel.bounding_box()
        assert followed is not None and abs(followed["width"] - stored()) < 1, (
            followed,
            stored(),
        )
        page.set_viewport_size(VIEWPORT)

        # Dragged narrow, the panel stops at 14rem so its forms still fit.
        drag_handle(page, handle, VIEWPORT["width"] - 40)
        narrowest = panel.bounding_box()
        assert narrowest is not None and abs(narrowest["width"] - 14 * rem) < 1, (
            narrowest
        )

        # Dragged as far as it goes (75% of the window), the panel itself gives way: the charts keep 20rem and the run list its width.
        drag_handle(page, handle, 10)
        main = page.locator(".main-wrap").bounding_box()
        sidebar = page.locator(".sidebar").bounding_box()
        widest = panel.bounding_box()
        assert main is not None and sidebar is not None and widest is not None
        assert abs(main["width"] - 20 * rem) < 0.5, main
        assert abs(sidebar["width"] - VIEWPORT["width"] / 5) < 1, sidebar
        assert abs(stored() - 0.75 * VIEWPORT["width"]) < 1, stored()
        dismiss(page, panel, defaults, "Close")

        # The next load restores the width before the panel opens, so opening resizes each chart once.
        page.reload(wait_until="domcontentloaded")
        page.locator(".chart-container").first.wait_for(timeout=20_000)
        page.wait_for_function("Object.keys(window.__kymo_charts || {}).length >= 1")
        page.wait_for_timeout(1000)
        page.evaluate(
            """() => {
                for (const chart of Object.values(window.__kymo_charts)) {
                    const setSize = chart.setSize.bind(chart);
                    chart.__resizes = 0;
                    chart.setSize = size => (chart.__resizes++, setSize(size));
                }
            }"""
        )
        panel = open_editor(page, defaults, "Project settings")
        reopened = panel.bounding_box()
        assert reopened is not None and abs(reopened["width"] - widest["width"]) < 1, (
            reopened,
            widest,
        )
        page.wait_for_timeout(1000)
        resizes = page.evaluate(
            "Object.values(window.__kymo_charts).map(chart => chart.__resizes)"
        )
        assert all(count <= 1 for count in resizes), resizes
        dismiss(page, panel, defaults, "Close")

        # A drag the panel closes under (Esc mid-drag) saves nothing, and leaves the width it had.
        width = "[localStorage.getItem('kymo_options_w'), document.documentElement.style.getPropertyValue('--kymo-options-w')]"
        before = page.evaluate(width)
        panel = open_editor(page, defaults, "Project settings")
        grip = handle.bounding_box()
        assert grip is not None
        y = grip["y"] + grip["height"] / 2
        page.mouse.move(grip["x"] + grip["width"] / 2, y)
        page.mouse.down()
        page.mouse.move(grip["x"] - 120, y, steps=3)
        page.keyboard.press("Escape")
        expect(panel).to_have_count(0)
        page.mouse.move(grip["x"] - 160, y)
        page.mouse.up()
        assert page.evaluate(width) == before, (page.evaluate(width), before)
    finally:
        page.set_viewport_size(VIEWPORT)
        page.evaluate(
            """() => {
                localStorage.removeItem('kymo_options_w');
                document.documentElement.style.removeProperty('--kymo-options-w');
            }"""
        )


def panels_beside_maximize(
    page: Page, chart: Locator, metric: Locator, section: Locator, defaults: Locator
) -> None:
    overlay = page.locator(".maximize-overlay")
    panel = open_editor(page, section, "Configure section")
    # The open target's own trigger shows pressed, and closes it.
    expect(section).to_have_attribute("aria-expanded", "true")
    expect(defaults).to_have_attribute("aria-expanded", "false")
    section.click()
    expect(panel).to_have_count(0)
    expect(section).to_be_focused()
    expect(section).to_have_attribute("aria-expanded", "false")
    panel = open_editor(page, section, "Configure section")
    # Maximizing beside a non-chart panel focuses the maximized chart, not the covered grid.
    chart.locator('button[title="Maximize"]').press("Enter")
    expect(overlay).to_be_focused()
    expect(panel).to_be_visible()
    # The grid beneath is out of reach: Tab and clicks can't land on its hidden controls.
    main = page.locator("main.main-content")
    expect(main).to_have_attribute("inert", "true")
    # Closing the panel now can't return focus to its opener in the inert grid, so the maximized chart takes it.
    panel.get_by_role("button", name="Close", exact=True).click()
    expect(panel).to_have_count(0)
    expect(overlay).to_be_focused()
    page.keyboard.press("Escape")
    expect(overlay).to_have_count(0)
    expect(main).not_to_have_attribute("inert", re.compile(".*"))

    # Configure maximized the chart, so closing the panel returns to the grid even after it switched targets.
    chart_panel = open_editor(page, metric, "Configure Metric")
    replacement = open_editor(page, defaults, "Project settings")
    expect(chart_panel).to_have_count(0)
    expect(overlay).to_be_visible()
    close_panel(page, replacement, defaults, "Close")
    expect(overlay).to_have_count(0)
    expect(page).not_to_have_url(re.compile(r"[?&]chart="))

    # A close the panel didn't ask for (the chart's own Close) still returns the focus it took along.
    chart_panel = open_editor(page, metric, "Configure Metric")
    chart_panel.get_by_role("spinbutton", name="Max runs", exact=True).focus()
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)
    expect(chart_panel).to_have_count(0)
    expect(metric).to_be_focused()

    # However narrow the window, the panel docks beside the maximized chart, never over it: the chart's own Close stays in reach.
    try:
        for width in (900, 640):
            page.set_viewport_size({"width": width, "height": VIEWPORT["height"]})
            chart_panel = open_editor(page, metric, "Configure Metric")
            docked = chart_panel.bounding_box()
            maximized = overlay.bounding_box()
            assert docked is not None and maximized is not None
            assert maximized["x"] + maximized["width"] <= docked["x"] + 0.5, (
                maximized,
                docked,
            )
            assert_on_top(
                overlay.locator('button[title="Close"]'), f"{width}px maximized Close"
            )
            dismiss(page, chart_panel, metric, "Escape")
    finally:
        page.set_viewport_size(VIEWPORT)


def arrows_move_the_chart_panel(page: Page, metric: Locator) -> None:
    """←/→ move the maximize to a neighbouring chart and the chart panel follows: pressed in the panel, focus follows into it; pressed on the maximized chart, focus stays with the chart for its own keys. A text field keeps its own arrows, and closing still returns to where Configure started."""
    chart_param = "new URL(location.href).searchParams.get('chart')"
    panel = open_editor(page, metric, "Configure Metric")
    target = page.locator("#kymo-panel-target")
    opened, title = page.evaluate(chart_param), target.inner_text()
    forward, back = "ArrowRight", "ArrowLeft"
    # Pressed on a control the switch replaces, in the panel.
    log_y_control(panel).focus()
    page.keyboard.press(forward)
    moved = page.evaluate(chart_param)
    assert moved != opened, "→ didn't move the maximized chart"
    expect(target).not_to_have_text(title)
    expect(panel_body(panel)).to_be_focused()
    panel.get_by_role("spinbutton", name="Width", exact=True).focus()
    page.keyboard.press("ArrowRight")
    assert page.evaluate(chart_param) == moved, "a text field's arrows moved the chart"
    # Pressed on the maximized chart, on a control the switch replaces and on the overlay itself.
    overlay = page.locator(".maximize-overlay")
    overlay.locator('button[title="Close"]').focus()
    page.keyboard.press(back)
    page.wait_for_function(f"{chart_param} === {json.dumps(opened)}")
    expect(target).to_have_text(title)
    expect(overlay).to_be_focused()
    page.keyboard.press(forward)
    page.wait_for_function(f"{chart_param} === {json.dumps(moved)}")
    expect(overlay).to_be_focused()
    panel_body(panel).focus()
    dismiss(page, panel, metric, "Escape")


def follow(page: Page, panel: Locator, key: str) -> None:
    """Press ←/→ in the panel's focus, moving the maximized chart the panel follows."""
    panel_body(panel).focus()
    shown = page.url
    page.keyboard.press(key)
    expect(page).not_to_have_url(shown)


def follow_then_close_focus(page: Page, chart: Locator) -> None:
    """Opened from the maximized chart's own Configure: a ←/→ follow commits the chart's edits, so back on it Revert has nothing to undo; and it takes that button away with the chart, so closing the panel focuses the Configure of the chart now maximized."""
    chart.locator('button[title="Maximize"]').click()
    overlay = page.locator(".maximize-overlay")
    configure = overlay.locator('button[title="Configure"]')
    panel = open_editor(page, configure, "Configure Metric")
    revert = panel.get_by_role("button", name="Revert", exact=True)
    checked = log_y_control(panel).is_checked()
    log_y_control(panel).set_checked(not checked)
    expect(revert).to_be_enabled()
    shown = page.url
    follow(page, panel, "ArrowRight")
    panel_body(panel).focus()
    page.keyboard.press("ArrowLeft")
    expect(page).to_have_url(shown)
    expect(log_y_control(panel)).to_be_checked(checked=not checked)
    expect(revert).to_be_disabled()
    log_y_control(panel).set_checked(checked)
    follow(page, panel, "ArrowRight")
    panel.get_by_role("button", name="Close", exact=True).click()
    expect(panel).to_have_count(0)
    expect(configure).to_be_focused()
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)


def panel_keys_and_tips(page: Page, metric: Locator, section: Locator) -> None:
    """The keyboard scrolls the panel it opens focused on, and a chart's hover readout ignores the panel beside it, painting over it."""
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 360})
    try:
        panel = open_editor(page, metric, "Configure Metric")
        body = panel_body(panel)
        assert body.evaluate("e => e.scrollHeight > e.clientHeight")
        page.keyboard.press("PageDown")
        expect(body).not_to_have_js_property("scrollTop", 0)
        dismiss(page, panel, metric, "Escape")
    finally:
        page.set_viewport_size(VIEWPORT)

    panel = open_editor(page, metric, "Configure Metric")
    body = panel_body(panel)
    plot = page.locator(".maximize-overlay .u-over").first
    expect(plot).to_be_visible()
    tip = page.locator(".kymo-tip-src").filter(visible=True)
    # Beside the panel, a readout keeps to the crosshair's right as it does without one (it flips only at the window's edge) and paints over the panel without taking its focus.
    box = plot.bounding_box()
    assert box is not None
    cx = box["x"] + box["width"] * 0.95
    page.mouse.move(cx, box["y"] + box["height"] / 2)
    expect(tip).to_be_visible()
    shown, docked = tip.bounding_box(), panel.bounding_box()
    assert shown is not None and docked is not None
    assert cx < shown["x"] and docked["x"] < shown["x"] + shown["width"], (
        cx,
        shown,
        docked,
    )
    assert_on_top(tip, "a readout beside the panel")
    expect(body).to_be_focused()
    # Its width cap is the window's third, as without the panel.
    assert tip.evaluate(
        "t => Math.abs(parseFloat(getComputedStyle(t).maxWidth) - 0.33 * innerWidth) < 0.5"
    ), tip.evaluate("t => getComputedStyle(t).maxWidth")
    # A chart column narrower than the readout leaves it whole, and Esc still closes the panel while it shows.
    page.evaluate(
        "document.documentElement.style.setProperty('--kymo-options-w', '60vw')"
    )
    try:
        column = page.locator(".main-wrap").bounding_box()
        box = plot.bounding_box()
        assert column is not None and box is not None
        assert column["width"] < shown["width"], (column, shown)
        page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        expect(tip).to_be_visible()
        narrow = tip.bounding_box()
        assert narrow is not None and abs(narrow["width"] - shown["width"]) < 0.5, (
            narrow,
            shown,
        )
        dismiss(page, panel, metric, "Escape")

        # Readouts don't live in their charts, so moving a section's node, as a reorder does, leaves them where they are: a moved chart's readout still paints over the panel. Moved out and straight back, the page stays as it was.
        rect = metric.locator(
            "xpath=ancestor::div[contains(concat(' ', @class, ' '), ' metric-rect ')][1]"
        )
        rect.locator(SECTION_OF).evaluate(
            "s => { const next = s.nextSibling; s.parentNode.appendChild(s); s.parentNode.insertBefore(s, next); }"
        )
        panel = open_editor(page, section, "Configure section")
        rect.scroll_into_view_if_needed()
        box = rect.locator(".u-over").first.bounding_box()
        assert box is not None
        page.mouse.move(box["x"] + box["width"] * 0.95, box["y"] + box["height"] / 2)
        expect(tip).to_be_visible()
        shown, docked = tip.bounding_box(), panel.bounding_box()
        assert shown is not None and docked is not None
        assert docked["x"] < shown["x"] + shown["width"], (shown, docked)
        assert_on_top(tip, "a moved chart's readout")
        dismiss(page, panel, section, "Escape")
    finally:
        page.evaluate(
            "document.documentElement.style.removeProperty('--kymo-options-w')"
        )


def source_edit_keeps_sections(page: Page) -> None:
    """Adding a source to a metadata chart leaves its editor's Metadata section mounted: the maximized chart's report holds until it reports again."""
    rect = page.locator('.metric-rect[data-slot-id="info/run_info"]')
    rect.scroll_into_view_if_needed()
    rect.hover()
    trigger = pin_trigger(page, rect.locator('button[title="Configure"]'))
    panel = open_editor(page, trigger, "Configure Metric")
    section = panel.get_by_role("button", name="Metadata", exact=True)
    expect(section).to_be_visible()
    # A remount, even for one render, leaves this node disconnected.
    shown = section.element_handle()
    panel.get_by_role("button", name="+ Add Source", exact=True).click()
    expect(
        panel.get_by_role("textbox", name="Source 2 metric filter", exact=True)
    ).to_be_visible()
    page.wait_for_timeout(500)
    assert shown.evaluate("e => e.isConnected"), "the Metadata section remounted"
    panel.get_by_role("button", name="Revert", exact=True).click()
    dismiss(page, panel, trigger, "Close")


class RpcTap:
    """Proxies a tab's RPC socket, recording each request's method in `sent` and holding back requests for the method `hold` until `release()`."""

    def __init__(self, tab: Page, hold: str | None) -> None:
        self.sent: list[str] = []
        self.hold = hold
        self.held: list = []
        tab.route_web_socket(
            re.compile(r"/(?:grpc-ws|trash/_kymo-grpc-ws-local-v1)"), self.connect
        )

    def connect(self, ws) -> None:
        server = ws.connect_to_server()

        def from_page(message) -> None:
            if isinstance(message, bytes):
                method = request_frame(message)[1].rsplit("/", 1)[-1]
                self.sent.append(method)
                if method == self.hold:
                    self.held.append((server, message))
                    return
            server.send(message)

        ws.on_message(from_page)
        server.on_message(ws.send)

    def release(self) -> int:
        """Send the held requests on, and hold none from now on; returns how many were held."""
        queued, self.held, self.hold = self.held, [], None
        for server, message in queued:
            server.send(message)
        return len(queued)


@contextmanager
def chart_tab(page: Page, chart: Locator, hold: str | None = None):
    """A second tab opened on `chart`'s `?chart=` link with its RPC socket tapped (`RpcTap`), shown maximized; closed on exit."""
    slot = chart.get_attribute("data-slot-id")
    assert slot
    link = urlsplit(page.url)._replace(query=urlencode({"chart": slot})).geturl()
    tab = page.context.new_page()
    tap = RpcTap(tab, hold)
    try:
        tab.goto(link, wait_until="domcontentloaded")
        expect(tab.locator(".maximize-overlay")).to_be_visible(timeout=20_000)
        yield tab, tap
    finally:
        tab.close()


def follow_reuses_discovery(page: Page, chart: Locator) -> None:
    """A chart panel following ←/→ reuses its sources' discovery: the current project's run list is the dashboard's own, and a run set's metric catalog is fetched once."""
    with chart_tab(page, chart) as (other, tap):
        overlay = other.locator(".maximize-overlay")
        other.locator("main .metric-rect").first.wait_for(timeout=20_000)
        swept = tap.sent.count("ListRunSetMetrics")
        open_editor(
            other, overlay.locator('button[title="Configure"]'), "Configure Metric"
        )
        other.wait_for_timeout(1500)
        assert tap.sent.count("ListRunSetMetrics") > swept, (
            "the editor fetched no catalog to reuse"
        )
        opened = len(tap.sent)
        for key in ("ArrowRight", "ArrowLeft"):
            follow(other, options_panel(other, "Configure Metric"), key)
            other.wait_for_timeout(1500)
        follows = tap.sent[opened:]
        assert "ListRuns" not in follows and "ListRunSetMetrics" not in follows, follows


def editor_waits_for_the_run_list(page: Page, chart: Locator) -> None:
    """A chart panel opened before the dashboard's run list lands sends no run list of its own: its sources in the dashboard's project wait for that one, so the list goes out once."""
    with chart_tab(page, chart, hold="ListRuns") as (other, tap):
        open_editor(
            other,
            other.locator(".maximize-overlay").locator('button[title="Configure"]'),
            "Configure Metric",
        )
        other.wait_for_timeout(1500)
        tap.release()
        expect(other.locator(".sidebar-run").first).to_be_visible(timeout=20_000)
        other.wait_for_timeout(1000)
        assert tap.sent.count("ListRuns") == 1, tap.sent


def chart_panel_ahead_of_the_sweep(
    page: Page, chart: Locator, defaults: Locator, storage_key: str
) -> None:
    """A chart link's panel opened before the metrics sweep lands shows the options the chart inherits, and so does the panel reopened once it lands."""
    panel = open_editor(page, defaults, "Project settings")
    expect(log_y_control(panel)).not_to_be_checked()
    toggle_live(page, log_y_control(panel), storage_key)
    dismiss(page, panel, defaults, "Close")

    with chart_tab(page, chart, hold="ListRunSetMetrics") as (other, tap):
        configure = other.locator('.maximize-overlay button[title="Configure"]')
        expect(other.locator("main .metric-rect")).to_have_count(0)
        panel = open_editor(other, configure, "Configure Metric")
        expect(log_y_control(panel)).to_be_checked()
        panel.get_by_role("button", name="Close", exact=True).click()
        # A project-settings edit made meanwhile is saved, and shows once the sweep lands.
        gear = other.get_by_title("Project settings")
        defaults_panel = open_editor(other, gear, "Project settings")
        log_x = defaults_panel.get_by_role("checkbox", name="Log X", exact=True)
        logged = not toggle_live(other, log_x, storage_key)
        dismiss(other, defaults_panel, gear, "Close")
        panel = open_editor(other, configure, "Configure Metric")
        held = tap.release()
        assert held, "the metrics sweep landed before it was held"
        other.locator("main .metric-rect").first.wait_for(timeout=20_000)
        # An open panel keeps the values it opened with; reopened, it reads the swept layout.
        dismiss(other, panel, configure, "Close")
        panel = open_editor(other, configure, "Configure Metric")
        expect(log_y_control(panel)).to_be_checked()
        expect(panel.get_by_role("checkbox", name="Log X", exact=True)).to_be_checked(
            checked=logged
        )


def panel_leaves_with_its_chart(page: Page, metric: Locator) -> None:
    """Hiding every run takes the chart out of the layout: its panel closes, and the chart stays maximized on its snapshot, as it does with no panel open, showing the edits made before. Its Configure is gone until the layout holds it again."""
    overlay = page.locator(".maximize-overlay")
    panel = open_editor(page, metric, "Configure Metric")
    title = overlay.locator(".rect-title")
    original = title.inner_text()
    title.dblclick()
    overlay.locator(".rect-title-input").fill("renamed before hiding")
    overlay.locator(".rect-title-input").press("Enter")
    expect(title).to_have_text("renamed before hiding")
    page.get_by_role("button", name="Hide all", exact=True).click()
    try:
        expect(panel).to_have_count(0)
        expect(overlay).to_be_visible()
        expect(title).to_have_text("renamed before hiding")
        expect(overlay.locator('button[title="Configure"]')).to_have_count(0)
        # Nor does it offer a rename, which could take no edit either.
        title.dblclick()
        expect(overlay.locator(".rect-title-input")).to_have_count(0)
    finally:
        page.get_by_role("button", name="Show all", exact=True).click()
    expect(overlay.locator('button[title="Configure"]')).to_have_count(1)
    title.dblclick()
    overlay.locator(".rect-title-input").fill(original)
    overlay.locator(".rect-title-input").press("Enter")
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)


def chart_panel_details(
    page: Page, chart: Locator, metric: Locator, section: Locator
) -> None:
    overlay = page.locator(".maximize-overlay")
    # Esc in a source's metric filter clears it first, like the app's other filters; the next Esc closes the panel.
    panel = open_editor(page, metric, "Configure Metric")
    box = panel.get_by_role("textbox", name="Source 1 metric filter", exact=True)
    box.fill("zzz")
    box.press("Escape")
    expect(box).to_have_value("")
    expect(panel).to_be_visible()
    dismiss(page, panel, metric, "Escape")

    # Configure on a maximized chart, then its own Close: focus goes to the grid copy's Configure.
    chart.locator('button[title="Maximize"]').click()
    maximized = pin_trigger(page, overlay.locator('button[title="Configure"]'))
    panel = open_editor(page, maximized, "Configure Metric")
    panel.get_by_role("spinbutton", name="Max runs", exact=True).focus()
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)
    expect(panel).to_have_count(0)
    expect(metric).to_be_focused()

    # A section showing fewer columns than a chart's saved width keeps that width through the chart panel's edits and Revert.
    panel = open_editor(page, metric, "Configure Metric")
    panel.get_by_role("spinbutton", name="Width", exact=True).fill("2")
    dismiss(page, panel, metric, "Close")
    expect(chart.locator("xpath=..")).to_have_attribute("style", re.compile(r"span 2"))
    panel = open_editor(page, section, "Configure section")
    columns = panel.get_by_role("spinbutton", name="Columns", exact=True)
    shown_columns = columns.input_value()
    columns.fill("1")
    dismiss(page, panel, section, "Close")
    # So do grid drags that only change the chart's height (down, then back up).
    for dy in (40, -40):
        drag_chart_height(page, chart, dy)
    panel = open_editor(page, metric, "Configure Metric")
    expect(panel.get_by_role("spinbutton", name="Width", exact=True)).to_have_value("1")
    log_y = log_y_control(panel)
    log_y.set_checked(not log_y.is_checked())
    panel.get_by_role("button", name="Revert", exact=True).click()
    dismiss(page, panel, metric, "Close")
    panel = open_editor(page, section, "Configure section")
    columns.fill(shown_columns if shown_columns != "1" else "4")
    dismiss(page, panel, section, "Close")
    expect(chart.locator("xpath=..")).to_have_attribute("style", re.compile(r"span 2"))
    panel = open_editor(page, metric, "Configure Metric")
    panel.get_by_role("spinbutton", name="Width", exact=True).fill("1")
    dismiss(page, panel, metric, "Close")


def lightbox_over_panel(page: Page, defaults: Locator) -> None:
    """The gallery lightbox is a modal dialog in the browser's top layer, so it covers the panel docked beside the charts, and its Esc closes it, not the panel."""
    image = page.locator('.metric-rect[data-slot-id="sample"] img').first
    image.scroll_into_view_if_needed()
    image.wait_for()
    panel = open_editor(page, defaults, "Project settings")
    image.click()
    lightbox = page.locator(".cdn-lightbox")
    expect(lightbox).to_be_visible()
    box = panel.bounding_box()
    assert box is not None
    covered = page.evaluate(
        "([x, y]) => !!document.elementFromPoint(x, y).closest('.cdn-lightbox')",
        [box["x"] + box["width"] / 2, box["y"] + box["height"] / 2],
    )
    assert covered, "the lightbox should cover the panel"
    page.keyboard.press("Escape")
    expect(lightbox).to_have_count(0)
    expect(panel).to_be_visible()
    panel_body(panel).focus()
    dismiss(page, panel, defaults, "Escape")


def other_tab_edit_survives(
    page: Page, editors, chart: Locator, metric: Locator, storage_key: str
) -> None:
    """Fields an open panel left alone, or changed and changed back, keep another tab's later edits to them through the panel's own edits and Revert; a chart keeps them through a rename here, and stays deleted when another tab deletes it."""
    other = page.context.new_page()

    def log_x(panel: Locator) -> Locator:
        return panel.get_by_role("checkbox", name="Log X", exact=True)

    def edit_elsewhere(trigger: Locator, name: str) -> tuple[str, bool]:
        """Change Smoothing and Log X in the other tab; return the values set."""
        twin = pin_trigger(other, trigger)
        panel = open_editor(other, twin, name)
        smoothing = panel.get_by_role("combobox", name="Smoothing", exact=True)
        target = (
            "triangular" if smoothing.input_value() != "triangular" else "ema-polyfit"
        )
        saving(other, storage_key, lambda: smoothing.select_option(target))
        logged = not toggle_live(other, log_x(panel), storage_key)
        dismiss(other, panel, twin, "Close")
        return target, logged

    def expect_elsewhere(trigger: Locator, name: str, edits: tuple[str, bool]) -> None:
        panel = open_editor(page, trigger, name)
        expect(
            panel.get_by_role("combobox", name="Smoothing", exact=True)
        ).to_have_value(edits[0])
        expect(log_x(panel)).to_be_checked(checked=edits[1])
        dismiss(page, panel, trigger, "Close")

    try:
        other.goto(page.url, wait_until="domcontentloaded")
        other.locator(".chart-container").first.wait_for(timeout=20_000)
        for trigger, name in editors:
            print(f"{name}: another tab's edit survives this panel's", flush=True)
            panel = open_editor(page, trigger, name)
            for _ in range(2):
                toggle_live(page, log_x(panel), storage_key)
            edits = edit_elsewhere(trigger, name)
            toggle_live(page, log_y_control(panel), storage_key)
            revert = panel.get_by_role("button", name="Revert", exact=True)
            saving(page, storage_key, revert.click)
            dismiss(page, panel, trigger, "Close")
            expect_elsewhere(trigger, name, edits)

        def inherits_log_y(scope: str, identity: str | None) -> bool:
            stored = page.evaluate(READ_OPTIONS, [storage_key, scope, identity])
            return "log_y" not in stored

        # Reset unpins a field though another tab changed the level above since the panel opened: the panel shows that level as it opened, and Reset sets its value.
        saved = page.evaluate("key => localStorage.getItem(key)", storage_key)
        for (trigger, name, scope), (parent, parent_name) in (
            ((editors[1][0], "Configure section", "section"), editors[0]),
            ((metric, "Configure Metric", "rect"), editors[1]),
        ):
            print(
                f"{name}: Reset unpins after another tab's {parent_name} edit",
                flush=True,
            )
            panel = open_editor(page, trigger, name)
            identity = (
                chart.get_attribute("data-slot-id")
                if scope == "rect"
                else panel.get_by_role(
                    "textbox", name="Name", exact=True
                ).get_attribute("placeholder")
            )
            assert inherits_log_y(scope, identity), f"{name} opened with Log Y pinned"
            toggle_live(page, log_y_control(panel), storage_key)
            dismiss(page, panel, trigger, "Close")
            panel = open_editor(page, trigger, name)
            twin = pin_trigger(other, parent)
            above = open_editor(other, twin, parent_name)
            toggle_live(other, log_y_control(above), storage_key)
            dismiss(other, above, twin, "Close")
            reset = (
                log_y_control(panel)
                .locator("xpath=ancestor::div[contains(@class, 'binding-field')][1]")
                .get_by_title("Reset to inherited value", exact=True)
            )
            saving(page, storage_key, reset.click)
            assert inherits_log_y(scope, identity), f"{name}'s Reset pinned"
            dismiss(page, panel, trigger, "Close")
        # Put back the defaults the other tab changed, in both tabs.
        page.evaluate(
            "([key, value]) => localStorage.setItem(key, value)", [storage_key, saved]
        )
        for tab in (page, other):
            tab.reload(wait_until="domcontentloaded")
            tab.locator(".chart-container").first.wait_for(timeout=20_000)

        print("Configure Metric: another tab's edit survives a rename", flush=True)
        edits = edit_elsewhere(metric, "Configure Metric")
        chart.locator(".rect-title").dblclick()
        rename = chart.locator(".rect-title-input")
        rename.fill("Renamed beside another tab")
        saving(page, storage_key, lambda: rename.press("Enter"))
        expect(chart.locator(".rect-title")).to_have_text("Renamed beside another tab")
        expect_elsewhere(metric, "Configure Metric", edits)

        print("Configure Metric: another tab's delete wins", flush=True)
        kept = page.evaluate("key => localStorage.getItem(key)", storage_key)
        slot = chart.get_attribute("data-slot-id")
        panel = open_editor(page, metric, "Configure Metric")
        twin = other.locator(f".metric-rect[data-slot-id={json.dumps(slot)}]")
        twin.hover()
        other.once("dialog", lambda dialog: dialog.accept())
        saving(other, storage_key, twin.locator('button[title="Delete"]').click)
        # This tab learns of the delete at its next edit, which records nothing and closes the panel with the chart.
        log_y_control(panel).click()
        expect(panel).to_have_count(0)
        expect(page.locator(".maximize-overlay")).to_have_count(0)
        expect(
            page.locator(f".metric-rect[data-slot-id={json.dumps(slot)}]")
        ).to_have_count(0)
        assert (
            slot
            in json.loads(
                page.evaluate("key => localStorage.getItem(key)", storage_key)
            )["deleted_rects"]
        )
        page.evaluate(
            "([key, value]) => localStorage.setItem(key, value)", [storage_key, kept]
        )
        page.reload(wait_until="domcontentloaded")
        page.locator(".chart-container").first.wait_for(timeout=20_000)
    finally:
        other.close()


def media_sections_open(page: Page) -> None:
    """Configure on a grid gallery or metadata chart opens on its own section, expanded, while the maximized copy's manifest is still on its way; the metadata chart's still does after the panel follows ←/→ to a chart added beside it and back, since the grid copy's hand-over outlives the maximize it was made for."""
    info = page.locator('.metric-rect[data-slot-id="info/run_info"]')
    neighbours = info.locator(SECTION_OF).locator(".metric-rect")
    info.locator(SECTION_OF).get_by_title("Add metric", exact=True).click()
    expect(neighbours).to_have_count(2)
    for slot, title, content, away_and_back in (
        (
            "info/run_info",
            "Metadata",
            ".metadata-viewer, .metadata-empty",
            ("ArrowRight", "ArrowLeft"),
        ),
        ("sample", "Image Gallery", ".cdn-image-item", ()),
    ):
        rect = page.locator(f".metric-rect[data-slot-id={json.dumps(slot)}]")
        rect.scroll_into_view_if_needed()
        # The grid copy has learned what it shows.
        rect.locator(content).first.wait_for()
        rect.hover()
        trigger = pin_trigger(page, rect.locator('button[title="Configure"]'))
        held = []
        page.route("**/cdn/*.json", lambda route: held.append(route))
        try:
            panel = open_editor(page, trigger, "Configure Metric")
            section = panel.get_by_role("button", name=title, exact=True)
            expect(section).to_have_attribute("aria-expanded", "true")
            for _ in range(50):
                if held:
                    break
                page.wait_for_timeout(100)
            assert held, "the maximized copy fetched no manifest to hold back"
            for key in away_and_back:
                follow(page, panel, key)
            expect(section).to_have_attribute("aria-expanded", "true")
        finally:
            for route in held:
                route.continue_()
            page.unroute("**/cdn/*.json")
        dismiss(page, panel, trigger, "Escape")
    neighbours.last.hover()
    page.once("dialog", lambda dialog: dialog.accept())
    neighbours.last.get_by_title("Delete", exact=True).click()
    expect(neighbours).to_have_count(1)


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
    # WebKit's Tab skips buttons unless Option is held, as Safari's does by default.
    back = (
        "Alt+Shift+Tab"
        if page.context.browser.browser_type.name == "webkit"
        else "Shift+Tab"
    )
    for _ in range(page.locator("button,input,a[href]").count() + 3):
        if trash.evaluate("element => element === document.activeElement"):
            break
        page.keyboard.press(back)
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
        "options panel fences require an active sidebar run for the colour-picker case",
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
        chart.locator(SECTION_OF).locator('.section-header button[title="Configure"]'),
    )
    defaults = pin_trigger(page, page.get_by_title("Project settings"))
    editors = (
        (defaults, "Project settings"),
        (section, "Configure section"),
        (metric, "Configure Metric"),
    )
    docking(page, defaults, section)
    double_presses_act_once(page, defaults, metric, storage_key)
    panel_resize(page, defaults)
    for trigger, name in editors:
        open_and_escape(page, trigger, name)
        keep_and_revert(page, trigger, name, storage_key)
        smoothing_hint(page, trigger, name)

    saved_layout = page.evaluate("key => localStorage.getItem(key)", storage_key)
    for trigger, name in editors:
        chart_option_fields(page, trigger, name, storage_key)
    chart_option_override_chips(page, metric, section, defaults, storage_key)
    section_fields(page, section, storage_key)
    settings_beside_the_panel(page, section)
    close_keeps_scroll(page, section)
    other_tab_edit_survives(page, editors, chart, metric, storage_key)
    chart_panel_ahead_of_the_sweep(page, chart, defaults, storage_key)
    follow_reuses_discovery(page, chart)
    editor_waits_for_the_run_list(page, chart)
    source_edit_keeps_sections(page)
    metric_picker(page, metric, storage_key)

    # Deleting the edited section closes its panel.
    panel = open_editor(page, section, "Configure section")
    page.once("dialog", lambda dialog: dialog.accept())
    section.locator("xpath=..").get_by_title("Delete section", exact=True).click()
    expect(panel).to_have_count(0)

    # Reset discards every customization, so an open editor must not survive to write its values back.
    page.once("dialog", lambda dialog: dialog.accept())
    panel = open_editor(page, defaults, "Project settings")
    page.get_by_title("Reset layout to auto-generated", exact=True).click()
    expect(panel).to_have_count(0)

    # Restore saved pins and section settings for the steps below.
    page.evaluate(
        """([key, value]) => value === null
            ? localStorage.removeItem(key) : localStorage.setItem(key, value)""",
        [storage_key, saved_layout],
    )
    page.reload(wait_until="domcontentloaded")
    page.locator(".chart-container").first.wait_for(timeout=20_000)

    # The panel consumes its Esc, so bulk selection stays active until the next one.
    bulk = page.locator("#sidebar-trash-trigger")
    bulk.press("Enter")
    expect(page.locator(".sidebar-trash-mode")).to_be_visible()
    panel = open_editor(page, defaults, "Project settings")
    dismiss(page, panel, defaults, "Escape")
    expect(page.locator(".sidebar-trash-mode")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator(".sidebar-trash-mode")).to_have_count(0)
    # Focus outside the sidebar (back on the panel's trigger) is not the picker's to move.
    expect(defaults).to_be_focused()

    color_picker_dismissal(page)
    panels_beside_maximize(page, chart, metric, section, defaults)
    panel_leaves_with_its_chart(page, metric)
    arrows_move_the_chart_panel(page, metric)
    follow_then_close_focus(page, chart)
    panel_keys_and_tips(page, metric, section)
    chart_panel_details(page, chart, metric, section)
    media_sections_open(page)
    lightbox_over_panel(page, defaults)

    # Configure on an already maximized chart leaves it maximized when the panel closes.
    chart.locator('button[title="Maximize"]').click()
    overlay = page.locator(".maximize-overlay")
    expect(overlay).to_be_visible()
    maximized_trigger = pin_trigger(page, overlay.locator('button[title="Configure"]'))
    panel = open_editor(page, maximized_trigger, "Configure Metric")
    # With its settings open beside it, the maximized chart's own Configure is gone; it comes back, with focus, as they close.
    expect(maximized_trigger).to_have_count(0)
    dismiss(page, panel, maximized_trigger, "Escape")
    expect(overlay).to_be_visible()
    # Project settings replace the chart panel; the chart stays maximized, and its own Esc still closes it.
    panel = open_editor(page, maximized_trigger, "Configure Metric")
    replacement = open_editor(page, defaults, "Project settings")
    expect(panel).to_have_count(0)
    dismiss(page, replacement, defaults, "Escape")
    expect(overlay).to_be_visible()
    page.keyboard.press("Escape")
    expect(overlay).to_have_count(0)

    # Esc in the inline header rename cancels the rename and leaves the chart maximized.
    chart.locator('button[title="Maximize"]').click()
    title = overlay.locator(".rect-title")
    original_title = title.inner_text()
    title.dblclick()
    rename = overlay.locator(".rect-title-input")
    rename.fill("cancelled panel fence rename")
    rename.press("Escape")
    expect(rename).to_have_count(0)
    expect(overlay).to_be_visible()
    expect(title).to_have_text(original_title)
    overlay.locator('button[title="Close"]').click()
    expect(overlay).to_have_count(0)

    page.goto(f"{parsed.scheme}://{parsed.netloc}/", wait_until="domcontentloaded")
    settings = pin_trigger(page, settings_trigger(page))
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
            # A context, so the cross-tab check can open a second page sharing its storage.
            page = browser.new_context(
                viewport=VIEWPORT, device_scale_factor=1
            ).new_page()
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("options panel fences passed")


if __name__ == "__main__":
    main()
