"""Exercise contextual help (docs/contextual-help.md): the ? preview and pin, where notes go, the Escape order, and the titles help replaced.

It only changes browser-local state (a filter, Trash-selection mode, a zoom, open panels); it never mutates runs.

Run it against a local stack seeded with seed_browser_fixture.py and seed_zoom_fixture.py:

    python help_fences.py http://127.0.0.1:<dashboard port> --browser webkit
"""

import argparse
import sys

from playwright.sync_api import Page, expect, sync_playwright

from user_settings_fences import VIEWPORT
from zoom_fences import RUN_PATH as ZOOM_RUN_PATH

DASHBOARD_PATH = "/browser-e2e"

VISIBILITY_DRAG = "Click (drag) to show/hide runs"
COUNTER_SHOWN = "Shown / total runs"
COUNTER_TRASH = "Selected for Trash / total runs"
TRASH_MARKING = "Red circle: selected for Trash"
RUN_FILTER = "Matches names, IDs, #numbers"
MORE_ACTIONS = "More actions"
TRASH_RUN = "Recoverable for 7 days, then deletes permanently"
RESIZE = "Drag to resize"
SECTION_HEADER = "Collapse/expand section"
SECTION_TRASH = "Remove section and panels; keep logged data"
CHART_TITLE = "Double-click to rename"
TRASH_SELECT = "Delete runs"
RESET_LAYOUT = "Reset layout"
CHART_GEAR = "Chart settings"
CHART_TRASH = "Remove chart; keep logged data"
CHART_RESIZE = "Drag horizontally: width; vertically: section chart heights"
ZOOM_DRAG = "Drag horizontally to zoom"
RESET_ZOOM = "Click to reset zoom"
MAXIMIZED_KEYS = "←/→: panels; Esc: close"
LIVE_EDITING = "Edits apply immediately"
PROJECT_DEFAULTS = "Most specific setting wins: chart, section, project"
LAYOUT_SAVED = "Settings and layout saved in this browser only"
CLICK_TO_COPY = "Click to copy"
LOG_SEARCH = "Searches the full log; resubmit to refresh"
LAST_LOGGED_SORT = "Press again to refresh the order"

# Each note's lines, where it is drawn and what holds it, after the frame that places it.
NOTES = """() => new Promise(done => requestAnimationFrame(() => requestAnimationFrame(() => done(
    [...document.querySelectorAll('.kymo-help-note')].map(note => {
        const box = note.getBoundingClientRect();
        const hit = document.elementFromPoint((box.left + box.right) / 2, (box.top + box.bottom) / 2);
        return {
            lines: note.textContent.split('\\n'),
            inside: box.left >= 0 && box.top >= 0 && box.right <= innerWidth && box.bottom <= innerHeight,
            hit: hit === note,
            popover: !!note.parentElement.closest('[popover]'),
        };
    })))))"""


def notes(page: Page) -> list[dict]:
    found = page.evaluate(NOTES)
    for note in found:
        assert note["inside"], f"a note reaches outside the window: {note['lines']}"
        assert not note["hit"], f"a note takes pointer events: {note['lines']}"
    return found


# For the first three rendered elements matching a selector (a family's note may sit on any of them): how each note carrying a text lies against the nearest.
BESIDE = """([selector, text]) => new Promise(done => requestAnimationFrame(() => requestAnimationFrame(() => {
    const boxes = [...document.querySelectorAll(selector)].filter(el => el.checkVisibility()).slice(0, 3).map(el => el.getBoundingClientRect());
    done([...document.querySelectorAll('.kymo-help-note')]
        .filter(note => note.textContent.split('\\n').includes(text))
        .flatMap(note => {
            const b = note.getBoundingClientRect();
            return boxes.map(box => ({
                over: b.left < box.right && box.left < b.right && b.top < box.bottom && box.top < b.bottom,
                gap: Math.max(b.left - box.right, box.left - b.right, b.top - box.bottom, box.top - b.bottom),
            }));
        }));
})))"""


def expect_beside(page: Page, selector: str, text: str) -> None:
    """The text's note touches the control without covering it."""
    found = page.evaluate(BESIDE, [selector, text])
    assert any(not note["over"] and note["gap"] <= 8 for note in found), (
        f"no note for {text!r} sits beside {selector}: {found}"
    )


def texts(page: Page) -> list[str]:
    return [line for note in notes(page) for line in note["lines"]]


def expect_texts(page: Page, present=(), absent=()) -> list[str]:
    shown = texts(page)
    for text in present:
        assert text in shown, f"{text!r} should show; shown: {shown}"
    for text in absent:
        assert text not in shown, f"{text!r} should not show; shown: {shown}"
    return shown


def trigger(page: Page):
    return page.locator("#kymo-help")


def pinned(page: Page) -> bool:
    return trigger(page).get_attribute("aria-pressed") == "true"


def park(page: Page) -> None:
    """Rest the pointer on the navbar's brand, which carries no help and reveals nothing."""
    page.locator(".navbar-brand").hover()


def pin(page: Page) -> None:
    assert not pinned(page)
    trigger(page).hover()
    page.mouse.down()
    page.mouse.up()
    assert pinned(page)
    park(page)


def escape(page: Page) -> None:
    page.keyboard.press("Escape")


def open_dashboard(page: Page, origin: str, path: str = DASHBOARD_PATH) -> None:
    page.goto(origin + path, wait_until="domcontentloaded")
    page.wait_for_selector(".chart-container canvas", timeout=30_000)
    expect(trigger(page)).to_have_attribute("aria-label", "Explain controls")
    assert trigger(page).get_attribute("title") is None, "? itself has no tooltip"
    park(page)


def pages_without_help(page: Page, origin: str) -> None:
    """Projects and Trash have nothing to annotate and no ?; the Projects texts are native titles."""
    page.goto(origin + "/trash", wait_until="domcontentloaded")
    page.wait_for_selector(".trash-header", timeout=30_000)
    expect(trigger(page)).to_have_count(0)
    page.goto(origin + "/", wait_until="domcontentloaded")
    page.wait_for_selector(".project-row", timeout=30_000)
    expect(trigger(page)).to_have_count(0)
    by_project = page.get_by_role("button", name="Sort by Project", exact=True)
    by_logged = page.get_by_role("button", name="Sort by Last logged", exact=True)
    by_logged.click()
    expect(by_logged).to_have_attribute("title", LAST_LOGGED_SORT)
    by_project.click()
    assert by_logged.get_attribute("title") is None, "only the active sort re-sorts"
    assert by_project.get_attribute("title") is None


def preview(page: Page) -> None:
    """Hovering or focusing ? shows the notes at once; leaving hides them; nothing is stored."""
    stored = "() => JSON.stringify(Object.entries(localStorage).sort())"
    before = page.evaluate(stored)
    assert notes(page) == []
    trigger(page).hover()
    shown = expect_texts(
        page,
        present=[VISIBILITY_DRAG, COUNTER_SHOWN, RESIZE, SECTION_HEADER],
        absent=[RUN_FILTER, TRASH_RUN, COUNTER_TRASH, CHART_TRASH, MAXIMIZED_KEYS],
    )
    assert ZOOM_DRAG in shown, f"the chart's own texts are missing: {shown}"
    assert CHART_TITLE in shown, f"the chart title is not explained: {shown}"
    # Titles that already explained their control show under ? too.
    assert TRASH_SELECT in shown and RESET_LAYOUT in shown, shown
    # Every run row's … button is brought out while notes show, and one is explained.
    assert MORE_ACTIONS in shown, shown
    dots = "[...document.querySelectorAll('.run-overflow-trigger')].map(el => getComputedStyle(el).opacity)"
    assert set(page.evaluate(dots)) == {"1"}
    park(page)
    assert set(page.evaluate(dots)) == {"0"}, "the dots stayed out after help went"
    trigger(page).hover()
    # Two runs, two circles, one note: a family shows one example, on re-entry too.
    assert texts(page).count(VISIBILITY_DRAG) == 1
    park(page)
    assert notes(page) == []
    assert not pinned(page)

    trigger(page).focus()
    expect_texts(page, present=[VISIBILITY_DRAG])
    # The keyboard presses it through the app's click bridge.
    page.keyboard.press("Enter")
    assert pinned(page)
    page.keyboard.press("Enter")
    assert not pinned(page)
    assert notes(page) == [], "an unpin hides the notes although ? keeps focus"
    # The pointer entering ? is a new entry; focus then holds the preview when it goes.
    trigger(page).hover()
    expect_texts(page, present=[VISIBILITY_DRAG])
    park(page)
    expect_texts(page, present=[VISIBILITY_DRAG])
    page.evaluate("document.activeElement.blur()")
    assert notes(page) == []
    assert page.evaluate(stored) == before, "help stores nothing"


def pin_and_unpin(page: Page) -> None:
    """A press pins without taking focus; an unpin hides the notes until the pointer comes back."""
    field = page.locator(".sidebar-filter")
    field.click()
    page.keyboard.type("off")
    page.keyboard.press("ArrowLeft")
    trigger(page).hover()
    page.mouse.down()
    page.mouse.up()
    assert pinned(page)
    expect(field).to_be_focused()
    assert field.evaluate("field => field.selectionStart") == 2, "the caret moved"
    expect_texts(page, present=[RUN_FILTER, VISIBILITY_DRAG])
    park(page)
    expect_texts(page, present=[RUN_FILTER, VISIBILITY_DRAG])

    # Escape goes to the filter first, and to help only when nothing else takes it.
    escape(page)
    expect(field).to_have_value("")
    assert pinned(page)
    escape(page)
    assert not pinned(page)
    assert notes(page) == []
    page.evaluate("document.activeElement.blur()")

    trigger(page).hover()
    page.mouse.down()
    page.mouse.up()
    page.mouse.down()
    page.mouse.up()
    assert not pinned(page)
    assert notes(page) == [], "the pointer is still on ?"
    park(page)
    trigger(page).hover()
    expect_texts(page, present=[VISIBILITY_DRAG], absent=[RUN_FILTER])
    park(page)


def presses_and_reveals(page: Page) -> None:
    """While pinned: no note under a held press, and controls that appear on hover are explained then."""
    pin(page)
    expect_texts(page, present=[SECTION_HEADER], absent=[CHART_TRASH, CHART_GEAR])
    chart = page.locator(".metric-rect").first
    chart.hover()
    expect_texts(page, present=[CHART_TRASH, CHART_GEAR, CHART_RESIZE])
    page.locator(".section-header").first.hover()
    expect_texts(page, present=[SECTION_TRASH])
    count = page.locator(".sidebar-meta")
    count.hover()
    expect_beside(page, ".sidebar-meta", COUNTER_SHOWN)
    page.mouse.down()
    assert notes(page) == [], "a note stayed under a held press"
    box = count.bounding_box()
    page.mouse.move(box["x"] + 60, box["y"] + 90, steps=3)
    assert notes(page) == [], "a note came back under a drag"
    page.mouse.up()
    expect_texts(page, present=[COUNTER_SHOWN])
    # A press that moves focus: the notes stay away until the release all the same.
    field = page.locator(".sidebar-filter")
    field.hover()
    page.mouse.down()
    expect(field).to_be_focused()
    assert notes(page) == [], "focus moving under a held press brought the notes back"
    page.mouse.up()
    expect_texts(page, present=[RUN_FILTER])
    # The keyboard presses a button through the app's click bridge, with no release to wait for.
    toggle = page.locator(".section-toggle").first
    toggle.focus()
    page.keyboard.press("Enter")
    expect(toggle).to_have_attribute("aria-expanded", "false")
    expect_texts(page, present=[SECTION_HEADER], absent=[ZOOM_DRAG])
    page.keyboard.press("Enter")
    expect(toggle).to_have_attribute("aria-expanded", "true")
    park(page)
    page.wait_for_selector(".chart-container canvas")
    expect_texts(page, present=[SECTION_HEADER, ZOOM_DRAG])
    page.evaluate("document.activeElement.blur()")
    escape(page)
    assert not pinned(page)


def popover_and_trash_selection(page: Page) -> None:
    """A note for a menu item is inside the menu; Escape closes the menu, then Trash selection, then help."""
    pin(page)
    row = page.locator(".sidebar-run").first
    row.hover()
    # The row's … button is explained.
    expect_texts(page, present=[MORE_ACTIONS])
    row.locator(".run-overflow-trigger").click()
    menu = page.locator(".run-overflow-menu:popover-open")
    expect(menu).to_have_count(1)
    (inside,) = [note for note in notes(page) if TRASH_RUN in note["lines"]]
    assert inside["popover"], "the menu's note is not inside the menu"
    expect_beside(page, ".run-overflow-menu:popover-open [data-help]", TRASH_RUN)
    escape(page)
    expect(menu).to_have_count(0)
    assert pinned(page)
    park(page)
    expect_texts(page, absent=[TRASH_RUN])

    page.locator(".sidebar-trash-trigger").click()
    expect(page.locator(".sidebar-trash-mode")).to_have_count(1)
    park(page)
    expect_texts(
        page,
        present=[COUNTER_TRASH, TRASH_MARKING],
        absent=[COUNTER_SHOWN, VISIBILITY_DRAG],
    )
    escape(page)
    expect(page.locator(".sidebar-trash-mode")).to_have_count(0)
    assert pinned(page)
    expect_texts(page, present=[COUNTER_SHOWN, VISIBILITY_DRAG])
    escape(page)
    assert not pinned(page)


def maximized_and_panel(page: Page) -> None:
    """What a maximized chart covers is not explained; the settings panel and the chart take Escape before help."""
    pin(page)
    chart = page.locator(".metric-rect").first
    chart.hover()
    chart.get_by_title("Maximize", exact=True).click()
    overlay = page.locator(".maximize-overlay")
    expect(overlay).to_have_count(1)
    park(page)
    expect_texts(
        page,
        present=[MAXIMIZED_KEYS, ZOOM_DRAG, VISIBILITY_DRAG],
        absent=[SECTION_HEADER],
    )
    escape(page)
    expect(overlay).to_have_count(0)
    assert pinned(page)
    # The app rendered with the pointer at rest: the notes follow it.
    expect_texts(page, present=[SECTION_HEADER], absent=[MAXIMIZED_KEYS])

    page.get_by_title("Project settings", exact=True).click()
    panel = page.locator("#kymo-panel")
    expect(panel).to_be_visible()
    park(page)
    shown = expect_texts(page, present=[LIVE_EDITING, PROJECT_DEFAULTS, LAYOUT_SAVED])
    # The run list's edge and the panel's are two families with one text.
    assert shown.count(RESIZE) == 2, shown
    panel.locator("input, select").first.focus()
    escape(page)
    expect(panel).to_have_count(0)
    assert pinned(page)
    escape(page)
    assert not pinned(page)


def replaced_titles(page: Page) -> None:
    """A text that replaced a title is that title, and the control keeps its old name."""
    section = page.locator(".section-header").first
    section.hover()
    trash = section.get_by_role("button", name="Delete section", exact=True)
    expect(trash).to_have_attribute("title", SECTION_TRASH)
    chart = page.locator(".metric-rect").first
    chart.hover()
    expect(
        chart.get_by_role("button", name="Delete chart", exact=True)
    ).to_have_attribute("title", CHART_TRASH)
    expect(chart.get_by_role("button", name=CHART_GEAR, exact=True)).to_have_attribute(
        "title", CHART_GEAR
    )
    expect(chart.locator(".rect-resize-handle")).to_have_attribute(
        "title", CHART_RESIZE
    )
    # The maximized chart's gear says the same.
    chart.get_by_title("Maximize", exact=True).click()
    maximized = page.locator(".maximize-overlay .metric-rect")
    expect(maximized.get_by_title(CHART_GEAR, exact=True)).to_have_count(1)
    escape(page)
    expect(page.locator(".maximize-overlay")).to_have_count(0)
    search = page.locator(".text-stream-search").first
    expect(search).to_have_attribute("title", LOG_SEARCH)
    expect(search).to_have_attribute("aria-label", "Search logs")


def large_font_and_copy(page: Page) -> None:
    """At the largest font every note still fits the window; captions say they copy."""
    page.evaluate(
        "document.documentElement.style.setProperty('--kymo-user-font-size', '24px')"
    )
    page.locator(".cdn-caption").first.scroll_into_view_if_needed()
    trigger(page).hover()
    assert texts(page), "no notes at the largest font"
    expect_beside(page, ".cdn-caption", CLICK_TO_COPY)
    park(page)
    # A short window: every note still fits (notes() checks each).
    page.set_viewport_size({"width": VIEWPORT["width"], "height": 420})
    trigger(page).hover()
    assert texts(page), "no notes in a short window"
    park(page)
    page.set_viewport_size(VIEWPORT)
    page.evaluate(
        "document.documentElement.style.removeProperty('--kymo-user-font-size')"
    )
    page.evaluate("scrollTo(0, 0)")


# The stylesheet's readout labels, on stand-in readouts built the way hover.js builds one (no seeded chart has a range or a borrowed point). Per readout: the labels drawn (over the first row's colored value, over the gray column's first populated cell, under a readout with a borrowed point, and the copy shortcut's over the readout), whether each has its tick and no other element one (a borrowed point's is on the last row's @ cell), and the looks of the labels drawn that differ from a note's.
READOUT_LABELS = """() => Object.fromEntries([
    ['raw', [['0.5', 'kymo-tip-rawval'], ['0.1\u20130.9', '']]],
    ['range', [['', ''], ['0.1\u20130.9', 'kymo-tip-range']]],
    ['bare', [['', ''], ['', '']]],
    ['borrowed', [['', '', ''], ['', '', '@ 3']]],
].map(([name, rows]) => {
    const tip = Object.assign(document.createElement('div'), {className: 'kymo-tip kymo-tip-src'});
    for (const [property, text] of [['--help-colored', 'Smoothed'], ['--help-raw', 'Raw value'], ['--help-range', 'Range'], ['--help-borrowed', '@'], ['--help-copy', 'Copy']]) {
        tip.style.setProperty(property, JSON.stringify(text));
    }
    tip.innerHTML = '<div class="kymo-tip-step">1</div><div class="kymo-tip-grid' + (name === 'borrowed' ? ' kymo-tip-borrowed' : '') + '" style="grid-template-columns:minmax(0,max-content) max-content max-content' + (name === 'borrowed' ? ' max-content' : '') + '">' + rows.map(([gray, cls, x]) =>
        '<div class="kymo-tip-row"><span class="kymo-tip-name">a</span><span class="kymo-tip-val">1</span><span class="kymo-tip-raw ' + cls + '">' + gray + '</span>' + (x === undefined ? '' : '<span class="kymo-tip-x">' + x + '</span>') + '</div>').join('') + '</div>';
    document.body.appendChild(tip);
    const style = (el, pseudo) => getComputedStyle(el, pseudo);
    const cells = [...tip.querySelectorAll('.kymo-tip-val, .kymo-tip-raw')], grid = tip.querySelector('.kymo-tip-grid'), xs = [...tip.querySelectorAll('.kymo-tip-x')];
    const labelled = [...cells, grid, tip];
    const labels = labelled.map(el => style(el, '::after').content);
    // The grid's tick is on the last @ cell; no other @ cell, and neither the grid itself, has anything.
    const ticks = [...cells, xs.at(-1) ?? grid, tip].map(el => style(el, '::before').content);
    const stray = [...xs.slice(0, -1).flatMap(el => [style(el, '::before').content, style(el, '::after').content]), ...xs.slice(-1).map(el => style(el, '::after').content), ...(xs.length ? [style(grid, '::before').content] : [])];
    const look = look => [look.backgroundColor, look.color, look.fontWeight, look.fontSize, look.paddingLeft, look.borderTopLeftRadius].join();
    const unlike = labelled.filter((el, at) => labels[at] !== 'none').map(el => look(style(el, '::after'))).filter(drawn => drawn !== look(style(document.querySelector('.kymo-help-note'))));
    tip.remove();
    return [name, {labels, ticked: ticks.join() === labels.join() && stray.every(content => content === 'none'), unlike}];
}))"""

# What the shown readout draws, if one shows: its copy tick and label, its first row's labels over the colored value and the gray one, and its grid's @ label.
SHOWN_LABELS = """() => {
    const tip = [...document.querySelectorAll('.kymo-tip-src')].find(tip => tip.style.display === 'block');
    const cell = selector => getComputedStyle(tip.querySelector(selector), '::after').content;
    return tip && [getComputedStyle(tip, '::before').content, getComputedStyle(tip, '::after').content, cell('.kymo-tip-row:first-child > .kymo-tip-val'), cell('.kymo-tip-rawval'), cell('.kymo-tip-grid')];
}"""

COPY = (
    '"\u2318C copies values"' if sys.platform == "darwin" else '"Ctrl+C copies values"'
)


def readout_labels(page: Page) -> None:
    """A chart readout is labelled while help is pinned: its columns (the gray one for what it holds), a borrowed point's @, and the copy shortcut."""
    names = ("raw", "range", "bare", "borrowed")
    off = {
        name: {"labels": ["none"] * 6, "ticked": True, "unlike": []} for name in names
    }
    assert page.evaluate(READOUT_LABELS) == off
    pin(page)
    drawn = {
        "raw": ['"Smoothed"', '"Raw value"', "none", "none", "none", '"Copy"'],
        "range": ['"Smoothed"', "none", "none", '"Range"', "none", '"Copy"'],
        "bare": ['"Smoothed"', "none", "none", "none", "none", '"Copy"'],
        "borrowed": ['"Smoothed"', "none", "none", "none", '"@"', '"Copy"'],
    }
    readouts = page.evaluate(READOUT_LABELS)
    assert {name: readouts[name]["labels"] for name in names} == drawn
    # Every label has its tick and looks like a note (the release bundle's CSS minifier once took the labels' look away).
    assert all(
        readout["ticked"] and not readout["unlike"] for readout in readouts.values()
    ), readouts
    # A held press takes every label and tick off with the notes.
    page.evaluate("document.documentElement.setAttribute('data-kymo-held', '')")
    held = page.evaluate(READOUT_LABELS)
    page.evaluate("document.documentElement.removeAttribute('data-kymo-held')")
    assert held == off, held
    # A real readout: the seeded chart is smoothed, and its script and hover.js leave what the stylesheet reads.
    plot = page.locator(".chart-container .u-over").first
    plot.scroll_into_view_if_needed()
    box = plot.bounding_box()
    page.mouse.move(box["x"] + box["width"] * 0.4, box["y"] + box["height"] / 2)
    # No row of it is borrowed, so nothing explains an @.
    shown = [COPY, COPY, '"Smoothed"', '"Raw value"', "none"]
    page.wait_for_function(
        f"shown => String(({SHOWN_LABELS})()) === String(shown)", arg=shown
    )
    page.mouse.down()
    assert page.evaluate(SHOWN_LABELS) == ["none"] * 5
    page.mouse.up()
    assert page.evaluate(SHOWN_LABELS) == shown
    park(page)
    page.evaluate("scrollTo(0, 0)")
    escape(page)
    assert not pinned(page)


def navigation_ends_a_pin(page: Page) -> None:
    pin(page)
    page.locator(".sidebar-run .run-details-link").first.click()
    page.wait_for_url("**/browser-e2e/*")
    expect(trigger(page)).to_have_attribute("aria-pressed", "false")
    assert notes(page) == []
    # Help is off, not stuck: one press pins it again.
    pin(page)
    escape(page)
    assert not pinned(page)
    page.go_back()
    page.wait_for_selector(".sidebar-run", timeout=30_000)


def zoomed_chart(page: Page, origin: str) -> None:
    """A chart's zoom changes what it explains."""
    open_dashboard(page, origin, ZOOM_RUN_PATH)
    trigger(page).hover()
    expect_texts(page, present=[ZOOM_DRAG], absent=[RESET_ZOOM])
    park(page)
    plot = page.locator(".chart-container .u-over").first
    plot.scroll_into_view_if_needed()
    box = plot.bounding_box()
    y = box["y"] + box["height"] / 2
    # Escape belongs to a drag in progress: it cancels the drag and leaves the pin.
    pin(page)
    page.mouse.move(box["x"] + box["width"] * 0.3, y)
    page.mouse.down()
    page.mouse.move(box["x"] + box["width"] * 0.5, y, steps=4)
    assert page.evaluate("!!window.__kymo_zg"), "the drag did not arm"
    escape(page)
    assert not page.evaluate("!!window.__kymo_zg")
    assert pinned(page)
    page.mouse.up()
    expect_texts(page, absent=[RESET_ZOOM])
    escape(page)
    assert not pinned(page)

    page.mouse.move(box["x"] + box["width"] * 0.3, y)
    page.mouse.down()
    page.mouse.move(box["x"] + box["width"] * 0.6, y, steps=4)
    page.mouse.up()
    page.evaluate("scrollTo(0, 0)")
    trigger(page).hover()
    page.wait_for_function(
        "text => [...document.querySelectorAll('.kymo-help-note')].some(n => n.textContent.includes(text))",
        arg=RESET_ZOOM,
    )
    park(page)


def run_fences(page: Page, origin: str) -> None:
    pages_without_help(page, origin)
    open_dashboard(page, origin)
    preview(page)
    pin_and_unpin(page)
    presses_and_reveals(page)
    popover_and_trash_selection(page)
    maximized_and_panel(page)
    replaced_titles(page)
    large_font_and_copy(page)
    readout_labels(page)
    navigation_ends_a_pin(page)
    zoomed_chart(page, origin)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="dashboard origin")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        page = browser.new_context(viewport=VIEWPORT, device_scale_factor=1).new_page()
        try:
            run_fences(page, args.url.rstrip("/"))
        finally:
            browser.close()
    print("contextual help fences passed")


if __name__ == "__main__":
    main()
