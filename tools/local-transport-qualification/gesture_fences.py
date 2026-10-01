"""Check sidebar focus, native activation, and drag-paint isolation.

Bulk mode and normal mode paint different sets: bulk paints the deletion selection, normal paints
chart visibility. A gesture that stayed armed across the switch used to carry on into the other set,
which queued runs for deletion the user never picked. The guarantee is that each transition ends an
armed gesture at the transition itself, and it lives in component wiring rather than in a pure
function, so nothing below the browser can check it.

Each direction is checked in three parts, because the assertion that matters is an absence and an
absence is equally true when the test does nothing at all:

  1. the press armed a gesture (it painted the row it started on);
  2. the gesture propagates to a second row on hover, with no mode change -- the positive control,
     without which a broken hover, a moved hit target or a dropped button state would leave both
     fences green forever;
  3. after the mode change, hovering paints nothing into the other mode's set.

Each direction is asserted separately, because a round trip would be cleared by either of the two
cleanups and so proves neither of them individually.

Covers the cancel-button exit from bulk mode. The Escape exit and the post-delete exit end the paint
through different call sites and are not exercised here.

Run this check locally against any dashboard project with at least two runs:

    python gesture_fences.py http://127.0.0.1:8080/some-project --browser webkit

WebKit matters: only its native checkbox aux-click activation makes the right/middle-click assertions catch a missing aux-click guard.
The platform overrides check both app policies; they do not simulate another OS's native menus. The fence suppresses native menus, so those still need a hand test.

The sidebar only renders when no run is selected (see dashboard_layout.rs), so the URL must be a
project page, not a run page.
"""

import argparse

from playwright.sync_api import expect, sync_playwright

IN_BULK = "!!document.querySelector('.sidebar-trash-mode')"


def click_without_native_menu(page, target, **options) -> None:
    # Record app cancellation, then keep the native menu from consuming later test input.
    probe = page.evaluate_handle("""() => {
        const cancelled = [];
        const listener = event => {
            cancelled.push(event.defaultPrevented);
            event.preventDefault();
        };
        window.addEventListener('contextmenu', listener);
        return {cancelled, listener};
    }""")
    try:
        target.click(**options)
    finally:
        cancelled = probe.evaluate("""probe => {
            window.removeEventListener('contextmenu', probe.listener);
            return probe.cancelled;
        }""")
        probe.dispose()
    assert not any(cancelled), "the app suppressed the native context menu"


def run_fences(page) -> None:
    """Check drag focus and both mode transitions in a sidebar with at least two runs."""
    page.wait_for_selector(".sidebar-run", timeout=30_000)
    rows = page.locator(".sidebar-run")
    if rows.count() < 2:
        raise AssertionError("gesture fences need a project with at least two runs")

    # The two rows are pinned by run id rather than by nth(), which re-resolves before every action
    # and would silently move to another run if a live list reordered mid-test. Run ids are
    # caller-supplied and may contain quotes or other CSS metacharacters, so each id goes through
    # CSS.escape -- and the result goes inside quotes, because a bare "-" escapes to "\-" and an
    # empty id to "", neither of which parses as an unquoted attribute value. Empty ids are legal
    # server-side (ingest.rs storable_ident), hence `is not None` rather than a truthiness check.
    def pin(index: int):
        run_id = rows.nth(index).get_attribute("data-run-id")
        assert run_id is not None, "sidebar rows should carry data-run-id"
        escaped = page.evaluate("id => CSS.escape(id)", run_id)
        return page.locator(f'.sidebar-run[data-run-id="{escaped}"]')

    armed_row, painted_row = pin(0), pin(1)

    def visible_order() -> list:
        return page.eval_on_selector_all(
            ".sidebar-run", "els => els.map(e => e.getAttribute('data-run-id'))"
        )

    order = visible_order()

    def visible(row) -> bool:
        # Normal mode has native checkboxes; bulk mode uses spans.
        return row.locator(".run-marker-input:checked").count() == 1

    def expect_visible(row, selected: bool) -> None:
        control = row.locator(".run-marker-input")
        expect(control).to_be_checked(checked=selected)
        assert control.get_attribute("title").startswith(
            "Hide " if selected else "Show "
        )

    marker = armed_row.locator(".run-marker-input")
    field = page.locator(".sidebar-filter")
    label = armed_row.locator(".run-marker-toggle")
    for padding in (False, True):
        field.focus()
        before = visible(armed_row)
        if padding:
            label.click(position={"x": 3, "y": 3})
        else:
            marker.click()
        expect(field).to_be_focused()
        expect_visible(armed_row, not before)

    assert page.evaluate("!Object.hasOwn(navigator, 'platform')"), (
        "platform policy checks require an unmodified navigator.platform"
    )
    for mac in (True, False):
        try:
            page.evaluate(
                """mac => Object.defineProperty(navigator, 'platform', {
                    configurable: true, value: mac ? 'MacIntel' : 'Linux x86_64'
                })""",
                mac,
            )
            before = visible(armed_row)
            click_without_native_menu(page, marker, modifiers=["Control"])
            expect_visible(armed_row, before if mac else not before)
        finally:
            page.evaluate("delete navigator.platform")
    for button in ("right", "middle"):
        before = visible(armed_row)
        click_without_native_menu(page, marker, button=button)
        expect_visible(armed_row, before)

    # A padding drag keeps the filter focused and paints both rows once.
    field.focus()
    selected = not visible(armed_row)
    label.hover(position={"x": 3, "y": 3})
    page.mouse.down()
    try:
        painted_row.locator(".run-marker-toggle").hover(position={"x": 3, "y": 3})
    finally:
        page.mouse.up()
    expect(field).to_be_focused()
    expect_visible(armed_row, selected)
    expect_visible(painted_row, selected)

    # A release must end painting before the next press, including a Ctrl-held release.
    for ctrl_release in (False, True):
        before = visible(armed_row)
        if visible(painted_row) != before:
            painted_row.locator(".run-marker-input").click()
        expect_visible(painted_row, before)
        marker.hover()
        page.mouse.down()
        try:
            if ctrl_release:
                page.keyboard.down("Control")
        finally:
            page.mouse.up()
            if ctrl_release:
                page.keyboard.up("Control")
        expect_visible(armed_row, not before)
        # Keep the button held over the rows: a buttonless hover clears stale paint itself.
        page.locator(".sidebar-filter-row").hover()
        page.mouse.down()
        try:
            armed_row.hover()
            painted_row.hover()
        finally:
            page.mouse.up()
        expect_visible(painted_row, before)

    assert visible_order() == order, (
        "the run list changed before the keyboard focus check"
    )
    field.focus()
    tab = "Alt+Tab" if page.context.browser.browser_type.name == "webkit" else "Tab"
    # Firefox includes the scrollable sidebar itself in the tab order.
    for _ in range(2):
        page.keyboard.press(tab)
        if marker.evaluate("el => el === document.activeElement"):
            break
    expect(marker).to_be_focused()
    expect(marker).to_have_css("outline-style", "solid")

    # Clicking B preserves keyboard focus on A; Space still toggles A.
    other_before = visible(painted_row)
    painted_row.locator(".run-marker-input").click()
    expect(marker).to_be_focused()
    expect_visible(armed_row, selected)
    expect_visible(painted_row, not other_before)
    page.keyboard.press("Space")
    expect_visible(armed_row, not selected)
    expect_visible(painted_row, not other_before)
    # Native detail-zero activation must still toggle exactly once.
    marker.evaluate("el => el.click()")
    expect_visible(armed_row, selected)

    def queued_for_deletion() -> int:
        return page.locator(".sidebar-run-trash-selected").count()

    def hover(row) -> None:
        # The pointer has to leave the row without leaving the sidebar: the sidebar's own onmouseleave ends a gesture, so hovering away past its edge would let these fences pass even while broken. Naming both targets as locators keeps that structural rather than positional -- the filter row lives inside the sidebar and is never a run row -- and re-resolves them after each render, since the two modes lay the sidebar out differently. Playwright keeps the primary button held across a hover, and its actionability checks fail if either target stops being visible, stable, or able to receive pointer events.
        page.locator(".sidebar-filter-row").hover()
        row.hover()
        page.wait_for_timeout(300)

    # Hide both rows first: a leaked bulk gesture paints "select", which would be invisible against
    # rows that are already visible.
    for row in (armed_row, painted_row):
        if visible(row):
            row.locator(".run-marker-toggle").click()
            page.wait_for_timeout(300)
        assert not visible(row), "the marker press did not hide the run"
    # A gesture ends by itself when the visible ordering changes, so on a live project an unrelated
    # run arriving mid-test would end it for reasons that have nothing to do with the mode change and
    # let the fences below pass without their cleanup. Pin the ordering and check it before each.
    assert visible_order() == order, "the run list changed before the mode checks"

    # Context-menu gestures must not arm deletion painting either.
    page.locator("#sidebar-trash-trigger").press("Enter")
    page.wait_for_function(IN_BULK, timeout=10_000)
    try:
        for mac in (True, False):
            page.evaluate(
                """mac => Object.defineProperty(navigator, 'platform', {
                    configurable: true, value: mac ? 'MacIntel' : 'Linux x86_64'
                })""",
                mac,
            )
            click_without_native_menu(page, armed_row, modifiers=["Control"])
            expect(page.locator(".sidebar-run-trash-selected")).to_have_count(
                0 if mac else 1
            )
    finally:
        page.evaluate("delete navigator.platform")
        page.locator(".sidebar-trash-cancel").press("Enter")
        page.wait_for_function(f"!({IN_BULK})", timeout=10_000)

    # Leaving bulk mode must end a bulk gesture on its own, or the next hover in normal mode repaints
    # chart visibility.
    page.locator("#sidebar-trash-trigger").press("Enter")
    page.wait_for_function(IN_BULK, timeout=10_000)
    armed_row.hover()
    page.mouse.down()
    assert queued_for_deletion() == 1, "the bulk press armed nothing"
    hover(painted_row)
    assert queued_for_deletion() == 2, (
        "the armed bulk gesture did not propagate on hover, so the fence below would prove nothing"
    )
    page.locator(".sidebar-trash-cancel").press("Enter")
    page.wait_for_function(f"!({IN_BULK})", timeout=10_000)
    hover(painted_row)
    page.mouse.up()
    # The pinned row, not a total: on a live dashboard another row going hidden could offset a leaked
    # one and let a total-delta comparison pass while the invariant is broken.
    assert visible_order() == order, (
        "the run list changed mid-test; this fence proved nothing"
    )
    assert not visible(painted_row), (
        "a bulk gesture survived leaving bulk mode and painted chart visibility"
    )

    # Entering bulk mode must end a visibility gesture on its own, or the next hover queues runs for
    # deletion the user never picked.
    armed_row.locator(".run-marker-toggle").hover()
    page.mouse.down()
    assert visible(armed_row), "the visibility press armed nothing"
    hover(painted_row)
    assert visible(painted_row), (
        "the armed visibility gesture did not propagate on hover, so the fence below would prove nothing"
    )
    page.locator("#sidebar-trash-trigger").press("Enter")
    page.wait_for_function(IN_BULK, timeout=10_000)
    hover(painted_row)
    page.mouse.up()
    assert visible_order() == order, (
        "the run list changed mid-test; this fence proved nothing"
    )
    painted = queued_for_deletion()
    assert painted == 0, (
        f"a visibility gesture survived entering bulk mode and queued {painted} run(s) for deletion"
    )
    page.locator(".sidebar-trash-cancel").press("Enter")
    page.wait_for_function(f"!({IN_BULK})", timeout=10_000)

    # An unchanged draft makes this safe even if a regression accidentally blurs the editor.
    armed_row.locator(".run-overflow-trigger").click()
    armed_row.locator(".run-overflow-menu:popover-open").get_by_role(
        "button", name="Rename", exact=True
    ).click()
    editor = armed_row.locator(".run-name-inline-input")
    expect(editor).to_be_focused()
    try:
        original = editor.input_value()
        assert original == original.strip(), (
            "rename focus check requires an already-trimmed name"
        )
        before = visible(armed_row)
        marker.click()
        expect_visible(armed_row, not before)
        expect(editor).to_be_focused()
        expect(editor).to_have_value(original)
    finally:
        page.keyboard.press("Escape")
    expect(editor).to_have_count(0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="dashboard project URL with at least two runs")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("gesture fences passed")


if __name__ == "__main__":
    main()
