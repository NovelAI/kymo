"""Browser fences for browser-local Settings preview and persistence."""

import argparse
import json
import math
from urllib.parse import urlsplit

from playwright.sync_api import (
    Locator,
    Page,
    Route,
    TimeoutError,
    expect,
    sync_playwright,
)


STORAGE_KEY = "kymo_user_config_v1"
VIEWPORT = {"width": 1_440, "height": 900}


def native_dialog(page: Page, name: str) -> Locator:
    dialog = page.get_by_role("dialog", name=name, exact=True)
    expect(dialog).to_be_visible()
    assert dialog.evaluate("element => element.matches('dialog:modal')"), (
        f"{name} did not enter the browser's modal top layer"
    )
    return dialog


def open_editor(
    page: Page, trigger: Locator, name: str, *, key: str | None = None
) -> Locator:
    if key is None:
        trigger.click()
    else:
        trigger.press(key)
    dialog = native_dialog(page, name)
    expect(dialog.locator(".modal")).to_be_focused()
    return dialog


def backdrop_cancel(page: Page, dialog: Locator) -> None:
    # Check the hit target so a layout change cannot turn this into a Cancel-button click.
    assert dialog.evaluate("element => document.elementFromPoint(4, 4) === element"), (
        "the viewport corner is not the native editor backdrop"
    )
    page.mouse.click(4, 4)


def assert_native_focus(page: Page, dialog: Locator, trigger: Locator) -> None:
    controls = dialog.locator(
        "input:enabled, select:enabled, button:enabled, textarea:enabled, a[href]"
    ).filter(visible=True)
    first = controls.first
    first.focus()
    # Inertness must reject explicit background focus as well as keyboard navigation.
    trigger.evaluate("element => element.focus()")
    expect(first).to_be_focused()
    count = controls.count()
    for key, start in (("Tab", first), ("Shift+Tab", controls.last)):
        # Walk through both boundaries; browser chrome may expose body at the wrap point.
        start.focus()
        moved = False
        for _ in range(count + 3):
            page.keyboard.press(key)
            focus = start.evaluate(
                """start => {
                    const active = document.activeElement;
                    return {
                        inside: start.closest('dialog').contains(active),
                        chrome: active === document.body,
                        moved: active !== start && active.matches('input,select,button,textarea,a[href]'),
                    };
                }"""
            )
            assert focus["inside"] or focus["chrome"], (
                f"{key} focused interactive background content: {focus}"
            )
            moved |= focus["inside"] and focus["moved"]
        # Browser/OS preferences choose the control types visited; still require actual movement.
        assert count < 2 or moved, f"{key} did not move between editor controls"


def stored_config(page: Page) -> dict | None:
    raw = page.evaluate("key => localStorage.getItem(key)", STORAGE_KEY)
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise AssertionError(f"stored settings are not an object: {value!r}")
    return value


def expect_single_click(page: Page, enabled: bool) -> None:
    expect(page.locator("html")).to_have_attribute(
        "data-kymo-single-click-unzoom", str(enabled).lower()
    )


def expect_hover_settings(
    page: Page, *, nearest: bool = False, same_name: bool = False
) -> None:
    for name, enabled in (
        ("show-nearest-point", nearest),
        ("highlight-same-name", same_name),
    ):
        expect(page.locator("html")).to_have_attribute(
            f"data-kymo-{name}", str(enabled).lower()
        )


def wait_font(page: Page, pixels: int) -> None:
    expected = f"{pixels}px"
    # At 16px, the browser default alone does not prove the app stylesheet loaded.
    page.wait_for_function(
        "value => getComputedStyle(document.documentElement).fontSize === value "
        "&& getComputedStyle(document.body).fontSize === value "
        "&& getComputedStyle(document.documentElement).getPropertyValue('--font-size-15').trim() !== ''",
        arg=expected,
    )


def assert_finite_geometry(geometry: dict[str, float]) -> None:
    if not all(math.isfinite(value) for value in geometry.values()):
        raise AssertionError(f"non-finite geometry: {geometry}")


def load_with_held_stylesheet(page: Page, url: str) -> None:
    """Hold kymo.css and assert the parser, and so the app's loader in <body>, waits for it (main.rs THEME_BOOT)."""
    held_stylesheets: list[Route] = []

    def hold_stylesheet(route: Route) -> None:
        held_stylesheets.append(route)

    with page.route("**/kymo*.css", hold_stylesheet):
        try:
            with page.expect_request("**/kymo*.css"):
                page.goto(url, wait_until="commit")
            # Hold long enough that a parser not blocked on the stylesheet would reach <body>.
            page.wait_for_timeout(250)
            if page.evaluate("document.body !== null"):
                raise AssertionError(
                    "the page parsed past <head> before kymo.css loaded"
                )
        finally:
            # Release on this stack so wait failures cannot poison Playwright's route dispatcher.
            for route in held_stylesheets:
                route.fallback()


def open_settings(page: Page, *, key: str | None = None) -> Locator:
    trigger = page.get_by_role("button", name="Settings", exact=True)
    return open_editor(page, trigger, "Settings", key=key)


def assert_settings_focus(page: Page, *, save_enabled: bool = False) -> None:
    dialog = native_dialog(page, "Settings")
    save = page.get_by_role("button", name="Save", exact=True)
    if save_enabled:
        expect(save).to_be_enabled()
    else:
        expect(save).to_be_disabled()
    assert_native_focus(page, dialog, page.locator(".page-user-settings-trigger"))


def set_font_size(page: Page, pixels: int) -> None:
    slider = page.get_by_role("slider", name="Font size")
    minimum = int(slider.get_attribute("min"))
    slider.press("Home")
    wait_font(page, minimum)
    for value in range(minimum + 1, pixels + 1):
        slider.press("ArrowRight")
        wait_font(page, value)
    expect(slider).to_have_value(str(pixels))


def reset(page: Page) -> None:
    page.evaluate(f"localStorage.removeItem({STORAGE_KEY!r})")
    page.reload(wait_until="domcontentloaded")
    page.locator(".project-card").first.wait_for(timeout=20_000)
    wait_font(page, 16)
    expect_single_click(page, True)
    expect_hover_settings(page)


def assert_large_log_geometry(page: Page) -> None:
    page.locator('.sidebar-run[data-run-id="browser-e2e"] .run-details-link').click()
    logs = page.locator(".section:has(.section-name:text-is('logs'))")
    logs.scroll_into_view_if_needed()
    if logs.locator(".section-grid").count() == 0:
        logs.locator(".section-name").click()
    body = logs.locator('[data-slot-id="logs/std_out"] .text-stream-log')
    expect(body).to_have_count(1)
    expect(body.locator(".text-stream-line").first).to_have_text(
        "settings log line 0000"
    )
    expect(body.locator(".text-stream-line").nth(1)).to_be_visible()
    geometry = body.evaluate(
        """body => {
            const rows = body.querySelectorAll('.text-stream-line');
            return {
                height: rows[0].getBoundingClientRect().height,
                stride: rows[1].getBoundingClientRect().top
                    - rows[0].getBoundingClientRect().top,
            };
        }"""
    )
    assert_finite_geometry(geometry)
    if geometry != {"height": 26, "stride": 26}:
        raise AssertionError(f"24px log text lost its 26px row stride: {geometry}")

    body.evaluate("body => { body.scrollTop = 417 * 26; }")
    expect(
        body.locator(".text-stream-line").filter(has_text="settings log line 0417")
    ).to_have_count(1)
    page.wait_for_function(
        """body => {
            const row = [...body.querySelectorAll('.text-stream-line')]
                .find(row => row.textContent === 'settings log line 0417');
            if (!row) return false;
            const top = row.getBoundingClientRect().top - body.getBoundingClientRect().top;
            return Math.abs(top - parseFloat(getComputedStyle(body).paddingTop)) < 0.5;
        }""",
        arg=body.element_handle(),
    )
    spacer = body.locator(".text-stream-spacer").first.evaluate(
        "element => element.getBoundingClientRect().height"
    )
    if not math.isfinite(spacer) or spacer <= 0:
        raise AssertionError("deep log scroll did not move the virtual window")


def assert_compact_trash(page: Page) -> None:
    page.locator(".trash-run").first.wait_for(timeout=20_000)
    table = page.get_by_role("table")
    for width in (640, 320):
        page.set_viewport_size({"width": width, "height": 900})
        expect(table).to_have_count(1)
        expect(table.get_by_role("columnheader")).to_have_count(3)
        rows = page.locator(".trash-run").count()
        expect(table.get_by_role("row")).to_have_count(rows + 1)
        expect(table.get_by_role("cell")).to_have_count(rows * 3)
        overflow = table.evaluate(
            """table => Math.max(
                document.documentElement.scrollWidth - innerWidth,
                table.parentElement.scrollWidth - table.parentElement.clientWidth
            )"""
        )
        if not math.isfinite(overflow) or overflow > 1:
            raise AssertionError(f"{width}px Trash page overflowed by {overflow}px")


def assert_maximized_chart_fits(page: Page) -> None:
    geometry = """() => {
        const chart = Object.values(window.__kymo_charts || {})
            .find(chart => chart.root.closest('.maximize-content'));
        if (!chart) return null;
        const rect = chart.root.closest('.metric-rect');
        const style = getComputedStyle(rect);
        const end = rect.parentElement.getBoundingClientRect().bottom
            - parseFloat(style.paddingBottom);
        return {
            gap: chart.root.getBoundingClientRect().bottom - end,
            connected: chart.root.isConnected,
            viewport: innerHeight,
            chart_height: chart.height,
            header_height: rect.querySelector('.rect-header').getBoundingClientRect().height,
            content_height: rect.parentElement.getBoundingClientRect().height,
            main_height: document.querySelector('.main-wrap').clientHeight,
            line_height: style.lineHeight,
        };
    }"""
    try:
        page.wait_for_function(
            f"() => {{ const g = ({geometry})(); return g && Math.abs(g.gap) < 2; }}",
            timeout=20_000,
        )
    except TimeoutError as error:
        raise AssertionError(
            f"maximized chart did not fit: {page.evaluate(geometry)}"
        ) from error


def run_fences(page: Page, *, dashboard_path: str | None = None) -> None:
    parsed = urlsplit(page.url)
    origin = f"{parsed.scheme}://{parsed.netloc}/"
    reset(page)

    open_settings(page, key="Enter")
    assert_settings_focus(page)
    expect(
        page.get_by_role("checkbox", name="Single-click to exit chart zoom")
    ).to_be_checked()
    for name in (
        "Show each run’s nearest point when hovering a gap",
        "Highlight all runs with the same name",
    ):
        checkbox = page.get_by_role("checkbox", name=name, exact=True)
        expect(checkbox).not_to_be_checked()
        checkbox.check()
    expect_hover_settings(page, nearest=True, same_name=True)
    set_font_size(page, 18)
    if stored_config(page) is not None:
        raise AssertionError("font preview wrote localStorage before Save")
    expect(page.locator(".project-card-name").first).to_have_css("font-size", "18px")
    assert_settings_focus(page, save_enabled=True)
    page.get_by_role("button", name="Cancel").press("Space")
    wait_font(page, 16)
    expect_hover_settings(page)
    page.wait_for_function(
        "() => document.activeElement?.classList.contains('page-user-settings-trigger')"
    )

    open_settings(page)
    set_font_size(page, 18)
    page.keyboard.press("Escape")
    wait_font(page, 16)

    open_settings(page)
    set_font_size(page, 18)
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").uncheck()
    page.locator("#kymo-show-nearest-point").check()
    page.locator("#kymo-highlight-same-name").check()
    expect_single_click(page, False)
    expect_hover_settings(page, nearest=True, same_name=True)
    backdrop_cancel(page, native_dialog(page, "Settings"))
    wait_font(page, 16)
    expect_single_click(page, True)
    expect_hover_settings(page)

    # A route transition can unmount the dialog without dispatching any of
    # its explicit dismissal events. The drop guard must still roll back.
    open_settings(page)
    set_font_size(page, 18)
    page.evaluate("document.querySelector('.trash-nav-link').click()")
    page.locator(".trash-page").wait_for()
    wait_font(page, 16)
    if stored_config(page) is not None:
        raise AssertionError("route-unmounted preview persisted unexpectedly")

    page.goto(origin, wait_until="domcontentloaded")
    page.locator(".project-card").first.wait_for(timeout=20_000)

    # A denied localStorage write keeps the draft open for a retry rather
    # than reporting a session-only preview as saved.
    page.evaluate(
        """() => {
            window.__kymo_original_set_item = Storage.prototype.setItem;
            Storage.prototype.setItem = function() {
                throw new DOMException('blocked', 'SecurityError');
            };
        }"""
    )
    open_settings(page)
    set_font_size(page, 18)
    page.locator("#kymo-show-nearest-point").check()
    page.locator("#kymo-highlight-same-name").check()
    page.get_by_role("button", name="Save").press("Enter")
    wait_font(page, 18)
    expect_hover_settings(page, nearest=True, same_name=True)
    expect(page.get_by_role("dialog", name="Settings")).to_be_visible()
    expect(page.get_by_role("alert")).to_contain_text("Could not save settings")
    if stored_config(page) is not None:
        raise AssertionError("blocked settings write unexpectedly reached localStorage")
    page.evaluate(
        """() => {
            Storage.prototype.setItem = window.__kymo_original_set_item;
            delete window.__kymo_original_set_item;
        }"""
    )
    page.get_by_role("button", name="Save").click()
    page.get_by_role("dialog", name="Settings").wait_for(state="detached")
    open_settings(page)
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").uncheck()
    page.get_by_role("button", name="Save").click()
    page.reload(wait_until="domcontentloaded")
    page.locator(".project-card").first.wait_for(timeout=20_000)
    wait_font(page, 18)
    expect_single_click(page, False)
    expect_hover_settings(page, nearest=True, same_name=True)
    recovered = stored_config(page)
    if recovered != {
        "font_size": 18,
        "single_click_unzoom": False,
        "show_nearest_point": True,
        "highlight_same_name": True,
    }:
        raise AssertionError(f"settings retry did not recover all values: {recovered}")

    open_settings(page)
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").check()
    page.locator("#kymo-show-nearest-point").uncheck()
    page.locator("#kymo-highlight-same-name").uncheck()
    page.get_by_role("button", name="Save").click()
    page.get_by_role("dialog", name="Settings").wait_for(state="detached")
    if stored_config(page) != {
        "font_size": 18,
    }:
        raise AssertionError("restoring the defaults retained a chart override")
    expect_hover_settings(page)

    page.evaluate(
        f'localStorage.setItem({STORAGE_KEY!r}, \'{{"font_size":"large","future":true}}\')'
    )
    page.reload(wait_until="domcontentloaded")
    wait_font(page, 17)
    expect_hover_settings(page)
    open_settings(page)
    set_font_size(page, 18)
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").uncheck()
    page.locator("#kymo-show-nearest-point").check()
    expect_hover_settings(page, nearest=True)
    if stored_config(page) != {
        "font_size": "large",
        "future": True,
    }:
        raise AssertionError("checkbox preview wrote storage before Save")
    page.get_by_role("button", name="Save").click()
    wait_font(page, 18)
    stored = stored_config(page)
    if stored != {
        "font_size": 18,
        "single_click_unzoom": False,
        "show_nearest_point": True,
        "future": True,
    }:
        raise AssertionError(f"Save lost sparse/unknown settings: {stored}")
    page.reload(wait_until="domcontentloaded")
    wait_font(page, 18)
    expect_single_click(page, False)
    expect_hover_settings(page, nearest=True)

    if dashboard_path is not None:
        baseline = None
        for pixels in (16, 24):
            page.goto(origin, wait_until="domcontentloaded")
            open_settings(page)
            set_font_size(page, pixels)
            page.get_by_role("button", name="Save").click()
            page.set_viewport_size({"width": 640, "height": 900})
            dashboard_url = origin + dashboard_path.lstrip("/")
            if pixels == 24:
                load_with_held_stylesheet(page, dashboard_url)
            else:
                page.goto(dashboard_url, wait_until="domcontentloaded")
            page.locator(".sidebar-run").first.wait_for(timeout=20_000)
            wait_font(page, pixels)
            page.wait_for_function(
                "font => Object.values(window.__kymo_charts || {}).some(chart => chart.axes[0].font[0] === font)",
                arg=f"{(12 * pixels + 7) // 15}px sans-serif",
                timeout=20_000,
            )
            geometry = page.evaluate(
                """() => {
                    const navbar = document.querySelector('.navbar');
                    const actions = document.querySelector('.sidebar-actions');
                    const charts = Object.values(window.__kymo_charts);
                    const rect = charts[0].root.closest('.metric-rect');
                    return {
                        page_overflow: document.documentElement.scrollWidth - innerWidth,
                        navbar_overflow: navbar.scrollWidth - navbar.clientWidth,
                        actions_overflow: actions.scrollWidth - actions.clientWidth,
                        navbar_height: navbar.getBoundingClientRect().height,
                        actions_height: actions.getBoundingClientRect().height,
                        min_plot_width: Math.min(...charts.map(chart => chart.bbox.width / devicePixelRatio)),
                        rect_height: rect.getBoundingClientRect().height,
                        rect_min_height: parseFloat(getComputedStyle(rect).minHeight),
                    };
                }"""
            )
            assert_finite_geometry(geometry)
            if any(
                geometry[key] > 0
                for key in ("page_overflow", "navbar_overflow", "actions_overflow")
            ):
                raise AssertionError(f"{pixels}px controls overflowed: {geometry}")
            if geometry["min_plot_width"] < 39.5:
                raise AssertionError(f"{pixels}px font collapsed a plot: {geometry}")
            if abs(geometry["rect_height"] - geometry["rect_min_height"]) > 0.5:
                raise AssertionError(
                    f"mounted card and placeholder height diverged: {geometry}"
                )
            if baseline is not None:
                for key in ("navbar_height", "actions_height"):
                    if geometry[key] <= baseline[key]:
                        raise AssertionError(
                            f"{key} did not grow with font size: {baseline}, {geometry}"
                        )
            baseline = geometry

        page.locator('.metric-rect button[title="Maximize"]').first.click()
        for height in (900, 700):
            page.set_viewport_size({"width": 640, "height": height})
            assert_maximized_chart_fits(page)
        page.keyboard.press("Escape")
        page.locator(".maximize-overlay").wait_for(state="detached")
        page.set_viewport_size({"width": 640, "height": 900})
        assert_large_log_geometry(page)
        page.goto(origin + "trash", wait_until="domcontentloaded")
        wait_font(page, 24)
        assert_compact_trash(page)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="dashboard origin")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1)
        try:
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("user settings fences passed")


if __name__ == "__main__":
    main()
