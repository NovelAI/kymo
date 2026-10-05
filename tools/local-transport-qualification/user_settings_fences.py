"""Browser fences for browser-local Settings, saved as each change is made, and the options panel shell they share."""

import argparse
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


def options_panel(page: Page, name: str) -> Locator:
    """The docked options panel titled `name` (an <aside>, so named complementary content)."""
    return page.get_by_role("complementary", name=name, exact=True)


def open_editor(
    page: Page, trigger: Locator, name: str, *, key: str | None = None
) -> Locator:
    if key is None:
        trigger.click()
    else:
        trigger.press(key)
    panel = options_panel(page, name)
    expect(panel).to_be_visible()
    expect(panel).to_be_focused()
    return panel


def close_panel(page: Page, panel: Locator, trigger: Locator, how: str) -> None:
    """Close with Esc (from focus inside the panel) or the close button; both keep the edits and return focus to the opener."""
    if how == "Escape":
        assert panel.evaluate("element => element.contains(document.activeElement)"), (
            "Esc closes the panel only from focus inside it"
        )
        page.keyboard.press("Escape")
    else:
        assert how == "Close", f"unknown dismissal: {how}"
        panel.get_by_role("button", name="Close", exact=True).click()
    expect(panel).to_have_count(0)
    expect(trigger).to_be_focused()


def expect_stored(page: Page, expected: dict | None) -> None:
    """Wait for the stored settings to equal `expected`: each change commits from its event handler, after the input event Playwright waits for."""
    try:
        page.wait_for_function(
            """([key, expected]) => {
                const raw = localStorage.getItem(key);
                if (expected === null || raw === null) return raw === null && expected === null;
                const value = JSON.parse(raw);
                const keys = Object.keys(value).sort(), want = Object.keys(expected).sort();
                return JSON.stringify(keys) === JSON.stringify(want)
                    && want.every(k => JSON.stringify(value[k]) === JSON.stringify(expected[k]));
            }""",
            arg=[STORAGE_KEY, expected],
            timeout=5_000,
        )
    except TimeoutError as error:
        raise AssertionError(
            f"stored settings {page.evaluate('key => localStorage.getItem(key)', STORAGE_KEY)} != expected {expected}"
        ) from error


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


def settings_trigger(page: Page) -> Locator:
    return page.get_by_role("button", name="Settings", exact=True)


def open_settings(page: Page, *, key: str | None = None) -> Locator:
    return open_editor(page, settings_trigger(page), "Settings", key=key)


def close_settings(page: Page) -> None:
    close_panel(page, options_panel(page, "Settings"), settings_trigger(page), "Close")


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
    page.locator(".project-row").first.wait_for(timeout=20_000)
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
    trigger = settings_trigger(page)

    # The keyboard opens the panel with focus inside it and nothing to revert.
    panel = open_settings(page, key="Enter")
    # The Projects page's panel keeps its fixed width: Settings' font slider would resize a dragged rem width under the pointer.
    expect(panel.locator(".options-panel-resize")).to_be_hidden()
    revert = panel.get_by_role("button", name="Revert", exact=True)
    expect(revert).to_be_disabled()
    # Settings shows pressed while its panel is open, and pressing it again closes the panel.
    expect(trigger).to_have_attribute("aria-expanded", "true")
    trigger.press("Enter")
    expect(panel).to_have_count(0)
    expect(trigger).to_be_focused()
    expect(trigger).to_have_attribute("aria-expanded", "false")
    panel = open_settings(page, key="Enter")
    expect(
        page.get_by_role("checkbox", name="Single-click to exit chart zoom")
    ).to_be_checked()

    # Each change applies and saves as it is made.
    for name in (
        "Show each run’s nearest point when hovering a gap",
        "Highlight all runs with the same name",
    ):
        checkbox = page.get_by_role("checkbox", name=name, exact=True)
        expect(checkbox).not_to_be_checked()
        checkbox.check()
    expect_hover_settings(page, nearest=True, same_name=True)
    set_font_size(page, 18)
    expect(page.locator(".project-row-name").first).to_have_css("font-size", "18px")
    expect_stored(
        page,
        {"font_size": 18, "show_nearest_point": True, "highlight_same_name": True},
    )

    # Revert puts back the values the panel opened with and keeps it open.
    expect(revert).to_be_enabled()
    revert.press("Space")
    wait_font(page, 16)
    expect_hover_settings(page)
    expect_stored(page, None)
    expect(revert).to_be_disabled()
    expect(panel).to_be_visible()

    # Esc and the close button both keep what was changed, and hand focus back.
    set_font_size(page, 18)
    close_panel(page, panel, trigger, "Escape")
    wait_font(page, 18)
    expect_stored(page, {"font_size": 18})
    panel = open_settings(page)
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").uncheck()
    expect_single_click(page, False)
    close_panel(page, panel, trigger, "Close")
    expect_stored(page, {"font_size": 18, "single_click_unzoom": False})

    # Leaving the page with the panel open keeps what was saved: nothing is a preview.
    open_settings(page)
    page.locator("#kymo-show-nearest-point").check()
    page.evaluate("document.querySelector('.trash-nav-link').click()")
    page.locator(".trash-page").wait_for()
    wait_font(page, 18)
    expect_stored(
        page,
        {"font_size": 18, "single_click_unzoom": False, "show_nearest_point": True},
    )

    # A refused write applies nothing and says so; the next accepted change clears the alert.
    page.goto(origin, wait_until="domcontentloaded")
    reset(page)
    page.evaluate(
        """() => {
            window.__kymo_original_set_item = Storage.prototype.setItem;
            Storage.prototype.setItem = function() {
                throw new DOMException('blocked', 'SecurityError');
            };
        }"""
    )
    panel = open_settings(page)
    # click(), not check(): the refused change must not stay checked.
    page.locator("label:has(#kymo-highlight-same-name)").click()
    expect(page.get_by_role("alert")).to_contain_text("Could not save settings")
    expect(page.locator("#kymo-highlight-same-name")).not_to_be_checked()
    slider = page.get_by_role("slider", name="Font size")
    slider.press("ArrowRight")
    expect(slider).to_have_value("16")
    wait_font(page, 16)
    expect_hover_settings(page)
    expect_stored(page, None)
    page.evaluate(
        """() => {
            Storage.prototype.setItem = window.__kymo_original_set_item;
            delete window.__kymo_original_set_item;
        }"""
    )
    page.locator("#kymo-highlight-same-name").check()
    expect(page.get_by_role("alert")).to_have_count(0)
    expect_hover_settings(page, same_name=True)
    expect_stored(page, {"highlight_same_name": True})
    close_panel(page, panel, trigger, "Close")

    # Sparse and unknown stored values survive edits; a default value is not stored.
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
    expect_stored(
        page,
        {
            "font_size": 18,
            "single_click_unzoom": False,
            "show_nearest_point": True,
            "future": True,
        },
    )
    page.get_by_role("checkbox", name="Single-click to exit chart zoom").check()
    expect_stored(page, {"font_size": 18, "show_nearest_point": True, "future": True})
    close_settings(page)
    page.reload(wait_until="domcontentloaded")
    wait_font(page, 18)
    expect_single_click(page, True)
    expect_hover_settings(page, nearest=True)

    if dashboard_path is not None:
        baseline = None
        for pixels in (16, 24):
            page.goto(origin, wait_until="domcontentloaded")
            open_settings(page)
            set_font_size(page, pixels)
            close_settings(page)
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

        # Even too narrow for the charts' 20rem, an options panel docks beside them, never over them, at no less than 14rem and without overflowing.
        defaults = page.get_by_title("Project settings")
        panel = open_editor(page, defaults, "Project settings")
        laid = panel.evaluate(
            """panel => {
                const box = panel.getBoundingClientRect();
                const row = document.querySelector('.content-row').getBoundingClientRect();
                const main = document.querySelector('.main-wrap').getBoundingClientRect();
                const rem = parseFloat(getComputedStyle(document.documentElement).fontSize);
                return {
                    page_overflow: document.documentElement.scrollWidth - innerWidth,
                    body_overflow: panel.querySelector('.options-panel-body').scrollWidth
                        - panel.querySelector('.options-panel-body').clientWidth,
                    under_floor: 14 * rem - box.width,
                    over_charts: main.right - box.left,
                    right: row.right - box.right,
                    top: box.top - row.top,
                };
            }"""
        )
        assert_finite_geometry(laid)
        if laid["page_overflow"] > 0 or laid["body_overflow"] > 0:
            raise AssertionError(f"{pixels}px options panel overflowed: {laid}")
        if (
            laid["under_floor"] > 0.5
            or laid["over_charts"] > 0.5
            or any(abs(laid[key]) > 0.5 for key in ("right", "top"))
        ):
            raise AssertionError(f"narrow options panel is misplaced: {laid}")
        close_panel(page, panel, defaults, "Escape")

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
    parser.add_argument(
        "--dashboard-path",
        help="also check a dashboard at 640px and 24px fonts, as CI does with /browser-e2e",
    )
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1)
        try:
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page, dashboard_path=args.dashboard_path)
        finally:
            browser.close()
    print("user settings fences passed")


if __name__ == "__main__":
    main()
