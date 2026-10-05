"""Browser fences for the mkdb2_* -> kymo_* localStorage migration.

State written by a pre-rename bundle must still apply, an ordinary visit must
leave it untouched (rollback keeps working), and the first real edit must
write the kymo_* key and retire the legacy one. Needs a project with at least
two runs and one section (the browser-e2e fixture).
"""

import argparse
import json

from playwright.sync_api import Page, expect, sync_playwright

from fences_common import drag_handle
from user_settings_fences import (
    VIEWPORT,
    close_settings,
    open_settings,
    set_font_size,
    wait_font,
)

PROJECT = "browser-e2e"
MARKER = "legacy layout marker"


def storage(page: Page) -> dict[str, str]:
    return page.evaluate(
        "() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)]))"
    )


def row(page: Page, run_id: str):
    escaped = page.evaluate("id => CSS.escape(id)", run_id)
    return page.locator(f'.sidebar-run[data-run-id="{escaped}"]')


def section_ids(page: Page) -> list[str]:
    return page.eval_on_selector_all(
        ".section[data-section-id]", "els => els.map(e => e.dataset.sectionId)"
    )


def open_project(page: Page, origin: str) -> None:
    page.goto(origin + PROJECT, wait_until="domcontentloaded")
    page.locator(".sidebar-run").nth(1).wait_for(timeout=30_000)
    page.locator(".section[data-section-id]").first.wait_for(timeout=30_000)


def run_fences(page: Page, origin: str) -> None:
    origin = origin.rstrip("/") + "/"
    # A dark OS makes step 1's light theme prove the legacy key was read.
    page.emulate_media(color_scheme="dark")
    # Discover the fixture's run and section ids from a clean profile.
    page.goto(origin, wait_until="domcontentloaded")
    page.evaluate("localStorage.clear()")
    open_project(page, origin)
    runs = page.eval_on_selector_all(
        ".sidebar-run", "els => els.map(e => e.getAttribute('data-run-id'))"
    )
    first, second = runs[0], runs[1]
    sections = section_ids(page)

    # A v2 diff exercises the post-AI-1383 format through the legacy key.
    diff: dict = {
        "format_version": 2 if len(sections) > 1 else 1,
        "section_overrides": [{"key": sections[0], "patch": {"display_name": MARKER}}],
    }
    if len(sections) > 1:
        diff["section_order"] = [{"id": sections[0], "at": {"after": sections[1]}}]
    legacy = {
        "mkdb2_theme": "light",
        "mkdb2_user_config_v1": json.dumps(
            {"font_size": 20, "show_nearest_point": True}
        ),
        "mkdb2_sidebar_w": "333",
        f"mkdb2_selected_runs_v2_{PROJECT}": json.dumps([first]),
        f"mkdb2_color_{first}": "#123456",
        f"mkdb2_layout_diff_{PROJECT}": json.dumps(diff),
    }
    page.evaluate(
        "entries => { localStorage.clear(); for (const [k, v] of Object.entries(entries)) localStorage.setItem(k, v); }",
        legacy,
    )

    # 1. Every legacy value applies.
    open_project(page, origin)
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    wait_font(page, 20)
    expect(page.locator("html")).to_have_attribute(
        "data-kymo-show-nearest-point", "true"
    )
    # The legacy pixel width converts to a percentage of the window width.
    page.wait_for_function(
        "() => Math.abs(document.querySelector('.sidebar').getBoundingClientRect().width - 333) < 1"
    )
    expect(row(page, first).locator(".run-marker-input")).to_be_checked()
    expect(row(page, second).locator(".run-marker-input")).not_to_be_checked()
    swatch = row(page, first).locator('[style*="--run-color"]').first
    assert "#123456" in (swatch.get_attribute("style") or ""), (
        "legacy run color was not applied"
    )
    expect(
        page.locator(f'.section[data-section-id="{sections[0]}"] .section-header')
    ).to_contain_text(MARKER)
    if len(sections) > 1:
        order = section_ids(page)
        assert order.index(sections[0]) > order.index(sections[1]), (
            f"legacy v2 section order was not applied: {order}"
        )

    # 2. An ordinary visit is a pure read: nothing written, nothing retired.
    page.reload(wait_until="domcontentloaded")
    open_project(page, origin)
    after_visit = storage(page)
    assert all(after_visit.get(k) == v for k, v in legacy.items()), (
        f"a read-only visit changed legacy state: {after_visit}"
    )
    # The reconnect schedule is transport state that every connect writes, with no legacy key to migrate.
    written = sorted(
        k for k in after_visit if k.startswith("kymo_") and k != "kymo_ws_reconnect"
    )
    assert not written, f"a read-only visit wrote canonical keys: {written}"

    # 3. The first edit of each value writes kymo_* and retires mkdb2_*.
    def migrated(canonical: str, legacy_key: str) -> dict[str, str]:
        page.wait_for_function(
            "([c, l]) => localStorage.getItem(c) !== null && localStorage.getItem(l) === null",
            arg=[canonical, legacy_key],
        )
        return storage(page)

    # Toggling to dark stores "dark" only while the OS prefers light; the stored light theme holds across the OS change.
    page.emulate_media(color_scheme="light")
    page.get_by_title("Switch to dark mode").click()
    assert migrated("kymo_theme", "mkdb2_theme")["kymo_theme"] == "dark"

    row(page, second).locator(".run-marker-input").click()
    selection = migrated(
        f"kymo_selected_runs_v2_{PROJECT}", f"mkdb2_selected_runs_v2_{PROJECT}"
    )
    assert sorted(json.loads(selection[f"kymo_selected_runs_v2_{PROJECT}"])) == sorted(
        [first, second]
    )

    page.locator(f'.section[data-section-id="{sections[0]}"] .section-header').click(
        position={"x": 60, "y": 8}
    )
    layout = migrated(f"kymo_layout_diff_{PROJECT}", f"mkdb2_layout_diff_{PROJECT}")
    saved = json.loads(layout[f"kymo_layout_diff_{PROJECT}"])
    assert MARKER in json.dumps(saved["section_overrides"]), (
        f"the first layout edit dropped the legacy diff: {saved}"
    )

    handle = page.locator(".sidebar-resize")
    box = handle.bounding_box()
    assert box is not None
    drag_handle(page, handle, box["x"] + 40)
    width = float(migrated("kymo_sidebar_w", "mkdb2_sidebar_w")["kymo_sidebar_w"])
    rendered = page.locator(".sidebar").bounding_box()
    assert rendered is not None
    assert rendered["width"] > 333, "the sidebar drag did not widen the sidebar"
    assert abs(width / 100 * VIEWPORT["width"] - rendered["width"]) < 1, (
        f"stored {width}% does not match the rendered {rendered['width']}px"
    )

    # Settings live on the project list; the first change saved migrates the key.
    page.goto(origin, wait_until="domcontentloaded")
    page.locator(".project-row").first.wait_for(timeout=20_000)
    wait_font(page, 20)
    open_settings(page)
    set_font_size(page, 18)
    close_settings(page)
    config = json.loads(
        migrated("kymo_user_config_v1", "mkdb2_user_config_v1")["kymo_user_config_v1"]
    )
    assert config.get("font_size") == 18 and config.get("show_nearest_point") is True, (
        f"the first settings save dropped legacy fields: {config}"
    )

    # The color was only read, so its legacy key must survive untouched.
    assert storage(page).get(f"mkdb2_color_{first}") == "#123456"


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
            run_fences(page, args.url)
        finally:
            browser.close()
    print("legacy storage fences passed")


if __name__ == "__main__":
    main()
