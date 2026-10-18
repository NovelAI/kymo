"""Browser checks for section/panel creation against an intercepted RPC fixture.

Run against a local dx serve; no requests are forwarded to a backend:
    python creation_fences.py http://127.0.0.1:8094 --browser chromium
"""

import argparse
import json
import re
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright

from fences_common import encode_response, render_turn, request_frame
from text_scroll_fences import Fixture, PRIMARY, PROJECT, RUNS, pb
from user_settings_fences import options_panel, panel_body


STORAGE_KEY = f"kymo_layout_diff_{PROJECT}"
VIEWPORT = {"width": 1440, "height": 900}


class CreationFixture(Fixture):
    def __init__(self):
        super().__init__()
        # While set, the metrics sweep's replies wait for `release_metrics`.
        self.hold_metrics = False
        self.held = []

    def request(self, socket, message):
        identifier, path, _ = request_frame(message, urlsplit(socket.url).path)
        method = path.rsplit("/", 1)[-1]
        if self.hold_metrics and method in ("ListMetrics", "ListRunSetMetrics"):
            self.held.append((socket, message))
        # A new panel starts numeric while its chosen metric's type is discovered.
        elif path == "/kymo.Kymo/QueryChart":
            socket.send(
                encode_response(identifier, pb.ChartResponse().SerializeToString())
            )
        else:
            super().request(socket, message)

    def release_metrics(self):
        self.hold_metrics = False
        held, self.held = self.held, []
        for socket, message in held:
            super().request(socket, message)


def stored_section(page, section_id):
    """The settings stored for section `section_id`, its override patches merged."""
    stored = json.loads(page.evaluate("key => localStorage.getItem(key)", STORAGE_KEY))
    patch = {}
    for override in stored.get("section_overrides", []):
        if override["key"] == section_id:
            patch.update(override["patch"])
    return patch


def section_of(control):
    return control.locator(
        "xpath=ancestor::div[contains(concat(' ', @class, ' '), ' section ')][1]"
    )


def named_section(page, name):
    """The section whose name field `name` is open, by a locator that outlives the field."""
    sid = section_of(name).get_attribute("data-section-id")
    return page.locator(f".section[data-section-id={json.dumps(sid)}]")


def pointer_open(page, trigger, focused):
    trigger.hover()
    page.mouse.down()
    expect(focused).to_be_focused()
    page.mouse.up()
    expect(focused).to_be_focused()


def close_metric(page, panel, trigger):
    panel.get_by_role("button", name="Close", exact=True).click()
    expect(panel).to_have_count(0)
    expect(page.locator(".maximize-overlay")).to_have_count(0)
    expect(page).not_to_have_url(re.compile(r"[?&]chart="))
    expect(trigger).to_be_focused()


def cross_layout(page):
    """Reload with the logs section at a new section's priority, named "O logs": a new section ("New Section") lands above it at the top, and a rename can cross the tall section in either direction."""
    page.evaluate(
        "([key, value]) => localStorage.setItem(key, JSON.stringify(value))",
        [
            STORAGE_KEY,
            {
                "format_version": 1,
                "section_overrides": [
                    {
                        "key": "logs",
                        "patch": {
                            "display_name": "O logs",
                            "priority": 0,
                            "collapsed": False,
                            "rows_per_page": 0,
                            "max_columns": 2,
                            "chart_height": 280,
                        },
                    }
                ],
            },
        ],
    )
    page.reload(wait_until="domcontentloaded")
    expect(page.locator(".text-stream-line").first).to_be_visible(timeout=20_000)


def rename_reveal(page):
    cross_layout(page)
    add = page.get_by_title("Add a section", exact=True)
    name = page.get_by_role("textbox", name="Section name", exact=True)
    main = page.locator("main.main-content")

    filter_input = page.get_by_placeholder("Filter panels", exact=True)
    scroll_top = "e => e.scrollTop"
    # A last section, so the renamed ones below land above it and a reveal's scroll is not clamped at the end.
    add.click()
    name.fill("Zzz tail")
    name.press("Enter")
    expect(name).to_have_count(0)

    print(
        "rename: Enter and a press in neutral space follow the section across the dashboard",
        flush=True,
    )
    for finish in ("Enter", "neutral"):
        add.click()
        section = named_section(page, name)
        before = main.evaluate(scroll_top)
        name.fill(f"Z renamed on {finish}")
        if finish == "Enter":
            name.press("Enter")
        else:
            # The navbar's empty middle: nothing focusable there, so the blur hands focus to the page.
            spacer = page.locator(".navbar-spacer").bounding_box()
            bar = page.locator(".navbar").bounding_box()
            page.mouse.click(
                spacer["x"] + spacer["width"] / 2, bar["y"] + bar["height"] / 2
            )
        expect(section.locator(".section-name")).to_have_text(f"Z renamed on {finish}")
        expect(section.locator(".section-header")).to_be_in_viewport(ratio=0.99)
        assert main.evaluate(scroll_top) > before + 2000
        if finish == "Enter":
            # The whole short section comes into view, so the Add panel Enter focused is on screen, by the least distance: its bottom at the view's.
            empty_add = section.get_by_role("button", name="Add panel", exact=True)
            expect(empty_add).to_be_focused()
            expect(empty_add).to_be_in_viewport(ratio=1)
            gap = section.evaluate(
                "s => document.querySelector('main.main-content').getBoundingClientRect().bottom - s.getBoundingClientRect().bottom"
            )
            assert abs(gap) <= 2, gap

    print(
        "rename: leaving the field for another control keeps the page and that control's focus",
        flush=True,
    )
    add.click()
    section = named_section(page, name)
    render_turn(page)
    before = main.evaluate(scroll_top)
    name.fill("Z renamed on blur")
    filter_input.click()
    expect(section.locator(".section-name")).to_have_text("Z renamed on blur")
    render_turn(page)
    assert main.evaluate(scroll_top) == before
    expect(filter_input).to_be_focused()

    print("rename: Configure on the section being named follows it", flush=True)
    add.click()
    configured = named_section(page, name)
    before = main.evaluate(scroll_top)
    name.fill("Z configured while named")
    configured.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    expect(configured.locator(".section-header")).to_be_in_viewport(ratio=0.99)
    assert main.evaluate(scroll_top) > before + 2000
    settings.get_by_role("button", name="Close", exact=True).click()

    print(
        "rename: Configure on another section while naming leaves the page still",
        flush=True,
    )
    add.click()
    other = named_section(page, name)
    # A name that keeps this section in place, above the tall logs section.
    name.fill("B named while another opens")
    # The other section is short and cut by the view's bottom edge: revealing it would scroll.
    tail = page.locator(".section", has=page.get_by_text("Zzz tail", exact=True))
    main.evaluate(
        "(main, h) => main.scrollTop += h.getBoundingClientRect().bottom - main.getBoundingClientRect().bottom",
        tail.locator(".section-header").element_handle(),
    )
    render_turn(page)
    before = main.evaluate(scroll_top)
    tail.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    expect(other.locator(".section-name")).to_have_text("B named while another opens")
    render_turn(page)
    assert abs(main.evaluate(scroll_top) - before) <= 2
    settings.get_by_role("button", name="Close", exact=True).click()
    main.evaluate("e => e.scrollTop = 0")

    print(
        "rename: the settings Name moves the page once the edit settles, not while typing",
        flush=True,
    )
    section.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    editor_name = settings.get_by_role("textbox", name="Name", exact=True)
    # The panel body's mount focus repeats a task later; let it land before taking the field.
    render_turn(page)
    editor_name.click()
    editor_name.press("ControlOrMeta+a")
    before = main.evaluate(scroll_top)
    for letter in "A top":
        editor_name.press_sequentially(letter)
        render_turn(page)
        # WebKit rounds the scroll by a pixel as the section leaves the view.
        assert abs(main.evaluate(scroll_top) - before) <= 2, letter
    expect(section.locator(".section-name")).to_have_text("A top")
    expect(editor_name).to_be_focused()
    # Moving on to the next field settles the name.
    page.keyboard.press("Tab")
    expect(section.locator(".section-header")).to_be_in_viewport(ratio=0.99)
    assert main.evaluate(scroll_top) < 100
    settings.get_by_role("button", name="Revert", exact=True).click()
    expect(section.locator(".section-name")).to_have_text("Z renamed on blur")
    expect(section.locator(".section-header")).to_be_in_viewport(ratio=0.99)
    expect(panel_body(settings)).to_be_focused()

    # An unrelated settings edit must not pull the viewport back to this section, nor must its Close.
    main.evaluate("e => e.scrollTop = 0")
    settings.get_by_role("spinbutton", name="Columns", exact=True).fill("3")
    render_turn(page)
    assert main.evaluate(scroll_top) == 0
    settings.get_by_role("button", name="Close", exact=True).click()
    expect(settings).to_have_count(0)
    render_turn(page)
    assert main.evaluate(scroll_top) == 0

    print("rename: Revert follows a rename still being typed back", flush=True)
    section.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    editor_name = settings.get_by_role("textbox", name="Name", exact=True)
    render_turn(page)
    editor_name.click()
    editor_name.press("ControlOrMeta+a")
    editor_name.press_sequentially("A typed")
    expect(section.locator(".section-name")).to_have_text("A typed")
    # Wheeled away from both places, with Name still focused.
    main.evaluate("e => e.scrollTop = e.scrollHeight / 2")
    render_turn(page)
    expect(editor_name).to_be_focused()
    settings.get_by_role("button", name="Revert", exact=True).click()
    expect(section.locator(".section-name")).to_have_text("Z renamed on blur")
    expect(section.locator(".section-header")).to_be_in_viewport(ratio=0.99)

    # A rename left for a control outside the panel waits for Close.
    main.evaluate("e => e.scrollTop = 0")
    editor_name.fill("Zz settled on close")
    filter_input.click()
    render_turn(page)
    assert main.evaluate(scroll_top) == 0
    expect(filter_input).to_be_focused()
    settings.get_by_role("button", name="Close", exact=True).click()
    expect(section.locator(".section-header")).to_be_in_viewport(ratio=0.99)
    assert main.evaluate(scroll_top) > 2000

    print(
        "rename: a subsequent new section keeps its reveal and input focus", flush=True
    )
    add.click()
    name.fill("ZZZ previous section")
    add.click()
    render_turn(page)
    expect(name).to_be_focused()
    expect(name).to_be_in_viewport(ratio=0.99)
    name.press("Escape")

    print("rename: Add panel on the section being named follows it", flush=True)
    add.click()
    added = named_section(page, name)
    name.fill("Z panel added while named")
    added.get_by_role("button", name="Add panel", exact=True).click()
    chart_settings = options_panel(page, "Configure Metric")
    picker = chart_settings.get_by_role("combobox", name="Source 1 metric", exact=True)
    expect(picker).to_be_focused()
    picker.select_option(PRIMARY)
    close_metric(page, chart_settings, added.get_by_title("Add metric", exact=True))
    expect(added.locator(".section-header")).to_be_in_viewport(ratio=0.99)

    print("rename: renaming a tall section in place leaves the page still", flush=True)
    main.evaluate("e => e.scrollTop = 0")
    logs = page.locator('.section[data-section-id="logs"]')
    logs.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    editor_name = settings.get_by_role("textbox", name="Name", exact=True)
    render_turn(page)
    editor_name.click()
    editor_name.press("End")
    editor_name.press_sequentially("!")
    expect(logs.locator(".section-name")).to_have_text("O logs!")
    page.keyboard.press("Tab")
    render_turn(page)
    assert main.evaluate(scroll_top) <= 2, main.evaluate(scroll_top)
    settings.get_by_role("button", name="Close", exact=True).click()


def checks(page, url):
    fixture = CreationFixture()
    page.route_web_socket(
        re.compile(r"/(?:grpc-ws|trash/_kymo-grpc-ws-local-v1)"), fixture.connect
    )
    page.route(
        "**/alerts",
        lambda route: route.fulfill(content_type="application/json", body="[]"),
    )
    page.route(
        "https://fonts.googleapis.com/**",
        lambda route: route.fulfill(content_type="text/css", body=""),
    )
    storage = {
        f"kymo_selected_runs_v2_{PROJECT}": json.dumps(RUNS),
        STORAGE_KEY: json.dumps(
            {
                "format_version": 1,
                "section_overrides": [
                    {
                        "key": "logs",
                        "patch": {
                            "collapsed": False,
                            "priority": 100,
                            "max_columns": 2,
                            "rows_per_page": 0,
                            "chart_height": 280,
                        },
                    }
                ],
            }
        ),
    }
    page.add_init_script(f"""if (!sessionStorage.creationSeeded) {{
        for (const [key, value] of Object.entries({json.dumps(storage)})) localStorage.setItem(key, value);
        sessionStorage.creationSeeded = 'yes';
    }}""")
    add_section = page.get_by_title("Add a section", exact=True)
    name = page.get_by_role("textbox", name="Section name", exact=True)
    panel = options_panel(page, "Configure Metric")
    picker = panel.get_by_role("combobox", name="Source 1 metric", exact=True)
    filter_input = page.get_by_placeholder("Filter panels", exact=True)
    main = page.locator("main.main-content")
    # WebKit's Tab skips buttons, as Safari's does.
    tab_reaches_buttons = page.context.browser.browser_type.name != "webkit"

    print("new section: unavailable until the layout loads", flush=True)
    fixture.hold_metrics = True
    page.goto(f"{url.rstrip('/')}/{PROJECT}/", wait_until="domcontentloaded")
    expect(page.get_by_text("Loading metrics...")).to_be_visible(timeout=20_000)
    expect(add_section).to_be_disabled()
    runs_filter = page.get_by_placeholder("Filter runs...", exact=True)
    runs_filter.fill("scroll")
    fixture.release_metrics()
    expect(page.locator(".text-stream-line").first).to_be_visible(timeout=20_000)
    expect(add_section).to_be_enabled()
    render_turn(page)
    expect(runs_filter).to_be_focused()
    expect(name).to_have_count(0)
    runs_filter.fill("")

    print("new section: pointer focus, reveal, Enter, and empty action", flush=True)
    pointer_open(page, add_section, name)
    expect(name).to_be_in_viewport()
    section_id = section_of(name).get_attribute("data-section-id")
    section = named_section(page, name)
    header = section.locator(".section-header")
    naming_height = header.evaluate("e => e.getBoundingClientRect().height")
    assert name.evaluate("e => getComputedStyle(e).outlineStyle") == "none"
    name.fill("  My training panels  ")
    name.press("Enter")
    expect(name).to_have_count(0)
    expect(section.locator(".section-name")).to_have_text("My training panels")
    assert stored_section(page, section_id)["display_name"] == "My training panels"
    # The field takes the title's place without growing the header.
    height = header.evaluate("e => e.getBoundingClientRect().height")
    assert abs(height - naming_height) < 0.01, (naming_height, height)
    empty_add = section.get_by_role("button", name="Add panel", exact=True)
    expect(empty_add).to_be_in_viewport()
    page.mouse.move(0, 0)
    expect(empty_add).to_be_visible()
    assert empty_add.evaluate("e => getComputedStyle(e).opacity") == "1"

    print(
        "empty section: opens and focuses metric selection; choice survives close/reload",
        flush=True,
    )
    pointer_open(page, empty_add, picker)
    expect(picker.locator(f"option[value={json.dumps(PRIMARY)}]")).to_have_count(1)
    picker.select_option(PRIMARY)
    expect(panel.get_by_role("button", name="Revert", exact=True)).to_be_enabled()
    close_metric(page, panel, section.get_by_title("Add metric", exact=True))
    expect(empty_add).to_have_count(0)
    expect(section.locator(".metric-rect")).to_have_count(1)
    page.reload(wait_until="domcontentloaded")
    expect(section.locator(".section-name")).to_have_text("My training panels")
    section.scroll_into_view_if_needed()
    expect(section.locator(".rect-title")).to_have_text("00-primary")
    expect(name).to_have_count(0)

    print("successive additions keep the previous inline name", flush=True)
    add_section.click()
    name.fill("First of two")
    first = named_section(page, name)
    add_section.click()
    expect(name).to_be_focused()
    expect(first.locator(".section-name")).to_have_text("First of two")
    name.press("Escape")

    print(
        "naming: pressing the field or the header bar keeps the section open",
        flush=True,
    )
    add_section.click()
    new = named_section(page, name)
    name.click()
    expect(name).to_be_focused()
    expect(new.locator(".section-grid")).to_have_count(1)
    name.fill("Bar pressed")
    bar = new.locator(".section-header").bounding_box()
    page.mouse.click(bar["x"] + bar["width"] - 30, bar["y"] + bar["height"] / 2)
    expect(name).to_have_count(0)
    expect(new.locator(".section-name")).to_have_text("Bar pressed")
    expect(new.locator(".section-toggle")).to_have_attribute("aria-expanded", "true")
    expect(new.locator(".section-grid")).to_have_count(1)

    print("naming: deleting the section drops its unsaved name", flush=True)
    stale = []
    page.on(
        "console",
        lambda message: (
            "no longer in layout" in message.text and stale.append(message.text)
        ),
    )
    add_section.click()
    new = named_section(page, name)
    name.fill("Deleted while named")
    page.once("dialog", lambda dialog: dialog.accept())
    new.get_by_title("Delete section", exact=True).click()
    expect(new).to_have_count(0)
    add_section.click()
    expect(name).to_be_focused()
    name.press("Escape")
    assert not stale, stale

    print("section settings opens with the completed inline name", flush=True)
    add_section.click()
    name.fill("Name before settings")
    section_of(name).get_by_title("Configure", exact=True).click()
    settings = options_panel(page, "Configure section")
    expect(settings.get_by_role("textbox", name="Name", exact=True)).to_have_value(
        "Name before settings"
    )
    settings.get_by_role("button", name="Close", exact=True).click()

    print("adding a panel finishes the inline section name", flush=True)
    add_section.click()
    name.fill("Name before panel")
    new = named_section(page, name)
    new.get_by_role("button", name="Add panel", exact=True).click()
    expect(picker).to_be_focused()
    picker.select_option(PRIMARY)
    close_metric(page, panel, new.get_by_title("Add metric", exact=True))
    expect(new.locator(".section-name")).to_have_text("Name before panel")

    print("new section: keyboard, Escape, whitespace, blur, and IME", flush=True)
    for value, how, label in (
        ("discard this", "Escape", "New Section"),
        ("   ", "Enter", "New Section"),
        ("Blur saves", "blur", "Blur saves"),
    ):
        add_section.press("Enter")
        expect(name).to_be_focused()
        new = named_section(page, name)
        name.fill(value)
        name.dispatch_event(
            "keydown", {"key": "Enter", "code": "Enter", "isComposing": True}
        )
        expect(name).to_be_visible()
        if how == "blur":
            filter_input.click()
        else:
            name.press(how)
        expect(name).to_have_count(0)
        expect(new.locator(".section-name")).to_have_text(label)
        empty_add = new.get_by_role("button", name="Add panel", exact=True)
        expect(empty_add).to_be_visible()
        # Enter and Escape hand focus to the section's next step; a blur leaves it where it went.
        if how != "blur":
            expect(empty_add).to_be_focused()

    print("new section: clears panel filter and reveals its name", flush=True)
    filter_input.fill("nothing matches this")
    expect(page.locator(".section")).to_have_count(0)
    add_section.press("Space")
    expect(filter_input).to_have_value("")
    expect(name).to_be_focused()
    expect(name).to_be_in_viewport()
    name.fill("Filtered creation")
    name.press("Enter")

    print("header add: collapsed section and active filter", flush=True)
    filter_input.fill(PRIMARY)
    expect(section.get_by_role("button", name="Add panel", exact=True)).to_have_count(0)
    section.locator(".section-name").click()
    expect(section.locator(".section-grid")).to_have_count(0)
    section.get_by_title("Add metric", exact=True).press("Enter")
    expect(picker).to_be_focused()
    expect(filter_input).to_have_value("")
    picker.select_option(PRIMARY)
    close_metric(page, panel, section.get_by_title("Add metric", exact=True))
    expect(section.locator(".metric-rect")).to_have_count(2)

    print("header add: returns to the last page of a paginated section", flush=True)
    section.locator('.section-header button[title="Configure"]').click()
    settings = options_panel(page, "Configure section")
    settings.get_by_role("spinbutton", name="Columns", exact=True).fill("1")
    settings.get_by_role("spinbutton", name="Rows / page", exact=True).fill("1")
    settings.get_by_role("button", name="Close", exact=True).click()
    section.get_by_title("Add metric", exact=True).click()
    expect(picker).to_be_focused()
    picker.select_option(PRIMARY)
    close_metric(page, panel, section.get_by_title("Add metric", exact=True))
    expect(section.get_by_title("Jump to page", exact=True)).to_have_value("3")
    expect(section.locator(".metric-rect")).to_have_count(1)

    print(
        "new section: leaves a maximized chart and focuses the grid input", flush=True
    )
    # Off-screen panels render placeholders, so bring one back first.
    chart = page.locator('.section[data-section-id="logs"] .metric-rect').first
    chart.scroll_into_view_if_needed()
    chart.get_by_title("Configure", exact=True).click()
    expect(panel_body(panel)).to_be_focused()
    pointer_open(page, add_section, name)
    expect(page.locator(".maximize-overlay")).to_have_count(0)
    expect(name).to_be_in_viewport()
    name.fill("From maximized chart")
    name.press("Enter")

    print("new section: leaves a chart its Maximize button maximized", flush=True)
    chart.scroll_into_view_if_needed()
    chart.get_by_title("Maximize", exact=True).click()
    expect(page.locator(".maximize-overlay")).to_have_count(1)
    pointer_open(page, add_section, name)
    expect(page.locator(".maximize-overlay")).to_have_count(0)
    name.press("Escape")

    print("new section: a visible one doesn't move the page", flush=True)
    # Enough sections after the first for its header to sit 300 px down with room to spare.
    zetas = 15
    for i in range(zetas):
        add_section.click()
        name.fill(f"Zeta {i}")
        name.press("Enter")
    expect(name).to_have_count(0)
    # The last Enter's reveal lands first (WebKit can hold its frame), or it would undo the scroll below.
    last = page.locator(
        ".section", has=page.get_by_text(f"Zeta {zetas - 1}", exact=True)
    )
    expect(last.locator(".section-header")).to_be_in_viewport()
    render_turn(page)
    first = page.locator(".section", has=page.get_by_text("Zeta 0", exact=True))
    # Its successor's header mid-view: the new section takes that place.
    offset = (
        "(main, h) => h.getBoundingClientRect().top - main.getBoundingClientRect().top"
    )
    header = first.locator(".section-header").element_handle()
    main.evaluate(f"(main, h) => main.scrollTop += ({offset})(main, h) - 300", header)
    render_turn(page)
    assert abs(main.evaluate(offset, header) - 300) <= 2
    before = main.evaluate("e => e.scrollTop")
    add_section.click()
    expect(name).to_be_focused()
    render_turn(page)
    # Within WebKit's pixel of rounding.
    moved = main.evaluate("e => e.scrollTop") - before
    assert abs(moved) <= 2, moved
    expect(section_of(name)).to_be_in_viewport(ratio=1)

    print(
        "naming: Tab out keeps focus on the next control, in view, when the name moves the section",
        flush=True,
    )
    tabbed = named_section(page, name)
    name.fill("Zz tabbed")
    page.keyboard.press("Tab")
    expect(name).to_have_count(0)
    if tab_reaches_buttons:
        add_metric = tabbed.get_by_title("Add metric", exact=True)
        expect(add_metric).to_be_focused()
        expect(add_metric).to_be_in_viewport()

    print("new section: one at the viewport's edge comes fully into view", flush=True)
    main.evaluate(
        "(main, h) => main.scrollTop += h.getBoundingClientRect().bottom - main.getBoundingClientRect().bottom",
        first.locator(".section-header").element_handle(),
    )
    render_turn(page)
    before = main.evaluate("e => e.scrollTop")
    add_section.click()
    expect(name).to_be_focused()
    render_turn(page)
    expect(
        section_of(name).get_by_role("button", name="Add panel", exact=True)
    ).to_be_in_viewport(ratio=1)
    # By the least distance: just the rest of the section.
    moved = main.evaluate("e => e.scrollTop") - before
    assert 0 < moved < 200, moved
    name.press("Escape")

    print(
        "naming: Tab out from the top brings the next control into view as the rename moves it",
        flush=True,
    )
    # Firefox needs the scroll branch here; Chromium scrolls the Tab target itself.
    cross_layout(page)
    add_section.click()
    tabbed = named_section(page, name)
    assert main.evaluate("e => e.scrollTop") == 0
    name.fill("Z tabbed from the top")
    page.keyboard.press("Tab")
    expect(name).to_have_count(0)
    if tab_reaches_buttons:
        add_metric = tabbed.get_by_title("Add metric", exact=True)
        expect(add_metric).to_be_focused()
        expect(add_metric).to_be_in_viewport()
        assert main.evaluate("e => e.scrollTop") > 2000
    rename_reveal(page)
    assert not fixture.errors, f"unexpected fixture RPCs: {fixture.errors}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(
            headless=True, timeout=15_000
        )
        try:
            page = browser.new_page(viewport=VIEWPORT)
            checks(page, args.url)
        finally:
            browser.close()
    print(f"creation fences passed ({args.browser})")


if __name__ == "__main__":
    main()
