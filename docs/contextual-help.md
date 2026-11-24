# Contextual help for Kymo controls

The inventory was audited at `92358e9f4` (2026-11-12); its line references are for that revision, except where a row cites code the feature added.

## Purpose

Kymo has compact controls and useful gestures whose behavior is not always visible. Contextual help explains those missing facts in place and leaves self-describing controls alone.

The defining cases: Show all / Hide all get nothing; dragging across the visibility circles is revealed; the unclear icons (a gear, a trash can, the edited dot) are explained even where a title already says it, since titles go unread.

## Content rules

1. Explain a hidden gesture, an ambiguous symbol, or a material scope or consequence absent from the label.
2. Skip text that merely restates a label. Do not explain every button for consistency.
3. Use a short phrase, usually 2–6 words, at most 8, with no closing period.
4. Say the action or meaning first, in ordinary words.
5. Show the current behavior, with a variant per state where it differs.
6. No algorithm derivations; the smoothing hover hints keep the scientific background.
7. Keep required consequences and failure information in the visible warning, confirmation, or error. Help does not replace them, and their copy is outside the word limit.
8. A native `title` is not an explanation: almost nobody reads one. A control that needs explaining gets a note whatever its title says, and one that needs none is skipped for being clear, never for having a title. An icon-only control whose icon is unclear is explained; a clear one (a step or page arrow, a close ×, the theme toggle) is not. Where an explained control has a title, the title is the note’s text, so the control never says two things; if the old title was its only accessible name (section trash, chart trash), that name moves to `aria-label`. The panel filter is the one exception: its note is “Search” and its longer title stays (the owner’s choice). A title may say something narrower than the note (a visibility circle’s “Hide run” / “Show run” under “Click (drag) to show/hide runs”, a caption’s or a long name’s own text).

### Decisions used in the inventory

- **Explain:** the listed text is shown.
- **Conditional:** the listed text is shown only in the stated state.
- **Existing copy:** the interface already presents the information as visible text; nothing is added.
- **Skip:** nothing is added, usually because the visible text or standard interaction is sufficient; the row gives the reason. Skip never removes accessible names, full-name reveals, validation, confirmations, or existing guidance.

## Interaction

One **?** button, accessible name **Explain controls**, with a pressed state (`aria-pressed`). It sits beside the theme toggle on a dashboard. A page with nothing to annotate has no **?**: Trash has no explanations, and the two on Projects are native titles.

- Hovering or focusing **?** shows at once an annotation for every **Explain** row, and every **Conditional** row whose condition holds, whose control is on screen (the readout labels show on the hovered chart’s readout, so in practice while pinned). A repeated family shows one note, not one per row, chart, or dot, on whichever of its first three members on screen has room.
- Pressing **?** toggles a pin. Unpinned, the annotations hide once the pointer and keyboard focus have both left **?**. Unpinning, by press or Escape, hides them until the pointer or focus next enters **?**.
- Pinned help is nonmodal: controls stay usable, focus is not trapped, and pressing **?** leaves an active editor’s focus and caret alone. Other presses keep the pin. Navigating to another page ends it.
- Escape dismisses pinned help last, after every existing layer: a native dialog or popover, in-app handlers (filters, renames, the settings panel, a chart drag), Trash selection, then the maximized chart (`src/components/sidebar.rs:460`, `src/components/dashboard_layout.rs:56`). Help’s bubble-phase `window` listener unpins only when the key was not prevented, no mouse button is held, no top-layer element is open, Trash selection is off, no chart is maximized, and no chart drag is under way. Unpinned, help does not handle Escape.
- Controls that appear only on hover (section and chart actions, the chart resize handle) show their explanation on direct hover or focus while help is pinned. The run `…` triggers are the exception: every row shows its dots while notes show, and one is explained.
- Outside help, nothing new appears on hover, except that an explained control’s title is its note’s text (rule 8), as are the two Projects texts.
- Editor-specific help shows only while that editor is open, and a gesture hint only while its gesture is available; a disabled button (Revert, collapse sections) keeps its note, as it keeps its title. Help never opens settings or expands sections.
- A note sits against its control: under it or over it, sliding along it as far as its tick still lands on it (on the middle of an icon-sized control), else right or left of it, with a tick on that side; a control over half the window tall (a resize edge) takes the sides first. Nothing a reader needs is hidden: a note stays inside the viewport at the largest user font size, and off the other notes, an open popover, **?**, and every icon-sized thing that has a note (of a repeated family, its first members, up to the last its note may sit on), is an explained button, or is hidden (where a chart’s or section’s actions will appear). No note against its control moves when the pointer brings out another control (one off its control may). Among its free spots a note weighs, heaviest first: leaving a neighbour its last spot, not lying on another control that has a note, then the family’s first member before its second or third, under before over before the sides, and less slide before more, added up (a slide counts in note lengths, so a long one can outweigh a side). A note with no spot against its control chooses among the nearest ones further off on its first two sides, in line with it, with a thin line back to the control in place of the tick (under any note in between, and clear of what notes keep off where a spot of that ring allows); failing that (a maximized chart), the first place inside its control clear of the other notes and of what notes keep off, else its top left corner, which only a window too small for every note reaches, at the largest font. Notes take no pointer events, and hide while a mouse button is held, so none sits under a drag.
- An explanation for a control inside a popover (the run `…` menu) or for a popover itself (the color picker) is rendered inside that popover, while help is previewed or pinned: a page-level overlay paints beneath the top layer. Pressing **?** light-dismisses an open popover, so pin first, then open it.

**?** explains what is on screen; it is not a tour.

## Inventory

This inventory covers the Projects page, project and run dashboards, settings, Trash, charts, metadata, text logs, galleries, and application notices. Repeated instances use the same decision. Conditional fields are included even when absent from the initial screen.

### Run sidebar

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Show all / Show all listed / Hide all | Skip | — | The visible labels state the action. | `src/components/sidebar.rs:404–406`; `src/components/sidebar/view.rs:275` |
| Visibility circles: click and drag | Conditional | Click (drag) to show/hide runs | The circle does not say that a click toggles one run, nor that a drag paints across several; the one note covers both. On each circle; one example shows. Pressing a circle arms the drag; it continues over any part of a row. Normal mode only. Its title says only what a click would do now (“Hide run” / “Show run”). | `src/components/sidebar/view.rs:675`; `src/components/sidebar/view.rs:624` |
| Hovering a run row | Skip | — | The highlight across charts shows as soon as the pointer is on a row. | `src/components/sidebar/view.rs:624`; `src/components/sidebar/view.rs:647` |
| Run name link | Skip | — | Conventional navigation; the run name is already a link. | `src/components/sidebar/view.rs:499`; `src/components/sidebar/view.rs:740` |
| Long run names revealed on hover | Skip | — | The interaction reveals the content directly; explaining it adds no understanding. | `src/components/sidebar/view.rs:742` |
| `#number` suffix when any run names repeat | Conditional | #N: run number, shown when names repeat | The numbers disambiguate identity. If any name repeats, all rows show their run number. On the run name, in both modes. | `src/components/sidebar.rs:70` |
| Normal-mode `selected/total` counter | Conditional | Shown / total runs | Counts runs shown via the sidebar, not rendered series; a chart may cap runs or bind another set. “Shown”, since “selected” means Trash selection. Normal mode only. | `src/components/sidebar/view.rs:239`; `src/components/sidebar/view.rs:293` |
| Filter runs input | Skip | — | Placeholder identifies the ordinary filtering action. | `src/components/sidebar/view.rs:336` |
| Run filter's extra matching fields | Conditional | Matches names, IDs, #numbers | Searchable IDs/numbers are hidden. Shown while the filter is focused. | `src/components/sidebar/view.rs:176` |
| Header Trash icon | Explain | Delete runs | A trash can alone. Also its title. It starts a selection mode, whose footer says what to do; nothing is trashed until “Move N runs to Trash”. | `src/components/sidebar/view.rs:316` |
| Run overflow `…` trigger | Explain | More actions | Three dots alone. Also its title; its accessible name adds the run’s (“More actions for {name}”). The trigger shows only while its row is hovered or the trigger is focused (always on devices without hover), so every row shows its dots while notes show, for the note to point at. | `src/components/sidebar/view.rs:752`; `assets/kymo.css:503–529` |
| Copy run name | Skip | — | Menu label fully describes the action. | `src/components/sidebar/view.rs:769` |
| Rename menu item | Skip | — | Menu label fully describes the action. | `src/components/sidebar/view.rs:780` |
| Inline rename's save/cancel behavior | Skip | — | Conventional inline editing does not need another annotation; retain validation and keyboard behavior. | `src/components/sidebar.rs:715` |
| Rename validation and request errors | Skip | — | Existing validation and error messages provide the needed local explanation. | `src/components/sidebar.rs:593`; `src/components/sidebar.rs:734` |
| Change color… menu item | Skip | — | Its label describes the action. | `src/components/sidebar/view.rs:798` |
| Per-run Delete menu item | Explain | Recoverable for 7 days, then deletes permanently | Unlike the header icon, this moves one run to Trash at once, with no selection or confirmation. Shown inside the menu (a popover, see Interaction). A run that is still logging says so in the item (below). | `src/components/sidebar/view.rs:822`; `src/components/sidebar.rs:237`; `src/pages/trash_page.rs:311` |
| Sidebar right-edge resize handle | Explain | Drag to resize | Unlabeled thin divider conceals a useful gesture. | `src/components/sidebar/view.rs:417`; `src/util/width_drag.js:6` |
| Success toasts and failure feedback | Skip | — | Existing feedback states the result. | `src/components/sidebar.rs:344`; `src/components/sidebar/view.rs:446` |
| Empty run list / no matching runs | Skip | — | Existing empty-state message explains the current state. | `src/components/sidebar.rs:413`; `src/components/sidebar/view.rs:355` |

### Trash selection inside the sidebar

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Clicking or dragging whole rows to select for Trash | Existing copy | — | Already present in the selection footer. | `src/components/sidebar/view.rs:371`; `src/components/sidebar/view.rs:596` |
| Select all / Select all listed / Select none | Skip | — | Labels describe ordinary selection actions. | `src/components/sidebar.rs:398` |
| Trash-mode `selected/total` counter | Conditional | Selected for Trash / total runs | The same ratio means something else in this mode. | `src/components/sidebar/view.rs:239`; `src/components/sidebar/view.rs:293` |
| Circle and row marking while selecting for Trash | Conditional | Red circle: selected for Trash | A selected row's circle is filled and ringed in red whether or not the run is shown. | `src/components/sidebar/view.rs:659`; `assets/kymo.css:1915` |
| Selected-outside-filter count | Skip | — | Existing “selected outside filter” text already resolves the ambiguity. | `src/components/sidebar/view.rs:375` |
| Cancel `×` | Skip | — | A × beside the selection count: conventional. | `src/components/sidebar/view.rs:303` |
| Move N runs to Trash | Skip | — | Text includes both action and scope; no additional tooltip is needed. | `src/components/sidebar/view.rs:390` |
| Moving to Trash… disabled state | Skip | — | Existing progress label explains why the action is unavailable. | `src/components/sidebar/view.rs:404` |
| Running/stuck selected runs' logging-loss warning | Existing copy | — | A visible consequence warning (rule 7). | `src/components/sidebar.rs:158`; `src/components/sidebar/view.rs:380` |
| Partial failure and retry selection | Skip | — | Existing feedback explicitly leaves failed/unresolved runs selected and explains the result. | `src/components/sidebar.rs:332`; `src/components/sidebar.rs:357` |

A run that is still logging says so in its Delete item, as the bulk footer does for a selection: “Still logging; new points will be dropped” (`src/components/sidebar/view.rs`; the run deletion design rules out a confirmation there).

### Run status glyphs

Shape and colour alone say a run’s state, so each glyph carries a note naming it, which is also its title and accessible name (`role="img"`, not focusable). A glyph precedes each sidebar run’s name and follows an active run’s page title once the run is known. A run in Trash shows none: the server stops tracking its liveness there. One note shows per state on screen.

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Running dot | Conditional | Running: still logging | A plain dot. Shown for a run in this state. | `src/components/sidebar/view.rs:10` |
| Stuck pause glyph | Conditional | Stuck: main metrics stalled | Alive, but its main metrics stopped arriving. Shown for a run in this state. | `src/components/sidebar/view.rs:11` |
| Unresponsive warning glyph | Conditional | Unresponsive: silent 10+ s | Temporary silence, not a confirmed failure. Shown for a run in this state. | `src/components/sidebar/view.rs:18` |
| Presumed-dead glyph | Conditional | Presumed dead: silent 10+ min | A silence-based inference, not a confirmed death. Shown for a run in this state. | `src/components/sidebar/view.rs:24` |
| Crashed cross | Conditional | Crashed: non-zero exit | The run exited with an error. Shown for a run in this state. | `src/components/sidebar/view.rs:30` |
| Finished check mark | Conditional | Finished: clean exit | The run exited cleanly. Shown for a run in this state. | `src/components/sidebar/view.rs:36` |
| Unknown dot | Conditional | Status unknown | The same dot as running, in another colour. Shown for a run in this state. | `src/components/sidebar/view.rs:42` |

### Projects page

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Project row/name link | Skip | — | Conventional navigation to the named project. | `src/pages/projects_page.rs:234` |
| Filter projects | Skip | — | Placeholder identifies ordinary filtering; no hidden matching semantics beyond project name. | `src/pages/projects_page.rs:212` |
| Project sort header | Skip | — | Conventional column sorting; accessible name already says Sort by Project. | `src/pages/projects_page.rs:148`; `src/pages/projects_page.rs:231` |
| Last logged sort header | Conditional | Press again to refresh the order | Rows never reorder on new activity; pressing the header again re-sorts. A native title, since Projects has no **?**; only while Last logged is the active sort. | `src/pages/projects_page.rs:55`; `src/pages/projects_page.rs:155–159` |
| Last logged relative value / exact-time hover | Existing copy | — | The Last logged heading supplies the meaning; retain the exact-time reveal without adding a tutorial for hovering. | `src/pages/projects_page.rs:26`; `src/pages/projects_page.rs:240–243` |
| Project without a Last logged time `—` | Conditional | No logging activity recorded | A bare dash can mean missing data or an error; clarify only this state, as the dash’s native title, since Projects has no **?**. Not “nothing logged”: runs imported after the server first seeded the value are not reflected. | `src/pages/projects_page.rs:29`; `proto/kymo.proto:371` |
| Trash navigation link | Skip | — | Explicit Trash text and View runs in Trash label suffice. | `src/pages/projects_page.rs:177` |
| Settings button | Skip | — | Explicit Settings text; its title “User settings” stays. Individual settings are covered under Browser-wide user settings. | `src/pages/projects_page.rs:185`; `src/components/user_settings.rs:12` |
| Project loading, no-projects, and no-matches states | Skip | — | Existing state messages describe the condition and getting started where applicable. | `src/pages/projects_page.rs:190`; `src/pages/projects_page.rs:226` |

### Trash page

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Seven-day recovery rule | Existing copy | — | Already visible below the Trash heading. | `src/pages/trash_page.rs:311` |
| ← Projects | Skip | — | Direction and destination are explicit. | `src/pages/trash_page.rs:309` |
| Trashed run name link | Skip | — | Conventional navigation; the run page it opens states that the run is in Trash. | `src/pages/trash_page.rs:522`; `src/pages/run_page.rs:90` |
| `#number` on a trashed run | Skip | — | Always shown here; a run number beside a name needs no explanation. | `src/pages/trash_page.rs:532` |
| Project link beside a trashed run | Skip | — | Conventional navigation to the named project. | `src/pages/trash_page.rs:534` |
| Runs count | Skip | — | Explicit noun and number leave no ambiguous ratio. | `src/pages/trash_page.rs:297`; `src/pages/trash_page.rs:348` |
| Deletes permanently column / date / remaining time | Skip | — | Header and remaining-time text already identify deadline and consequence. | `src/pages/trash_page.rs:354`; `src/pages/trash_page.rs:542` |
| Restore | Skip | — | Action label and surrounding Trash context suffice. | `src/pages/trash_page.rs:491`; `src/pages/trash_page.rs:552` |
| Restoring… progress | Skip | — | Existing progress label explains the disabled button. | `src/pages/trash_page.rs:481` |
| Unavailable after expiry | Existing copy | — | The Expired state gives the reason. | `src/pages/trash_page.rs:486`; `src/pages/trash_page.rs:497` |
| Load more / Showing N of M | Skip | — | Familiar pagination and explicit progress count. | `src/pages/trash_page.rs:384`; `src/pages/trash_page.rs:447` |
| Retry after loading failure | Skip | — | Existing error and Retry label fully describe the recovery action. | `src/pages/trash_page.rs:316` |
| Restore success/failure/verification messages | Skip | — | Existing feedback states the result. | `src/pages/trash_page.rs:588`; `src/pages/trash_page.rs:600` |
| Loading Trash / Trash is empty | Skip | — | Existing state text is sufficient. | `src/pages/trash_page.rs:378` |

### Shared settings panel and inheritance

| Item | Decision | Displayed explanation | Justification | Source |
|---|---|---|---|---|
| Live editing, once per settings panel | Explain | Edits apply immediately | There is no Save button; users otherwise have to infer whether Close saves or cancels. One shared note covers all fields. A smoothing-slider drag writes when it rests or is released. Dashboard panels only: the user Settings panel on Projects already says “Saved in this browser”. | `src/components/options_panel.rs:15`; `src/components/options_editor.rs:77` |
| Revert | Explain | Undo changes since opening | “Revert” alone does not identify its baseline: the opening snapshot, not inherited defaults. Also its title. | `src/components/options_panel.rs:81`; `src/components/options_editor.rs:273`; `src/components/section_editor.rs:107` |
| Disabled Revert | Skip | — | The disabled state says there is nothing to revert. Revert’s own note shows either way, as its title does. | `src/components/options_panel.rs:80` |
| Close × | Skip | — | Closing is conventional; the live-editing note explains persistence. | `src/components/options_panel.rs:89` |
| Settings panel’s left resize edge | Explain | Drag to resize | An unlabelled edge conceals a useful gesture. Dashboard panels only: the edge is hidden in the user Settings panel on the Projects page. | `src/components/options_panel.rs:67`; `src/util/width_drag.js`; `assets/kymo.css:1244` |
| Panel title and target subtitle | Skip | — | They already identify the editor and the object being changed. | `src/components/options_panel.rs:69` |
| Collapsible editor groups: Plots, Smoothing, Image Gallery, Metadata | Skip | — | Their headings, chevrons, and expanded state communicate ordinary disclosure behavior. | `src/components/options_editor.rs:289` |
| Edited/reset dot ● | Explain | Reset to parent value | Looks passive but resets the field; “parent” (the level the field inherits from) distinguishes it from Revert’s snapshot. Also its title and accessible name. | `src/components/options_editor.rs:26–30` |
| Override chips below a defaults field | Explain | Click to clear this override | They resemble labels but are buttons: each names a section (§) or chart that overrides this default, and a click clears that override so it inherits again. Also its title; the accessible name says whose override it is. One note shows for them all. | `src/components/options_editor.rs:200`; `src/components/options_editor.rs:230` |
| Project-level chart defaults | Explain | Most specific setting wins: chart, section, project | Titled “Project settings”; nothing says its fields are defaults that sections and charts can override. The text states the whole rule, so the section panel shows the same one. Override chips appear only once an override exists. Anchor: the panel header. | `src/components/options_editor.rs:250`; `src/state/layout_config.rs:3356` |
| Section-level Chart defaults | Conditional | Most specific setting wins: chart, section, project | Places a section default between the chart’s own setting and the project default; the same text as the project panel’s. On the Chart defaults heading, while the group is open. | `src/components/section_editor.rs:187`; `src/components/options_editor.rs:289`; `src/state/layout_config.rs:3356` |
| Browser-local layout persistence | Explain | Settings and layout saved in this browser only | The persistence scope is not present in the dashboard editors. Anchor: the panel header, beside the live-editing note. | `src/state/layout_config.rs:2829` (the browser storage key) |

### Section editor

| Item | Decision | Displayed explanation | Justification | Source |
|---|---|---|---|---|
| Name | Skip | — | A labelled text field sufficiently describes renaming the section. | `src/components/section_editor.rs:128` |
| Columns | Skip | — | The numeric column count directly describes the grid arrangement; additional wording would restate the label. | `src/components/section_editor.rs:156`; `src/components/section.rs:18` |
| Rows / page | Explain | 0 shows all rows | Zero’s special meaning cannot be inferred from the label or number input. | `src/components/section_editor.rs:172`; `src/components/section.rs:32` |
| Chart defaults disclosure | Skip | — | The disclosure itself is conventional; its heading carries the scope explanation above while open. | `src/components/section_editor.rs:187` |

### Data sources and chart appearance

| Item | Decision | Displayed explanation | Justification | Source |
|---|---|---|---|---|
| Data Sources / Source N headings | Skip | — | They already group the controls and distinguish sources; explaining the headings adds no action. | `src/components/binding_editor.rs:360`; `src/components/binding_editor.rs:1113` |
| + Add data source | Explain | Add data to this chart | The label matches the “Data Sources” heading above it; the note says that a source adds data to the same chart rather than creating another, including for media and logs. | `src/components/binding_editor.rs:386` |
| Remove source | Skip | — | “Remove” within the numbered source row clearly identifies the action and object. | `src/components/binding_editor.rs:1114` |
| Project selector / Current Project option | Skip | — | The explicit Project label and option names already identify the data scope. | `src/components/binding_editor.rs:1123` |
| Runs: Shown Runs | Conditional | Follows current run or sidebar visibility | The set follows navigation and sidebar visibility rather than freezing when configured. Shown while Shown Runs is selected. | `src/components/binding_editor.rs:1163`; `src/state/layout_config.rs` (`RunRef::Selected`) |
| Runs: All Runs | Skip | — | The name states that every run in the selected project is included. | `src/components/binding_editor.rs:1183` |
| Runs: Specific Runs | Conditional | Unaffected by sidebar visibility | The independent, persistent selection is a consequence beyond the option’s name. Shown while Specific Runs is selected. | `src/components/binding_editor.rs:1184`; `src/state/layout_config.rs` (`RunRef::Specific`) |
| Shown Runs / All Runs disabled for a named project | Conditional | Use Current Project for Shown/All Runs | Disabled options alone do not reveal the required next step. They are disabled for every project picked by name, the current one included. On the source row: a native option can hold no note. | `src/components/binding_editor.rs:1102`; `src/components/binding_editor.rs:1182` |
| Specific-run checkboxes | Skip | — | Labelled checkboxes conventionally select the named runs; no new gesture is involved. | `src/components/binding_editor.rs:1194` |
| No runs in this project | Skip | — | State text is sufficient. | `src/components/binding_editor.rs:1217` |
| Metric selector | Skip | — | “Metric” and “Select metric” already state the operation. | `src/components/binding_editor.rs:1237` |
| Filter metrics | Skip | — | The placeholder describes filtering the adjacent metric choices. | `src/components/binding_editor.rs:1284` |
| Metric filter result count | Skip | — | “N of M” is a conventional result count with immediate context. | `src/components/binding_editor.rs:1302` |
| Disabled incompatible metric groups | Existing copy | — | The option group already states “other sources are …”; do not duplicate its explanation. | `src/components/binding_editor.rs:1264` |
| X axis: Step / Relative time / Wall time | Skip | — | Conventional axis choices; the chart shows the result. | `src/components/binding_editor.rs:426–428` |
| X metric | Conditional | Use this metric instead of step | It replaces the X coordinate rather than adding another plotted series; that distinction is not clear from the short label. The field exists only while X axis is Step. | `src/components/binding_editor.rs:435`; `src/components/binding_editor.rs:112` |
| X metric: Step (default) | Skip | — | The option already names its behavior and default status. | `src/components/binding_editor.rs:449` |
| Log X / Log Y | Skip | — | Labelled scale toggles are standard chart controls. | `src/components/options_editor.rs:419`; `src/components/options_editor.rs:421` |
| Max runs | Explain | 0 means unlimited | The label explains the cap; only the sentinel value needs help. | `src/components/options_editor.rs:423`; `src/state/layout_config.rs:89` |
| Width | Explain | Width in section columns | The bare number’s unit is hidden; users could assume pixels or a percentage. | `src/components/binding_editor.rs:535`; `src/state/layout_config.rs:534` |

### Smoothing and conditional media controls

| Item | Decision | Displayed explanation | Justification | Source |
|---|---|---|---|---|
| Smoothing selector as a whole | Skip | — | Do not restate “smooth the chart”; the selected algorithm’s hint already appears on hovering the select. | `src/components/options_editor.rs:335` |
| Smoothing: None | Skip | — | The option already states that smoothing is disabled. | `src/components/options_editor.rs:348` |
| Smoothing: Triangular polyfit | Existing copy | “Polynomial fit where weight ~ (step + 1). Causal.” (hover hint) | The hint already conveys causal behavior and weighting. | `src/components/options_editor.rs:38`; `src/components/options_editor.rs:385` |
| Smoothing: EMA polyfit | Existing copy | “Local polynomial fit on (causal) EMA. Order 0 is typical EMA smoothing.” (hover hint) | The hint already relates the algorithm to EMA and its order-zero case. | `src/components/options_editor.rs:40`; `src/components/options_editor.rs:385` |
| Smoothing: (1−x²)² polyfit | Existing copy | The existing paragraph (hover hint) | The hint warns to compare runs at matching X values, unfinished endpoints included. | `src/components/options_editor.rs:43`; `src/components/options_editor.rs:385` |
| Fit order | Skip | — | The options already explain themselves as “0 — average”, “1 — linear”, and “2 — quadratic”. | `src/components/options_editor.rs:357` |
| Window | Conditional | Smoothing width counts points, not steps | The number’s unit is not stated: it is a count of the run’s own logged points (the server’s “median sample intervals”), so with a point every 10 steps a window of 5 spans 50 steps. The field exists only for (1−x²)² polyfit. | `src/components/options_editor.rs:375`; `kymo-server/src/chart.rs:3004` |
| Decay length | Conditional | Larger values keep more history | The label gives no direction. It names no unit, since the length is in steps on a step axis and in points on a time axis. The field exists only for EMA polyfit. | `src/components/options_editor.rs:381` |
| Slider and adjacent numeric input | Skip | — | Both edit the same labelled value using conventional controls; no need to describe input mechanics. | `src/components/options_editor.rs:127` |
| Gallery Mode: Select index | Conditional | Same image position across runs | “Index” alone does not reveal the cross-run comparison. Shown while this mode is selected, even where a single run makes the gallery group by run (`src/components/cdn_gallery.rs:957`). | `src/components/binding_editor.rs:498`; `src/components/cdn_gallery.rs:1412` |
| Gallery Mode: Group by run | Skip | — | The option plainly names the grouping; a description would paraphrase it. | `src/components/binding_editor.rs:499` |
| Gallery Mode: Interleaved | Conditional | One grid per image index | The grouping is not stated by the single-word label. Shown while this mode is selected, as above. | `src/components/binding_editor.rs:500`; `src/components/cdn_gallery.rs:1484` |
| Metadata: Diff only (hide keys identical across runs) | Existing copy | — | The parenthetical already provides the full explanation. | `src/components/binding_editor.rs:523` |

The Smoothing select shows the selected algorithm’s hint in a box beside the panel while the pointer is over it (`assets/kymo.css:1300`); that box is the explanation, and is mouse-only.

### Browser-wide user settings

| Item | Decision | Displayed explanation | Justification | Source |
|---|---|---|---|---|
| Saved in this browser subtitle | Existing copy | — | Persistence scope is explicitly stated, so no duplicate callout is needed. | `src/components/user_settings.rs:88` |
| Font size slider and px readout | Skip | — | The label and units state both meaning and value. | `src/components/user_settings.rs:100` |
| Single-click to exit chart zoom | Skip | — | The full checkbox label explains the behavior. | `src/components/user_settings.rs:38` |
| Highlight all runs with the same name | Skip | — | The full label already explains which runs are affected. | `src/components/user_settings.rs:43` |
| Show each run’s nearest point when hovering a gap | Skip | — | The full label already describes the feature and trigger. A borrowed point’s X is explained on the readout that shows one (Numeric charts). | `src/components/user_settings.rs:48` |
| Chart sections visible by default (instead of collapsed) | Existing copy | — | Both the setting and its alternative are explicit in the label. | `src/components/user_settings.rs:53` |
| Save failure message | Existing copy | — | The inline alert already explains the failure and next step; adding help would compete with actionable feedback. | `src/components/user_settings.rs:139` |

### Navbar and page chrome

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Kymo brand / project breadcrumb links | Skip | — | Conventional navigation with visible destinations; prose would repeat the link. | `src/components/navbar.rs:44–65` |
| Current project / run name | Skip | — | Identifies the current context; retain full-name overflow reveal, not a tutorial. | `src/components/navbar.rs:49–90` |
| Filter panels | Explain | Search | The owner’s wording, as a note; the title “Show only panels whose name contains this text (case-insensitive)” stays. The placeholder names the filtered objects and the title states the matching, except that a panel’s bound metric names match too (`src/state/layout_config.rs:631`); that title is a separate copy follow-up. | `src/components/navbar.rs:95–109` |
| Clear panel filter × | Skip | — | A × in a filter field: conventional. | `src/components/navbar.rs:113–118` |
| Collapse / expand all sections icon | Explain | Collapse sections / Expand sections / Collapse listed sections / Expand listed sections | An icon alone. Also its state-dependent title; “listed” while a panel filter narrows the sections. | `src/components/navbar.rs:37–42,121–129` |
| + Section | Skip | — | Explicit creation label; new inline name field supplies the next step. | `src/components/navbar.rs:130–163` |
| Project-settings gear | Explain | Project settings | A gear alone, one of three kinds on the page. Also its title; the defaults-scope explanation is inside the panel it opens. | `src/components/navbar.rs:165–171` |
| Reset-layout icon | Explain | Reset layout | A bare restore glyph. Also its title; the confirmation states that all customizations are discarded. | `src/components/navbar.rs:173–182` |
| Theme sun / moon (navbar and Projects page) | Skip | — | A sun or moon in a navbar is a familiar theme switch. | `src/components/theme_toggle.rs:8–17` |
| **?** itself | Explain | Press to keep help shown | The preview goes when the pointer leaves **?**, and nothing else says a press keeps it. One text for both states, so that pinning moves no other note. | `src/components/navbar.rs:201–221` |

### Sections and panels

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Section header bar | Explain | Collapse/expand section | The whole bar toggles collapse, with no chevron or action label; its accessible name is the section name and count plus an expanded state. | `src/components/section.rs:165–181,270–281` |
| Section title / panel count | Skip | — | Identifying content. Both sit inside the header toggle above and add no separate action. | `src/components/section.rs:277–280` |
| Section grip | Explain | Drag to reorder | Grip shape does not teach the drag. Also its title. | `src/components/section.rs:182–201` |
| Section + icon | Explain | Add panel | A plus alone. Also its title. | `src/components/section.rs:283–288` |
| Empty-section + Add panel | Skip | — | Already explicit visible action text; do not duplicate the plus-icon hint. | `src/components/section.rs:383–390` |
| Section gear | Explain | Configure section | A gear alone. Also its title. | `src/components/section.rs:290–309` |
| Section trash can | Explain | Remove section and panels; keep logged data | The same trash icon deletes runs elsewhere; layout-only scope and contained panels matter. Also its title; “Delete section” is its accessible name. | `src/components/section.rs:312–325`; `src/state/app_state.rs:603` |
| Section Previous / Next page arrows | Skip | — | Familiar arrows beside a page number. | `src/components/section.rs:335–342,366–373` |
| Section page-number input | Skip | — | A page number in a field between the arrows: ordinary form behavior. | `src/components/section.rs:345–364` |
| New section’s inline name input | Skip | — | Appears focused after creation and is labelled “Section name”; ordinary Enter/blur/Esc behavior. | `src/components/section.rs:203–267` |
| Chart title | Conditional | Double-click to rename | Text lacks an edit icon; the gesture is otherwise invisible. Not on a maximized chart the layout no longer holds, which cannot be renamed. | `src/components/metric_rect.rs:394–407` |
| Chart-title rename input | Existing copy | — | Existing “Label (empty = auto)” placeholder explains the non-obvious blank value locally; ordinary Enter/blur/Esc behavior. | `src/components/metric_rect.rs:359–389` |
| Chart maximize icon | Explain | Maximize | An icon alone. Also its title; its shortcuts are covered by the maximized-view row. | `src/components/metric_rect.rs:413–423` |
| Chart gear | Explain | Chart settings | A gear alone. Also its title and accessible name, on a grid chart and on the maximized one. On a grid chart it also maximizes the chart, which shows as it happens. | `src/components/metric_rect.rs:425–441` |
| Maximized chart × | Skip | — | Familiar dismissal; no deletion consequence. | `src/components/metric_rect.rs:444–453` |
| Chart trash can | Explain | Remove chart; keep logged data | Must distinguish removing a visualization from moving underlying runs to Trash. Also its title; “Delete chart” is its accessible name. | `src/components/metric_rect.rs:455–467`; `src/state/app_state.rs:765` |
| Chart corner resize handle | Explain | Drag horizontally: width; vertically: section chart heights | The drag feature is hidden, and its vertical scope is unexpectedly section-wide. Also its title. | `src/components/metric_rect.rs:489–522`; `src/components/metric_rect/resize.js:26–48` |
| Loading spinner / No data / failure messages | Existing copy | — | State feedback rather than controls, shown inline in each chart body. | `src/components/metric_rect.rs:485–487`; `src/components/metric_rect/numeric_content.rs:603` |

The confirmations state the scope too: `Delete section "{name}" and its panels? Logged data is kept.` and `Delete chart "{title}"? Logged data is kept.` (`src/components/section.rs:326`, `src/components/metric_rect.rs:472`).

### Numeric charts and maximized navigation

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Numeric plot drag | Explain | Drag horizontally to zoom | A plain graph does not reveal its custom selection gesture. | `src/components/uplot_chart/create.js:616–683` |
| Reset numeric zoom | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/create.js:545,565–580` |
| X-axis strip below the plot | Explain | Drag axis to rescale | The axis pull, which widens or narrows the range, is not discoverable from a conventional axis; it is not a pan. Its own note, on the axis. | `src/components/uplot_chart/create.js:582–586,685–739` |
| Zoom shared across charts | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/create.js:528–543`; `src/components/uplot_chart/create_js.rs:113` |
| Zoom ending at newest data | Conditional | Include right edge to follow new data | Hidden follow behavior. Shown on an unzoomed chart of a live run that displays its newest data. | `src/components/uplot_chart/create.js:400–401,485–491,681` |
| Cancelling a chart drag | Skip | — | Escape cancels the drag, but the hint could only show mid-gesture, when notes are hidden. In a maximized chart the same Escape also closes the chart. | `src/components/uplot_chart/create.js:546–560` |
| Copy values at a chart point | Conditional | ⌘C copies values / Ctrl+C copies values | Clipboard shortcut has no visible button; the second text is for non-Mac platforms. Copies populated series at the hovered column. Shown over the hovered chart’s readout, with a tick down at it, so only while a readout shows (owner’s choice). | `assets/kymo.css` (readout labels); `src/components/uplot_chart/copy.js:4–7,15–35,41–62` |
| Ordinary hover readout | Skip | — | Hover immediately displays values and run names; another “hover for values” message teaches little. | `src/components/uplot_chart/hover.js:58–105` |
| Hovering a series | Skip | — | Dims the other runs and marks the run’s sidebar row as the pointer moves; visible at once. | `src/components/uplot_chart/hover.js:35–49` |
| Faint trace behind a smoothed line | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/create_js.rs:160–165` |
| Raw range / shaded envelope | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/create_js.rs:18–20,167–199` |
| Colored and gray values in a chart’s hover readout | Conditional | Smoothed / Raw value / Range | The readout does not say which number is which. While notes show, the hovered chart’s readout carries a label just above its first row’s colored value on a smoothed chart, and one above the gray column’s first populated cell saying which of the two it holds, each with a tick down at the value. | `assets/kymo.css` (readout labels); `src/components/uplot_chart/help.js`; `src/components/uplot_chart/hover.js:85–97` |
| Hollow non-finite markers | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/create_js.rs:29–33,209`; `src/components/uplot_chart/hover.js:36–38,75–80` |
| `×N` after a readout value | Skip | — | Not explained (owner’s choice). | `src/components/uplot_chart/hover.js:80`; `proto/kymo.proto:525`; `kymo-server/src/query.rs:1681,1777` |
| Borrowed-point @ X readout | Conditional | @: nearest sample’s X | Shown under the hovered chart’s readout, with a tick up at its `@` column, only while that readout shows an `@` (owner’s choice). The value after `@` is the borrowed sample’s own X, or its bucket’s bounds on a downsampled chart. | `assets/kymo.css` (readout labels); `src/components/uplot_chart/hover.js:54–60,65–83` |
| Maximized panel keyboard navigation | Conditional | ←/→: panels; Esc: close | Hidden shortcut; maximized view only. Arrows are ignored in fields and sideways scrollers. | `src/components/dashboard_layout.rs:50–69` |
| Maximized gallery step shortcuts | Conditional | ↑: next step; ↓: previous | Hidden shortcut with a non-obvious direction. Shown only on a maximized gallery with more than one step. | `src/components/dashboard_layout.rs:50–69` |

### Image galleries, files, audio, and video

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Gallery thumbnail | Skip | — | Conventional; the thumbnail keeps its composed accessible name (“Open image N from source M (run) full-size”, plus the caption). | `src/components/cdn_gallery.rs:95–111,145–162` |
| Gallery step slider | Skip | — | Labelled “step N (i/n)”; the index slider is labelled “index”. | `src/components/cdn_gallery.rs:974–1016` |
| Gallery step while new steps arrive | Conditional | Stays on this step as new ones arrive | With no remembered choice a gallery starts at the latest step, then holds its step as new ones arrive; unlike charts and logs it does not follow. Shown only while a source run has not ended and the gallery has more than one step. | `src/components/cdn_gallery.rs:26–33,793–797` |
| Gallery sources with nothing at the shown step | Conditional | Only runs with an image at this step | Runs that logged at other steps drop out of the grid as the step changes, with nothing saying why. On the step slider’s row, while a source has nothing at the shown step. | `src/components/cdn_gallery.rs:826–834,977` |
| Gallery Previous / Next step arrows | Skip | — | Arrows at the two ends of the step slider. | `src/components/cdn_gallery.rs:978–1013` |
| Gallery image-index slider | Conditional | Same image position across runs | Bare “index” can be confused with step. The slider exists only in Select index mode, which needs two or more sources; same text as that mode’s row. | `src/components/cdn_gallery.rs:1405–1455` |
| Gallery caption | Explain | Click to copy | Click-to-copy is signalled only by a hover cursor and outline. One note shows for them all; each caption’s title is its own text. | `src/components/cdn_gallery.rs:163–165`; `src/components/copy_text.rs:31–70` |
| Gallery source/run labels and step counters | Skip | — | Identifying text and current positions, not unexplained actions. | `src/components/cdn_gallery.rs:198–203,1016` |
| Full-size preview left/right arrows | Skip | — | Arrows at the picture’s sides: the next and previous image. | `src/components/cdn_gallery.rs:1169,1181–1182` |
| Full-size preview up/down arrows | Skip | — | Arrows over and under the picture: the next and previous step. | `src/components/cdn_gallery.rs:1169,1183–1184` |
| Full-size preview keyboard controls | Skip | — | The arrow keys do what the on-screen arrows do; Esc closing a dialog is conventional. | `src/components/cdn_gallery.rs:1224–1264` |
| Full-size preview backdrop click | Skip | — | Familiar dismissal. | `src/components/cdn_gallery.rs:1240–1241` |
| Full-size preview caption | Skip | — | Deliberately plain selectable text, unlike gallery captions: it does not copy on click. | `src/components/cdn_gallery.rs:1277–1283` |
| File-name links | Skip | — | Visible filenames are conventional links; ordinary browser behavior suffices. | `src/components/cdn_gallery.rs:1341–1367` |
| Native audio/video play, pause, seek, volume, fullscreen | Skip | — | Browser-standard controls. Starting one player pauses the others, evident when it happens. | `src/components/cdn_gallery.rs:1368–1390`; `src/main.rs:41–44` |
| Gallery pending/upload/retry states | Existing copy | — | Inline status already names pending, failed and retrying sources. | `src/components/cdn_gallery.rs:216,1019–1029` |

### Logs, metadata, and shared text copying

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Log-search field | Explain | Searches the full log; resubmit to refresh | Search submits the full log and freezes live updates; resubmitting refreshes. Also its title. | `src/components/text_stream.rs:214–246,352–361` |
| Log Search / Clear buttons | Skip | — | Explicit action words; the field’s single hint supplies the hidden search scope. | `src/components/text_stream.rs:246–255` |
| Log run tabs | Skip | — | Named tabs indicate their content; retain full-name reveals. | `src/components/text_stream.rs:260–280` |
| Live log scrolling | Conditional | Scroll up pauses; bottom resumes following | Hidden follow behavior matters while watching live output. Only for a live run with no search applied. | `src/components/text_stream/viewport.rs:171–191` |
| Log line count and matching-line count | Skip | — | Units and matching state are visible text. | `src/components/text_stream.rs:202–212` |
| Log-line elapsed-time tooltip | Skip | — | Existing value reveal is sufficient; no explanation of timestamp arithmetic or server implementation. | `src/components/text_stream.rs:540–551` |
| Metadata value | Explain | Click a value to copy | A table cell does not look interactive. One note on the metadata table, not on each value; each value’s title is its displayed text. | `src/components/metadata_viewer.rs:277–289`; `src/components/copy_text.rs:31–70` |
| Metadata table showing only differing keys | Conditional | Showing only keys that differ across runs | Diff only is on by default and the table does not say that it hides keys equal across runs; only the editor’s checkbox does. A second line of the table’s note, with Diff only on and two or more runs. | `src/components/metadata_viewer.rs:99–112` |
| Metadata branch chevrons | Skip | — | Standard disclosure; the button’s accessible name is the key itself, with an expanded state. | `src/components/metadata_viewer.rs:236–252` |
| Metadata cell tint | Conditional | Highlights differences across runs | Colored background encodes comparison, not decoration. Shown only with two or more runs and a tinted cell. | `src/components/metadata_viewer.rs:211–222,325–345` |
| Metadata Key / run-column headings | Skip | — | Content labels and run names already identify table dimensions. | `src/components/metadata_viewer.rs:115–125` |
| Blank metadata cell | Conditional | Blank: run lacks this key | A blank leaf cell can read as an empty value or a failure. On the blank cell, under the tint’s line where it is tinted; a branch row’s blanks carry none (the rows under it say which keys a run lacks). The “No metadata” and “No differing keys” messages cover empty tables. | `src/components/metadata_viewer.rs:99–108,284–289` |
| Ordinary text selection / browser Copy | Skip | — | Familiar; click-to-copy leaves it working. | `src/components/copy_text.rs:9–28,42–70` |

### Escape

Esc for the maximized chart, the image preview, a chart drag and inline renames is listed with those controls.

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Esc clears a non-empty filter or search (runs, projects, panels, metrics, logs) | Skip | — | A convention shared by every filter. | `src/components/sidebar/view.rs:344`; `src/pages/projects_page.rs:219`; `src/components/navbar.rs:104`; `src/components/binding_editor.rs:1293`; `src/components/text_stream.rs:238` |
| Esc closes the settings panel (focus inside it) | Skip | — | Same as Close, which is visible; edits are kept either way. | `src/components/options_panel.rs:59–65` |
| Esc cancels Trash selection | Skip | — | Same as the visible Cancel ×. | `src/components/sidebar.rs:460–464` |

### Color picker

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Palette swatch | Skip | — | The swatch shows the color it sets. | `src/components/color_picker.rs:45–63` |
| Custom color input | Skip | — | Visible “Custom” label and native picker fully express the action. | `src/components/color_picker.rs:66–80` |
| Color Reset | Skip | — | In a color dialog the target is clear; do not repeat “reset color.” | `src/components/color_picker.rs:82–94` |
| Color choice persistence | Explain | Saved in this browser | States the storage scope once, inside the dialog (a popover, see Interaction); no hint per swatch. | `src/components/color_picker.rs:16–17,57–60,74–78` |

### Application notices and recovery

| Item | Decision | Displayed explanation | Justification | Source |
| --- | --- | --- | --- | --- |
| Connecting / Reconnecting notice | Existing copy | — | The notice says what waits: “… charts are paused until then”. | `src/components/notice_bar.rs:56–68` |
| Unsupported-tab-version notice | Existing copy | — | The reload requirement belongs in the notice so it is visible without opening help. | `src/components/notice_bar.rs:47–54` |
| Server or chart-data failure notice | Existing copy | — | The notice carries the actual problem and diagnostic detail. | `src/components/notice_bar.rs:200–214` |
| Dismiss notice | Skip | — | The action is explicitly named. Its session-scoped bookkeeping does not require a tutorial. | `src/components/notice_bar.rs:227–237` |
| Delete them / Keep them in a cleanup question | Existing copy | — | The server-supplied question must identify the affected files and consequence in visible text. These answers need context, not a generic tooltip. | `src/components/notice_bar.rs:213–226` |
| Saved-layout error: Reload this tab | Existing copy | — | Existing text explains that reloading retries the saved layout with the current build. | `src/pages/settings_error.rs:30–46` |
| Saved-layout error: Reset layout | Existing copy | — | Existing text explains what is discarded and which settings remain. | `src/pages/settings_error.rs:30–60` |
| Saved-layout error: Back to projects | Skip | — | The destination is explicit. | `src/pages/settings_error.rs:62–65` |
| Run load: Retry | Skip | — | The preceding error identifies the failed load and the button names the recovery action. | `src/pages/run_page.rs:66–76` |
| In Trash / recovery-expired run notice | Existing copy | — | Lifecycle and remaining recovery time already appear beside the run. | `src/pages/run_page.rs:90–109` |
| View in Trash / View Trash link on a trashed run | Skip | — | The link names its destination. | `src/pages/run_page.rs:97–107` |

## Implementation

- An explained control carries its text in a `data-help` attribute, from the constants in `src/help.rs`; a replaced title is the same constant. `src/util/help.js` reads the attributes only while **?** is hovered, focused, or pinned, and keeps no listener or observer otherwise. It is an overlay: it docks nothing, changes no chart width, triggers no chart-data request, alters no selection, and stores nothing.
- A family is a text on one kind of element (its first class, else its tag), so the attribute goes on every row of a list and one note shows. Placement (`spots` and `arrange` in `src/util/help.js`) is one pass with no backtracking, and remembers nothing between scans but which families it has seen hidden since the notes came up: the note with the fewest spots left (counting, for one with none against its control, those of its nearest ring further off) chooses next and takes its cheapest, where a spot also costs what it takes from the notes still waiting (prohibitively, for a neighbour’s last spot), so in a row of icons no note takes its neighbour’s only place. Notes choose in tiers: a popover’s, the page’s, then those of controls only the pointer brings out (a family seen with a member rendered but not shown, now or earlier in this showing, so a page’s only chart counts too); every hidden explained control is weighed as if its note were there, which is what keeps the showing notes still, except under a maximized chart, where none will come out. The work is counted, roughly one per box compared, and past a fixed bound the remaining notes go to their corners (a dashboard uses under a tenth of it). A depth-first search over spots was rejected: it spent its budget on the last notes while the conflict was elsewhere, and moved distant notes when one control appeared.
- While notes show, the root element carries `data-kymo-help`, on which the stylesheet brings out every run row’s `…` button and draws the readout labels. A held mouse button adds `data-kymo-held`, which takes the labels off with the notes and leaves the dots: the first attribute matches every run row, and changing it at each press would restyle them all (a reviewer measured 10 ms in WebKit on 253 rows).
- `data-help-within` holds a selector the control must be inside for its text to show (`:focus` for the run filter, `.maximize-content` for the gallery step keys).
- A numeric chart’s state-dependent texts come from `src/components/uplot_chart/help.js`, which reads what `create.js` already keeps on the uPlot instance and three attributes the chart component renders. They join the chart container’s own text in its note (one another chart already shows is left out, as for any family), except the X-axis strip’s, which is on uPlot’s element for that axis: the script puts the text there at the first scan after a chart’s creation, so creation does no work for help. The readout’s labels (their row under Numeric charts) are the stylesheet’s work: the script leaves the texts on the readout, `hover.js` classes the gray column’s first populated cell by what it holds, and the labels are drawn out of flow, so the readout keeps its size and nothing more runs as the pointer moves. The gray label does not wrap: it can reach past the readout’s right edge, across the cursor line on a readout left of the pointer, and is cut off against the window’s edge. The copy and borrowed-point rows’ labels are the stylesheet’s too, above the readout’s box and below it (`hover.js` classes a grid that shows an `@`); each wraps to the readout’s width, since past it a label would lie on the pointer, and is lost where the readout touches the window’s top or bottom. The labels repeat the note’s look in their own rules, because the release bundle’s CSS minifier takes a label’s declarations when it shares the note’s rule, a shape `check_help.mjs` describes and refuses anywhere in the stylesheet.
- “Live” is a running or stuck run for charts and logs, and a run that has not ended for galleries.
- A note is a child of `<body>`, or of the open popover or modal dialog that holds its control, and is hidden from assistive technology. While help is pinned, one polite live region announces the explanation of the control that takes focus, of the explained element around it (a field’s row), or of those inside it (a run’s link), and no other.
- While notes show, the script rescans, at most once per frame, on a scroll of the page or of something that holds an explained control (and twice more, a frame apart, since run rows scrolled into view count as not rendered for up to two frames), a resize, the pointer crossing onto another element (not from run row to run row, where it brings out nothing and each crossing repaints the charts’ highlight), a press’s release (a pointerup, a native drag’s end, a context menu), a keyboard activation, focus moving or leaving, the pointer leaving the window, and a render of the app that adds or removes an explained control or changes an attribute a text depends on (`data-help`, `data-help-within`, the chart’s three) (one `MutationObserver` on the app’s root; chart readouts live outside it, and a live reply’s loading spinner does not count; such a render is scanned twice, a frame apart, since rows just added to the run list count as not rendered for a frame). A layout change none of these brings (a notice coming or going, a maximized chart catching up with a window resize) is not seen: the notes catch up at the next of the above.
- Cost: help adds nothing to repaint and no pass of its own to a chart’s render or a uPlot rebuild; while notes show, a rebuild replaces the X axis that carries a text, which brings two or three scans. To hover it adds a flag check per readout row and a class or two on the readout, about 1 µs per rebuild of a 200-row readout, and while notes show the readout labels add style and layout to each rebuild (0.1 to 0.4 ms for a 200-row readout in Chromium and WebKit; measured 2026-11-13 and 2026-11-15). A scan runs only while notes show, at most once per frame: on a 253-run project (11 charts; each run row carries four explained elements, its status glyph among them) it takes about 10 ms in headless Chromium, about two thirds of it reading the page (the hit tests and boxes of elements that may carry a note, and every explained element’s attributes), and 2 ms on a two-run dashboard, of which placement is about 0.3 ms.
- Checks: `scripts/check_help.mjs` (CI) covers the scripts against a stand-in page and holds every text to this document’s rows, the word limit and the no-period rule, and every title written as a help constant to its `data-help`; `tools/local-transport-qualification/help_fences.py` drives a real browser against a seeded local stack and is run by hand: at every step no note lies on another or on a shown explained button, at the largest font in a short and an 800×600 window too, and none moves as the pointer brings out a chart’s or a section’s actions.

## Source notes

`src/` and `assets/` paths are relative to `kymo-frontend/`; `proto/`, `docs/`, and `kymo-server/` to `kymo/`. Braced placeholders in quoted titles stand for the source’s template fields.

## Changelog

- 2026-11-23: a run page shows its run’s glyph right after its title (owner), except in Trash.
- 2026-11-23: **?** is an icon (Material Symbols “question_mark”) as tall as its neighbours; the text glyph had stood a third shorter, in a narrower box (owner).
- 2026-11-16: whole-feature review (six reviewers and four Claude lenses). A branch row’s blanks carry no note (one under a run’s non-empty object had said the run lacks the key). A note with no spot against its control chooses among the next ring’s spots before the others take them, and one with none anywhere takes the first place inside its control clear of the other notes (at the largest font, notes had overlapped in windows narrower than about 850 px). A kept box overhanging a spot by under a pixel no longer closes it (the run counter’s note had gone rows away at large fonts). Hidden controls under a maximized chart no longer steer notes. Two scans follow a scroll (run rows scrolled into view had lost their notes). The reset dot’s accessible name is its text. New checks: the fence’s placement, the X axis’s single write, a minifier shape with any pseudo-class, titles written as help constants paired with their `data-help`. A cleanup round followed (the same reviewers).
- 2026-11-16: published with the public export, so its mentions of unpublished docs gave way to what they said, and a project name to its size.
- 2026-11-15: the reset-zoom, shared-zoom, faint-line, shaded-band, hollow-circle and `×N` notes removed (owner); their rows are Skip, and a Skip row may now give a reason other than sufficiency. The copy shortcut’s and the `@` explanations moved from the chart’s note onto the hovered chart’s readout, shown only while it shows them (owner). **?** says “Press to keep help shown” (was “Press to keep notes shown or hide them”; owner). The `@` text is “@: nearest sample’s X” (was “… or bucket range”; owner). In release builds the readout labels had lost their look to the CSS minifier (Implementation); fixed, with a check on the stylesheet’s shape and the browser fence comparing every label’s look with a note’s.
- 2026-11-14: placement replaced by the best of eight independent designs (Implementation), after a depth-first search of mine and the one-at-a-time placement before it. On the scoring pages (five states at four window sizes, default and 24px fonts, Chromium and WebKit, and a 253-run project) no note is untied, overlaps another, lies on an explained button, or moves when the pointer brings out a chart’s or section’s buttons; one-at-a-time placement left 24 of 410 notes off their controls. A note with no room against its control gets a line back to it, and a family’s note may sit on a later, less crowded member (both the owner’s choices). General and cleanup reviews: a held press takes the readout labels off and leaves the run dots (`data-kymo-held`), and Escape does nothing during one; a press ends on pointerup, a native drag’s end or a context menu; a family seen hidden stays known as pointer-brought while notes show; focus leaving and the pointer leaving the window rescan; notes are hidden from assistive technology. A last review pass: the run filter now takes `#117` as it advertises; the header trash keeps “Select runs to move to Trash” as its accessible name; a scroll that moves nothing explained (a log following its tail) no longer scans. Reworded by the owner: “Collapse/expand section”, “#N: run number, shown when names repeat”, “Matches names, IDs, #numbers”, “Red circle: selected for Trash”, “Reset to parent value”, “Undo changes since opening”, “Edits apply immediately”, “Settings and layout saved in this browser only”, “Chart settings” for the chart’s gear in both its states (was “Maximizes chart, opens settings beside it” and “Configure chart”), the button “+ Add data source” (was “+ Add Source”), and one text for both defaults panels, “Most specific setting wins: chart, section, project”; the visibility circles’ titles are “Hide run” / “Show run” and the run `…` trigger’s is “More actions” (their accessible names keep the run’s name). A title no longer counts as an explanation (owner: almost nobody reads them): rows that leaned on one are Skip where the control is clear. Added after a last review for what was still unexplained: a note per run status glyph, “Blank: run lacks this key” and “Showing only keys that differ across runs” on metadata tables, “Only runs with an image at this step” on a gallery, and “Press to keep notes shown or hide them” on **?**; visible text for a still-logging run’s Delete, the two layout-deletion confirmations and the connecting notice; the chart’s settings panel is titled “Chart settings” (was “Configure Metric”); two chart texts now name their subject (“Hollow circles: …”, “@: …”) and the metadata table’s says “Click a value to copy”. The override chips say “Click to clear this override”. The Window field’s note gives its unit (“Smoothing width counts points, not steps”). The EMA field is labelled “Decay length” (was “Time constant (steps)”, wrong on time axes).
- 2026-11-13: third review, against the built code. The gray readout label follows the column’s first populated cell (a range had been labelled “Raw value”) and the labels sit inside the readout, just above the values, with ticks, adding no space; the run `…` dots show on every row while notes show, a note left of its control has a tick too, and no note lands where hidden action buttons appear; the chart title’s note sits by the title text, notes may sit anywhere along their control and small controls are placed first, the X-axis strip has its own note and the panel filter says “Search”, and “Hover to highlight across charts” is gone; “Delete runs”, “Collapse sections” and the run menu’s “Delete” (was “Trash”) and its text reworded; a tick points at the middle of any icon-sized control; the `×N` text no longer says “in this view”; stale sentences corrected, and the rule for which icon-only controls are explained stated (rule 8).
- 2026-11-13: **?** also shows the titles of the header trash (now “Delete runs”), reset-layout icon (now “Reset layout”), chart maximize icon, run `…` trigger, collapse/expand icon (“all” dropped), the project gear, the section and maximized-chart gears (now “Configure section” and “Configure chart”), the section’s add-panel icon, edited dot, Revert and section grip; the visibility text became “Click (drag) to show/hide runs”; the range-band text was reworded and the hover readout’s colored and gray values are labelled on the readout itself; texts lost their closing periods.
- 2026-11-13: implemented, then reviewed twice. Readout rows are explained on the chart; notes hide during a held press; the focused control’s text is announced while pinned. Implementation boundaries merged into Implementation and Acceptance criteria removed (each repeated Interaction, a content rule or a row).
- 2026-11-13: design settled over two reviews against source. A native `title` that explains its control is the explanation, and a replacing text becomes the title; the preview is instant, the pin a press toggle with no close button, and Escape dismisses it last; no annotation cap, priority order, help markers, or narrow-screen mode; Projects and Trash have no **?**. False or unbuildable rows were corrected, **Label only** merged into **Existing copy**, and the Purpose table, duplicate rows and repeated rules removed.
