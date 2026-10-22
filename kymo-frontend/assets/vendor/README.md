# Vendored dashboard assets

These files are bundled into both hosted and local frontend builds so the local dashboard performs no runtime internet requests.

- `uPlot.*` is upstream `leeoniya/uPlot` master at commit `e179494eaba3e62400a2e1beeeea5aaffb6dfdaa`, unreleased (its package.json still says 1.6.32); its `dist/` matches a fresh `npm run build`. Known limit, accepted until upstream fixes it: an axis whose bounds reach 1e17 in magnitude draws no ticks or labels.
- The Source Sans Pro 400/600 and Source Code Pro 400 files are the exact Google Fonts artifacts previously loaded by `main.rs` at runtime.
- The theme toggle's sun and moon in `src/components/icons.rs` are Twemoji graphics; `LICENSE-twemoji.md` carries their attribution.
- The other icon paths in `src/components/icons.rs`, and the checkbox checkmark mask in `assets/kymo.css`, come from Bootstrap Icons (`LICENSE-bootstrap-icons.txt`) and Material Icons (`LICENSE-material-icons.txt`).

Keep the adjacent upstream license files when updating any asset.

To update uPlot, copy `dist/uPlot.iife.min.js` and `dist/uPlot.min.css` from an upstream commit, then update the recorded commit.
kymo's highlight relies on uPlot drawing a series or band group with a `_focus` member last.
