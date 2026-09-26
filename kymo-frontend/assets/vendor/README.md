# Vendored dashboard assets

These files are bundled into both hosted and local frontend builds so the local dashboard performs no runtime internet requests.

- `uPlot.*` is the existing focus-order patch from `ad8e/uPlot` at commit `1316f1ffd345ed8f7b5f834fa111475d1ad3839f`.
- The Source Sans Pro 400/600 and Source Code Pro 400 files are the exact Google Fonts artifacts previously loaded by `main.rs` at runtime.
- The theme toggle's sun and moon in `src/components/icons.rs` are Twemoji graphics; `LICENSE-twemoji.md` carries their attribution.
- The other icon paths in `src/components/icons.rs`, and the checkbox checkmark mask in `assets/kymo.css`, come from Bootstrap Icons (`LICENSE-bootstrap-icons.txt`) and Material Icons (`LICENSE-material-icons.txt`).

Keep the adjacent upstream license files when updating any asset.

To rebuild the uPlot patch, edit the fork's source and run `npm run build`.
Replace `uPlot.iife.min.js` and `uPlot.min.css` here, then update the recorded commit.
An upstream release can replace the fork once it preserves drawing `_focus` series last.
