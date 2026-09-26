//! Fixed-row request planning, restoration decisions, and DOM measurement.

use dioxus::prelude::*;
use wasm_bindgen::{closure::Closure, JsCast};

pub(super) const DEFAULT_LINE_HEIGHT_PX: u32 = 16;
const OVERSCAN_LINES: u64 = 80;

/// The server window covering a viewport, its start quantized so scrolling refetches only after crossing an overscan band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct VirtualWindow {
    pub(super) offset: u64,
    pub(super) limit: u32,
}

impl VirtualWindow {
    fn for_viewport(scroll_top: f64, client_height: u32, line_height_px: u32) -> Self {
        let visible_start = (scroll_top.max(0.0) / f64::from(line_height_px)).floor() as u64;
        let visible_lines =
            u64::from(client_height.max(line_height_px)).div_ceil(u64::from(line_height_px));
        let page_start = (visible_start / OVERSCAN_LINES) * OVERSCAN_LINES;
        let offset = page_start.saturating_sub(OVERSCAN_LINES);
        // One band above, the current band, and one below. Include the full
        // viewport in case a user makes a log panel unusually tall.
        let limit = visible_lines
            .saturating_add(OVERSCAN_LINES * 3)
            .min(u64::from(u32::MAX)) as u32;
        Self { offset, limit }
    }
}

#[derive(Clone, Copy)]
pub(super) struct MeasuredViewport {
    pub(super) scroll_height: i32,
    pub(super) client_height: i32,
}

impl MeasuredViewport {
    fn max_top(self) -> i32 {
        self.scroll_height.saturating_sub(self.client_height).max(0)
    }
}

/// A remembered reading position: a logical top row, or the end of a followed stream.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum ScrollAnchor {
    Line(f64),
    End,
}

/// A log's reading position; it ignores DOM scroll events until a covering window is placed, including across hidden layouts, cleared data, and transient retries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct ScrollViewport {
    // Fractional row coordinate survives font changes.
    line: f64,
    // Scrollbar-independent request height; only resizing writes it.
    height: u32,
    restoring: bool,
    // Pinned to the end: installs move `line` with the stream's growth, and placement targets the measured bottom.
    following: bool,
    // Stream length `line` was last planned against.
    total_lines: Option<u64>,
}

impl ScrollViewport {
    pub(super) fn new(anchor: ScrollAnchor) -> Self {
        let (line, following) = match anchor {
            ScrollAnchor::Line(line) => (line, false),
            // The first response's length locates the end.
            ScrollAnchor::End => (0.0, true),
        };
        Self {
            line,
            height: 0,
            restoring: true,
            following,
            total_lines: None,
        }
    }

    pub(super) fn anchor(self) -> ScrollAnchor {
        if self.following {
            ScrollAnchor::End
        } else {
            ScrollAnchor::Line(self.line)
        }
    }

    pub(super) fn window(self, line_height: u32) -> Option<VirtualWindow> {
        (self.height > 0).then(|| {
            VirtualWindow::for_viewport(self.scroll_top(line_height), self.height, line_height)
        })
    }

    /// Rendered rows still need a scroll position: a pending restore, or the bottom of a followed stream.
    pub(super) fn placing(self) -> bool {
        self.restoring || self.following
    }

    pub(super) fn installing(&mut self, total_lines: u64, line_height: u32) {
        if self.following {
            // Keep the distance from the end; a first length, or a log that still fit its box (line 0), estimates it from the box height.
            self.line = match self.total_lines {
                Some(previous) if self.line > 0.0 => {
                    (self.line + total_lines as f64 - previous as f64).max(0.0)
                }
                _ => self.end_line(total_lines as f64 * f64::from(line_height), line_height),
            };
        } else if self.line >= total_lines as f64 {
            self.discard_anchor();
        }
        self.total_lines = Some(total_lines);
    }

    /// Terminal errors clear the rows; the anchor waits for a later push.
    pub(super) fn cleared(&mut self) {
        self.restoring = true;
    }

    pub(super) fn discard_anchor(&mut self) {
        self.line = 0.0;
        self.restoring = true;
        self.following = false;
    }

    pub(super) fn restore_target(
        &mut self,
        rendered: VirtualWindow,
        measured: MeasuredViewport,
        line_height: u32,
    ) -> Option<f64> {
        if !self.placing()
            || measured.client_height <= 0
            || self.window(line_height) != Some(rendered)
        {
            return None;
        }
        let max_top = f64::from(measured.max_top());
        let top = if self.following {
            self.line = self.end_line(f64::from(measured.scroll_height), line_height);
            max_top
        } else {
            // A taller viewport can move an in-range anchor into an earlier band.
            // Match integer DOM scrollTop before checking that band's rows.
            let top = self.scroll_top(line_height).min(max_top).round();
            self.line = top / f64::from(line_height);
            top
        };
        (self.window(line_height) == Some(rendered)).then(|| {
            self.restoring = false;
            top
        })
    }

    fn scroll_top(self, line_height: u32) -> f64 {
        self.line * f64::from(line_height)
    }

    fn end_line(self, content_px: f64, line_height: u32) -> f64 {
        ((content_px - f64::from(self.height)) / f64::from(line_height)).max(0.0)
    }

    fn resized(&mut self, height: u32) {
        self.height = height;
        if height == 0 {
            self.restoring = true;
        }
    }

    /// Within half a row of the bottom, a live stream starts following; any position above it stops.
    pub(super) fn scrolled(
        &mut self,
        top: f64,
        measured: MeasuredViewport,
        line_height: u32,
        live: bool,
    ) {
        if self.restoring || !top.is_finite() {
            return;
        }
        if f64::from(measured.max_top()) - top > f64::from(line_height) / 2.0 {
            self.following = false;
        } else if live {
            self.following = true;
        }
        self.line = if self.following {
            self.end_line(f64::from(measured.scroll_height), line_height)
        } else {
            top.max(0.0) / f64::from(line_height)
        };
    }
}

// Dioxus 0.7.9's onresize does not unobserve on component removal.
pub(super) struct TextResizeObserver {
    observer: web_sys::ResizeObserver,
    _callback: Closure<dyn FnMut()>,
}

impl TextResizeObserver {
    pub(super) fn new(
        element: &web_sys::HtmlElement,
        mut viewport: Signal<ScrollViewport>,
    ) -> Self {
        let target = element.clone();
        let callback = Closure::<dyn FnMut()>::new(move || {
            if !target.is_connected() {
                return;
            }
            // clientHeight changes when a horizontal scrollbar appears.
            let height = target.offset_height().max(0) as u32;
            if viewport.peek().height != height {
                viewport.write().resized(height);
            }
        });
        let observer = web_sys::ResizeObserver::new(callback.as_ref().unchecked_ref())
            .expect("browser supports ResizeObserver");
        observer.observe(element);
        Self {
            observer,
            _callback: callback,
        }
    }
}

impl Drop for TextResizeObserver {
    fn drop(&mut self) {
        self.observer.disconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::FontSize;

    fn at(line: f64, height: u32) -> ScrollViewport {
        let mut viewport = ScrollViewport::new(ScrollAnchor::Line(line));
        viewport.resized(height);
        viewport
    }

    fn settled(line: f64, height: u32) -> ScrollViewport {
        ScrollViewport {
            restoring: false,
            ..at(line, height)
        }
    }

    fn measured(scroll_height: i32, client_height: i32) -> MeasuredViewport {
        MeasuredViewport {
            scroll_height,
            client_height,
        }
    }

    // A manual scroll far above the bottom of a finished stream.
    fn scroll_mid(viewport: &mut ScrollViewport, top: f64, line_height: u32) {
        viewport.scrolled(top, measured(i32::MAX, 216), line_height, false);
    }

    #[test]
    fn unusable_anchor_resets_to_start_for_rejection_or_shortened_stream() {
        for total in [0, 1, 79, 80, 160, 1000] {
            let mut viewport = settled(1000.25, 320);
            viewport.installing(total, 16);
            assert_eq!(viewport.line, 0.0);
            assert_eq!(viewport.window(16).unwrap().offset, 0);
            assert!(viewport.restoring);
        }
        let mut rejected = settled(1000.25, 320);
        rejected.discard_anchor();
        assert!(rejected.restoring);
        let at_start = rejected;
        rejected.discard_anchor();
        assert_eq!(
            rejected, at_start,
            "rejection at zero must not refetch in a loop"
        );
        assert_eq!(rejected.line, 0.0);
    }

    #[test]
    fn installing_rearms_on_clear_and_preserves_state_for_usable_rows() {
        for total in [101, 200, 300] {
            let mut viewport = settled(100.5, 320);
            viewport.installing(total, 16);
            assert!(!viewport.restoring);
            assert_eq!(viewport.line, 100.5);
        }
        let mut cleared = settled(100.5, 320);
        cleared.cleared();
        assert!(cleared.restoring);
        assert_eq!(cleared.line, 100.5);
        let mut pending = ScrollViewport::new(ScrollAnchor::Line(100.5));
        pending.installing(200, 16);
        assert!(pending.restoring, "an initial restore must stay pending");
        let mut boundary = ScrollViewport::new(ScrollAnchor::Line(100.0));
        boundary.installing(100, 16);
        assert_eq!(boundary.line, 0.0, "line indices are zero-based");
    }

    #[test]
    fn stale_hidden_or_settled_views_cannot_change_the_anchor() {
        let mut viewport = at(4173.25, 320);
        let window = viewport.window(16).unwrap();
        let stale = VirtualWindow::for_viewport(0.0, 320, 16);
        let before = viewport;
        assert_eq!(viewport.restore_target(stale, measured(320, 320), 16), None);
        assert_eq!(
            viewport, before,
            "stale data must not clamp the saved row to zero"
        );
        assert_eq!(viewport.restore_target(window, measured(320, 0), 16), None);
        assert_eq!(viewport, before);

        viewport.resized(0);
        let hidden = viewport;
        assert_eq!(
            viewport.restore_target(window, measured(320, 320), 16),
            None
        );
        assert_eq!(viewport, hidden);

        viewport = settled(viewport.line, 320);
        let settled = viewport;
        assert_eq!(
            viewport.restore_target(window, measured(320, 320), 16),
            None
        );
        assert_eq!(viewport, settled);
    }

    #[test]
    fn cross_band_clamp_waits_for_matching_rows() {
        let mut viewport = at(230.25, 3200);
        let old = viewport.window(16).unwrap();
        assert_eq!(viewport.restore_target(old, measured(3848, 3200), 16), None);
        assert_eq!(viewport.line, 40.5);
        assert!(viewport.restoring);
        scroll_mid(&mut viewport, 0.0, 16);

        let tail = viewport.window(16).unwrap();
        assert_ne!(tail, old);
        assert_eq!(
            viewport.restore_target(tail, measured(3848, 3200), 16),
            Some(648.0)
        );
        assert!(!viewport.restoring);
    }

    #[test]
    fn scroll_restore_ignores_initial_zero_and_then_accepts_manual_scrolls() {
        let saved_top = 417.0 * f64::from(DEFAULT_LINE_HEIGHT_PX) + 3.0;
        let mut viewport = ScrollViewport::new(ScrollAnchor::Line(
            saved_top / f64::from(DEFAULT_LINE_HEIGHT_PX),
        ));
        assert_eq!(viewport.window(DEFAULT_LINE_HEIGHT_PX), None);
        viewport.resized(216);
        let saved_window = viewport.window(DEFAULT_LINE_HEIGHT_PX).unwrap();
        assert!(saved_window.offset > 0);

        scroll_mid(&mut viewport, 0.0, DEFAULT_LINE_HEIGHT_PX);
        assert_eq!(viewport.scroll_top(DEFAULT_LINE_HEIGHT_PX), saved_top);
        assert_eq!(viewport.window(DEFAULT_LINE_HEIGHT_PX), Some(saved_window));

        viewport.resized(232);
        assert_eq!(
            viewport.restore_target(
                viewport.window(DEFAULT_LINE_HEIGHT_PX).unwrap(),
                measured(1_000_000, 232),
                DEFAULT_LINE_HEIGHT_PX,
            ),
            Some(saved_top)
        );
        assert!(!viewport.restoring);
        assert_eq!(viewport.height, 232);
        scroll_mid(
            &mut viewport,
            800.0 * f64::from(DEFAULT_LINE_HEIGHT_PX),
            DEFAULT_LINE_HEIGHT_PX,
        );
        assert!(viewport.window(DEFAULT_LINE_HEIGHT_PX).unwrap().offset > saved_window.offset);
        scroll_mid(&mut viewport, 0.0, DEFAULT_LINE_HEIGHT_PX);
        assert_eq!(viewport.window(DEFAULT_LINE_HEIGHT_PX).unwrap().offset, 0);
    }

    #[test]
    fn pixel_round_trip_does_not_drift_on_repeated_restoration() {
        let mut viewport = at(53.0 / 19.0, 216);
        for _ in 0..5 {
            viewport.cleared();
            let window = viewport.window(19).unwrap();
            let top = viewport.restore_target(window, measured(19_008, 216), 19);
            assert_eq!(top, Some(53.0));
            assert!(!viewport.restoring);
        }
    }

    #[test]
    fn logical_line_survives_every_font_size_without_changing_reading_location() {
        let line = 4173.0 + 5.0 / 17.0;
        for pixels in FontSize::MIN..=FontSize::MAX {
            let line_height = FontSize::new(pixels)
                .unwrap()
                .scale_px(DEFAULT_LINE_HEIGHT_PX);
            let mut viewport = at(line, 216);
            let window = viewport.window(line_height).unwrap();
            let Some(top) = viewport.restore_target(window, measured(1_000_216, 216), line_height)
            else {
                panic!("in-range row should restore immediately")
            };
            assert_eq!((top / f64::from(line_height)).floor(), 4173.0);
            assert!((top - line * f64::from(line_height)).abs() <= 0.5);
            let window = viewport.window(line_height).unwrap();
            assert!(window.offset <= 4173 && window.offset + u64::from(window.limit) > 4173);
        }
    }

    #[test]
    fn empty_stream_resets_before_growth_without_clamping_stale_data() {
        let mut viewport = at(100_000.0, 216);
        let stale = viewport.window(16).unwrap();
        viewport.installing(0, 16);
        assert_eq!(viewport.line, 0.0);
        assert_eq!(viewport.restore_target(stale, measured(216, 216), 16), None);
        scroll_mid(&mut viewport, 100_000.0, 16);

        let at_start = viewport.window(16).unwrap();
        assert_eq!(at_start.offset, 0);
        assert_eq!(
            viewport.restore_target(at_start, measured(216, 216), 16),
            Some(0.0)
        );
        viewport.installing(100, 16);
        assert_eq!(
            viewport.restore_target(at_start, measured(1608, 216), 16),
            None
        );
        assert_eq!(viewport.line, 0.0);
    }

    #[test]
    fn in_range_restore_uses_measured_extent_and_client_height() {
        let mut viewport = at(1500.0 / f64::from(DEFAULT_LINE_HEIGHT_PX), 232);
        // The browser supplies the exact extent including padding and the horizontal scrollbar.
        let window = viewport.window(DEFAULT_LINE_HEIGHT_PX).unwrap();
        assert_eq!(
            viewport.restore_target(window, measured(1592, 216), DEFAULT_LINE_HEIGHT_PX),
            Some(1376.0)
        );
        assert_eq!(viewport.scroll_top(DEFAULT_LINE_HEIGHT_PX), 1376.0);
        assert_eq!(viewport.height, 232);
        assert!(!viewport.restoring);

        let before = viewport;
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            scroll_mid(&mut viewport, invalid, DEFAULT_LINE_HEIGHT_PX);
            assert_eq!(viewport, before);
        }
        scroll_mid(&mut viewport, -1.0, DEFAULT_LINE_HEIGHT_PX);
        assert_eq!(viewport.line, 0.0);
    }

    #[test]
    fn zero_height_pauses_window_planning_and_preserves_the_anchor() {
        let mut viewport = settled(4173.25, 216);
        let original = viewport.window(16).unwrap();
        viewport.resized(0);
        assert_eq!(viewport.window(16), None);
        assert!(viewport.restoring);
        scroll_mid(&mut viewport, 0.0, 16);
        viewport.resized(800);
        let enlarged = viewport.window(16).unwrap();
        assert_eq!(enlarged.offset, original.offset);
        assert!(enlarged.limit > original.limit);
        assert_eq!(viewport.line, 4173.25);
    }

    #[test]
    fn virtual_window_covers_viewport_with_overscan() {
        let scroll_line = 417u64;
        let viewport_lines = 23u64;
        for pixels in FontSize::MIN..=FontSize::MAX {
            let font_size = FontSize::new(pixels).unwrap();
            let line_height = font_size.scale_px(DEFAULT_LINE_HEIGHT_PX);
            let window = VirtualWindow::for_viewport(
                (scroll_line * u64::from(line_height)) as f64,
                (viewport_lines * u64::from(line_height)) as u32,
                line_height,
            );

            assert!(window.offset <= scroll_line.saturating_sub(OVERSCAN_LINES));
            assert!(
                window.offset + u64::from(window.limit)
                    >= scroll_line + viewport_lines + OVERSCAN_LINES
            );
        }
    }

    #[test]
    fn virtual_window_is_stable_inside_a_scroll_band() {
        for pixels in FontSize::MIN..=FontSize::MAX {
            let font_size = FontSize::new(pixels).unwrap();
            let line_height = font_size.scale_px(DEFAULT_LINE_HEIGHT_PX);
            let at = |line: u64| {
                VirtualWindow::for_viewport(
                    (line * u64::from(line_height)) as f64,
                    line_height * 20,
                    line_height,
                )
            };

            assert_eq!(at(81), at(159));
            assert_ne!(at(159), at(160));
        }
    }

    // Rows plus the body's 8px of vertical padding, as the browser measures them.
    fn stream(total: u64, line_height: u32, client_height: i32) -> MeasuredViewport {
        measured(total as i32 * line_height as i32 + 8, client_height)
    }

    #[test]
    fn end_anchor_locates_the_tail_then_moves_with_growth() {
        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(320);
        let probe = viewport.window(16).unwrap();
        assert_eq!(probe.offset, 0, "the first request only learns the length");

        viewport.installing(8_000, 16);
        assert_eq!(viewport.line, 7_980.0);
        let tail = viewport.window(16).unwrap();
        assert_ne!(tail, probe);
        assert_eq!(
            viewport.restore_target(probe, stream(8_000, 16, 320), 16),
            None
        );
        let top = viewport.restore_target(tail, stream(8_000, 16, 320), 16);
        assert_eq!(top, Some(f64::from(stream(8_000, 16, 320).max_top())));
        assert!(!viewport.restoring);
        assert_eq!(viewport.anchor(), ScrollAnchor::End);

        viewport.installing(8_100, 16);
        let grown = viewport.window(16).unwrap();
        let top = viewport.restore_target(grown, stream(8_100, 16, 320), 16);
        assert_eq!(top, Some(f64::from(stream(8_100, 16, 320).max_top())));
    }

    #[test]
    fn a_followed_log_that_fit_its_box_re_estimates_its_end_on_growth() {
        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(320);
        viewport.installing(0, 16);
        viewport.installing(165, 16);
        let mut fresh = ScrollViewport::new(ScrollAnchor::End);
        fresh.resized(320);
        fresh.installing(165, 16);
        assert_eq!(viewport.line, fresh.line);
        assert_eq!(viewport.window(16), fresh.window(16));
    }

    #[test]
    fn band_edge_correction_converges_without_replanning_the_estimate() {
        // Padding can move an estimated tail across one band boundary.
        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(328);
        viewport.installing(820, 16);
        let estimated = viewport.window(16).unwrap();
        let measured_tail = stream(820, 16, 328);
        assert_eq!(viewport.restore_target(estimated, measured_tail, 16), None);
        let corrected = viewport.window(16).unwrap();
        assert_ne!(corrected, estimated);

        viewport.installing(820, 16);
        assert_eq!(
            viewport.window(16),
            Some(corrected),
            "an unchanged length keeps the measured tail"
        );
        assert_eq!(
            viewport.restore_target(corrected, measured_tail, 16),
            Some(f64::from(measured_tail.max_top()))
        );
    }

    #[test]
    fn following_stops_above_the_bottom_and_resumes_there_only_while_live() {
        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(320);
        viewport.installing(1_000, 16);
        let bottom = stream(1_000, 16, 320);
        let window = viewport.window(16).unwrap();
        let top = viewport.restore_target(window, bottom, 16).unwrap();

        viewport.scrolled(top - 7.0, bottom, 16, true);
        assert_eq!(viewport.anchor(), ScrollAnchor::End, "within half a row");
        viewport.scrolled(top - 9.0, bottom, 16, true);
        assert_eq!(viewport.anchor(), ScrollAnchor::Line((top - 9.0) / 16.0));
        assert!(!viewport.placing());

        viewport.installing(1_100, 16);
        assert_eq!(
            viewport.line,
            (top - 9.0) / 16.0,
            "an unfollowed view keeps its row"
        );
        viewport.scrolled(top, bottom, 16, false);
        assert!(
            !viewport.following,
            "a finished stream never starts following"
        );
        viewport.scrolled(top, bottom, 16, true);
        assert!(viewport.following);
    }

    #[test]
    fn rejection_and_errors_keep_following_bounded() {
        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(320);
        viewport.installing(5_000, 16);
        viewport.cleared();
        assert!(viewport.restoring);
        assert_eq!(
            viewport.anchor(),
            ScrollAnchor::End,
            "a terminal error keeps the end for a later push"
        );

        viewport.discard_anchor();
        assert_eq!(
            viewport.anchor(),
            ScrollAnchor::Line(0.0),
            "a rejected tail must not refetch in a loop"
        );
        viewport.installing(6_000, 16);
        assert_eq!(viewport.window(16).unwrap().offset, 0);
    }

    #[test]
    fn short_or_empty_followed_streams_use_the_first_window() {
        for total in [0, 1, 100, 159] {
            let mut viewport = ScrollViewport::new(ScrollAnchor::End);
            viewport.resized(320);
            let probe = viewport.window(16).unwrap();
            viewport.installing(total, 16);
            assert_eq!(viewport.window(16), Some(probe), "total {total}");
            let bottom = stream(total, 16, 320);
            assert_eq!(
                viewport.restore_target(probe, bottom, 16),
                Some(f64::from(bottom.max_top()))
            );
        }
    }

    #[test]
    fn followed_end_keeps_its_band_when_a_wide_row_adds_a_scrollbar() {
        // A wide row changes clientHeight enough to straddle the band boundary.
        let total = 8_012;
        let with_bar = stream(total, 17, 205);
        let without_bar = stream(total, 17, 213);
        let band = |measured: MeasuredViewport| (measured.max_top() / 17) / 80;
        assert_ne!(band(with_bar), band(without_bar));

        let mut viewport = ScrollViewport::new(ScrollAnchor::End);
        viewport.resized(213);
        viewport.installing(total, 17);
        let planned = viewport.window(17).unwrap();
        assert_eq!(planned.offset, 7_840);
        assert_eq!(
            viewport.restore_target(planned, with_bar, 17),
            Some(f64::from(with_bar.max_top()))
        );
        assert_eq!(viewport.window(17), Some(planned));
        viewport.scrolled(f64::from(with_bar.max_top()), with_bar, 17, true);
        assert_eq!(
            viewport.window(17),
            Some(planned),
            "the placement's scroll event"
        );
        assert_eq!(
            viewport.restore_target(planned, without_bar, 17),
            Some(f64::from(without_bar.max_top()))
        );
        assert_eq!(viewport.window(17), Some(planned));
        assert!(planned.offset + u64::from(planned.limit) >= total);
    }
}
