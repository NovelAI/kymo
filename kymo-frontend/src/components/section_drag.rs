use dioxus::prelude::*;
use dioxus::web::WebEventExt;
use wasm_bindgen::JsCast;

use crate::state::section_order::SectionGap;
use crate::state::DashboardState;

#[derive(Clone)]
struct DragSession {
    source: String,
    // The full section order at dragstart, including sections the panel
    // filter hides, so a drop beside a visible section keeps hidden
    // neighbours in place. If discovery or another tab changes the order,
    // the edit funnel validates these IDs against fresh storage instead of
    // silently moving to a different gap.
    ids: Vec<String>,
}

impl DragSession {
    fn gap(&self, index: usize, after: bool) -> SectionGap {
        let boundary = index + usize::from(after);
        SectionGap {
            predecessor: self.ids[..boundary]
                .iter()
                .rev()
                .find(|id| **id != self.source)
                .cloned(),
            successor: self.ids[boundary..]
                .iter()
                .find(|id| **id != self.source)
                .cloned(),
        }
    }

    /// The one gap a visible boundary means: directly after the section
    /// above it, or before the first section at the top. `visible` is the
    /// rendered order and `boundary` counts the sections above the pointer.
    /// Boundaries touching the source, drops that would not move it, and
    /// sections unknown to this session have no gap.
    fn boundary_gap(&self, visible: &[String], boundary: usize) -> Option<SectionGap> {
        let above = boundary.checked_sub(1).and_then(|index| visible.get(index));
        let below = visible.get(boundary);
        if above == Some(&self.source) || below == Some(&self.source) {
            return None;
        }
        let position = |id: &String| self.ids.iter().position(|known| known == id);
        let gap = match (above, below) {
            (Some(above), _) => self.gap(position(above)?, true),
            (None, Some(below)) => self.gap(position(below)?, false),
            (None, None) => return None,
        };
        let source = self.ids.iter().position(|id| *id == self.source)?;
        (gap != self.gap(source, false)).then_some(gap)
    }
}

/// Rendered section IDs in order, and how many lie above `y` by midpoint.
fn visible_boundary(list: &web_sys::Element, y: f64) -> (Vec<String>, usize) {
    let mut visible = Vec::new();
    let mut boundary = 0;
    if let Ok(sections) = list.query_selector_all(".section[data-section-id]") {
        for index in 0..sections.length() {
            let Some(section) = sections
                .item(index)
                .and_then(|node| node.dyn_into::<web_sys::Element>().ok())
            else {
                continue;
            };
            let rect = section.get_bounding_client_rect();
            if y >= rect.top() + rect.height() / 2.0 {
                boundary = visible.len() + 1;
            }
            visible.push(section.get_attribute("data-section-id").unwrap_or_default());
        }
    }
    (visible, boundary)
}

/// Marks this page's section drags. Only drags carrying it are accepted,
/// and a private type keeps text fields from accepting the drop.
const DRAG_TYPE: &str = "application/x-kymo-section";

fn carries_section(event: &DragEvent) -> bool {
    event
        .data()
        .try_as_web_event()
        .and_then(|web_event| web_event.data_transfer())
        .is_some_and(|transfer| transfer.types().includes(&DRAG_TYPE.into(), 0))
}

/// The gap a drop would land in, and the section edge that draws it.
type Marker = (SectionGap, (String, bool));

/// Title-bar reordering. Native drag-and-drop supplies scrolling and
/// cancellation without a global pointer bridge; identity and allowed targets
/// come only from this page's session.
#[derive(Clone, Copy)]
pub struct SectionDrag {
    session: Signal<Option<DragSession>>,
    marker: Signal<Option<Marker>>,
}

impl SectionDrag {
    pub fn provide() -> Self {
        use_context_provider(|| SectionDrag {
            session: Signal::new(None),
            marker: Signal::new(None),
        })
    }

    pub fn is_source(&self, id: &str) -> bool {
        self.session
            .read()
            .as_ref()
            .is_some_and(|session| session.source == id)
    }

    /// The (before, after) edge of `id` that draws the insertion marker.
    /// Each gap has one canonical edge: after the section above, or before
    /// the first section.
    pub fn marker_on(&self, id: &str) -> (bool, bool) {
        match self.marker.read().as_ref() {
            Some((_, (target, after))) if target == id => (!after, *after),
            _ => (false, false),
        }
    }

    pub fn start(&self, event: &DragEvent, source: String, ids: Vec<String>) {
        let transfer = event.data_transfer();
        // Firefox starts a drag only with data; identity comes from the session.
        if transfer.set_data(DRAG_TYPE, "section").is_err() {
            event.prevent_default();
            return;
        }
        transfer.set_effect_allowed("move");
        // Drag the whole title bar rather than the small handle.
        if let Some(web_event) = event.data().try_as_web_event() {
            let header = web_event
                .target()
                .and_then(|target| target.dyn_into::<web_sys::Element>().ok())
                .and_then(|handle| handle.closest(".section-header").ok().flatten());
            if let (Some(header), Some(transfer)) = (header, web_event.data_transfer()) {
                let rect = header.get_bounding_client_rect();
                let x = f64::from(web_event.client_x()) - rect.left();
                let y = f64::from(web_event.client_y()) - rect.top();
                transfer.set_drag_image(&header, x as i32, y as i32);
            }
        }
        let (mut session, mut marker) = (self.session, self.marker);
        session.set(Some(DragSession { source, ids }));
        marker.set(None);
    }

    fn gap_at(&self, event: &DragEvent, list: &web_sys::Element) -> Option<Marker> {
        let session = self.session.peek().clone()?;
        let (visible, boundary) = visible_boundary(list, event.client_coordinates().y);
        let gap = session.boundary_gap(&visible, boundary)?;
        let edge = match boundary {
            0 => (visible[0].clone(), false),
            _ => (visible[boundary - 1].clone(), true),
        };
        Some((gap, edge))
    }

    /// Handles dragenter too: WebKit makes the list the drop target only when
    /// dragenter is cancelled.
    pub fn over(&self, event: &DragEvent, list: Option<&web_sys::Element>) {
        if self.session.peek().is_none() || !carries_section(event) {
            return;
        }
        event.prevent_default();
        event.data_transfer().set_drop_effect("move");
        let next = list.and_then(|list| self.gap_at(event, list));
        let mut marker = self.marker;
        if *marker.peek() != next {
            marker.set(next);
        }
    }

    /// Clears the marker once the pointer leaves the list, not when it
    /// crosses between the list's children.
    pub fn leave(&self, event: &DragEvent, list: Option<&web_sys::Element>) {
        let inside = event
            .data()
            .try_as_web_event()
            .and_then(|web_event| web_event.related_target())
            .and_then(|related| related.dyn_into::<web_sys::Node>().ok())
            .is_some_and(|node| list.is_some_and(|list| list.contains(Some(&node))));
        let mut marker = self.marker;
        if !inside && marker.peek().is_some() {
            marker.set(None);
        }
    }

    /// Lands where the marker showed; no marker means no move.
    pub fn drop(&self, state: DashboardState, event: &DragEvent) {
        if !carries_section(event) {
            return;
        }
        let Some(session) = self.session.peek().clone() else {
            return;
        };
        event.prevent_default();
        let gap = self.marker.peek().as_ref().map(|(gap, _)| gap.clone());
        // dragend also fires after a successful drop. Clear first so it
        // cannot publish a second cancellation.
        self.clear();
        match gap {
            Some(gap) => state.reorder_section(session.source, gap),
            None => state.cancel_section_reorder(),
        }
    }

    pub fn end(&self, state: DashboardState) {
        if self.session.peek().is_some() {
            self.clear();
            state.cancel_section_reorder();
        }
    }

    fn clear(&self) {
        let (mut session, mut marker) = (self.session, self.marker);
        session.set(None);
        marker.set(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(source: &str, ids: &[&str]) -> DragSession {
        DragSession {
            source: source.to_string(),
            ids: ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    #[test]
    fn gap_uses_typed_ends_and_excludes_the_source() {
        let session = session("B", &["", "B", "C"]);
        assert_eq!(
            session.gap(0, false),
            SectionGap {
                predecessor: None,
                successor: Some("".to_string()),
            }
        );
        assert_eq!(session.gap(1, false), session.gap(1, true));
        assert_eq!(session.gap(0, true), session.gap(1, false));
        assert_eq!(session.gap(2, false), session.gap(1, false));
        assert_ne!(session.gap(0, false), session.gap(1, false));
        assert_ne!(session.gap(2, true), session.gap(1, false));
        assert_eq!(
            session.gap(1, true),
            SectionGap {
                predecessor: Some("".to_string()),
                successor: Some("C".to_string()),
            }
        );
        assert_eq!(
            session.gap(2, true),
            SectionGap {
                predecessor: Some("C".to_string()),
                successor: None,
            }
        );
    }

    fn ids(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| id.to_string()).collect()
    }

    fn after(id: &str, successor: Option<&str>) -> Option<SectionGap> {
        Some(SectionGap {
            predecessor: Some(id.to_string()),
            successor: successor.map(str::to_string),
        })
    }

    #[test]
    fn each_visible_boundary_has_one_gap_and_neighbours_of_the_source_have_none() {
        let session = session("B", &["A", "B", "C", "D"]);
        let visible = ids(&["A", "B", "C", "D"]);
        assert_eq!(
            session.boundary_gap(&visible, 0),
            Some(SectionGap {
                predecessor: None,
                successor: Some("A".to_string()),
            })
        );
        assert_eq!(session.boundary_gap(&visible, 1), None);
        assert_eq!(session.boundary_gap(&visible, 2), None);
        assert_eq!(session.boundary_gap(&visible, 3), after("C", Some("D")));
        assert_eq!(session.boundary_gap(&visible, 4), after("D", None));
        assert_eq!(session.boundary_gap(&[], 0), None);
    }

    #[test]
    fn filtered_boundaries_mean_directly_after_the_section_above() {
        // H1 and H2 are hidden by the panel filter.
        let session = session("D", &["H1", "A", "H2", "B", "D"]);
        let visible = ids(&["A", "B", "D"]);
        assert_eq!(
            session.boundary_gap(&visible, 0),
            Some(SectionGap {
                predecessor: Some("H1".to_string()),
                successor: Some("A".to_string()),
            })
        );
        assert_eq!(session.boundary_gap(&visible, 1), after("A", Some("H2")));
        // Beside the source: never an invisible move past a hidden section.
        assert_eq!(session.boundary_gap(&visible, 2), None);
        assert_eq!(session.boundary_gap(&visible, 3), None);
    }

    #[test]
    fn a_boundary_that_would_not_move_the_source_has_no_gap() {
        // The source is elsewhere; only the resulting gap decides.
        let moving = session("B", &["A", "H", "B", "C"]);
        assert_eq!(
            moving.boundary_gap(&ids(&["A", "C"]), 1),
            after("A", Some("H"))
        );
        let unmoved = session("B", &["A", "B", "H", "C"]);
        assert_eq!(unmoved.boundary_gap(&ids(&["A", "C"]), 1), None);
    }

    #[test]
    fn unknown_sections_have_no_gap() {
        let session = session("B", &["A", "B"]);
        assert_eq!(session.boundary_gap(&ids(&["A", "B", "E"]), 3), None);
    }
}
