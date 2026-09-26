use crate::util::js_bridge::js_string;

const TEMPLATE: &str = include_str!("resize.js");

pub(super) fn build(start_y: f64, rect_id: &str) -> String {
    let rect_id = js_string(rect_id);
    TEMPLATE
        .replace("__START_Y__", &start_y.to_string())
        .replace("__RECT_ID__", &rect_id)
}

#[cfg(test)]
mod tests {
    use super::build;

    #[test]
    fn dynamic_rect_id_is_a_json_literal_escaped_again_for_css() {
        let rect_id = "loss\"']\\\n\u{2028}__START_Y__";
        let script = build(42.5, rect_id);
        let encoded = serde_json::to_string(rect_id).unwrap();

        assert!(script.contains("let sy=42.5;"));
        assert!(script.contains(&format!("let rid={encoded};")));
        assert!(script.contains(
            "document.querySelector('.rect-resize-handle[data-rect-id=\"'+CSS.escape(rid)+'\"]')"
        ));
    }
}
