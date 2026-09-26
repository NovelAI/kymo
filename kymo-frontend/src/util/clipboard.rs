use dioxus::prelude::*;

use super::js_bridge::js_string;

/// Copies within the user gesture: the script starts synchronously, and its channel closes when the script ends, so no caller needs its reply.
pub(crate) fn write_text(text: &str) {
    let js = format!(
        "const text = {};\n{}",
        js_string(text),
        include_str!("clipboard.js")
    );
    document::eval(&js);
}
