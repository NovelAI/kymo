// A late rejection must not retry over a newer call.
const attempt = {};
window.__kymo_textCopyAttempt = attempt;

const fallback = () => {
  const active = document.activeElement;
  const selection = window.getSelection();
  const ranges = selection ? Array.from({length: selection.rangeCount}, (_, i) => selection.getRangeAt(i).cloneRange()) : [];
  const area = document.createElement('textarea');
  // WebKit requires a nonempty selection even when the copy event supplies an empty string.
  area.value = text || ' ';
  area.readOnly = true;
  area.style.cssText = 'position:fixed;top:0;left:0;opacity:0';
  let wrote = false;
  // Supply the original string even when the textarea normalizes line endings.
  area.addEventListener('copy', event => {
    if (event.clipboardData) {
      event.clipboardData.setData('text/plain', text);
      event.preventDefault();
      wrote = true;
    }
  });
  document.body.appendChild(area);
  try {
    area.focus({preventScroll: true});
    area.select();
    area.setSelectionRange(0, area.value.length);
    return document.execCommand('copy') && wrote;
  } finally {
    area.remove();
    if (active?.isConnected) active.focus({preventScroll: true});
    if (selection) {
      selection.removeAllRanges();
      for (const range of ranges) selection.addRange(range);
    }
  }
};
let ok = false;
try {
  if (window.isSecureContext && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    ok = true;
  }
} catch (_) {}
if (!ok && window.__kymo_textCopyAttempt === attempt) {
  try { ok = fallback(); } catch (_) {}
}
dioxus.send(window.__kymo_textCopyAttempt === attempt ? ok : null);
