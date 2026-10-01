// Keeps data-overflow on each .fade-overflow element while its content is wider than its box (kymo.css fades the text only then). Elements are tracked from insertion to removal, children included, so text that changes in place re-marks its box.
(() => {
  if (window.__kymo_overflowFade) return;
  window.__kymo_overflowFade = true;
  const TRACKED = '.fade-overflow, .fade-overflow > *';

  const resize = new ResizeObserver(entries => {
    const boxes = new Set(entries.map(({target}) => target.closest('.fade-overflow')).filter(Boolean));
    // Children count at their own width, so a child popped out of the flow (a hover reveal) still overflows. Child and box both use rounded rendered widths, so zoom and transforms cancel (WebKit's offsetWidth reads an exactly fitting inline child 1px too wide). Read every width before the first write.
    const marks = [...boxes].map(box => {
      const boxWidth = Math.round(box.getBoundingClientRect().width);
      return [box, box.scrollWidth > box.clientWidth || [...box.children].some(child => Math.round(child.getBoundingClientRect().width) > boxWidth)];
    });
    for (const [box, overflows] of marks) box.toggleAttribute('data-overflow', overflows);
  });

  new MutationObserver(records => {
    for (const {addedNodes, removedNodes} of records) {
      for (const node of removedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        // A removed node has left its box, so it no longer matches: unobserve it regardless (a no-op if it was never observed).
        resize.unobserve(node);
        for (const el of node.querySelectorAll(TRACKED)) resize.unobserve(el);
      }
      for (const node of addedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        if (node.matches(TRACKED)) resize.observe(node);
        for (const el of node.querySelectorAll(TRACKED)) resize.observe(el);
      }
    }
  }).observe(document.documentElement, {childList: true, subtree: true});
})();
