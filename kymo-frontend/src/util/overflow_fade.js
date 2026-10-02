// Keeps data-overflow on each .fade-overflow element while its content reaches past its right edge (kymo.css fades the text only then). Boxes and their direct children are tracked from insertion to removal, so a child whose text changes in place re-marks its box.
(() => {
  if (window.__kymo_overflowFade) return;
  window.__kymo_overflowFade = true;
  const TRACKED = '.fade-overflow, .fade-overflow > *';
  // One Range for the script's lifetime: each Range left for garbage collection stays live and slows every DOM mutation until then.
  const range = document.createRange();

  const resize = new ResizeObserver(entries => {
    const boxes = new Set(entries.map(({target}) => target.closest('.fade-overflow')).filter(Boolean));
    // A box overflows when text at any depth or a direct child's box (so a child popped out of the flow for a hover reveal still counts) ends more than half a pixel past the box's right edge. Both edges come from rendered rects, so zoom and transforms cancel; comparing scrollWidth, clientWidth or an inline child's offsetWidth instead marks text that fits, because WebKit rounds those apart. Boxes are left-to-right, unscrolled and have no right border. Read every rect before the first write: each write makes the next read recalculate style, about 15 times slower over the Trash page's 200 boxes when their marks flip.
    const marks = [...boxes].map(box => {
      const edge = box.getBoundingClientRect().right;
      range.selectNodeContents(box);
      return [box, [...range.getClientRects()].some(r => r.right - edge > 0.5)];
    });
    for (const [box, overflows] of marks) box.toggleAttribute('data-overflow', overflows);
  });

  new MutationObserver(records => {
    for (const {addedNodes, removedNodes} of records) {
      for (const node of removedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        // Unobserve without matching: a child removed from its box no longer matches TRACKED, and unobserving an element that was never observed is a no-op.
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
