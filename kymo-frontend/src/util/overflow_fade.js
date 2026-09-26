// Keeps data-overflow on each .fade-overflow element while its content is wider than its box (kymo.css fades the text only then). Elements are tracked from insertion to removal, children included, so text that changes in place re-marks its box.
(() => {
  if (window.__kymo_overflowFade) return;
  window.__kymo_overflowFade = true;
  const CLASS = 'fade-overflow';
  const isBox = node => node?.classList?.contains(CLASS);

  const resize = new ResizeObserver(entries => {
    const boxes = new Set();
    for (const {target} of entries) {
      const box = isBox(target) ? target : target.parentElement;
      if (isBox(box)) boxes.add(box);
    }
    // Children count at their own width, so a child popped out of the flow (a hover reveal) still overflows. Read every width before the first write.
    const marks = [...boxes].map(box => [box, Math.max(box.scrollWidth, ...Array.from(box.children, child => child.offsetWidth)) > box.clientWidth]);
    for (const [box, overflows] of marks) box.toggleAttribute('data-overflow', overflows);
  });

  const track = (box, method) => {
    resize[method](box);
    for (const child of box.children) resize[method](child);
  };
  const scan = (node, method) => {
    if (isBox(node)) track(node, method);
    for (const box of node.getElementsByClassName(CLASS)) track(box, method);
  };

  new MutationObserver(records => {
    for (const {target, addedNodes, removedNodes} of records) {
      const inBox = isBox(target);
      for (const node of removedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        if (inBox) resize.unobserve(node);
        scan(node, 'unobserve');
      }
      for (const node of addedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;
        if (inBox) resize.observe(node);
        scan(node, 'observe');
      }
    }
  }).observe(document.documentElement, {childList: true, subtree: true});
})();
