(()=>{
let sy=__START_Y__;
let rid=__RECT_ID__;
let handle=document.querySelector('.rect-resize-handle[data-rect-id="'+CSS.escape(rid)+'"]');
if(!handle)return;
let mrect=handle.closest('.metric-rect');
let grid=mrect?.closest('.section-grid');
if(!grid)return;
let gb=grid.getBoundingClientRect();
let mb=mrect.getBoundingClientRect();
let gap=parseFloat(getComputedStyle(grid).gap)||8;
let totalCols=getComputedStyle(grid).gridTemplateColumns.split(' ').length;
let colW=(gb.width-(totalCols-1)*gap)/totalCols;
let lastPageY=sy;
// util/width_drag.js's drags share this overlay: each drag sets its whole cssText at the start and hides it at the end.
let ov=document.getElementById('resize-drag-overlay');
if(!ov){ov=document.createElement('div');ov.id='resize-drag-overlay';document.body.appendChild(ov);}
let pv=document.getElementById('resize-drag-preview');
if(!pv){pv=document.createElement('div');pv.id='resize-drag-preview';document.body.appendChild(pv);}
ov.style.cssText='position:fixed;inset:0;z-index:9998;cursor:nwse-resize;';
pv.style.cssText='position:fixed;display:none;left:'+mb.left+'px;top:'+mb.top+'px;z-index:9999;pointer-events:none;border:2px solid color-mix(in srgb,var(--text-headings) 60%,transparent);background:color-mix(in srgb,var(--text-headings) 8%,transparent);align-items:center;justify-content:center;font-size:var(--font-size-14);font-weight:bold;color:color-mix(in srgb,var(--text-headings) 70%,transparent);';
// Rects are auto-placed, so this rect's left edge is wherever the row
// packer put it — measure the drag from mb.left, not gb.left, or every
// rect after the first in its row snaps as if it spanned from column 1.
// A span of c columns ends c*(colW+gap)-gap right of mb.left, so the nearest span rounds this.
function snap(mx){
  return Math.max(1,Math.min(totalCols,Math.round((mx-mb.left+gap)/(colW+gap))));
}
let startCols=snap(mb.right),snapCols=startCols;
function onMove(e){
  // A release outside the browser sends no mouseup. Reentry still exposes
  // the released button state; finish from the last in-drag coordinates.
  if(e.buttons===0){onUp();return;}
  e.preventDefault();
  lastPageY=e.pageY;
  snapCols=snap(e.clientX);
  pv.style.width=snapCols*colW+(snapCols-1)*gap+'px';
  pv.style.height=Math.min(Math.max(60,e.clientY-mb.top),gb.height+200)+'px';
  pv.textContent=snapCols+(snapCols>=totalCols?' (full)':'');
  pv.style.display='flex';
}
function onUp(e){
  let pageY=e?.pageY??lastPageY;
  ov.style.display='none';
  pv.style.display='none';
  document.removeEventListener('mousemove',onMove);
  document.removeEventListener('mouseup',onUp);
  window.removeEventListener('blur',onUp);
  // A span the drag didn't change stays as saved, even one wider than the section shows.
  try{dioxus.send({span:snapCols===startCols?null:snapCols,dy:pageY-sy});}catch(_){}
}
document.addEventListener('mousemove',onMove);
document.addEventListener('mouseup',onUp);
window.addEventListener('blur',onUp);
})()
