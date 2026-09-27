(()=>{
if(window.__kymo_chartCopy)return;

// ClipboardEvent.clipboardData is synchronous, permission-free, and works on
// the deployed HTTP frontend. Native Copy/Cmd+C reaches onCopy directly. On
// macOS literal Control+C is not a Copy command, so onKeydown narrowly turns
// that chord into the same event via execCommand('copy').
let textCell=function(value){
  let cell=String(value).replace(/[\t\r\n\u2028\u2029]+/g,' ');
  return /^\s*[=+\-@]/.test(cell)?"'"+cell:cell;
};
let pointerX=null,pointerY=null;
let setPoint=function(event){pointerX=event.clientX;pointerY=event.clientY};
let onMouseout=function(event){if(event.relatedTarget==null)pointerX=pointerY=null};
let buildText=function(chart,idx){
  let m=chart&&chart.__kymo_copy;
  if(!m||!chart.root||!chart.root.isConnected||!Number.isInteger(idx)||idx<0)return null;
  let x=chart.data&&chart.data[0];
  if(!x||idx>=x.length||x[idx]==null)return null;

  let xValue=x[idx]-m.xShift;
  // Undoing the time axis's +0.001s render offset can expose binary
  // subtraction noise (1.001 - 0.001 -> 0.9999999999999999). Source time is
  // millisecond based; 15 significant digits removes only that float noise.
  if(m.xShift===0.001)xValue=Number(xValue.toPrecision(15));
  let xCell=String(xValue),rows=[];
  for(let i=0;i<m.seriesCount;i++){
    let series=chart.series[m.lineBase+i];
    let value=chart.data[m.lineBase+i]&&chart.data[m.lineBase+i][idx];
    let marker=m.nanCols&&chart.data[m.nanBase+i]?chart.data[m.nanBase+i][idx]:null;
    let cell='';
    if(value!=null&&!Number.isNaN(value))cell=String(value);
    else if(marker!=null&&!Number.isNaN(marker))cell=marker===2?'Infinity':marker===3?'-Infinity':'NaN';
    if(cell!=='')rows.push(textCell(series&&series.label!=null?series.label:'')+'\t'+cell);
  }
  return rows.length?textCell(m.xLabel)+': '+xCell+'\n\n'+rows.join('\n'):null;
};

let isEditable=function(node){
  return !!(node&&node.closest&&node.closest('input,textarea,select,[contenteditable]:not([contenteditable="false"])'));
};
let currentText=function(target){
  if(pointerX==null||isEditable(target)||isEditable(document.activeElement))return null;
  let hit=document.elementFromPoint(pointerX,pointerY);
  let host=hit&&hit.closest?hit.closest('.chart-container'):null;
  if(!host||!host.id)return null;
  let chart=window.__kymo_charts&&window.__kymo_charts[host.id];
  if(!chart||!chart.over||!chart.posToIdx)return null;
  let selection=window.getSelection&&window.getSelection();
  if(selection&&!selection.isCollapsed)return null;
  // The hover mapping, so copy takes exactly the column the tooltip shows.
  let pos=window.__kymo_hp.plotPosition(chart,pointerX,pointerY);
  return pos?buildText(chart,chart.posToIdx(pos.left)):null;
};
let onCopy=function(event){
  if(event.defaultPrevented)return;
  let text=currentText(event.target);
  if(text==null||!event.clipboardData)return;
  try{
    event.clipboardData.setData('text/plain',text);
    event.preventDefault();
  }catch(_){}
};
let fallbackCopy=function(text){
  let active=document.activeElement;
  let area=document.createElement('textarea');
  area.value=text;area.readOnly=true;
  area.style.position='fixed';area.style.top='0';area.style.left='0';area.style.opacity='0';
  document.body.appendChild(area);area.select();area.setSelectionRange(0,text.length);
  try{return !!document.execCommand('copy')}catch(_){return false}finally{
    area.remove();
    if(active&&active.focus)try{active.focus({preventScroll:true})}catch(_){try{active.focus()}catch(_){}}
  }
};
let onKeydown=function(event){
  let isC=(typeof event.key==='string'&&event.key.toLowerCase()==='c')||event.code==='KeyC';
  if(event.defaultPrevented||event.repeat||event.isComposing||!isC||!event.ctrlKey||event.metaKey||event.altKey||event.shiftKey)return;
  let text=currentText(event.target);
  if(text==null)return;
  let copied=false;
  try{copied=!!document.execCommand('copy')}catch(_){}
  if(!copied)copied=fallbackCopy(text);
  if(copied){event.preventDefault();return}
  if(window.isSecureContext&&navigator.clipboard&&navigator.clipboard.writeText){
    event.preventDefault();
    navigator.clipboard.writeText(text).catch(function(){fallbackCopy(text)});
  }
};

window.__kymo_chartCopy={buildText:buildText,onCopy:onCopy,onKeydown:onKeydown};
document.addEventListener('copy',onCopy,true);
window.addEventListener('keydown',onKeydown,true);
window.addEventListener('mousemove',setPoint,true);
window.addEventListener('mouseout',onMouseout,true);
})();
