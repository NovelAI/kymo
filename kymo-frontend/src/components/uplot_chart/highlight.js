(()=>{
if(window.__kymo_setHl)return;

let hoverName=null,highlightByName=false,framePending=false;
// setHl's chartId limits dimming and lifting to the hovered chart, so a run change there repaints one canvas; null (sidebar hover) covers every chart. Tooltip and sidebar rows follow the run everywhere.
let scope=null;

function nameKey(name){return typeof name==='string'?name:null;}
function groupName(){return highlightByName?hoverName:null;}
function matches(rid,name){
  let hot=window.__kymo_hlrun;
  if(hot==null)return false;
  let group=groupName();
  return group!==null?name===group:rid===hot;
}
// Tooltip rows use series indices, keeping raw identities out of their HTML.
window.__kymo_retagRows=function(tp,runs,names){
  let trs=tp.querySelectorAll('[data-r]');
  for(let j=0;j<trs.length;j++){
    let r=trs[j],i=+r.dataset.r;
    r.classList.toggle('kymo-tip-row-hot',matches(runs[i],names[i]));
  }
};
function retagSidebar(){
  // Resolve the current root because navigation may have replaced the sidebar.
  let sidebar=document.querySelector('.sidebar-body');
  if(!sidebar)return;
  for(let row of sidebar.querySelectorAll('.sidebar-run-hl'))row.classList.remove('sidebar-run-hl');
  let rid=window.__kymo_hlrun;
  if(rid==null)return;
  let group=groupName();
  let attr=group!==null?'data-run-name':'data-run-id';
  let value=group!==null?group:rid;
  for(let row of sidebar.querySelectorAll('.sidebar-run['+attr+'="'+window.CSS.escape(value)+'"]'))row.classList.add('sidebar-run-hl');
}
window.__kymo_applyHl=function(c){
  let m=c.__kymo_hl;if(!m)return;
  let tp=c.__kymo_tip;
  // Retag rebuilt tooltips even when the canvas already has the final selection.
  if(tp&&tp.style.display!=='none')window.__kymo_retagRows(tp,m.runs,m.names);
  let rid=scope===null||window.__kymo_charts[scope]===c?window.__kymo_hlrun||null:null;
  let group=groupName();
  let present=rid!==null&&(group!==null?m.names.includes(group):m.runs.includes(rid));
  // Different selections absent from this chart have identical pixels.
  let eff=rid===null?'':present?selectionKey(rid,group):'#';
  if(c.__kymo_hleff===eff)return;
  c.__kymo_hleff=eff;
  // Native focus selects one series; a run group spans raw, envelope, and line columns across charts.
  let lb=1+m.n*m.rs;
  for(let i=0;i<m.n;i++){
    let hl=rid!==null&&matches(m.runs[i],m.names[i]);
    let a=(rid===null||hl)?1:0.25;
    // All columns share alpha; _focus lifts each envelope with its band.
    for(let k=0;k<m.rs;k++){let se=c.series[1+i*m.rs+k];se.alpha=a;se._focus=hl;}
    let sl=c.series[lb+i];sl.alpha=a;sl._focus=hl;
    if(m.nan)c.series[lb+m.n+i].alpha=a;
  }
  c.redraw(false);
};
function scheduleCharts(){
  // Coalesce hover changes into one paint of the final selection.
  if(framePending)return;
  framePending=true;
  requestAnimationFrame(function(){
    framePending=false;
    let cs=window.__kymo_charts||{};
    for(let k in cs)window.__kymo_applyHl(cs[k]);
  });
}
function selectionKey(rid,group){
  return rid===null?'':group!==null?'n'+group:'r'+rid;
}
function updateHl(rid,name,refreshSidebar,chartId){
  rid=rid||null;
  name=rid===null?null:nameKey(name);
  chartId=rid===null?null:chartId||null;
  let byName=document.documentElement.getAttribute('data-kymo-highlight-same-name')==='true';
  // Keep the prior mode until after comparison so a setting-only flip changes the selection.
  let previous=selectionKey(window.__kymo_hlrun||null,groupName());
  let changed=previous!==selectionKey(rid,byName?name:null);
  // A scope change alone also repaints; applyHl's cache skips charts whose lift is unchanged.
  let moved=chartId!==scope;
  // Keep the latest member so disabling grouping restores its raw ID.
  window.__kymo_hlrun=rid;
  hoverName=name;
  highlightByName=byName;
  scope=chartId;
  if(changed||refreshSidebar)retagSidebar();
  if(changed||moved)scheduleCharts();
}
window.__kymo_setHl=function(rid,name,chartId){
  updateHl(rid,name,false,chartId);
};
// Refresh settings and names, and restamp sidebar rows after a render.
window.__kymo_refreshHl=function(freshChart){
  let rid=window.__kymo_hlrun;
  let name=hoverName;
  if(rid!=null){
    function chartName(c){
      let m=c&&c.__kymo_hl,i=m?m.runs.indexOf(rid):-1;
      return i>=0?nameKey(m.names[i]):null;
    }
    // Prefer the fresh chart, then live sidebar rows, then mounted charts; retain the hover name if none resolves it.
    let resolved=chartName(freshChart);
    if(resolved===null){
      let sidebar=document.querySelector('.sidebar-body');
      let row=sidebar&&sidebar.querySelector('.sidebar-run[data-run-id="'+window.CSS.escape(rid)+'"]');
      if(row)resolved=nameKey(row.getAttribute('data-run-name'));
    }
    if(resolved===null){
      let cs=window.__kymo_charts||{};
      for(let k in cs){resolved=chartName(cs[k]);if(resolved!==null)break;}
    }
    if(resolved!==null)name=resolved;
  }
  updateHl(rid,name,true,scope);
};
})();
