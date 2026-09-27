function updateHover(u){
  // Embedded verbatim in create.js: no template placeholders. Shares the chart closure's DOM nodes, series layout, and formatters.
  let idx=u.cursor.idx;
  let isSrc=window.__kymo_hoversrc===el.id;
  // The source tooltip stacks above synced readouts (z 71 vs 70).
  tip.classList.toggle('kymo-tip-src',isSrc);
  if(idx==null){
    tip.style.display='none';
    hotpt.style.display='none';
    if(isSrc)window.__kymo_setHl(null);
    return
  }
  let l=u.cursor.left,t=u.cursor.top;
  // Hide covered charts' fixed tips immediately, before zone virtualization unmounts them. The maximized chart lives outside the locked main element.
  if(!isSrc&&el.closest('.main-content-locked')){tip.style.display='none';return}
  let bx=el.getBoundingClientRect();
  // Offscreen charts must not leave fixed, viewport-clamped readouts at the screen edge.
  if(!isSrc&&(bx.bottom<=0||bx.top>=window.innerHeight||bx.right<=0||bx.left>=window.innerWidth)){tip.style.display='none';return}
  // Fresh data expires both caches. Viewport width sets the tooltip's 33vw width cap; highlight changes only retag rows.
  let showNearest=document.documentElement.getAttribute('data-kymo-show-nearest-point')==='true';
  let key=(isSrc?'s':'y')+idx+'|'+showNearest+'|'+window.innerWidth;
  let tc=u.data.__kymo_tipc;
  let hit=!!tc&&tc.key===key;
  // Each series keeps its exact-column sample when present; its fallback uses the nearest sampled data x, including non-finite markers. The lookup cache expires with u.data.
  let pointIndex=u.data.__kymo_pointIndex||(u.data.__kymo_pointIndex=createHoverPointLookup(u.data,lineBase,nanBase,nanCols));
  let vals=[],nks=[],idxs=[],hot=-1,hotD=1e9;
  // Markers sit markR+markGap CSS px inside their border (bottom, or top for +∞) — the cursor-space mirror of the draw hook's device-px placement.
  let markerY=u.bbox.height/devicePixelRatio-(markR+markGap);
  let markerYT=markR+markGap;
  if(isSrc||!hit)for(let i=0;i<labels.length;i++){
    let di=lineBase+i;
    let nc=nanCols>0?u.data[nanBase+i]:null;
    let pi=pointIndex(i,idx,showNearest);
    let has=pi>=0;
    idxs.push(pi);
    let v=has?u.data[di][pi]:null;
    // Kinds 1-3 have only a marker after gap mapping; restore a NaN sentinel to display their kind. Kind 4 marks non-finite custom x on a finite row and keeps that value.
    if(v==null&&has&&nc&&nc[pi]!=null)v=NaN;
    nks.push(isNaN(v)&&v!=null&&nc?nc[pi]:0);
    vals.push(v);
    // Borrowed points add readouts; only the hovered column drives highlighting.
    if(isSrc&&pi===idx&&v!=null){
      // Measure non-finite samples at their hollow marker so they remain selectable and keep the run highlighted through gaps.
      let py=isNaN(v)?(nks[i]===2?markerYT:markerY):u.valToPos(v,'y');
      let d=Math.abs(py-t);
      if(d<hotD){hotD=d;hot=i;}
    }
  }
  if(isSrc&&hot>=0&&!isNaN(vals[hot])){
    let x=u.data[0][idx],v=vals[hot],sx=u.scales.x,sy=u.scales.y;
    hotpt.style.left=u.valToPos(x,'x')+'px';
    hotpt.style.top=u.valToPos(v,'y')+'px';
    hotpt.style.borderColor=colors[hot];
    // u.over does not clip children; keep offscreen endpoints readable without painting their dots into axes. Decided in value space, where the plot bounds are exact (bbox is rounded to half device pixels).
    hotpt.style.display=x>=sx.min&&x<=sx.max&&v>=sy.min&&v<=sy.max?'block':'none';
  }else{
    // A NaN winner has no finite y to dot — its hollow marker is its mark.
    hotpt.style.display='none';
  }
  // Marker-only samples also drive the highlight, scoped to this chart.
  if(isSrc)window.__kymo_setHl(hot>=0?runIds[hot]:null,hot>=0?runNames[hot]:null,el.id);
  if(hit){
    if(tc.empty){tip.style.display='none';return}
  }else{
    // Bucket bounds are real, unshifted x values, unioned across series. Single points undo both step-log and log-time rendering shifts.
    let fmtPointX=function(pi){
      if(hasXr){
        let xl=u.data[xrBase][pi],xh=u.data[xrBase+1][pi];
        if(xl!=null&&xh!=null&&xh>xl)return fmtX(xl)+'\u2013'+fmtX(xh);
      }
      return fmtX(u.data[0][pi]-readoutXShift);
    };
    let html=isSrc?'<div class="kymo-tip-step">'+fmtPointX(idx)+'</div>':'';
    // Sort by plotted value descending, with non-finite samples last and ties in series order.
    let order=[];
    for(let i=0;i<labels.length;i++)if(vals[i]!=null)order.push(i);
    order.sort(function(a,b){
      let va=vals[a],vb=vals[b];
      let ta=isNaN(va)?1:0,tb=isNaN(vb)?1:0;
      if(ta!==tb)return ta-tb;
      if(ta)return a-b;
      return vb-va;
    });
    // Columns are name/value/[raw]/[x] on the source, value/[x] on peers. Borrowed x metadata gets its own column so numeric values stay aligned.
    let hasPointX=order.some(i=>idxs[i]!==idx);
    let rows='';
    for(let oi=0;oi<order.length;oi++){
      let i=order[oi];
      let pi=idxs[i];
      let v=vals[i];
      let vs=isNaN(v)?(nks[i]===2?'+∞':nks[i]===3?'-∞':'NaN'):v.toPrecision(4);
      // A kind-4 marker collapses this run's unplottable-x samples onto one anchor slot: surface how many when it's more than one.
      if(xnanC[i]>1&&nanCols>0){let nci=u.data[nanBase+i];if(nci&&nci[pi]===4)vs+=' ×'+xnanC[i];}
      let rowOpen='<div class="kymo-tip-row" data-r="'+i+'">';
      let pointX=hasPointX?'<span class="kymo-tip-x">'+(pi!==idx?'@ '+fmtPointX(pi):'')+'</span>':'';
      if(isSrc){
        let row=rowOpen+'<span class="kymo-tip-name fade-overflow" style="color:'+colors[i]+'">'+labels[i]+'</span><span class="kymo-tip-val" style="color:'+colors[i]+'">'+vs+'</span>';
        if(hasRange||hasRaw){
          // Equal envelope bounds need one raw value, which smoothing may still need beside the plotted value.
          let mb=1+i*rawStride+(hasRaw?1:0);
          let lo=hasRange?u.data[mb][pi]:null,hi=hasRange?u.data[mb+1][pi]:null;
          let rs;
          if(lo==null||isNaN(lo)||hi==null||isNaN(hi)||lo==hi){
            let rv=hasRaw?u.data[1+i*rawStride][pi]:(isSmoothed&&lo!=null&&!isNaN(lo)?lo:null);
            rs=(rv==null||isNaN(rv))?'':rv.toPrecision(4);
          }else{
            rs=lo.toPrecision(4)+'\u2013'+hi.toPrecision(4);
          }
          row+='<span class="kymo-tip-raw">'+rs+'</span>';
        }
        rows+=row+pointX+'</div>';
      }else{
        // Synced chart: value colored by the run, followed by borrowed x metadata when needed.
        rows+=rowOpen+'<span class="kymo-tip-val" style="color:'+colors[i]+'">'+vs+'</span>'+pointX+'</div>';
      }
    }
    if(!rows){
      // Cache empty content too; a header alone is not a useful tooltip.
      u.data.__kymo_tipc={key:key,empty:true};
      tip.style.display='none';return
    }
    html+='<div class="kymo-tip-grid" style="grid-template-columns:'+(isSrc?('minmax(0,max-content) max-content'+((hasRange||hasRaw)?' max-content':'')):'max-content')+(hasPointX?' max-content':'')+'">'+rows+'</div>';
    tip.innerHTML=html;
    tip.style.display='block';
    // Measure at left 0 so fixed-position shrink-to-fit uses only the intended 33vw cap. Reposition before paint.
    tip.style.left='0px';
    // Sizes measured once per content; retags can't change them (the hot class is paint-only — gutter reserved unconditionally, see CSS).
    tc=u.data.__kymo_tipc={key:key,w:tip.offsetWidth,h:tip.offsetHeight};
  }
  // Retag every show; synced hooks may run before the source publishes its highlight, so applyHl retags again after that publication. Hidden tips also miss changes while setHl may early-return.
  window.__kymo_retagRows(tip,runIds,runNames);
  tip.style.display='block';
  let cx=bx.left+u.bbox.left/devicePixelRatio+l;
  let tw=tc.w,th=tc.h;
  let tx,ty;
  if(isSrc){
    // Place fixed source tips beside the crosshair, flipping at viewport edges.
    let cy=bx.top+u.bbox.top/devicePixelRatio+t;
    let gap=14;
    tx=cx+gap;
    if(tx+tw>window.innerWidth-4)tx=cx-gap-tw;
    ty=cy-th-10;
    if(ty<4)ty=cy+12;
    // A tall row list placed below can still run off the bottom.
    if(ty+th>window.innerHeight-4)ty=Math.max(4,window.innerHeight-4-th);
  }else{
    // Center synced readouts beside the mirrored crosshair.
    let ph=u.bbox.height/devicePixelRatio;
    tx=cx+12;
    if(tx+tw>window.innerWidth-4)tx=cx-12-tw;
    ty=bx.top+u.bbox.top/devicePixelRatio+ph/2-th/2;
    ty=Math.min(Math.max(ty,4),window.innerHeight-th-4);
  }
  tip.style.left=tx+'px';
  tip.style.top=ty+'px';
}
