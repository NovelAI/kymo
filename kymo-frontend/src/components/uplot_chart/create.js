return(()=>{
try{
if(!window.__kymo_zm){
__KYMO_ZOOM_MATH__
window.__kymo_zm=KYMO_ZOOM_MATH;
}
let {AXIS_LINEAR,AXIS_LOG,AXIS_LOG1P,buildFrozenSupports,buildFrozenSourceMap,mapSourcePageX,expandWholeBuckets}=window.__kymo_zm;
let {createHoverPointLookup,plotPosition}=window.__kymo_hp;
if(!window.__kymo_charts) window.__kymo_charts={};
let el=document.getElementById('__KYMO_ID__');
if(!el)return'chart container is missing';
let data=window.__kymo_data&&window.__kymo_data['__KYMO_ID__'];
if(!data||!data.length)return'chart data is missing';
// uPlot only treats null as a gap. NaN survives LINEAR rendering by
// accident (non-finite canvas coords are ignored) but on LOG scales it
// clamps to the plot floor — every gap slot became a point at the bottom
// decade with line segments plunging to it, painting a false "fill"
// under log-y lines. Convert gap slots in one JS pass; x (data[0]) has
// no gaps and stays typed.
// An indexed loop, not Array.from(c,fn): the callback form costs ~7x more on typed arrays (set_data_js repeats this).
data=data.map((c,i)=>{if(i==0)return c;let n=c.length,o=new Array(n);for(let j=0;j<n;j++){let v=c[j];o[j]=v!==v?null:v;}return o;});
// The chart takes its container's box, as the ResizeObserver below does, so height is not structural config and a height change resizes in place. A hidden container has no box: it falls back to 600 wide at its CSS height.
let box=el.getBoundingClientRect(),w=box.width||600,h=box.height||parseFloat(getComputedStyle(el).height);
let prev=window.__kymo_charts['__KYMO_ID__'];
// isConnected guards a recreated container div: updating a chart
// whose canvas was detached would paint nothing, forever.
if(prev&&prev.root&&prev.root.isConnected&&prev.__kymo_cfghash==='__KYMO_CFG_HASH__'){
  // Same-config create evals can overlap when a queued create races a
  // missing-chart fallback. Coalesce the later eval in place so that
  // delivery races cannot reset cursor/tooltip/highlight mid-hover.
  // Container sizing belongs only to the ResizeObserver below. An eager
  // setSize here could consume a real width change before the observer sees
  // it, leaving an active gesture mapped through stale frozen geometry.
  prev.setData(data);
  return'';
}
if(prev){
  // A structural source rebuild invalidates the gesture-start geometry and
  // slot snapshot. Peer rebuilds are harmless and repaint after registration.
  let zg=window.__kymo_zg;
  if(zg&&zg.srcId==='__KYMO_ID__'&&zg.cancel)zg.cancel();
  // A rebuilt hover source clears through the normal path first, so synced readouts don't outlive the old instance.
  if(window.__kymo_hoversrc==='__KYMO_ID__')prev.setCursor({left:-10,top:-10},true,true);
  prev.destroy();
  delete window.__kymo_charts['__KYMO_ID__'];
}
// Labels are HTML-escaped, colors normalized to #RRGGBB, and IDs/names only JS-escaped.
// Raw identities never enter innerHTML; tooltip rows use series indices.
let labels=[__KYMO_LABELS__];
let colors=[__KYMO_COLORS__];
let runIds=[__KYMO_RUN_IDS__];
let runNames=[__KYMO_RUN_NAMES__];
let xnanC=[__KYMO_XNAN_COUNTS__];
let hasRaw=__KYMO_HAS_RAW__;
let hasRange=__KYMO_HAS_RANGE__;
// Draw order: run i's raw/envelope block ([raw,] min, max) at 1+i*rawStride, its values line at lineBase+i — every values line strokes above every block.
let rawStride=__KYMO_RAW_STRIDE__;
let lineBase=1+labels.length*rawStride;
// Smoothing on: the values column is a stroked smoothed curve; its raw evidence rides in the raw column (passthrough) or the envelope (downsampled).
let isSmoothed=__KYMO_SMOOTHED__;
let nanCols=__KYMO_NAN_MARKERS__;
// Marker columns (and series) live at nanBase+i — after the values lines.
let nanBase=lineBase+labels.length;
// X-range columns (no series entries; read by tooltips and whole-bucket zoom) trail the markers.
let hasXr=__KYMO_HAS_XRANGE__;
let xrBase=nanBase+nanCols;
let zoomRefetch=__KYMO_ZOOM_REFETCH__;
let isTimeAxis=__KYMO_IS_TIME__;
let isWallTime=__KYMO_IS_WALL__;
// Tooltip
// Readouts live under <body>, outside the charts' stacking contexts, so they paint over the docked options panel (z-index 70 against its 60) and a section's move never takes them along. The chart's teardown removes its readout.
let tip=document.getElementById('__KYMO_ID__-tip');
if(!tip){tip=document.createElement('div');tip.id='__KYMO_ID__-tip';tip.className='kymo-tip';document.body.appendChild(tip);}
// Hidden until this instance's hover hook shows it on the next move or synced update, so a failed rebuild leaves no stale tooltip.
tip.style.display='none';
let xShift=__KYMO_X_SHIFT__;
let readoutXShift=__KYMO_READOUT_X_SHIFT__;
// Value formatter for Y and step X. Zero is just "0": fixed-precision formatting rendered step 0 as "0.00" (AI-1385).
// Precision comes from the grain, not fixed digit counts. On linear axes the grain is the tick spacing: uPlot hands the whole split list, decFor(step) is however many digits the spacing needs, and adjacent ticks can never render identically (toExponential(1) used to collapse 1.50e6/1.52e6/1.54e6 into three "1.5e6" labels).
// Log axes (asked directly: scale.distr 3, pv) and degenerate single-tick lists have no meaningful axis-wide spacing, so the grain is the value itself. uPlot labels integral mantissas exactly and filtered-out splits arrive as null, yielding the natural 1k, 10k, 100k, 1M ladder.
// Values >=1000 wear k/M/B/T suffixes; of the largest unit fitting the grain and the unit below it, whichever renders narrower wins — the decimal point costs a digit slot, so "1547k" beats "1.547M" but "2.5M" beats "2500k"; ties keep the larger unit.
// Rounding cannot merge linear neighbors because unit <= step makes consecutive tick mantissas differ in their integer digits. Linear spacings under 1000 (deep y-zoom) keep plain grouped digits; fl floors every sub-1 label at its leading digit, including near-constant metrics whose tiny step decFor's 1e-6 tolerance swallows to 0.
let yFmt=function(u,vals,ax){
  var step=Infinity;
  for(var i=1;i<vals.length;i++){
    var d=vals[i]-vals[i-1];
    if(d>0&&d<step)step=d;
  }
  var pv=u.scales[u.axes[ax].scale].distr==3||!isFinite(step);
  var decFor=function(s){
    var dec=0;
    while(dec<8&&Math.abs(s*Math.pow(10,dec)-Math.round(s*Math.pow(10,dec)))>1e-6)dec++;
    return dec;
  };
  return vals.map(function(v){
    if(v==null)return'';
    if(v==0)return'0';
    var a=Math.abs(v);
    if(a<0.001)return v.toExponential(decFor((pv?a:step)/Math.pow(10,Math.floor(Math.log10(a)))));
    if(a>=1000&&(pv||step>=1000)){
      var us=[[1e12,'T'],[1e9,'B'],[1e6,'M'],[1e3,'k'],[1,'']],lim=pv?a:step,j=0;
      while(us[j][0]>lim)j++;
      var lab=function(k,fd){return(v/us[k][0]).toLocaleString(undefined,{useGrouping:false,maximumFractionDigits:fd})+us[k][1]};
      var s1=lab(j,3),s2=lab(j+1,0);
      return s1.length<=s2.length?s1:s2;
    }
    var fl=a<1?-Math.floor(Math.log10(a)):0;
    return v.toLocaleString(undefined,{maximumFractionDigits:pv?fl+2:Math.max(decFor(step),fl)});
  });
};
let sameWallDay=function(a,b){
  var da=new Date(a*1000),db=new Date(b*1000);
  return da.getFullYear()===db.getFullYear()&&da.getMonth()===db.getMonth()&&da.getDate()===db.getDate();
};
let fmtWall=function(v,withDate){var d=new Date(v*1000);return withDate?d.toLocaleString():d.toLocaleTimeString()};
let xFmtRaw=isWallTime
  ?function(u,vals){var s=u.scales.x,withDate=s.min!=null&&s.max!=null&&!sameWallDay(s.min,s.max);return vals.map(function(v){return v==null?'':fmtWall(v,withDate)})}
  :isTimeAxis
    ?function(u,vals){return vals.map(function(v){if(v==null)return'';if(v<60)return v.toFixed(0)+'s';if(v<3600)return(v/60).toFixed(1)+'m';return(v/3600).toFixed(1)+'h'})}
    :yFmt;
// log(x+1) axes label REAL steps: ticks arrive shifted, subtract before formatting.
let xFmt=xShift?function(u,vals,ax){return xFmtRaw(u,vals.map(function(v){return v==null?null:v-xShift}),ax)}:xFmtRaw;
// log(x+1) x axis (step charts): uPlot's native log splits land on nice SHIFTED values (1,2,5,10 -> steps 0,1,4,9 — ugly labels). Generate splits at nice REAL steps instead — 0 plus 1-2-5 ladders per decade, the densest tier that fits the width — mapped +1 for positioning; xFmt subtracts the shift back off. Sub-decade zooms behave linearly and get plain nice increments.
let xSplits=function(u,axisIdx,smin,smax){
  let lo=Math.max(smin-xShift,0),hi=smax-xShift;
  if(!(hi>lo))return[smin,smax];
  let cap=Math.max(3,Math.floor(u.bbox.width/devicePixelRatio/70));
  if(hi/Math.max(lo,1)<10){
    let raw=(hi-lo)/cap,p=Math.pow(10,Math.floor(Math.log10(raw)));
    let step=raw<=p?p:raw<=2*p?2*p:raw<=5*p?5*p:10*p;
    let out=[];
    for(let r=Math.ceil(lo/step)*step;r<=hi+step*1e-9;r+=step)out.push(r+xShift);
    return out;
  }
  let mk=function(ms){
    let out=lo<=0?[0]:[];
    let k0=Math.floor(Math.log10(Math.max(lo,1)));
    let k1=Math.ceil(Math.log10(hi));
    for(let k=k0;k<=k1;k++)for(let j=0;j<ms.length;j++){let r=ms[j]*Math.pow(10,k);if(r>=lo&&r<=hi&&r>=1)out.push(r);}
    return out;
  };
  let pick=mk([1,2,5]);
  if(pick.length>cap)pick=mk([1]);
  if(pick.length>cap){let s=Math.ceil(pick.length/cap);pick=pick.filter(function(_,i){return i%s===0});}
  return pick.map(function(r){return r+xShift});
};
// Single-x formatter for tooltip readouts (xFmt formats axis tick
// LISTS; wall/relative time need their cursor-grade precision back).
// Custom numeric X uses the same significant-digit policy as Y tooltip rows;
// otherwise sub-cent values collapse to zero under the step formatter. Steps
// show the exact hovered value: the axis formatter's suffixes/rounding
// ("1547k") would misreport which step the cursor is on. The fraction cap only
// touches synthetic non-integer x (interpolated/bucket-center rows) — real
// logged steps are integers and render in full.
let fmtX=function(v){
  if(isWallTime)return fmtWall(v,true);
  if(isTimeAxis){if(v<60)return v.toFixed(1)+'s';if(v<3600)return(v/60).toFixed(1)+'m';return(v/3600).toFixed(1)+'h'}
  if(!zoomRefetch)return v.toPrecision(4);
  return v.toLocaleString(undefined,{maximumFractionDigits:2});
};
// Nearest-point marker, drawn by the setCursor hook.
let hotpt=document.createElement('div');
hotpt.className='kymo-hotpt';
hotpt.style.display='none';
// Live colors: uPlot resolves function strokes at every draw, so a theme
// flip only needs a redraw. Snapshotting these at creation left mounted
// charts wearing the old theme (the create-JS is theme-independent, so
// no recreate ever fired for a theme change).
let cssv=function(n){return getComputedStyle(document.documentElement).getPropertyValue(n).trim()};
let axColor=function(){return cssv('--chart-axis')};
let gridColor=function(){return cssv('--chart-grid')};
// Y range: round the data bounds outward to multiples of the coarsest
// 1/2/5*10^k increment that fits in a 10%-of-span slack budget. Bounds
// are nice numbers when niceness is cheap, and slack per side stays in
// [0, 10%] — unlike uPlot's default (10% pad + unbounded outward
// rounding). Re-runs per x-zoom since y auto-ranges to visible data.
let yNice=function(u,mn,mx){
  if(!isFinite(mn)||!isFinite(mx))return[mn,mx];
  let span=mx-mn;
  // Flat/ulp-noise spans (e.g. smoothed constant metrics) explode uPlot's tick walk into millions of ticks (uPlot #1135). A custom range fn bypasses the default ranger's flat fallback, so mirror its flat test and delegate those spans back to rangeNum.
  if(!(span>1e-24)||Math.max(Math.abs(mn),Math.abs(mx))/span>1e10)return uPlot.rangeNum(mn,mx,0.1,true);
  let cap=0.1*span;
  let p=Math.pow(10,Math.floor(Math.log10(cap)));
  let i=cap>=5*p?5*p:cap>=2*p?2*p:p;
  return[Math.floor(mn/i)*i,Math.ceil(mx/i)*i];
};
// uPlot walks a log axis decade by decade from floor(log10(min)); a min <=0
// (log10 -> -Infinity) or a non-finite bound (an x-zoom that caught no points
// leaves uPlot's auto min/max at +-Infinity) makes that walk never terminate
// and freezes the tab. logClamp is the one rule both log scales pass through:
// keep a finite positive bound, else fall back to the data's own positive
// extent (lo,hi); nothing positive at all -> a neutral decade. (The axis-pan
// path guards the same hazard with its exp(-690) clamp.)
let logClamp=function(mn,mx,lo,hi){
  if(!(lo<=hi))return[1,10];
  return[(isFinite(mn)&&mn>0)?mn:lo,(isFinite(mx)&&mx>0)?mx:hi];
};
// Log-x range: pin to the zoom's own endpoints; data[0] is ascending and >0
// (the build trims x<=0 for log-x), so its ends are the positive extremes a
// bound extrapolated past 0 falls back to.
let logSafeX=function(u,mn,mx){let X=u.data[0];return logClamp(mn,mx,X[0],X[X.length-1]);};
// Log-y counterpart of yNice: bounds snap to m*10^k — m from the coarse
// ladder (1/2/5) when that costs <=10% of the displayed decades in log
// space, the fine ladder as fallback, the raw data bound last. uPlot's
// own log ranging always rounds to integer-mantissa steps, which on a
// sub-decade chart is mostly empty space.
let yNiceLog=function(u,mn,mx){
  if(!(isFinite(mn)&&mn>0&&isFinite(mx))){
    // Degenerate: a <=0 sample or envelope edge in view (it clips at the bottom border via uPlot's log clamp and must not drive the range), or an empty window. Range to the strictly-positive extent of the y evidence in the visible x window. The dense envelope, when present, provably contains the raw column (a duplicate-x band spans every point at its x; min==max==sample elsewhere) and the unsmoothed values line (the bucket mean), so those reads are skipped as redundant (AI-1411); a smoothed line stays scanned — its trailing window reaches samples outside the visible window, so it can extend past the window's envelope. y isn't sorted, so this needs a scan, but only the degenerate case pays for it.
    let X=u.data[0],x0=u.scales.x.min,x1=u.scales.x.max;
    let lo=Infinity,hi=0,scan=function(col){for(let j=0;j<col.length;j++){let v=col[j];if(v==null||v<=0)continue;if(isFinite(x0)&&(X[j]<x0||X[j]>x1))continue;if(v<lo)lo=v;if(v>hi)hi=v;}};
    for(let i=0;i<labels.length;i++){let b=1+i*rawStride;for(let k=(hasRange&&hasRaw)?1:0;k<rawStride;k++)scan(u.data[b+k]);if(isSmoothed||!hasRange)scan(u.data[lineBase+i]);}
    [mn,mx]=logClamp(mn,mx,lo,hi);
  }
  let cap=0.1*(Math.log10(mx)-Math.log10(mn));
  if(!(cap>0))return[mn/1.25,mx*1.25];
  let snap=function(v,up){
    let k=Math.pow(10,Math.floor(Math.log10(v)));
    let m=v/k;
    for(const ms of [[1,2,5,10],[1,1.5,2,3,4,5,6,7,8,9,10]]){
      let c;
      if(up)c=k*ms.find(function(x){return x>=m-1e-12});
      else{let lo=ms.filter(function(x){return x<=m+1e-12});c=k*lo[lo.length-1];}
      if(Math.abs(Math.log10(c/v))<=cap)return c;
    }
    return v;
  };
  return[snap(mn,false),snap(mx,true)];
};
// NaN-marker geometry, in CSS px: hollow circles of radius markR, with their
// bottom edge markGap above the plot's bottom edge (center markR+markGap up).
// The draw hook paints them; setCursor measures the cursor to the same spot so
// a NaN reads as a hoverable sample. One source so the two can't drift apart.
let markR=3.5,markGap=1.5;
let u=new uPlot({
  width:w,height:h,
  // Replaces uPlot's autoPadSide (17 top / 25 right). Top 8 is the whole title-to-plot gap (.rect-header has no margin-bottom) and absorbs y-max stroke caps; the rename hover strip (.rect-title::before) deadens its top half and must stay <= it. Right 12 keeps the last x label's centered half on-canvas and leaves a gutter for edge-hover and gutter-select drags.
  padding:[8,12,0,0],
  legend:{show:false},
  // _focus is our z-order signal (patched drawSeries strokes _focus'd series last); alpha:1 keeps uPlot's native focus from ALSO dimming, so series.alpha stays entirely ours.
  focus:{alpha:1},
  // x auto IS the zoom policy: it gates setData's re-range (uPlot consults it nowhere else that matters in mode 1). Unzoomed, every live flush re-ranges x to the full extent — the chart follows a growing run. While a client-side zoom is active (__kymo_userzoom: set by drag-select/gutter-select/axis-pull, cleared by the configured reset click) setData keeps the current window but still commits, so new points landing inside the window paint on the flush that delivers them. Declaring the policy on the scale keeps every setData caller a plain setData(data) — passing resetScales=false instead skips the commit and freezes the zoomed chart until the next interaction.
  scales:{x:{time:false,distr:__KYMO_X_DISTR__,...__KYMO_X_RANGE__,auto:u=>!u.__kymo_userzoom},...__KYMO_SYNC_SCALE__,y:{distr:__KYMO_Y_DISTR__,...__KYMO_Y_RANGE__},nan:{range:[0,1]}},
  // kymo owns zoom gestures. Leaving uPlot's native drag armed gives every
  // synced chart an independent pixel-space drag state, which lets a peer
  // revive the source selection and makes every peer commit on mouseup.
  // Unbind mousedown for that, the overlay's hover listeners because the container hover block below maps every move, and dblclick because kymo's reset below is the only reset; cursor/readout sync remains.
  // The no-op drag.click is required because uPlot's capture-phase click
  // handler otherwise compares against an unset native mousedown coordinate
  // and suppresses every click over the plot.
  cursor:{points:{show:false},y:false,bind:{mousedown:()=>null,mouseenter:()=>null,mousemove:()=>null,mouseleave:()=>null,dblclick:()=>null},drag:{click:()=>{}},sync:{key:'kymo-__KYMO_SYNC_KEY__',scales:['__KYMO_SYNC_SCALE_KEY__',null]}},
  hooks:{
    draw:[function(u){
      if(!nanCols)return;
      // Hollow NaN-marker circles, seated a fixed pixel offset above the
      // plot's bottom edge. A marker flags WHERE a NaN happened — it has
      // no y value; rendering through the series machinery forced a fake
      // one (0, or the min positive value on log scales) that stretched
      // the y range on every chart with a logged NaN.
      let ctx=u.ctx,dpr=devicePixelRatio;
      let r=markR*dpr;
      // +∞ (kind 2) seats at the TOP border — the side its data exceeds; every other kind at the bottom.
      let cyB=u.bbox.top+u.bbox.height-r-markGap*dpr;
      let cyT=u.bbox.top+r+markGap*dpr;
      ctx.save();
      // uPlot clips its own series to the plot box, but hooks draw on the
      // raw canvas — clip here too, or an endpoint marker spills into the
      // axis gutter (it renders as a half circle instead).
      ctx.beginPath();
      ctx.rect(u.bbox.left,u.bbox.top,u.bbox.width,u.bbox.height);
      ctx.clip();
      ctx.lineWidth=1.5*dpr;
      // Each marker column's filled slots, indexed once per immutable u.data array (like hover's point lookup), so a repaint visits the markers, not the whole axis.
      let marks=u.data.__kymo_marks||(u.data.__kymo_marks=u.data.slice(nanBase,nanBase+nanCols).map(col=>{let js=[];if(col)for(let j=0;j<col.length;j++)if(col[j]!=null)js.push(j);return js;}));
      for(let i=0;i<nanCols;i++){
        let col=u.data[nanBase+i];
        let s=u.series[nanBase+i];
        if(!col||!s||s.show===false)continue;
        ctx.globalAlpha=s.alpha==null?1:s.alpha;
        ctx.strokeStyle=colors[i];
        for(let j of marks[i]){
          let cx=u.valToPos(u.data[0][j],'x',true);
          if(cx<u.bbox.left-r||cx>u.bbox.left+u.bbox.width+r)continue;
          ctx.beginPath();
          ctx.arc(cx,col[j]===2?cyT:cyB,r,0,2*Math.PI);
          ctx.stroke();
        }
      }
      ctx.restore();
    }],
    setCursor:[__KYMO_HOVER__],
    setScale:[function(_,key){
      let zg=window.__kymo_zg;
      if(key==='x'&&zg&&zg.paint)zg.paint();
    }],
    setSize:[function(){
      let zg=window.__kymo_zg;
      if(zg&&zg.paint)zg.paint();
    }]
  },
  axes:[
    // The app-root preference scales canvas text too; size is ticks 5 + gap 6 + the rendered font.
    {stroke:axColor,grid:{show:false},ticks:{stroke:gridColor,width:1,size:5},font:'__KYMO_AXIS_FONT_SIZE__px sans-serif',gap:6,size:__KYMO_X_AXIS_SIZE__,values:xFmt,...__KYMO_X_SPLITS__},
    // size is a function: the axis grows to fit its widest label (uPlot re-asks on every scale change) until the narrow-cell plot floor below wins. Every label is measured — with proportional glyphs the longest string isn't always the widest. The preference-scaled floor keeps plot left edges aligned across a grid column of short-label charts; the cycleNum guard is the uPlot autosize idiom.
    {stroke:axColor,grid:{stroke:gridColor,width:1},ticks:{stroke:gridColor,width:1,size:5},font:'__KYMO_AXIS_FONT_SIZE__px sans-serif',gap:6,
     size:function(u,values,axisIdx,cycleNum){
       let ax=u.axes[axisIdx];
       if(cycleNum>1)return ax._size;
       u.ctx.font=ax.font[0];
       let w=0;
       for(const v of values||[])w=Math.max(w,u.ctx.measureText(v).width);
       let measured=Math.max(__KYMO_Y_AXIS_MIN_SIZE__,Math.ceil(ax.ticks.size+ax.gap+w/devicePixelRatio));
       // A narrow grid cell can be smaller than a long formatted label. Cap
       // this gutter after measurement so the right pad (12px above) and a
       // minimally useful plot keep non-negative geometry; ordinary charts
       // are wider than the cap and retain their full labels.
       let maxAxis=Math.max(0,u.width-12-40);
       return Math.min(measured,maxAxis);
     },values:yFmt}
  ],
  series:[__KYMO_SERIES_LIST__],
  bands:[__KYMO_BANDS_LIST__]
},data,el);
u.over.appendChild(hotpt);
// The page-global copy handler resolves the live chart by id, then reads this
// refreshed per-instance metadata. That avoids listeners closing over stale
// columns when a structural config change rebuilds the uPlot instance.
u.__kymo_copy={xLabel:'__KYMO_X_LABEL__',lineBase:lineBase,seriesCount:labels.length,nanBase:nanBase,nanCols:nanCols,xShift:readoutXShift};
window.__kymo_charts['__KYMO_ID__']=u;
u.__kymo_cfghash='__KYMO_CFG_HASH__';
// Dynamic metadata read by the once-installed gesture listeners. Keep it on
// the current uPlot instance so a config rebuild cannot leave those listeners
// using the first build's x-range column offsets or log transform.
u.__kymo_zoom={zr:zoomRefetch,xShift:xShift,xrBase:hasXr?xrBase:null,axis:u.scales.x.distr===3?(xShift?AXIS_LOG1P:AXIS_LOG):AXIS_LINEAR};
u.__kymo_hl={runs:runIds,names:runNames,rs:rawStride,n:labels.length,nan:nanCols>0};
// applyHl's retag reaches the tip through the instance; the tip node outlives rebuilds, so a recreated u re-adopts it.
u.__kymo_tip=tip;
// Fresh charts are unhighlighted; seed the cache to avoid a redraw when no selection is active.
u.__kymo_hleff='';
// Refresh may schedule peers; apply to this fresh chart now even if the group is unchanged.
if(window.__kymo_hlrun)window.__kymo_refreshHl(u);
window.__kymo_applyHl(u);
if(window.__kymo_zg&&window.__kymo_zg.paint)window.__kymo_zg.paint();
// Deferred debt: this remains one ResizeObserver per mounted chart. Zone virtualization keeps that count viewport-bounded; consolidate it only if that bound changes or profiling shows observer bookkeeping matters.
if(!window.__kymo_ro) window.__kymo_ro={};
if(window.__kymo_ro['__KYMO_ID__']) window.__kymo_ro['__KYMO_ID__'].disconnect();
window.__kymo_ro['__KYMO_ID__']=new ResizeObserver(()=>{
  let c=window.__kymo_charts['__KYMO_ID__'];
  if(c){
    let r=el.getBoundingClientRect();
    if(r.width>0){
      // The box is the chart's size: its height is fixed by CSS (--kymo-chart-height), which the chart can't inflate, so this setSize never re-triggers the observer. Height compares exactly, so a 1 px height change still lands.
      if(Math.abs(c.width-r.width)<=1&&c.height===r.height)return;
      let owner=window.__kymo_zg;
      c.setSize({width:r.width,height:r.height});
      // Only a real container resize invalidates the frozen source geometry.
      // Restore/cancel after uPlot releases its commit queue; internal axis
      // size convergence also fires setSize hooks but never reaches here.
      if(owner&&owner.srcId==='__KYMO_ID__'&&owner.cancel)queueMicrotask(function(){if(window.__kymo_zg===owner)owner.cancel();});
    }
  }
});
window.__kymo_ro['__KYMO_ID__'].observe(el);
// Configurable click/double-click to reset zoom. Listeners live on the
// persistent container and read the root preference + current chart each
// time, so a settings change needs no chart rebuild.
let eachPeer=function(sync,fn){
  let cs=window.__kymo_charts||{};
  for(let id in cs){let peer=cs[id],meta=peer&&peer.__kymo_zoom;if(meta&&peer.cursor.sync.key===sync)fn(peer,meta);}
};
let singleClickUnzoom=function(){return document.documentElement.getAttribute('data-kymo-single-click-unzoom')==='true';};
// Click suppression follows pointer travel in both axes; selecting a zoom
// range still uses horizontal displacement only. Latch during movement so
// returning to the starting point cannot turn a drag into a reset click.
let suppressDragClick=function(start,end){
  if(Math.hypot(end.pageX-start.pageX,end.pageY-start.pageY)>=3)el.__kymo_skipUnzoomClick=true;
};
let resetZoom=function(){
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c)return;
    if(c.__kymo_zoom.zr){
      el.dispatchEvent(new CustomEvent('kymo-zoom',{detail:{xmin:null,xmax:null},bubbles:true}));
    }else{
      eachPeer(c.cursor.sync.key,function(peer,meta){
        if(meta.zr||!peer.__kymo_userzoom||!peer.data[0].length)return;
        peer.setScale('x',{min:peer.data[0][0],max:peer.data[0][peer.data[0].length-1]});
        peer.__kymo_userzoom=false;
      });
    }
};
if(!el.__kymo_unzoomclick){
  el.__kymo_unzoomclick=true;
  el.addEventListener('click',function(ev){
    if(!singleClickUnzoom()||ev.detail!==1)return;
    if(el.__kymo_skipUnzoomClick){el.__kymo_skipUnzoomClick=false;return;}
    resetZoom();
  });
  el.addEventListener('dblclick',function(){
    if(!singleClickUnzoom())resetZoom();
  });
}
// The axis-pull strip, measured from the plot overlay like plotPosition, so it starts exactly where the hover band ends.
let strip=function(c,ev){
  let r=el.getBoundingClientRect(),o=c.over.getBoundingClientRect();
  let left=o.left-r.left,w=o.width,px=ev.clientX-r.left;
  return {left:left,w:w,px:px,inside:w>0&&ev.clientY-o.top>o.height&&ev.clientY<=r.bottom&&px>=left-10&&px<=left+w+10};
};
// kymo owns hover: this one container listener maps every move through plotPosition, so the plot and its side gutters share one rule. The y-axis gutter and right padding stay live on the first/last column (AI-1279); above/below the plot band (title, axis strip) clears.
// The source (window.__kymo_hoversrc) is the chart the mouse moves over: only it builds the full tooltip and drives the highlight; synced charts show compact readouts.
// setCursor's third arg publishes to the cursor-sync group (AI-1405), so synced readouts follow the cursor and its clears.
if(!el.__kymo_hover){
  el.__kymo_hover=true;
  el.addEventListener('mousemove',function(ev){
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c)return;
    let pos=plotPosition(c,ev.clientX,ev.clientY),zg=window.__kymo_zg;
    // The pointer shows the live gesture's own pointer, else what a press would do: crosshair where hover and zoom-select are live, ew-resize on the axis strip, the default arrow elsewhere.
    el.style.cursor=zg?zg.cursor:pos?'crosshair':strip(c,ev).inside?'ew-resize':'';
    window.__kymo_hoversrc='__KYMO_ID__';
    c.setCursor(pos||{left:-10,top:-10},true,true);
  });
  // Only the source clears what it published, as on rebuild and unmount: clearing while it is still the source lets updateHover hide the tooltip and drop the highlight.
  el.addEventListener('mouseleave',function(){
    if(window.__kymo_hoversrc!=='__KYMO_ID__')return;
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(c)c.setCursor({left:-10,top:-10},true,true);
    window.__kymo_hoversrc=null;
  });
}
let stepSupports=function(c,m){
  if(!m||!m.zr)return null;
  let xl=m.xrBase!=null?c.data[m.xrBase]:null,xh=m.xrBase!=null?c.data[m.xrBase+1]:null;
  let frozen=buildFrozenSupports({plotXs:c.data[0],xrMin:xl,xrMax:xh,xShift:m.xShift});
  return frozen.coverage?frozen:null;
};
// One kymo-owned x-selection gesture for plot and side-gutter starts.
// uPlot's native mousedown is disabled above, so synced peers remain hover
// sources only: every pageX is interpreted through the physical source's
// frozen geometry and bucket evidence, even while the pointer is over a peer.
if(!el.__kymo_zoomselect){
  el.__kymo_zoomselect=true;
  let hide={left:0,width:0,top:0,height:0};
  let clearBoxes=function(sync){eachPeer(sync,function(c){c.setSelect(hide,false);});};
  let paintRange=function(sync,range){
    eachPeer(sync,function(c,m){
      let realLo=c.scales.x.min-m.xShift,realHi=c.scales.x.max-m.xShift;
      let lo=Math.max(range.lo,realLo),hi=Math.min(range.hi,realHi);
      if(!(Number.isFinite(lo)&&Number.isFinite(hi)&&hi>lo)){c.setSelect(hide,false);return;}
      let p0=c.valToPos(lo+m.xShift,'x'),p1=c.valToPos(hi+m.xShift,'x');
      if(!(Number.isFinite(p0)&&Number.isFinite(p1))){c.setSelect(hide,false);return;}
      // lo/hi are already clamped to this peer's visible range, so the box lands on its plot up to float error: no pixel clamp, and no layout read in this write loop. Height is drawing geometry.
      c.setSelect({left:p0,width:p1-p0,top:0,height:c.bbox.height/devicePixelRatio},false);
    });
  };
  let snapshot=function(c){
    let m=c.__kymo_zoom;
    if(!m||!c.data||!c.data[0]||!c.data[0].length)return null;
    // Refetched step charts resolve real, unshifted bucket evidence. Client
    // time/custom charts never recover raw evidence, so retain their exact
    // already-plotted domain (including relative log-time's folded +1ms).
    let supports=stepSupports(c,m);
    if(m.zr&&!supports)return null;
    let o=c.over.getBoundingClientRect();
    let visualLo=c.scales.x.min-m.xShift,visualHi=c.scales.x.max-m.xShift;
    let coverageLo=m.zr?supports.coverage.lo:visualLo,coverageHi=m.zr?supports.coverage.hi:visualHi;
    let map=buildFrozenSourceMap({axis:m.axis,plotLeft:o.left+window.scrollX,width:o.width,visualLo:visualLo,visualHi:visualHi,coverageLo:coverageLo,coverageHi:coverageHi});
    return map?{source:c,srcId:'__KYMO_ID__',sync:c.cursor.sync.key,zr:m.zr,supports:supports,map:map}:null;
  };
  let endpoint=function(s,x){
    // Client-only charts retain their exact, plot-clamped selection. Step
    // charts extrapolate without bound in the frozen source transform.
    if(!s.zr)x=Math.min(Math.max(x,s.map.plotLeft),s.map.plotLeft+s.map.width);
    return mapSourcePageX(s.map,x);
  };
  let nominal=function(s){
    let a=endpoint(s,s.downX),b=endpoint(s,s.lastX);
    if(!(Number.isFinite(a)&&Number.isFinite(b)))return null;
    return {lo:Math.min(a,b),hi:Math.max(a,b)};
  };
  let resolved=function(s){
    let r=nominal(s);
    if(!r)return null;
    return s.zr?expandWholeBuckets(s.supports,r.lo,r.hi):r;
  };
  let applyClientScale=function(s,range){
    eachPeer(s.sync,function(c,m){
      let lo=range.lo+m.xShift,hi=range.hi+m.xShift;
      if(Number.isFinite(lo)&&Number.isFinite(hi)&&hi>lo){c.setScale('x',{min:lo,max:hi});c.__kymo_userzoom=true;}
    });
  };
  el.addEventListener('mousedown',function(ev){
    if(ev.button!==0)return;
    // Clear a stale latch from a drag released outside this element (no click
    // follows in that case). This mousedown owns the next possible click.
    el.__kymo_skipUnzoomClick=false;
    if(window.__kymo_zg)return;
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c)return;
    // Presses where hover is live (plot height, gutters included) select; the axis strip below belongs to axis-pull.
    if(!plotPosition(c,ev.clientX,ev.clientY))return;
    let s=snapshot(c);
    if(!s)return;
    s.downX=s.lastX=ev.pageX;
    let cleanup=function(){
      window.removeEventListener('mousemove',onmove);
      window.removeEventListener('mouseup',onup);
      window.removeEventListener('blur',cleanup);
      window.removeEventListener('keydown',onkey);
      clearBoxes(s.sync);
      if(window.__kymo_zg===s)window.__kymo_zg=null;
    };
    let moved=function(){return Math.abs(s.lastX-s.downX)>=3;};
    let paint=function(){
      if(window.__kymo_zg!==s)return;
      let r=moved()?resolved(s):null;
      if(r&&r.hi>r.lo)paintRange(s.sync,r);else clearBoxes(s.sync);
    };
    let finish=function(finalX){
      if(window.__kymo_zg!==s)return;
      if(Number.isFinite(finalX))s.lastX=finalX;
      let live=window.__kymo_charts[s.srcId];
      let range=moved()&&live===s.source?resolved(s):null;
      cleanup();
      if(!range||!(range.hi>range.lo))return;
      if(s.zr){
        el.dispatchEvent(new CustomEvent('kymo-zoom',{detail:{xmin:range.lo,xmax:range.hi},bubbles:true}));
      }else applyClientScale(s,range);
    };
    let onmove=function(e2){
      if(window.__kymo_zg!==s)return;
      if((e2.buttons&1)===0){finish(null);return;} // missed mouseup: commit last held position
      let x=e2.pageX;
      s.lastX=x;
      suppressDragClick(ev,e2);
      paint();
    };
    let onup=function(e2){if(e2.button===0){suppressDragClick(ev,e2);finish(e2.pageX);}};
    let onkey=function(e2){if(e2.key==='Escape')cleanup();};
    s.cancel=cleanup;s.paint=paint;s.cursor='crosshair';
    window.__kymo_zg=s;
    ev.preventDefault();
    window.addEventListener('mousemove',onmove);
    window.addEventListener('mouseup',onup);
    window.addEventListener('blur',cleanup);
    window.addEventListener('keydown',onkey);
  });
}
// X-axis bound drag (AI-1284): grab the axis strip below the plot and
// PULL the axis — the value under the cursor stays under the cursor
// while the far edge stays pinned (left half pins x-max, right half
// pins x-min). Starting mid-strip causes no jump (the map is identity
// until you move), and dragging right with a left-half grab stretches
// the axis to reveal more history on the left. Geometry is computed in
// the frame locked at mousedown; the rescale applies live, and release
// routes the final range through the normal zoom path (full-resolution
// refetch on step axes). Listeners are registered once per element and look the live chart up by id, surviving chart recreations.
if(!el.__kymo_axisdrag){
  el.__kymo_axisdrag=true;
  el.addEventListener('mousedown',function(ev){
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c||ev.button!==0||window.__kymo_zg)return;
    let st=strip(c,ev);
    if(!st.inside)return;
    // On a log x scale pixels are linear in log(value): run the same
    // pull math in log space and exponentiate on the way out.
    let isLog=c.scales.x.distr===3;
    let T=isLog?Math.log:function(v){return v};
    let mn=T(c.scales.x.min),mx=T(c.scales.x.max);
    let p0=st.px-st.left;
    // Pull bounds are float-safety only: the drag is direct manipulation
    // and must respond for as long as the hand moves — bounding at the
    // data extent went dead the moment a full view had nothing left to
    // reveal. Empty space past the data is what the hand asked for, and a
    // released pull snaps a step axis back to data via the refetch. In
    // transformed units: on log, exp(+-690) keeps min strictly positive
    // and max finite, so step 0 and the tick-walk freeze stay
    // unreachable; on linear, comfortably sub-overflow. Unreachable by
    // hand either way (~300 decades = a 17000px drag).
    let d0=c.data[0];
    if(!d0.length)return; // log-x can trim every point; nothing to pull
    let zm=c.__kymo_zoom,frozen=stepSupports(c,zm);
    if(zm&&zm.zr&&!frozen)return;
    let lim=isLog?690:1e300;
    let ad={which:p0<st.w/2?'min':'max',mn:mn,mx:mx,v0:mn+p0/st.w*(mx-mn),p0:p0,left:st.left,w:st.w,moved:false,inv:isLog?Math.exp:function(v){return v},blo:-lim,bhi:lim,coverage:frozen&&frozen.coverage,userzoom:!!c.__kymo_userzoom};
    ev.preventDefault();
    // Window listeners live only for the duration of the drag — idle
    // charts add no per-mousemove work and hold no references.
    let owner=null;
    let finish=function(commit){
      window.removeEventListener('mousemove',onmove);
      window.removeEventListener('mouseup',onup);
      window.removeEventListener('blur',oncancel);
      window.removeEventListener('keydown',onkey);
      let c2=window.__kymo_charts['__KYMO_ID__'];
      let state=ad,moved=state&&state.moved;
      ad=null;
      if(window.__kymo_zg===owner)window.__kymo_zg=null;
      if(!commit&&c2&&state){c2.setScale('x',{min:state.inv(state.mn),max:state.inv(state.mx)});c2.__kymo_userzoom=state.userzoom;}
      if(!commit||!c2||!moved)return;
      let meta=c2.__kymo_zoom;
      if(meta.zr){
        if(!state.coverage)return;
        let xmin=c2.scales.x.min-meta.xShift,xmax=c2.scales.x.max-meta.xShift;
        // The hand-owned bound may extrapolate, but the far pinned edge is
        // represented by a bucket midpoint on downsampled charts. Preserve
        // that edge's complete source coverage instead of truncating it.
        if(state.which==='min')xmax=state.coverage.hi;else xmin=state.coverage.lo;
        el.dispatchEvent(new CustomEvent('kymo-zoom',{detail:{xmin:xmin,xmax:xmax},bubbles:true}));
      }
    };
    let onup=function(e2){if(!e2||e2.button===0){if(e2)suppressDragClick(ev,e2);finish(true);}};
    let oncancel=function(){finish(false);};
    let onkey=function(e2){if(e2.key==='Escape')finish(false);};
    let onmove=function(ev2){
      let c2=window.__kymo_charts['__KYMO_ID__'];
      if(!ad||!c2)return;
      // mouseup outside the window never arrives — finish when the button
      // state says the drag is over.
      if((ev2.buttons&1)===0){onup();return;}
      let r2=el.getBoundingClientRect();
      let p1=ev2.clientX-r2.left-ad.left;
      ad.moved=Math.abs(p1-ad.p0)>=1;
      if(ad.moved)el.__kymo_skipUnzoomClick=true;
      suppressDragClick(ev,ev2);
      // Exact pin inside the plot box: v0 stays under the cursor, the far
      // edge stays pinned. Past the pixel clamps — including the cursor
      // leaving the panel — the pull continues exponentially at the rate
      // the 1/p asymptote has at the clamp (C1 handoff), so dragging
      // off-panel keeps revealing instead of saturating at the edge. The
      // clamp sits 25px in: that pins the handoff at an e-fold per 25px
      // (~1 decade per 58px) — a controllable pull — and softens only the
      // last pixels, which the raw asymptote made hyper-sensitive anyway.
      // The blo/bhi float-safety bounds are unreachable by hand; without
      // them the asymptote overflowed, and on log landed min on 0, where
      // uPlot's per-decade tick walk never terminates.
      let m=Math.min(25,ad.w/4);
      let q=Math.min(Math.max(p1,m),ad.w-m);
      if(ad.which==='min'){
        // Fitting [v0, max] into [q, w] gives span (mx-v0)*w/(w-q).
        let lnS=Math.log(ad.mx-ad.v0)+Math.log(ad.w/(ad.w-q))+(p1-q)/(ad.w-q);
        c2.setScale('x',{min:ad.inv(Math.max(ad.mx-Math.exp(lnS),ad.blo)),max:ad.inv(ad.mx)});
      }else{
        // Mirror: min pinned at the left edge, span (v0-mn)*w/q.
        let lnS=Math.log(ad.v0-ad.mn)+Math.log(ad.w/q)-(p1-q)/q;
        c2.setScale('x',{min:ad.inv(ad.mn),max:ad.inv(Math.min(ad.mn+Math.exp(lnS),ad.bhi))});
      }
      // Client-side charts have no release refetch: the pulled window is the new view and must survive live refreshes.
      if(!c2.__kymo_zoom.zr)c2.__kymo_userzoom=true;
    };
    owner={srcId:'__KYMO_ID__',cancel:oncancel,cursor:'ew-resize'};
    window.__kymo_zg=owner;
    window.addEventListener('mousemove',onmove);
    window.addEventListener('mouseup',onup);
    window.addEventListener('blur',oncancel);
    window.addEventListener('keydown',onkey);
  });
}
return'';
}catch(error){
  return error&&error.stack?String(error.stack):String(error);
}finally{
  // This script returns from Dioxus's evaluator wrapper so Eval::join can
  // receive the status. Close explicitly because that return bypasses the
  // wrapper's injected close and would otherwise retain one channel per
  // chart recreation.
  dioxus.close();
}
})();
