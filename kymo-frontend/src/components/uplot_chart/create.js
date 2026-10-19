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
// The chart takes its container's box, as the ResizeObserver below does, so height is not structural config and a height change resizes in place. A hidden container has no box: it falls back to 600 wide at its CSS height.
// This and the ResizeObserver floor the width, because uPlot mishandles a fractional one; a whole width gives a whole plot, since uPlot rounds the y axis's size up and the padding below is whole.
// uPlot draws the cursor line at a rounded x but hides it once that x passes the plot's unrounded width (elTrans in uPlot's dom.js), so from a fraction of .5 the line vanishes on right-edge hovers, which hover_points.js snaps to the plot's edge.
// Its canvas bitmap is round(w * dpr) pixels stretched over w * dpr device pixels, which smears 1px lines when that isn't whole.
// Rounding the container in CSS fails at browser zoom, where layout measures a whole CSS width a hair short (596.989 for 597).
let box=el.getBoundingClientRect(),w=Math.floor(box.width)||600,h=box.height||parseFloat(getComputedStyle(el).height);
// The x axis a client-side zoom's window is in: its kind (the sync key) and its scale. Its log shift follows from them: relative time on a log axis always starts at 0, and wall time and custom x never shift.
let axisKey='__KYMO_SYNC_KEY__|__KYMO_X_DISTR__';
// A client-side zoom lives only on the instance, so a rebuild for anything but the x axis (a recolor, a rename, a run's first NaN or spread bucket) carries it over rather than resetting what the user zoomed to. Step charts zoom through the step store and never set it.
// During an axis pull on this chart the window carried is the one the pull started from (its `start`): the pull, cancelled below, has already moved the scale, and uPlot commits its restore later. A selection moves no scale before its release.
let gesture=window.__kymo_zg&&window.__kymo_zg.srcId==='__KYMO_ID__'?window.__kymo_zg.start:null;
let was=prev&&(gesture||{min:prev.scales.x.min,max:prev.scales.x.max,userzoom:prev.__kymo_userzoom});
// Limit: a second create before uPlot's commit microtask, which no path is known to make, would carry the first instance's still-null scale and draw blank until a reset.
let keepZoom=was&&was.userzoom&&prev.__kymo_axiskey===axisKey?{min:was.min,max:was.max}:null;
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
let isLogX=__KYMO_X_DISTR__===3;
// Tooltip
// Readouts live under <body>, outside the charts' stacking contexts, so they paint over the docked options panel (z-index 70 against its 60) and a section's move never takes them along. The chart's teardown removes its readout.
let tip=document.getElementById('__KYMO_ID__-tip');
if(!tip){tip=document.createElement('div');tip.id='__KYMO_ID__-tip';tip.className='kymo-tip';document.body.appendChild(tip);}
// Hidden until this instance's hover hook shows it on the next move or synced update, so a failed rebuild leaves no stale tooltip.
tip.style.display='none';
// What the plotted x adds to the real one: a zero-present log x plots x+1 in the wire's units, 1 on a step axis and 1 ms on a time one. Labels and readouts take it off, and so does every window handed between charts, which each receiving chart puts its own back on. xShift is that same 1 on a step axis, whose zoom geometry (AXIS_LOG1P, bucket evidence) works in it; a time axis keeps its geometry unshifted.
let readoutXShift=__KYMO_READOUT_X_SHIFT__;
let xShift=zoomRefetch?readoutXShift:0;
// The decimal digits s needs, up to `cap` (8 by default). The comparison with 1e-6 is inclusive, so an increment of exactly 1e-6 needs six.
let decFor=function(s,cap){
  var dec=0;
  while(dec<(cap||8)&&Math.abs(s*Math.pow(10,dec)-Math.round(s*Math.pow(10,dec)))>=1e-6)dec++;
  return dec;
};
// The formatters are kept, as wallFormats are: toLocaleString with options builds one per call, about 8 us in headless Chromium, and a label can make two.
let numFormats={};
let fmtNum=function(v,grouping,fd){
  var key=grouping+'|'+fd;
  if(!numFormats[key])numFormats[key]=new Intl.NumberFormat(undefined,grouping?{maximumFractionDigits:fd}:{useGrouping:false,maximumFractionDigits:fd});
  return numFormats[key].format(v);
};
// Value formatter for Y and X. Zero is just "0": fixed-precision formatting rendered step 0 as "0.00" (AI-1385).
// Precision comes from the grain, not fixed digit counts. On linear axes the grain is the tick spacing (`step`): decFor(step) is however many digits the spacing needs, and adjacent ticks can never render identically (toExponential(1) used to collapse 1.50e6/1.52e6/1.54e6 into three "1.5e6" labels).
// A step of 0 (a log ladder) has no meaningful axis-wide spacing, so the grain is the value itself; log values are integral mantissas, which print exactly (the natural 1k, 10k, 100k, 1M ladder), and the splits a log y axis's filter drops arrive as null and print blank.
// Values >=1000 wear k/M/B/T suffixes; of the largest unit fitting the grain and the unit below it, whichever renders narrower wins — the decimal point costs a digit slot, so "1547k" beats "1.547M" but "2.5M" beats "2500k"; ties keep the larger unit.
// Rounding cannot merge linear neighbors because unit <= step makes consecutive tick mantissas differ in their integer digits. Linear spacings under 1000 (deep y-zoom) keep plain grouped digits; fl floors every sub-1 label at its leading digit, including near-constant metrics whose tiny step decFor's 1e-6 tolerance swallows to 0.
let numFmt=function(vals,step){
  var pv=!(step>0);
  return vals.map(function(v){
    if(v==null)return'';
    if(v==0)return'0';
    var a=Math.abs(v);
    if(a<0.001)return v.toExponential(decFor((pv?a:step)/Math.pow(10,Math.floor(Math.log10(a)))));
    if(a>=1000&&(pv||step>=1000)){
      var us=[[12,'T'],[9,'B'],[6,'M'],[3,'k'],[0,'']],lim=pv?a:step,j=0,e=v.toExponential().split('e');
      while(Math.pow(10,us[j][0])>lim)j++;
      // The value's own decimal digits, moved to the unit: dividing by it can land an ulp off, and 5.5e26 read 549999999999999.94T.
      var lab=function(k,fd){return fmtNum(+(e[0]+'e'+(e[1]-us[k][0])),false,fd)+us[k][1]};
      var s1=lab(j,3),s2=lab(j+1,0);
      return s1.length<=s2.length?s1:s2;
    }
    var fl=a<1?-Math.floor(Math.log10(a)):0;
    return fmtNum(v,true,pv?fl+2:Math.max(decFor(step),fl));
  });
};
// The y axis's grain is the increment uPlot chose, as a linear x axis's is; a log y axis labels each tick by its own value.
let yFmt=function(u,vals,ax,space,incr){return numFmt(vals,__KYMO_Y_DISTR__===3?0:incr)};
let sameWallDay=function(a,b){
  var da=new Date(a*1000),db=new Date(b*1000);
  return da.getFullYear()===db.getFullYear()&&da.getMonth()===db.getMonth()&&da.getDate()===db.getDate();
};
// `fd` fractional digits of seconds (none by default); `dateOnly` drops the time, for linear wall ticks a day or more apart, which wallSplits puts on local midnights. Naming any time field drops the locale's default fields, so all are named. The formatters are kept: building one per label costs about 25 us.
let wallFormats={};
let fmtWall=function(v,withDate,fd,dateOnly){
  // To the millisecond: Date truncates, and a whole-millisecond tick k/1000 s times 1000 can come back just under k (for times such as 2038-01-19 to 2039-09-07, just below 2^41 ms), which would read a millisecond early.
  var d=new Date(Math.round(v*1000));
  if(dateOnly)return d.toLocaleDateString();
  // Limit: fractional seconds need a time within Date's range (about 275,000 years either way): past it Intl's format would throw inside uPlot's commit. Whole-second labels there read Invalid Date.
  if(!fd)return withDate?d.toLocaleString():d.toLocaleTimeString();
  var key=withDate+'|'+fd;
  if(!wallFormats[key])wallFormats[key]=new Intl.DateTimeFormat(undefined,Object.assign(withDate?{year:'numeric',month:'numeric',day:'numeric'}:{},{hour:'numeric',minute:'2-digit',second:'2-digit',fractionalSecondDigits:fd}));
  return wallFormats[key].format(d);
};
// Relative time as a stopwatch shows it: seconds under a minute, else m:ss, or h:mm:ss once hours do, with `digits` digits of seconds. The fields come from magnitudes rounded to that precision, the value's and `top`'s (the largest value in the list, so a list's labels all have the same fields): at one digit 59.96 s reads 1:00.0, not 60.0s or 0:60.0, and 3599.96 s reads 1:00:00.0. Counted in whole units of the last digit, so no float remainder can print 0:60.
let fmtRel=function(v,digits,top){
  var q=Math.pow(10,digits),n=Math.round(Math.abs(v)*q),t=Math.round(top*q),sign=v<0?'-':'';
  if(t<60*q)return sign+(n/q).toFixed(digits)+'s';
  var m=Math.floor(n/(60*q)),rest=n-m*60*q,sec=(rest/q).toFixed(digits);
  if(rest<10*q)sec='0'+sec;
  return sign+(t<3600*q?m:Math.floor(m/60)+':'+(m%60<10?'0':'')+m%60)+':'+sec;
};
// X tick labels at real x, from the ticks' spacing `step`. Time ticks take their precision from it, as numbers do, so neighbours never print alike: fractional digits only where the spacing needs them (a 0.5 s spacing gets one; the increment tables keep time ticks whole milliseconds apart). A log ladder (step 0) has no common spacing: each tick takes its own digits and fields, though wall time stops at the three digits Intl prints, which throws on more.
let xLabels=!isTimeAxis?function(u,vals,step){return numFmt(vals,step)}:function(u,vals,step){
  var s=u.scales.x,digits=decFor(step,3),top=0,withDate=isWallTime&&!sameWallDay(s.min,s.max),dateOnly=!isLogX&&step>=86400;
  if(!isWallTime&&step)for(var i=0;i<vals.length;i++)top=Math.max(top,Math.abs(vals[i]));
  return vals.map(function(v){var d=step?digits:decFor(v,isWallTime?3:9);return isWallTime?fmtWall(v,withDate,d,dateOnly):fmtRel(v,d,step?top:Math.abs(v))});
};
// The increment, ticks and labels pickTicks last chose, the ticks as uPlot was handed them.
let shown={};
// uPlot's values callback: the labels pickTicks measured, for the very list it returned (uPlot passes it through the filter unchanged). Any other list is labelled at real x (log(x+1) ticks arrive shifted), by each tick's own value on a log axis and, on a linear one, which only a direct call hands another list, at `incr`.
let xFmt=function(u,vals,ax,space,incr){
  if(vals===shown.ticks)return shown.labels;
  if(readoutXShift)vals=vals.map(function(v){return v-readoutXShift});
  return xLabels(u,vals,isLogX?0:incr);
};
// Each axis's linear increments, which a log axis also ticks at under a decade. Numbers: uPlot's own (1, 2, 2.5 and 5 times each power of ten, spelled as it spells them); a step axis takes the whole ones, since steps are integers. Time: round durations, so the stopwatch labels read 0:05:00 and 12:00:00 and wall clocks 3:15:00, rather than uPlot's numeric 0:08:20 and 13:53:20 (and 3:16:40). The tables are built once per page and reused by every chart create, each of which would build them cold.
if(!window.__kymo_incrs){
  let num=[];
  for(let e=-32;e<32;e++)for(let m of[1,2,2.5,5])num.push(+(m+'e'+e));
  let time=[0.001,0.002,0.005,0.01,0.02,0.05,0.1,0.2,0.5,1,2,5,10,15,30,60,120,300,600,900,1800,3600,7200,10800,21600,43200,86400,172800,259200,604800,1209600,2592000,5184000,7862400,15724800,31536000];
  // Past a year, 2, 5 and 10 times each power of ten of years, to ten million years. Limit: a window pulled wider than 50 px of the largest increment (about 70 million years at 400 px, 230 million at 1200 px) gets no ticks on a linear axis, and a log one ticks on its ladder.
  for(let year=31536000;year<1e14;year*=10)time.push(2*year,5*year,10*year);
  window.__kymo_incrs={num:num,step:num.filter(function(v){return v%1===0}),time:time};
}
let incrs=window.__kymo_incrs[isTimeAxis?'time':zoomRefetch?'step':'num'];
// Label widths in CSS px, in axis `axisIdx`'s font.
let measurer=function(u,axisIdx){
  u.ctx.font=u.axes[axisIdx].font[0];
  return function(label){return u.ctx.measureText(label).width/devicePixelRatio};
};
// Room a label w px wide needs between tick centres: uPlot's 50 px minimum, and the label plus an 18 px gap.
let room=function(w){return Math.max(50,Math.ceil(w)+18)};
// The multiples of st in [lo,hi], as the decimals they spell: st's digits times k, then its exponent (k*st carries float error into labels: 28 times 1e24 is 2.7999999999999996e25, "27999999999999996B"). Null past 2^53, where that product rounds and the counter stops advancing.
let multiples=function(lo,hi,st){
  let [digits,exp]=st.toExponential().split('e'),mult=+digits.replace('.',''),shift=exp-(digits.split('.')[1]||'').length,k0=Math.ceil(lo/st-1e-9),k1=Math.floor(hi/st+1e-9),out=[];
  if(!(Math.max(Math.abs(k0),Math.abs(k1))*mult<2**53))return null;
  for(let k=k0;k<=k1;k++)out.push(+(k*mult+'e'+shift));
  return out;
};
// Whether neighbouring labels a and b, wa and wb px wide, print apart and fit `gap` px apart.
let fits=function(a,b,wa,wb,gap){return a!==b&&room(Math.max(wa,wb))<=gap};
// The first of the axis's increments, from the finest uPlot's 50 px minimum allows, whose ticks (ticksAt, placed by at) all fit, labelled as the axis prints them: a coarser increment can print more digits, labels change width across a window (an exponent, a date), and a spacing finer than decFor resolves prints neighbours alike. A window too narrow for two labels still gets one tick (an increment's only multiple in it, or the crowded increment's tick nearest its middle) rather than none from a coarser increment with no multiple in it. Returns the increment, its ticks and their labels.
let pickTicks=function(u,axisIdx,lo,hi,dim,ticksAt,at){
  let width=measurer(u,axisIdx),crowded=null;
  for(let st of incrs){
    if(st<(hi-lo)*50/dim)continue;
    let ticks=ticksAt(lo,hi,st),n=ticks&&ticks.length;
    if(!ticks)continue;
    if(!n)break;
    crowded={step:st,ticks:ticks};
    // The last pair first: log gaps shrink and linear labels mostly widen there, so most crowded increments go no further than two labels.
    if(n>1){
      let pair=xLabels(u,ticks.slice(n-2),st);
      if(!fits(pair[0],pair[1],width(pair[0]),width(pair[1]),at(ticks[n-1])-at(ticks[n-2])))continue;
    }
    let labels=xLabels(u,ticks,st),w=labels.map(width),i=1;
    while(i<n&&fits(labels[i-1],labels[i],w[i-1],w[i],at(ticks[i])-at(ticks[i-1])))i++;
    if(i===n)return{step:st,ticks:ticks,labels:labels};
  }
  if(!crowded)return{step:Infinity,ticks:[],labels:[]};
  let mid=crowded.ticks.reduce(function(a,b){return Math.abs(at(b)-dim/2)<Math.abs(at(a)-dim/2)?b:a});
  return{step:crowded.step,ticks:[mid],labels:xLabels(u,[mid],crowded.step)};
};
// Wall ticks fall on round local times: multiples of the increment on the local clock, so ticks stay on the clock's grid across a DST change. Increments of 30 days and up tick at the starts of local months, every n-th from January of year 0. Limits: a window starting in Date's first months or years (about 271,800 BC) gets none, its first start lying before Date's range; Date reports whole-minute offsets, so where a zone's offset had seconds (local mean time, before about 1900) ticks sit those seconds off the round time.
let wallSplits=function(mn,mx,incr){
  let out=[];
  if(incr>=2592000){
    // Counted in months from year 0, n months a tick: the increment in twelfths of 365 days (1, 2, 3 and 6, then 12 a year).
    let n=Math.round(incr/2628000),d=new Date(mn*1000),k=Math.floor((d.getFullYear()*12+d.getMonth())/n)*n;
    for(;;k+=n){
      // setFullYear, unlike the Date constructor, reads years 0 to 99 as written; it carries months past 11 into the year.
      d.setFullYear(0,k,1);
      d.setHours(0,0,0,0);
      let t=d.getTime()/1000;
      if(!(t<=mx))return out;
      if(t>=mn)out.push(t);
    }
  }
  // Up to an hour, ticks step evenly through a DST change, which shifts the clock by a whole number of them (but Lord Howe's 30 minutes against hourly ticks): a repeated hour ticks twice and a skipped one not at all.
  // A longer increment places each local time at its own offset, the last tick's tried first: a time the change repeats ticks once, at its first instant, and one it skips ticks where Date reads it, as late after the change as it falls in the skipped hour (a midnight change's day tick at 1 AM).
  // A spring-forward also brings the tick after it an hour closer than the increment (midnight and 3 AM 2 hours apart on 3-hour ticks): under 90% of it, that tick gives way, so labels keep the spacing they were picked for.
  let offset=function(t){return -new Date(t*1000).getTimezoneOffset()*60||0},o=offset(mn),k=Math.ceil((mn+o)/incr);
  // Past 2^52 the counter stops advancing (a window millions of years out, where Date has given up anyway).
  if(!(Math.abs(k)<2**52))return out;
  for(;;k++){
    let t=k*incr-o;
    if(incr>3600){
      let o2=offset(t);
      if(o2!==o){let t2=k*incr-o2;if(offset(t2)===o2)t=t2;o=o2;}
    }
    if(!(t<=mx))return out;
    if(t>=mn&&!(t-out[out.length-1]<incr*0.9))out.push(t);
  }
};
// Log x ticks, on every log x axis. uPlot's own log splits fall only on whole multiples of each decade's power of ten (a zoom to 2100..2900 gets none), and the vendored uPlot's log label filter keeps only the powers on a horizontal axis (it compares signed pixel distances; upstream master takes their abs()). So ticks fall on nice real x, in the axis's units (steps, seconds or the custom metric), plotted at x + readoutXShift: a zero-present axis plots its real 0 at 1 step or 1 ms.
// Neighbours keep room() apart where they land. Under a decade the axis ticks where pickTicks puts its linear increments, placed on log geometry; wider, at the densest tier below that fits. A zero-present axis keeps its 0, dropping the first tick after it if they collide.
let xSplits=function(u,axisIdx,smin,smax){
  // Real x has no data in (0, S) on a zero-present axis, nor below a step on a step axis, and time labels print nothing finer than a microsecond, wall clocks a millisecond: the ladder starts there.
  let S=readoutXShift,lo=Math.max(smin-S,0),hi=smax-S,first=Math.max(lo,S,zoomRefetch?1:isWallTime?0.001:isTimeAxis?1e-6:0);
  if(!(hi>lo))return[smin,smax];
  // Pixels per decade, from differences of logs: a pulled window's max over its min can overflow. Limit: in a window narrower than about 1e-13 of its magnitude those differences are a few ulps, and ticks space unevenly.
  let dim=u.bbox.width/devicePixelRatio,decade=dim/(Math.log10(smax)-Math.log10(smin));
  // How much farther apart ticks at real a and b must be to fit labels wa and wb wide: 1 or less fits.
  let crowding=function(a,b,wa,wb){return room(Math.max(wa,wb))/(decade*(Math.log10(b+S)-Math.log10(a+S)))};
  if(hi<10*first){
    let base=Math.log10(smin),picked=pickTicks(u,axisIdx,lo,hi,dim,multiples,function(v){return decade*(Math.log10(v+S)-base)});
    if(picked.ticks.length){
      shown={step:picked.step,ticks:picked.ticks.map(function(v){return v+S}),labels:picked.labels};
      return shown.ticks;
    }
  }
  // Tier s: 0 is 1, 2 and 5 times each power, s >= 1 every s-th power. A pulled window can span hundreds of decades, so tiers whose neighbours are closer than 50 px wherever they land are skipped unmeasured, and a crowded tier of powers jumps to the spacing its most crowded pair needs. One tick always fits, and a window between ladder values (only a pull past every increment reaches one) gets none.
  let K0=Math.floor(Math.log10(first)),K1=Math.ceil(Math.log10(hi)),width=measurer(u,axisIdx),out,widths;
  for(let s=decade*Math.log10(2)>=50?0:Math.ceil(50/decade);;){
    out=[];
    for(let k=s?Math.ceil(K0/s)*s:K0;k<=K1;k+=s||1)for(let mant of s?[1]:[1,2,5]){let v=+(mant+'e'+k);if(v>=first&&v<=hi)out.push(v)}
    // A stride can pass every power in the window (3 across 1.1e24..1.1e26): the power nearest its middle stands alone.
    if(!out.length){let v=+('1e'+Math.round((Math.log10(first)+Math.log10(hi))/2));if(v>=first&&v<=hi)out.push(v)}
    widths=xLabels(u,out,0).map(width);
    let worst=0;
    for(let i=1;i<out.length;i++)worst=Math.max(worst,crowding(out[i-1],out[i],widths[i-1],widths[i]));
    if(!(worst>1))break;
    s=Math.max(s+1,Math.ceil(s*worst));
  }
  if(lo===0){
    if(out.length&&crowding(0,out[0],width(xLabels(u,[0],0)[0]),widths[0])>1)out.shift();
    out.unshift(0);
  }
  return out.map(function(v){return v+S});
};
// A log axis's one increment, Infinity, fits any window, so uPlot always calls xSplits (it skips the call when none fits); its labels bypass uPlot's log filter.
let xTicks={splits:xSplits,incrs:[Infinity],filter:function(u,s){return s}};
if(!isLogX){
  // A linear axis picks its ticks when uPlot asks for its increments: it returns the one picked, which a spacing of 1 always lets through, and splits hands over the ticks picked (or, called directly for another increment, places that one's). Limit: uPlot also refuses an increment under 5 whose decimals and the window's integer digits pass 17, so a custom-x window narrower than about 1e-15 of its magnitude, or than 1e-15 below 1, gets no ticks.
  let ticksAt=isWallTime?wallSplits:multiples;
  xTicks={
    space:1,
    incrs:function(u,axisIdx,mn,mx,dim){
      shown=pickTicks(u,axisIdx,mn,mx,dim,ticksAt,function(v){return dim*(v-mn)/(mx-mn)});
      return[shown.step];
    },
    splits:function(u,axisIdx,mn,mx,incr){return incr===shown.step?shown.ticks:ticksAt(mn,mx,incr)||[]},
  };
}
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
  if(isTimeAxis)return fmtRel(v,1,Math.abs(v));
  if(!zoomRefetch)return v.toPrecision(4);
  return fmtNum(v,true,2);
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
// uPlot walks a log y axis decade by decade from floor(log10(min)); a min <=0
// (log10 -> -Infinity) or a non-finite bound (an x-zoom that caught no points
// leaves uPlot's auto min/max at +-Infinity) makes that walk never terminate
// and freezes the tab, as does any min below about 1e-25, where the walk rounds its ticks to 0. logClamp is the one rule both log scales pass through:
// keep a finite positive bound, else fall back to the data's own positive
// extent (lo,hi); nothing positive at all -> a neutral decade.
let logClamp=function(mn,mx,lo,hi){
  if(!(lo<=hi))return[1,10];
  return[(isFinite(mn)&&mn>0)?mn:lo,(isFinite(mx)&&mx>0)?mx:hi];
};
// Log-x range: pin to the zoom's own endpoints, where uPlot's default snaps to powers of 10; data[0] is ascending and >0
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
  scales:{x:{time:false,distr:__KYMO_X_DISTR__,...(isLogX?{range:logSafeX}:{}),auto:u=>!u.__kymo_userzoom},...__KYMO_SYNC_SCALE__,y:{distr:__KYMO_Y_DISTR__,range:__KYMO_Y_DISTR__===3?yNiceLog:yNice},nan:{range:[0,1]}},
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
    {stroke:axColor,grid:{show:false},ticks:{stroke:gridColor,width:1,size:5},font:'__KYMO_AXIS_FONT_SIZE__px sans-serif',gap:6,size:__KYMO_X_AXIS_SIZE__,values:xFmt,...xTicks},
    // size is a function: the axis grows to fit its widest label (uPlot re-asks on every scale change) until the narrow-cell plot floor below wins. Every label is measured — with proportional glyphs the longest string isn't always the widest. The preference-scaled floor keeps plot left edges aligned across a grid column of short-label charts; the cycleNum guard is the uPlot autosize idiom.
    {stroke:axColor,grid:{stroke:gridColor,width:1},ticks:{stroke:gridColor,width:1,size:5},font:'__KYMO_AXIS_FONT_SIZE__px sans-serif',gap:6,
     size:function(u,values,axisIdx,cycleNum){
       let ax=u.axes[axisIdx];
       if(cycleNum>1)return ax._size;
       let measured=Math.max(__KYMO_Y_AXIS_MIN_SIZE__,Math.ceil(ax.ticks.size+ax.gap+Math.max(0,...(values||[]).map(measurer(u,axisIdx)))));
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
// Also read by chart_alive (uplot_chart.rs): a chart missing here is recreated at its next reply, even one that changes nothing.
window.__kymo_charts['__KYMO_ID__']=u;
u.__kymo_cfghash='__KYMO_CFG_HASH__';
u.__kymo_axiskey=axisKey;
if(keepZoom){u.setScale('x',keepZoom);u.__kymo_userzoom=true;}
// Dynamic metadata read by the once-installed gesture listeners. Keep it on
// the current uPlot instance so a config rebuild cannot leave those listeners
// using the first build's x-range column offsets or log transform.
u.__kymo_zoom={zr:zoomRefetch,xShift:xShift,readoutXShift:readoutXShift,xrBase:hasXr?xrBase:null,axis:isLogX?(xShift?AXIS_LOG1P:AXIS_LOG):AXIS_LINEAR};
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
      // The box is the chart's size, its width floored as at creation. Its height is fixed by CSS (--kymo-chart-height), which the chart can't inflate, so this setSize never re-triggers the observer. Width compares within 1 px, the most the floor takes off, so the observer's first call, just after each create, resizes nothing; height compares exactly, so a 1 px height change still lands.
      if(Math.abs(c.width-r.width)<=1&&c.height===r.height)return;
      let owner=window.__kymo_zg;
      c.setSize({width:Math.floor(r.width),height:r.height});
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
// A client-side zoom's one commit, for a selection and an axis pull alike: the real-x window goes to every chart in the sync group but `except` (an axis pull's source, already showing it). A log-x peer takes the part it can show: a lower bound at or below 0 (a pull past a linear chart's data reaches there) is raised to its first plotted x by its range function (logSafeX); a window ending before that x, which logSafeX would turn inside out, leaves the peer alone.
let applyClientRange=function(sync,range,except){
  eachPeer(sync,function(c,m){
    if(c===except)return;
    let lo=range.lo+m.readoutXShift,hi=range.hi+m.readoutXShift;
    if(c.scales.x.distr===3&&!(lo>0)&&!(hi>c.data[0][0]))return;
    c.setScale('x',{min:lo,max:hi});c.__kymo_userzoom=true;
  });
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
      let realLo=c.scales.x.min-m.readoutXShift,realHi=c.scales.x.max-m.readoutXShift;
      let lo=Math.max(range.lo,realLo),hi=Math.min(range.hi,realHi);
      if(!(Number.isFinite(lo)&&Number.isFinite(hi)&&hi>lo)){c.setSelect(hide,false);return;}
      let p0=c.valToPos(lo+m.readoutXShift,'x'),p1=c.valToPos(hi+m.readoutXShift,'x');
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
    // The map reads the zoom geometry's x; toReal takes the rest of the plotted shift off, for the real x the gesture's windows are in.
    return map?{source:c,srcId:'__KYMO_ID__',sync:c.cursor.sync.key,zr:m.zr,supports:supports,map:map,lo:visualLo,hi:visualHi,toReal:m.readoutXShift-m.xShift}:null;
  };
  let endpoint=function(s,x){
    // Client-only charts retain their exact, plot-clamped selection. Step
    // charts extrapolate without bound in the frozen source transform.
    // A clamped edge is the scale's own bound: through log and back, a shifted chart's 0.001 (real 0) returns as 0.0010000000000000002, a real 2e-19, which would start a shifted peer's window just past its first point and drop its 0s tick.
    if(!s.zr){if(x<=s.map.plotLeft)return s.lo;if(x>=s.map.plotLeft+s.map.width)return s.hi;}
    return mapSourcePageX(s.map,x);
  };
  let nominal=function(s){
    let a=endpoint(s,s.downX),b=endpoint(s,s.lastX);
    if(!(Number.isFinite(a)&&Number.isFinite(b)))return null;
    return {lo:Math.min(a,b)-s.toReal,hi:Math.max(a,b)-s.toReal};
  };
  let resolved=function(s){
    let r=nominal(s);
    if(!r)return null;
    return s.zr?expandWholeBuckets(s.supports,r.lo,r.hi):r;
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
      }else applyClientRange(s.sync,range,null);
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
    // The drag is direct manipulation
    // and must respond for as long as the hand moves — bounding at the
    // data extent went dead the moment a full view had nothing left to
    // reveal. Empty space past the data is what the hand asked for, and a
    // released pull snaps a step axis back to data via the refetch.
    // In transformed units: on log, exp(-690) and exp(690) keep both bounds positive and finite; on linear, comfortably sub-overflow.
    let d0=c.data[0];
    if(!d0.length)return; // log-x can trim every point; nothing to pull
    let zm=c.__kymo_zoom,frozen=stepSupports(c,zm);
    if(zm&&zm.zr&&!frozen)return;
    // lo and hi are the scale's own bounds, which the pinned edge and a cancel's restore keep exactly, as a selection's clamped edge does (endpoint).
    let ad={which:p0<st.w/2?'min':'max',mn:mn,mx:mx,lo:c.scales.x.min,hi:c.scales.x.max,v0:mn+p0/st.w*(mx-mn),p0:p0,left:st.left,w:st.w,moved:false,inv:isLog?Math.exp:function(v){return v},blo:isLog?-690:-1e300,bhi:isLog?690:1e300,coverage:frozen&&frozen.coverage,userzoom:!!c.__kymo_userzoom};
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
      if(!commit&&c2&&state){c2.setScale('x',{min:state.lo,max:state.hi});c2.__kymo_userzoom=state.userzoom;}
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
      }else{
        // The pulled window is the group's, as a selection's is; peers take it once, at release, not on every move.
        applyClientRange(c2.cursor.sync.key,{lo:c2.scales.x.min-meta.readoutXShift,hi:c2.scales.x.max-meta.readoutXShift},c2);
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
      // The blo/bhi bounds keep the asymptote from overflowing.
      let m=Math.min(25,ad.w/4);
      let q=Math.min(Math.max(p1,m),ad.w-m);
      if(ad.which==='min'){
        // Fitting [v0, max] into [q, w] gives span (mx-v0)*w/(w-q).
        let lnS=Math.log(ad.mx-ad.v0)+Math.log(ad.w/(ad.w-q))+(p1-q)/(ad.w-q);
        c2.setScale('x',{min:ad.inv(Math.max(ad.mx-Math.exp(lnS),ad.blo)),max:ad.hi});
      }else{
        // Mirror: min pinned at the left edge, span (v0-mn)*w/q.
        let lnS=Math.log(ad.v0-ad.mn)+Math.log(ad.w/q)-(p1-q)/q;
        c2.setScale('x',{min:ad.lo,max:ad.inv(Math.min(ad.mn+Math.exp(lnS),ad.bhi))});
      }
      // Client-side charts have no release refetch: the pulled window is the new view and must survive live refreshes. A hand within a pixel of its press marks no zoom, though its window may have moved by less than a pixel.
      if(!c2.__kymo_zoom.zr)c2.__kymo_userzoom=ad.moved||ad.userzoom;
    };
    owner={srcId:'__KYMO_ID__',cancel:oncancel,cursor:'ew-resize',start:{min:c.scales.x.min,max:c.scales.x.max,userzoom:ad.userzoom}};
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
