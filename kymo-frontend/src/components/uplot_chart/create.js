return(()=>{
try{
if(!window.__kymo_zm){
__KYMO_ZOOM_MATH__
window.__kymo_zm=KYMO_ZOOM_MATH;
}
let {AXIS_LINEAR,AXIS_LOG,AXIS_LOG1P,buildFrozenSupports,buildSourceMap,mapSourcePlotX,expandWholeBuckets}=window.__kymo_zm;
let {nullGaps,createHoverPointLookup,plotPosition}=window.__kymo_hp;
if(!window.__kymo_charts) window.__kymo_charts={};
let el=document.getElementById('__KYMO_ID__');
if(!el)return'chart container is missing';
let data=window.__kymo_data&&window.__kymo_data['__KYMO_ID__'];
if(!data||!data.length)return'chart data is missing';
data=nullGaps(data);
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
// This chart's axis pull, while one holds it: the gesture owner with the pull's `range` (a selection's owner, with the same srcId, has none).
let pulling=function(){let zg=window.__kymo_zg;return zg&&zg.srcId==='__KYMO_ID__'&&zg.range?zg:null;};
// A client-side zoom lives only on the instance, so a rebuild for anything but the x axis (a recolor, a rename, a run's first NaN or spread bucket) carries it over rather than resetting what the user zoomed to. Step charts zoom through the step store and never set it.
// During an axis pull on this chart the window carried is the one the pull started from (its own lo, hi and userzoom), not the scale it has moved: its cancel below only queues the restore, which destroy drops. A selection moves no scale before its release.
let was=prev&&(pulling()||{lo:prev.scales.x.min,hi:prev.scales.x.max,userzoom:prev.__kymo_userzoom});
// Limit: a second create before uPlot's commit microtask, which no path is known to make, would carry the first instance's still-null scale: a fixed zoom shows its full extent but stays marked zoomed until a reset, and a following one is left without an x minimum.
let keepZoom=was&&was.userzoom&&prev.__kymo_axiskey===axisKey?was:null;
if(prev){
  // A structural source rebuild replaces the instance the active gesture reads, and with it a pull's gesture-start geometry and slot snapshot. Peer rebuilds are harmless and repaint after registration.
  let zg=window.__kymo_zg;
  if(zg&&zg.srcId==='__KYMO_ID__')zg.cancel();
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
// xnanC, hasRaw, hasRange and isSmoothed are read only by the embedded hover.js (the tooltip), which shares this closure.
let xnanC=[__KYMO_XNAN_COUNTS__];
let hasRaw=__KYMO_HAS_RAW__;
let hasRange=__KYMO_HAS_RANGE__;
// Draw order: run i's raw/envelope block ([raw,] min, max) at 1+i*rawStride, its values line at lineBase+i — every values line strokes above every block.
let rawStride=__KYMO_RAW_STRIDE__;
let lineBase=1+labels.length*rawStride;
// Smoothing on: the values column is a stroked smoothed curve; its raw evidence rides in the raw column (passthrough) or the envelope (downsampled).
let isSmoothed=__KYMO_SMOOTHED__;
let nanCols=__KYMO_NAN_MARKERS__;
// Data-only marker columns live at nanBase+i — after the values lines.
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
// What the plotted x adds to the real one: a zero-present log x plots x+1 in the wire's units, 1 on a step axis and 1 ms on a time one. Labels and readouts take it off, and so does every window handed between charts, which each receiving chart puts its own back on.
let readoutXShift=__KYMO_READOUT_X_SHIFT__;
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
// Log x ticks, on every log x axis. uPlot's own log splits fall only on whole multiples of each decade's power of ten (a zoom to 2100..2900 gets none), and its log label filter keeps a label only by its leading digit. So ticks fall on nice real x, in the axis's units (steps, seconds or the custom metric), plotted at x + readoutXShift: a zero-present axis plots its real 0 at 1 step or 1 ms.
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
// Limit: from 1e17 in magnitude up, the vendored uPlot accepts no increment, so no axis gets ticks there (an upstream bug; see assets/vendor/README.md).
// A log axis's one increment, Infinity, fits any window below that, so uPlot calls xSplits (it skips the call when none fits); its labels bypass uPlot's log filter.
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
  // Flat and ulp-noise spans (e.g. smoothed constant metrics) take uPlot's default padded range (uPlot.rangeNum) rather than a rounding that magnifies the noise.
  if(!(span>1e-24)||Math.max(Math.abs(mn),Math.abs(mx))/span>1e10)return uPlot.rangeNum(mn,mx,0.1,true);
  let cap=0.1*span;
  let p=Math.pow(10,Math.floor(Math.log10(cap)));
  let i=cap>=5*p?5*p:cap>=2*p?2*p:p;
  return[Math.floor(mn/i)*i,Math.ceil(mx/i)*i];
};
// Log-x range: the data's own ends, not the powers of 10 uPlot's default would snap to. uPlot calls it only to range data[0], which is ascending and >0 (the build trims x<=0 for log-x); explicit bounds skip it (commitZoom clamps its own).
// No data gets a neutral decade, and a single x value a decade either side, since this replaces uPlot's own single-value widening.
// Limit: from about 1.8e307 up, that decade overflows to Infinity.
let logXRange=function(u,mn,mx){return mn==null?[1,10]:mn===mx?[mn/10,mn*10]:[mn,mx];};
// Log-y counterpart of yNice: bounds snap to m*10^k — m from the coarse
// ladder (1/2/5) when that costs <=10% of the displayed decades in log
// space, the fine ladder as fallback, the raw data bound last. uPlot's
// own log ranging rounds out to whole powers of 10, which on a
// sub-decade chart is mostly empty space.
let yNiceLog=function(u,mn,mx){
  // uPlot's log scan skips values <=0 and passes null when the window has no positive value: a neutral decade.
  if(mn==null)return[1,10];
  let cap=0.1*(Math.log10(mx)-Math.log10(mn));
  if(!(cap>0))return[mn/1.25,mx*1.25];
  let snap=function(v,up){
    let k=Math.pow(10,Math.floor(Math.log10(v)));
    let m=v/k;
    for(const ms of [[1,2,5,10],[1,1.5,2,3,4,5,6,7,8,9,10]]){
      let c=k*(up?ms.find(x=>x>=m-1e-12):ms.filter(x=>x<=m+1e-12).at(-1));
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
// Line series span their gaps and draw samples only as segments, so a series whose column holds one sample on the chart (one raw point, or one bucket) would draw nothing: linePaths builds it a mark and keeps it on the line path, which lasts until the data, scales or size change, the mark's inputs.
// The mark: a dot, or on an envelope's min edge (this series; the max edge is the next) a bar from the bucket's min to its max, points.bar CSS px wide and cut at the plot's top and bottom like its band, unless they're within a dot of each other (canvas y grows downward). A bar cut to nothing at a log axis's floor draws nothing, like its band; the border circle for its nonpositive min marks it.
let loneMark=function(u,si,i){
  let s=u.series[si],sx=u.scales.x,sy=u.scales.y,xv=u.data[0][i],mn=u.data[si][i],mx=s.points.bar?u.data[si+1][i]:mn;
  // A sample outside the scales has none, as uPlot culls its own dots: one past the window's edge, or wholly below a log axis, whose border circle marks it instead.
  if(!(xv>=sx.min&&xv<=sx.max&&mx>=sy.min&&mn<=sy.max))return null;
  let pr=u.pxRatio,dot=s.points.size*pr,bar=s.points.bar*pr,b=u.bbox,x=Math.round(u.valToPos(xv,'x',true)),lo=u.valToPos(mn,'y',true),hi=u.valToPos(mx,'y',true),fill=new Path2D();
  // A dot centered on whole device pixels as uPlot centers its own, and a bar on the columns uPlot strokes a line of its width at that x. The cull keeps a dot within a dot of the plot, where uPlot clips its own, so a mark needs no clip.
  if(lo-hi<dot)fill.arc(x,Math.round((lo+hi)/2),dot/2,0,2*Math.PI);
  else{hi=Math.max(hi,b.top);fill.rect(x-Math.floor(bar/2),hi,bar,Math.min(lo,b.top+b.height)-hi);}
  return{fill};
};
// A line's or min edge's points (create_js.rs), given an envelope mark's fill and bar width (a line's dot takes its stroke, as uPlot fills a point of width 0): points.show, which gates uPlot's drawing, and points.paths, which uPlot would otherwise rebuild on every repaint, both return the mark its line path carries.
let markOf=(u,si)=>u.series[si]._paths.mark;
let lonePoints=(fill,bar)=>({show:markOf,size:5,width:0,fill,bar,paths:markOf});
// A column's first sample, scanned in from its start (dir 1), or its last, from its end (dir -1), found once per data array (setData replaces it); the column's length going forward when it has none. Its gaps are null: the NaNs in kymo's data are nulled before uPlot gets it.
let sampleEnd=function(u,si,dir){
  let r=u.data.__kymo_ends||(u.data.__kymo_ends=[]),k=2*si+(dir<0);
  if(r[k]===undefined){let d=u.data[si],n=d.length,j=dir>0?0:n-1;while(j>=0&&j<n&&d[j]==null)j+=dir;r[k]=j;}
  return r[k];
};
// Every drawn series names linePaths (create_js.rs). uPlot's range runs out to the nearest sample outside the window, or to the column's end where there is none, and uPlot's line builder would scan in from such an end to a sample on every rebuild (live data, zoom, resize), across the whole leading gap of a late-starting column, trailing gap of a stopped one, or all of an empty one; linePaths strokes from sample to sample, taking an end that lands on a gap from sampleEnd. It never returns null, which uPlot would rebuild on every repaint: a column with no sample gets an empty stand-in, kept until the next rebuild.
let emptyPaths={flags:0},linearPaths=uPlot.paths.linear();
let linePaths=function(u,si,i0,i1){
  // An end on a sample stands for the column's first or last: one sample at both is lone, and a sample it hides lies past the window, where loneMark culls a false mark.
  let d=u.data[si],f=d[i0]!=null?i0:sampleEnd(u,si,1);
  if(f===d.length)return emptyPaths;
  let l=d[i1]!=null?i1:sampleEnd(u,si,-1),p=linearPaths(u,si,f,l);
  p.mark=f===l?loneMark(u,si,f):null;
  return p;
};
let u=new uPlot({
  width:w,height:h,
  // Replaces uPlot's autoPadSide (17 top / 25 right). Top 8 is the whole title-to-plot gap (.rect-header has no margin-bottom) and absorbs y-max stroke caps; the rename hover strip (.rect-title::before) deadens its top half and must stay <= it. Right 12 keeps the last x label's centered half on-canvas and leaves a gutter for edge-hover and gutter-select drags.
  padding:[8,12,0,0],
  legend:{show:false},
  // _focus is our z-order signal (uPlot paints a series or band group with a _focus member last); alpha:1 keeps uPlot's native focus from ALSO dimming, so series.alpha stays entirely ours.
  focus:{alpha:1},
  // x auto IS the zoom policy: it gates setData's re-range (uPlot consults it nowhere else that matters in mode 1). Declaring it on the scale keeps every setData caller a plain setData(data) — passing resetScales=false instead skips the commit and freezes the zoomed chart until the next interaction.
  // Unzoomed, every live flush re-ranges x to the full extent — the chart follows a growing run. A fixed client-side zoom (__kymo_userzoom true: set by a selection or an axis pull, cleared by the configured reset click) keeps its window but still commits, so new points landing inside it paint on the flush that delivers them. A following zoom (__kymo_userzoom 'follow': a selection to the right edge of a chart that showed its newest point, or a left-half axis pull on a window ending exactly at it) re-ranges like an unzoomed chart, its start pinned by the x range wrapped below. While an axis pull holds the chart, the pull decides, on step charts too: a following one re-ranges, any other keeps its window.
  scales:{x:{time:false,distr:__KYMO_X_DISTR__,...(isLogX?{range:logXRange}:{}),auto:u=>{let pull=pulling();return pull?pull.follow:u.__kymo_userzoom!==true;}},...__KYMO_SYNC_SCALE__,y:{distr:__KYMO_Y_DISTR__,range:__KYMO_Y_DISTR__===3?yNiceLog:yNice}},
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
      // Hollow NaN-marker circles, a fixed pixel offset inside the plot's border. A marker flags WHERE a NaN happened and has no y value for the series machinery to place.
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
      let marks=u.data.__kymo_marks||(u.data.__kymo_marks=u.data.slice(nanBase,nanBase+nanCols).map(col=>{let js=[];for(let j=0;j<col.length;j++)if(col[j]!=null)js.push(j);return js;}));
      for(let i=0;i<marks.length;i++){
        let col=u.data[nanBase+i];
        ctx.globalAlpha=u.series[lineBase+i].alpha;
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
    // size is a function: the axis grows to fit its widest label (uPlot re-asks on every scale change) until the narrow-cell plot floor below wins. Every label is measured — with proportional glyphs the longest string isn't always the widest. The preference-scaled floor keeps plot left edges aligned across a grid column of short-label charts.
    {stroke:axColor,grid:{stroke:gridColor,width:1},ticks:{stroke:gridColor,width:1,size:5},font:'__KYMO_AXIS_FONT_SIZE__px sans-serif',gap:6,
     size:function(u,values,axisIdx){
       let ax=u.axes[axisIdx],pull=pulling();
       // An axis pull holds the width it pressed on, so the plot's left edge stays where the pull's frame (the plot's page x at the press) has it while the labels change: wider ones are cut at the canvas edge until the pull ends.
       let measured=pull?pull.hold:Math.max(__KYMO_Y_AXIS_MIN_SIZE__,Math.ceil(ax.ticks.size+ax.gap+Math.max(0,...(values||[]).map(measurer(u,axisIdx)))));
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
// The end of a following window from lo to hi on chart c: its newest x when that lies past lo, else hi (a chart whose data ends before lo, or has none, keeps its window).
let followEnd=function(c,lo,hi){let xs=c.data[0],e=xs[xs.length-1];return e>lo?e:hi;};
// Whether chart c shows its newest data, which a selection to its right edge or a left-half axis pull may follow: its window ends at or past its newest x and, on a step chart, the answer had no right bound (`stepOpen` on its x column, push_data_to_js); a step chart's window falls short of its data only after a pull narrowed it, until that pull's refetch answers.
let showsNewest=function(c){let xs=c.data[0];return c.scales.x.max>=xs[xs.length-1]&&(!c.__kymo_zoom.zr||xs.stepOpen);};
// Puts the client-side window from lo to hi on chart c with zoom state `zoom` (its __kymo_userzoom: falsy none, true fixed, 'follow' ending at followEnd). Unzoomed, lo and hi go unused: open bounds range the data as on creation, so a single x value widens rather than collapsing to a zero span.
let zoomX=function(c,lo,hi,zoom){c.__kymo_userzoom=zoom;c.setScale('x',zoom?{min:lo,max:zoom==='follow'?followEnd(c,lo,hi):hi}:{min:null,max:null});};
// A reply's re-range (x auto is on) keeps a following window's start and ends it at followEnd, or, while a following axis pull holds the chart, solves the pull against the reply's newest x.
let fitX=u.scales.x.range;
u.scales.x.range=function(u,mn,mx,k){let pull=pulling();return pull?pull.range():u.__kymo_userzoom==='follow'?[u.scales.x.min,followEnd(u,u.scales.x.min,u.scales.x.max)]:fitX(u,mn,mx,k);};
if(keepZoom)zoomX(u,keepZoom.lo,keepZoom.hi,keepZoom.userzoom);
// Dynamic metadata read by the once-installed gesture listeners. Keep it on
// the current uPlot instance so a config rebuild cannot leave those listeners
// using the first build's x-range column offsets or log transform.
u.__kymo_zoom={zr:zoomRefetch,readoutXShift:readoutXShift,xrBase:hasXr?xrBase:null,axis:isLogX?(zoomRefetch&&readoutXShift?AXIS_LOG1P:AXIS_LOG):AXIS_LINEAR};
u.__kymo_hl={runs:runIds,names:runNames,rs:rawStride};
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
      // resizing also fires setSize hooks but never reaches here.
      if(owner&&owner.srcId==='__KYMO_ID__')queueMicrotask(function(){if(window.__kymo_zg===owner)owner.cancel();});
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
// A zoom's one commit, for a selection and an axis pull alike: the real-x window (`range`) made on chart src, following the newest data when `follow`. A step chart asks the step store for it, a following window with no xmax, for the store and the server to leave open. A client chart's goes to every chart in its sync group, a following one ending at each chart's own newest point, now and on every reply; the source of an axis pull, already showing it (`shown`), takes only the zoom state.
let commitZoom=function(src,range,follow,shown){
  if(src.__kymo_zoom.zr){el.dispatchEvent(new CustomEvent('kymo-zoom',{detail:{xmin:range.lo,xmax:follow?null:range.hi},bubbles:true}));return;}
  let zoom=follow?'follow':true;
  eachPeer(src.cursor.sync.key,function(c,m){
    if(shown&&c===src){c.__kymo_userzoom=zoom;return;}
    let lo=range.lo+m.readoutXShift,hi=range.hi+m.readoutXShift;
    // A log-x peer takes the part it can show: a lower bound at or below 0 (a pull past a linear chart's data reaches there) is raised to its first plotted x, and the peer is left alone when the window ends before that x or it has no x data.
    if(c.scales.x.distr===3&&!(lo>0)){if(!(hi>c.data[0][0]))return;lo=c.data[0][0];}
    zoomX(c,lo,hi,zoom);
  });
};
let singleClickUnzoom=function(){return document.documentElement.getAttribute('data-kymo-single-click-unzoom')==='true';};
// Gives the page's one gesture slot to owner for the drag pressed by ev, until a release (a mouseup, or a move with the button up when the mouseup landed outside the window and never came) calls end(true), or a cancel (Escape, lost focus, owner.cancel) calls end(false). A drag ends where its last move left it, release latch included: a browser delivers any pending move before the mouseup, and the dashboard's document never scrolls under a held button (its shell scrolls .main-content, which moves no pointer page x). Each move, once the hand is 3 px from the press in any direction, latches the click the release will make, so coming back can't turn a drag into a reset click, then goes to move; a cancel latches that click too, so it can't reset what the cancel kept (the next press clears a latch no click took). Window listeners live only for the drag: idle charts add no per-mousemove work and hold no references.
let drag=function(ev,owner,move,end){
  let stop=function(commit){
    for(let type in on)window.removeEventListener(type,on[type]);
    if(window.__kymo_zg===owner)window.__kymo_zg=null;
    end(commit);
  };
  let cancel=function(){el.__kymo_skipUnzoomClick=true;stop(false);};
  let on={
    mousemove:function(e){if((e.buttons&1)===0)stop(true);else{if(Math.hypot(e.pageX-ev.pageX,e.pageY-ev.pageY)>=3)el.__kymo_skipUnzoomClick=true;move(e);}},
    mouseup:function(e){if(e.button===0)stop(true);},
    blur:cancel,
    keydown:function(e){if(e.key==='Escape')cancel();},
  };
  owner.cancel=cancel;
  window.__kymo_zg=owner;
  ev.preventDefault();
  for(let type in on)window.addEventListener(type,on[type]);
};
let resetZoom=function(){
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c)return;
    if(c.__kymo_zoom.zr)el.dispatchEvent(new CustomEvent('kymo-zoom',{detail:{xmin:null,xmax:null},bubbles:true}));
    else eachPeer(c.cursor.sync.key,function(peer){if(peer.__kymo_userzoom)zoomX(peer,null,null,false);});
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
// The axis-pull strip, measured from the plot overlay like plotPosition, so it starts exactly where the hover band ends. pageLeft is the plot's left edge in the page, the frame a pull holds for its moves as a selection holds wrapLeft: no layout read per move, and a chart that moves without resizing mid-pull (a new metric's chart landing before it) leaves the window where the hand had it rather than jumping.
let strip=function(c,ev){
  let r=el.getBoundingClientRect(),o=c.over.getBoundingClientRect();
  let left=o.left-r.left,w=o.width,px=ev.clientX-r.left;
  return {left:left,w:w,px:px,pageLeft:o.left+window.scrollX,inside:w>0&&ev.clientY-o.top>o.height&&ev.clientY<=r.bottom&&px>=left-10&&px<=left+w+10};
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
  if(!m.zr)return null;
  let xl=m.xrBase!=null?c.data[m.xrBase]:null,xh=m.xrBase!=null?c.data[m.xrBase+1]:null;
  let frozen=buildFrozenSupports({plotXs:c.data[0],xrMin:xl,xrMax:xh,xShift:m.readoutXShift});
  return frozen.coverage?frozen:null;
};
// One kymo-owned x-selection gesture for plot and side-gutter starts; its mousedown listener also starts the axis pull (pullPress) for a press on the strip below.
// uPlot's native mousedown is disabled above, so synced peers remain hover
// sources only: every pageX is interpreted through the source chart,
// even while the pointer is over a peer.
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
  el.addEventListener('mousedown',function(ev){
    if(ev.button!==0)return;
    // Clear a stale latch from a drag released outside this element (no click
    // follows in that case). This mousedown owns the next possible click.
    el.__kymo_skipUnzoomClick=false;
    if(window.__kymo_zg)return;
    let c=window.__kymo_charts['__KYMO_ID__'];
    if(!c)return;
    // Presses where hover is live (plot height, gutters included) select; one on the axis strip below pulls the axis.
    if(!plotPosition(c,ev.clientX,ev.clientY)){pullPress(ev,c);return;}
    let m=c.__kymo_zoom,sync=c.cursor.sync.key,lastX=ev.pageX,supportsData,supports;
    if(!c.data[0].length)return;
    // The plot as it is now: uPlot writes the overlay's inline width and its left within uPlot's wrap, so these read no layout. The wrap's page x (wrapLeft) is the press's; a container resize cancels the gesture.
    let wrapLeft=c.over.getBoundingClientRect().left+window.scrollX-parseFloat(c.over.style.left),width=function(){return parseFloat(c.over.style.width);};
    // The plot x of a page x, snapped as the hover snaps it (plotPosition): within 1 px of a side is that side, where the readout shows the first or the newest point.
    let plotX=function(x){let px=x-wrapLeft-parseFloat(c.over.style.left),w=width();return px>0&&px<=1?0:px<w&&px>=w-1?w:px;};
    // The real x of a selection end at plot x px, read from the plot as it is now: the x under the pointer, or past an edge, a step chart's extrapolation from its bucket coverage, and a client chart's own bound for that edge, less its plotted shift. A step chart maps through its plot, visual range and bucket evidence (`supports`, which resolved expands the window to, rebuilt only when a reply replaces the data).
    let endpoint=function(px){
      // The bound is exact where posToVal, through log10 and back, is not: at the right edge of a log chart from 0.001 to 3600.001 it returns 3600.0009999999957, which would end the window just short of its last point.
      if(!m.zr)return (px<=0?c.scales.x.min:px>=width()?c.scales.x.max:c.posToVal(px,'x'))-m.readoutXShift;
      if(supportsData!==c.data){supportsData=c.data;supports=stepSupports(c,m);}
      return mapSourcePlotX(supports&&buildSourceMap({axis:m.axis,width:width(),visualLo:c.scales.x.min-m.readoutXShift,visualHi:c.scales.x.max-m.readoutXShift,coverageLo:supports.coverage.lo,coverageHi:supports.coverage.hi}),px);
    };
    let px=plotX(ev.pageX),w=width(),x0=endpoint(px);
    // A press whose x can't be read, as on a step chart without bucket evidence, makes no selection.
    if(!Number.isFinite(x0))return;
    // The press keeps the x it had or, made at or past an edge, its place past that edge, resolved against the plot as it is now.
    let down=px<=0?{edge:0,past:px}:px>=w?{edge:1,past:px-w}:x0;
    // The real-x window the selection makes, or null when it makes none; the moving end is read anew from the pointer. `reachesRightEdge`: an end is at or past the right edge, so the window may follow the newest data there.
    let resolved=function(){
      if(Math.abs(lastX-ev.pageX)<3)return null;
      let w=width(),px=plotX(lastX),a=typeof down==='number'?down:endpoint(down.edge*w+down.past),b=endpoint(px);
      if(!(Number.isFinite(a)&&Number.isFinite(b)))return null;
      let r=m.zr?expandWholeBuckets(supports,a,b):{lo:Math.min(a,b),hi:Math.max(a,b)};
      if(!r||!(r.hi>r.lo))return null;
      r.reachesRightEdge=px>=w||down.edge===1;
      return r;
    };
    let s={srcId:'__KYMO_ID__',cursor:'crosshair',paint:function(){
      let r=resolved();
      if(r)paintRange(sync,r);else clearBoxes(sync);
    }};
    drag(ev,s,function(e2){lastX=e2.pageX;s.paint();},function(commit){
      let range=commit&&resolved();
      clearBoxes(sync);
      // A window that reached the right edge of a chart showing its newest data follows it.
      if(range)commitZoom(c,range,range.reachesRightEdge&&showsNewest(c));
    });
  });
}
// X-axis pull (AI-1284): grab the axis strip below the plot and pull. In the frame locked at mousedown the grabbed value stays under the hand while the far edge stays pinned (left half pins x-max, right half x-min); a press makes no jump (the map is identity until the hand moves, inside the pixel clamps), and a pull responds for as long as the hand moves, into empty space past the data (bounding it at the data's extent went dead the moment a full view had nothing left to reveal). Moves rescale the source live; the release commits the group's window, or refetches a step chart, which snaps it back to its data.
// A left-half pull on a window ending exactly at its newest x, showing its newest data (showsNewest), follows it while held and after: its pinned edge is the newest x, which a reply mid-pull moves with the grabbed value kept under the hand between the pixel clamps. Any other pull keeps its window through replies, a left-half one on a window reaching past its data included (a lone x value, which uPlot widens, or a zoom out past the data), whose far edge would otherwise jump to the newest x at the first move.
// The press itself comes through the selection's mousedown listener (a press outside the plot), which has looked the live chart up by id and cleared any stale click latch.
let pullPress=function(ev,c){
  let st=strip(c,ev);
  if(!st.inside)return;
  // On a log x scale pixels are linear in log(value): run the same pull math in log space and exponentiate on the way out. bound, in transformed units, keeps the asymptote from overflowing: on log, exp(±690) keeps the window's ends positive and finite; on linear, ±1e300 is comfortably sub-overflow.
  let isLog=c.scales.x.distr===3;
  let T=isLog?Math.log:function(v){return v},inv=isLog?Math.exp:T,bound=isLog?690:1e300;
  let mn=T(c.scales.x.min),mx=T(c.scales.x.max);
  // m is the pixel clamp's inset, 25px in or a quarter of a narrow plot.
  let p0=st.px-st.left,p1=p0,w=st.w,m=Math.min(25,w/4),v0=mn+p0/w*(mx-mn),grab=inv(v0);
  let pressData=c.data,d0=pressData[0],x0=d0[0],xN=d0[d0.length-1];
  if(!d0.length)return; // log-x can trim every point; nothing to pull
  let zm=c.__kymo_zoom,cov=stepSupports(c,zm)?.coverage;
  let which=p0<w/2?'min':'max';
  // The pull's gesture owner: it holds the y gutter at its laid-out width (the y size function) and, following, ranges x for a reply mid-pull (x auto is off for any other pull). lo and hi are the scale's own bounds, kept exactly by a fixed pull's pinned edge and a restore, as a selection's end past an edge does (endpoint); they and userzoom are what a rebuild mid-pull carries.
  let pull={srcId:'__KYMO_ID__',cursor:'ew-resize',hold:c.axes[1]._size,lo:c.scales.x.min,hi:c.scales.x.max,userzoom:c.__kymo_userzoom};
  pull.follow=which==='min'&&pull.hi===xN&&showsNewest(c);
  // A step window other than its data's extent is an earlier pull's, its refetch pending: a restore keeps it as it was. A lone x value has no extent to compare (uPlot widens it), so its window restores as an unzoomed one, on the data held then.
  let keepStepWindow=zm.zr&&x0!==xN&&(pull.lo!==x0||pull.hi!==xN);
  // The window for the hand at its last place (p1), as [min, max]. Exact pin between the pixel clamps: v0 stays under the cursor, the far edge stays pinned (a following pull's at the chart's newest x). Past them — including the cursor leaving the panel — the pull continues exponentially at the rate the 1/p asymptote has at the clamp (C1 handoff), so dragging off-panel keeps revealing instead of saturating at the edge. The clamp sits 25px in: that pins the handoff at an e-fold per 25px (~1 decade per 58px) — a controllable pull — and softens only the last pixels, which the raw asymptote made hyper-sensitive anyway.
  pull.range=function(){
    let q=Math.min(Math.max(p1,m),w-m);
    if(which==='min'){
      let hi=pull.follow?followEnd(c,grab,pull.hi):pull.hi,mx=T(hi);
      // Fitting [v0, max] into [q, w] gives span (mx-v0)*w/(w-q).
      let lnS=Math.log(mx-v0)+Math.log(w/(w-q))+(p1-q)/(w-q);
      return [inv(Math.max(mx-Math.exp(lnS),-bound)),hi];
    }
    // Mirror: min pinned at the left edge, span (v0-mn)*w/q.
    let lnS=Math.log(v0-mn)+Math.log(w/q)-(p1-q)/q;
    return [pull.lo,inv(Math.min(mn+Math.exp(lnS),bound))];
  };
  // A rebuild or unmount cancels the pull before it replaces the chart, so the pull keeps the chart it pressed on (c) to the end.
  drag(ev,pull,function(ev2){
    p1=ev2.pageX-st.pageLeft;
    if(Math.abs(p1-p0)>=1)el.__kymo_skipUnzoomClick=true;
    let win=pull.range();
    c.setScale('x',{min:win[0],max:win[1]});
  },function(commit){
    let moved=Math.abs(p1-p0)>=1;
    // A cancel, or a release back within a pixel of its press that left something changed (a reply mid-hold, or a window its moves shifted), restores what the press found: a kept step window as it was, else the press's zoom on the data held now (none on a step chart, so it ranges all its data, replies mid-hold included). A plain click commits nothing.
    if(!commit||!moved&&(c.data!==pressData||c.scales.x.min!==pull.lo||c.scales.x.max!==pull.hi)){
      if(keepStepWindow)c.setScale('x',{min:pull.lo,max:pull.hi});else zoomX(c,pull.lo,pull.hi,pull.userzoom);
    }
    // With the hold gone the gutter re-fits its labels. uPlot lays out only for a changed scale, so a cancel always asks (its restore may change none, after labels changed under a still hand), joining the restore's queued commit; a release asks only when the width changed, which the size function, measuring again, predicts exactly (y labels don't depend on the gutter). A queued redraw, not a batch: the destroy that follows a rebuild's or an unmount's cancel drops it.
    let a=c.axes[1];
    if(!commit||Math.ceil(a.size(c,a._values,1))!==a._size)c.redraw(false,true);
    if(!commit||!moved)return;
    // Peers take the pulled window, fixed or following, once, at release. On a downsampled step chart the hand-owned bound may extrapolate, but the pinned edge is a bucket midpoint: at or past the press data's first or last x it widens to the press buckets' coverage there, never narrowing (a lone x value's widened window keeps its edge); inside the data (a window an earlier pull left, its refetch pending) it goes as shown.
    let S=zm.readoutXShift,lo=c.scales.x.min-S,hi=c.scales.x.max-S;
    if(cov){if(which==='min'){if(pull.hi>=xN)hi=Math.max(hi,cov.hi);}else if(pull.lo<=x0)lo=Math.min(lo,cov.lo);}
    commitZoom(c,{lo:lo,hi:hi},pull.follow,true);
  });
};
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
