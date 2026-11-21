# -*- coding: utf-8 -*-
"""期货/期权监控 Web 仪表盘 v2（可调参数版）

  GET  /                        仪表盘页面
  GET  /api/data?periods=...&band=..&mult=..&pct=..
                               按参数即时计算（K线缓存TTL 5分钟，过期后台自动刷新）
  POST /api/scan               强制刷新K线（约1分钟）
  POST /api/iv                 强制刷新OpenVlab百分位（约2分钟）

周期可选：30分钟/2小时/日线/周线/月线（默认日线）
滑杆：带宽阈值(1~20%,步进0.5,默认4) 振幅倍数(1~20,步进0.5,默认2) IV百分位(0~100,步进5,默认25)
"""
import copy
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import requests

import market_scan as ms

PORT = 8899
KLINE_TTL = 300          # K线缓存有效期（秒）
DAILY_KEEP = 560         # 日线保留根数（约26个月，供月线布林(20)计算）

PERIOD_LIST = [("30", "30分钟"), ("120", "2小时"), ("daily", "日线"),
               ("weekly", "周线"), ("monthly", "月线")]
PERIOD_LABELS = dict(PERIOD_LIST)

_lock = threading.Lock()
_cache = {"daily": {}, "30": {}, "120": {}, "names": {}, "prices": {},
          "day_range": {}, "ts": 0.0, "scanning": False, "progress": "尚未扫描"}
_status = {"scan": "", "iv": ""}


# ---------------------------------------------------------------- 月线聚合
def monthly_closes(daily_bars, price, realtime=True):
    """把日线聚合成月线收盘序列；realtime=True 时本月用实时价"""
    months = []
    for d, o, h, l, c in daily_bars:
        m = d[:7]
        if months and months[-1][0] == m:
            months[-1] = (m, c)
        else:
            months.append((m, c))
    if not months:
        return []
    if realtime and price is not None:
        cur = datetime.now().strftime("%Y-%m")
        if months[-1][0] == cur:
            months[-1] = (cur, float(price))
        else:
            months.append((cur, float(price)))
    return [c for _, c in months]


# ---------------------------------------------------------------- K线缓存
def _do_refresh():
    """抓取全部品种 K线（日线/30分/120分）+ 名称 + 现价"""
    with _lock:
        if _cache["scanning"]:
            return
        _cache["scanning"] = True
        _cache["progress"] = f"扫描中… {datetime.now():%H:%M:%S}"
    try:
        session = requests.Session()
        products = ms.fetch_products(session)
        targets = sorted(products)
        _status["scan"] = f"扫描中…（{len(targets)}个品种，约1分钟）"

        names = ms.fetch_names(targets, session)

        def fetch_many(fn, *fixed):
            out = {}
            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = {ex.submit(fn, *fixed, p, session): p for p in targets}
                for fut in as_completed(futs):
                    p = futs[fut]
                    try:
                        out[p] = fut.result()
                    except Exception:
                        pass
                    time.sleep(0.02)
            return out

        daily = fetch_many(lambda p, s: ms.fetch_daily_kline(f"{p}0", s, DAILY_KEEP))
        k30 = fetch_many(lambda p, s: ms.fetch_kline(f"{p}0", 30, s))
        k120 = fetch_many(lambda p, s: ms.fetch_kline(f"{p}0", 120, s))
        prices = {p: products[p]["price"] for p in targets}
        day_range = {p: (b[-1][2], b[-1][3]) for p, b in daily.items() if b}
        with _lock:
            _cache.update({"daily": daily, "30": k30, "120": k120,
                           "names": names, "prices": prices,
                           "day_range": day_range, "ts": time.time(),
                           "scanning": False,
                           "progress": f"完成 {datetime.now():%H:%M:%S}（{len(daily)}个品种）"})
        _status["scan"] = f"完成 {datetime.now():%H:%M:%S}"
    except Exception as e:
        with _lock:
            _cache["scanning"] = False
            _cache["progress"] = f"扫描失败: {e}"
        _status["scan"] = f"扫描失败: {e}"


def refresh_async(force=False):
    with _lock:
        fresh = (time.time() - _cache["ts"]) < KLINE_TTL
        if _cache["scanning"] or (fresh and not force):
            return
    threading.Thread(target=_do_refresh, daemon=True).start()


# ---------------------------------------------------------------- 结果计算
def compute_results(periods):
    """对选定周期逐品种计算布林判定，返回 {'daily|RB': (pos,price,upper,mid,lower)}"""
    cfg = ms.load_cfg()
    res = {}
    for pkey in periods:
        if pkey in ("30", "120"):
            bars_map = _cache.get(pkey) or {}
            for prod, price in _cache["prices"].items():
                bars = bars_map.get(prod)
                if not bars or not price:
                    continue
                try:
                    closes = ms.minutes_closes(bars, price, True)
                    res[f"{pkey}|{prod}"] = ms.judge(closes, price, cfg)
                except Exception:
                    pass
        else:
            for prod, bars in _cache["daily"].items():
                price = _cache["prices"].get(prod)
                if not price or not bars:
                    continue
                try:
                    if pkey == "daily":
                        closes = ms.daily_closes(bars, price, True)
                    elif pkey == "weekly":
                        closes = ms.weekly_closes(bars, price, True)
                    elif pkey == "monthly":
                        closes = monthly_closes(bars, price, True)
                    else:
                        continue
                    res[f"{pkey}|{prod}"] = ms.judge(closes, price, cfg)
                except Exception:
                    pass
    return res


def build_data(periods, band_thr, amp_mult, pct):
    cfg = ms.load_cfg()
    cfg2 = copy.deepcopy(cfg)
    cfg2["periods"] = periods
    cfg2.setdefault("scan", {})["bandwidth_pct"] = band_thr
    cfg2["scan"]["amp_mult"] = amp_mult
    cfg2.setdefault("iv", {})["percentile"] = pct

    res = compute_results(periods)
    state = {"_last_results": res, "_names": _cache["names"],
             "_day_range": _cache["day_range"]}

    # 轨外清单（手动分组，支持monthly等任意周期）
    grouped = {}
    for key, val in res.items():
        pkey, prod = key.split("|", 1)
        grouped.setdefault(pkey, []).append((prod, val))
    sections = []
    for pkey in periods:
        above, below = [], []
        for prod, val in sorted(grouped.get(pkey, [])):
            pos, price, upper, mid, lower = val
            line = ms.fmt_line(prod, _cache["names"].get(prod, ""),
                               price, upper, lower, pos)
            if pos == "above":
                above.append(line)
            elif pos == "below":
                below.append(line)
        sections.append({"period": PERIOD_LABELS.get(pkey, pkey),
                         "above": above, "below": below,
                         "desc": f"布林({cfg['boll_window']},{cfg['boll_ndev']})"})
    cnt = sum(len(s["above"]) + len(s["below"]) for s in sections)

    band = ms.bandwidth_lines(state, cfg2, band_thr)
    amp = ms.amplitude_lines(state, amp_mult)

    # IV 报告（按滑杆百分位过滤，读取缓存不联网）
    below, weak, focus = [], [], []
    iv_meta = {"latest_date": "", "real_date": ""}
    try:
        import iv_scan
        report = iv_scan.build_report(cfg2)
        below = report[0]
        weak = [it["prod"] for it in report[1]]
        iv_meta = report[2]
        for pkey in periods:
            grp = []
            for it in sorted(below, key=lambda x: x["rank"]):
                val = res.get(f"{pkey}|{it['prod']}")
                if not val or val[0] not in ("above", "below"):
                    continue
                grp.append({"name": it["name"], "prod": it["prod"],
                            "exp": it["exp"],
                            "pos": "上轨之上" if val[0] == "above" else "下轨之下",
                            "price": val[1], "iv": it["iv"], "rank": it["rank"],
                            "dte": it.get("dte")})
            focus.append({"period": PERIOD_LABELS.get(pkey, pkey),
                          "count": len(grp), "items": grp})
    except Exception as e:
        iv_meta["error"] = str(e)

    return {
        "now": f"{datetime.now():%Y-%m-%d %H:%M:%S}",
        "kline_age": f"{(time.time() - _cache['ts']) / 60:.0f}" if _cache["ts"] else "-",
        "kline_progress": _cache["progress"],
        "scanning": _cache["scanning"],
        "pending": not _cache["ts"],
        "out_count": cnt,
        "periods": periods,
        "band_thr": band_thr, "amp_mult": amp_mult, "pct": pct,
        "sections": sections, "band": band, "amp": amp,
        "focus": focus, "below": below, "weak": weak,
        "iv_meta": iv_meta, "status": _status,
        # 静态页面（GitHub Pages）本地计算所需的原始数据
        "results": res,
        "names": _cache["names"],
        "day_range": _cache["day_range"],
        "boll_window": cfg.get("boll_window"),
        "boll_ndev": cfg.get("boll_ndev"),
    }


# ---------------------------------------------------------------- 后台任务
def run_iv():
    def job():
        _status["iv"] = "刷新OpenVlab百分位中…（约2分钟）"
        try:
            import iv_scan
            iv_scan.update()
            _status["iv"] = f"完成 {datetime.now():%H:%M:%S}"
        except Exception as e:
            _status["iv"] = f"刷新失败: {e}"
    threading.Thread(target=job, daemon=True).start()


# ---------------------------------------------------------------- 页面
PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>期货/期权监控仪表盘</title>
<style>
:root{--bg:#0f1419;--card:#1a2129;--line:#2a3441;--txt:#dbe4ee;--dim:#8b98a8;
--green:#4ade80;--red:#f87171;--amber:#fbbf24;--blue:#60a5fa;--purple:#c084fc}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font:14px/1.7 "Segoe UI",system-ui,sans-serif;padding:20px;max-width:1100px;margin:0 auto}
h1{font-size:20px;margin-bottom:4px}
.sub{color:var(--dim);font-size:12px;margin-bottom:14px}
.bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px}
button{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:9px 18px;font-size:13px;cursor:pointer;transition:.2s}
button:hover{background:#1d4ed8}
button:disabled{background:#374151;cursor:wait}
.badge{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:6px 12px;font-size:12px;color:var(--dim)}
.pending{background:#3b2f14;border:1px solid #665013;color:var(--amber);border-radius:10px;padding:10px 16px;margin-bottom:14px;display:none}
.ctl{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px 18px;margin-bottom:16px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px}
.ctl .full{grid-column:1/-1}
.ctl label{font-size:12px;color:var(--dim);display:block;margin-bottom:6px}
.ctl .val{color:var(--blue);font-weight:bold;font-size:14px;margin-left:6px}
input[type=range]{width:100%;accent-color:#60a5fa}
.chk{display:flex;gap:6px;flex-wrap:wrap}
.chk label{background:#232c38;border:1px solid var(--line);border-radius:8px;padding:5px 12px;font-size:13px;color:var(--txt);cursor:pointer;user-select:none;display:flex;align-items:center;gap:6px}
.chk input{accent-color:#2563eb}
.chk label:has(input:checked){background:#1e3a5f;border-color:#60a5fa}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px 20px;margin-bottom:16px}
.card h2{font-size:15px;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}
.card h2 .n{color:var(--dim);font-size:12px;font-weight:normal}
.grp{margin:8px 0;padding:8px 12px;border-left:3px solid var(--blue);background:#151b23;border-radius:0 8px 8px 0}
.grp.g2{border-color:var(--purple)}.grp.g3{border-color:var(--amber)}
.grp .t{color:var(--dim);font-size:12px;margin-bottom:6px}
.row{display:flex;justify-content:space-between;gap:12px;padding:7px 4px;border-bottom:1px dashed #232c38;flex-wrap:wrap;align-items:baseline}
.row:last-child{border-bottom:0}
.tag{display:inline-block;background:#232c38;border-radius:6px;padding:1px 8px;font-size:12px;margin-right:6px}
.up{color:var(--red)}.dn{color:var(--green)}
.iv{color:var(--blue);font-family:Consolas,monospace}
.rk{font-family:Consolas,monospace}
.rk.lo{color:var(--green)}.rk.mid{color:var(--amber)}.rk.none{color:var(--dim)}
.bar-out{height:5px;background:#232c38;border-radius:3px;width:100%;margin-top:3px}
.bar-in{height:5px;border-radius:3px;background:linear-gradient(90deg,#4ade80,#fbbf24,#f87171)}
.dim{color:var(--dim)}.mono{font-family:Consolas,monospace}
details summary{cursor:pointer;color:var(--blue);font-size:13px;padding:4px 0}
.spin{display:inline-block;width:12px;height:12px;border:2px solid #555;border-top-color:#60a5fa;border-radius:50%;animation:r 1s linear infinite;vertical-align:-2px;margin-right:6px}
@keyframes r{to{transform:rotate(360deg)}}
footer{color:var(--dim);font-size:11px;margin-top:24px;text-align:center}
</style>
</head>
<body>
<h1>期货/期权监控仪表盘</h1>
<div class="sub">布林轨外（周期可选） · 带宽/振幅阈值可调 · OpenVlab IV分位</div>
<div class="ctl">
  <div class="full">
    <label>出轨判定周期（可多选，默认日线）</label>
    <div class="chk" id="periods"></div>
  </div>
  <div><label>带宽阈值（第二部分）<span class="val" id="v-band"></span></label>
    <input type="range" id="s-band" min="1" max="20" step="0.5" value="4"></div>
  <div><label>带宽 ÷ 日内振幅（第三部分）<span class="val" id="v-mult"></span></label>
    <input type="range" id="s-mult" min="1" max="20" step="0.5" value="2"></div>
  <div><label>IV Rank 百分位阈值（第五部分）<span class="val" id="v-pct"></span></label>
    <input type="range" id="s-pct" min="0" max="100" step="5" value="25"></div>
</div>
<div class="bar">
  <button onclick="doRefresh(this)">⟳ 刷新</button>
  <span class="badge" id="st-scan"></span><span class="badge" id="st-iv"></span>
  <span class="badge" id="age"></span><span class="badge" id="now"></span>
</div>
<div class="pending" id="pending">K线数据扫描中，约1分钟，页面自动刷新…</div>
<div id="app">加载中…</div>
<footer>数据仅供研究参考 · 与推送逻辑同源（本地实时计算）· 本地端口 8899</footer>
<script>
const PERIODS=[["30","30分钟"],["120","2小时"],["daily","日线"],["weekly","周线"],["monthly","月线"]];
let P=JSON.parse(localStorage.getItem("fm_periods")||'["daily"]');
if(!P.length)P=["daily"];
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const rkCls=r=>r==null?'none':r<10?'lo':r<25?'mid':'';
const rkTxt=r=>r==null?'暂缺':r+'%';

// 初始化控件
document.getElementById('periods').innerHTML=PERIODS.map(([k,n])=>
 `<label><input type="checkbox" value="${k}" ${P.includes(k)?'checked':''}>${n}</label>`).join('');
document.querySelectorAll('#periods input').forEach(cb=>cb.onchange=()=>{
  P=[...document.querySelectorAll('#periods input:checked')].map(c=>c.value);
  if(!P.length){cb.checked=true;P=[cb.value];}
  localStorage.setItem('fm_periods',JSON.stringify(P));load();});
function bindSlider(id,vid,fmt){const s=document.getElementById(id),v=document.getElementById(vid);
  v.textContent=fmt(s.value);
  s.oninput=()=>{v.textContent=fmt(s.value)};
  s.onchange=()=>{clearTimeout(window._t);window._t=setTimeout(load,350)};}
bindSlider('s-band','v-band',x=>x+'%');
bindSlider('s-mult','v-mult',x=>x+'倍');
bindSlider('s-pct','v-pct',x=>x+'%');

function params(){const q=new URLSearchParams();
  q.set('periods',P.join(','));
  q.set('band',document.getElementById('s-band').value);
  q.set('mult',document.getElementById('s-mult').value);
  q.set('pct',document.getElementById('s-pct').value);
  return q.toString();}

function focusHtml(d){
  let h=`<h2>【一】出轨低波品种（出轨 且 IV Rank &lt; ${d.pct}%）<span class="n">与推送逻辑同源</span></h2>`;
  d.focus.forEach(g=>{
    h+=`<div class="grp g2"><div class="t">▶ ${esc(g.period)}出轨（${g.count}个）</div>`;
    h+=g.count? g.items.map(it=>`<div class="row"><div><b>${esc(it.name)}</b><span class="dim">(${esc(it.prod)}${esc(it.exp||'')})</span> <span class="tag ${it.pos==='上轨之上'?'up':'dn'}">${esc(it.pos)}</span><span class="tag">IV ${it.iv}%</span><span class="tag">Rank ${it.rank}%</span></div><div class="dim mono">现价 ${it.price}${it.dte!=null?' | 剩余 '+it.dte+' 天':''}</div></div>`).join(''):'<div class="dim">无</div>';
    h+='</div>';});
  return h;
}
function ivTable(list){
  return `<table><tr><th>品种</th><th>主力月</th><th>IV</th><th>IV Rank</th><th>剩余</th></tr>`+
  list.map(it=>`<tr><td><b>${esc(it.name)}</b><span class="dim">(${esc(it.prod)})</span></td>
    <td class="mono dim">${esc(it.exp||'')}</td><td class="iv">${it.iv}%</td>
    <td class="rk ${rkCls(it.rank)}">${rkTxt(it.rank)}${it.rank!=null?`<div class="bar-out"><div class="bar-in" style="width:${Math.min(it.rank,100)}%"></div></div>`:''}</td>
    <td class="dim">${it.dte!=null?it.dte+'天':''}</td></tr>`).join('')+'</table>';
}
function render(d){
  document.getElementById('st-scan').innerHTML=d.status.scan?'<span class="spin"></span>'+esc(d.status.scan):'';
  document.getElementById('st-iv').innerHTML=d.status.iv?'<span class="spin"></span>'+esc(d.status.iv):'';
  document.getElementById('age').textContent='行情K线 '+(d.kline_age==='-'?'未扫描':d.kline_age+' 分钟前更新');
  document.getElementById('now').textContent=d.now;
  document.getElementById('pending').style.display=d.pending||d.scanning?'block':'none';
  if(d.pending){return;}   // 首次扫描未完成，等待轮询
  let h='';
  h+=`<div class="card">${focusHtml(d)}</div>`;
  h+=`<div class="card"><h2>【二】日线布林带小于 ${d.band_thr}%<span class="n">共 ${d.band.length} 个</span></h2>`+
     (d.band.length?d.band.map(s=>`<div class="row"><div>${esc(s)}</div></div>`).join(''):'<div class="dim">无</div>')+'</div>';
  h+=`<div class="card"><h2>【三】日线布林带小于 ${d.amp_mult} 倍日内振幅<span class="n">共 ${d.amp.length} 个</span></h2>`+
     (d.amp.length?d.amp.map(s=>`<div class="row"><div>${esc(s)}</div></div>`).join(''):'<div class="dim">无</div>')+'</div>';
  h+=`<div class="card"><h2>【四】出轨品种<span class="n">轨外 ${d.out_count} 项</span></h2>`;
  d.sections.forEach(s=>{
    h+=`<div class="grp"><div class="t">${esc(s.period)}（${esc(s.desc)}）</div>`;
    if(s.above.length)h+=s.above.map(x=>`<div class="row"><div><span class="tag up">上轨之上</span>${esc(x)}</div></div>`).join('');
    if(s.below.length)h+=s.below.map(x=>`<div class="row"><div><span class="tag dn">下轨之下</span>${esc(x)}</div></div>`).join('');
    if(!s.above.length&&!s.below.length)h+='<div class="dim">无轨外品种</div>';
    h+='</div>';});
  h+='</div>';
  h+=`<div class="card"><h2>【五】低波品种（IV Rank &lt; ${d.pct}%）<span class="n">共 ${d.below.length} 个，按Rank升序</span></h2>`+
     (d.below.length?ivTable(d.below):'<div class="dim">无</div>');
  if(d.weak.length)h+=`<div class="dim" style="margin-top:8px">暂无OpenVlab Rank（下轮补抓）：${esc(d.weak.join(', '))}</div>`;
  h+='</div>';
  if(d.iv_meta.error)h+=`<div class="card dim">IV数据异常：${esc(d.iv_meta.error)}</div>`;
  document.getElementById('app').innerHTML=h;
}
async function load(){
  try{
    const r=await fetch('/api/data?'+params());
    const d=await r.json();
    render(d);
    if(d.pending||d.scanning)clearInterval(window._auto),window._auto=setInterval(load,3000);
    else clearInterval(window._auto);
  }catch(e){document.getElementById('app').textContent='加载失败: '+e;}
}
function doRefresh(btn){           // 同时触发K线扫描与IV百分位刷新，两者都完成才恢复按钮
  btn.disabled=true;btn.innerHTML='<span class="spin"></span>刷新中…';
  Promise.allSettled([fetch('/api/scan',{method:'POST'}),
                      fetch('/api/iv',{method:'POST'})]).catch(()=>{});
  const wait=setInterval(()=>{fetch('/api/data?'+params()).then(r=>r.json()).then(d=>{
    document.getElementById('st-scan').innerHTML=d.status.scan?'<span class="spin"></span>'+esc(d.status.scan):'';
    document.getElementById('st-iv').innerHTML=d.status.iv?'<span class="spin"></span>'+esc(d.status.iv):'';
    const busy=d.status.scan.startsWith('扫描中')||d.status.iv.startsWith('刷新');
    if(!busy){clearInterval(wait);btn.disabled=false;btn.innerHTML='⟳ 刷新';load();}
  }).catch(()=>{});},2000);}
load();setInterval(load,60000);
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", PAGE.encode("utf-8"))
        elif u.path == "/api/data":
            q = parse_qs(u.query)
            periods = [p.strip() for p in
                       (q.get("periods", ["daily"])[0].split(","))
                       if p.strip() in dict(PERIOD_LIST)]
            if not periods:
                periods = ["daily"]
            band = min(20.0, max(1.0, float(q.get("band", [4])[0])))
            mult = min(20.0, max(1.0, float(q.get("mult", [2])[0])))
            pct = min(100.0, max(0.0, float(q.get("pct", [25])[0])))
            # K线过期则后台刷新（有旧数据先返回旧数据）
            refresh_async(force=False)
            data = build_data(periods, band, mult, pct)
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(data, ensure_ascii=False,
                                  default=str).encode("utf-8"))
        else:
            self._send(404, "text/plain; charset=utf-8", b"not found")

    def do_POST(self):
        if self.path == "/api/scan":
            refresh_async(force=True)
            self._send(200, "application/json; charset=utf-8", b'{"ok":true}')
        elif self.path == "/api/iv":
            run_iv()
            self._send(200, "application/json; charset=utf-8", b'{"ok":true}')
        else:
            self._send(404, "text/plain; charset=utf-8", b"not found")

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    refresh_async(force=False)   # 启动即预取K线
    run_iv()                     # 启动即后台补抓今日IV百分位（增量，缺失才抓）
    print(f"仪表盘 http://127.0.0.1:{PORT}  (Ctrl+C 退出)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
