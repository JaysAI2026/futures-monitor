#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
商品期权 平值(ATM)隐含波动率 采集 + 历史分位数筛选

数据源（交易所官网公开日度数据）：
  上期所/上期能源: kx{date}.dat 中 o_cursigma（各合约月份隐含波动率，交易所公布）
  郑商所:         OptionDataDaily.txt（逐合约 隐含波动率，用 DELTA≈0.5 定位平值）
  广期所/大商所:  暂未接入（广期所只公布历史波动率；大商所接口有反爬 412）

平值定义：成交量最大的月份（主力月）中，最接近平值的执行价（郑商所按 call DELTA 最接近 0.5；
上期所直接取主力月交易所公布的隐含波动率）。

用法：
  python iv_scan.py --backfill 60    # 回填最近 60 个交易日历史
  python iv_scan.py --update         # 采集最新交易日 IV 并更新历史
  python iv_scan.py --report         # 输出低于 30% 分位的品种（不推送）
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests

from bollinger_push import UA, fetch_names

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IV_HIST_PATH = os.path.join(BASE_DIR, "iv_history.json")

SHFE_OPT_URL = "https://www.shfe.com.cn/data/tradedata/option/dailydata/kx{date}.dat"
SHFE_HEADERS = {"User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"}
CZCE_OPT_URL = ("http://www.czce.com.cn/cn/DFSStaticFiles/Option/{year}/{date}/"
                "OptionDataDaily.txt")


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def load_history():
    if os.path.exists(IV_HIST_PATH):
        try:
            with open(IV_HIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_history(hist):
    with open(IV_HIST_PATH, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=1)


# ---------------------------------------------------------------- 上期所/上期能源
def fetch_iv_shfe(date, session):
    """返回 {品种: 平值IV%}；非交易日/无数据返回 None"""
    try:
        r = session.get(SHFE_OPT_URL.format(date=date), headers=SHFE_HEADERS,
                        timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None
    rows = data.get("o_cursigma") or []
    best = {}   # prod -> (volume, sigma)
    for row in rows:
        iid = str(row.get("INSTRUMENTID", "")).upper()
        m = re.match(r"^([A-Z]+)\d+$", iid)
        try:
            sigma = float(row.get("SIGMA"))
        except (TypeError, ValueError):
            continue
        if not m or sigma <= 0:
            continue
        prod = m.group(1)
        try:
            vol = float(row.get("VOLUME") or 0)
        except (TypeError, ValueError):
            vol = 0
        cur = best.get(prod)
        if cur is None or vol > cur[0]:
            best[prod] = (vol, sigma)
    return {p: round(s * 100, 2) for p, (_, s) in best.items()}


# ---------------------------------------------------------------- 郑商所
def _parse_czce(text):
    """解析 OptionDataDaily.txt -> [ {prod, month, cp, strike, vol, oi, delta, iv} ]"""
    contracts = []
    cols = None
    for line in text.splitlines():
        if "合约代码" in line and "隐含波动率" in line:
            cols = [c.strip() for c in line.split("|")]
            continue
        if cols is None:
            continue
        parts = [c.strip() for c in line.split("|")]
        if len(parts) < 10:
            continue
        m = re.match(r"^([A-Z]+)(\d{3})([CP])(\d+)$", parts[0])
        if not m:
            continue

        def num(i):
            try:
                return float(parts[i].replace(",", "")) if i < len(parts) else None
            except (ValueError, IndexError):
                return None

        idx = {name: i for i, name in enumerate(cols)}
        contracts.append({
            "prod": m.group(1), "month": m.group(2), "cp": m.group(3),
            "strike": float(m.group(4)),
            "vol": num(idx.get("成交量(手)")) or 0,
            "oi": num(idx.get("持仓量")) or 0,
            "delta": num(idx.get("DELTA")),
            "iv": num(idx.get("隐含波动率")),
        })
    return contracts


def fetch_iv_czce(date, session):
    """返回 {品种: 平值IV%}；非交易日返回 None"""
    url = CZCE_OPT_URL.format(year=date[:4], date=date)
    try:
        r = session.get(url, headers={"User-Agent": UA,
                                      "Referer": "http://www.czce.com.cn/cn/Index.htm"},
                        timeout=30)
        if r.status_code != 200:
            return None
        r.encoding = "utf-8"
        if "郑州商品交易所期权每日行情表" not in r.text:
            return None
        contracts = _parse_czce(r.text)
    except Exception:
        return None
    if not contracts:
        return {}

    month_key = defaultdict(float)
    for c in contracts:
        month_key[(c["prod"], c["month"])] += c["vol"] or 0
    oi_key = defaultdict(float)
    for c in contracts:
        oi_key[(c["prod"], c["month"])] += c["oi"] or 0

    best_month = {}     # prod -> month
    for (prod, month), vol in month_key.items():
        cur = best_month.get(prod)
        if cur is None:
            best_month[prod] = month
            continue
        prev_v = month_key[(prod, cur)] or oi_key[(prod, cur)]
        new_v = vol or oi_key[(prod, month)]
        if new_v > prev_v:
            best_month[prod] = month

    out = {}
    for prod, month in best_month.items():
        series = [c for c in contracts if c["prod"] == prod and c["month"] == month]
        calls = [c for c in series if c["cp"] == "C" and c["delta"] is not None
                 and c["iv"] is not None and c["iv"] > 0]
        if not calls:
            continue
        atm = min(calls, key=lambda c: abs(abs(c["delta"]) - 0.5))
        strike = atm["strike"]
        call_iv = atm["iv"]
        puts = [c for c in series if c["cp"] == "P" and c["strike"] == strike
                and c["iv"] and c["iv"] > 0]
        iv = (call_iv + puts[0]["iv"]) / 2 if puts else call_iv
        out[prod] = round(iv, 2)
    return out


# ---------------------------------------------------------------- OpenVlab 主数据源
# OpenVlab 覆盖的期权交易所（商品 + ETF 期权）
OPENVLAB_EXCHANGES = {"SHFE", "INE", "CZCE", "DCE", "GFEX", "SHSE", "SZSE"}

# ETF 期权：surface-map 用缩写键，expiries/summary/history 用真实代码
ETF_KEY_MAP = {          # 真实代码 -> surface-map 键
    "510050": "50ETF",   # 上证50ETF
    "510300": "300ETF",  # 沪深300ETF
    "510500": "500ETF",  # 中证500ETF
    "588000": "000ETF",  # 科创50ETF
    "588080": "080ETF",  # 科创板50ETF
    "159901": "901ETF",  # 深证100ETF
    "159915": "915ETF",  # 创业板ETF
    "159919": "919ETF",  # 沪深300ETF深
    "159922": "922ETF",  # 中证500ETF深
}
ETF_NAME_MAP = {
    "510050": "上证50ETF", "510300": "沪深300ETF", "510500": "中证500ETF",
    "588000": "科创50ETF", "588080": "科创板50ETF", "159901": "深证100ETF",
    "159915": "创业板ETF", "159919": "沪深300ETF深", "159922": "中证500ETF深",
}
ETF_SURFACE_TO_CODE = {v: k for k, v in ETF_KEY_MAP.items()}


def _openvlab_allowed_products(client):
    """OpenVlab 目录中大商所/广期所的品种代码集合（缓存到函数属性）"""
    cache = getattr(_openvlab_allowed_products, "_cache", None)
    if cache is not None:
        return cache
    allowed = set()
    try:
        for p in (client.catalog_products().get("products") or []):
            if p.get("exchange") in OPENVLAB_EXCHANGES:
                allowed.add(str(p.get("productUnd", "")).upper())
    except Exception as e:
        log(f"OpenVlab 目录获取失败: {e}")
    _openvlab_allowed_products._cache = allowed
    return allowed


META_FILE = os.path.join(BASE_DIR, "iv_meta.json")


def _load_meta():
    try:
        with open(META_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_meta(meta):
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)


def fetch_iv_openvlab(session=None):
    """OpenVlab surface-map：一次调用取全市场实时 ATM IV（主数据源）。

    返回 (ivs, fronts)：ivs={prod: iv%}，fronts={prod: 前月到期码}；
    未配置 Key 或失败返回 None。
    结构: { "M_O": {"exp": "202701", "exps": {"202701": {
              "forward":..., "theo_vol_points": [{"delta":0.5,"theo_vol":...}]}}}}
    """
    try:
        from openvlab import OpenVlabClient
    except ImportError:
        return None
    client = OpenVlabClient()
    if not client.enabled:
        return None
    allowed = _openvlab_allowed_products(client)
    if not allowed:
        return None
    try:
        data = client.surface_map()
    except Exception as e:
        log(f"OpenVlab 获取失败: {e}")
        return None
    if not isinstance(data, dict) or not data:
        log(f"OpenVlab surface-map 返回异常: {type(data).__name__}")
        return None

    out, fronts = {}, {}
    try:
        for prod_key, info in data.items():
            prod = None
            m = re.match(r"^([A-Za-z]+)_O$", str(prod_key))
            if m:
                prod = m.group(1).upper()
            elif str(prod_key) in ETF_SURFACE_TO_CODE:
                prod = ETF_SURFACE_TO_CODE[str(prod_key)]    # ETF期权
            if not prod or prod not in allowed:
                continue
            if not isinstance(info, dict):
                continue
            exps = info.get("exps") or {}
            exp = info.get("exp") or (next(iter(exps), None))
            row = exps.get(exp) if exp else None
            if not isinstance(row, dict):
                continue
            pts = row.get("theo_vol_points") or []
            valid = [p for p in pts
                     if isinstance(p, dict)
                     and isinstance(p.get("theo_vol"), (int, float))
                     and p["theo_vol"] > 0]
            if not valid:
                continue
            atm = None
            for p in valid:
                if isinstance(p.get("delta"), (int, float)) \
                        and abs(p["delta"] - 0.5) < 1e-6:
                    atm = p["theo_vol"]
                    break
            if atm is None:   # 退而取 delta 最接近 0.5 的点
                nearest = min(valid,
                              key=lambda p: abs(p.get("delta", 0) - 0.5))
                atm = nearest["theo_vol"]
            out[prod] = round(atm, 2)
            fronts[prod] = exp
    except Exception as e:
        log(f"OpenVlab 数据解析失败: {e}")
        return None
    log(f"OpenVlab: {len(out)} 个品种 ATM IV")
    return out, fronts


def _expiry_dates(client):
    """各品种各到期月的到期日（缓存到 iv_meta.json）。

    已有目录的品种跳过；失败/空缺的品种每次调用自动重试。
    """
    meta = _load_meta()
    today = datetime.now().strftime("%Y-%m-%d")
    cached = meta.get("expiries") or {}
    out = dict(cached.get("map") or {})
    allowed = sorted(_openvlab_allowed_products(client))
    todo = [p for p in allowed if not out.get(p)]
    if cached.get("fetched") == today and not todo:
        return out
    for prod in todo:
        try:
            exps = client.expiries(prod).get("expiries") or []
            if exps:
                out[prod] = {e.get("exp"): e.get("expiry_date") for e in exps}
        except Exception as e:
            log(f"OpenVlab {prod} 到期目录失败: {e}")
            time.sleep(3)
        time.sleep(1.2)
    meta["expiries"] = {"fetched": today, "map": out}
    _save_meta(meta)
    log(f"OpenVlab 到期目录: {len(out)} 个品种（本次补抓 {len(todo)}）")
    return out


def _daily_ranks(client, fronts, force=False):
    """各品种的 OpenVlab IV Rank（官方百分位，日终口径）。

    force=True 时每次全量实时抓取（推送时调用）；限流/失败的品种
    回退用最近一次缓存值。缓存槽：盘中一个、17:00 后一个。
    """
    meta = _load_meta()
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    slot = today + ("" if now.hour < 17 else "_pm")
    prev_ranks = dict((meta.get("ranks") or {}).get(slot)
                      or (meta.get("ranks") or {}).get(today) or {})
    if force:
        todo = dict(sorted(fronts.items()))          # 全量实时抓取
    else:
        todo = {p: e for p, e in sorted(fronts.items())
                if p not in prev_ranks}              # 只补缺失
    if not todo:
        return prev_ranks
    ranks = dict(prev_ranks) if force else prev_ranks
    for prod, exp in todo.items():
        try:
            s = client.summary(prod, exp)
            ranks[prod] = {
                "iv_rank": s.get("iv_rank"),
                "as_of": s.get("as_of"),
                "window": s.get("iv_rank_window"),
            }
        except Exception as e:
            log(f"OpenVlab {prod} summary失败: {e}")
            time.sleep(3)
        time.sleep(0.9)
    meta.setdefault("ranks", {})[slot] = ranks
    _save_meta(meta)
    log(f"OpenVlab IV Rank[{slot}]: {len(ranks)} 个品种（本次实时抓取 {len(todo)}）")
    return ranks


# ---------------------------------------------------------------- 采集与合并
def collect_for_date(date, session):
    """返回 ({prod: iv}, {prod: 来源}, {prod: 前月到期码})。

    OpenVlab 实时数据为主（覆盖全部商品期权品种）；交易所官网日度数据
    仅用于兜底 OpenVlab 未覆盖的个别品种。
    """
    ivs, srcs, fronts = {}, {}, {}
    ovl = fetch_iv_openvlab(session)
    if ovl:
        ovl_ivs, ovl_fronts = ovl
        for p, v in ovl_ivs.items():
            ivs[p] = v
            srcs[p] = "OpenVlab"
        fronts.update(ovl_fronts)
    shfe = fetch_iv_shfe(date, session)
    if shfe:
        for p, v in shfe.items():
            if p not in ivs:
                ivs[p] = v
                srcs[p] = "上期所/上期能源"
    czce = fetch_iv_czce(date, session)
    if czce:
        for p, v in czce.items():
            if p not in ivs:
                ivs[p] = v
                srcs[p] = "郑商所"
    ok = ovl is not None or shfe is not None or czce is not None
    return (ivs if ok else None), srcs, fronts


def last_n_dates(n_days, until=None):
    """往前数 n 个自然日（含周末，抓取时 404/失败自动跳过非交易日）"""
    end = datetime.strptime(until, "%Y%m%d") if until else datetime.now()
    return [(end - timedelta(days=i)).strftime("%Y%m%d") for i in range(n_days)]


def backfill(days=60, until=None):
    session = requests.Session()
    hist = load_history()
    got = 0
    for date in last_n_dates(days, until):
        if date in hist:
            continue
        ivs, _, _ = collect_for_date(date, session)
        if ivs is None:
            continue    # 非交易日或接口未出数据
        if ivs:
            hist[date] = ivs
            got += 1
            log(f"{date}: 采集 {len(ivs)} 个品种")
    save_history(hist)
    log(f"回填完成，新增 {got} 天，共 {len(hist)} 天历史")


def update(today=None):
    """采集最新交易日并追加历史（合并写入，避免部分数据覆盖完整数据）。

    返回 (当日ivs, 来源, 前月到期码)；同时刷新每日缓存的 OpenVlab IV Rank。
    """
    session = requests.Session()
    hist = load_history()
    date = today or datetime.now().strftime("%Y%m%d")
    ivs, srcs, fronts = collect_for_date(date, session)
    if ivs is None or not ivs:
        log(f"{date} 未获取到期权数据（可能非交易日或尚未发布）")
        return None, {}, {}
    day = hist.setdefault(date, {})
    day.update(ivs)
    save_history(hist)
    log(f"{date}: 合并更新后共 {len(day)} 个品种 IV")
    # 记录当日各品种前月到期码；每日首次更新时抓取 IV Rank
    if fronts:
        meta = _load_meta()
        meta.setdefault("fronts", {})[date] = fronts
        meta["expiries_fetched"] = meta.get("expiries_fetched") or False
        _save_meta(meta)
        try:
            from openvlab import OpenVlabClient
            client = OpenVlabClient()
            if client.enabled:
                _expiry_dates(client)
                _daily_ranks(client, fronts, force=True)   # 每次推送实时抓取百分位
        except ImportError:
            pass
    return day, srcs, fronts


def backfill_openvlab(window_days=70):
    """用 OpenVlab 的 ATM IV 历史序列回填大商所/广期所品种"""
    from openvlab import OpenVlabClient

    client = OpenVlabClient()
    if not client.enabled:
        log("OpenVlab 未配置 Key，跳过回填")
        return
    allowed = sorted(_openvlab_allowed_products(client))
    if not allowed:
        log("OpenVlab 目录为空，跳过回填")
        return
    hist = load_history()
    cutoff = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
    done = 0
    for prod in allowed:
        # 增量：已有足够历史样本的品种跳过（重试时省配额）
        covered = sum(1 for d in hist.values() if prod in d)
        if covered >= window_days - 25:
            done += 1
            continue
        try:
            exps = client.expiries(prod).get("expiries") or []
            if not exps:
                continue
            exp = exps[0]["exp"]
            data = client._get(f"/volatility/history/{prod}/{exp}")
            rows = (data or {}).get("atmv") or []
        except Exception as e:
            log(f"OpenVlab {prod} 历史获取失败: {e}")
            time.sleep(3)    # 限流时放慢节奏
            continue
        time.sleep(1.2)      # 控制在限流额度内（每品种2次请求，120次/分钟）
        n = 0
        for row in rows:
            d = str(row.get("date", ""))
            v = row.get("value")
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            if not d or d < cutoff or v <= 0:
                continue
            hist.setdefault(d.replace("-", ""), {})[prod] = round(v, 2)
            n += 1
        done += 1
        log(f"OpenVlab {prod}({exp}): 回填 {n} 天")
        save_history(hist)
    log(f"OpenVlab 回填完成: {done}/{len(allowed)} 个品种")


# ---------------------------------------------------------------- 分位数与报告
def percentile_rank(values, x):
    if not values:
        return None
    below = sum(1 for v in values if v <= x)
    return below / len(values) * 100


def build_report(cfg, hist=None, today=None):
    """返回 (below_list, weak_list, meta)"""
    scan_cfg = cfg.get("iv", {})
    pct_threshold = scan_cfg.get("percentile", 30)
    hist = hist if hist is not None else load_history()
    dates = sorted(hist.keys())
    if not dates:
        return [], [], {"error": "无历史数据"}
    latest_date = dates[-1]
    latest = dict(hist[latest_date])
    # 盘中推送时今日交易所数据未发布：缺失品种回退用上一交易日收盘值
    if len(latest) < scan_cfg.get("min_products", 55) and len(dates) > 1:
        prev = hist[dates[-2]]
        for p, v in prev.items():
            latest.setdefault(p, v)
        meta_note = f"{latest_date}（部分品种为 {dates[-2]} 收盘值）"
    else:
        meta_note = latest_date
    window = scan_cfg.get("history_days", 60)
    recent = dates[-window:]

    # OpenVlab 每日 IV Rank（优先使用）与剩余天数
    meta_m = _load_meta()
    now_dt = datetime.now()
    rank_slot = now_dt.strftime("%Y-%m-%d") + ("" if now_dt.hour < 17 else "_pm")
    ranks_all = meta_m.get("ranks") or {}
    ranks = ranks_all.get(rank_slot) \
        or ranks_all.get(now_dt.strftime("%Y-%m-%d")) or {}
    if not ranks:
        # 非交易日/当日Rank未发布：回退最近一个有数据的槽
        for _slot in sorted(ranks_all.keys(), reverse=True):
            if ranks_all.get(_slot):
                ranks = ranks_all[_slot]
                break
    fronts = (meta_m.get("fronts") or {}).get(latest_date) or {}
    exp_map = (meta_m.get("expiries") or {}).get("map") or {}
    today_dt = datetime.now()

    def _dte(prod):
        """该品种当前前月合约的剩余自然日"""
        exp = fronts.get(prod)
        ed = (exp_map.get(prod) or {}).get(exp)
        if not ed:
            return None
        try:
            return (datetime.strptime(ed, "%Y-%m-%d") - today_dt).days
        except (TypeError, ValueError):
            return None

    names = fetch_names(sorted(latest.keys()),
                        requests.Session()) if latest else {}
    # ETF期权中文名（期货名称接口查不到）
    for code, nm in ETF_NAME_MAP.items():
        if code in latest:
            names[code] = nm

    def _exp_label(prod):
        """主力月到期码 -> 显示标签（202612 -> 2612月）"""
        exp = fronts.get(prod) or ""
        if len(exp) == 6:
            return f"{exp[2:4]}{exp[4:6]}月"
        return exp

    below, weak = [], []
    for prod, iv in sorted(latest.items()):
        ovl_rank = (ranks.get(prod) or {}).get("iv_rank")
        if ovl_rank is None:
            # 只用 OpenVlab 官方 IV Rank，缺失不本地估算
            weak.append({"prod": prod, "iv": iv,
                         "exp": _exp_label(prod), "n": 0, "no_rank": True})
            continue
        rank = float(ovl_rank)              # OpenVlab 250日窗口 IV Rank
        series = [hist[d][prod] for d in recent if prod in hist[d]]
        lo, hi = min(series), max(series)
        item = {
            "prod": prod, "iv": iv, "rank": rank,
            "exp": _exp_label(prod),
            "dte": _dte(prod),
            "lo": round(lo, 2), "hi": round(hi, 2),
            "n": len(series), "name": names.get(prod, prod),
            "date": f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}",
        }
        if rank < pct_threshold:
            below.append(item)
    below.sort(key=lambda x: x["rank"])    # 全局按IV Rank从低到高
    return below, weak, {"latest_date": meta_note, "real_date": latest_date}


def build_message(cfg, report=None):
    """生成推送消息 (title, body, meta)；无数据时 title 为 None。

    report 可传入已算好的 (below, weak, meta)，避免重复拉取数据。
    """
    below, weak, meta = report if report else build_report(cfg)
    if meta.get("error"):
        return None, None, meta
    iv_cfg = cfg.get("iv", {})
    pct = iv_cfg.get("percentile", 30)
    if below:
        lines = []
        for it in sorted(below, key=lambda x: x["rank"]):
            seg = (f"- {it['name']}({it['prod']}{it['exp']}) IV {it['iv']}% | "
                   f"IV Rank {it['rank']:.0f}%")
            if it.get("dte") is not None:
                seg += f" | 剩余 {it['dte']} 天到期"
            seg += f" | 60日区间 {it['lo']}~{it['hi']}%"
            lines.append(seg)
        title = f"期权IV扫描：{len(below)} 个品种IV低于{pct}%分位"
        body = f"低于 {pct}% 分位的品种（共{len(below)}个）：\n\n" + "\n\n".join(lines)
    else:
        title = f"期权IV扫描：无品种IV低于{pct}%分位"
        body = f"当前无品种IV低于{pct}%分位"
    if weak:
        w = ", ".join(it['prod'] for it in weak[:12])
        body += f"\n\n暂无OpenVlab IV Rank（下轮补抓）：{w}"
    ovl_prods = sorted(_openvlab_allowed_products(
        _safe_client()) if _safe_client() else set())
    latest = load_history().get(meta.get("real_date"), {})
    exch_prods = [p for p in sorted(latest) if p not in ovl_prods]
    if exch_prods:
        body += ("\n\n注：IV为主力月平值隐含波动率，取自OpenVlab实时数据；"
                 + "、".join(exch_prods) + " 取自交易所官网日度数据")
    else:
        body += "\n\n注：IV为主力月平值隐含波动率，全部取自OpenVlab实时数据"
    body += "\nIV Rank为OpenVlab官方百分位（250日窗口，日终口径，推送时实时抓取）；括号内为主力月"
    body += f"\n数据日期 {meta.get('latest_date')}"
    body += f"\n推送时间 {datetime.now():%Y-%m-%d %H:%M}"
    return title, body, meta


def _safe_client():
    try:
        from openvlab import OpenVlabClient
        c = OpenVlabClient()
        return c if c.enabled else None
    except ImportError:
        return None


# ---------------------------------------------------------------- CLI
def main():
    parser = argparse.ArgumentParser(description="商品期权平值IV采集与分位数筛选")
    parser.add_argument("--backfill", type=int, default=0, help="回填N个自然日历史")
    parser.add_argument("--backfill-ovl", action="store_true",
                        help="用OpenVlab历史序列回填大商所/广期所品种")
    parser.add_argument("--update", action="store_true", help="采集最新数据并更新历史")
    parser.add_argument("--report", action="store_true", help="输出报告（不推送）")
    parser.add_argument("--push", action="store_true",
                        help="采集最新数据并立即推送IV日报")
    args = parser.parse_args()

    if args.backfill:
        backfill(args.backfill)
        return
    if args.backfill_ovl:
        backfill_openvlab()
        return
    if args.update:
        update()
        return
    if args.report:
        cfg_path = os.path.join(BASE_DIR, "config.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        below, weak, meta = build_report(cfg)
        if meta.get("error"):
            print(meta["error"])
            return
        print(f"数据日期: {meta.get('latest_date')}")
        print(f"--- 低于 {cfg.get('iv', {}).get('percentile', 30)}% 分位的品种 ---")
        for it in below:
            print(f"{it['name']}({it['prod']}) IV={it['iv']}% "
                  f"分位={it['rank']:.0f}% 区间[{it['lo']}~{it['hi']}] 样本{it['n']}天")
        print(f"--- 样本不足({len(weak)}个) ---")
        for it in weak:
            print(f"{it['prod']} IV={it['iv']}% 样本{it['n']}天")
        return
    if args.push:
        cfg_path = os.path.join(BASE_DIR, "config.json")
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        update()
        title, body, meta = build_message(cfg)
        if not title:
            print(f"无法生成报告: {meta.get('error')}")
            return
        import market_scan as ms
        state = ms.load_state()
        limit = cfg.get("scan", {}).get("daily_push_limit", 0)
        if ms.push_budget_left(state, limit):
            ms.push_wechat(title, body, dict(cfg.get("push", {})))
            state["_push_count"] = state.get("_push_count", 0) + 1
            ms.save_state(state)
            print(f"已推送：{title}")
        else:
            print(f"[额度已满 {limit}条/日] 未推送")
        return


if __name__ == "__main__":
    main()
