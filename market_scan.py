#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全市场商品期货 多周期 布林轨道扫描 + 微信推送

扫描上期所/大商所/郑商所/上期能源全部商品期货品种（主力合约），
在日线 / 2小时 / 周线 三个周期上，找出"价格在布林上轨之上"或"价格在布林下轨之下"的品种，
推送时注明周期与品种中文名。

用法：
  python market_scan.py           # 常驻扫描
  python market_scan.py --once    # 只扫描一轮并打印结果（不推送）
"""

import argparse
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

from bollinger_push import (UA, bollinger, fetch_daily_kline, fetch_kline,
                            fetch_names, in_session, push_wechat)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
STATE_PATH = os.path.join(BASE_DIR, "scan_state.json")

LIST_URL = ("https://futsseapi.eastmoney.com/list/{market}"
            "?orderBy=dm&sort=asc&pageSize=500&pageIndex=0")
MARKETS = (113, 114, 115, 142)   # 上期所/大商所/郑商所/上期能源

PERIOD_LABELS = {
    "daily": "日线", "weekly": "周线",
    "240": "4小时", "120": "2小时", "60": "1小时",
    "30": "30分钟", "15": "15分钟", "5": "5分钟",
}


def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def load_cfg():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------- 周期解析
def get_periods(cfg):
    raw = cfg.get("periods") or [cfg.get("period", "daily")]
    out = []
    for p in raw:
        key = str(p).strip().lower()
        if key in ("daily", "day", "d"):
            key = "daily"
        elif key in ("weekly", "week", "w"):
            key = "weekly"
        else:
            try:
                key = str(int(float(key)))
            except ValueError:
                continue
        if key not in out:
            out.append(key)
    return out or ["daily"]


def period_label(key):
    return PERIOD_LABELS.get(key, f"{key}分钟")


# ---------------------------------------------------------------- 品种列表
def fetch_products(session):
    """返回 {品种: {price, vol}}，取每个品种成交量最大的合约作为代表"""
    products = {}
    for mkt in MARKETS:
        try:
            resp = session.get(LIST_URL.format(market=mkt),
                               headers={"User-Agent": UA}, timeout=20)
            resp.raise_for_status()
            items = (resp.json() or {}).get("list", [])
        except Exception as e:
            log(f"品种列表获取失败(市场{mkt}): {e}")
            continue
        for it in items:
            dm = str(it.get("dm", ""))
            m = re.match(r"^([a-zA-Z]+)\d+$", dm)
            if not m:
                continue
            prod = m.group(1).upper()
            try:
                vol = float(it.get("vol") or 0)
            except (TypeError, ValueError):
                vol = 0.0
            price = it.get("p")
            try:
                price = float(price) if price not in (None, "-", "") else None
            except (TypeError, ValueError):
                price = None
            cur = products.get(prod)
            if cur is None or vol > cur["vol"]:
                products[prod] = {"price": price, "vol": vol}
    return products


# ---------------------------------------------------------------- 收盘序列
def daily_closes(bars, price, realtime=True):
    """日线收盘序列。

    realtime=True（默认）：使用实时日K——当天K线未在数据源出现时用实时价补一根，
    已出现时把收盘价刷新为实时价，与本机行情软件显示一致；
    realtime=False：15:00 前剔除当天未收盘K线。
    """
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if realtime:
        if bars and bars[-1][0] == today:
            bars = bars[:-1] + [(today, bars[-1][1], bars[-1][2],
                                 bars[-1][3], price)]
        else:
            bars = bars + [(today, price, price, price, price)]
        return [b[4] for b in bars]
    if bars and bars[-1][0] == today and now.strftime("%H:%M") < "15:00":
        bars = bars[:-1]
    return [b[4] for b in bars]


def _week_key(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    iso = dt.isocalendar()
    return (iso[0], iso[1])


def weekly_closes(daily_bars, price=None, realtime=True):
    """把日线聚合成周线收盘序列；realtime=True 时本周用实时价（实时周K）"""
    weeks = []      # [(week_key, close)]
    for d, o, h, l, c in daily_bars:
        wk = _week_key(d)
        if weeks and weeks[-1][0] == wk:
            weeks[-1] = (wk, c)
        else:
            weeks.append((wk, c))
    if not weeks:
        return []
    now = datetime.now()
    cur_week = _week_key(now.strftime("%Y-%m-%d"))
    if realtime and price is not None:
        if weeks[-1][0] == cur_week:
            weeks[-1] = (cur_week, price)
        else:
            weeks.append((cur_week, price))
        return [c for _, c in weeks]
    week_done = now.weekday() > 4 or (now.weekday() == 4
                                      and now.strftime("%H:%M") >= "15:00")
    if weeks[-1][0] == cur_week and not week_done:
        weeks = weeks[:-1]
    return [c for _, c in weeks]


def minutes_closes(bars, price, realtime=True):
    """分钟周期收盘序列；realtime=True 时当前形成中的K线用实时价"""
    if not bars:
        return []
    closes = [b[4] for b in bars[:-1]]
    if realtime:
        closes.append(price)
    return closes


# ---------------------------------------------------------------- 单周期判定
def judge(closes, price, cfg):
    window = cfg["boll_window"]
    ndev = cfg["boll_ndev"]
    if len(closes) < window:
        raise ValueError(f"K线不足({len(closes)}<{window})")
    band = bollinger(closes, window, ndev, cfg.get("boll_ddof", 0))
    if band is None:
        raise ValueError("布林计算失败")
    mid, upper, lower = band
    # 轨外定义：价格 >= 上轨 或 <= 下轨（min_break_pct 默认 0，不做额外缓冲）
    brk = cfg.get("scan", {}).get("min_break_pct", 0) / 100.0
    if price >= upper * (1 + brk):
        pos = "above"
    elif price <= lower * (1 - brk):
        pos = "below"
    else:
        pos = "inside"
    return pos, price, upper, mid, lower


# ---------------------------------------------------------------- 消息构造
def fmt_line(prod, name, price, upper, lower, pos):
    tag = f"{name}({prod})" if name and name != prod else prod
    if pos == "above":
        pct = (price / upper - 1) * 100
        return f"{tag} {price:g} | 上轨 {upper:.2f}（高出 {pct:.2f}%）"
    pct = (1 - price / lower) * 100
    return f"{tag} {price:g} | 下轨 {lower:.2f}（低出 {pct:.2f}%）"


def build_sections(blocks, cfg):
    """blocks: [(period_key, above_lines, below_lines)] -> 消息正文

    三个周期都会出现在推送里；某周期无轨外品种时明确标注「无」，
    便于确认该周期确实扫描过。
    """
    parts = []
    desc = f"布林({cfg['boll_window']},{cfg['boll_ndev']})"
    for pkey, above, below in blocks:
        seg = [f"**{period_label(pkey)}**（{desc}）"]
        if above:
            seg.append("上轨之上：\n\n" + "\n\n".join("- " + x for x in above))
        if below:
            seg.append("下轨之下：\n\n" + "\n\n".join("- " + x for x in below))
        if not above and not below:
            seg.append("无轨外品种")
        parts.append("\n\n".join(seg))
    if not parts:
        parts.append("全部品种均在布林轨道内（当前无轨外品种）")
    parts.append(f"\n时间 {datetime.now():%Y-%m-%d %H:%M}")
    return "\n\n".join(parts)


def push_budget_left(state, limit):
    """每日推送额度检查；limit<=0 表示不限。跨天自动重置"""
    today = datetime.now().strftime("%Y-%m-%d")
    if state.get("_push_date") != today:
        state["_push_date"] = today
        state["_push_count"] = 0
    return limit <= 0 or state.get("_push_count", 0) < limit


def push_alert(title, blocks, cfg, state=None):
    """推送并计数；配置了 daily_push_limit 时超出额度的消息只记日志不发送。

    布林类推送为 17:30 的期权IV日报预留 iv.reserve 条额度。
    """
    limit = cfg.get("scan", {}).get("daily_push_limit", 0)
    reserve = cfg.get("iv", {}).get("reserve", 0)
    if limit > 0:
        limit = max(0, limit - reserve)
    if state is not None and not push_budget_left(state, limit):
        log(f"[额度已满 {limit}条/日] 未推送: {title}")
        return False
    push_cfg = dict(cfg.get("push", {}))
    push_wechat(title, build_sections(blocks, cfg), push_cfg)
    if state is not None:
        state["_push_count"] = state.get("_push_count", 0) + 1
        extra = f"/{limit}" if limit else ""
        log(f"已推送（今日第 {state['_push_count']}{extra} 条）")
    return True


# ---------------------------------------------------------------- 扫描一轮
def fetch_daily_map(targets, session, cfg):
    keep = max(cfg["boll_window"] * 5 + 20, 150)
    out = {}
    with ThreadPoolExecutor(max_workers=cfg.get("scan", {}).get("max_workers", 6)) as ex:
        futs = {ex.submit(fetch_daily_kline, f"{p}0", session, keep): p
                for p in targets}
        for fut in as_completed(futs):
            prod = futs[fut]
            try:
                out[prod] = fut.result()
            except Exception as e:
                log(f"{prod} 日线获取失败: {e}")
            time.sleep(0.03)
    return out


def fetch_min_map(targets, minutes, session, cfg):
    out = {}
    with ThreadPoolExecutor(max_workers=cfg.get("scan", {}).get("max_workers", 6)) as ex:
        futs = {ex.submit(fetch_kline, f"{p}0", minutes, session): p
                for p in targets}
        for fut in as_completed(futs):
            prod = futs[fut]
            try:
                out[prod] = fut.result()
            except Exception as e:
                log(f"{prod} {minutes}分钟K获取失败: {e}")
            time.sleep(0.03)
    return out


def scan_once(cfg, state, session, notify=True):
    scan_cfg = cfg.get("scan", {})
    periods = get_periods(cfg)
    min_vol = scan_cfg.get("min_volume", 1000)
    exclude = {str(p).upper() for p in scan_cfg.get("exclude_products", [])}
    include = [str(p).upper() for p in scan_cfg.get("include_products", [])]

    products = fetch_products(session)
    if not products:
        log("品种列表为空，跳过本轮")
        return
    targets = [p for p in products
               if p not in exclude and (include and p in include
                                        or products[p]["vol"] >= min_vol)]
    log(f"本轮扫描 {len(targets)} 个品种 × {len(periods)} 个周期"
        f"（{'/'.join(period_label(p) for p in periods)}）")

    names = fetch_names(targets, session)

    # (周期, 品种) -> (pos, price, upper, mid, lower)
    results = {}
    rt = bool(cfg.get("realtime_bar", True))
    daily_map = fetch_daily_map(targets, session, cfg)

    # 日内振幅：日线最后一根K线的（高-低），供「带宽 vs 振幅」筛选
    state["_day_range"] = {prod: (bars[-1][2], bars[-1][3])
                           for prod, bars in daily_map.items() if bars}

    for prod, bars in daily_map.items():
        price = products[prod]["price"] or (bars[-1][4] if bars else None)
        if price is None:
            continue
        if "daily" in periods:
            try:
                results[("daily", prod)] = judge(
                    daily_closes(bars, price, rt), price, cfg)
            except Exception as e:
                log(f"{prod} 日线分析失败: {e}")
        if "weekly" in periods:
            try:
                results[("weekly", prod)] = judge(
                    weekly_closes(bars, price, rt), price, cfg)
            except Exception as e:
                log(f"{prod} 周线分析失败: {e}")

    for pkey in periods:
        if pkey in ("daily", "weekly"):
            continue
        minutes = int(pkey)
        min_map = fetch_min_map(list(daily_map), minutes, session, cfg)
        for prod, bars in min_map.items():
            if len(bars) < 2:
                continue
            price = products[prod]["price"] or bars[-1][4]
            try:
                results[(pkey, prod)] = judge(
                    minutes_closes(bars, price, rt), price, cfg)
            except Exception as e:
                log(f"{prod} {minutes}分钟分析失败: {e}")

    # 统计与新触发判断
    positions = state.setdefault("positions", {})
    blocks_new, blocks_all = [], []
    total_new = 0
    for pkey in periods:
        pos_map = positions.setdefault(pkey, {})
        new_above, new_below, above, below = [], [], [], []
        for (k, prod), (pos, price, upper, mid, lower) in sorted(results.items()):
            if k != pkey:
                continue
            line = fmt_line(prod, names.get(prod, ""), price, upper, lower, pos) \
                if pos != "inside" else None
            old = pos_map.get(prod, "inside")
            if pos != "inside" and old != pos:
                (new_above if pos == "above" else new_below).append(line)
            pos_map[prod] = pos
            if pos == "above":
                above.append(line)
            elif pos == "below":
                below.append(line)
        total_new += len(new_above) + len(new_below)
        n_ab, n_be = len(above), len(below)
        log(f"{period_label(pkey)}: 上轨之上 {n_ab} 个, 下轨之下 {n_be} 个, "
            f"新触发 {len(new_above) + len(new_below)} 个")
        for line in above + below:
            log("  " + line)
        blocks_new.append((pkey, new_above, new_below))
        blocks_all.append((pkey, above, below))

    state["_last_results"] = {f"{k[0]}|{k[1]}": list(v) for k, v in results.items()}
    state["_names"] = names
    state["_updated"] = time.time()
    save_state(state)


# ---------------------------------------------------------------- 定时推送
def snapshot_blocks(state, cfg):
    """从最近一轮扫描结果生成三周期 blocks"""
    res = state.get("_last_results") or {}
    names = state.get("_names") or {}
    periods = get_periods(cfg)
    grouped = {}
    for key, val in res.items():
        pkey, prod = key.split("|", 1)
        grouped.setdefault(pkey, []).append((prod, val))
    blocks = []
    for pkey in periods:
        above, below = [], []
        for prod, (pos, price, upper, mid, lower) in sorted(grouped.get(pkey, [])):
            line = fmt_line(prod, names.get(prod, ""), price, upper, lower, pos)
            if pos == "above":
                above.append(line)
            elif pos == "below":
                below.append(line)
        blocks.append((pkey, above, below))
    return blocks


def bandwidth_lines(state, cfg, thr_pct):
    """日线布林带收窄清单：(上轨-下轨)/价格 ≤ thr_pct% 的品种，按带宽升序"""
    res = state.get("_last_results") or {}
    names = state.get("_names") or {}
    rows = []
    for key, val in res.items():
        pkey, prod = key.split("|", 1)
        if pkey != "daily":
            continue
        pos, price, upper, mid, lower = val
        if not price or upper is None or lower is None or price <= 0:
            continue
        width = (upper - lower) / price * 100
        if width <= thr_pct:
            rows.append((width, prod, price))
    rows.sort()
    out = []
    for width, prod, price in rows:
        nm = names.get(prod, "")
        tag = f"{nm}({prod})" if nm and nm != prod else prod
        out.append(f"- {tag} {price:g} | 带宽 {width:.2f}%")
    return out


def amplitude_lines(state, mult):
    """第三部分清单：日线布林带宽 < 日内振幅×mult 的品种，按带宽/振幅升序"""
    res = state.get("_last_results") or {}
    names = state.get("_names") or {}
    day_rng = state.get("_day_range") or {}
    rows = []
    for key, val in res.items():
        pkey, prod = key.split("|", 1)
        if pkey != "daily":
            continue
        pos, price, upper, mid, lower = val
        hi, lo = day_rng.get(prod) or (None, None)
        if hi is None or lo is None or upper is None or lower is None:
            continue
        amp = hi - lo
        if amp <= 0:
            continue
        band = upper - lower
        if band < amp * mult:
            rows.append((band / amp, prod, price, band, amp, hi, lo))
    rows.sort()
    out = []
    for ratio, prod, price, band, amp, hi, lo in rows:
        nm = names.get(prod, "")
        tag = f"{nm}({prod})" if nm and nm != prod else prod
        out.append(f"- {tag} {price:g} | 带宽 {band:g}（日内振幅 {amp:g}，"
                   f"{ratio:.2f}倍）")
    return out


def build_push_message(cfg, state, session):
    """执行一轮扫描并组装完整播报消息，返回 (标题, 正文)。

    本地定时推送与云端云函数共用同一套组装逻辑。
    """
    try:
        scan_once(cfg, state, session, notify=False)
    except Exception as e:
        log(f"定时推送前扫描异常: {e}")

    blocks = snapshot_blocks(state, cfg)
    cnt = sum(len(b[1]) + len(b[2]) for b in blocks)

    focus_blocks = []
    iv_report = None
    body = ""
    title = f"期货定时播报：{cnt} 项在轨外"
    band_thr = cfg.get("scan", {}).get("bandwidth_pct", 3)
    band = bandwidth_lines(state, cfg, band_thr)
    band_part = (f"【二】日线布林带小于{band_thr}%（共{len(band)}个）：\n\n"
                 + ("\n".join(band) if band else "无"))
    amp_mult = cfg.get("scan", {}).get("amp_mult", 2)
    amp = amplitude_lines(state, amp_mult)
    amp_part = (f"【三】日线布林带小于{amp_mult:g}倍振幅"
                f"（共{len(amp)}个）：\n\n"
                + ("\n".join(amp) if amp else "无"))
    try:
        import iv_scan
        ivs = iv_scan.update()[0]
        if ivs:
            iv_report = iv_scan.build_report(cfg)
            below = iv_report[0]
            pct = cfg.get("iv", {}).get("percentile", 25)
            res = state.get("_last_results") or {}
            total_focus = 0
            for pkey in get_periods(cfg):
                grp = []
                for it in sorted(below, key=lambda x: x["rank"]):
                    val = res.get(f"{pkey}|{it['prod']}")
                    if not val or val[0] not in ("above", "below"):
                        continue
                    pos_word = "上轨之上" if val[0] == "above" else "下轨之下"
                    seg = (f"- {it['name']}({it['prod']}{it['exp']}) "
                           f"{pos_word} 现价 {val[1]}"
                           f" | IV {it['iv']}% | IV Rank {it['rank']:.0f}%")
                    if it.get("dte") is not None:
                        seg += f" | 剩余 {it['dte']} 天到期"
                    grp.append(seg)
                total_focus += len(grp)
                focus_blocks.append(
                    f"▶ {period_label(pkey)}出轨（{len(grp)}个）：\n\n"
                    + "\n".join(grp if grp else ["无"]))
            title = (f"期货播报：重点{total_focus}个"
                     f" | 轨外{cnt}项")
        body = "【四】出轨品种：\n\n" + build_sections(blocks, cfg)
        if ivs:
            body = (f"【一】出轨低波品种：\n\n"
                    + "\n\n".join(focus_blocks)
                    + "\n\n————————\n\n" + band_part
                    + "\n\n————————\n\n" + amp_part
                    + "\n\n————————\n\n") + body
        else:
            body = ("【一】出轨低波品种：IV数据未获取，暂无法判定"
                    "\n\n————————\n\n"
                    + band_part + "\n\n————————\n\n" + amp_part
                    + "\n\n————————\n\n") + body
        if ivs:
            t2, b2, _m = iv_scan.build_message(cfg, iv_report)
            if t2:
                n = t2.split("：", 1)[-1]
                title += f"，{n}"
                body += "\n\n————————\n\n【五】低波品种：\n\n" + b2
    except Exception as e:
        log(f"IV/重点部分生成失败: {e}")
        if not body:
            body = (band_part + "\n\n————————\n\n" + amp_part
                    + "\n\n————————\n\n【四】出轨品种：\n\n"
                    + build_sections(blocks, cfg))
    return title, body


def maybe_scheduled_push(cfg, state, session):
    """固定时点推送：每条 = 三周期轨外清单 + 期权IV分位数清单（合并一条消息）。

    每次执行前从磁盘重读推送状态（外部改动/多进程即时生效）；
    已过时点超过宽限期则跳过不补推，避免重启后连环补发。
    """
    slots = cfg.get("scan", {}).get("push_times") or []
    if not slots:
        return
    now = datetime.now()
    if now.weekday() >= 5:
        return    # 周末不推
    # 从磁盘重读状态（服务内存中的 state 可能已过期）
    try:
        fresh = load_state()
        fresh["_last_results"] = state.get("_last_results")
        fresh["_names"] = state.get("_names")
        state.clear()
        state.update(fresh)
    except Exception:
        pass
    today = now.strftime("%Y-%m-%d")
    done = state.setdefault("_pushed_slots", {})
    if done.get("_date") != today:
        done.clear()
        done["_date"] = today
    grace = int(cfg.get("scan", {}).get("slot_grace_minutes", 15))
    hm = now.strftime("%H:%M")
    now_min = now.hour * 60 + now.minute
    for slot in sorted(slots):
        s = str(slot).strip()
        if not s or hm < s or done.get(s):
            continue
        # 已过时点超过宽限期：跳过不补推（防止重启后连环补发）
        try:
            h, m = s.split(":")
            overdue = now_min - (int(h) * 60 + int(m))
        except ValueError:
            overdue = 0
        if overdue > grace:
            done[s] = True
            save_state(state)
            log(f"{s} 时点已过宽限期({grace}分钟)，跳过补推")
            continue
        title, body = build_push_message(cfg, state, session)

        limit = cfg.get("scan", {}).get("daily_push_limit", 0)
        if push_budget_left(state, limit):
            push_wechat(title, body, dict(cfg.get("push", {})))
            state["_push_count"] = state.get("_push_count", 0) + 1
            log(f"已按计划推送 {s} 时点播报（今日第 {state['_push_count']} 条）")
        else:
            log(f"[额度已满 {limit}条/日] {s} 时点播报未发送")
        done[s] = True
        save_state(state)
        break    # 一个 tick 只处理一个时点


# ---------------------------------------------------------------- 收盘汇总
def push_summary(state, cfg):
    res = state.get("_last_results") or {}
    names = state.get("_names") or {}
    periods = get_periods(cfg)
    grouped = {}
    for key, val in res.items():
        pkey, prod = key.split("|", 1)
        grouped.setdefault(pkey, []).append((prod, val))
    blocks = []
    for pkey in periods:
        above, below = [], []
        for prod, (pos, price, upper, mid, lower) in sorted(grouped.get(pkey, [])):
            if pos != "above" and pos != "below":
                continue
            line = fmt_line(prod, names.get(prod, ""), price, upper, lower, pos)
            (above if pos == "above" else below).append(line)
        blocks.append((pkey, above, below))
    total = sum(len(b[1]) + len(b[2]) for b in blocks)
    if not total:
        return False
    push_alert(f"期货布林轨道收盘汇总：{total} 项在轨外", blocks, cfg, state)
    return True


def maybe_daily_summary(cfg, state, session):
    summary_time = str(cfg.get("scan", {}).get("daily_summary_time", "")).strip()
    if not summary_time:
        return
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if state.get("_summary_date") == today or now.weekday() >= 5:
        return
    if now.strftime("%H:%M") < summary_time:
        return
    if not state.get("_last_results"):
        scan_once(cfg, state, session, notify=False)
    if push_summary(state, cfg):
        state["_summary_date"] = today
        save_state(state)
        log("已推送收盘汇总")


# ---------------------------------------------------------------- 期权IV日报
def maybe_iv_report(cfg, state, session):
    """每个交易日收盘后推送一次「平值IV低于30%分位」的品种清单"""
    import iv_scan

    iv_cfg = cfg.get("iv", {})
    push_time = str(iv_cfg.get("push_time", "")).strip()
    if not push_time:
        return
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    if state.get("_iv_report_date") == today or now.weekday() >= 5:
        return
    if now.strftime("%H:%M") < push_time:
        return
    if now.strftime("%H:%M") > "21:00":
        # 超过21:00仍取不到数据就放弃当天，避免每分钟空抓
        state["_iv_report_date"] = today
        save_state(state)
        return

    ymd = now.strftime("%Y%m%d")
    ivs, srcs, _fronts = iv_scan.update(ymd)
    if not ivs:
        return    # 数据未发布，下个循环重试
    if len(ivs) < iv_cfg.get("min_products", 55):
        # 数据不完整（如 OpenVlab 瞬时只返回部分品种），不推送不标记，
        # update() 是合并写入，下轮重试会自动补齐缺失品种
        log(f"IV数据不完整（{len(ivs)} 个品种），下轮重试")
        return

    pct_threshold = iv_cfg.get("percentile", 30)
    below, weak, meta = iv_scan.build_report(cfg)
    if meta.get("error"):
        return

    lines = []
    for it in below:
        lines.append(f"- {it['name']}({it['prod']}) IV {it['iv']}% | "
                     f"历史分位 {it['rank']:.0f}% | "
                     f"60日区间 {it['lo']}~{it['hi']}%（{it['n']}天样本）")
    if lines:
        body = "低于 " + str(pct_threshold) + "% 分位的品种：\n" + "\n".join(lines)
        title = f"期权IV扫描：{len(below)} 个品种IV低于{pct_threshold}%分位"
    else:
        body = f"当前无品种IV低于{pct_threshold}%分位"
        title = f"期权IV扫描：无品种IV低于{pct_threshold}%分位"
    if weak:
        w = ", ".join(f"{it['prod']}({it['n']}天)" for it in weak[:12])
        body += f"\n\n样本不足暂未评级：{w}"
    ovl_prods = sorted(p for p, s in srcs.items() if s == "OpenVlab")
    if ovl_prods:
        body += ("\n\n注：IV为主力月平值隐含波动率；"
                 + "、".join(ovl_prods) + " 来自OpenVlab数据源")
    else:
        body += "\n\n注：IV为主力月平值隐含波动率；大商所、广期所品种暂未接入"
    body += f"\n数据日期 {meta.get('latest_date')}"

    limit = cfg.get("scan", {}).get("daily_push_limit", 0)
    state["_iv_report_date"] = today
    if push_budget_left(state, limit):
        push_wechat(title, body, dict(cfg.get("push", {})))
        state["_push_count"] = state.get("_push_count", 0) + 1
        log(f"已推送期权IV日报（今日第 {state['_push_count']} 条）")
    else:
        log(f"[额度已满 {limit}条/日] 期权IV日报未推送（数据已入历史）")
    save_state(state)


def main():
    parser = argparse.ArgumentParser(description="全市场期货多周期布林轨道扫描")
    parser.add_argument("--once", action="store_true", help="只扫描一轮后退出")
    parser.add_argument("--force", action="store_true", help="忽略交易时段限制")
    args = parser.parse_args()

    cfg = load_cfg()
    state = load_state()
    session = requests.Session()
    interval = cfg.get("scan", {}).get("scan_interval_minutes", 30)
    label = "/".join(period_label(p) for p in get_periods(cfg))

    log(f"启动全市场扫描: 周期 {label} 布林({cfg['boll_window']},{cfg['boll_ndev']})"
        f" 每{interval}分钟一轮")

    if args.once:
        scan_once(cfg, state, session, notify=False)
        return

    try:
        scan_once(cfg, state, session)
    except Exception as e:
        log(f"启动扫描异常: {e}")

    # 每 60 秒检查一次：交易时段内、距上轮满 interval 分钟才扫描，
    # 这样既保证 30 分钟节奏，又不会因为休眠跨过开盘时间而漏掉首轮
    last_scan = time.time()
    sleeping = False
    while True:
        time.sleep(60)
        active = args.force or in_session(cfg)
        if not active:
            if not sleeping:
                log("当前不在交易时段，休眠（开盘自动唤醒）")
                sleeping = True
            continue
        sleeping = False
        if time.time() - last_scan >= interval * 60:
            try:
                scan_once(cfg, state, session)
            except Exception as e:
                log(f"扫描异常: {e}")
            last_scan = time.time()
        try:
            maybe_scheduled_push(cfg, state, session)
        except Exception as e:
            log(f"定时推送异常: {e}")
        try:
            maybe_iv_report(cfg, state, session)
        except Exception as e:
            log(f"期权IV日报异常: {e}")


if __name__ == "__main__":
    main()
