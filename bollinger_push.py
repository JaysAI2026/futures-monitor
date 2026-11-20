#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
商品期货布林轨道触轨监控 + 微信推送

功能：
  - 轮询新浪财经期货分钟 K 线（1/5/15/30/60 分钟）
  - 用已完成的 N 根 K 线计算布林轨道（中轨=SMA，上下轨=中轨±N倍标准差）
  - 最新价触及上轨 / 下轨时，通过 Server酱 / pushplus / 企业微信机器人 推送到微信
  - 仅在由"轨道内 → 触轨"状态切换时推送，防止重复刷屏；每合约有独立冷却时间

用法：
  python bollinger_push.py           # 常驻监控
  python bollinger_push.py --once    # 只检测一轮后退出（用于测试）

数据来源（免费公开接口，无需鉴权）：
  - 分钟K线: stock2.finance.sina.com.cn  InnerFuturesNewService.getFewMinLine
  - 合约名称: hq.sinajs.cn
"""

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
STATE_PATH = os.path.join(BASE_DIR, "state.json")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")

KLINE_URL = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
             "var%20_k=/InnerFuturesNewService.getFewMinLine")
SPOT_URL = "https://hq.sinajs.cn/list=nf_{symbol}"
DAILY_URL = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
             "var%20_k=/InnerFuturesNewService.getDailyKLine")


# ---------------------------------------------------------------- 基础工具
def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def load_config():
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


# ---------------------------------------------------------------- 行情获取
def fetch_kline(symbol, period, session):
    """返回 [(时间, 开, 高, 低, 收), ...]，最后一根为正在形成的当期K线"""
    letters = re.match(r"[A-Za-z]+", symbol).group(0)
    params = {"symbol": symbol.upper(), "type": period,
              "_": int(time.time() * 1000)}
    headers = {
        "User-Agent": UA,
        "Referer": f"https://finance.sina.com.cn/futures/quotes/{letters}.shtml",
    }
    resp = session.get(KLINE_URL, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    m = re.search(r"var\s+_k\s*=\s*\((.*)\)\s*;", resp.text, re.S)
    if not m:
        raise ValueError("K线响应解析失败: " + resp.text[:120])
    data = json.loads(m.group(1))
    if not data:
        raise ValueError(f"无K线数据（合约代码 {symbol} 可能不存在）")
    return [(d["d"], float(d["o"]), float(d["h"]),
             float(d["l"]), float(d["c"])) for d in data]


def fetch_names(symbols, session):
    """批量取品种中文名，返回 {'RB': '螺纹钢', ...}"""
    if not symbols:
        return {}
    url = "https://hq.sinajs.cn/list=" + ",".join(f"nf_{s}0" for s in symbols)
    try:
        resp = session.get(url, headers={"Referer": "https://finance.sina.com.cn",
                                         "User-Agent": UA}, timeout=15)
        resp.encoding = "gbk"
    except Exception:
        return {}
    names = {}
    for line in resp.text.strip().splitlines():
        m = re.match(r'var hq_str_nf_([A-Za-z]+)0="([^,"]*)', line)
        if m:
            code = m.group(1).upper()
            nm = m.group(2).replace("连续", "").replace("主力", "").strip()
            names[code] = nm or code
    return names


def fetch_daily_kline(symbol, session, keep=150):
    """日线K线，返回 [(日期, 开, 高, 低, 收), ...]；只保留最近 keep 根"""
    letters = re.match(r"[A-Za-z]+", symbol).group(0)
    headers = {
        "User-Agent": UA,
        "Referer": f"https://finance.sina.com.cn/futures/quotes/{letters}.shtml",
    }
    params = {"symbol": symbol.upper(), "_": int(time.time() * 1000)}
    resp = session.get(DAILY_URL, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    m = re.search(r"var\s+_k\s*=\s*\((.*)\)\s*;", resp.text, re.S)
    if not m:
        raise ValueError("日线响应解析失败: " + resp.text[:120])
    data = json.loads(m.group(1))
    if not data:
        raise ValueError(f"无日线数据（{symbol} 可能不存在）")
    data = data[-keep:]
    return [(d["d"], float(d["o"]), float(d["h"]),
             float(d["l"]), float(d["c"])) for d in data]


def fetch_name(symbol, session):
    """获取合约中文名，失败时回退为代码本身"""
    try:
        headers = {"User-Agent": UA,
                   "Referer": "https://finance.sina.com.cn"}
        resp = session.get(SPOT_URL.format(symbol=symbol.upper()),
                           headers=headers, timeout=10)
        m = re.search(r'"(.*)"', resp.content.decode("gbk", errors="ignore"))
        if m:
            return m.group(1).split(",")[0] or symbol
    except Exception:
        pass
    return symbol


# ---------------------------------------------------------------- 布林计算
def bollinger(closes, window, ndev, ddof=0):
    """返回 (中轨, 上轨, 下轨)；ddof=0 总体标准差（默认），ddof=1 样本标准差"""
    seg = closes[-window:]
    if len(seg) < window:
        return None
    mid = sum(seg) / window
    var = sum((x - mid) ** 2 for x in seg) / (window - ddof)
    sd = math.sqrt(var)
    return mid, mid + ndev * sd, mid - ndev * sd


# ---------------------------------------------------------------- 微信推送
def push_wechat(title, content, push_cfg):
    channels = 0
    sendkey = push_cfg.get("serverchan_sendkey", "").strip()
    if sendkey:
        try:
            r = requests.post(f"https://sctapi.ftqq.com/{sendkey}.send",
                              data={"title": title, "desp": content},
                              timeout=15)
            log(f"  Server酱推送: HTTP {r.status_code} {r.text[:80]}")
            channels += 1
        except Exception as e:
            log(f"  Server酱推送失败: {e}")

    token = push_cfg.get("pushplus_token", "").strip()
    if token:
        try:
            r = requests.post("https://www.pushplus.plus/send",
                              json={"token": token, "title": title,
                                    "content": content,
                                    "template": "markdown"},
                              timeout=15)
            log(f"  pushplus推送: HTTP {r.status_code} {r.text[:80]}")
            channels += 1
        except Exception as e:
            log(f"  pushplus推送失败: {e}")

    webhook = push_cfg.get("wecom_webhook", "").strip()
    if webhook:
        try:
            r = requests.post(webhook,
                              json={"msgtype": "markdown",
                                    "markdown": {"content":
                                                 f"**{title}**\n{content}"}},
                              timeout=15)
            log(f"  企业微信推送: HTTP {r.status_code} {r.text[:80]}")
            channels += 1
        except Exception as e:
            log(f"  企业微信推送失败: {e}")

    if channels == 0:
        log("  未配置任何推送通道，请编辑 config.json 的 push 字段")


# ---------------------------------------------------------------- 交易时段
def in_session(cfg):
    now = datetime.now()
    if now.weekday() >= 5:  # 周末休市
        return False
    hm = now.strftime("%H:%M")
    for s, e in cfg.get("sessions", []):
        if s <= e:
            if s <= hm <= e:
                return True
        else:  # 跨午夜时段（如夜盘 21:00-01:00）
            if hm >= s or hm <= e:
                return True
    return False


# ---------------------------------------------------------------- 检测一轮
def check_symbol(symbol, cfg, session, states):
    period = cfg["period_minutes"]
    window = cfg["boll_window"]
    ndev = cfg["boll_ndev"]
    cooldown = cfg.get("notify_cooldown_minutes", 60) * 60

    bars = fetch_kline(symbol, period, session)
    if len(bars) < window + 1:
        log(f"{symbol}: K线数据不足（{len(bars)}根），跳过")
        return
    price = bars[-1][4]                       # 最新价 = 当期形成中K线的最新价
    band = bollinger([b[4] for b in bars[:-1]], window, ndev)
    if band is None:
        log(f"{symbol}: 数据不足，无法计算布林轨道")
        return
    mid, upper, lower = band

    if price >= upper:
        pos = "above"
    elif price <= lower:
        pos = "below"
    else:
        pos = "inside"

    st = states.setdefault(symbol, {"pos": "inside", "last": 0})
    now_ts = time.time()
    should_notify = (pos != "inside"
                     and st["pos"] != pos
                     and now_ts - st["last"] > cooldown)

    band_str = (f"上轨 {upper:.2f} / 中轨 {mid:.2f} / 下轨 {lower:.2f}")
    log(f"{symbol}: 最新价 {price:.2f} | {band_str} | 状态: {pos}")

    if should_notify:
        name = fetch_name(symbol, session)
        direction = "上轨" if pos == "above" else "下轨"
        title = f"{symbol} 触及布林{direction}"
        content = (
            f"- 合约：**{symbol}**（{name}）\n"
            f"- 周期：{period} 分钟\n"
            f"- 最新价：**{price:.2f}**\n"
            f"- 布林轨道：{band_str}\n"
            f"- 参数：{window}周期 / {ndev}倍标准差\n"
            f"- 时间：{datetime.now():%Y-%m-%d %H:%M:%S}\n"
        )
        log(f"{symbol}: 触发推送 -> 布林{direction}")
        push_wechat(title, content, cfg.get("push", {}))
        st["last"] = now_ts
    st["pos"] = pos


def run_cycle(cfg, states, session):
    for symbol in cfg["symbols"]:
        try:
            check_symbol(symbol, cfg, session, states)
        except Exception as e:
            log(f"{symbol}: 检测异常 {e}")
    save_state(states)


# ---------------------------------------------------------------- 入口
def main():
    parser = argparse.ArgumentParser(description="期货布林轨道触轨微信推送")
    parser.add_argument("--once", action="store_true",
                        help="只检测一轮后退出（测试用）")
    parser.add_argument("--force", action="store_true",
                        help="忽略交易时段限制（配合 --once 测试用）")
    args = parser.parse_args()

    cfg = load_config()
    states = load_state()
    session = requests.Session()

    log(f"启动监控: 合约={cfg['symbols']} 周期={cfg['period_minutes']}分钟 "
        f"窗口={cfg['boll_window']} 标准差={cfg['boll_ndev']}")

    if args.once:
        run_cycle(cfg, states, session)
        return

    while True:
        if args.force or in_session(cfg):
            run_cycle(cfg, states, session)
        else:
            log("当前不在交易时段，休眠")
        time.sleep(cfg.get("poll_interval_seconds", 30))


if __name__ == "__main__":
    main()
