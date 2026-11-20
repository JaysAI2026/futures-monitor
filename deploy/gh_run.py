# -*- coding: utf-8 -*-
"""GitHub Actions 一次性运行器：定时触发 → 扫描+推送 → 状态由工作流提交回仓库

环境变量：
  SC_SENDKEY  Server酱SendKey（来自Actions Secrets，优先于config.json）
  FORCE=1     手动测试模式（不校验时点、不标记已推、不计额度外推送）
"""
import os
import sys
from datetime import datetime

# 以 python deploy/gh_run.py 运行时，把仓库根目录加入模块搜索路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

import market_scan as ms

# Actions 传入的 SendKey 优先于 config.json（避免密钥进仓库）
_cfg = ms.load_cfg()
_sendkey = os.environ.get("SC_SENDKEY", "").strip()
if _sendkey:
    _cfg.setdefault("push", {})["serverchan_sendkey"] = _sendkey


def _pick_slot(cfg, state, now, force):
    """与本地 maybe_scheduled_push 同逻辑：返回到点时点或None"""
    slots = cfg.get("scan", {}).get("push_times") or []
    today = now.strftime("%Y-%m-%d")
    done = state.setdefault("_pushed_slots", {})
    if done.get("_date") != today:
        done.clear()
        done["_date"] = today
    grace = int(cfg.get("scan", {}).get("slot_grace_minutes", 15))
    now_min = now.hour * 60 + now.minute
    for s in sorted(str(x).strip() for x in slots):
        if not s or done.get(s):
            continue
        try:
            h, m = s.split(":")
        except ValueError:
            continue
        overdue = now_min - (int(h) * 60 + int(m))
        if force and overdue >= 0:
            return s
        if 0 <= overdue <= grace + 10:
            return s
    return None


def main():
    now = datetime.now()
    print(f"[{now:%Y-%m-%d %H:%M:%S}] Actions 运行开始")
    force = os.environ.get("FORCE") == "1"

    cfg = _cfg
    state = ms.load_state()
    slot = _pick_slot(cfg, state, now, force)
    if slot is None:
        print("无到点时点（可能已推过或定时延迟超宽限期），退出")
        return

    title, body = ms.build_push_message(cfg, state, requests.Session())

    limit = cfg.get("scan", {}).get("daily_push_limit", 0)
    if force or ms.push_budget_left(state, limit):
        ms.push_wechat(title, body, dict(cfg.get("push", {})))
        if not force:
            state["_push_count"] = state.get("_push_count", 0) + 1
            state.setdefault("_pushed_slots", {})[slot] = True
        print(f"已推送 {slot}：{title}")
    else:
        print(f"[额度已满 {limit}条/日] {slot} 未发送")

    ms.save_state(state)
    print(body[:800])


if __name__ == "__main__":
    main()
