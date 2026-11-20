# -*- coding: utf-8 -*-
"""GitHub Actions 数据快照：抓取K线+IV → 计算全量数据 → docs/data.json

页面静态托管（GitHub Pages），本脚本在云端定时运行生成数据。
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dashboard as db  # noqa: E402


def main():
    print("抓取K线（日线/30分/120分，约1分钟）…", flush=True)
    db._do_refresh()

    iv_all, no_rank = [], []
    try:
        import iv_scan
        print("刷新IV百分位（增量）…", flush=True)
        iv_scan.update()
        cfg100 = db.ms.load_cfg()
        cfg100.setdefault("iv", {})["percentile"] = 100   # 取全量列表，页面端按滑杆过滤
        report = iv_scan.build_report(cfg100)
        iv_all = report[0]
        no_rank = report[1]
    except Exception as e:
        print("IV刷新失败:", e, flush=True)

    data = db.build_data(["30", "120", "daily", "weekly", "monthly"], 4, 2, 25)
    data["iv_all"] = iv_all          # 全部有Rank的品种（页面按百分位滑杆过滤）
    data["no_rank"] = no_rank        # 暂无Rank清单

    os.makedirs("docs", exist_ok=True)
    with open("docs/data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    print(f"data.json 已生成：轨外{data['out_count']}项 | 低波{len(iv_all)}条 | "
          f"快照时间 {data['now']}", flush=True)


if __name__ == "__main__":
    main()
