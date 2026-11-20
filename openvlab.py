#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenVlab 公共 API 客户端（只读）

文档: https://api.openvlab.cn/skill.md
鉴权: Authorization: Bearer <API Key>
     Key 从项目根目录 openvlab.key 文件读取（或环境变量 OPENVLAB_KEY），
     不写入聊天/源码/git。

REST base: https://api.openvlab.cn/public-api/v1
统一响应信封: {"request_id","data","meta","error"}；空数据 = HTTP 200 + 空 data
"""

import json
import os

import requests

BASE = "https://api.openvlab.cn/public-api/v1"
KEY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "openvlab.key")


def get_api_key():
    env = os.environ.get("OPENVLAB_KEY", "").strip()
    if env:
        return env
    if os.path.exists(KEY_FILE):
        try:
            with open(KEY_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            return ""
    return ""


class OpenVlabClient:
    def __init__(self, api_key=None, session=None):
        self.key = api_key or get_api_key()
        self.session = session or requests.Session()
        self._account = None

    @property
    def enabled(self):
        return bool(self.key)

    def _get(self, path, **params):
        r = self.session.get(
            BASE + path,
            params={k: v for k, v in params.items() if v is not None},
            headers={"Authorization": f"Bearer {self.key}",
                     "User-Agent": "iv-scan/1.0"},
            timeout=20)
        if r.status_code == 401:
            raise PermissionError("OpenVlab 鉴权失败（API Key 无效或过期）")
        if r.status_code == 403:
            raise PermissionError(f"OpenVlab 拒绝访问: {r.text[:120]}")
        if r.status_code == 429:
            raise RuntimeError("OpenVlab 限流(429)")
        r.raise_for_status()
        body = r.json()
        if body.get("error"):
            raise RuntimeError(f"OpenVlab 错误: {body['error']}")
        return body.get("data")

    # ------------------------------------------------------------ 账户
    def account(self, force=False):
        if self._account is None or force:
            self._account = self._get("/account")
        return self._account

    # ------------------------------------------------------------ 目录
    def catalog_products(self, add_overseas=False):
        return self._get("/market/catalog/products",
                         add_overseas="1" if add_overseas else None)

    def expiries(self, product_und):
        return self._get(f"/market/catalog/{product_und}/expiries")

    # ------------------------------------------------------------ 波动率
    def surface_map(self, products=None, add_overseas=False):
        """全市场波动率曲面地图：一次调用返回所有品种各到期月的 ATM IV 等"""
        return self._get("/volatility/surface-map",
                         products=",".join(products) if products else None,
                         add_overseas="1" if add_overseas else None)

    def summary(self, product_und, exp):
        """单品种单到期月：ATM IV / IV Rank / skew 快照"""
        return self._get(f"/volatility/summary/{product_und}/{exp}")

    def iv_rank(self, product_und, exp, window=None):
        return self._get(f"/volatility/iv-rank/{product_und}/{exp}",
                         window=window)

    def atmv_series(self, product_und, exp, freq="60m"):
        return self._get(f"/volatility/atmv-series/{product_und}/{exp}",
                         freq=freq)


if __name__ == "__main__":
    c = OpenVlabClient()
    if not c.enabled:
        print("未找到 API Key：请把 Key 保存到 openvlab.key（项目根目录）")
    else:
        try:
            acc = c.account()
            print("账户信息:", json.dumps(acc, ensure_ascii=False)[:400])
        except Exception as e:
            print("账户查询失败:", e)
