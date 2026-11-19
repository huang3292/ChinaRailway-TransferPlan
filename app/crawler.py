"""内置爬虫：车站经由车次（车站大屏）、车次时刻表、站间余票、票价。

仅访问 12306 公开查询接口，全局限速，原始返回全部留档。
"""
import json
import re
import threading
import time

import requests

from . import db, importer

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
KYFW = "https://kyfw.12306.cn"
MOBILE = "https://mobile.12306.cn"
MIN_INTERVAL = 1.0    # 初始/默认请求间隔（秒）
MIN_FLOOR = 0.4       # 智能模式最快间隔
MAX_CEIL = 12.0       # 智能模式最慢间隔（触发风控后）


class CrawlError(Exception):
    pass


class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self._lock = threading.Lock()
        self._last = 0.0
        self._left_ticket_url = None
        self._inited = False
        self._interval = MIN_INTERVAL
        self._ok_streak = 0
        self._manual = None
        self._load_rate()

    def _load_rate(self):
        try:
            if db.get_setting("rate_mode") == "manual":
                v = db.get_setting("rate_interval")
                if v:
                    self._manual = max(MIN_FLOOR, min(MAX_CEIL, float(v)))
        except Exception:
            self._manual = None

    # ---- 自适应限速 ----
    def status(self) -> dict:
        return {
            "mode": "manual" if self._manual is not None else "auto",
            "interval": round(self._interval, 2),
            "manual_interval": round(self._manual, 2) if self._manual is not None else None,
            "ok_streak": self._ok_streak,
            "floor": MIN_FLOOR,
            "ceiling": MAX_CEIL,
        }

    def set_rate(self, mode: str, interval: float | None = None) -> dict:
        if mode == "manual":
            val = float(interval if interval is not None else MIN_INTERVAL)
            self._manual = max(MIN_FLOOR, min(MAX_CEIL, val))
        else:
            self._manual = None
        db.set_setting("rate_mode", "manual" if self._manual is not None else "auto")
        if self._manual is not None:
            db.set_setting("rate_interval", self._manual)
        return self.status()

    def _note_ok(self):
        self._ok_streak += 1
        if self._ok_streak >= 3:
            self._interval = max(MIN_FLOOR, self._interval * 0.8)
            self._ok_streak = 0

    def _note_throttled(self):
        self._ok_streak = 0
        self._interval = min(MAX_CEIL, self._interval * 2.0 + 0.3)

    def _throttle(self):
        with self._lock:
            target = self._manual if self._manual is not None else self._interval
            wait = target - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()

    def _request(self, method: str, url: str, *, params=None, data=None, referer=None):
        headers = {"Referer": referer} if referer else {}
        last_err = None
        for attempt in range(3):
            self._throttle()
            try:
                resp = self.session.request(method, url, params=params, data=data,
                                            headers=headers, timeout=20)
                resp.raise_for_status()
                self._note_ok()
                return resp
            except requests.RequestException as e:
                last_err = e
                self._note_throttled()
                time.sleep(2 * (attempt + 1))
        raise CrawlError(f"请求失败 {url}: {last_err}")

    def _json(self, method, url, **kw):
        resp = self._request(method, url, **kw)
        try:
            return resp.json()
        except ValueError:
            self._note_throttled()
            raise CrawlError(f"{url} 返回非 JSON（可能被风控，稍后再试）")

    def ensure_init(self):
        if self._inited:
            return
        resp = self._request("GET", f"{KYFW}/otn/leftTicket/init")
        m = re.search(r"CLeftTicketUrl\s*=\s*'([^']+)'", resp.text)
        self._left_ticket_url = m.group(1) if m else "leftTicket/query"
        self._inited = True

    # ---- 车站站名表 ----
    def crawl_stations(self) -> dict:
        resp = self._request("GET", f"{KYFW}/otn/resources/js/framework/station_name.js")
        return importer.ingest(resp.text, "stations", source="crawl")

    # ---- 车站经由的所有车次（车站大屏，含到发时刻/跨天数/开行日期） ----
    def crawl_station_trains(self, telecode: str, date: str) -> dict:
        payload = self._json(
            "POST", f"{MOBILE}/wxxcx/wechat/bigScreen/queryTrainByStation",
            data={"train_start_date": date.replace("-", ""), "train_station_code": telecode})
        if not isinstance(payload.get("data"), list):
            self._note_throttled()
            raise CrawlError(f"车站大屏返回异常: {str(payload)[:200]}")
        return importer.ingest(json.dumps(payload, ensure_ascii=False), "station_screen",
                               ctx={"station": telecode, "date": date}, source="crawl")

    # ---- 车次时刻表（全部经停站） ----
    def crawl_timetable(self, train_no: str, date: str,
                        from_telecode: str, to_telecode: str) -> dict:
        payload = self._json(
            "GET", f"{KYFW}/otn/czxx/queryByTrainNo",
            params={"train_no": train_no, "from_station_telecode": from_telecode,
                    "to_station_telecode": to_telecode, "depart_date": date},
            referer=f"{KYFW}/otn/leftTicket/init")
        ctx = {"train_no": train_no, "date": date}
        try:
            return importer.ingest(json.dumps(payload, ensure_ascii=False),
                                   "train_stops", ctx=ctx, source="crawl")
        except ValueError:
            # 网页接口失败时退回小程序停站接口
            payload = self._json(
                "POST", f"{MOBILE}/wxxcx/wechat/ticketInfo/getStopStation",
                data={"train_no": train_no, "train_date": date.replace("-", ""),
                      "f_station_telcode": from_telecode})
            return importer.ingest(json.dumps(payload, ensure_ascii=False),
                                   "train_stops", ctx=ctx, source="crawl")

    # ---- 站间余票（含车次/时刻/席别余票） ----
    def crawl_tickets(self, from_telecode: str, to_telecode: str, date: str) -> dict:
        self.ensure_init()
        params = {"leftTicketDTO.train_date": date,
                  "leftTicketDTO.from_station": from_telecode,
                  "leftTicketDTO.to_station": to_telecode,
                  "purpose_codes": "ADULT"}
        payload = self._json("GET", f"{KYFW}/otn/{self._left_ticket_url}",
                             params=params, referer=f"{KYFW}/otn/leftTicket/init")
        if payload.get("status") is False and payload.get("c_url"):
            self._left_ticket_url = payload["c_url"]
            payload = self._json("GET", f"{KYFW}/otn/{self._left_ticket_url}",
                                 params=params, referer=f"{KYFW}/otn/leftTicket/init")
        if not isinstance((payload.get("data") or {}).get("result"), list):
            self._note_throttled()
            raise CrawlError(f"余票查询返回异常: {str(payload)[:200]}")
        return importer.ingest(json.dumps(payload, ensure_ascii=False), "left_ticket",
                               ctx={"from": from_telecode, "to": to_telecode, "date": date},
                               source="crawl")

    # ---- 票价 ----
    def crawl_price(self, train_no: str, from_station_no: str, to_station_no: str,
                    seat_types: str, date: str) -> dict:
        self.ensure_init()
        payload = self._json(
            "GET", f"{KYFW}/otn/leftTicket/queryTicketPrice",
            params={"train_no": train_no, "from_station_no": from_station_no,
                    "to_station_no": to_station_no, "seat_types": seat_types,
                    "train_date": date},
            referer=f"{KYFW}/otn/leftTicket/init")
        if not isinstance(payload.get("data"), dict) or "train_no" not in payload["data"]:
            self._note_throttled()
            raise CrawlError(f"票价查询返回异常: {str(payload)[:200]}")
        return importer.ingest(json.dumps(payload, ensure_ascii=False), "price",
                               ctx={"train_no": train_no, "date": date,
                                    "from_station_no": from_station_no,
                                    "to_station_no": to_station_no},
                               source="crawl")


crawler = Crawler()


def station_timetable_context(train_no: str) -> dict | None:
    """从已入库数据中找车次的始发/终到电报码，供时刻表查询使用。"""
    r = db.rows("""SELECT start_station_telecode a, end_station_telecode b, start_train_date d
                   FROM station_trains WHERE train_no=? AND start_station_telecode IS NOT NULL
                   ORDER BY station_train_date DESC LIMIT 1""", (train_no,))
    if r:
        return {"from": r[0]["a"], "to": r[0]["b"], "date": r[0]["d"]}
    r = db.rows("""SELECT from_telecode a, to_telecode b, train_date d FROM tickets
                   WHERE train_no=? ORDER BY train_date DESC LIMIT 1""", (train_no,))
    if r:
        return {"from": r[0]["a"], "to": r[0]["b"], "date": r[0]["d"]}
    return None