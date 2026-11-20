"""内置爬虫：车站经由车次（车站大屏）、车次时刻表、站间余票、票价。

仅访问 12306 公开查询接口，全局限速，原始返回全部留档。
"""
import json
import re
import threading
import time

try:
    import requests
except ImportError:
    # 无 requests 三方库时回退至标准库 HTTP 会话，保证纯 Python 环境可直接启动与抓取
    import http.cookiejar
    import urllib.error
    import urllib.parse
    import urllib.request

    class _StdlibRequestException(Exception):
        pass

    class _StdlibResponse:
        def __init__(self, status_code: int, body: bytes, charset: str = "utf-8"):
            self.status_code = status_code
            self._body = body
            self.text = body.decode(charset or "utf-8", errors="replace")

        def raise_for_status(self):
            if self.status_code >= 400:
                raise _StdlibRequestException(f"HTTP {self.status_code}")

        def json(self):
            return json.loads(self.text)

    class _StdlibSession:
        def __init__(self):
            self.headers: dict[str, str] = {}
            self._opener = urllib.request.build_opener(
                urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
            )

        def request(self, method: str, url: str, params=None, data=None, headers=None, timeout=20):
            if params:
                qs = urllib.parse.urlencode(params)
                url = f"{url}{'&' if '?' in url else '?'}{qs}"
            req_headers = dict(self.headers)
            if headers:
                req_headers.update(headers)
            body = None
            if data is not None:
                body = urllib.parse.urlencode(data).encode("utf-8") if isinstance(data, dict) else data
            req = urllib.request.Request(url, data=body, headers=req_headers, method=method.upper())
            try:
                with self._opener.open(req, timeout=timeout) as resp:
                    charset = resp.headers.get_content_charset() or "utf-8"
                    return _StdlibResponse(resp.status, resp.read(), charset)
            except urllib.error.HTTPError as e:
                charset = e.headers.get_content_charset() if e.headers else "utf-8"
                return _StdlibResponse(e.code, e.read(), charset or "utf-8")
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                raise _StdlibRequestException(str(e)) from e

        def close(self):
            pass

    class _RequestsFallback:
        RequestException = _StdlibRequestException
        Session = _StdlibSession

    requests = _RequestsFallback()

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


class CrawlCancelled(CrawlError):
    pass


def _is_maintenance_window() -> bool:
    t = db.cn_now().time()
    return (t.hour == 23 and t.minute >= 30) or t.hour < 5


def _maint_hint() -> str:
    return "（当前处于北京时间 23:30–05:00 12306 夜间维护时段）" if _is_maintenance_window() else ""


class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self._lock = threading.Lock()
        self._req_lock = threading.Lock()
        self._cancel_tls = threading.local()
        self._last = 0.0
        self._left_ticket_url = None
        self._inited = False
        self._interval = MIN_INTERVAL
        self._ok_streak = 0
        self._manual = None
        self._load_rate()

    def set_cancel_event(self, ev: threading.Event | None):
        self._cancel_tls.ev = ev

    def _check_cancel(self, ev: threading.Event | None = None):
        token = ev or getattr(self._cancel_tls, "ev", None)
        if token is not None and token.is_set():
            raise CrawlCancelled("任务已取消")

    def _sleep(self, sec: float, ev: threading.Event | None = None):
        token = ev or getattr(self._cancel_tls, "ev", None)
        if sec <= 0:
            self._check_cancel(token)
            return
        if token is None:
            time.sleep(sec)
            return
        if token.wait(sec):
            raise CrawlCancelled("任务已取消")

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
        with self._lock:
            return {
                "mode": "manual" if self._manual is not None else "auto",
                "interval": round(self._interval, 2),
                "manual_interval": round(self._manual, 2) if self._manual is not None else None,
                "ok_streak": self._ok_streak,
                "floor": MIN_FLOOR,
                "ceiling": MAX_CEIL,
            }

    def set_rate(self, mode: str, interval: float | None = None) -> dict:
        with self._lock:
            if mode == "manual":
                val = float(interval if interval is not None else MIN_INTERVAL)
                self._manual = max(MIN_FLOOR, min(MAX_CEIL, val))
            else:
                self._manual = None
            manual = self._manual
        db.set_setting("rate_mode", "manual" if manual is not None else "auto")
        if manual is not None:
            db.set_setting("rate_interval", manual)
        return self.status()

    def _note_ok(self):
        with self._lock:
            self._ok_streak += 1
            if self._ok_streak >= 3:
                self._interval = max(MIN_FLOOR, self._interval * 0.8)
                self._ok_streak = 0

    def _note_throttled(self):
        with self._lock:
            self._ok_streak = 0
            self._interval = min(MAX_CEIL, self._interval * 2.0 + 0.3)

    def _throttle(self, ev: threading.Event | None = None):
        while True:
            self._check_cancel(ev)
            with self._lock:
                target = self._manual if self._manual is not None else self._interval
                wait = target - (time.monotonic() - self._last)
                if wait <= 0:
                    self._last = time.monotonic()
                    return
            self._sleep(min(wait, 0.25), ev)

    def _request(self, method: str, url: str, *, params=None, data=None, referer=None,
                 cancel_event: threading.Event | None = None):
        headers = {"Referer": referer} if referer else {}
        last_err = None
        for attempt in range(3):
            self._check_cancel(cancel_event)
            with self._req_lock:
                self._throttle(cancel_event)
                try:
                    resp = self.session.request(method, url, params=params, data=data,
                                                headers=headers, timeout=20)
                    if resp.status_code in (403, 429):
                        self._note_throttled()
                        resp.raise_for_status()
                    resp.raise_for_status()
                    self._note_ok()
                    return resp
                except CrawlCancelled:
                    raise
                except requests.RequestException as e:
                    last_err = e
                    self._note_throttled()
            self._sleep(2 * (attempt + 1), cancel_event)
        raise CrawlError(f"请求失败 {url}: {last_err}{_maint_hint()}")

    def reset_session(self):
        """重置 HTTP 会话与左票初始化状态，用于会话失效自愈或人工排障。"""
        with self._req_lock:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = requests.Session()
            self.session.headers.update({"User-Agent": UA})
            self._left_ticket_url = None
            self._inited = False

    def _json(self, method, url, **kw):
        resp = self._request(method, url, **kw)
        try:
            return resp.json()
        except ValueError:
            # 遇到非 JSON（如登录跳转/维护 HTML）时清除初始化标记，触发上层重初始化
            with self._req_lock:
                self._inited = False
            self._note_throttled()
            raise CrawlError(f"{url} 返回非 JSON（可能被风控或跳转维护页）{_maint_hint()}")

    def ensure_init(self) -> str:
        with self._req_lock:
            if self._inited and self._left_ticket_url:
                return self._left_ticket_url
        resp = self._request("GET", f"{KYFW}/otn/leftTicket/init")
        m = re.search(r"CLeftTicketUrl\s*=\s*'([^']+)'", resp.text)
        with self._req_lock:
            self._left_ticket_url = m.group(1) if m else "leftTicket/query"
            self._inited = True
            return self._left_ticket_url

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
            msg = str(payload.get("errorMsg") or payload.get("messages") or "")
            if not any(k in msg for k in ("无数据", "暂无", "不存在")):
                self._note_throttled()
            raise CrawlError(f"车站大屏返回异常: {str(payload)[:200]}{_maint_hint()}")
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
        params = {"leftTicketDTO.train_date": date,
                  "leftTicketDTO.from_station": from_telecode,
                  "leftTicketDTO.to_station": to_telecode,
                  "purpose_codes": "ADULT"}
        for retry in range(2):
            lt_url = self.ensure_init()
            try:
                payload = self._json("GET", f"{KYFW}/otn/{lt_url}",
                                     params=params, referer=f"{KYFW}/otn/leftTicket/init")
                if payload.get("status") is False and payload.get("c_url"):
                    with self._req_lock:
                        self._left_ticket_url = payload["c_url"]
                        lt_url = self._left_ticket_url
                    payload = self._json("GET", f"{KYFW}/otn/{lt_url}",
                                         params=params, referer=f"{KYFW}/otn/leftTicket/init")
                break
            except CrawlCancelled:
                raise
            except CrawlError:
                with self._req_lock:
                    need_reinit = not self._inited
                if retry == 0 and need_reinit:
                    continue
                raise
        if not isinstance((payload.get("data") or {}).get("result"), list):
            msgs = str(payload.get("messages") or "")
            if not any(k in msgs for k in ("预售", "日期", "暂无", "不存在", "没有")):
                self._note_throttled()
            raise CrawlError(f"余票查询返回异常: {str(payload)[:200]}{_maint_hint()}")
        return importer.ingest(json.dumps(payload, ensure_ascii=False), "left_ticket",
                               ctx={"from": from_telecode, "to": to_telecode, "date": date},
                               source="crawl")

    # ---- 票价 ----
    def crawl_price(self, train_no: str, from_station_no: str, to_station_no: str,
                    seat_types: str, date: str) -> dict:
        for retry in range(2):
            self.ensure_init()
            try:
                payload = self._json(
                    "GET", f"{KYFW}/otn/leftTicket/queryTicketPrice",
                    params={"train_no": train_no, "from_station_no": from_station_no,
                            "to_station_no": to_station_no, "seat_types": seat_types,
                            "train_date": date},
                    referer=f"{KYFW}/otn/leftTicket/init")
                break
            except CrawlCancelled:
                raise
            except CrawlError:
                with self._req_lock:
                    need_reinit = not self._inited
                if retry == 0 and need_reinit:
                    continue
                raise
        if not isinstance(payload.get("data"), dict) or "train_no" not in payload["data"]:
            msgs = str(payload.get("messages") or "")
            if not any(k in msgs for k in ("暂无", "不存在", "预售")):
                self._note_throttled()
            raise CrawlError(f"票价查询返回异常: {str(payload)[:200]}{_maint_hint()}")
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
