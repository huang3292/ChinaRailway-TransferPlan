import json
import re
import threading
import time
import uuid
from datetime import date as dt_date
from pathlib import Path

try:
    from fastapi import FastAPI, HTTPException, UploadFile
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel
except ImportError:
    import types as _types
    from typing import Any as _Any, Union as _Union, get_args as _get_args, get_origin as _get_origin, get_type_hints as _get_type_hints

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str = ""):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class UploadFile:
        def __init__(self, filename: str = "", content: bytes = b""):
            self.filename = filename
            self._content = content

        async def read(self) -> bytes:
            return self._content

    class FileResponse:
        def __init__(self, path):
            self.path = str(path)

    class StaticFiles:
        def __init__(self, directory):
            self.directory = str(directory)

    def _coerce_field(val, tp):
        if tp is None or tp is _Any:
            return val
        origin = _get_origin(tp)
        if origin in (_types.UnionType, _Union):
            args = _get_args(tp)
            if val is None and any(a is type(None) for a in args):
                return None
            non_none = [a for a in args if a is not type(None)]
            if val == "" and len(non_none) == 1 and non_none[0] in (int, float):
                return None
            if len(non_none) == 1:
                return _coerce_field(val, non_none[0])
            return val
        if origin is list:
            item_tp = (_get_args(tp) or (_Any,))[0]
            return [_coerce_field(x, item_tp) for x in (val or [])]
        if isinstance(tp, type) and issubclass(tp, BaseModel) and isinstance(val, dict):
            return tp(**val)
        if tp is bool and not isinstance(val, bool):
            return str(val).strip().lower() in ("1", "true", "yes", "on") if isinstance(val, str) else bool(val)
        if tp is int and not isinstance(val, int):
            return int(val)
        if tp is float and not isinstance(val, float):
            return float(val)
        return val

    class BaseModel:
        def __init__(self, **kw):
            cls = self.__class__
            try:
                hints = _get_type_hints(cls, globalns=globals(), localns=locals())
            except Exception:
                hints = dict(getattr(cls, "__annotations__", {}))
            for k, tp in hints.items():
                if k in kw:
                    setattr(self, k, _coerce_field(kw[k], tp))
                elif hasattr(cls, k):
                    val = getattr(cls, k)
                    setattr(self, k, list(val) if isinstance(val, list) else dict(val) if isinstance(val, dict) else val)
                else:
                    raise ValueError(f"缺少必填字段: {k}")
            for k, v in kw.items():
                if k not in hints:
                    setattr(self, k, v)

        def model_dump(self, exclude=None):
            ex = exclude or set()
            return {k: v for k, v in self.__dict__.items() if k not in ex}

    class _Route:
        def __init__(self, methods: set[str], path: str, endpoint):
            self.methods = methods
            self.path = path
            self.endpoint = endpoint

    class FastAPI:
        def __init__(self, title: str = ""):
            self.title = title
            self.routes: list[_Route] = []
            self.startup_handlers: list = []
            self.mounts: list[tuple[str, StaticFiles]] = []

        def on_event(self, event: str):
            def _dec(fn):
                if event == "startup":
                    self.startup_handlers.append(fn)
                return fn
            return _dec

        def _add(self, method: str, path: str):
            def _dec(fn):
                self.routes.append(_Route({method}, path, fn))
                return fn
            return _dec

        def get(self, path: str):
            return self._add("GET", path)

        def post(self, path: str):
            return self._add("POST", path)

        def delete(self, path: str):
            return self._add("DELETE", path)

        def mount(self, path: str, app_obj, name: str = ""):
            self.mounts.append((path.rstrip("/"), app_obj))


from . import db, importer, transfer
from .crawler import CrawlCancelled, CrawlError, crawler, station_timetable_context

app = FastAPI(title="12306 智能中转")
STATIC = Path(__file__).resolve().parent.parent / "static"

_jobs_lock = threading.Lock()
jobs: dict[str, dict] = {}
_job_cancels: dict[str, threading.Event] = {}
MAX_JOBS = 100


def _create_job(initial: dict) -> tuple[str, threading.Event]:
    job_id = uuid.uuid4().hex[:8]
    ev = threading.Event()
    initial = dict(initial, created_at=time.time())
    with _jobs_lock:
        if len(jobs) >= MAX_JOBS:
            done_ids = [jid for jid, j in jobs.items() if j.get("status") != "running"]
            done_ids.sort(key=lambda jid: jobs[jid].get("created_at", 0))
            for jid in done_ids[:len(jobs) - MAX_JOBS + 1]:
                jobs.pop(jid, None)
                _job_cancels.pop(jid, None)
        jobs[job_id] = initial
        _job_cancels[job_id] = ev
    return job_id, ev


def _job_update(job_id: str, **kwargs):
    with _jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)


def _job_inc(job_id: str, field: str, delta: int = 1):
    with _jobs_lock:
        if job_id in jobs:
            jobs[job_id][field] = jobs[job_id].get(field, 0) + delta


def _job_append(job_id: str, field: str, val):
    with _jobs_lock:
        if job_id in jobs:
            jobs[job_id].setdefault(field, []).append(val)


@app.on_event("startup")
def startup():
    db.init_db()


def _fmt_station(name: str | None, telecode: str | None) -> str:
    """统一格式化车站为「站名(电报码)」。"""
    if name and telecode:
        return f"{name}({telecode})"
    return name or telecode or "--"


def _fmt_train(code: str | None, train_no: str | None) -> str:
    """统一格式化车次为「车次(代号)」。"""
    if code and train_no:
        return f"{code}({train_no})"
    return code or train_no or "--"


def _station_name_map(telecodes: set[str] | list[str]) -> dict[str, str]:
    tcs = [x for x in set(telecodes) if x]
    if not tcs:
        return {}
    ph = ",".join("?" * len(tcs))
    return {r["telecode"]: r["name"]
            for r in db.rows(f"SELECT telecode, name FROM stations WHERE telecode IN ({ph})", tuple(tcs))}


def _train_code_map(train_nos: set[str] | list[str]) -> dict[str, str]:
    tns = [x for x in set(train_nos) if x]
    if not tns:
        return {}
    out = {}
    ph = ",".join("?" * len(tns))
    for tbl in ("station_trains", "train_stops", "tickets"):
        for r in db.rows(
            f"SELECT train_no, station_train_code FROM {tbl} WHERE train_no IN ({ph}) AND station_train_code IS NOT NULL",
            tuple(tns),
        ):
            out.setdefault(r["train_no"], r["station_train_code"])
    return out


def _validate_date(d: str) -> str:
    """校验日期字符串为合法的 YYYY-MM-DD 格式。"""
    if not isinstance(d, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
        raise ValueError(f"日期格式不合法“{d}”，请使用 YYYY-MM-DD")
    try:
        dt_date.fromisoformat(d)
    except ValueError:
        raise ValueError(f"日期不存在“{d}”，请使用有效的 YYYY-MM-DD")
    return d


def _check_date(d: str) -> str:
    try:
        return _validate_date(d)
    except ValueError as e:
        raise HTTPException(400, str(e))


def _station_or_400(q: str) -> dict:
    s = db.resolve_station(q)
    if not s:
        raise HTTPException(400, f"车站“{q}”不存在，请先在「爬取数据」页更新车站站名表")
    return s


def _run(fn, *args):
    try:
        return fn(*args)
    except (CrawlError, ValueError) as e:
        raise HTTPException(400, str(e))


# ---------- 基础 ----------
@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/stats")
def stats():
    out = {}
    for t in ("stations", "station_trains", "train_stops", "tickets", "prices", "raw_payloads"):
        out[t] = db.rows(f"SELECT COUNT(*) c FROM {t}")[0]["c"]
    return out


@app.get("/api/stations")
def stations(q: str = "", limit: int = 30):
    if q:
        like = f"%{q}%"
        return db.rows("""SELECT * FROM stations WHERE name LIKE ? OR telecode LIKE ? OR pinyin LIKE ?
                          OR short LIKE ?
                          ORDER BY CASE WHEN name=? OR telecode=? THEN 0
                                        WHEN name LIKE ? OR pinyin LIKE ? THEN 1 ELSE 2 END, name
                          LIMIT ?""",
                     (like, like, like, like, q, q.upper(), f"{q}%", f"{q}%", limit))
    return db.rows("SELECT * FROM stations LIMIT ?", (limit,))


# ---------- 请求速率 ----------
@app.get("/api/rate")
def get_rate():
    return crawler.status()


class RateBody(BaseModel):
    mode: str
    interval: float | None = None


@app.post("/api/rate")
def set_rate(body: RateBody):
    if body.mode not in ("auto", "manual"):
        raise HTTPException(400, "mode 必须为 auto 或 manual")
    return crawler.set_rate(body.mode, body.interval)


@app.post("/api/crawler/reset")
def reset_crawler_session():
    """手动重置爬虫 HTTP 会话与初始化状态。"""
    crawler.reset_session()
    return {"ok": True}


# ---------- 导入 ----------
class ImportBody(BaseModel):
    payload: str
    api_type: str | None = None
    train_no: str | None = None
    station_train_code: str | None = None
    date: str | None = None
    from_station_no: str | None = None
    to_station_no: str | None = None


@app.post("/api/import")
def do_import(body: ImportBody):
    if body.date:
        _check_date(body.date)
    ctx = body.model_dump(exclude={"payload", "api_type"})
    return _run(importer.ingest, body.payload, body.api_type, ctx, "import")


@app.post("/api/import/file")
async def do_import_file(file: UploadFile, api_type: str | None = None,
                         train_no: str | None = None, station_train_code: str | None = None,
                         date: str | None = None,
                         from_station_no: str | None = None, to_station_no: str | None = None):
    if date:
        _check_date(date)
    text = (await file.read()).decode("utf-8-sig", errors="replace")
    ctx = {"train_no": train_no, "station_train_code": station_train_code, "date": date,
           "from_station_no": from_station_no, "to_station_no": to_station_no}
    return _run(importer.ingest, text, api_type or None, ctx, "import")


# ---------- 爬取 ----------
class StationDate(BaseModel):
    station: str
    date: str


@app.post("/api/crawl/stations")
def crawl_stations():
    return _run(crawler.crawl_stations)


@app.post("/api/crawl/station_trains")
def crawl_station_trains(body: StationDate):
    _check_date(body.date)
    s = _station_or_400(body.station)
    return _run(crawler.crawl_station_trains, s["telecode"], body.date)


class TimetableReq(BaseModel):
    train_no: str
    date: str | None = None
    from_telecode: str | None = None
    to_telecode: str | None = None


@app.post("/api/crawl/timetable")
def crawl_timetable(body: TimetableReq):
    if body.date:
        _check_date(body.date)
    ctx = station_timetable_context(body.train_no)
    date = body.date or (ctx and ctx["date"])
    f = body.from_telecode or (ctx and ctx["from"])
    t = body.to_telecode or (ctx and ctx["to"])
    if not (date and f and t):
        raise HTTPException(400, "缺少日期或始发/终到站电报码，请先爬取该站经由车次或余票")
    return _run(crawler.crawl_timetable, body.train_no, date, f, t)


class TicketReq(BaseModel):
    from_station: str
    to_station: str
    date: str
    with_prices: bool = False


@app.post("/api/crawl/tickets")
def crawl_tickets(body: TicketReq):
    _check_date(body.date)
    f, t = _station_or_400(body.from_station), _station_or_400(body.to_station)
    result = _run(crawler.crawl_tickets, f["telecode"], t["telecode"], body.date)
    if body.with_prices:
        job_id, ev = _create_job({"status": "running", "done": 0, "total": 0, "errors": []})
        threading.Thread(target=_price_job, args=(job_id, ev, f["telecode"], t["telecode"], body.date),
                         daemon=True).start()
        result["price_job"] = job_id
    return result


def _price_job(job_id, ev: threading.Event, f_tc, t_tc, date):
    crawler.set_cancel_event(ev)
    try:
        tk = db.rows("""SELECT train_no,station_train_code,from_station_no,to_station_no,seat_types FROM tickets
                        WHERE from_telecode=? AND to_telecode=? AND train_date=?""", (f_tc, t_tc, date))
        _job_update(job_id, total=len(tk))
        for r in tk:
            if ev.is_set():
                _job_update(job_id, status="cancelled")
                return
            t_label = _fmt_train(r["station_train_code"], r["train_no"])
            _job_update(job_id, current=f"票价 {t_label}")
            try:
                crawler.crawl_price(r["train_no"], r["from_station_no"], r["to_station_no"],
                                    r["seat_types"] or "", date)
            except CrawlCancelled:
                _job_update(job_id, status="cancelled")
                return
            except Exception as e:
                _job_append(job_id, "errors", f"票价 {t_label}: {e}")
            _job_inc(job_id, "done")
        _job_update(job_id, status="done")
    finally:
        crawler.set_cancel_event(None)
        db.close_conn()


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    with _jobs_lock:
        if job_id not in jobs:
            raise HTTPException(404, "任务不存在")
        j = jobs[job_id]
        return {k: (list(v) if isinstance(v, list) else v) for k, v in j.items() if k != "created_at"}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    with _jobs_lock:
        if job_id not in jobs:
            raise HTTPException(404, "任务不存在")
        ev = _job_cancels.get(job_id)
        if ev:
            ev.set()
        if jobs[job_id].get("status") == "running":
            jobs[job_id]["status"] = "cancelled"
        return {"job_id": job_id, "status": jobs[job_id]["status"]}


class PriceReq(BaseModel):
    train_no: str
    from_station_no: str
    to_station_no: str
    seat_types: str = ""
    date: str


@app.post("/api/crawl/price")
def crawl_price(body: PriceReq):
    _check_date(body.date)
    return _run(crawler.crawl_price, body.train_no, body.from_station_no,
                body.to_station_no, body.seat_types, body.date)


# ---------- 一键导入向导 / 在线模式共用 ----------
MAX_TIMETABLES = 200   # 单次向导最多抓取的车次时刻表数（大站车次很多时封顶）
_TRAIN_CLASS_ORDER = {"G": 0, "C": 0, "D": 0, "Z": 1, "T": 1, "K": 1, "S": 3}


def _train_rank(code: str | None) -> tuple[int, str]:
    """按车型优先级排序：G/C/D > Z/T/K > 普速/其他 > S(市郊)。"""
    c = (code or "").strip().upper()
    prefix = c[0] if c else ""
    return (_TRAIN_CLASS_ORDER.get(prefix, 2), c)


class RouteImportReq(BaseModel):
    from_city: str
    to_city: str
    date: str
    deep: bool = True


def _route_job(job_id: str, ev: threading.Event, from_q: str, to_q: str, date: str, deep: bool):
    crawler.set_cancel_event(ev)

    def run(txt, fn, *args):
        if ev.is_set():
            raise CrawlCancelled("任务已取消")
        _job_update(job_id, current=txt)
        try:
            fn(*args)
        except CrawlCancelled:
            raise
        except Exception as e:
            _job_append(job_id, "errors", f"{txt}: {e}")
        _job_inc(job_id, "done")
        _job_append(job_id, "log", txt)

    try:
        fstations = db.resolve_stations(from_q, True)
        tstations = db.resolve_stations(to_q, True)
        if not fstations or not tstations:
            _job_update(job_id, status="error", error="车站/城市不存在，请先在「爬取数据」页更新车站站名表")
            return
        tc_name = {s["telecode"]: s["name"] for s in fstations + tstations}
        ftcs = [s["telecode"] for s in fstations]
        ttcs = [s["telecode"] for s in tstations]
        all_tcs = list(dict.fromkeys(ftcs + ttcs))

        _job_update(job_id, phase="screen", total=len(all_tcs))
        for tc in all_tcs:
            run(f"车站大屏 {_fmt_station(tc_name.get(tc), tc)}", crawler.crawl_station_trains, tc, date)

        if deep:
            ph = ",".join("?" * len(all_tcs))
            rows = db.rows(
                f"""SELECT DISTINCT train_no,station_train_code,start_station_telecode,end_station_telecode,start_train_date
                    FROM station_trains WHERE station_telecode IN ({ph}) AND station_train_date=?""",
                (*all_tcs, date))
            uniq = {}
            for r in rows:
                if r["train_no"] and r["train_no"] not in uniq:
                    uniq[r["train_no"]] = r
            all_items = sorted(uniq.values(), key=lambda r: _train_rank(r["station_train_code"]))
            items = all_items[:MAX_TIMETABLES]
            truncated = max(0, len(all_items) - len(items))
            with _jobs_lock:
                cur_total = jobs[job_id]["total"]
            _job_update(job_id, phase="timetable", total=cur_total + len(items), truncated=truncated)
            if truncated > 0:
                _job_append(
                    job_id,
                    "log",
                    f"【时刻表截断提示】共命中 {len(all_items)} 趟，按车型优先抓取前 {len(items)} 趟，跳过其余 {truncated} 趟",
                )
            for r in items:
                tn = r["train_no"]
                t_label = _fmt_train(r["station_train_code"], tn)
                f = r["start_station_telecode"]
                t = r["end_station_telecode"]
                d = r["start_train_date"] or date
                if not f or not t:
                    ctx = station_timetable_context(tn) or {}
                    f, t, d = ctx.get("from"), ctx.get("to"), ctx.get("date") or date
                if not f or not t:
                    run(f"时刻表 {t_label} 跳过（缺始发/终到站）", lambda: None)
                    continue
                run(f"时刻表 {t_label}", crawler.crawl_timetable, tn, d, f, t)

        pairs = [(f, t) for f in ftcs for t in ttcs if f != t]
        with _jobs_lock:
            cur_total = jobs[job_id]["total"]
        _job_update(job_id, phase="tickets", total=cur_total + len(pairs))
        for f, t in pairs:
            run(f"余票 {_fmt_station(tc_name.get(f), f)}→{_fmt_station(tc_name.get(t), t)}",
                crawler.crawl_tickets, f, t, date)

        if deep:
            phf, pht = ",".join("?" * len(ftcs)), ",".join("?" * len(ttcs))
            tk = db.rows(
                f"""SELECT t.train_no,t.station_train_code,t.from_station_no,t.to_station_no,t.seat_types
                    FROM tickets t
                    WHERE t.from_telecode IN ({phf}) AND t.to_telecode IN ({pht}) AND t.train_date=?
                      AND NOT EXISTS (
                          SELECT 1 FROM prices p WHERE p.train_no=t.train_no
                            AND p.from_station_no=t.from_station_no
                            AND p.to_station_no=t.to_station_no
                            AND p.train_date=t.train_date
                      )""",
                (*ftcs, *ttcs, date))
            with _jobs_lock:
                cur_total = jobs[job_id]["total"]
            _job_update(job_id, phase="price", total=cur_total + len(tk))
            for r in tk:
                run(f"票价 {_fmt_train(r['station_train_code'], r['train_no'])}",
                    crawler.crawl_price, r["train_no"], r["from_station_no"] or "",
                    r["to_station_no"] or "", r["seat_types"] or "", date)

        _job_update(job_id, phase="done", status="done")
    except CrawlCancelled:
        _job_update(job_id, status="cancelled")
    except Exception as e:
        _job_update(job_id, status="error", error=str(e))
    finally:
        crawler.set_cancel_event(None)
        db.close_conn()


@app.post("/api/import/route")
def import_route(body: RouteImportReq):
    _check_date(body.date)
    fstations = db.resolve_stations(body.from_city, True)
    tstations = db.resolve_stations(body.to_city, True)
    if not fstations or not tstations:
        raise HTTPException(400, "车站/城市不存在，请先在「爬取数据」页更新车站站名表")
    job_id, ev = _create_job({"status": "running", "done": 0, "total": 0, "phase": "init",
                               "current": "", "truncated": 0, "log": [], "errors": [], "result": {}})
    threading.Thread(target=_route_job, args=(job_id, ev, body.from_city, body.to_city,
                                              body.date, body.deep), daemon=True).start()
    return {"job_id": job_id,
            "from_stations": [s["name"] for s in fstations],
            "to_stations": [s["name"] for s in tstations],
            "from_station_labels": [_fmt_station(s["name"], s["telecode"]) for s in fstations],
            "to_station_labels": [_fmt_station(s["name"], s["telecode"]) for s in tstations],
            "date": body.date, "deep": body.deep}


# ---------- 浏览 ----------
@app.get("/api/station_trains")
def station_trains(station: str, date: str | None = None):
    if date:
        _check_date(date)
    s = _station_or_400(station)
    if date:
        return db.rows("""SELECT * FROM station_trains WHERE station_telecode=? AND station_train_date=?
                          ORDER BY start_time""", (s["telecode"], date))
    return db.rows("""SELECT * FROM station_trains WHERE station_telecode=?
                      ORDER BY station_train_date, start_time LIMIT 500""", (s["telecode"],))


@app.get("/api/timetable")
def timetable(train_no: str):
    n2c = db.station_lookup()
    r = db.rows("SELECT DISTINCT train_date FROM train_stops WHERE train_no=? ORDER BY train_date DESC LIMIT 1",
                (train_no,))
    if r:
        stops = db.rows("""SELECT * FROM train_stops WHERE train_no=? AND train_date=? ORDER BY station_no""",
                        (train_no, r[0]["train_date"]))
        for s in stops:
            if not s.get("telecode"):
                s["telecode"] = n2c.get(s.get("station_name"))
        code = next((s.get("station_train_code") for s in stops if s.get("station_train_code")), None)
        if not code:
            code = _train_code_map([train_no]).get(train_no)
        return {"train_no": train_no, "station_train_code": code,
                "stops": stops, "complete": True, "source": "train_stops"}
    # 退回车站大屏：把已抓取各站的经停拼成（可能不完整的）时刻表
    scr = db.rows("""SELECT station_train_code, station_no, station_name, station_telecode, arrive_time, start_time,
                            stopover_time, station_train_date
                     FROM station_trains WHERE train_no=? AND station_no IS NOT NULL
                     ORDER BY station_no, station_train_date DESC""", (train_no,))
    seen, stops, code = set(), [], None
    for s in scr:
        code = code or s.get("station_train_code")
        if s["station_no"] in seen:
            continue
        seen.add(s["station_no"])
        stops.append({"station_no": s["station_no"], "station_name": s["station_name"],
                      "telecode": s["station_telecode"] or n2c.get(s.get("station_name")),
                      "arrive_time": s["arrive_time"], "start_time": s["start_time"],
                      "stopover_time": s["stopover_time"], "enabled": 1})
    if stops:
        return {"train_no": train_no, "station_train_code": code,
                "stops": stops, "complete": False, "source": "station_screen"}
    raise HTTPException(404, f"车次({train_no})暂无时刻表数据")


@app.get("/api/trains")
def trains(q: str = "", limit: int = 50):
    like = f"%{q}%"
    return db.rows("""SELECT train_no, station_train_code, MAX(train_date) train_date,
                             COUNT(*) stop_count FROM train_stops
                      WHERE station_train_code LIKE ? OR train_no LIKE ?
                      GROUP BY train_no LIMIT ?""", (like, like, limit))


@app.get("/api/tickets")
def tickets(from_station: str, to_station: str, date: str):
    _check_date(date)
    f, t = _station_or_400(from_station), _station_or_400(to_station)
    return db.rows("""SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?
                      ORDER BY start_time""", (f["telecode"], t["telecode"], date))


@app.get("/api/prices")
def prices(train_no: str):
    return db.rows("SELECT * FROM prices WHERE train_no=? ORDER BY train_date DESC, seat_type", (train_no,))


@app.delete("/api/cache")
def clear_cache(before_date: str | None = None, include_raw: bool = False):
    if before_date:
        _check_date(before_date)
    return {"deleted": db.clear_cache(before_date or None, include_raw)}


# ---------- 换乘搜索 ----------
def _validate_search_params(date: str, max_legs: int, cross_extra: int,
                            frontier_k: int | None, prune_level: str,
                            max_total_min: int | None, detour_ratio: float | None):
    _check_date(date)
    if max_legs < 1 or max_legs > 6:
        raise HTTPException(400, "max_legs 必须在 1-6 之间")
    if cross_extra < 0 or cross_extra > 360:
        raise HTTPException(400, "cross_extra 必须在 0-360 之间")
    if frontier_k is not None and (frontier_k < 1 or frontier_k > 32):
        raise HTTPException(400, "frontier_k 必须在 1-32 之间")
    if prune_level not in ("strict", "balanced", "relaxed", "custom"):
        raise HTTPException(400, "prune_level 必须为 strict / balanced / relaxed / custom")
    if max_total_min is not None and (max_total_min < 60 or max_total_min > 10080):
        raise HTTPException(400, "max_total_min 必须在 60-10080 分钟之间")
    if detour_ratio is not None and (detour_ratio < 0 or detour_ratio > 20):
        raise HTTPException(400, "detour_ratio 必须在 0-20 之间")


@app.get("/api/transfers")
def transfers(from_station: str, to_station: str, date: str,
              min_wait: int = 30, max_wait: int = 360, limit: int = 0,
              max_legs: int = 2, expand_city: bool = True,
              cross_extra: int = 60, frontier_k: int | None = None,
              prune_level: str = "balanced",
              max_total_min: int | None = None,
              detour_ratio: float | None = None,
              prune_dominated: bool | None = None,
              allow_cross_station: bool = True,
              allow_prefixes: str = "",
              exclude_prefixes: str = "",
              include_stations: str = "",
              exclude_stations: str = ""):
    _validate_search_params(date, max_legs, cross_extra, frontier_k, prune_level, max_total_min, detour_ratio)
    return _run(
        transfer.search, from_station, to_station, date,
        min_wait, max_wait, limit, max_legs, expand_city, cross_extra, frontier_k,
        prune_level, max_total_min, detour_ratio, prune_dominated,
        allow_cross_station, allow_prefixes, exclude_prefixes,
        include_stations, exclude_stations,
    )


class TransferSearchReq(BaseModel):
    from_station: str
    to_station: str
    date: str
    min_wait: int = 30
    max_wait: int = 360
    limit: int = 0
    max_legs: int = 2
    expand_city: bool = True
    cross_extra: int = 60
    frontier_k: int | None = None
    prune_level: str = "balanced"
    max_total_min: int | None = None
    detour_ratio: float | None = None
    prune_dominated: bool | None = None
    allow_cross_station: bool = True
    allow_prefixes: str = ""
    exclude_prefixes: str = ""
    include_stations: str = ""
    exclude_stations: str = ""


def _transfer_search_job(job_id: str, ev: threading.Event, body: TransferSearchReq):
    def on_prog(done: int, total: int, cur: str):
        _job_update(job_id, done=done, total=total, sub_done=0, sub_total=0, current=cur)

    def on_sub(s_done: int, s_total: int, cur: str):
        _job_update(job_id, sub_done=s_done, sub_total=s_total, current=cur)

    try:
        res = transfer.search(
            body.from_station, body.to_station, body.date,
            min_wait=body.min_wait, max_wait=body.max_wait, limit=body.limit,
            max_legs=body.max_legs, expand_city=body.expand_city,
            cross_extra=body.cross_extra, frontier_k=body.frontier_k,
            prune_level=body.prune_level,
            max_total_min=body.max_total_min,
            detour_ratio=body.detour_ratio,
            prune_dominated=body.prune_dominated,
            allow_cross_station=body.allow_cross_station,
            allow_prefixes=body.allow_prefixes,
            exclude_prefixes=body.exclude_prefixes,
            include_stations=body.include_stations,
            exclude_stations=body.exclude_stations,
            progress_cb=on_prog, cancel_event=ev, sub_cb=on_sub,
        )
        if ev.is_set():
            _job_update(job_id, status="cancelled")
        else:
            _job_update(job_id, status="done", phase="done", sub_done=0, sub_total=0, result=res)
    except transfer.SearchCancelled:
        _job_update(job_id, status="cancelled")
    except Exception as e:
        _job_update(job_id, status="error", error=str(e))
    finally:
        db.close_conn()


@app.post("/api/transfers/start")
def start_transfers(body: TransferSearchReq):
    """启动可实时查询进度与中途打断的换乘搜索任务。"""
    _validate_search_params(
        body.date, body.max_legs, body.cross_extra, body.frontier_k,
        body.prune_level, body.max_total_min, body.detour_ratio,
    )
    fstations = db.resolve_stations(body.from_station, body.expand_city)
    tstations = db.resolve_stations(body.to_station, body.expand_city)
    if not fstations or not tstations:
        missing = body.from_station if not fstations else body.to_station
        raise HTTPException(400, f"车站/城市“{missing}”不存在，请先在「爬取数据」页更新车站站名表")
    total_steps = 6 if body.max_legs < 3 else 5 + (body.max_legs - 1)
    job_id, ev = _create_job({
        "status": "running", "phase": "search", "done": 0, "total": total_steps,
        "sub_done": 0, "sub_total": 0,
        "current": "正在准备换乘搜索...", "errors": [], "result": {},
    })
    threading.Thread(target=_transfer_search_job, args=(job_id, ev, body), daemon=True).start()
    return {"job_id": job_id}


# ---------- 方案票价/余票实时查询 ----------
class PlanLeg(BaseModel):
    train_no: str
    train: str | None = None
    date: str
    from_telecode: str | None = None
    to_telecode: str | None = None
    from_name: str = ""
    to_name: str = ""


class PlanTicketsReq(BaseModel):
    legs: list[PlanLeg]


def _leg_ident(l: PlanLeg) -> tuple:
    return (l.train_no, l.from_telecode or l.from_name, l.to_telecode or l.to_name, l.date)


@app.post("/api/plan_tickets")
def plan_tickets(body: PlanTicketsReq):
    legs, seen = [], set()
    for l in body.legs:
        _check_date(l.date)
        fs = db.resolve_station(l.from_telecode) if l.from_telecode else db.resolve_station(l.from_name)
        ts = db.resolve_station(l.to_telecode) if l.to_telecode else db.resolve_station(l.to_name)
        f = l.from_telecode or (fs or {}).get("telecode")
        t = l.to_telecode or (ts or {}).get("telecode")
        if not f or not t:
            raise HTTPException(400, f"无法识别车站：{l.from_name or l.from_telecode} / {l.to_name or l.to_telecode}")
        key = _leg_ident(l)
        if key in seen:
            continue
        seen.add(key)
        legs.append({
            "key": "|".join(key), "train_no": l.train_no, "train": l.train or "",
            "from": f, "to": t,
            "from_name": l.from_name or (fs or {}).get("name") or "",
            "to_name": l.to_name or (ts or {}).get("name") or "",
            "date": l.date,
        })
    if not legs:
        raise HTTPException(400, "没有可查询的行程段")
    job_id, ev = _create_job({"status": "running", "phase": "tickets", "done": 0, "total": 0,
                               "current": "正在准备查询余票...", "errors": [], "result": {}})
    threading.Thread(target=_plan_tickets_job, args=(job_id, ev, legs), daemon=True).start()
    return {"job_id": job_id, "legs": len(legs)}


def _plan_tickets_job(job_id: str, ev: threading.Event, legs: list[dict]):
    crawler.set_cancel_event(ev)
    try:
        today = db.cn_today().isoformat()
        tc_name = _station_name_map({l["from"] for l in legs} | {l["to"] for l in legs})
        for l in legs:
            if l.get("from_name"):
                tc_name.setdefault(l["from"], l["from_name"])
            if l.get("to_name"):
                tc_name.setdefault(l["to"], l["to_name"])
        train_codes = _train_code_map({l["train_no"] for l in legs})
        for l in legs:
            if l.get("train"):
                train_codes.setdefault(l["train_no"], l["train"])

        pairs = list(dict.fromkeys((l["from"], l["to"], l["date"]) for l in legs))
        sno = transfer._load_sno()
        done_pairs: set[tuple] = set()
        failed_pairs: dict[tuple, str] = {}

        def build_out(tk_map: dict, allow_missing_error: bool) -> dict:
            out = {}
            for l in legs:
                pair = (l["from"], l["to"], l["date"])
                res = {}
                r = tk_map.get((l["train_no"], l["from"], l["to"], l["date"]))
                if r:
                    res["seats"] = json.loads(r["seats"] or "{}")
                    res["can_web_buy"] = r["can_web_buy"]
                    ts = r.get("fetched_at") or db.cn_now().isoformat(timespec="seconds")
                    res["ticket_updated"] = ts.replace("T", " ")[:16]
                elif allow_missing_error and pair in failed_pairs:
                    res["error"] = f"余票请求失败：{failed_pairs[pair]}"
                elif allow_missing_error and pair in done_pairs:
                    res["error"] = "当日该区间余票查询中无此车次"
                fno = l.get("fno") or (r["from_station_no"] if r else None) or sno.get((l["train_no"], l["from"]))
                tno = l.get("tno") or (r["to_station_no"] if r else None) or sno.get((l["train_no"], l["to"]))
                if fno and tno:
                    fno_s, tno_s = str(fno).zfill(2), str(tno).zfill(2)
                    pr = db.rows("""SELECT seat_type,seat_name,price,train_date FROM prices
                                    WHERE train_no=? AND from_station_no=? AND to_station_no=? AND train_date=?
                                    ORDER BY price""",
                                 (l["train_no"], fno_s, tno_s, l["date"]))
                    if not pr:
                        pr = db.rows("""SELECT seat_type,seat_name,price,train_date FROM prices
                                        WHERE train_no=? AND from_station_no=? AND to_station_no=?
                                        AND train_date=(SELECT MAX(train_date) FROM prices WHERE train_no=? AND from_station_no=? AND to_station_no=?)
                                        ORDER BY price""",
                                     (l["train_no"], fno_s, tno_s, l["train_no"], fno_s, tno_s))
                    if pr:
                        res["prices"] = [{"seat_type": x["seat_type"], "seat": x["seat_name"], "price": x["price"]} for x in pr]
                        if pr[0]["train_date"]:
                            res["price_date"] = pr[0]["train_date"]
                            if pr[0]["train_date"] != l["date"]:
                                res["price_stale"] = True
                if res:
                    out[l["key"]] = res
            return out

        def load_tk(target_pairs) -> dict:
            tk_map = {}
            for f, t, d in target_pairs:
                for r in db.rows("""SELECT t.*, r.fetched_at FROM tickets t
                                    LEFT JOIN raw_payloads r ON r.id=t.raw_id
                                    WHERE t.from_telecode=? AND t.to_telecode=? AND t.train_date=?""",
                                 (f, t, d)):
                    tk_map[(r["train_no"], f, t, d)] = r
                    if r["station_train_code"]:
                        train_codes.setdefault(r["train_no"], r["station_train_code"])
            return tk_map

        _job_update(job_id, phase="tickets", total=len(pairs))
        for f, t, d in pairs:
            if ev.is_set():
                _job_update(job_id, result=build_out(load_tk(done_pairs), False), status="cancelled")
                return
            _job_update(job_id, current=f"余票 {_fmt_station(tc_name.get(f), f)}→{_fmt_station(tc_name.get(t), t)} ({d})")
            # 已过去的日期不再实时查询，仅用已入库缓存
            ok = True
            if d >= today:
                try:
                    crawler.crawl_tickets(f, t, d)
                except CrawlCancelled:
                    _job_update(job_id, result=build_out(load_tk(done_pairs), False), status="cancelled")
                    return
                except Exception as e:
                    ok = False
                    failed_pairs[(f, t, d)] = str(e)
                    _job_append(job_id, "errors",
                                f"余票 {_fmt_station(tc_name.get(f), f)}→{_fmt_station(tc_name.get(t), t)} {d}: {e}")
            if ok:
                done_pairs.add((f, t, d))
            _job_inc(job_id, "done")

        tk = load_tk(pairs)
        need = {}
        for l in legs:
            r = tk.get((l["train_no"], l["from"], l["to"], l["date"]))
            fno = (r["from_station_no"] if r else None) or sno.get((l["train_no"], l["from"]))
            tno = (r["to_station_no"] if r else None) or sno.get((l["train_no"], l["to"]))
            l["fno"], l["tno"] = fno, tno
            if not fno or not tno or l["date"] < today:
                continue
            fno, tno = str(fno).zfill(2), str(tno).zfill(2)
            if db.rows("SELECT 1 FROM prices WHERE train_no=? AND from_station_no=? AND to_station_no=? AND train_date=? LIMIT 1",
                       (l["train_no"], fno, tno, l["date"])):
                continue
            need.setdefault((l["train_no"], fno, tno, l["date"]), (r["seat_types"] if r else "") or "")

        # 余票阶段结束先回传一次结果，界面可立即展示席别余票与内嵌票价
        _job_update(job_id, result=build_out(tk, True))

        with _jobs_lock:
            cur_total = jobs[job_id]["total"]
        _job_update(job_id, phase="price", total=cur_total + len(need))
        for (tn, fno, tno, d), st in need.items():
            if ev.is_set():
                _job_update(job_id, result=build_out(tk, True), status="cancelled")
                return
            t_label = _fmt_train(train_codes.get(tn), tn)
            _job_update(job_id, current=f"票价 {t_label} {fno}→{tno}")
            try:
                crawler.crawl_price(tn, fno, tno, st, d)
            except CrawlCancelled:
                _job_update(job_id, result=build_out(tk, True), status="cancelled")
                return
            except Exception as e:
                _job_append(job_id, "errors", f"票价 {t_label} {fno}→{tno}: {e}")
            _job_inc(job_id, "done")
            _job_update(job_id, result=build_out(tk, True))

        _job_update(job_id, result=build_out(tk, True), phase="done", status="done")
    finally:
        crawler.set_cancel_event(None)
        db.close_conn()


app.mount("/static", StaticFiles(directory=STATIC), name="static")
