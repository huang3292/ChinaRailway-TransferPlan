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


from . import db, geo, importer, transfer
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
    now = time.time()
    initial = dict(
        initial,
        created_at=now,
        finished_at=None,
        phase_started_at=now,
        phase_done_base=int(initial.get("done") or 0),
        sub_started_at=None,
        sub_key=None,
    )
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
    now = time.time()
    with _jobs_lock:
        j = jobs.get(job_id)
        if not j:
            return
        # 切换 phase 时重置阶段计时基准，避免“前一轻量阶段耗时”污染后续重量级请求预估
        if "phase" in kwargs and kwargs["phase"] != j.get("phase"):
            j["phase_started_at"] = now
            j["phase_done_base"] = int(kwargs.get("done", j.get("done") or 0))
        j.update(kwargs)
        st = int(j.get("sub_total") or 0)
        if st > 0:
            sk = (int(j.get("done") or 0), st)
            # 每个子阶段(如 BFS 第 2 程 / 第 3 程)独立计时，不拿前 4 步缓存初始化耗时去外推重循环
            if sk != j.get("sub_key"):
                j["sub_key"] = sk
                j["sub_started_at"] = now
        else:
            j["sub_key"] = None
            j["sub_started_at"] = None
        if j.get("status") in ("done", "cancelled", "error") and not j.get("finished_at"):
            j["finished_at"] = now


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


_TRAIN_DIGITS_RE = re.compile(r"\d+")


def _train_digits(code: str | None) -> str:
    """取车次名中的数字部分（去掉字母头）。"""
    m = _TRAIN_DIGITS_RE.search((code or "").upper())
    return m.group(0) if m else ""


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


_CITY_MODES = ("single", "name", "string", "mixed", "all")


def _query_cities(q: str, limit: int = 8) -> list[dict]:
    if q:
        like, pref = f"%{q}%", f"{q}%"
        cities = db.rows(
            """SELECT city AS name, COUNT(*) AS station_count
               FROM stations
               WHERE city <> '' AND (
                   city LIKE ?
                   OR ((name = city OR name = city || '市' OR name LIKE city || '%')
                       AND (pinyin LIKE ? OR lower(short)=lower(?) OR lower(short) LIKE lower(?)))
               )
               GROUP BY city
               ORDER BY CASE WHEN city=? THEN 0
                             WHEN city LIKE ? THEN 1
                             WHEN MAX(CASE WHEN (name=city OR name=city||'市')
                                  AND (lower(short)=lower(?) OR lower(pinyin)=lower(?)) THEN 1 ELSE 0 END)=1 THEN 2
                             WHEN MAX(CASE WHEN name LIKE city||'%' AND lower(short)=lower(?) THEN 1 ELSE 0 END)=1 THEN 3
                             ELSE 4 END,
                        station_count DESC,
                        city
               LIMIT ?""",
            (like, pref, q, pref, q, pref, q, q, q, limit),
        )
    else:
        cities = db.rows(
            """SELECT city AS name, COUNT(*) AS station_count
               FROM stations WHERE city <> ''
               GROUP BY city ORDER BY station_count DESC, city LIMIT ?""",
            (limit,),
        )
    for c in cities:
        c["kind"] = "city"
        c["telecode"] = ""
    return cities


@app.get("/api/stations")
def stations(q: str = "", limit: int = 30, include_cities: bool = False, only_cities: bool = False):
    q = (q or "").strip()
    if only_cities:
        return _query_cities(q, min(limit, 15))
    if q:
        like = f"%{q}%"
        pref = f"{q}%"
        qu = q.upper()
        rows = db.rows(
            """SELECT * FROM stations WHERE name LIKE ? OR telecode LIKE ? OR pinyin LIKE ?
               OR short LIKE ?
               ORDER BY CASE WHEN name=? OR telecode=? THEN 0
                             WHEN name LIKE ? OR pinyin LIKE ? OR lower(short)=lower(?) THEN 1
                             WHEN lower(short) LIKE lower(?) THEN 2
                             ELSE 3 END, name
               LIMIT ?""",
            (like, like, like, like, q, qu, pref, pref, q, pref, limit),
        )
        if include_cities:
            cities = _query_cities(q, 8)
            return cities + rows[:max(limit - len(cities), 0)]
        return rows
    return db.rows("SELECT * FROM stations ORDER BY name LIMIT ?", (limit,))


@app.get("/api/stations/resolve")
def resolve_stations_preview(q: str = "", city_mode: str = "name", expand_city: bool = True):
    if city_mode not in _CITY_MODES:
        raise HTTPException(400, "city_mode 必须为 single / name / string / mixed / all")
    q_s = (q or "").strip()
    if not q_s:
        has_st = bool(db.rows("SELECT 1 FROM stations LIMIT 1"))
        return {
            "ok": False,
            "mode": city_mode,
            "count": 0,
            "stations": [],
            "empty_db": not has_st,
            "error": "" if has_st else "站名表为空，请先在「爬取数据」页更新车站站名表",
        }
    sts = db.resolve_stations(q_s, expand_city, city_mode)
    return {
        "ok": bool(sts),
        "mode": city_mode,
        "count": len(sts),
        "stations": [{"name": s["name"], "telecode": s["telecode"], "city": s.get("city", "")} for s in sts],
        "empty_db": False,
        "error": "" if sts else db.station_resolve_error(q_s, city_mode),
    }


@app.get("/api/hubs")
def hubs():
    return {"hubs": list(transfer.MAJOR_HUBS), "text": ",".join(transfer.MAJOR_HUBS)}


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


_JOB_INTERNAL_KEYS = {
    "created_at",
    "finished_at",
    "phase_started_at",
    "phase_done_base",
    "sub_started_at",
    "sub_key",
}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    now = time.time()
    with _jobs_lock:
        if job_id not in jobs:
            raise HTTPException(404, "任务不存在")
        j = jobs[job_id]
        snap = dict(j)
        out = {k: (list(v) if isinstance(v, list) else v) for k, v in j.items() if k not in _JOB_INTERNAL_KEYS}
    end_t = snap.get("finished_at") or now
    created_t = snap.get("created_at") or end_t
    out["elapsed_sec"] = round(max(0.0, end_t - created_t), 1)
    if out.get("status") == "running":
        total = int(out.get("total") or 0)
        done = int(out.get("done") or 0)
        sub_total = int(out.get("sub_total") or 0)
        sub_done = int(out.get("sub_done") or 0)
        sub_t0 = snap.get("sub_started_at")
        if sub_total > 0 and 0 < sub_done < sub_total and sub_t0:
            sub_elapsed = max(0.0, now - sub_t0)
            if sub_done >= 5 or sub_elapsed >= 0.8:
                out["sub_eta_sec"] = round(sub_elapsed / sub_done * (sub_total - sub_done), 1)
        # 换乘搜索前几步为毫秒级内存构图，后续 BFS 为秒级重分支，禁止跨步骤线性外推总体剩余时间；
        # 仅同类请求队列（票价/余票/导入阶段）按当前 phase 的单步均速估算剩余时间。
        if out.get("phase") != "search" and total > done:
            phase_base = int(snap.get("phase_done_base") or 0)
            phase_done = done - phase_base
            phase_t0 = snap.get("phase_started_at") or created_t
            phase_elapsed = max(0.0, now - phase_t0)
            if phase_done >= 2 and phase_elapsed >= 0.8:
                out["eta_sec"] = round(phase_elapsed / phase_done * (total - done), 1)
    return out


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
            if not jobs[job_id].get("finished_at"):
                jobs[job_id]["finished_at"] = time.time()
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
    city_mode: str | None = None
    from_city_mode: str | None = None
    to_city_mode: str | None = None


def _route_job(job_id: str, ev: threading.Event, from_q: str, to_q: str, date: str, deep: bool,
               city_mode: str | None = None, from_city_mode: str | None = None,
               to_city_mode: str | None = None):
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
        f_mode = from_city_mode or city_mode
        t_mode = to_city_mode or city_mode
        fstations = db.resolve_stations(from_q, True, f_mode)
        tstations = db.resolve_stations(to_q, True, t_mode)
        if not fstations or not tstations:
            missing = from_q if not fstations else to_q
            miss_mode = f_mode if not fstations else t_mode
            _job_update(job_id, status="error", error=db.station_resolve_error(missing, miss_mode))
            return
        tc_name = {s["telecode"]: s["name"] for s in fstations + tstations}
        ftcs = [s["telecode"] for s in fstations]
        ttcs = [s["telecode"] for s in tstations]
        all_tcs = list(dict.fromkeys(ftcs + ttcs))

        _job_update(job_id, phase="screen", total=len(all_tcs))
        # 大屏当日"无任何经由车次"的车站（停办客运如厦门高崎/漳州东、季节性停运如永定仅 K230 汛期不开），
        # 其站名/电报码进此集合；后续区间余票按 OD 端点剔除它们，省去必然为空的查询。
        # WHY：crawl_station_trains 对空站有两种表征——data 为空列表(parsed==0)，或 data 非列表且
        # errorMsg 含"无数据/暂无/不存在"(抛 CrawlError)；两者都判为空站。而网络/风控类失败不含这些
        # 关键词，保守视为"未知"，保留查询以免偶发失败漏掉真实车次（口径同 crawl_all_timetables.py）。
        no_train_stations: list[str] = []
        for tc in all_tcs:
            label = f"车站大屏 {_fmt_station(tc_name.get(tc), tc)}"
            _job_update(job_id, current=label)
            try:
                if ev.is_set():
                    raise CrawlCancelled("任务已取消")
                if not crawler.crawl_station_trains(tc, date).get("parsed"):
                    no_train_stations.append(tc)
            except CrawlCancelled:
                raise
            except Exception as e:
                if any(k in str(e) for k in ("无数据", "暂无", "不存在")):
                    no_train_stations.append(tc)
                else:
                    _job_append(job_id, "errors", f"{label}: {e}")
            _job_inc(job_id, "done")
            _job_append(job_id, "log", label)

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

        # 区间余票 OD 组合剔除大屏当日无图定车次的车站：这些站发不出/接不到任何车，
        # leftTicket 必返空，跳过即减少无谓请求；抓取失败(非"无数据")的车站不在集合内，仍照常查询（保守不漏）。
        skip_st = set(no_train_stations)
        pairs = [(f, t) for f in ftcs if f not in skip_st
                 for t in ttcs if t not in skip_st if f != t]
        if skip_st:
            _job_append(job_id, "log",
                        "大屏无图定车次，跳过其区间余票：" +
                        "、".join(_fmt_station(tc_name.get(tc), tc) for tc in no_train_stations))
        with _jobs_lock:
            cur_total = jobs[job_id]["total"]
        _job_update(job_id, phase="tickets", total=cur_total + len(pairs))
        for f, t in pairs:
            run(f"余票 {_fmt_station(tc_name.get(f), f)}→{_fmt_station(tc_name.get(t), t)}",
                crawler.crawl_tickets, f, t, date)

        if deep:
            phf, pht = ",".join("?" * len(ftcs)), ",".join("?" * len(ttcs))
            have_prices = {
                (r["train_no"], str(r["from_station_no"] or "").zfill(2), str(r["to_station_no"] or "").zfill(2))
                for r in db.rows(
                    "SELECT train_no,from_station_no,to_station_no FROM prices WHERE train_date=?",
                    (date,),
                )
            }
            tk = [
                r for r in db.rows(
                    f"""SELECT train_no,station_train_code,from_station_no,to_station_no,seat_types
                        FROM tickets
                        WHERE from_telecode IN ({phf}) AND to_telecode IN ({pht}) AND train_date=?""",
                    (*ftcs, *ttcs, date),
                )
                if (
                    r["train_no"],
                    str(r["from_station_no"] or "").zfill(2),
                    str(r["to_station_no"] or "").zfill(2),
                ) not in have_prices
            ]
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
    for m in (body.city_mode, body.from_city_mode, body.to_city_mode):
        if m is not None and m not in _CITY_MODES:
            raise HTTPException(400, "city_mode 必须为 single / name / string / mixed / all")
    f_mode = body.from_city_mode or body.city_mode
    t_mode = body.to_city_mode or body.city_mode
    fstations = db.resolve_stations(body.from_city, True, f_mode)
    tstations = db.resolve_stations(body.to_city, True, t_mode)
    if not fstations or not tstations:
        missing = body.from_city if not fstations else body.to_city
        miss_mode = f_mode if not fstations else t_mode
        raise HTTPException(400, db.station_resolve_error(missing, miss_mode))
    job_id, ev = _create_job({"status": "running", "done": 0, "total": 0, "phase": "init",
                               "current": "", "truncated": 0, "log": [], "errors": [], "result": {}})
    threading.Thread(
        target=_route_job,
        args=(job_id, ev, body.from_city, body.to_city, body.date, body.deep,
              body.city_mode, body.from_city_mode, body.to_city_mode),
        daemon=True,
    ).start()
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
    tk_rows = db.rows(
        """SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?
           ORDER BY start_time""",
        (f["telecode"], t["telecode"], date),
    )
    if not tk_rows:
        return tk_rows
    tns = list({r["train_no"] for r in tk_rows if r.get("train_no")})
    price_buckets: dict[tuple[str, str, str], dict[str, list[dict]]] = {}
    if tns:
        ph = ",".join("?" * len(tns))
        for pr in db.rows(
            f"""SELECT train_no,from_station_no,to_station_no,train_date,seat_type,seat_name,price
                FROM prices WHERE train_no IN ({ph})
                ORDER BY train_date DESC, price ASC""",
            tuple(tns),
        ):
            fno_s = str(pr.get("from_station_no") or "").zfill(2)
            tno_s = str(pr.get("to_station_no") or "").zfill(2)
            key = (pr["train_no"], fno_s, tno_s)
            d_str = pr.get("train_date") or ""
            price_buckets.setdefault(key, {}).setdefault(d_str, []).append({
                "seat_type": pr["seat_type"],
                "seat": pr.get("seat_name") or pr["seat_type"],
                "price": pr["price"],
            })
    for r in tk_rows:
        r["from_name"] = f["name"]
        r["to_name"] = t["name"]
        fno_s = str(r.get("from_station_no") or "").zfill(2)
        tno_s = str(r.get("to_station_no") or "").zfill(2)
        by_date = price_buckets.get((r["train_no"], fno_s, tno_s), {})
        if date in by_date:
            r["prices"] = by_date[date]
            r["price_date"] = date
            r["price_stale"] = False
        elif by_date:
            # 优先最新实测单日快照，与 _plan_tickets_job / transfer._price_map 口径一致
            latest_d = max(by_date)
            r["prices"] = by_date[latest_d]
            r["price_date"] = latest_d
            r["price_stale"] = bool(latest_d)
        else:
            r["prices"] = []
    return tk_rows


@app.get("/api/prices")
def prices(train_no: str):
    return db.rows("SELECT * FROM prices WHERE train_no=? ORDER BY train_date DESC, seat_type", (train_no,))


@app.delete("/api/cache")
def clear_cache(before_date: str | None = None, include_raw: bool = False):
    # WHY: before_date 缺省时 db.clear_cache(None) 会全表删光 station_trains/train_stops/
    # tickets/prices（全国全量重爬需数小时）。HTTP 入口一律要求显式日期；确需全量清空请传
    # 远期日期（如 2100-01-01），或在脚本里直接调用 db.clear_cache(None)。
    if not before_date:
        raise HTTPException(400, "必须显式给出 before_date（YYYY-MM-DD），否则将清空全部已爬数据")
    _check_date(before_date)
    return {"deleted": db.clear_cache(before_date, include_raw)}


# ---------- 换乘搜索 ----------
def _parse_group_ids(raw: str | None) -> list[int] | None:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    if s.lower() in ("none", "-"):
        return []
    out: list[int] = []
    for part in s.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            raise HTTPException(400, f"active_group_ids 格式不合法：“{p}”")
    return out


def _validate_search_params(date: str, max_legs: int, cross_extra: int,
                            frontier_k: int | None, prune_level: str,
                            max_total_min: int | None, detour_ratio: float | None,
                            include_stations_mode: str = "or",
                            city_mode: str | None = None,
                            from_city_mode: str | None = None,
                            to_city_mode: str | None = None,
                            cross_radius_km: float = geo.DEFAULT_CROSS_RADIUS_KM,
                            cross_time_mode: str = "smart",
                            transit_discount_on: bool = True,
                            transit_discount_min: int = geo.DEFAULT_TRANSIT_DISCOUNT_MIN,
                            transit_proximity_m: int = geo.DEFAULT_TRANSIT_PROXIMITY_M,
                            fallback_ungeocoded: bool = False):
    _check_date(date)
    if max_legs < 1 or max_legs > 6:
        raise HTTPException(400, "max_legs 必须在 1-6 之间")
    if cross_extra < 0 or cross_extra > 360:
        raise HTTPException(400, "cross_extra 必须在 0-360 之间")
    if cross_radius_km < 0 or cross_radius_km > geo.MAX_CROSS_RADIUS_KM:
        raise HTTPException(400, f"cross_radius_km 必须在 0-{int(geo.MAX_CROSS_RADIUS_KM)} km 之间")
    if (cross_time_mode or "smart").strip().lower() not in ("smart", "fixed"):
        raise HTTPException(400, "cross_time_mode 必须为 smart 或 fixed")
    if transit_discount_min < 0 or transit_discount_min > 60:
        raise HTTPException(400, "transit_discount_min 必须在 0-60 分钟之间")
    if transit_proximity_m < 200 or transit_proximity_m > 5000:
        raise HTTPException(400, "transit_proximity_m 必须在 200-5000 米之间")
    if cross_radius_km == 0 and fallback_ungeocoded:
        raise HTTPException(400, "半径 R=0（关闭按距离跨站建边）时不允许开启无坐标同城回退")
    if frontier_k is not None and (frontier_k < 0 or frontier_k > 64):
        raise HTTPException(400, "frontier_k 必须在 0-64 之间（0 表示不限束宽）")
    if prune_level not in ("unbounded", "relaxed", "balanced", "strict", "custom"):
        raise HTTPException(400, "prune_level 必须为 unbounded / relaxed / balanced / strict / custom")
    if max_total_min is not None and max_total_min != 0 and (max_total_min < 60 or max_total_min > 20160):
        raise HTTPException(400, "max_total_min 必须为 0（不限）或在 60-20160 分钟之间")
    if detour_ratio is not None and (detour_ratio < 0 or detour_ratio > 20):
        raise HTTPException(400, "detour_ratio 必须在 0-20 之间")
    if (include_stations_mode or "or").strip().lower() not in ("or", "and"):
        raise HTTPException(400, "include_stations_mode 必须为 or 或 and")
    for m in (city_mode, from_city_mode, to_city_mode):
        if m is not None and m not in _CITY_MODES:
            raise HTTPException(400, "city_mode 必须为 single / name / string / mixed / all")


# WHY: HTTP 默认必须带上限——两段换乘枚举不设限（深圳→广州实测 37.4 万条），全量 JSON 可达
# 数百 MB，会把服务端与浏览器一起打死的。要全量请显式 limit=0。
@app.get("/api/transfers")
def transfers(from_station: str, to_station: str, date: str,
              min_wait: int = 30, max_wait: int = 360, limit: int = 1000,
              max_legs: int = 2, expand_city: bool = True,
              cross_extra: int = 60, frontier_k: int | None = None,
              prune_level: str = "balanced",
              max_total_min: int | None = None,
              detour_ratio: float | None = None,
              prune_dominated: bool | None = None,
              allow_cross_station: bool = True,
              hub_only: bool = False,
              hub_stations: str = "",
              allow_prefixes: str = "",
              exclude_prefixes: str = "",
              include_stations: str = "",
              exclude_stations: str = "",
              include_stations_mode: str = "or",
              city_mode: str | None = None,
              from_city_mode: str | None = None,
              to_city_mode: str | None = None,
              cross_radius_km: float = geo.DEFAULT_CROSS_RADIUS_KM,
              cross_time_mode: str = "smart",
              transit_discount_on: bool = True,
              transit_discount_min: int = geo.DEFAULT_TRANSIT_DISCOUNT_MIN,
              transit_proximity_m: int = geo.DEFAULT_TRANSIT_PROXIMITY_M,
              active_group_ids: str | None = None,
              fallback_ungeocoded: bool = False):
    _validate_search_params(
        date, max_legs, cross_extra, frontier_k, prune_level,
        max_total_min, detour_ratio, include_stations_mode,
        city_mode, from_city_mode, to_city_mode,
        cross_radius_km=cross_radius_km,
        cross_time_mode=cross_time_mode,
        transit_discount_on=transit_discount_on,
        transit_discount_min=transit_discount_min,
        transit_proximity_m=transit_proximity_m,
        fallback_ungeocoded=fallback_ungeocoded,
    )
    gids = _parse_group_ids(active_group_ids)
    return _run(
        transfer.search, from_station, to_station, date,
        min_wait, max_wait, limit, max_legs, expand_city, cross_extra, frontier_k,
        prune_level, max_total_min, detour_ratio, prune_dominated,
        allow_cross_station, hub_only, hub_stations,
        allow_prefixes, exclude_prefixes,
        include_stations, exclude_stations, include_stations_mode,
        city_mode, from_city_mode, to_city_mode,
        cross_radius_km, cross_time_mode, transit_discount_on,
        transit_discount_min, transit_proximity_m, gids, fallback_ungeocoded,
    )


class TransferSearchReq(BaseModel):
    from_station: str
    to_station: str
    date: str
    min_wait: int = 30
    max_wait: int = 360
    limit: int = 1000
    max_legs: int = 2
    expand_city: bool = True
    cross_extra: int = 60
    frontier_k: int | None = None
    prune_level: str = "balanced"
    max_total_min: int | None = None
    detour_ratio: float | None = None
    prune_dominated: bool | None = None
    allow_cross_station: bool = True
    hub_only: bool = False
    hub_stations: str = ""
    allow_prefixes: str = ""
    exclude_prefixes: str = ""
    include_stations: str = ""
    exclude_stations: str = ""
    include_stations_mode: str = "or"
    city_mode: str | None = None
    from_city_mode: str | None = None
    to_city_mode: str | None = None
    cross_radius_km: float = geo.DEFAULT_CROSS_RADIUS_KM
    cross_time_mode: str = "smart"
    transit_discount_on: bool = True
    transit_discount_min: int = geo.DEFAULT_TRANSIT_DISCOUNT_MIN
    transit_proximity_m: int = geo.DEFAULT_TRANSIT_PROXIMITY_M
    active_group_ids: list[int] | None = None
    fallback_ungeocoded: bool = False


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
            hub_only=body.hub_only,
            hub_stations=body.hub_stations,
            allow_prefixes=body.allow_prefixes,
            exclude_prefixes=body.exclude_prefixes,
            include_stations=body.include_stations,
            exclude_stations=body.exclude_stations,
            include_stations_mode=body.include_stations_mode,
            city_mode=body.city_mode,
            from_city_mode=body.from_city_mode,
            to_city_mode=body.to_city_mode,
            cross_radius_km=body.cross_radius_km,
            cross_time_mode=body.cross_time_mode,
            transit_discount_on=body.transit_discount_on,
            transit_discount_min=body.transit_discount_min,
            transit_proximity_m=body.transit_proximity_m,
            active_group_ids=body.active_group_ids,
            fallback_ungeocoded=body.fallback_ungeocoded,
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
        body.include_stations_mode, body.city_mode,
        body.from_city_mode, body.to_city_mode,
        cross_radius_km=body.cross_radius_km,
        cross_time_mode=body.cross_time_mode,
        transit_discount_on=body.transit_discount_on,
        transit_discount_min=body.transit_discount_min,
        transit_proximity_m=body.transit_proximity_m,
        fallback_ungeocoded=body.fallback_ungeocoded,
    )
    f_mode = body.from_city_mode or body.city_mode
    t_mode = body.to_city_mode or body.city_mode
    fstations = db.resolve_stations(body.from_station, body.expand_city, f_mode)
    tstations = db.resolve_stations(body.to_station, body.expand_city, t_mode)
    if not fstations or not tstations:
        missing = body.from_station if not fstations else body.to_station
        miss_mode = f_mode if not fstations else t_mode
        raise HTTPException(400, db.station_resolve_error(missing, miss_mode))
    total_steps = 6 if body.max_legs < 3 else 5 + (body.max_legs - 1)
    job_id, ev = _create_job({
        "status": "running", "phase": "search", "done": 0, "total": total_steps,
        "sub_done": 0, "sub_total": 0,
        "current": "正在准备换乘搜索...", "errors": [], "result": {},
    })
    threading.Thread(target=_transfer_search_job, args=(job_id, ev, body), daemon=True).start()
    return {"job_id": job_id}


# ---------- 地理坐标、重轨邻近与换乘规则管理（方案 C v2） ----------
@app.get("/api/geo/status")
def geo_status():
    st = geo.geo_coverage_status()
    st["max_radius_km"] = geo.MAX_CROSS_RADIUS_KM
    st["default_radius_km"] = geo.DEFAULT_CROSS_RADIUS_KM
    st["default_proximity_m"] = geo.DEFAULT_TRANSIT_PROXIMITY_M
    st["default_discount_min"] = geo.DEFAULT_TRANSIT_DISCOUNT_MIN
    return st


@app.get("/api/geo/stations")
def geo_stations(status: str = "suspect", q: str = "", limit: int = 80):
    st_f = (status or "all").strip().lower()
    if st_f not in ("all", "missing", "suspect", "ok", "verified"):
        raise HTTPException(400, "status 必须为 all / missing / suspect / ok / verified")
    lim = max(1, min(500, int(limit)))
    clauses = []
    params: list = []
    if st_f == "missing":
        clauses.append("(lat IS NULL OR lon IS NULL OR coord_status='missing')")
    elif st_f != "all":
        clauses.append("coord_status=?")
        params.append(st_f)
    q_s = (q or "").strip()
    if q_s:
        like = f"%{q_s}%"
        clauses.append("(name LIKE ? OR telecode LIKE ? OR city LIKE ? OR pinyin LIKE ?)")
        params.extend([like, like, like, like])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    total = db.rows(f"SELECT COUNT(*) c FROM stations {where}", tuple(params))[0]["c"]
    items = db.rows(
        f"""SELECT telecode, name, city, lat, lon, coord_source, coord_updated_at,
                   coord_status, coord_confidence, coord_issue,
                   transit_zone_id, nearest_rail_stop_distance_m, rail_access_mode, is_major_hub
            FROM stations {where}
            ORDER BY CASE coord_status WHEN 'suspect' THEN 0 WHEN 'missing' THEN 1 WHEN 'ok' THEN 2 ELSE 3 END, name
            LIMIT ?""",
        (*params, lim),
    )
    return {"stations": items, "total": total}


class GeoStationReq(BaseModel):
    lat: float | None = None
    lon: float | None = None
    coord_status: str = "verified"
    transit_zone_id: str | None = None
    nearest_rail_stop_distance_m: int | None = -1
    is_major_hub: bool | None = None
    issue: str = ""


@app.post("/api/geo/station/{telecode}")
def geo_update_station(telecode: str, body: GeoStationReq):
    tc = (telecode or "").strip().upper()
    if body.lat is not None and body.lon is not None and not geo.in_china_bbox(body.lat, body.lon):
        raise HTTPException(400, "经纬度超出中国境内边界，拒绝保存")
    _run(
        db.update_station_coords,
        tc, body.lat, body.lon, "manual", body.coord_status,
        1.0 if body.coord_status == "verified" else None,
        body.issue,
        body.transit_zone_id,
        body.nearest_rail_stop_distance_m,
        None,
        body.is_major_hub,
    )
    if body.nearest_rail_stop_distance_m == -1 and body.lat is not None and body.lon is not None:
        geo.recompute_rail_proximity()
    row = db.rows("SELECT * FROM stations WHERE telecode=?", (tc,))
    return {"station": row[0] if row else None, "status": geo.geo_coverage_status()}


@app.post("/api/geo/nominatim/{telecode}")
def geo_manual_nominatim(telecode: str):
    try:
        return geo.manual_nominatim_lookup(telecode)
    except ValueError as e:
        msg_s = str(e)
        code = 429 if "礼貌间隔" in msg_s else 400
        raise HTTPException(code, msg_s)
    except Exception as e:
        raise HTTPException(502, f"Nominatim 请求失败：{e}")


@app.post("/api/geo/audit")
def geo_audit():
    audit = geo.audit_station_coords()
    prox = geo.recompute_rail_proximity()
    return {"audit": audit, "proximity": prox, "status": geo.geo_coverage_status()}


class GeoCrawlReq(BaseModel):
    trial_tiles: int | None = None
    force: bool = False
    reset_checkpoint: bool = False
    use_overpass: bool = True
    use_wikidata: bool = True
    overpass_delay: float = 3.5


def _geo_crawl_job(job_id: str, ev: threading.Event, body: GeoCrawlReq):
    def on_prog(done_u: int, tot_u: int, cur_msg: str, sub_d: int = 0, sub_t: int = 0):
        _job_update(job_id, done=done_u, total=tot_u, sub_done=sub_d, sub_total=sub_t, current=cur_msg)

    def on_log(line: str):
        _job_append(job_id, "log", line)

    try:
        crawler_geo = geo.GeoCoordCrawler()
        res = crawler_geo.run(
            force=body.force,
            reset_checkpoint=body.reset_checkpoint,
            use_overpass=body.use_overpass,
            use_wikidata=body.use_wikidata,
            trial_tiles=body.trial_tiles,
            overpass_delay=max(1.0, float(body.overpass_delay)),
            cancel_event=ev,
            progress_cb=on_prog,
            log_cb=on_log,
        )
        for err in res.get("errors") or []:
            _job_append(job_id, "errors", err)
        if ev.is_set():
            _job_update(job_id, status="cancelled", result=res)
        else:
            _job_update(job_id, status="done", phase="done", result=res)
    except InterruptedError:
        _job_update(job_id, status="cancelled")
    except Exception as e:
        _job_update(job_id, status="error", error=str(e))
    finally:
        db.close_conn()


@app.post("/api/geo/crawl/start")
def geo_crawl_start(body: GeoCrawlReq):
    if body.trial_tiles is not None and (body.trial_tiles < 1 or body.trial_tiles > len(geo.CHINA_OVERPASS_TILES)):
        raise HTTPException(400, f"trial_tiles 必须在 1-{len(geo.CHINA_OVERPASS_TILES)} 之间")
    tot = (body.trial_tiles or len(geo.CHINA_OVERPASS_TILES)) if body.use_overpass else 0
    if body.use_wikidata and body.trial_tiles is None:
        tot += 1
    job_id, ev = _create_job({
        "status": "running", "phase": "geo", "done": 0, "total": max(1, tot),
        "sub_done": 0, "sub_total": 0, "current": "准备公共地理数据源抓取…",
        "log": [], "errors": [], "result": {},
    })
    threading.Thread(target=_geo_crawl_job, args=(job_id, ev, body), daemon=True).start()
    return {"job_id": job_id}


@app.get("/api/transfer_groups")
def get_transfer_groups():
    return {"groups": db.list_transfer_groups()}


class TransferGroupReq(BaseModel):
    id: int | None = None
    name: str
    members: list[str]
    default_extra_min: int | None = 45
    enabled_by_default: bool = True
    connect_mode: str = "full_mesh"
    hub_telecode: str = ""
    note: str = ""


@app.post("/api/transfer_groups")
def save_transfer_group_api(body: TransferGroupReq):
    grp = _run(
        db.save_transfer_group,
        body.name, body.members, body.default_extra_min, body.note,
        body.enabled_by_default, body.connect_mode, body.hub_telecode, body.id,
    )
    return {"group": grp, "groups": db.list_transfer_groups()}


@app.delete("/api/transfer_groups/{group_id}")
def delete_transfer_group_api(group_id: int):
    ok = db.delete_transfer_group(group_id)
    if not ok:
        raise HTTPException(404, f"换乘组 #{group_id} 不存在")
    return {"ok": True, "groups": db.list_transfer_groups()}


@app.get("/api/transfer_pairs")
def get_transfer_pairs():
    return {"pairs": db.list_transfer_pair_overrides()}


class TransferPairReq(BaseModel):
    a_telecode: str
    b_telecode: str
    mode: str = "allow"
    custom_extra_min: int | None = None
    note: str = ""


@app.post("/api/transfer_pairs")
def save_transfer_pair_api(body: TransferPairReq):
    a_st = db.resolve_station(body.a_telecode)
    b_st = db.resolve_station(body.b_telecode)
    t1 = a_st["telecode"] if a_st else body.a_telecode
    t2 = b_st["telecode"] if b_st else body.b_telecode
    pair = _run(db.save_transfer_pair_override, t1, t2, body.mode, body.custom_extra_min, body.note)
    return {"pair": pair, "pairs": db.list_transfer_pair_overrides()}


@app.delete("/api/transfer_pairs/{pair_id}")
def delete_transfer_pair_api(pair_id: int):
    ok = db.delete_transfer_pair_override(pair_id)
    if not ok:
        raise HTTPException(404, f"站对覆盖规则 #{pair_id} 不存在")
    return {"ok": True, "pairs": db.list_transfer_pair_overrides()}


@app.get("/api/transfer_pairs/preview")
def preview_transfer_pair(a: str, b: str,
                          cross_radius_km: float = geo.DEFAULT_CROSS_RADIUS_KM,
                          cross_time_mode: str = "smart",
                          transit_discount_on: bool = True,
                          transit_discount_min: int = geo.DEFAULT_TRANSIT_DISCOUNT_MIN,
                          transit_proximity_m: int = geo.DEFAULT_TRANSIT_PROXIMITY_M,
                          active_group_ids: str | None = None,
                          fallback_ungeocoded: bool = False):
    sa, sb = _station_or_400(a), _station_or_400(b)
    st_rows = db.rows(
        """SELECT telecode, name, city, lat, lon, coord_status, coord_confidence,
                  transit_zone_id, nearest_rail_stop_distance_m, rail_access_mode, is_major_hub
           FROM stations"""
    )
    idx = geo.StationGeoIndex(st_rows, db.list_transfer_groups(), db.list_transfer_pair_overrides(), geo.load_transit_cities())
    cfg = geo.resolve_cross_cfg(
        cross_extra=60,
        cross_radius_km=cross_radius_km,
        cross_time_mode=cross_time_mode,
        transit_discount_on=transit_discount_on,
        transit_discount_min=transit_discount_min,
        transit_proximity_m=transit_proximity_m,
        active_group_ids=_parse_group_ids(active_group_ids),
        fallback_ungeocoded=fallback_ungeocoded,
        geo_index=idx,
    )
    tc1, tc2 = sa["telecode"], sb["telecode"]
    ev_res = geo.eval_cross_pair(tc1, tc2, idx, cfg)
    c1, c2 = idx.coords.get(tc1), idx.coords.get(tc2)
    raw_dist = geo.haversine_km(c1[0], c1[1], c2[0], c2[1]) if (c1 and c2) else None
    return {
        "a": {"telecode": tc1, "name": sa["name"], "city": sa.get("city", ""), "coord_status": idx.status_of.get(tc1, "missing"), "transit_zone_id": idx.zone_of.get(tc1, ""), "nearest_rail_stop_distance_m": idx.rail_dist_of.get(tc1), "rail_access_mode": idx.access_mode_of.get(tc1, "none"), "is_major_hub": bool(idx.is_hub.get(tc1))},
        "b": {"telecode": tc2, "name": sb["name"], "city": sb.get("city", ""), "coord_status": idx.status_of.get(tc2, "missing"), "transit_zone_id": idx.zone_of.get(tc2, ""), "nearest_rail_stop_distance_m": idx.rail_dist_of.get(tc2), "rail_access_mode": idx.access_mode_of.get(tc2, "none"), "is_major_hub": bool(idx.is_hub.get(tc2))},
        "distance_km": raw_dist,
        "connected": ev_res is not None,
        "eval": ev_res,
    }


@app.get("/api/transit_cities")
def get_transit_cities():
    cities = geo.load_transit_cities()
    return {
        "cities": [{"city": k, "label": v} for k, v in sorted(cities.items())],
        "zones": geo.CROSS_CITY_TRANSIT_ZONES,
    }


class TransitCityReq(BaseModel):
    city: str
    label: str | None = "地铁/市域"
    enabled: bool = True


@app.post("/api/transit_cities")
def save_transit_city_api(body: TransitCityReq):
    if not (body.city or "").strip():
        raise HTTPException(400, "城市名称不能为空")
    cities = geo.save_transit_city_override(body.city, body.label, body.enabled)
    return {"cities": [{"city": k, "label": v} for k, v in sorted(cities.items())]}


@app.post("/api/transit_cities/reset")
def reset_transit_cities_api():
    cities = geo.reset_transit_cities()
    return {"cities": [{"city": k, "label": v} for k, v in sorted(cities.items())]}


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

        def find_tk(maps, l: dict):
            """先用 train_no 精确匹配；miss 时按同区间同日期的车次数字段兜底。

            WHY（根级口径）：2026-11-25 乘车日起国铁把普速车次头改为 V 等（K1068→V1068），
            train_no 同步变化，方案里存的旧头车次号在实时余票里查不到。车次数字部分在
            改头中保持不变；同数字多行时按"最后爬取为准"取 id 最大行（与重爬覆盖原则一致），
            改头生效日前后混存的新旧行因此都收敛到最新口径。
            """
            tk_map, by_code = maps
            r = tk_map.get((l["train_no"], l["from"], l["to"], l["date"]))
            if r:
                return r
            dig = _train_digits(l.get("train") or train_codes.get(l["train_no"], ""))
            if not dig:
                return None
            cands = by_code.get((dig, l["from"], l["to"], l["date"]))
            if not cands:
                return None
            return max(cands, key=lambda x: x["id"])

        def build_out(maps, allow_missing_error: bool) -> dict:
            out = {}
            for l in legs:
                pair = (l["from"], l["to"], l["date"])
                res = {}
                r = find_tk(maps, l)
                if r:
                    res["seats"] = json.loads(r["seats"] or "{}")
                    res["can_web_buy"] = r["can_web_buy"]
                    ts = r.get("fetched_at") or db.cn_now().isoformat(timespec="seconds")
                    res["ticket_updated"] = ts.replace("T", " ")[:16]
                    live_code = r["station_train_code"] or ""
                    plan_code = l.get("train") or train_codes.get(l["train_no"], "")
                    if live_code and plan_code and live_code != plan_code:
                        res["live_train"] = live_code
                elif allow_missing_error and pair in failed_pairs:
                    res["error"] = f"余票请求失败：{failed_pairs[pair]}"
                elif allow_missing_error and pair in done_pairs:
                    res["error"] = "当日该区间余票查询中无此车次"
                fno = l.get("fno") or (r["from_station_no"] if r else None) or sno.get((l["train_no"], l["from"]))
                tno = l.get("tno") or (r["to_station_no"] if r else None) or sno.get((l["train_no"], l["to"]))
                if fno and tno:
                    fno_s, tno_s = str(fno).zfill(2), str(tno).zfill(2)
                    pr = []
                    for pno in dict.fromkeys([x for x in (r["train_no"] if r else None, l["train_no"]) if x]):
                        pr = db.rows("""SELECT seat_type,seat_name,price,train_date FROM prices
                                        WHERE train_no=? AND from_station_no=? AND to_station_no=? AND train_date=?
                                        ORDER BY price""",
                                     (pno, fno_s, tno_s, l["date"]))
                        if not pr:
                            pr = db.rows("""SELECT seat_type,seat_name,price,train_date FROM prices
                                            WHERE train_no=? AND from_station_no=? AND to_station_no=?
                                            AND train_date=(SELECT MAX(train_date) FROM prices WHERE train_no=? AND from_station_no=? AND to_station_no=?)
                                            ORDER BY price""",
                                         (pno, fno_s, tno_s, pno, fno_s, tno_s))
                        if pr:
                            break
                    if pr:
                        res["prices"] = [{"seat_type": x["seat_type"], "seat": x["seat_name"], "price": x["price"]} for x in pr]
                        if pr[0]["train_date"]:
                            res["price_date"] = pr[0]["train_date"]
                            if pr[0]["train_date"] != l["date"]:
                                res["price_stale"] = True
                if res:
                    out[l["key"]] = res
            return out

        def load_tk(target_pairs):
            tk_map, by_code = {}, {}
            for f, t, d in target_pairs:
                for r in db.rows("""SELECT t.*, r.fetched_at FROM tickets t
                                    LEFT JOIN raw_payloads r ON r.id=t.raw_id
                                    WHERE t.from_telecode=? AND t.to_telecode=? AND t.train_date=?""",
                                 (f, t, d)):
                    tk_map[(r["train_no"], f, t, d)] = r
                    if r["station_train_code"]:
                        train_codes.setdefault(r["train_no"], r["station_train_code"])
                        dig = _train_digits(r["station_train_code"])
                        if dig:
                            by_code.setdefault((dig, f, t, d), []).append(r)
            return tk_map, by_code

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

        maps = load_tk(pairs)
        need = {}
        for l in legs:
            r = find_tk(maps, l)
            fno = (r["from_station_no"] if r else None) or sno.get((l["train_no"], l["from"]))
            tno = (r["to_station_no"] if r else None) or sno.get((l["train_no"], l["to"]))
            l["fno"], l["tno"] = fno, tno
            if not fno or not tno or l["date"] < today:
                continue
            fno, tno = str(fno).zfill(2), str(tno).zfill(2)
            pno = r["train_no"] if r else l["train_no"]
            if db.rows("SELECT 1 FROM prices WHERE train_no=? AND from_station_no=? AND to_station_no=? AND train_date=? LIMIT 1",
                       (pno, fno, tno, l["date"])):
                continue
            need.setdefault((pno, fno, tno, l["date"]), (r["seat_types"] if r else "") or "")

        # 余票阶段结束先回传一次结果，界面可立即展示席别余票与内嵌票价
        _job_update(job_id, result=build_out(maps, True))

        with _jobs_lock:
            cur_total = jobs[job_id]["total"]
        _job_update(job_id, phase="price", total=cur_total + len(need))
        for (tn, fno, tno, d), st in need.items():
            if ev.is_set():
                _job_update(job_id, result=build_out(maps, True), status="cancelled")
                return
            t_label = _fmt_train(train_codes.get(tn), tn)
            _job_update(job_id, current=f"票价 {t_label} {fno}→{tno}")
            try:
                crawler.crawl_price(tn, fno, tno, st, d)
            except CrawlCancelled:
                _job_update(job_id, result=build_out(maps, True), status="cancelled")
                return
            except Exception as e:
                _job_append(job_id, "errors", f"票价 {t_label} {fno}→{tno}: {e}")
            _job_inc(job_id, "done")
            _job_update(job_id, result=build_out(maps, True))

        _job_update(job_id, result=build_out(maps, True), phase="done", status="done")
    finally:
        crawler.set_cancel_event(None)
        db.close_conn()


app.mount("/static", StaticFiles(directory=STATIC), name="static")
