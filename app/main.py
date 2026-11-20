import json
import threading
import uuid
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import db, importer, transfer
from .crawler import CrawlError, crawler, station_timetable_context

app = FastAPI(title="12306 智能中转")
STATIC = Path(__file__).resolve().parent.parent / "static"

jobs: dict[str, dict] = {}


@app.on_event("startup")
def startup():
    db.init_db()


def _station_or_400(q: str) -> dict:
    s = db.resolve_station(q)
    if not s:
        raise HTTPException(400, f"车站“{q}”不存在，请先更新车站站名表")
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
    ctx = body.model_dump(exclude={"payload", "api_type"})
    return _run(importer.ingest, body.payload, body.api_type, ctx, "import")


@app.post("/api/import/file")
async def do_import_file(file: UploadFile, api_type: str | None = None,
                         train_no: str | None = None, date: str | None = None,
                         from_station_no: str | None = None, to_station_no: str | None = None):
    text = (await file.read()).decode("utf-8-sig", errors="replace")
    ctx = {"train_no": train_no, "date": date,
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
    s = _station_or_400(body.station)
    return _run(crawler.crawl_station_trains, s["telecode"], body.date)


class TimetableReq(BaseModel):
    train_no: str
    date: str | None = None
    from_telecode: str | None = None
    to_telecode: str | None = None


@app.post("/api/crawl/timetable")
def crawl_timetable(body: TimetableReq):
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
    f, t = _station_or_400(body.from_station), _station_or_400(body.to_station)
    result = _run(crawler.crawl_tickets, f["telecode"], t["telecode"], body.date)
    if body.with_prices:
        job_id = uuid.uuid4().hex[:8]
        jobs[job_id] = {"status": "running", "done": 0, "total": 0, "errors": []}
        threading.Thread(target=_price_job, args=(job_id, f["telecode"], t["telecode"], body.date),
                         daemon=True).start()
        result["price_job"] = job_id
    return result


def _price_job(job_id, f_tc, t_tc, date):
    tk = db.rows("""SELECT train_no,from_station_no,to_station_no,seat_types FROM tickets
                    WHERE from_telecode=? AND to_telecode=? AND train_date=?""", (f_tc, t_tc, date))
    jobs[job_id]["total"] = len(tk)
    for r in tk:
        try:
            crawler.crawl_price(r["train_no"], r["from_station_no"], r["to_station_no"],
                                r["seat_types"] or "", date)
        except Exception as e:
            jobs[job_id]["errors"].append(f"{r['train_no']}: {e}")
        jobs[job_id]["done"] += 1
    jobs[job_id]["status"] = "done"


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "任务不存在")
    return jobs[job_id]


class PriceReq(BaseModel):
    train_no: str
    from_station_no: str
    to_station_no: str
    seat_types: str = ""
    date: str


@app.post("/api/crawl/price")
def crawl_price(body: PriceReq):
    return _run(crawler.crawl_price, body.train_no, body.from_station_no,
                body.to_station_no, body.seat_types, body.date)


# ---------- 一键导入向导 / 在线模式共用 ----------
MAX_TIMETABLES = 200   # 单次向导最多抓取的车次时刻表数（大站车次很多时封顶）


class RouteImportReq(BaseModel):
    from_city: str
    to_city: str
    date: str
    deep: bool = True


def _route_job(job_id: str, from_q: str, to_q: str, date: str, deep: bool):
    job = jobs[job_id]

    def run(txt, fn, *args):
        job["current"] = txt
        try:
            fn(*args)
        except Exception as e:
            job["errors"].append(f"{txt}: {e}")
        job["done"] += 1
        job["log"].append(txt)

    try:
        fstations = db.resolve_stations(from_q, True)
        tstations = db.resolve_stations(to_q, True)
        if not fstations or not tstations:
            job["status"] = "error"
            job["error"] = "车站/城市不存在，请先在“爬取数据”页更新车站站名表"
            return
        ftcs = [s["telecode"] for s in fstations]
        ttcs = [s["telecode"] for s in tstations]
        all_tcs = list(dict.fromkeys(ftcs + ttcs))

        job["phase"], job["total"] = "screen", len(all_tcs)
        for tc in all_tcs:
            run(f"车站大屏 {tc}", crawler.crawl_station_trains, tc, date)

        if deep:
            ph = ",".join("?" * len(all_tcs))
            rows = db.rows(
                f"""SELECT DISTINCT train_no,start_station_telecode,end_station_telecode,start_train_date
                    FROM station_trains WHERE station_telecode IN ({ph}) AND station_train_date=?""",
                (*all_tcs, date))
            uniq = {}
            for r in rows:
                if r["train_no"] and r["train_no"] not in uniq:
                    uniq[r["train_no"]] = r
            items = list(uniq.values())[:MAX_TIMETABLES]
            job["phase"], job["total"] = "timetable", job["total"] + len(items)
            for r in items:
                tn = r["train_no"]
                f = r["start_station_telecode"]
                t = r["end_station_telecode"]
                d = r["start_train_date"] or date
                if not f or not t:
                    ctx = station_timetable_context(tn) or {}
                    f, t, d = ctx.get("from"), ctx.get("to"), ctx.get("date") or date
                if not f or not t:
                    run(f"时刻表 {tn} 跳过（缺始发/终到）", lambda: None)
                    continue
                run(f"时刻表 {tn}", crawler.crawl_timetable, tn, d, f, t)

        pairs = [(f, t) for f in ftcs for t in ttcs if f != t]
        job["phase"], job["total"] = "tickets", job["total"] + len(pairs)
        for f, t in pairs:
            run(f"余票 {f}→{t}", crawler.crawl_tickets, f, t, date)

        if deep:
            phf, pht = ",".join("?" * len(ftcs)), ",".join("?" * len(ttcs))
            tk = db.rows(
                f"""SELECT train_no,from_station_no,to_station_no,seat_types FROM tickets
                    WHERE from_telecode IN ({phf}) AND to_telecode IN ({pht}) AND train_date=?""",
                (*ftcs, *ttcs, date))
            job["phase"], job["total"] = "price", job["total"] + len(tk)
            for r in tk:
                run(f"票价 {r['train_no']}",
                    crawler.crawl_price, r["train_no"], r["from_station_no"] or "",
                    r["to_station_no"] or "", r["seat_types"] or "", date)

        job["phase"], job["status"] = "done", "done"
    except Exception as e:
        job["status"], job["error"] = "error", str(e)


@app.post("/api/import/route")
def import_route(body: RouteImportReq):
    fstations = db.resolve_stations(body.from_city, True)
    tstations = db.resolve_stations(body.to_city, True)
    if not fstations or not tstations:
        raise HTTPException(400, "车站/城市不存在，请先在“爬取数据”页更新车站站名表")
    job_id = uuid.uuid4().hex[:8]
    jobs[job_id] = {"status": "running", "done": 0, "total": 0, "phase": "init",
                    "current": "", "log": [], "errors": [], "result": {}}
    threading.Thread(target=_route_job, args=(job_id, body.from_city, body.to_city,
                                              body.date, body.deep), daemon=True).start()
    return {"job_id": job_id,
            "from_stations": [s["name"] for s in fstations],
            "to_stations": [s["name"] for s in tstations],
            "date": body.date, "deep": body.deep}


# ---------- 浏览 ----------
@app.get("/api/station_trains")
def station_trains(station: str, date: str | None = None):
    s = _station_or_400(station)
    if date:
        return db.rows("""SELECT * FROM station_trains WHERE station_telecode=? AND station_train_date=?
                          ORDER BY start_time""", (s["telecode"], date))
    return db.rows("""SELECT * FROM station_trains WHERE station_telecode=?
                      ORDER BY station_train_date, start_time LIMIT 500""", (s["telecode"],))


@app.get("/api/timetable")
def timetable(train_no: str):
    r = db.rows("SELECT DISTINCT train_date FROM train_stops WHERE train_no=? ORDER BY train_date DESC LIMIT 1",
                (train_no,))
    if r:
        stops = db.rows("""SELECT * FROM train_stops WHERE train_no=? AND train_date=? ORDER BY station_no""",
                        (train_no, r[0]["train_date"]))
        return {"stops": stops, "complete": True, "source": "train_stops"}
    # 退回车站大屏：把已抓取各站的经停拼成（可能不完整的）时刻表
    scr = db.rows("""SELECT station_no, station_name, station_telecode, arrive_time, start_time,
                            stopover_time, station_train_date
                     FROM station_trains WHERE train_no=? AND station_no IS NOT NULL
                     ORDER BY station_no, station_train_date DESC""", (train_no,))
    seen, stops = set(), []
    for s in scr:
        if s["station_no"] in seen:
            continue
        seen.add(s["station_no"])
        stops.append({"station_no": s["station_no"], "station_name": s["station_name"],
                      "telecode": s["station_telecode"], "arrive_time": s["arrive_time"],
                      "start_time": s["start_time"], "stopover_time": s["stopover_time"], "enabled": 1})
    if stops:
        return {"stops": stops, "complete": False, "source": "station_screen"}
    raise HTTPException(404, "该车次暂无时刻表数据")


@app.get("/api/trains")
def trains(q: str = "", limit: int = 50):
    like = f"%{q}%"
    return db.rows("""SELECT train_no, station_train_code, MAX(train_date) train_date,
                             COUNT(*) stop_count FROM train_stops
                      WHERE station_train_code LIKE ? OR train_no LIKE ?
                      GROUP BY train_no LIMIT ?""", (like, like, limit))


@app.get("/api/tickets")
def tickets(from_station: str, to_station: str, date: str):
    f, t = _station_or_400(from_station), _station_or_400(to_station)
    return db.rows("""SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?
                      ORDER BY start_time""", (f["telecode"], t["telecode"], date))


@app.get("/api/prices")
def prices(train_no: str):
    return db.rows("SELECT * FROM prices WHERE train_no=? ORDER BY train_date DESC, seat_type", (train_no,))


# ---------- 换乘搜索 ----------
@app.get("/api/transfers")
def transfers(from_station: str, to_station: str, date: str,
              min_wait: int = 30, max_wait: int = 360, limit: int = 0,
              max_legs: int = 2, expand_city: bool = True):
    if max_legs < 1 or max_legs > 6:
        raise HTTPException(400, "max_legs 必须在 1-6 之间")
    return _run(transfer.search, from_station, to_station, date,
                min_wait, max_wait, limit, max_legs, expand_city)


# ---------- 方案票价/余票实时查询 ----------
class PlanLeg(BaseModel):
    train_no: str
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
        f = l.from_telecode or (db.resolve_station(l.from_name) or {}).get("telecode")
        t = l.to_telecode or (db.resolve_station(l.to_name) or {}).get("telecode")
        if not f or not t:
            raise HTTPException(400, f"无法识别车站：{l.from_name or l.from_telecode} / {l.to_name or l.to_telecode}")
        key = _leg_ident(l)
        if key in seen:
            continue
        seen.add(key)
        legs.append({"key": "|".join(key), "train_no": l.train_no, "from": f, "to": t, "date": l.date})
    if not legs:
        raise HTTPException(400, "没有可查询的行程段")
    job_id = uuid.uuid4().hex[:8]
    jobs[job_id] = {"status": "running", "done": 0, "total": 0, "errors": [], "result": {}}
    threading.Thread(target=_plan_tickets_job, args=(job_id, legs), daemon=True).start()
    return {"job_id": job_id, "legs": len(legs)}


def _plan_tickets_job(job_id: str, legs: list[dict]):
    job = jobs[job_id]
    today = date.today().isoformat()
    pairs = list(dict.fromkeys((l["from"], l["to"], l["date"]) for l in legs))
    job["total"] = len(pairs)
    for f, t, d in pairs:
        # 已过去的日期不再实时查询，仅用已入库缓存
        if d >= today:
            try:
                crawler.crawl_tickets(f, t, d)
            except Exception as e:
                job["errors"].append(f"{f}→{t} {d}: {e}")
        job["done"] += 1

    tk = {}
    for f, t, d in pairs:
        for r in db.rows("""SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?""",
                         (f, t, d)):
            tk[(r["train_no"], f, t, d)] = r

    sno = {(r["train_no"], r["telecode"]): r["station_no"]
           for r in db.rows("SELECT train_no,telecode,station_no FROM train_stops WHERE telecode IS NOT NULL")}
    need = {}
    for l in legs:
        r = tk.get((l["train_no"], l["from"], l["to"], l["date"]))
        fno = (r["from_station_no"] if r else None) or sno.get((l["train_no"], l["from"]))
        tno = (r["to_station_no"] if r else None) or sno.get((l["train_no"], l["to"]))
        l["fno"], l["tno"] = fno, tno
        if not fno or not tno or l["date"] < today:
            continue
        fno, tno = str(fno).zfill(2), str(tno).zfill(2)
        if db.rows("SELECT 1 FROM prices WHERE train_no=? AND from_station_no=? AND to_station_no=? LIMIT 1",
                   (l["train_no"], fno, tno)):
            continue
        need.setdefault((l["train_no"], fno, tno, l["date"]), (r["seat_types"] if r else "") or "")
    job["total"] += len(need)
    for (tn, fno, tno, d), st in need.items():
        try:
            crawler.crawl_price(tn, fno, tno, st, d)
        except Exception as e:
            job["errors"].append(f"票价 {tn} {fno}→{tno}: {e}")
        job["done"] += 1

    out = {}
    for l in legs:
        res = {}
        r = tk.get((l["train_no"], l["from"], l["to"], l["date"]))
        if r:
            res["seats"] = json.loads(r["seats"] or "{}")
            res["can_web_buy"] = r["can_web_buy"]
        else:
            res["error"] = "当日该区间余票查询中无此车次"
        if l.get("fno") and l.get("tno"):
            pr = db.rows("""SELECT seat_name,price FROM prices WHERE train_no=? AND from_station_no=?
                            AND to_station_no=? ORDER BY price""",
                         (l["train_no"], str(l["fno"]).zfill(2), str(l["tno"]).zfill(2)))
            if pr:
                res["prices"] = [{"seat": x["seat_name"], "price": x["price"]} for x in pr]
        out[l["key"]] = res
    job["result"] = out
    job["status"] = "done"


app.mount("/static", StaticFiles(directory=STATIC), name="static")