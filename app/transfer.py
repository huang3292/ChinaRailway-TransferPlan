"""换乘搜索。

三个数据引擎，结果合并去重：
A. 车站大屏（station_trains）：真实开行日期优先，无当日数据时按每日开行假设平移
B. 车次时刻表（train_stops）：完整经停站，按每日开行假设推算日期
C. 站间余票（tickets）：精确日期的直达补充

同站一次换乘 + 同城异站换乘（额外预留30分钟），校验最小/最大换乘时间，支持跨天。
剔除第二程折返经过出发站/第一程路径的无意义方案。
"""
import json
import math
import re
from datetime import date, datetime, timedelta

from . import db, fare


def _int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _minutes(t: str | None) -> int | None:
    if not t:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", t.strip())
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _dur_min(t: str | None) -> int | None:
    if not t:
        return None
    m = re.fullmatch(r"(?:(\d+)小时)?(?:(\d+)分)?", t.strip())
    if not m or not any(m.groups()):
        return None
    return int(m.group(1) or 0) * 60 + int(m.group(2) or 0)


def _date_min(d: str | None) -> int | None:
    try:
        return date.fromisoformat(d).toordinal() * 1440
    except (ValueError, TypeError):
        return None


def _fmt_epoch(m: int) -> str:
    return (datetime.fromordinal(m // 1440) + timedelta(minutes=m % 1440)).strftime("%Y-%m-%d %H:%M")


def _fmt(base: datetime, minutes: int) -> str:
    return (base + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M")


def _price_map() -> dict:
    out = {}
    for r in db.rows("SELECT train_no,from_station_no,to_station_no,seat_name,price FROM prices"):
        key = (r["train_no"], str(r["from_station_no"]).zfill(2), str(r["to_station_no"]).zfill(2))
        out.setdefault(key, []).append({"seat": r["seat_name"], "price": r["price"]})
    return out


def _seat_map() -> dict:
    out = {}
    for r in db.rows("SELECT train_no,from_telecode,to_telecode,train_date,seats,can_web_buy FROM tickets"):
        out[(r["train_no"], r["from_telecode"], r["to_telecode"], r["train_date"])] = r
    return out


def _enrich(leg: dict, train_no: str, fno, tno, ftc, ttc, dep_date: str,
            prices: dict, seats: dict, code: str = "", km=None,
            duration_min: int | None = None, cal: dict | None = None,
            sno: dict | None = None) -> dict:
    if sno:
        if fno is None and ftc:
            fno = sno.get((train_no, ftc))
        if tno is None and ttc:
            tno = sno.get((train_no, ttc))
    if fno is not None and tno is not None:
        p = prices.get((train_no, str(fno).zfill(2), str(tno).zfill(2)))
        if p:
            leg["prices"] = p
    if "prices" not in leg and code:
        est = fare.estimate(code, km=km, duration_min=duration_min, cal=cal)
        if est:
            leg["price_est"] = est
    tk = seats.get((train_no, ftc, ttc, dep_date))
    if tk:
        leg["seats"] = json.loads(tk["seats"] or "{}")
        leg["can_web_buy"] = tk["can_web_buy"]
    return leg


# ---------- 引擎 A：车站大屏（真实日期优先，无当日数据时按每日开行假设平移） ----------
def _load_screen() -> dict:
    trains = {}
    for r in db.rows("SELECT * FROM station_trains"):
        tn = r["train_no"]
        if not tn:
            continue
        # 同一 train_no 可能对应多个开行日期/往返班次，按开行实例分组
        run = (tn, r["start_train_date"] or r["station_train_date"])
        t = trains.setdefault(run, {"code": r["station_train_code"], "obs": {}})
        t["start_key"] = r["start_station_telecode"] or r["start_station_name"]
        t["end_key"] = r["end_station_telecode"] or r["end_station_name"]
        t["start_name"] = r["start_station_name"]
        t["end_name"] = r["end_station_name"]
        d = _date_min(r["station_train_date"])
        if d is None:
            continue
        key = r["station_telecode"] or r["station_name"]
        o = t["obs"].setdefault(key, {"name": r["station_name"], "telecode": r["station_telecode"],
                                      "no": r["station_no"], "code": r["station_train_code"],
                                      "dist": _int(r["distance"])})
        arr, dep = _minutes(r["arrive_time"]), _minutes(r["start_time"])
        if arr is not None:
            o["arr"] = d + arr
        if dep is not None:
            o["dep"] = d + dep
        # 始发/终到推导：running_time 为始发至本站的累计历时，
        # 终到历时 = 不小于该值且与 (end_arrive - start_start) 同余(mod 1440) 的最小值
        sd = _date_min(r["start_train_date"])
        ss, run = _minutes(r["start_start_time"]), _dur_min(r["running_time"])
        ea = _minutes(r["end_arrive_time"])
        if sd is not None and ss is not None:
            so = t["obs"].setdefault(t["start_key"], {"name": t["start_name"], "telecode": None, "no": 1})
            so.setdefault("dep", sd + ss)
            so.setdefault("name", t["start_name"])
            if run is not None and ea is not None:
                total_mod = (ea - ss) % 1440
                total = run + (total_mod - run) % 1440
                eo = t["obs"].setdefault(t["end_key"], {"name": t["end_name"], "telecode": None, "no": None})
                eo.setdefault("arr", sd + ss + total)
                eo.setdefault("name", t["end_name"])
        no = r["station_no"]
        if no is not None:
            if t.get("min_no") is None or no < t["min_no"]:
                t["min_no"], t["start_code"] = no, r["station_train_code"]
            if t.get("max_no") is None or no > t["max_no"]:
                t["max_no"], t["end_code"] = no, r["station_train_code"]
    for t in trains.values():
        if t["start_key"] in t["obs"]:
            t["obs"][t["start_key"]].setdefault("code", t.get("start_code") or t["code"])
        if t["end_key"] in t["obs"]:
            t["obs"][t["end_key"]].setdefault("code", t.get("end_code") or t["code"])
    return trains


def _ordered(a: dict, b: dict) -> bool:
    return a.get("no") is None or b.get("no") is None or a["no"] < b["no"]


def _dep_keys(key, name, available, cities, exclude=frozenset()):
    """同站 + 同城异站的候选出发站键（按物理站去重，排除指定站）。"""
    city_of, city_groups, tele_of = cities
    out = [key]
    c = city_of.get(key) or city_of.get(name or "")
    if c:
        ck = tele_of.get(key, key)
        out += [x for x in sorted(city_groups[c])
                if x in available and tele_of.get(x, x) != ck and tele_of.get(x, x) not in exclude]
    return out


def _search_screen(trains, fkeys, tkeys, day_min, min_wait, max_wait, prices, seats, cal, sno, cities):
    by_station = {}
    for tn, t in trains.items():
        for k in t["obs"]:
            by_station.setdefault(k, []).append(tn)
    available = set(by_station)
    to_obs = {}
    for tn, t in trains.items():
        hits = [(k, t["obs"][k]) for k in tkeys if k in t["obs"] and "arr" in t["obs"][k]]
        if hits:
            to_obs[tn] = hits

    at_from = list(dict.fromkeys(tn for k in fkeys for tn in by_station.get(k, ())))
    tele_of = cities[2]
    exact = set()
    for tn in at_from:
        for k in fkeys:
            o = trains[tn]["obs"].get(k)
            if o and "dep" in o and o["dep"] // 1440 == day_min // 1440:
                exact.add(tn[0])
                break

    def leg(tn, t, a, b, shift):
        dep_date = _fmt_epoch(a["dep"] + shift)[:10]
        km = None
        if a.get("dist") is not None and b.get("dist") is not None and b["dist"] > a["dist"]:
            km = b["dist"] - a["dist"]
        return _enrich({
            "train": a.get("code") or t["code"], "train_no": tn[0],
            "from": a.get("name") or "", "to": b.get("name") or "",
            "from_telecode": a.get("telecode"), "to_telecode": b.get("telecode"),
            "depart": _fmt_epoch(a["dep"] + shift), "arrive": _fmt_epoch(b["arr"] + shift),
            "duration_min": b["arr"] - a["dep"],
        }, tn[0], a.get("no"), b.get("no"), a.get("telecode"), b.get("telecode"), dep_date, prices, seats,
            code=a.get("code") or t["code"], km=km, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

    direct, transfers = [], []
    for tn in at_from:
        t = trains[tn]
        fo, shift = None, 0
        for k in fkeys:
            o = t["obs"].get(k)
            if o and "dep" in o and o["dep"] // 1440 == day_min // 1440:
                fo = o
                break
        if fo is None:
            # 无当日数据：取最近爬取的班次，按每日开行假设平移到查询日期
            if tn[0] in exact:
                continue
            for k in fkeys:
                o = t["obs"].get(k)
                if o and "dep" in o:
                    fo = o
                    shift = day_min - (o["dep"] // 1440) * 1440
                    break
        if fo is None:
            continue
        a_dep = fo["dep"] + shift
        assumed = shift != 0
        for k, to in to_obs.get(tn, ()):
            if to["arr"] + shift > a_dep and _ordered(fo, to):
                direct.append({"total_min": to["arr"] - fo["dep"], "date_assumed": assumed,
                               "legs": [leg(tn, t, fo, to, shift)]})
        before_from = set(fkeys)
        if fo.get("no") is not None:
            before_from |= {k2 for k2, o2 in t["obs"].items()
                            if o2.get("no") is not None and o2["no"] < fo["no"]}
        for sk, so in t["obs"].items():
            if sk in fkeys or sk in tkeys or "arr" not in so or not _ordered(fo, so):
                continue
            a_arr_s = so["arr"] + shift
            if a_arr_s <= a_dep:
                continue
            forbidden = before_from | {sk}
            passed_canon = {tele_of.get(k2, k2) for k2 in before_from}
            if so.get("no") is not None and fo.get("no") is not None:
                passed = {k2 for k2, o2 in t["obs"].items()
                          if o2.get("no") is not None and fo["no"] <= o2["no"] < so["no"]}
                if passed & tkeys:
                    continue  # 换乘前已越过到达站，属于折返
                forbidden |= passed
                passed_canon |= {tele_of.get(k2, k2) for k2 in passed}
            for dk in _dep_keys(sk, so.get("name"), available, cities, exclude=passed_canon):
                same = dk == sk
                eff_min = min_wait if same else min_wait + 30
                for bn in by_station.get(dk, ()):
                    if bn[0] == tn[0]:
                        continue
                    b = trains[bn]
                    bs = b["obs"].get(dk)
                    if not bs or "dep" not in bs:
                        continue
                    n = math.ceil((a_arr_s + eff_min - bs["dep"]) / 1440)
                    wait = bs["dep"] + n * 1440 - a_arr_s
                    if wait > max_wait:
                        continue
                    b_dep = bs["dep"] + n * 1440
                    for tk2, bt in to_obs.get(bn, ()):
                        if tk2 == dk or not _ordered(bs, bt) or bt["arr"] + n * 1440 <= b_dep:
                            continue
                        # 第二程重新经过出发站/第一程走过的站 = 无意义折返
                        if bs.get("no") is not None and bt.get("no") is not None:
                            if any(k2 in forbidden and o2.get("no") is not None
                                   and bs["no"] < o2["no"] <= bt["no"] for k2, o2 in b["obs"].items()):
                                continue
                        transfers.append({
                            "total_min": bt["arr"] + n * 1440 - a_dep, "wait_min": wait,
                            "transfer_station": so.get("name") or sk, "same_station": same,
                            "date_assumed": assumed or n != 0,
                            "legs": [leg(tn, t, fo, so, shift), leg(bn, b, bs, bt, n * 1440)],
                        })
    return direct, transfers


# ---------- 引擎 B：时刻表（每日开行假设） ----------
class Train:
    __slots__ = ("train_no", "code", "stops")

    def __init__(self, train_no, code, stops):
        self.train_no = train_no
        self.code = code
        self.stops = stops


def _load_trains() -> list[Train]:
    rows = db.rows("""SELECT train_no, station_train_code, train_date, station_no, station_name,
                             telecode, arrive_time, start_time, enabled
                      FROM train_stops ORDER BY train_no, train_date DESC, station_no""")
    trains, seen = [], set()
    cur_no, cur_date, stops, code = None, None, [], None
    for r in rows:
        if (r["train_no"], r["train_date"]) != (cur_no, cur_date):
            if stops and cur_no not in seen:
                seen.add(cur_no)
                trains.append(Train(cur_no, code, stops))
            cur_no, cur_date = r["train_no"], r["train_date"]
            stops, code = [], r["station_train_code"]
        stops.append(r)
    if stops and cur_no not in seen:
        trains.append(Train(cur_no, code, stops))

    for t in trains:
        day, prev = 0, None
        for s in t.stops:
            arr, dep = _minutes(s["arrive_time"]), _minutes(s["start_time"])
            if arr is None and dep is None:
                s["arr"] = s["dep"] = None
                continue
            if arr is None:
                arr = dep
            if prev is not None and arr < prev:
                day += 1
            if dep is None:
                dep = arr
            elif dep < arr:
                day += 1
            s["arr"], s["dep"] = day * 1440 + arr, day * 1440 + dep
            s["key"] = s["telecode"] or s["station_name"]
            prev = dep
        t.stops = [s for s in t.stops if s.get("arr") is not None]
    return [t for t in trains if len(t.stops) >= 2]


def _search_timetable(trains, fkeys, tkeys, base, min_wait, max_wait, prices, seats, cal, sno, cities):
    available = {s["key"] for t in trains for s in t.stops}

    def find_idx(t, keys):
        return [i for i, s in enumerate(t.stops) if s["key"] in keys and s["enabled"]]

    def leg(t, i, j, shift):
        a, b = t.stops[i], t.stops[j]
        return _enrich({
            "train": t.code, "train_no": t.train_no,
            "from": a["station_name"], "to": b["station_name"],
            "from_telecode": a["telecode"], "to_telecode": b["telecode"],
            "depart": _fmt(base, a["dep"] + shift), "arrive": _fmt(base, b["arr"] + shift),
            "duration_min": b["arr"] - a["dep"],
        }, t.train_no, a["station_no"], b["station_no"], a["telecode"], b["telecode"],
            _fmt(base, a["dep"] + shift)[:10], prices, seats,
            code=t.code, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

    direct, transfers = [], []
    for A in trains:
        fi = find_idx(A, fkeys)
        if not fi:
            continue
        i = fi[0]
        shift_a = -(A.stops[i]["dep"] // 1440) * 1440
        a_dep = A.stops[i]["dep"] + shift_a
        for j in find_idx(A, tkeys):
            if j > i:
                l = leg(A, i, j, shift_a)
                direct.append({"total_min": l["duration_min"], "date_assumed": True, "legs": [l]})
        for s_idx in range(i + 1, len(A.stops)):
            S = A.stops[s_idx]
            if not S["enabled"]:
                continue
            if S["key"] in tkeys:
                break  # 已到到达站，再往后换乘属于折返
            forbidden = {A.stops[x]["key"] for x in range(i, s_idx)}
            a_arr_s = S["arr"] + shift_a
            for dk in _dep_keys(S["key"], S["station_name"], available, cities,
                                exclude={cities[2].get(x, x) for x in forbidden}):
                same = dk == S["key"]
                eff_min = min_wait if same else min_wait + 30
                for B in trains:
                    if B.train_no == A.train_no:
                        continue
                    b_s = [k for k, s in enumerate(B.stops) if s["key"] == dk and s["enabled"]]
                    if not b_s:
                        continue
                    k = b_s[0]
                    b_to = [m for m in find_idx(B, tkeys) if m > k]
                    if not b_to:
                        continue
                    m = b_to[0]
                    # 第二程重新经过出发站/第一程走过的站/再次回到换乘站 = 无意义折返
                    if any(B.stops[y]["key"] in forbidden or B.stops[y]["key"] == dk
                           for y in range(k + 1, m + 1)):
                        continue
                    dep_base = B.stops[k]["dep"]
                    n = math.ceil((a_arr_s + eff_min - dep_base) / 1440)
                    wait = dep_base + n * 1440 - a_arr_s
                    if wait > max_wait:
                        continue
                    transfers.append({
                        "total_min": B.stops[m]["arr"] + n * 1440 - a_dep, "wait_min": wait,
                        "transfer_station": S["station_name"], "same_station": same,
                        "date_assumed": True,
                        "legs": [leg(A, i, s_idx, shift_a), leg(B, k, m, n * 1440)],
                    })
    return direct, transfers


def search(from_q: str, to_q: str, date: str,
           min_wait: int = 30, max_wait: int = 360, limit: int = 0) -> dict:
    fs, ts = db.resolve_station(from_q), db.resolve_station(to_q)
    if not fs or not ts:
        raise ValueError("车站不存在，请先在“爬取数据”页更新车站站名表")
    fkeys = {fs["telecode"], fs["name"]}
    tkeys = {ts["telecode"], ts["name"]}
    base = datetime.strptime(date, "%Y-%m-%d")
    day_min = base.toordinal() * 1440
    prices, seats = _price_map(), _seat_map()
    cal = fare.load_calibration()
    sno = {(r["train_no"], r["telecode"]): r["station_no"]
           for r in db.rows("SELECT train_no,telecode,station_no FROM train_stops WHERE telecode IS NOT NULL")}
    city_of, city_groups, tele_of = {}, {}, {}
    for r in db.rows("SELECT telecode,name,city FROM stations"):
        tele_of[r["name"]] = r["telecode"]
        if r["city"]:
            city_of[r["telecode"]] = r["city"]
            city_of[r["name"]] = r["city"]
            city_groups.setdefault(r["city"], set()).update((r["telecode"], r["name"]))
    cities = (city_of, city_groups, tele_of)

    screen = _load_screen()
    tt = _load_trains()
    d1, t1 = _search_screen(screen, fkeys, tkeys, day_min, min_wait, max_wait, prices, seats, cal, sno, cities)
    d2, t2 = _search_timetable(tt, fkeys, tkeys, base, min_wait, max_wait, prices, seats, cal, sno, cities)

    def rank(p):
        return (0 if any("prices" in l for l in p["legs"]) else 1,
                1 if p.get("date_assumed") else 0)

    merged_d = {}
    for p in d1 + d2:
        key = (p["legs"][0]["train_no"], p["legs"][0]["depart"])
        if key not in merged_d or rank(p) < rank(merged_d[key]):
            merged_d[key] = p
    direct = list(merged_d.values())
    merged_t = {}
    for p in t1 + t2:
        key = (p["legs"][0]["train_no"], p["legs"][1]["train_no"],
               p["transfer_station"], p["legs"][1]["from"], p["legs"][0]["depart"])
        if key not in merged_t or rank(p) < rank(merged_t[key]):
            merged_t[key] = p
    transfers = list(merged_t.values())

    # 引擎 C：仅有余票数据的直达
    have = {p["legs"][0]["train_no"] for p in direct}
    for r in db.rows("""SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?""",
                     (fs["telecode"], ts["telecode"], date)):
        if r["train_no"] in have:
            continue
        dep, arr = _minutes(r["start_time"]), _minutes(r["arrive_time"])
        if dep is None or arr is None:
            continue
        dur = arr - dep
        if dur < 0:
            dur += 1440
        direct.append({"total_min": dur, "legs": [_enrich({
            "train": r["station_train_code"], "train_no": r["train_no"],
            "from": fs["name"], "to": ts["name"],
            "from_telecode": r["from_telecode"], "to_telecode": r["to_telecode"],
            "depart": _fmt(base, dep), "arrive": _fmt(base, dep + dur),
            "duration_min": dur,
        }, r["train_no"], r["from_station_no"], r["to_station_no"],
            r["from_telecode"], r["to_telecode"], date, prices, seats,
            code=r["station_train_code"], duration_min=dur, cal=cal, sno=sno)]})

    direct.sort(key=lambda x: x["total_min"])
    transfers.sort(key=lambda x: x["total_min"])
    return {
        "from": fs["name"], "to": ts["name"], "date": date,
        "screen_trains": len(screen), "timetable_trains": len(tt),
        "note": "大屏数据优先按真实开行日期，无当日数据时按最近爬取日期每日开行假设平移；"
                "异站换乘为同城车站，额外预留30分钟；已剔除折返/绕回出发站的无意义方案",
        "direct": direct[:limit] if limit > 0 else direct,
        "transfers": transfers[:limit] if limit > 0 else transfers,
    }