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

from . import db, fare, parsers


class SearchCancelled(Exception):
    """换乘搜索被用户中途打断。"""
    pass


def _check_cancel(cancel_event):
    if cancel_event is not None and cancel_event.is_set():
        raise SearchCancelled("搜索任务已取消")


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


def _epoch(s: str) -> int:
    return _date_min(s[:10]) + _minutes(s[11:16])


def _fmt(base: datetime, minutes: int) -> str:
    return (base + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M")


def _price_map(target_date: str | None = None) -> dict:
    buckets: dict[tuple, dict[str, list]] = {}
    for r in db.rows(
        "SELECT train_no,train_date,from_station_no,to_station_no,seat_type,seat_name,price "
        "FROM prices ORDER BY train_date ASC, id ASC"
    ):
        key = (r["train_no"], str(r["from_station_no"]).zfill(2), str(r["to_station_no"]).zfill(2))
        d = r["train_date"] or ""
        buckets.setdefault(key, {}).setdefault(d, []).append(
            {"seat_type": r["seat_type"], "seat": r["seat_name"], "price": r["price"]}
        )
    out = {}
    for key, by_date in buckets.items():
        if target_date and target_date in by_date:
            chosen_date, stale = target_date, False
        else:
            chosen_date = max(by_date)
            stale = bool(target_date and chosen_date and chosen_date != target_date)
        # 同席别去重（保留最后写入）
        dedup = {x["seat_type"]: x for x in by_date[chosen_date]}
        out[key] = {"items": list(dedup.values()), "date": chosen_date, "stale": stale}
    return out


def _seat_map(target_date: str | None = None) -> dict:
    out = {}
    sql = ("SELECT t.train_no,t.from_telecode,t.to_telecode,t.train_date,t.seats,t.can_web_buy,"
           "r.fetched_at FROM tickets t LEFT JOIN raw_payloads r ON r.id=t.raw_id")
    params = ()
    if target_date:
        # 多程跨天可能查到次日余票，仍按全日期索引或限定 >= target_date
        sql += " WHERE t.train_date >= ?"
        params = (target_date,)
    for r in db.rows(sql, params):
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
        fno_s, tno_s = str(fno).zfill(2), str(tno).zfill(2)
        leg["from_station_no"] = fno_s
        leg["to_station_no"] = tno_s
        p = prices.get((train_no, fno_s, tno_s))
        if p:
            leg["prices"] = p["items"]
            if p["date"]:
                leg["price_date"] = p["date"]
            if p["stale"]:
                leg["price_stale"] = True
    if "prices" not in leg and code:
        est = fare.estimate(code, km=km, duration_min=duration_min, cal=cal)
        if est:
            leg["price_est"] = est
    tk = seats.get((train_no, ftc, ttc, dep_date))
    if tk:
        leg["seats"] = json.loads(tk["seats"] or "{}")
        leg["can_web_buy"] = tk["can_web_buy"]
        if tk.get("fetched_at"):
            leg["ticket_updated"] = tk["fetched_at"].replace("T", " ")[:16]
    return leg


# ---------- 引擎 A：车站大屏（真实日期优先，无当日数据时按每日开行假设平移） ----------
def _load_screen(train_filter: set[str] | None = None) -> dict:
    trains = {}
    if train_filter is not None:
        if not train_filter:
            return trains
        if len(train_filter) <= 500:
            ph = ",".join("?" * len(train_filter))
            recs = db.rows(f"SELECT * FROM station_trains WHERE train_no IN ({ph})", tuple(train_filter))
        else:
            recs = [r for r in db.rows("SELECT * FROM station_trains") if r["train_no"] in train_filter]
    else:
        recs = db.rows("SELECT * FROM station_trains")
    for r in recs:
        tn = r["train_no"]
        if not tn:
            continue
        # 同一 train_no 可能对应多个开行日期/往返班次，按开行实例分组
        run = (tn, r["start_train_date"] or r["station_train_date"])
        sd_min = _date_min(r["start_train_date"]) or _date_min(r["station_train_date"])
        t = trains.setdefault(run, {"code": r["station_train_code"], "obs": {}, "running_figs": set(),
                                    "source_date": r["station_train_date"], "sd_min": sd_min})
        if t.get("sd_min") is None and sd_min is not None:
            t["sd_min"] = sd_min
        if r["running_fig"]:
            t["running_figs"].add(r["running_fig"])
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
        ss, run_m = _minutes(r["start_start_time"]), _dur_min(r["running_time"])
        ea = _minutes(r["end_arrive_time"])
        if sd is not None and ss is not None:
            so = t["obs"].setdefault(t["start_key"], {"name": t["start_name"],
                                                      "telecode": r["start_station_telecode"] or None, "no": 1})
            so.setdefault("dep", sd + ss)
            so.setdefault("name", t["start_name"])
            if run_m is not None and ea is not None:
                total_mod = (ea - ss) % 1440
                total = run_m + (total_mod - run_m) % 1440
                eo = t["obs"].setdefault(t["end_key"], {"name": t["end_name"],
                                                        "telecode": r["end_station_telecode"] or None, "no": None})
                eo.setdefault("arr", sd + ss + total)
                eo.setdefault("name", t["end_name"])
        no = r["station_no"]
        if no is not None:
            if t.get("min_no") is None or no < t["min_no"]:
                t["min_no"], t["start_code"] = no, r["station_train_code"]
            if t.get("max_no") is None or no > t["max_no"]:
                t["max_no"], t["end_code"] = no, r["station_train_code"]

    # 跨夜大屏桥接：同一 train_no 在不同始发日班次下若缺失停靠站，按各自始发日差值平移补全，
    # 解决“两端仅各爬同一查询日大屏，跨零点车次因 start_train_date 相差 1 天而断裂”的问题
    by_tn: dict[str, list[tuple[tuple, dict]]] = {}
    for run, t in trains.items():
        by_tn.setdefault(run[0], []).append((run, t))
    for run_list in by_tn.values():
        if len(run_list) <= 1:
            continue
        # 先拍摄各班次原始停靠快照，避免链式二次桥接混淆
        snapshots = {run: {k: dict(v) for k, v in t["obs"].items()} for run, t in run_list}
        for run, t in run_list:
            sd1 = t.get("sd_min")
            if sd1 is None:
                continue
            others = sorted(
                ((r2, t2) for r2, t2 in run_list if r2 != run and t2.get("sd_min") is not None),
                key=lambda x: (abs(sd1 - x[1]["sd_min"]), -x[1]["sd_min"]),
            )
            for r2, t2 in others:
                delta = sd1 - t2["sd_min"]
                for k, o in snapshots[r2].items():
                    if k not in t["obs"]:
                        co = dict(o)
                        if "arr" in co:
                            co["arr"] = co["arr"] + delta
                        if "dep" in co:
                            co["dep"] = co["dep"] + delta
                        co["bridged"] = True
                        t["obs"][k] = co
                        ono = co.get("no")
                        if ono is not None:
                            if t.get("min_no") is None or ono < t["min_no"]:
                                t["min_no"] = ono
                            if t.get("max_no") is None or ono > t["max_no"]:
                                t["max_no"] = ono
                    elif t["obs"][k].get("no") is None and o.get("no") is not None:
                        ono = o["no"]
                        t["obs"][k]["no"] = ono
                        if t.get("max_no") is None or ono > t["max_no"]:
                            t["max_no"] = ono

    for t in trains.values():
        if t["start_key"] in t["obs"]:
            t["obs"][t["start_key"]].setdefault("code", t.get("start_code") or t["code"])
        if t["end_key"] in t["obs"]:
            eo = t["obs"][t["end_key"]]
            eo.setdefault("code", t.get("end_code") or t["code"])
            if eo.get("no") is None and t.get("max_no") is not None and t["end_key"] != t["start_key"]:
                eo["no"] = t["max_no"] + 1
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


def _fig_valid(t: dict, query_date: str) -> bool:
    figs = t.get("running_figs")
    if not figs:
        return True
    checks = [parsers.running_fig_allows(f, query_date) for f in figs]
    known = [c for c in checks if c is not None]
    if known and not any(known):
        return False
    return True


def _search_screen(trains, fkeys, tkeys, day_min, query_date, min_wait, max_wait,
                   prices, seats, cal, sno, cities, cross_extra=60, cancel_event=None):
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
    sorted_fkeys = sorted(fkeys)
    for tn in at_from:
        _check_cancel(cancel_event)
        t = trains[tn]
        fo_cands, seen_phys = [], set()
        for k in sorted_fkeys:
            o = t["obs"].get(k)
            if o and "dep" in o and o["dep"] // 1440 == day_min // 1440:
                phys = (o.get("telecode") or o.get("name"), o["dep"])
                if phys not in seen_phys:
                    seen_phys.add(phys)
                    fo_cands.append((o, 0))
        if not fo_cands:
            # 无当日数据：取最近爬取的班次，按每日开行假设平移到查询日期
            if tn[0] in exact or not _fig_valid(t, query_date):
                continue
            for k in sorted_fkeys:
                o = t["obs"].get(k)
                if o and "dep" in o:
                    phys = (o.get("telecode") or o.get("name"), o["dep"])
                    if phys not in seen_phys:
                        seen_phys.add(phys)
                        fo_cands.append((o, day_min - (o["dep"] // 1440) * 1440))
        for fo, shift in fo_cands:
            a_dep = fo["dep"] + shift
            assumed = (shift != 0) or bool(fo.get("bridged"))
            for _k, to in to_obs.get(tn, ()):
                if to["arr"] + shift > a_dep and _ordered(fo, to):
                    direct.append({"total_min": to["arr"] - fo["dep"],
                                   "date_assumed": assumed or bool(to.get("bridged")),
                                   "source_date": t.get("source_date"),
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
                    eff_min = min_wait if same else min_wait + cross_extra
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
                        b_dep_date = _fmt_epoch(bs["dep"] + n * 1440)[:10]
                        if n != 0 and not _fig_valid(b, b_dep_date):
                            continue
                        b_dep = bs["dep"] + n * 1440
                        for tk2, bt in to_obs.get(bn, ()):
                            if tk2 == dk or tk2 in forbidden or not _ordered(bs, bt) or bt["arr"] + n * 1440 <= b_dep:
                                continue
                            # 第二程重新经过出发站/第一程走过的站 = 无意义折返
                            if bs.get("no") is not None and bt.get("no") is not None:
                                if any(k2 in forbidden and o2.get("no") is not None
                                       and bs["no"] < o2["no"] <= bt["no"] for k2, o2 in b["obs"].items()):
                                    continue
                            is_assumed = (assumed or n != 0 or bool(so.get("bridged"))
                                          or bool(bs.get("bridged")) or bool(bt.get("bridged")))
                            transfers.append({
                                "total_min": bt["arr"] + n * 1440 - a_dep, "wait_min": wait,
                                "transfer_station": so.get("name") or sk, "same_station": same,
                                "date_assumed": is_assumed,
                                "source_date": t.get("source_date") or b.get("source_date"),
                                "legs": [leg(tn, t, fo, so, shift), leg(bn, b, bs, bt, n * 1440)],
                            })
    return direct, transfers


# ---------- 引擎 B：时刻表（每日开行假设） ----------
class Train:
    __slots__ = ("train_no", "code", "train_date", "stops")

    def __init__(self, train_no, code, train_date, stops):
        self.train_no = train_no
        self.code = code
        self.train_date = train_date
        self.stops = stops


def _load_trains(train_filter: set[str] | None = None) -> list[Train]:
    if train_filter is not None:
        if not train_filter:
            return []
        if len(train_filter) <= 500:
            ph = ",".join("?" * len(train_filter))
            recs = db.rows(f"""SELECT train_no, station_train_code, train_date, station_no, station_name,
                                      telecode, arrive_time, start_time, enabled
                               FROM train_stops WHERE train_no IN ({ph})
                               ORDER BY train_no, train_date DESC, station_no""", tuple(train_filter))
        else:
            recs = [r for r in db.rows("""SELECT train_no, station_train_code, train_date, station_no, station_name,
                                                 telecode, arrive_time, start_time, enabled
                                          FROM train_stops ORDER BY train_no, train_date DESC, station_no""")
                    if r["train_no"] in train_filter]
    else:
        recs = db.rows("""SELECT train_no, station_train_code, train_date, station_no, station_name,
                                 telecode, arrive_time, start_time, enabled
                          FROM train_stops ORDER BY train_no, train_date DESC, station_no""")
    trains, seen = [], set()
    cur_no, cur_date, stops, code = None, None, [], None
    for r in recs:
        if (r["train_no"], r["train_date"]) != (cur_no, cur_date):
            if stops and cur_no not in seen:
                seen.add(cur_no)
                trains.append(Train(cur_no, code, cur_date, stops))
            cur_no, cur_date = r["train_no"], r["train_date"]
            stops, code = [], r["station_train_code"]
        stops.append(r)
    if stops and cur_no not in seen:
        trains.append(Train(cur_no, code, cur_date, stops))

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


def _search_timetable(trains, fkeys, tkeys, base, min_wait, max_wait, prices, seats, cal, sno, cities,
                      cross_extra=60, cancel_event=None):
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
        _check_cancel(cancel_event)
        fi = find_idx(A, fkeys)
        if not fi:
            continue
        tj = find_idx(A, tkeys)
        for i in fi:
            shift_a = -(A.stops[i]["dep"] // 1440) * 1440
            a_dep = A.stops[i]["dep"] + shift_a
            for j in tj:
                if j > i:
                    l = leg(A, i, j, shift_a)
                    direct.append({"total_min": l["duration_min"], "date_assumed": True,
                                   "source_date": A.train_date, "legs": [l]})
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
                    eff_min = min_wait if same else min_wait + cross_extra
                    for B in trains:
                        if B.train_no == A.train_no:
                            continue
                        b_s = [k for k, s in enumerate(B.stops) if s["key"] == dk and s["enabled"]]
                        if not b_s:
                            continue
                        b_all_to = find_idx(B, tkeys)
                        if not b_all_to:
                            continue
                        for k in b_s:
                            b_to = [m for m in b_all_to if m > k]
                            if not b_to:
                                continue
                            dep_base = B.stops[k]["dep"]
                            n = math.ceil((a_arr_s + eff_min - dep_base) / 1440)
                            wait = dep_base + n * 1440 - a_arr_s
                            if wait > max_wait:
                                continue
                            for m in b_to:
                                # 第二程重新经过出发站/第一程走过的站/再次回到换乘站 = 无意义折返
                                if any(B.stops[y]["key"] in forbidden or B.stops[y]["key"] == dk
                                       for y in range(k + 1, m + 1)):
                                    continue
                                transfers.append({
                                    "total_min": B.stops[m]["arr"] + n * 1440 - a_dep, "wait_min": wait,
                                    "transfer_station": S["station_name"], "same_station": same,
                                    "date_assumed": True,
                                    "source_date": A.train_date or B.train_date,
                                    "legs": [leg(A, i, s_idx, shift_a), leg(B, k, m, n * 1440)],
                                })
    return direct, transfers


# ---------- 多程换乘（3 程及以上，每日开行假设） ----------
MAX_TOTAL_MIN = 2880   # 总行程上限（分钟，48 小时），防止组合爆炸
FRONTIER_K = 8         # 每个站每层保留的最优到达状态数（剪枝）


def _load_services(screen: dict, tt: list) -> list[dict]:
    """把大屏与时刻表数据统一成“车次服务”，停站为相对始发的分钟数。"""
    services, seen = [], set()
    for t in tt:
        if t.train_no in seen:
            continue
        seen.add(t.train_no)
        stops = [{"key": s["key"], "name": s["station_name"], "telecode": s["telecode"],
                  "no": s["station_no"], "dep": s["dep"], "arr": s["arr"],
                  "enabled": s.get("enabled", 1)} for s in t.stops]
        if len(stops) >= 2:
            services.append({"train_no": t.train_no, "code": t.code, "stops": stops})
    for run, t in screen.items():
        tn = run[0]
        if tn in seen:
            continue
        obs = [o for o in t["obs"].values()
               if o.get("no") is not None and (o.get("dep") is not None or o.get("arr") is not None)]
        if len(obs) < 2:
            continue
        obs.sort(key=lambda o: o["no"])
        origin = next((o["dep"] for o in obs if o.get("dep") is not None), None)
        if origin is None:
            continue
        # 保留始发的实际发车时刻（仅平移到始发当日零点），跨天停站按累计分钟表示，
        # 与时刻表服务的表示一致，避免发车时刻被归零成 00:00
        base = origin - (origin % 1440)
        stops = []
        for o in obs:
            dep = o["dep"] - base if o.get("dep") is not None else None
            arr = o["arr"] - base if o.get("arr") is not None else None
            if dep is None:
                dep = arr
            if arr is None:
                arr = dep
            if dep is None:
                continue
            stops.append({"key": o.get("telecode") or o.get("name"), "name": o.get("name"),
                          "telecode": o.get("telecode"), "no": o.get("no"),
                          "dep": dep, "arr": arr, "enabled": 1})
        if len(stops) >= 2:
            services.append({"train_no": tn, "code": t.get("code") or obs[0].get("code"), "stops": stops})
    return services


def _search_multi(services, fkeys, tkeys, base_min, min_wait, max_wait, max_legs,
                  prices, seats, cal, sno, cities, real_tk=None, n2c=None,
                  cross_extra=60, frontier_k=FRONTIER_K,
                  cancel_event=None, depth_cb=None) -> list[dict]:
    """BFS 多程换乘，返回 3..max_legs 程的换乘方案（含换乘段明细）。"""
    if max_legs < 3:
        return []
    real_tk = real_tk or {}
    n2c = n2c or {}
    by_station = {}
    for si, sv in enumerate(services):
        for i, st in enumerate(sv["stops"]):
            if st["enabled"]:
                by_station.setdefault(st["key"], []).append((si, i))
    available = set(by_station)
    tele_of = cities[2]

    def leg_conflict(l: dict) -> bool:
        ftc = l.get("from_telecode") or n2c.get(l.get("from"))
        ttc = l.get("to_telecode") or n2c.get(l.get("to"))
        key = (ftc, ttc, l["depart"][:10])
        return None not in key and key in real_tk and l["train_no"] not in real_tk[key]

    def make_leg(sv, i, j, n):
        a, b = sv["stops"][i], sv["stops"][j]
        dep = base_min + a["dep"] + n * 1440
        arr = base_min + b["arr"] + n * 1440
        return _enrich({
            "train": sv["code"], "train_no": sv["train_no"],
            "from": a["name"], "to": b["name"],
            "from_telecode": a["telecode"], "to_telecode": b["telecode"],
            "depart": _fmt_epoch(dep), "arrive": _fmt_epoch(arr),
            "duration_min": b["arr"] - a["dep"],
        }, sv["train_no"], a["no"], b["no"], a["telecode"], b["telecode"],
            _fmt_epoch(dep)[:10], prices, seats, code=sv["code"],
            duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

    # 初始状态：从出发站坐首程，出发日 = 查询日
    init = []
    for fk in fkeys:
        for si, i in by_station.get(fk, ()):
            sv = services[si]
            st = sv["stops"][i]
            if st["key"] not in fkeys:
                continue
            n0 = -(st["dep"] // 1440)
            for j in range(i + 1, len(sv["stops"])):
                b = sv["stops"][j]
                if not b["enabled"]:
                    continue
                leg = make_leg(sv, i, j, n0)
                if leg_conflict(leg):
                    continue
                passed = frozenset(sv["stops"][k]["key"] for k in range(i, j + 1))
                init.append({"arr": _epoch(leg["arrive"]), "station": b["key"], "name": b["name"],
                             "passed": passed, "legs": [leg], "transfers": [],
                             "start": _epoch(leg["depart"]), "has_seats": "seats" in leg})

    results, frontier = [], init
    for depth in range(2, max_legs + 1):
        _check_cancel(cancel_event)
        if depth_cb is not None:
            depth_cb(depth, len(frontier))
        nxt = {}
        for state in frontier:
            _check_cancel(cancel_event)
            t = state["arr"]
            if t - state["start"] > MAX_TOTAL_MIN:
                continue
            exclude = frozenset(tele_of.get(x, x) for x in state["passed"])
            for dk in _dep_keys(state["station"], state["name"], available, cities, exclude=exclude):
                same = dk == state["station"]
                eff_min = min_wait if same else min_wait + cross_extra
                for si, i in by_station.get(dk, ()):
                    sv = services[si]
                    if sv["train_no"] == state["legs"][-1]["train_no"]:
                        continue
                    st_i = sv["stops"][i]
                    n = math.ceil((t + eff_min - (base_min + st_i["dep"])) / 1440)
                    dep = base_min + st_i["dep"] + n * 1440
                    wait = dep - t
                    if wait > max_wait:
                        continue
                    for j in range(i + 1, len(sv["stops"])):
                        b = sv["stops"][j]
                        if not b["enabled"] or b["key"] in state["passed"]:
                            continue
                        if any(sv["stops"][k]["key"] in state["passed"] for k in range(i + 1, j)):
                            continue
                        leg = make_leg(sv, i, j, n)
                        if leg_conflict(leg):
                            continue
                        new_passed = state["passed"] | frozenset(sv["stops"][k]["key"] for k in range(i, j + 1))
                        new_legs = state["legs"] + [leg]
                        new_transfers = state["transfers"] + [{
                            "station": state["name"],
                            "telecode": state["legs"][-1].get("to_telecode"),
                            "same": same,
                            "wait_min": wait,
                        }]
                        total = _epoch(leg["arrive"]) - state["start"]
                        if total > MAX_TOTAL_MIN:
                            continue
                        if b["key"] in tkeys and len(new_legs) >= 3:
                            results.append({"total_min": total, "date_assumed": True,
                                            "legs": new_legs, "transfers": new_transfers})
                        else:
                            nxt.setdefault(b["key"], []).append(
                                {"arr": _epoch(leg["arrive"]), "station": b["key"], "name": b["name"],
                                 "passed": new_passed, "legs": new_legs, "transfers": new_transfers,
                                 "start": state["start"],
                                 "has_seats": state["has_seats"] and ("seats" in leg)})
        frontier = []
        for bucket in nxt.values():
            bucket.sort(key=lambda s: (0 if s["has_seats"] else 1, s["arr"]))
            frontier.extend(bucket[:frontier_k])
        if not frontier:
            break

    dedup = {}
    for p in results:
        key = (tuple(l["train_no"] for l in p["legs"]),
               tuple(t["station"] for t in p["transfers"]),
               p["legs"][0]["depart"])
        if key not in dedup or p["total_min"] < dedup[key]["total_min"]:
            dedup[key] = p
    return list(dedup.values())


def _candidate_trains(fkeys: set[str], tkeys: set[str]) -> set[str]:
    """2 程以内预过滤：只加载经过出发站或到达站（含大屏始发终到）的候选 train_no。"""
    keys = list(fkeys | tkeys)
    if not keys:
        return set()
    ph = ",".join("?" * len(keys))
    out = set()
    for r in db.rows(
        f"""SELECT DISTINCT train_no FROM station_trains
            WHERE train_no IS NOT NULL AND (
                station_telecode IN ({ph}) OR station_name IN ({ph})
                OR start_station_telecode IN ({ph}) OR start_station_name IN ({ph})
                OR end_station_telecode IN ({ph}) OR end_station_name IN ({ph})
            )""",
        (*keys, *keys, *keys, *keys, *keys, *keys),
    ):
        out.add(r["train_no"])
    for r in db.rows(
        f"""SELECT DISTINCT train_no FROM train_stops
            WHERE telecode IN ({ph}) OR station_name IN ({ph})""",
        (*keys, *keys),
    ):
        out.add(r["train_no"])
    return out


def _load_sno(target_date: str | None = None) -> dict:
    out = {}
    for r in db.rows(
        "SELECT train_no,telecode,station_no,train_date FROM train_stops "
        "WHERE telecode IS NOT NULL ORDER BY train_date ASC, id ASC"
    ):
        out[(r["train_no"], r["telecode"])] = r["station_no"]
    if target_date:
        for r in db.rows(
            "SELECT train_no,telecode,station_no FROM train_stops "
            "WHERE telecode IS NOT NULL AND train_date=?",
            (target_date,),
        ):
            out[(r["train_no"], r["telecode"])] = r["station_no"]
    return out


def search(from_q: str, to_q: str, date: str,
           min_wait: int = 30, max_wait: int = 360, limit: int = 0,
           max_legs: int = 2, expand_city: bool = True,
           cross_extra: int = 60, frontier_k: int = FRONTIER_K,
           progress_cb=None, cancel_event=None) -> dict:
    total_steps = 6 if max_legs < 3 else 5 + (max_legs - 1)

    def step(done: int, msg: str):
        _check_cancel(cancel_event)
        if progress_cb is not None:
            progress_cb(done, total_steps, msg)

    step(1, "解析车站与价格/余票索引")
    fstations = db.resolve_stations(from_q, expand_city)
    tstations = db.resolve_stations(to_q, expand_city)
    if not fstations or not tstations:
        missing = from_q if not fstations else to_q
        raise ValueError(f"车站/城市“{missing}”不存在，请先在「爬取数据」页更新车站站名表")
    fkeys = {s["telecode"] for s in fstations} | {s["name"] for s in fstations}
    tkeys = {s["telecode"] for s in tstations} | {s["name"] for s in tstations}
    base = datetime.strptime(date, "%Y-%m-%d")
    day_min = base.toordinal() * 1440
    prices, seats = _price_map(date), _seat_map(date)
    n2c = db.station_lookup()
    # 当日已爬取到权威余票的 OD：用于校验“按每日开行推算”方案是否与实际余票冲突
    real_tk = {}
    for r in db.rows("SELECT from_telecode,to_telecode,train_date,train_no FROM tickets WHERE train_date >= ?", (date,)):
        if r["from_telecode"] and r["to_telecode"]:
            real_tk.setdefault((r["from_telecode"], r["to_telecode"], r["train_date"]), set()).add(r["train_no"])
    cal = fare.load_calibration()
    sno = _load_sno(date)
    city_of, city_groups, tele_of = {}, {}, {}
    for r in db.rows("SELECT telecode,name,city FROM stations"):
        tele_of[r["name"]] = r["telecode"]
        if r["city"]:
            city_of[r["telecode"]] = r["city"]
            city_of[r["name"]] = r["city"]
            city_groups.setdefault(r["city"], set()).update((r["telecode"], r["name"]))
    cities = (city_of, city_groups, tele_of)

    step(2, "加载大屏与时刻表缓存")
    cand = _candidate_trains(fkeys, tkeys) if max_legs <= 2 else None
    screen = _load_screen(cand)
    tt = _load_trains(cand)
    step(3, f"计算大屏直达/换乘（{len(screen)} 班次）")
    d1, t1 = _search_screen(screen, fkeys, tkeys, day_min, date, min_wait, max_wait,
                            prices, seats, cal, sno, cities, cross_extra=cross_extra,
                            cancel_event=cancel_event)
    step(4, f"计算时刻表直达/换乘（{len(tt)} 车次）")
    d2, t2 = _search_timetable(tt, fkeys, tkeys, base, min_wait, max_wait,
                               prices, seats, cal, sno, cities, cross_extra=cross_extra,
                               cancel_event=cancel_event)

    def rank(p):
        return (0 if any("prices" in l for l in p["legs"]) else 1,
                1 if p.get("date_assumed") else 0)

    merged_d = {}
    for p in d1 + d2:
        key = (p["legs"][0]["train_no"], p["legs"][0]["from"], p["legs"][0]["to"], p["legs"][0]["depart"])
        if key not in merged_d or rank(p) < rank(merged_d[key]):
            merged_d[key] = p
    direct = list(merged_d.values())
    merged_t = {}
    for p in t1 + t2:
        key = (p["legs"][0]["train_no"], p["legs"][1]["train_no"],
               p["transfer_station"], p["legs"][1]["from"], p["legs"][0]["from"],
               p["legs"][1]["to"], p["legs"][0]["depart"])
        if key not in merged_t or rank(p) < rank(merged_t[key]):
            merged_t[key] = p
    transfers = []
    for p in merged_t.values():
        if "transfers" not in p:
            p["transfers"] = [{"station": p.get("transfer_station"),
                               "telecode": p["legs"][0].get("to_telecode"),
                               "same": bool(p.get("same_station", True)),
                               "wait_min": p.get("wait_min", 0)}]
        transfers.append(p)

    # 引擎 D：多程换乘（3 程及以上，每日开行假设）
    if max_legs >= 3:
        services = _load_services(screen, tt)
        transfers.extend(_search_multi(
            services, fkeys, tkeys, day_min, min_wait, max_wait,
            max_legs, prices, seats, cal, sno, cities,
            real_tk=real_tk, n2c=n2c,
            cross_extra=cross_extra, frontier_k=frontier_k,
            cancel_event=cancel_event,
            depth_cb=lambda d, n_st: step(4 + (d - 1), f"多程 BFS 搜索第 {d}/{max_legs} 程（状态 {n_st}）"),
        ))

    step(total_steps, "汇总余票直达并校验冲突")

    # 引擎 C：仅有余票数据的直达
    have = {(p["legs"][0]["train_no"], p["legs"][0].get("from_telecode"), p["legs"][0].get("to_telecode"))
            for p in direct}
    seen_tk = set()
    for fst in fstations:
        for tst in tstations:
            for r in db.rows("""SELECT * FROM tickets WHERE from_telecode=? AND to_telecode=? AND train_date=?""",
                             (fst["telecode"], tst["telecode"], date)):
                tkey = (r["train_no"], r["from_telecode"], r["to_telecode"])
                if tkey in have or tkey in seen_tk:
                    continue
                seen_tk.add(tkey)
                dep, arr = _minutes(r["start_time"]), _minutes(r["arrive_time"])
                if dep is None or arr is None:
                    continue
                dur = arr - dep
                if dur < 0:
                    dur += 1440
                direct.append({"total_min": dur, "source_date": date, "legs": [_enrich({
                    "train": r["station_train_code"], "train_no": r["train_no"],
                    "from": fst["name"], "to": tst["name"],
                    "from_telecode": r["from_telecode"], "to_telecode": r["to_telecode"],
                    "depart": _fmt(base, dep), "arrive": _fmt(base, dep + dur),
                    "duration_min": dur,
                }, r["train_no"], r["from_station_no"], r["to_station_no"],
                    r["from_telecode"], r["to_telecode"], date, prices, seats,
                    code=r["station_train_code"], duration_min=dur, cal=cal, sno=sno)]})

    def _assumed_conflict(p) -> bool:
        """推算方案（无当日真实数据平移而来）若与实际余票冲突，则以实际余票为准剔除。"""
        if not p.get("date_assumed"):
            return False
        for l in p["legs"]:
            ftc = l.get("from_telecode") or n2c.get(l.get("from"))
            ttc = l.get("to_telecode") or n2c.get(l.get("to"))
            key = (ftc, ttc, l["depart"][:10])
            if None in key:
                continue
            if key in real_tk and l["train_no"] not in real_tk[key]:
                return True
        return False

    direct = [p for p in direct if not _assumed_conflict(p)]
    transfers = [p for p in transfers if not _assumed_conflict(p)]

    cn_now_str = db.cn_now().strftime("%Y-%m-%d %H:%M")
    for p in direct + transfers:
        p["departed"] = any(l["depart"] <= cn_now_str for l in p["legs"])
        # 若所有行程段均已有对应出发日的余票实测记录，说明开行已实测验证，不再标为推算
        if p["legs"] and all("seats" in l for l in p["legs"]):
            p["date_assumed"] = False
            p["source_date"] = p["legs"][0]["depart"][:10]
            ups = [l["ticket_updated"] for l in p["legs"] if l.get("ticket_updated")]
            if ups:
                p["ticket_updated"] = max(ups)

    direct.sort(key=lambda x: x["total_min"])
    transfers.sort(key=lambda x: x["total_min"])
    return {
        "from": from_q, "to": to_q, "date": date,
        "server_now": cn_now_str,
        "from_stations": [s["name"] for s in fstations],
        "to_stations": [s["name"] for s in tstations],
        "from_station_labels": [f"{s['name']}({s['telecode']})" for s in fstations],
        "to_station_labels": [f"{s['name']}({s['telecode']})" for s in tstations],
        "screen_trains": len(screen), "timetable_trains": len(tt),
        "note": f"时间基准：北京时间(UTC+8) · 异站换乘默认额外预留{cross_extra}分钟 · 无当日数据时按最近日期与 running_fig 校验推算并剔除余票冲突车次",
        "direct": direct[:limit] if limit > 0 else direct,
        "transfers": transfers[:limit] if limit > 0 else transfers,
    }
