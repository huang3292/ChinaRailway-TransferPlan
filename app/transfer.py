"""换乘搜索。

四个数据引擎，结果合并去重：
A. 车站大屏（station_trains）：真实开行日期优先，无当日数据时按每日开行假设平移
B. 车次时刻表（train_stops）：完整经停站，按每日开行假设推算日期
C. 站间余票（tickets）：精确日期的直达补充
D. 多程换乘（_search_multi）：max_legs>=3 时基于内存路网服务做 BFS 搜索

搜前仅执行两类裁剪：
1. 全局防无限递归与无意义分支（同车次、重访/过终点折返、城市绕回、同城内耗换乘、越过目的站、零/负耗时边）。
2. 四档剪枝预设（unbounded/relaxed/balanced/strict）及独立的指定枢纽站搜前收窄（hub_only / hub_stations）。

车次头（allow_prefixes/exclude_prefixes）与换乘站过滤（include_stations/exclude_stations 及 OR/AND 模式）
已提取为独立的后置过滤函数 filter_plans()，可在结果返回后即时过滤，无需重跑图搜索。
"""
import bisect
import gc
import heapq
import json
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta

from . import china_geo, db, fare, geo, parsers, rail_lines

_CACHE_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()
_MEM_CACHE: dict | None = None
_DATE_CACHE_VER = None
_DATE_CACHE: dict[str, dict] = {}
_MAX_DATE_CACHE = 4


def invalidate_cache():
    """清空进程内内存路网与按日票价/余票缓存。"""
    global _MEM_CACHE, _DATE_CACHE_VER
    with _CACHE_LOCK:
        _MEM_CACHE = None
        _DATE_CACHE_VER = None
        _DATE_CACHE.clear()

_MIN_RE = re.compile(r"(\d{1,2}):(\d{2})")
_COLON_DUR_RE = re.compile(r"(\d+):(\d{2})")
_INF = 10**9
_intern = sys.intern


def _istr(s):
    """站名/电报码/车次号等高重复字符串驻留，跨 11 万条停站记录共享同一对象。"""
    return _intern(s) if type(s) is str and s else s


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
    if not t or t == "----":
        return None
    s = t.strip()
    if len(s) == 5 and s[2] == ":" and "0" <= s[0] <= "9" and "0" <= s[1] <= "9" and "0" <= s[3] <= "9" and "0" <= s[4] <= "9":
        return int(s[:2]) * 60 + int(s[3:])
    m = _MIN_RE.fullmatch(s)
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _colon_dur(t: str | None) -> int | None:
    """解析 HH:MM（含 >=24h 或 >=100h，如 '28:30'）的历时字符串为分钟数。"""
    if not t or t == "----":
        return None
    m = _COLON_DUR_RE.fullmatch(t.strip())
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
    cur = db.get_conn().execute(
        "SELECT train_no,train_date,from_station_no,to_station_no,seat_type,seat_name,price "
        "FROM prices ORDER BY train_date ASC, id ASC"
    )
    for tn, td, fno, tno, stp, sname, price in cur:
        key = (tn, str(fno).zfill(2), str(tno).zfill(2))
        d = td or ""
        buckets.setdefault(key, {}).setdefault(d, []).append(
            {"seat_type": stp, "seat": sname, "price": price}
        )
    out = {}
    for key, by_date in buckets.items():
        if target_date and target_date in by_date:
            chosen_date, stale = target_date, False
        else:
            chosen_date = max(by_date)
            stale = bool(target_date and chosen_date and chosen_date != target_date)
        # 保留按日期索引字典，供 D+1 多日车次精准取乘车日实测票价；顶层结构保持兼容。
        by_date_items = {
            d_k: list({x["seat_type"]: x for x in rows_d}.values())
            for d_k, rows_d in by_date.items()
        }
        out[key] = {
            "items": by_date_items[chosen_date],
            "date": chosen_date,
            "stale": stale,
            "by_date": by_date_items,
        }
    return out


def _pick_price_rec(p: dict | None, dep_date: str | None = None) -> tuple[list | None, str, bool]:
    """优先按乘车日期匹配 by_date 票价，未命中则回退默认票价记录并标注 price_stale。"""
    if not p:
        return (None, "", False)
    by_d = p.get("by_date")
    if dep_date and by_d:
        exact = by_d.get(dep_date)
        if exact:
            return (exact, dep_date, False)
    p_date = p.get("date") or ""
    stale = bool(dep_date and p_date and p_date != dep_date) if dep_date else bool(p.get("stale"))
    return (p.get("items"), p_date, stale)


def _hm_of(m: int | None) -> str:
    if m is None:
        return ""
    mm = m % 1440
    return f"{mm // 60:02d}:{mm % 60:02d}"


def _seat_map(target_date: str | None = None) -> tuple[dict, dict, dict, dict]:
    """一次查询同时构建余票明细映射、(from,to,date)->train_no 冲突校验集、当日直达与改号桥接索引。"""
    out = {}
    real_tk = {}
    direct_tickets: dict[tuple[str, str], list[tuple]] = {}
    tk_aux = {"by_exact": {}, "by_digits": {}, "by_sched": {}}
    sql = (
        "SELECT t.train_no,t.station_train_code,t.from_telecode,t.to_telecode,t.train_date,"
        "t.from_station_no,t.to_station_no,t.start_time,t.arrive_time,t.duration,"
        "t.seats,t.can_web_buy,t.exchange_train_flag,t.dw_flag,t.train_flag,r.fetched_at "
        "FROM tickets t LEFT JOIN raw_payloads r ON r.id=t.raw_id"
    )
    params = ()
    if target_date:
        # 多程跨天可能查到次日余票，仍按全日期索引或限定 >= target_date
        sql += " WHERE t.train_date >= ?"
        params = (target_date,)
    for (
        tn, st_code, ftc, ttc, td, fno, tno, st_time, arr_time, dur,
        seats_js, cwb, ex_flag, dw_flag, tk_tflag, fetched_at,
    ) in db.get_conn().execute(sql, params):
        out[(tn, ftc, ttc, td)] = (seats_js, cwb, fetched_at, ex_flag, dw_flag, tk_tflag)
        rec = (tn, st_code, fno, tno, st_time, arr_time, seats_js, cwb, fetched_at, ex_flag, dw_flag, tk_tflag)
        tk_aux["by_exact"][(tn, ftc, ttc, td)] = rec
        if ftc and ttc:
            real_tk.setdefault((ftc, ttc, td), set()).add(tn)
            if st_code or tn:
                seen_digs = set()
                for c_src in (st_code, parsers.train_no_root_code(tn)):
                    dig = parsers.train_digits(c_src)
                    if dig and dig not in seen_digs:
                        seen_digs.add(dig)
                        tk_aux["by_digits"].setdefault((dig, ftc, ttc, td), []).append(rec)
            if st_time and arr_time:
                tk_aux["by_sched"].setdefault((ftc, ttc, td, st_time, arr_time), []).append(rec)
            if target_date and td == target_date:
                direct_tickets.setdefault((ftc, ttc), []).append(
                    (tn, st_code, fno, tno, st_time, arr_time, dur)
                )
    return out, real_tk, direct_tickets, tk_aux


_style_cache = {"ver": None, "map": {}, "meta": {}}


def _train_meta_maps() -> dict:
    """车站大屏车型与 train_flag 共享只读缓存（以 db.data_version() 为失效键）。

    WHY: 车型仅存在于车站大屏（普速写于 jiaolu_train_style），且同一车次在不同日期或
    不同接口的 12 位 train_no 末 2 位版本码可能不同；因此除精确 train_no 外同时索引
    10 位车次根(tn[:10]) 与大写车次号(station_train_code) 作只读外观兜底，绝不用于停站/票价关联。
    """
    ver = db.data_version()
    if _style_cache["ver"] != ver:
        m = {}
        style_root = {}
        style_code = {}
        flag_tn = {}
        flag_root = {}
        flag_code = {}
        for tn, code, st, jl, tf in db.get_conn().execute(
            "SELECT train_no, station_train_code, train_style, jiaolu_train_style, train_flag "
            "FROM station_trains ORDER BY query_date, id"
        ):
            v = (st or jl or "").strip()
            tf_s = (tf or "").strip()
            cu = (code or "").strip().upper()
            rt = tn[:10] if (tn and len(tn) == 12) else ""
            if v:
                if tn:
                    m[tn] = v
                if rt:
                    style_root[rt] = v
                if cu:
                    style_code[cu] = v
            if tf_s:
                if tn:
                    flag_tn[tn] = tf_s
                if rt:
                    flag_root[rt] = tf_s
                if cu:
                    flag_code[cu] = tf_s
        _style_cache["ver"] = ver
        _style_cache["map"] = m
        _style_cache["meta"] = {
            "style_tn": m,
            "style_root": style_root,
            "style_code": style_code,
            "flag_tn": flag_tn,
            "flag_root": flag_root,
            "flag_code": flag_code,
        }
    return _style_cache["meta"]


def _style_map() -> dict:
    """train_no -> 车底车型（如 CR400BF-Z/CRH380A/25G）。"""
    _train_meta_maps()
    return _style_cache["map"]


def _lookup_train_meta(
    train_no: str | None,
    live_train_no: str | None = None,
    code: str | None = None,
    style_map: dict | None = None,
) -> tuple[str, str]:
    """按 精确train_no → live_train_no → 10位根码 → station_train_code 查找 (车型, 大屏train_flag)。"""
    meta = _train_meta_maps()
    s_tn = style_map if style_map is not None else meta["style_tn"]
    st = ""
    for tn in (train_no, live_train_no):
        if tn and s_tn.get(tn):
            st = s_tn[tn]
            break
    if not st:
        for tn in (train_no, live_train_no):
            if tn and len(tn) == 12 and meta["style_root"].get(tn[:10]):
                st = meta["style_root"][tn[:10]]
                break
    if not st and code:
        cu = str(code).strip().upper()
        st = meta["style_code"].get(cu, "")
        if not st and "/" in cu:
            for part in cu.split("/"):
                if meta["style_code"].get(part.strip()):
                    st = meta["style_code"][part.strip()]
                    break

    tf = ""
    for tn in (live_train_no, train_no):
        if tn and meta["flag_tn"].get(tn):
            tf = meta["flag_tn"][tn]
            break
    if not tf:
        for tn in (live_train_no, train_no):
            if tn and len(tn) == 12 and meta["flag_root"].get(tn[:10]):
                tf = meta["flag_root"][tn[:10]]
                break
    if not tf and code:
        cu = str(code).strip().upper()
        tf = meta["flag_code"].get(cu, "")
    return st, tf


_stop_names_cache = {"ver": None, "by_tn": {}, "by_root": {}, "by_code": {}}


def _stop_names_maps() -> dict:
    """车次全序停站名只读缓存（以 db.data_version() 失效），专供运行线路拓扑消歧。"""
    ver = db.data_version()
    if _stop_names_cache["ver"] != ver:
        by_tn: dict[str, list[str]] = {}
        by_root: dict[str, list[str]] = {}
        by_code: dict[str, list[str]] = {}
        cur_tn = cur_dt = cur_code = None
        cur_stops: list[str] = []
        done_tns: set[str] = set()

        def _flush():
            if cur_tn and cur_tn not in done_tns and len(cur_stops) >= 2:
                done_tns.add(cur_tn)
                tup = list(cur_stops)
                by_tn[cur_tn] = tup
                if len(cur_tn) == 12:
                    by_root.setdefault(cur_tn[:10], tup)
                if cur_code:
                    by_code.setdefault(cur_code.strip().upper(), tup)

        for tn, st_code, td, sname in db.get_conn().execute(
            """SELECT train_no, station_train_code, train_date, station_name
               FROM train_stops
               WHERE COALESCE(enabled, 1) = 1
               ORDER BY train_no, train_date DESC, station_no"""
        ):
            if tn != cur_tn or td != cur_dt:
                _flush()
                cur_tn, cur_dt, cur_code = tn, td, st_code
                cur_stops = []
            elif not cur_code and st_code:
                cur_code = st_code
            if sname:
                cur_stops.append(sname)
        _flush()
        _stop_names_cache["ver"] = ver
        _stop_names_cache["by_tn"] = by_tn
        _stop_names_cache["by_root"] = by_root
        _stop_names_cache["by_code"] = by_code
    return _stop_names_cache


def _lookup_stop_names(
    train_no: str | None,
    live_train_no: str | None = None,
    code: str | None = None,
) -> list[str] | None:
    maps = _stop_names_maps()
    for tn in (train_no, live_train_no):
        if tn and tn in maps["by_tn"]:
            return maps["by_tn"][tn]
    for tn in (train_no, live_train_no):
        if tn and len(tn) == 12 and tn[:10] in maps["by_root"]:
            return maps["by_root"][tn[:10]]
    if code:
        cu = str(code).strip().upper()
        if cu in maps["by_code"]:
            return maps["by_code"][cu]
    return None


def _resolve_live_ticket(
    train_no: str,
    code: str | None,
    ftc: str | None,
    ttc: str | None,
    dep_date: str | None,
    dep_hm: str,
    arr_hm: str,
    tk_aux: dict | None,
    style_map: dict | None = None,
):
    """三层兼容解析实时余票记录（应对 2026-11-25 普速改字母+重编号导致的 train_no 变更）。

    1) 精确 (train_no, from, to, date)
    2) 同大类数字段 (T↔N, K/纯数字↔V, Z↔Z 等) — 严禁与动车组串车
    3) 普速物理发到时刻 (from, to, date, dep_hm, arr_hm) — 兜底如 K8278→V4278 的数字改号
    """
    if not tk_aux or not ftc or not ttc or not dep_date:
        return None
    rec = tk_aux["by_exact"].get((train_no, ftc, ttc, dep_date))
    if rec:
        return rec
    if style_map is None:
        style_map = _style_map()
    is_emu = parsers.is_emu_style(style_map.get(train_no))
    fam = parsers.train_class_family(code, is_emu=is_emu)
    digs = []
    for src in (code, parsers.train_no_root_code(train_no)):
        d_val = parsers.train_digits(src)
        if d_val and d_val not in digs:
            digs.append(d_val)
    for dig in digs:
        cands = tk_aux["by_digits"].get((dig, ftc, ttc, dep_date))
        if cands:
            matched = [
                c for c in cands
                if (not fam or parsers.train_class_family(
                    c[1] or parsers.train_no_root_code(c[0]),
                    is_emu=parsers.is_emu_style(style_map.get(c[0])),
                ) == fam)
            ]
            if matched:
                if dep_hm and arr_hm:
                    exact_tm = [c for c in matched if c[4] == dep_hm and c[5] == arr_hm]
                    if exact_tm:
                        return exact_tm[-1]
                return matched[-1]
    if (not code or parsers.is_conv_code(code, is_emu=is_emu)) and dep_hm and arr_hm:
        cands = tk_aux["by_sched"].get((ftc, ttc, dep_date, dep_hm, arr_hm))
        if cands:
            conv = [
                c for c in cands
                if parsers.is_conv_code(c[1], is_emu=parsers.is_emu_style(style_map.get(c[0])))
            ]
            if conv:
                if fam:
                    same_fam = [
                        c for c in conv
                        if parsers.train_class_family(c[1], is_emu=parsers.is_emu_style(style_map.get(c[0]))) == fam
                    ]
                    if same_fam:
                        return same_fam[-1]
                return conv[-1]
    return None


def _enrich(leg: dict, train_no: str, fno, tno, ftc, ttc, dep_date: str,
            prices: dict, seats: dict, code: str = "", km=None,
            duration_min: int | None = None, cal: dict | None = None,
            sno: dict | None = None, dep_hm: str = "", arr_hm: str = "",
            tk_aux: dict | None = None, style_map: dict | None = None,
            stop_names: list[str] | None = None) -> dict:
    if style_map is None:
        style_map = _style_map()
    if sno:
        if fno is None and ftc:
            fno = sno.get((train_no, ftc))
        if tno is None and ttc:
            tno = sno.get((train_no, ttc))
    if fno is not None and tno is not None:
        fno_s, tno_s = str(fno).zfill(2), str(tno).zfill(2)
        leg["from_station_no"] = fno_s
        leg["to_station_no"] = tno_s
        p_items, p_date, p_stale = _pick_price_rec(prices.get((train_no, fno_s, tno_s)), dep_date)
        if p_items:
            leg["prices"] = p_items
            if p_date:
                leg["price_date"] = p_date
            if p_stale:
                leg["price_stale"] = True

    live_tn = None
    ex_flag = ""
    dw_flag = ""
    tk_tflag = ""
    live_rec = _resolve_live_ticket(
        train_no, code, ftc, ttc, dep_date, dep_hm, arr_hm, tk_aux, style_map=style_map
    ) if tk_aux else None
    if live_rec:
        live_tn, live_code, live_fno, live_tno, _st_tm, _ar_tm, seats_js, cwb, fetched_at, ex_flag, dw_flag, tk_tflag = live_rec
        leg["seats"] = json.loads(seats_js or "{}")
        leg["can_web_buy"] = cwb
        if fetched_at:
            leg["ticket_updated"] = fetched_at.replace("T", " ")[:16]
        if live_code and code and live_code != code:
            leg["live_train"] = live_code
        if live_tn and live_tn != train_no:
            leg["live_train_no"] = live_tn
            if "prices" not in leg:
                lf = live_fno if live_fno is not None else fno
                lt = live_tno if live_tno is not None else tno
                if lf is not None and lt is not None:
                    p2_items, p2_date, p2_stale = _pick_price_rec(
                        prices.get((live_tn, str(lf).zfill(2), str(lt).zfill(2))), dep_date
                    )
                    if p2_items:
                        leg["prices"] = p2_items
                        if p2_date:
                            leg["price_date"] = p2_date
                        if p2_stale:
                            leg["price_stale"] = True
    else:
        tk = seats.get((train_no, ftc, ttc, dep_date))
        if tk:
            seats_js, cwb, fetched_at, ex_flag, dw_flag, tk_tflag = tk
            leg["seats"] = json.loads(seats_js or "{}")
            leg["can_web_buy"] = cwb
            if fetched_at:
                leg["ticket_updated"] = fetched_at.replace("T", " ")[:16]

    st, scr_flag = _lookup_train_meta(train_no, live_tn, code, style_map=style_map)
    if st:
        leg["train_style"] = st
    feat = parsers.extract_train_features(
        train_style=st,
        dw_flag=dw_flag,
        exchange_train_flag=ex_flag,
        train_flag=tk_tflag or scr_flag,
    )
    if feat["is_smart"]:
        leg["is_smart"] = True
    if feat["is_fuxing"]:
        leg["is_fuxing"] = True
    if feat["tags"]:
        leg["train_tags"] = feat["tags"]

    seq_stops = stop_names or _lookup_stop_names(train_no, live_tn, code)
    rl = rail_lines.infer_leg_lines(
        leg.get("from") or "",
        leg.get("to") or "",
        train_code=code or leg.get("train") or "",
        train_style=st,
        stop_names=seq_stops,
    )
    if rl:
        leg["rail_lines"] = rl

    if "prices" not in leg and code:
        est = fare.estimate(code, km=km, duration_min=duration_min, cal=cal, style=st)
        if est:
            leg["price_est"] = est
    return leg


# ---------- 引擎 A：车站大屏（真实日期优先，无当日数据时按每日开行假设平移） ----------
def _load_screen(train_filter: set[str] | None = None, cancel_event=None) -> dict:
    trains = {}
    conn = db.get_conn()
    cols = (
        "station_telecode,station_name,station_train_code,train_no,"
        "station_train_date,start_train_date,station_no,arrive_time,start_time,"
        "start_station_name,start_station_telecode,end_station_name,end_station_telecode,"
        "running_fig,start_start_time,end_arrive_time,running_time,distance,"
        "arrive_day_diff,start_day_diff"
    )
    if train_filter is not None:
        if not train_filter:
            return trains
        if len(train_filter) <= 500:
            ph = ",".join("?" * len(train_filter))
            recs = conn.execute(
                f"SELECT {cols} FROM station_trains WHERE train_no IN ({ph})",
                tuple(train_filter),
            ).fetchall()
        else:
            recs = [
                r for r in conn.execute(f"SELECT {cols} FROM station_trains")
                if r[3] in train_filter
            ]
    else:
        recs = conn.execute(f"SELECT {cols} FROM station_trains").fetchall()

    for r_idx, (
        stc, sname, st_code, tn, st_td, start_td, sno_val, arr_str, dep_str,
        s_name, s_tc, e_name, e_tc, rfig, ss_str, ea_str, run_str, dist_val,
        arr_diff, dep_diff
    ) in enumerate(recs):
        if (r_idx & 2047) == 0:
            _check_cancel(cancel_event)
        tn = _istr(tn)
        if not tn:
            continue
        # 同一 train_no 可能对应多个开行日期/往返班次，按开行实例分组
        run = (tn, start_td or st_td)
        sd = _date_min(start_td)
        sd_min = sd or _date_min(st_td)
        t = trains.setdefault(run, {"code": _istr(st_code), "obs": {}, "running_figs": set(),
                                    "source_date": st_td, "sd_min": sd_min})
        if t.get("sd_min") is None and sd_min is not None:
            t["sd_min"] = sd_min
        if rfig:
            t["running_figs"].add(rfig)
        t["start_key"] = _istr(s_tc or s_name)
        t["end_key"] = _istr(e_tc or e_name)
        t["start_name"] = _istr(s_name)
        t["end_name"] = _istr(e_name)
        d = _date_min(st_td)
        if d is None:
            continue
        key = _istr(stc or sname)
        o = t["obs"].setdefault(key, {"name": _istr(sname), "telecode": _istr(stc),
                                      "no": sno_val, "code": _istr(st_code),
                                      "dist": _int(dist_val)})
        arr, dep = _minutes(arr_str), _minutes(dep_str)
        ad, dd = _int(arr_diff), _int(dep_diff)
        if arr is not None:
            o["arr"] = (sd + ad * 1440 + arr) if (sd is not None and ad is not None) else (d + arr)
        if dep is not None:
            o["dep"] = (sd + dd * 1440 + dep) if (sd is not None and dd is not None) else (d + dep)
        # 缺 day_diff 时若停站跨越午夜（如 23:55 到达 / 次日 00:05 发车），大屏日期通常对应发车日，
        # 仅对 >12h 倒挂回退 arr -= 1440，避免误伤 12306 终到站偶发几分钟早发晚到记录
        if (sd is None or ad is None or dd is None) and o.get("arr") is not None and o.get("dep") is not None:
            if o["arr"] - o["dep"] > 720:
                o["arr"] -= 1440
        # 始发/终到推导：running_time 为始发至本站的累计历时，
        # 终到历时 = 不小于该值且与 (end_arrive - start_start) 同余(mod 1440) 的最小值
        ss, run_m = _minutes(ss_str), _dur_min(run_str)
        ea = _minutes(ea_str)
        if sd is not None and ss is not None:
            so = t["obs"].setdefault(t["start_key"], {"name": t["start_name"],
                                                      "telecode": _istr(s_tc) or None, "no": 1})
            so.setdefault("dep", sd + ss)
            so.setdefault("name", t["start_name"])
            if run_m is not None and ea is not None:
                total_mod = (ea - ss) % 1440
                total = run_m + (total_mod - run_m) % 1440
                eo = t["obs"].setdefault(t["end_key"], {"name": t["end_name"],
                                                        "telecode": _istr(e_tc) or None, "no": None})
                eo.setdefault("arr", sd + ss + total)
                eo.setdefault("name", t["end_name"])
        no = sno_val
        if no is not None:
            if t.get("min_no") is None or no < t["min_no"]:
                t["min_no"], t["start_code"] = no, _istr(st_code)
            if t.get("max_no") is None or no > t["max_no"]:
                t["max_no"], t["end_code"] = no, _istr(st_code)

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


def _dep_keys(key, name, available, cities, exclude=frozenset(), allow_cross=True):
    """同站 + 同城异站的候选出发站键（兼容无地理索引时的按城市回退接口）。"""
    if not allow_cross:
        return [key]
    city_of, city_groups, tele_of = cities
    out = [key]
    c = city_of.get(key) or city_of.get(name or "")
    if c:
        ck = tele_of.get(key, key)
        out += [x for x in sorted(city_groups[c])
                if x in available and tele_of.get(x, x) != ck and tele_of.get(x, x) not in exclude]
    return out


def _dep_edges(key, name, available, cities, geo_index, cross_cfg,
               exclude=frozenset(), allow_cross=True, truncation_out=None):
    """Scheme C 异站候选与动态接驳参数列表 [(dk, edge_meta), ...]。"""
    return geo.cross_candidates(
        key, name, available, cities, geo_index, cross_cfg,
        exclude=exclude, allow_cross=allow_cross,
        truncation_out=truncation_out,
    )


def _transfer_edge_fields(em: dict, wait_min: int, same: bool) -> dict:
    """统一组装全透明异站/同站接驳账本字段（含兼容旧字段）。"""
    eff = 0 if same else int(em.get("effective_extra", em.get("extra_min", 0)))
    return {
        "base_extra": 0 if same else int(em.get("base_extra", eff)),
        "hub_extra": 0 if same else int(em.get("hub_extra", 0)),
        "transit_discount": 0 if same else int(em.get("transit_discount", em.get("transit_discount_min", 0))),
        "transit_discount_min": 0 if same else int(em.get("transit_discount_min", em.get("transit_discount", 0))),
        "effective_extra": eff,
        "effective_cross_extra": eff,
        "min_buffer": wait_min if same else (wait_min - eff),
        "source_rule": "same" if same else (em.get("source_rule") or em.get("source") or "radius"),
        "cross_source": "same" if same else (em.get("source") or em.get("source_rule") or "radius"),
        "distance_km": 0.0 if same else em.get("distance_km"),
        "cross_distance_km": 0.0 if same else em.get("distance_km"),
        "rail_access_mode": "none" if same else (em.get("rail_access_mode") or "none"),
        "transit_reason": "" if same else (em.get("transit_reason") or ""),
        "transit_cities": [] if same else list(em.get("transit_cities") or []),
        "coord_confidence": 1.0 if same else float(em.get("coord_confidence", 0.0)),
        "is_fallback": False if same else bool(em.get("is_fallback", False)),
        "is_suspect": False if same else bool(em.get("is_suspect", False)),
    }


# ---------- 换乘树枝剪枝工具 ----------
PRUNE_PRESETS = {
    "unbounded": {"frontier_k": 0,  "max_total_min": 0,    "detour_ratio": 0.0, "prune_dominated": False},
    "relaxed":   {"frontier_k": 16, "max_total_min": 5760, "detour_ratio": 0.0, "prune_dominated": False},
    "balanced":  {"frontier_k": 8,  "max_total_min": 2880, "detour_ratio": 0.0, "prune_dominated": False},
    "strict":    {"frontier_k": 4,  "max_total_min": 2160, "detour_ratio": 2.0, "prune_dominated": True},
}
MAJOR_HUBS = (
    # 直辖市与省会/首府（31）
    "北京", "天津", "石家庄", "太原", "呼和浩特",
    "沈阳", "长春", "哈尔滨",
    "上海", "南京", "杭州", "合肥", "福州", "南昌", "济南",
    "郑州", "武汉", "长沙", "广州", "南宁", "海口",
    "重庆", "成都", "贵阳", "昆明", "拉萨",
    "西安", "兰州", "西宁", "银川", "乌鲁木齐",
    # 重要干线铁路枢纽（非省会，支持自定义增删）
    "徐州", "商丘", "阜阳", "蚌埠", "鹰潭", "株洲", "衡阳", "怀化", "襄阳",
    "深圳", "厦门", "青岛", "大连", "宁波", "温州", "苏州", "无锡",
    "桂林", "柳州", "洛阳", "宝鸡", "宜昌", "三明北",
)
MAJOR_HUB_TOKENS = [x.upper() for x in MAJOR_HUBS]
DETOUR_FLOOR_MIN = 480  # 绕行倍率保底时长（8小时），防止短途高铁线误杀普速与低价中转
SAFE_WAIT_MIN = 30      # 换乘稳妥度安全缓冲阈值（分钟）


def _leg_min_price(leg: dict) -> float | None:
    """获取单程最低参考票价（实测票价优先，否则取估算区间下限）。"""
    prices = leg.get("prices")
    if prices:
        vals = []
        for x in prices:
            v = x.get("price")
            if v is not None:
                fv = float(v)
                if fv > 0:
                    vals.append(fv)
        if vals:
            return min(vals)
    est = leg.get("price_est")
    if est and isinstance(est.get("seats"), dict):
        vals = []
        for x in est["seats"].values():
            v = x.get("min")
            if v is not None:
                fv = float(v)
                if fv > 0:
                    vals.append(fv)
        if vals:
            return min(vals)
    return None


def _legs_cost(legs: list[dict]) -> tuple[float | None, bool]:
    """计算多程累计最低参考价，返回 (总价, 是否全部已知)。"""
    total = 0.0
    for leg in legs:
        p = _leg_min_price(leg)
        if p is None:
            return (None, False)
        total += p
    return (total, True)


def _train_tier(legs: list[dict], style_map: dict | None = None) -> int:
    """缺价时按车型做保守分级：普速/混搭(0) < 动车(1) < 高铁/城际(2)。"""
    best = 0
    smap = style_map if style_map is not None else _style_map()
    for leg in legs:
        st = leg.get("train_style") or (smap.get(leg["train_no"]) if leg.get("train_no") else "")
        codes = _train_codes(leg.get("train"), is_emu=parsers.is_emu_style(st))
        if not codes:
            continue
        tier = 2 if any(c[0] in ("G", "C", "X") for c in codes) else (1 if any(c[0] == "D" for c in codes) else 0)
        best = max(best, tier)
    return best


def _transfer_safety(trs: list[dict], cross_extra: int) -> tuple[int, int]:
    """返回 (同站换乘次数, 最小有效换乘余量分钟数)，用于保护更稳妥的换乘方案。"""
    if not trs:
        return (0, 999999)
    same_cnt = sum(1 for t in trs if t.get("same", True))
    min_buf = min(
        (
            t.get("wait_min", 0)
            if t.get("same", True)
            else t.get("wait_min", 0) - t.get("effective_extra", t.get("effective_cross_extra", cross_extra))
        )
        for t in trs
    )
    return (same_cnt, min_buf)


def _hub_sig(trs: list[dict]) -> tuple:
    """提取中转枢纽路径标识（仅同中转站与同/异站属性完全一致才属同枢纽）。"""
    return tuple((t.get("telecode") or t.get("station") or "", bool(t.get("same", True))) for t in trs)


def _dominates(k_legs: int, k_start: int, k_arr: int, k_hubs: tuple,
               k_same: int, k_buf: int, k_cost: float | None, k_tier: int,
               p_legs: int, p_start: int, p_arr: int, p_hubs: tuple,
               p_same: int, p_buf: int, p_cost: float | None, p_tier: int,
               k_passed: frozenset | set | None = None,
               k_closed: frozenset | set | None = None,
               p_passed: frozenset | set | None = None,
               p_closed: frozenset | set | None = None) -> bool:
    """同级同枢纽支配判定：价格更低、换乘更稳妥、或路径已访问站/城集合未严格包络时不剪除。"""
    if k_legs > p_legs or k_start < p_start or k_arr > p_arr:
        return False
    if k_hubs != p_hubs:
        return False
    if k_same < p_same:
        return False
    if k_buf < SAFE_WAIT_MIN and p_buf > k_buf:
        return False
    # WHY: city_loop 与 loopback 剪枝依赖已访问站/离城集合；仅当优胜分支 k 禁用集是 p 的子集时，
    # k 才能合法扩展出 p 的所有后续后缀，保证开启支配剪枝后零漏解。
    if k_passed is not None and p_passed is not None and not (k_passed <= p_passed):
        return False
    if k_closed is not None and p_closed is not None and not (k_closed <= p_closed):
        return False
    if k_cost is not None and p_cost is not None:
        if k_cost > p_cost + 1e-6:
            return False
    elif k_tier > p_tier:
        return False
    return True


def _raw_leg_cost(train_no: str, code: str | None, fno, tno, ftc, ttc, dur_min: int,
                  prices: dict, sno: dict, cal: dict, cache: dict | None = None,
                  style_map: dict | None = None, dep_date: str | None = None) -> float | None:
    """快速提取或估算单程最低参考票价（用于剪枝阶段的价格保护与双路束宽）。"""
    if sno:
        if fno is None and ftc:
            fno = sno.get((train_no, ftc))
        if tno is None and ttc:
            tno = sno.get((train_no, ttc))
    if fno is not None and tno is not None:
        p_items, _, _ = _pick_price_rec(prices.get((train_no, str(fno).zfill(2), str(tno).zfill(2))), dep_date)
        if p_items:
            vals = [float(x["price"]) for x in p_items if x.get("price") is not None and float(x["price"]) > 0]
            if vals:
                return min(vals)
    if code and dur_min and dur_min > 0:
        smap = style_map if style_map is not None else _style_map()
        st = smap.get(train_no)
        ckey = (code, dur_min, st or "")
        if cache is not None and ckey in cache:
            return cache[ckey]
        est = fare.estimate(code, duration_min=dur_min, cal=cal, style=st)
        res = None
        if est and isinstance(est.get("seats"), dict):
            vals = [float(x["min"]) for x in est["seats"].values() if x.get("min") is not None and float(x["min"]) > 0]
            if vals:
                res = min(vals)
        if cache is not None:
            cache[ckey] = res
        return res
    return None


def _train_codes(code: str | None, is_emu: bool = False) -> frozenset[str]:
    """拆分合并车次号并扩展 ZNV↔ZTK 等价口径（如 K411 -> {'K411', 'V411'}），用于判重与过滤。"""
    return parsers.train_code_variants(code, is_emu=is_emu)


def _parse_tokens(s: str | None) -> list[str]:
    if not s:
        return []
    return [x.strip().upper() for x in re.split(r"[,，\s]+", str(s)) if x.strip()]


def _match_prefix(code: str | None, prefixes: list[str], is_emu: bool = False) -> bool:
    if not prefixes:
        return True
    parts = _train_codes(code, is_emu=is_emu)
    return any(part.startswith(px) for part in parts for px in prefixes)


def _match_station_tokens(key, name: str | None, city_of: dict, tele_of: dict, tokens: list[str]) -> bool:
    """精确匹配车站电报码、车站名或城市名，不做子串扩散（面向专业用户，避免‘北京南’扩成全北京）。"""
    if not tokens:
        return False
    tc = str(tele_of.get(key, key) or "").upper()
    nm = str(name or key or "").strip().upper()
    nm_bare = nm[:-1] if nm.endswith("站") and len(nm) > 1 else nm
    ct_raw = str(city_of.get(key) or city_of.get(name or "") or "").strip().upper()
    ct_norm = ct_raw[:-1] if ct_raw.endswith("市") and len(ct_raw) > 1 else ct_raw
    for tok in tokens:
        if tok == tc:
            return True
        tok_bare = tok[:-1] if tok.endswith("站") and len(tok) > 1 else tok
        if nm and (tok == nm or (tok != ct_raw and tok_bare == nm_bare)):
            return True
        if ct_norm:
            if tok == ct_raw or tok == ct_norm:
                return True
            if tok.endswith("市") and len(tok) > 1:
                is_known_station = (tok in tele_of) or (tok_bare in tele_of) or (nm == tok)
                if not is_known_station and tok[:-1] == ct_norm:
                    return True
    return False


def _transfer_inc_ok(sk, sk_name: str | None, dk, dk_name: str | None,
                     city_of: dict, tele_of: dict, inc_st: list[str], inc_mode: str) -> bool:
    if not inc_st:
        return True
    if inc_mode == "and":
        return all(
            _match_station_tokens(sk, sk_name, city_of, tele_of, [tok])
            or _match_station_tokens(dk, dk_name, city_of, tele_of, [tok])
            for tok in inc_st
        )
    return (
        _match_station_tokens(sk, sk_name, city_of, tele_of, inc_st)
        or _match_station_tokens(dk, dk_name, city_of, tele_of, inc_st)
    )


def _city_of_key(key, name: str | None, city_of: dict, tele_of: dict) -> str:
    return city_of.get(key) or (city_of.get(name) if name else None) or tele_of.get(key, key)


def _code_allowed(code: str | None, inc_pfx: list[str], exc_pfx: list[str], is_emu: bool = False) -> bool:
    if inc_pfx and not _match_prefix(code, inc_pfx, is_emu=is_emu):
        return False
    if exc_pfx and _match_prefix(code, exc_pfx, is_emu=is_emu):
        return False
    return True


def _resolve_hub_tokens(hub_only: bool, hub_stations: str | list[str] | None) -> list[str]:
    if not hub_only:
        return []
    toks = _parse_tokens(hub_stations) if not isinstance(hub_stations, (list, tuple)) else [str(x).strip().upper() for x in hub_stations if str(x).strip()]
    return toks if toks else list(MAJOR_HUB_TOKENS)


def _parse_province_tokens(val) -> set[str]:
    if not val:
        return set()
    items = val if isinstance(val, (list, tuple, set)) else _parse_tokens(str(val))
    out = set()
    for it in items:
        p = china_geo.normalize_province(str(it))
        if p:
            out.add(p)
    return out


def _build_prov_map(cities: tuple, geo_index=None) -> dict[str, str]:
    city_of, _, tele_of = cities
    # WHY: StationGeoIndex 将经纬度保存在 coords/all_coords -> (lat, lon)，此前读取不存在的 lat_of/lon_of 导致坐标边界回退失效。
    coords = getattr(geo_index, "coords", {}) if geo_index is not None else {}
    all_coords = getattr(geo_index, "all_coords", {}) if geo_index is not None else {}

    def _coord(k: str | None) -> tuple[float | None, float | None]:
        if not k:
            return (None, None)
        pt = coords.get(k) or all_coords.get(k)
        return (pt[0], pt[1]) if pt else (None, None)

    out: dict[str, str] = {}
    for nm, tc in tele_of.items():
        ct = city_of.get(tc) or city_of.get(nm) or ""
        lat, lon = _coord(tc)
        p = china_geo.infer_station_province(nm, ct, lat, lon)
        if p:
            out[tc] = p
            out[nm] = p
    for key, ct in city_of.items():
        if key not in out and ct:
            lat, lon = _coord(key)
            p = china_geo.infer_station_province(key, ct, lat, lon)
            if p:
                out[key] = p
    return out


def _station_province(
    key: str | None,
    name: str | None,
    city_of: dict,
    prov_of: dict | None = None,
    city_hint: str | None = None,
) -> str:
    if prov_of:
        p = (key and prov_of.get(key)) or (name and prov_of.get(name))
        if p:
            return p
    ct = (key and city_of.get(key)) or (name and city_of.get(name)) or city_hint or ""
    p = china_geo.infer_station_province(name or key, ct)
    if prov_of is not None and p:
        if key:
            prov_of[key] = p
        if name:
            prov_of[name] = p
    return p


def _prov_allowed(prov: str, allow_provs: set[str], exc_provs: set[str], block_unknown: bool = False) -> bool:
    if not allow_provs and not exc_provs and not block_unknown:
        return True
    if not prov:
        return not block_unknown
    if exc_provs and prov in exc_provs:
        return False
    if allow_provs and prov not in allow_provs:
        return False
    return True


def filter_plans(
    plans: list[dict],
    allow_prefixes: str | list[str] = "",
    exclude_prefixes: str | list[str] = "",
    include_stations: str | list[str] = "",
    exclude_stations: str | list[str] = "",
    include_stations_mode: str = "or",
    allow_cross_station: bool = True,
    cities: tuple | None = None,
    stats: dict | None = None,
    allow_provinces: str | list[str] | set[str] = "",
    exclude_provinces: str | list[str] | set[str] = "",
    prov_of: dict | None = None,
    block_unknown_prov: bool = False,
) -> list[dict]:
    """独立后置过滤器：按车型头、必经/排除换乘站（精确站名/城市+OR/AND）、同异站及中转省份过滤已生成的方案。"""
    inc_pfx = _parse_tokens(allow_prefixes) if not isinstance(allow_prefixes, (list, tuple)) else [str(x).strip().upper() for x in allow_prefixes if str(x).strip()]
    exc_pfx = _parse_tokens(exclude_prefixes) if not isinstance(exclude_prefixes, (list, tuple)) else [str(x).strip().upper() for x in exclude_prefixes if str(x).strip()]
    inc_st = _parse_tokens(include_stations) if not isinstance(include_stations, (list, tuple)) else [str(x).strip().upper() for x in include_stations if str(x).strip()]
    exc_st = _parse_tokens(exclude_stations) if not isinstance(exclude_stations, (list, tuple)) else [str(x).strip().upper() for x in exclude_stations if str(x).strip()]
    allow_provs = _parse_province_tokens(allow_provinces)
    exc_provs = _parse_province_tokens(exclude_provinces)
    prov_active = bool(allow_provs or exc_provs or block_unknown_prov)
    inc_mode = "and" if str(include_stations_mode or "or").strip().lower() == "and" else "or"
    if not inc_pfx and not exc_pfx and not inc_st and not exc_st and allow_cross_station and not prov_active:
        return list(plans)

    if cities is None:
        city_of, tele_of = {}, {}
        for tc, nm, ct in db.get_conn().execute("SELECT telecode,name,city FROM stations"):
            tele_of[nm] = tc
            if ct:
                city_of[tc] = ct
                city_of[nm] = ct
    else:
        city_of, _, tele_of = cities

    smap = _style_map() if (inc_pfx or exc_pfx) else {}

    def _leg_pfx_ok(l: dict) -> bool:
        st = l.get("train_style") or (smap.get(l["train_no"]) if l.get("train_no") else "")
        emu = parsers.is_emu_style(st)
        c_raw = l.get("train")
        c_live = l.get("live_train") or l.get("ticket_live")
        if inc_pfx and not (_match_prefix(c_raw, inc_pfx, is_emu=emu) or (c_live and _match_prefix(c_live, inc_pfx, is_emu=emu))):
            return False
        if exc_pfx and (_match_prefix(c_raw, exc_pfx, is_emu=emu) or (c_live and _match_prefix(c_live, exc_pfx, is_emu=emu))):
            return False
        return True

    out = []
    for p in plans:
        legs = p.get("legs") or []
        if (inc_pfx or exc_pfx) and not all(_leg_pfx_ok(l) for l in legs):
            _add_prune(stats, "custom_rule")
            continue
        trs = p.get("transfers") or []
        if not trs and inc_st:
            _add_prune(stats, "custom_rule")
            continue
        if trs:
            if not allow_cross_station and any(not tr.get("same", True) for tr in trs):
                _add_prune(stats, "custom_rule")
                continue
            # 每段换乘对应前程下车站 (tr) 与后程上车站 (legs[idx+1].from)
            pairs = []
            for idx, tr in enumerate(trs):
                sk_name = tr.get("station")
                sk_key = tr.get("telecode") or sk_name
                if idx + 1 < len(legs):
                    nl = legs[idx + 1]
                    dk_name = nl.get("from") or tr.get("to_station") or sk_name
                    dk_key = nl.get("from_telecode") or nl.get("from") or tr.get("to_telecode") or tr.get("to_station") or sk_key
                else:
                    dk_name, dk_key = sk_name, sk_key
                pairs.append((sk_key, sk_name, dk_key, dk_name))
            if prov_active and any(
                not _prov_allowed(_station_province(sk, sk_n, city_of, prov_of), allow_provs, exc_provs, block_unknown=block_unknown_prov)
                or (dk != sk and not _prov_allowed(_station_province(dk, dk_n, city_of, prov_of), allow_provs, exc_provs, block_unknown=block_unknown_prov))
                for sk, sk_n, dk, dk_n in pairs
            ):
                _add_prune(stats, "province_filter")
                continue
            if exc_st and any(
                _match_station_tokens(sk, sk_n, city_of, tele_of, exc_st)
                or (dk != sk and _match_station_tokens(dk, dk_n, city_of, tele_of, exc_st))
                for sk, sk_n, dk, dk_n in pairs
            ):
                _add_prune(stats, "custom_rule")
                continue
            if inc_st:
                if inc_mode == "and":
                    ok_inc = all(
                        any(
                            _match_station_tokens(sk, sk_n, city_of, tele_of, [tok])
                            or _match_station_tokens(dk, dk_n, city_of, tele_of, [tok])
                            for sk, sk_n, dk, dk_n in pairs
                        )
                        for tok in inc_st
                    )
                else:
                    ok_inc = any(
                        _match_station_tokens(sk, sk_n, city_of, tele_of, inc_st)
                        or _match_station_tokens(dk, dk_n, city_of, tele_of, inc_st)
                        for sk, sk_n, dk, dk_n in pairs
                    )
                if not ok_inc:
                    _add_prune(stats, "custom_rule")
                    continue
        out.append(p)
    return out


def _add_prune(stats: dict | None, key: str, delta: int = 1):
    if stats is not None:
        stats[key] = stats.get(key, 0) + delta


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
                   prices, seats, cal, sno, cities, cross_extra=60, cancel_event=None,
                   prune_cfg=None, prune_stats=None, sub_cb=None, need_transfers=True,
                   geo_index=None, cross_cfg=None, truncation_out=None, tk_aux=None):
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
    max_total_min = prune_cfg.get("max_total_min", 2880)
    if cross_cfg is None:
        cross_cfg = geo.resolve_cross_cfg(cross_extra=cross_extra, geo_index=geo_index)

    style_map = _style_map()
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
    city_of, _, tele_of = cities
    name_of = {tc: nm for nm, tc in tele_of.items()}
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
            code=a.get("code") or t["code"], km=km, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno,
            dep_hm=_hm_of(a["dep"]), arr_hm=_hm_of(b["arr"]), tk_aux=tk_aux, style_map=style_map)

    direct, transfers = [], []
    sorted_fkeys = sorted(fkeys)
    n_at_from = len(at_from)
    for f_idx, tn in enumerate(at_from, 1):
        _check_cancel(cancel_event)
        if sub_cb is not None:
            sub_cb(f_idx, max(1, n_at_from),
                   f"计算大屏直达/换乘（{len(trains)} 班次）：始发 {f_idx}/{n_at_from}，候选 {len(direct) + len(transfers)} 个")
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
        a_emu = parsers.is_emu_style(style_map.get(tn[0]))
        for fo, shift in fo_cands:
            a_code = fo.get("code") or t["code"]
            a_codes = _train_codes(a_code, is_emu=a_emu)
            a_dep = fo["dep"] + shift
            assumed = (shift != 0) or bool(fo.get("bridged"))
            for _k, to in to_obs.get(tn, ()):
                if to["arr"] + shift > a_dep and _ordered(fo, to):
                    dur = to["arr"] - fo["dep"]
                    if max_total_min and dur > max_total_min:
                        _add_prune(prune_stats, "total_time")
                        continue
                    direct.append({"total_min": dur,
                                   "date_assumed": assumed or bool(to.get("bridged")),
                                   "source_date": t.get("source_date"),
                                   "legs": [leg(tn, t, fo, to, shift)]})
            if not need_transfers:
                continue
            before_from = set(fkeys)
            if fo.get("no") is not None:
                before_from |= {k2 for k2, o2 in t["obs"].items()
                                if o2.get("no") is not None and o2["no"] < fo["no"]}
            from_city = _city_of_key(fo.get("telecode") or fo.get("name"), fo.get("name"), city_of, tele_of)
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
                        _add_prune(prune_stats, "passed_dest")
                        continue  # 换乘前已越过到达站，属于折返
                    forbidden |= passed
                    passed_canon |= {tele_of.get(k2, k2) for k2 in passed}
                s_city = _city_of_key(sk, so.get("name"), city_of, tele_of)
                closed_cities = {from_city} if s_city != from_city else set()
                dep_edges = _dep_edges(sk, so.get("name"), available, cities, geo_index, cross_cfg,
                                       exclude=passed_canon, allow_cross=allow_cross,
                                       truncation_out=truncation_out)
                if hub_tokens:
                    hub_edges = [
                        (dk, em) for dk, em in dep_edges
                        if _match_station_tokens(sk, so.get("name"), city_of, tele_of, hub_tokens)
                        or _match_station_tokens(dk, so.get("name") if dk == sk else name_of.get(dk), city_of, tele_of, hub_tokens)
                    ]
                    if not hub_edges:
                        _add_prune(prune_stats, "hub_filter")
                        continue
                    dep_edges = hub_edges
                for dk, em in dep_edges:
                    same = em["same"]
                    pair_extra = em["extra_min"]
                    eff_min = min_wait if same else min_wait + pair_extra
                    dk_city = _city_of_key(dk, so.get("name") if same else name_of.get(dk), city_of, tele_of)
                    # WHY: 跨市异站接驳后(如 昆山南[苏州] → 安亭北[上海])，dk_city 成为合法的当前出发市；
                    # 允许在离城前停靠 dk_city 内其余站点，仅禁止回到其它严格已离开的前序城市或折回前站。
                    eff_closed_cities = closed_cities - {dk_city}
                    for bn in by_station.get(dk, ()):
                        if bn not in to_obs:
                            continue
                        b = trains[bn]
                        bs = b["obs"].get(dk)
                        if not bs or "dep" not in bs:
                            continue
                        b_code = bs.get("code") or b["code"]
                        b_emu = parsers.is_emu_style(style_map.get(bn[0]))
                        if bn[0] == tn[0] or (a_codes and (a_codes & _train_codes(b_code, is_emu=b_emu))):
                            _add_prune(prune_stats, "repeat_train")
                            continue
                        n = (a_arr_s + eff_min - bs["dep"] + 1439) // 1440
                        wait = bs["dep"] + n * 1440 - a_arr_s
                        if wait > max_wait:
                            continue
                        b_dep_date = _fmt_epoch(bs["dep"] + n * 1440)[:10]
                        if n != 0 and not _fig_valid(b, b_dep_date):
                            continue
                        b_dep = bs["dep"] + n * 1440
                        for tk2, bt in to_obs.get(bn, ()):
                            if tk2 == dk or tk2 in forbidden or not _ordered(bs, bt) or bt["arr"] + n * 1440 <= b_dep:
                                _add_prune(prune_stats, "loopback")
                                continue
                            # 第二程重新经过出发站/第一程走过的站 = 无意义折返
                            if bs.get("no") is not None and bt.get("no") is not None:
                                if any(k2 in forbidden and o2.get("no") is not None
                                       and bs["no"] < o2["no"] <= bt["no"] for k2, o2 in b["obs"].items()):
                                    _add_prune(prune_stats, "loopback")
                                    continue
                                # 过头折返：第二程在目的站之后又折回第一程已走过的站 → 全程成环
                                if any(k2 in forbidden and o2.get("no") is not None
                                       and o2["no"] > bt["no"] for k2, o2 in b["obs"].items()):
                                    _add_prune(prune_stats, "loopback")
                                    continue
                                # 第二程途经已离开的前序城市非终点站，或离开接驳市后再绕回接驳市 → 绕圈折返
                                mid_obs = sorted(
                                    (
                                        (o2["no"], k2, o2)
                                        for k2, o2 in b["obs"].items()
                                        if o2.get("no") is not None and bs["no"] < o2["no"] < bt["no"]
                                    ),
                                    key=lambda item: item[0],
                                )
                                left_dk_city = False
                                looped_city = False
                                for _no2, k2, o2 in mid_obs:
                                    c2 = _city_of_key(k2, o2.get("name"), city_of, tele_of)
                                    if k2 not in tkeys and (
                                        c2 in eff_closed_cities or (left_dk_city and c2 == dk_city)
                                    ):
                                        looped_city = True
                                        break
                                    if c2 != dk_city:
                                        left_dk_city = True
                                if looped_city:
                                    _add_prune(prune_stats, "city_loop")
                                    continue
                            total_min = bt["arr"] + n * 1440 - a_dep
                            if max_total_min and total_min > max_total_min:
                                _add_prune(prune_stats, "total_time")
                                continue
                            is_assumed = (assumed or n != 0 or bool(so.get("bridged"))
                                          or bool(bs.get("bridged")) or bool(bt.get("bridged")))
                            ef = _transfer_edge_fields(em, wait, same)
                            transfers.append({
                                "total_min": total_min, "wait_min": wait,
                                "transfer_station": so.get("name") or sk, "same_station": same,
                                **ef,
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


def _load_trains(train_filter: set[str] | None = None, cancel_event=None, sub_cb=None) -> list[Train]:
    conn = db.get_conn()
    if train_filter is not None:
        if not train_filter:
            return []
        if len(train_filter) <= 500:
            ph = ",".join("?" * len(train_filter))
            recs = conn.execute(
                f"""SELECT train_no, station_train_code, train_date, station_no, station_name,
                           telecode, arrive_time, start_time, enabled
                    FROM train_stops WHERE train_no IN ({ph})
                    ORDER BY train_no, train_date DESC, station_no""",
                tuple(train_filter),
            ).fetchall()
        else:
            recs = [
                r for r in conn.execute(
                    """SELECT train_no, station_train_code, train_date, station_no, station_name,
                              telecode, arrive_time, start_time, enabled
                       FROM train_stops ORDER BY train_no, train_date DESC, station_no"""
                )
                if r[0] in train_filter
            ]
    else:
        recs = conn.execute(
            """SELECT train_no, station_train_code, train_date, station_no, station_name,
                      telecode, arrive_time, start_time, enabled
               FROM train_stops ORDER BY train_no, train_date DESC, station_no"""
        ).fetchall()

    trains, seen = [], set()
    cur_no = cur_date = code = None
    day, prev = 0, None
    stops = []
    n_recs = len(recs)
    # 停站采用紧凑字典以兼顾构建速度与可读性；一次遍历完成分组、累计分钟计算与过滤
    for r_idx, (tn, st_code, td, sno, sname, tc, arr_s, dep_s, en) in enumerate(recs, 1):
        if (r_idx & 4095) == 0 or r_idx == n_recs:
            _check_cancel(cancel_event)
            if sub_cb is not None:
                sub_cb(r_idx, max(1, n_recs), f"加载大屏与时刻表缓存：解析停站 {r_idx}/{n_recs}")
        if tn != cur_no or td != cur_date:
            if len(stops) >= 2 and cur_no not in seen:
                seen.add(cur_no)
                trains.append(Train(cur_no, code, cur_date, stops))
            cur_no, cur_date, code = _istr(tn), td, _istr(st_code)
            stops = []
            day, prev = 0, None
        if cur_no in seen:
            continue
        arr, dep = _minutes(arr_s), _minutes(dep_s)
        if arr is None and dep is None:
            continue
        if arr is None:
            arr = dep
        arr_day = day
        if prev is not None and arr < prev:
            day += 1
            arr_day = day
        if dep is None:
            dep = arr
        elif dep < arr:
            day += 1
        prev = dep
        stops.append({
            "station_no": sno, "station_name": _istr(sname), "telecode": _istr(tc),
            "enabled": en, "arr": arr_day * 1440 + arr, "dep": day * 1440 + dep,
            "key": _istr(tc or sname),
        })
    if len(stops) >= 2 and cur_no not in seen:
        trains.append(Train(cur_no, code, cur_date, stops))
    return trains


def _complete_screen_from_tt(screen: dict, tt: list) -> None:
    """用时刻表停站补全大屏班次的缺失经停，使按站序的折返/越过剪枝对残缺班次同样生效。

    残缺班次通常来自另一张运行图的 train_no 后缀（如 Z126 的 0J/0K），按 train_no 找不到基准，
    再退到「同车次码且同起讫站」这一方向安全的组合：同一物理车次途中改号（Z126/Z127、Z125/Z128）
    或跨图期都能命中，而反向车次的停站表不会被错贴。
    """
    by_no: dict[str, object] = {}
    by_route: dict[tuple, object] = {}
    for t in tt:
        if len(t.stops) < 2:
            continue
        route = (t.code, t.stops[0]["station_name"], t.stops[-1]["station_name"])
        for table, key in ((by_no, t.train_no), (by_route, route)):
            cur = table.get(key)
            if cur is None or len(t.stops) > len(cur.stops):
                table[key] = t
    if not by_no:
        return
    for run, t in screen.items():
        sd = t.get("sd_min")
        if sd is None or not t.get("obs"):
            continue
        src = by_no.get(run[0])
        if src is None:
            src = by_route.get((t.get("code"), t.get("start_name"), t.get("end_name")))
        if src is None:
            continue
        # WHY: 大屏与 train_stops 增量抓取经常跨越 7 天调图周期或无 date 字段；by_no 优先精准 train_no、
        # by_route 已按 (code, start_name, end_name) 同向锁定，按相对运行分钟 sd + s["arr"] 补全不受 7 天限制。
        obs = t["obs"]
        for s in src.stops:
            key = s["key"]
            if key in obs:
                continue
            obs[key] = {"name": s["station_name"], "telecode": s["telecode"], "no": s["station_no"],
                        "arr": sd + s["arr"], "dep": sd + s["dep"], "bridged": True}


def _search_timetable(trains, fkeys, tkeys, base, min_wait, max_wait, prices, seats, cal, sno, cities,
                      cross_extra=60, cancel_event=None, prune_cfg=None, prune_stats=None,
                      sub_cb=None, sub_cap: int = 100, real_tk=None, n2c=None, init_best_min=None,
                      need_transfers=True, geo_index=None, cross_cfg=None, truncation_out=None,
                      tk_aux=None):
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
    max_total_min = prune_cfg.get("max_total_min", 2880) or 0
    eff_detour = float(prune_cfg.get("detour_ratio") or 0.0)
    eff_dom = bool(prune_cfg.get("prune_dominated", False))
    # WHY: 车次头/必经排除站/省份过滤在 search() 后置 filter_plans 才生效；若过滤前就把违禁高速车计入 best_min
    # 或用其支配淘汰合规普速车，会造成“过滤后合法方案反而被删空”。故启用后置规则时延后到 filter_plans 后裁决。
    has_post_filter = bool(
        prune_cfg.get("inc_pfx") or prune_cfg.get("exc_pfx")
        or prune_cfg.get("inc_st") or prune_cfg.get("exc_st")
        or prune_cfg.get("allow_provs") or prune_cfg.get("exc_provs")
        or prune_cfg.get("block_unknown_prov")
    )
    if has_post_filter:
        eff_detour = 0.0
        eff_dom = False
    if cross_cfg is None:
        cross_cfg = geo.resolve_cross_cfg(cross_extra=cross_extra, geo_index=geo_index)
    real_tk = real_tk or {}
    n2c = n2c or {}
    style_map = _style_map()
    city_of, _, tele_of = cities
    name_of = {tc: nm for nm, tc in tele_of.items()}

    def _leg_conflict_raw(train_no, a, b, dep_date_str, code: str = ""):
        if not real_tk:
            return False
        ftc = a["telecode"] or n2c.get(a["station_name"])
        ttc = b["telecode"] or n2c.get(b["station_name"])
        if not ftc or not ttc:
            return False
        key = (ftc, ttc, dep_date_str)
        if key not in real_tk or train_no in real_tk[key]:
            return False
        if tk_aux and _resolve_live_ticket(
            train_no, code, ftc, ttc, dep_date_str, _hm_of(a["dep"]), _hm_of(b["arr"]), tk_aux, style_map=style_map
        ) is not None:
            return False
        return True

    # 预建站点、出发时间二分索引与停站元数据缓存，避免大循环内重复计算与 _enrich
    available = set()
    from_cands = []
    b_raw = {}
    meta_cache = {}

    def get_meta(T):
        m = meta_cache.get(T.train_no)
        if m is None:
            st_keys = [s["key"] for s in T.stops]
            st_canons = [tele_of.get(k, k) for k in st_keys]
            st_cities = [_city_of_key(s["key"], s["station_name"], city_of, tele_of) for s in T.stops]
            is_emu = parsers.is_emu_style(style_map.get(T.train_no))
            m = (_train_codes(T.code, is_emu=is_emu), st_keys, st_canons, st_cities)
            meta_cache[T.train_no] = m
        return m

    n_trains = len(trains)
    phase1_cap = max(1, (sub_cap * 15) // 100)
    for t_idx, T in enumerate(trains, 1):
        if (t_idx & 127) == 0 or t_idx == n_trains:
            _check_cancel(cancel_event)
            if sub_cb is not None:
                sub_cb(int(t_idx * phase1_cap / max(1, n_trains)), 100,
                       f"计算时刻表直达/换乘（{n_trains} 车次）：建索引 {t_idx}/{n_trains}")
        fi, tj = [], []
        for idx_s, s in enumerate(T.stops):
            sk = s["key"]
            available.add(sk)
            if s["enabled"]:
                if sk in fkeys:
                    fi.append(idx_s)
                if sk in tkeys:
                    tj.append(idx_s)
        if not fi and not tj:
            continue
        if fi:
            from_cands.append((T, fi, tj))
        if need_transfers and tj:
            get_meta(T)
            tj_set = set(tj)
            for k, s in enumerate(T.stops):
                if s["enabled"]:
                    b_raw.setdefault(s["key"], []).append((s["dep"] % 1440, T, k, tj, tj_set))

    b_at_station = {}
    b_dep_mods = {}
    if need_transfers:
        for dk, lst in b_raw.items():
            lst.sort(key=lambda x: x[0])
            b_at_station[dk] = [(x[1], x[2], x[3], x[4]) for x in lst]
            b_dep_mods[dk] = [x[0] for x in lst]
    del b_raw

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
            code=t.code, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno,
            dep_hm=_hm_of(a["dep"]), arr_hm=_hm_of(b["arr"]), tk_aux=tk_aux, style_map=style_map,
            stop_names=[s["station_name"] for s in t.stops[i : j + 1]])

    # 先算时刻表直达，立即得到最优直达用时用于绕行剪枝上界
    direct = []
    best_min = init_best_min if (init_best_min and init_best_min > 0 and not has_post_filter) else None
    for A, fi, tj in from_cands:
        if not tj:
            continue
        for i in fi:
            shift_a = -(A.stops[i]["dep"] // 1440) * 1440
            dep_date_a = _fmt(base, A.stops[i]["dep"] + shift_a)[:10]
            for j in tj:
                if j > i:
                    dur = A.stops[j]["arr"] - A.stops[i]["dep"]
                    if dur <= 0:
                        continue
                    if max_total_min and dur > max_total_min:
                        _add_prune(prune_stats, "total_time")
                        continue
                    l = leg(A, i, j, shift_a)
                    direct.append({"total_min": dur, "date_assumed": True,
                                   "source_date": A.train_date, "legs": [l]})
                    if not _leg_conflict_raw(A.train_no, A.stops[i], A.stops[j], dep_date_a, A.code):
                        if best_min is None or dur < best_min:
                            best_min = dur

    raw_transfers = []
    if not need_transfers:
        if sub_cb is not None:
            sub_cb(sub_cap, 100,
                   f"计算时刻表直达（{n_trains} 车次）：完成，候选 {len(direct)} 个")
        return direct, []
    n_from = len(from_cands)
    phase2_span = max(1, sub_cap - phase1_cap)
    for a_idx, (A, fi, _tj) in enumerate(from_cands, 1):
        _check_cancel(cancel_event)
        if sub_cb is not None:
            sub_cb(phase1_cap + int(a_idx * phase2_span / max(1, n_from)), 100,
                   f"计算时刻表直达/换乘（{n_trains} 车次）：匹配始发 {a_idx}/{n_from}，候选 {len(direct) + len(raw_transfers)} 个")
        a_codes, a_keys, a_canons, a_cities = get_meta(A)
        n_a_stops = len(A.stops)
        for i in fi:
            shift_a = -(A.stops[i]["dep"] // 1440) * 1440
            a_dep = A.stops[i]["dep"] + shift_a
            dep_date_a = _fmt(base, a_dep)[:10]
            forbidden = {a_keys[i]}
            forbidden_canon = {a_canons[i]}
            seen_cities = {a_cities[i]}
            for s_idx in range(i + 1, n_a_stops):
                _check_cancel(cancel_event)
                S = A.stops[s_idx]
                sk = a_keys[s_idx]
                sc_city = a_cities[s_idx]
                if not S["enabled"]:
                    forbidden.add(sk)
                    forbidden_canon.add(a_canons[s_idx])
                    seen_cities.add(sc_city)
                    continue
                if sk in tkeys:
                    _add_prune(prune_stats, "passed_dest")
                    break  # 已到到达站，再往后换乘属于折返
                leg1_dur = (S["arr"] + shift_a) - a_dep
                if leg1_dur <= 0:
                    forbidden.add(sk)
                    forbidden_canon.add(a_canons[s_idx])
                    seen_cities.add(sc_city)
                    continue
                detour_cap = max(DETOUR_FLOOR_MIN, best_min * eff_detour) if (eff_detour > 0 and best_min) else None
                eff_cap = max_total_min if max_total_min else _INF
                if detour_cap and detour_cap < eff_cap:
                    eff_cap = detour_cap
                if leg1_dur + min_wait >= eff_cap:
                    # 后续停站时间通常只增不减，但这里只跳过当前过长段确保稳健
                    forbidden.add(sk)
                    forbidden_canon.add(a_canons[s_idx])
                    seen_cities.add(sc_city)
                    continue
                if _leg_conflict_raw(A.train_no, A.stops[i], S, dep_date_a, A.code):
                    forbidden.add(sk)
                    forbidden_canon.add(a_canons[s_idx])
                    seen_cities.add(sc_city)
                    continue

                closed_cities = seen_cities - {sc_city}
                a_arr_s = S["arr"] + shift_a
                dep_edges = _dep_edges(sk, S["station_name"], available, cities, geo_index, cross_cfg,
                                       exclude=forbidden_canon, allow_cross=allow_cross,
                                       truncation_out=truncation_out)
                if hub_tokens:
                    hub_edges = [
                        (dk, em) for dk, em in dep_edges
                        if _match_station_tokens(sk, S["station_name"], city_of, tele_of, hub_tokens)
                        or _match_station_tokens(dk, S["station_name"] if dk == sk else name_of.get(dk), city_of, tele_of, hub_tokens)
                    ]
                    if not hub_edges:
                        _add_prune(prune_stats, "hub_filter")
                        forbidden.add(sk)
                        forbidden_canon.add(a_canons[s_idx])
                        seen_cities.add(sc_city)
                        continue
                    dep_edges = hub_edges
                for dk, em in dep_edges:
                    same = em["same"]
                    pair_extra = em["extra_min"]
                    b_list = b_at_station.get(dk)
                    if not b_list:
                        continue
                    eff_min = min_wait if same else min_wait + pair_extra
                    span = max_wait - eff_min
                    if span < 0:
                        continue
                    dk_city = _city_of_key(dk, S["station_name"] if same else name_of.get(dk), city_of, tele_of)
                    # WHY: 允许跨市换乘(如 昆山南[苏州]→安亭北[上海])后合法途经或出离 dk_city，
                    # 仅保留真正已脱离的前序非接驳市作为绕城剪枝黑名单，并在离 dk_city 后重新禁入 dk_city。
                    eff_closed_cities = closed_cities - {dk_city}

                    # 二分定位符合 [a_arr_s + eff_min, a_arr_s + max_wait] 的第二程车次切片
                    if span >= 1439:
                        cand_bs = b_list
                    else:
                        deps = b_dep_mods[dk]
                        lo_mod = (a_arr_s + eff_min) % 1440
                        hi_mod = (a_arr_s + max_wait) % 1440
                        if lo_mod <= hi_mod:
                            l_i = bisect.bisect_left(deps, lo_mod)
                            r_i = bisect.bisect_right(deps, hi_mod)
                            cand_bs = b_list[l_i:r_i]
                        else:
                            l_i = bisect.bisect_left(deps, lo_mod)
                            r_i = bisect.bisect_right(deps, hi_mod)
                            cand_bs = b_list[l_i:] + b_list[:r_i]

                    for B, k, b_all_to, _b_to_set in cand_bs:
                        b_codes, b_keys, _b_canons, b_cities = meta_cache[B.train_no]
                        if B.train_no == A.train_no or (a_codes and (a_codes & b_codes)):
                            _add_prune(prune_stats, "repeat_train")
                            continue
                        b_to = [m for m in b_all_to if m > k]
                        if not b_to:
                            continue
                        dep_base = B.stops[k]["dep"]
                        n = (a_arr_s + eff_min - dep_base + 1439) // 1440
                        wait = dep_base + n * 1440 - a_arr_s
                        if wait > max_wait:
                            continue
                        b_shift = n * 1440
                        b_dep_date = _fmt(base, dep_base + b_shift)[:10] if real_tk else ""

                        # 从 k+1 到最后一个目标站单次前向扫描：O(L) 判定折返/同城折返/超时
                        left_dk_city = False
                        has_city_loop = False
                        b_to_ptr = 0
                        n_b_to = len(b_to)
                        next_m = b_to[0]
                        max_m = b_to[-1]
                        # 过头折返：第二程在最后一个目的站之后又折回第一程已走过的站（出发站/途经站）→ 全程成环
                        if any(b_keys[z] in forbidden
                               for z in range(max_m + 1, len(B.stops))
                               if B.stops[z]["enabled"]):
                            _add_prune(prune_stats, "loopback")
                            continue
                        detour_cap = max(DETOUR_FLOOR_MIN, best_min * eff_detour) if (eff_detour > 0 and best_min) else None
                        for y in range(k + 1, max_m + 1):
                            yk = b_keys[y]
                            if yk in forbidden or yk == dk:
                                # 后续所有目标停站 m >= y 都会经过此重复站，一并记录并剪断
                                rem_m = n_b_to - b_to_ptr
                                if rem_m > 0:
                                    _add_prune(prune_stats, "loopback", rem_m)
                                break
                            yc = b_cities[y]
                            if y == next_m:
                                if has_city_loop or (yc in eff_closed_cities and yk not in tkeys) or (left_dk_city and yc == dk_city and yk not in tkeys):
                                    _add_prune(prune_stats, "city_loop")
                                else:
                                    b_arr_abs = B.stops[y]["arr"] + b_shift
                                    if b_arr_abs <= dep_base + b_shift:
                                        pass
                                    else:
                                        total_min = b_arr_abs - a_dep
                                        if max_total_min and total_min > max_total_min:
                                            _add_prune(prune_stats, "total_time")
                                        elif detour_cap and total_min > detour_cap:
                                            _add_prune(prune_stats, "detour")
                                        elif not (real_tk and _leg_conflict_raw(B.train_no, B.stops[k], B.stops[y], b_dep_date, B.code)):
                                            raw_transfers.append((
                                                total_min, wait, a_dep, b_arr_abs,
                                                A, i, s_idx, shift_a, B, k, y, b_shift,
                                                S["station_name"], same, A.train_date or B.train_date,
                                                em,
                                            ))
                                            if best_min is None or total_min < best_min:
                                                best_min = total_min
                                                if eff_detour > 0:
                                                    detour_cap = max(DETOUR_FLOOR_MIN, best_min * eff_detour)
                                b_to_ptr += 1
                                if b_to_ptr >= n_b_to:
                                    break
                                next_m = b_to[b_to_ptr]
                            if yc != dk_city:
                                left_dk_city = True
                            if (yc in eff_closed_cities or (left_dk_city and yc == dk_city)) and yk not in tkeys:
                                has_city_loop = True

                forbidden.add(sk)
                forbidden_canon.add(a_canons[s_idx])
                seen_cities.add(sc_city)

    # 在构建完整票价/余票 dict（_enrich + fare.estimate）前先完成最终绕行筛选（含8小时保底）与同级同枢纽支配剪枝
    if eff_detour > 0 and best_min and raw_transfers:
        final_detour_limit = max(DETOUR_FLOOR_MIN, best_min * eff_detour)
        kept = []
        for rt in raw_transfers:
            if rt[0] > final_detour_limit:
                _add_prune(prune_stats, "detour")
            else:
                kept.append(rt)
        raw_transfers = kept

    if eff_dom and len(raw_transfers) > 1:
        est_cache = {}
        groups: dict[tuple, list] = {}
        for rt in raw_transfers:
            A, i, s_idx, B, k, y = rt[4], rt[5], rt[6], rt[8], rt[9], rt[10]
            groups.setdefault((A.stops[i]["station_name"], B.stops[y]["station_name"]), []).append(rt)
        kept = []
        for grp in groups.values():
            grp.sort(key=lambda r: (r[3], -r[2]))
            accepted = []
            for rt in grp:
                wait, p_start, p_arr, A, i, s_idx, _sa, B, k, y, _sb, t_sname, same, _sd, em = rt[1:]
                sa1, sa2 = A.stops[i], A.stops[s_idx]
                sb1, sb2 = B.stops[k], B.stops[y]
                c1 = _raw_leg_cost(A.train_no, A.code, sa1["station_no"], sa2["station_no"],
                                   sa1["telecode"], sa2["telecode"], sa2["arr"] - sa1["dep"],
                                   prices, sno, cal, est_cache, style_map=style_map)
                c2 = _raw_leg_cost(B.train_no, B.code, sb1["station_no"], sb2["station_no"],
                                   sb1["telecode"], sb2["telecode"], sb2["arr"] - sb1["dep"],
                                   prices, sno, cal, est_cache, style_map=style_map)
                p_cost = (c1 + c2) if (c1 is not None and c2 is not None) else None
                p_tier = _train_tier([
                    {"train": A.code, "train_style": style_map.get(A.train_no)},
                    {"train": B.code, "train_style": style_map.get(B.train_no)},
                ], style_map=style_map)
                p_hubs = ((sa2["telecode"] or t_sname or "", bool(same)),)
                p_same = 1 if same else 0
                p_buf = wait if same else wait - em["extra_min"]
                if any(
                    _dominates(
                        2, k_it["start"], k_it["arr"], k_it["hubs"],
                        k_it["same"], k_it["buf"], k_it["cost"], k_it["tier"],
                        2, p_start, p_arr, p_hubs,
                        p_same, p_buf, p_cost, p_tier,
                    )
                    for k_it in accepted
                ):
                    _add_prune(prune_stats, "dominated")
                    continue
                accepted.append({
                    "rt": rt, "start": p_start, "arr": p_arr, "hubs": p_hubs,
                    "same": p_same, "buf": p_buf, "cost": p_cost, "tier": p_tier,
                })
            kept.extend(x["rt"] for x in accepted)
        raw_transfers = kept

    transfers = []
    for total_min, wait, _ad, _ba, A, i, s_idx, shift_a, B, k, y, b_shift, t_sname, same, sdate, em in raw_transfers:
        ef = _transfer_edge_fields(em, wait, same)
        transfers.append({
            "total_min": total_min, "wait_min": wait,
            "transfer_station": t_sname, "same_station": same,
            **ef,
            "date_assumed": True,
            "source_date": sdate,
            "legs": [leg(A, i, s_idx, shift_a), leg(B, k, y, b_shift)],
        })

    if sub_cb is not None:
        sub_cb(sub_cap, 100,
               f"计算时刻表直达/换乘（{n_trains} 车次）：完成，候选 {len(direct) + len(transfers)} 个")
    return direct, transfers


# ---------- 多程换乘（3 程及以上，每日开行假设） ----------
MAX_TOTAL_MIN = 2880   # 总行程上限（分钟，48 小时），防止组合爆炸
FRONTIER_K = 8         # 每个站每层保留的最优到达状态数（剪枝）


def _load_services(screen: dict, tt: list, cancel_event=None) -> list[dict]:
    """把大屏与时刻表数据统一成“车次服务”，停站为相对始发的分钟数。"""
    services, seen = [], set()
    for t_idx, t in enumerate(tt):
        if (t_idx & 511) == 0:
            _check_cancel(cancel_event)
        if t.train_no in seen:
            continue
        seen.add(t.train_no)
        stops = [{"key": s["key"], "name": s["station_name"], "telecode": s["telecode"],
                  "no": s["station_no"], "dep": s["dep"], "arr": s["arr"],
                  "enabled": s.get("enabled", 1)} for s in t.stops]
        if len(stops) >= 2:
            services.append({"train_no": t.train_no, "code": t.code, "stops": stops})
    for r_idx, (run, t) in enumerate(screen.items()):
        if (r_idx & 511) == 0:
            _check_cancel(cancel_event)
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


def _norm_tree_tok(s: str | None) -> str:
    return str(s or "").strip().upper()


class TreePruneController:
    """多程 BFS 分支树实时监控与人工剪枝控制器（支持按省/城市/站/路径热砍与 step/live 无缝切换）。"""

    def __init__(self, pause_after_layer: bool = False, on_state_change=None, prov_lookup: dict | None = None):
        self.lock = threading.Lock()
        self.pause_after_layer = bool(pause_after_layer)
        self.paused = False
        self.paused_depth = 0
        self.resume_event = threading.Event()
        self.pruned_provinces: set[str] = set()
        self.pruned_cities: set[str] = set()
        self.pruned_stations: set[str] = set()
        self.pruned_paths: set[tuple[str, ...]] = set()
        self.snapshot: dict | None = None
        self.user_pruned_count = 0
        self.on_state_change = on_state_change
        self.prov_lookup: dict[str, str] = dict(prov_lookup) if prov_lookup else {}
        self._last_snap_ts = 0.0
        # WHY: 当分支数增至数千时单次 build_tree_snapshot 可能超过 0.35s，固定 0.35s 冷却会退化为
        # 每个前沿状态都全量重建双树（O(N^2) 雪崩）。动态冷却保持采样耗时占比 <=20%。
        self._live_cooldown = 0.35
        self._has_rules = False
        self._paused_frontier: list | None = None
        self._paused_max_legs = 3
        self._paused_chain_fn = None
        self._paused_build_fn = None
        self._paused_prune_stats = None

    def has_rules(self) -> bool:
        return self._has_rules

    def _refresh_has_rules_locked(self) -> None:
        self._has_rules = bool(
            self.pruned_provinces or self.pruned_cities or self.pruned_stations or self.pruned_paths
        )

    def _get_prov_locked(self, city: str | None, key: str | None, name: str | None) -> str:
        p = (key and self.prov_lookup.get(key)) or (name and self.prov_lookup.get(name)) or ""
        if not p:
            p = china_geo.infer_station_province(name, city)
            if p:
                if key:
                    self.prov_lookup[key] = p
                if name:
                    self.prov_lookup[name] = p
        return p

    def _hop_tokens(self, city: str | None, key: str | None, name: str | None) -> set[str]:
        toks = set()
        c = _norm_tree_tok(city)
        if c:
            toks.add(c)
            if c.endswith("市") and len(c) > 1:
                toks.add(c[:-1])
        k = _norm_tree_tok(key)
        if k:
            toks.add(k)
        n = _norm_tree_tok(name)
        if n:
            toks.add(n)
            if n.endswith("站") and len(n) > 1:
                toks.add(n[:-1])
        return toks

    def _single_hop_pruned_locked(self, city: str | None, key: str | None, name: str | None) -> bool:
        if self.pruned_provinces:
            p_eff = self._get_prov_locked(city, key, name) or "未识别省份"
            if p_eff in self.pruned_provinces:
                return True
        c = _norm_tree_tok(city)
        c_bare = c[:-1] if c.endswith("市") and len(c) > 1 else c
        if c and (c in self.pruned_cities or c_bare in self.pruned_cities):
            return True
        k = _norm_tree_tok(key)
        n = _norm_tree_tok(name)
        n_bare = n[:-1] if n.endswith("站") and len(n) > 1 else n
        if (k and k in self.pruned_stations) or (n and (n in self.pruned_stations or n_bare in self.pruned_stations)):
            return True
        return False

    def _is_chain_pruned_locked(self, chain: list[tuple]) -> bool:
        if not self._has_rules:
            return False
        hop_sets = []
        for item in chain:
            city, key, name = item[0], item[1], item[2]
            if self._single_hop_pruned_locked(city, key, name):
                return True
            extra_dep = (item[3], item[4], item[5]) if len(item) >= 6 and item[4] and item[4] != key else None
            if extra_dep and self._single_hop_pruned_locked(*extra_dep):
                return True
            if self.pruned_paths:
                hs = self._hop_tokens(city, key, name)
                if extra_dep:
                    hs |= self._hop_tokens(*extra_dep)
                hop_sets.append(hs)
        if self.pruned_paths and hop_sets:
            n_hops = len(hop_sets)
            for pp in self.pruned_paths:
                if len(pp) <= n_hops and all(pp[idx] in hop_sets[idx] for idx in range(len(pp))):
                    return True
        return False

    def is_expansion_pruned(self, prev_chain: list[tuple], city: str, key: str, name: str, count_prune: bool = False) -> bool:
        if not self._has_rules:
            return False
        with self.lock:
            if not self._has_rules:
                return False
            hit = self._is_chain_pruned_locked(prev_chain + [(city, key, name)])
            if hit and count_prune:
                self.user_pruned_count += 1
            return hit

    def is_cross_dep_pruned(self, prev_chain: list[tuple], dep_city: str, dep_key: str, dep_name: str, count_prune: bool = False) -> bool:
        if not self._has_rules:
            return False
        with self.lock:
            if not self._has_rules:
                return False
            if self._single_hop_pruned_locked(dep_city, dep_key, dep_name):
                if count_prune:
                    self.user_pruned_count += 1
                return True
            if self.pruned_paths and prev_chain:
                arr_last = prev_chain[-1]
                mod_last = (arr_last[0], arr_last[1], arr_last[2], dep_city, dep_key, dep_name)
                if self._is_chain_pruned_locked(prev_chain[:-1] + [mod_last]):
                    if count_prune:
                        self.user_pruned_count += 1
                    return True
            return False

    def should_sample_live(self, interval: float | None = None) -> bool:
        now = time.monotonic()
        with self.lock:
            cd = self._live_cooldown if interval is None else interval
            return (not self.pause_after_layer) and (now - self._last_snap_ts) >= cd

    def _prune_snapshot_nodes_locked(self, nodes: list[dict]) -> list[dict]:
        kept = []
        for nd in nodes:
            kind = nd.get("kind")
            if kind == "province":
                pv = china_geo.normalize_province(nd.get("province") or nd.get("label")) or "未识别省份"
                if pv in self.pruned_provinces:
                    continue
                new_nd = dict(nd)
                if nd.get("children"):
                    old_ch = nd["children"]
                    pruned_ch = self._prune_snapshot_nodes_locked(old_ch)
                    if not pruned_ch:
                        continue
                    new_nd["children"] = pruned_ch
                    new_sum = sum(int(c.get("count") or 0) for c in pruned_ch)
                    if nd.get("more_children"):
                        old_sum = sum(int(c.get("count") or 0) for c in old_ch)
                        new_nd["count"] = max(new_sum, int(nd.get("count") or 0) - max(0, old_sum - new_sum))
                    else:
                        new_nd["count"] = new_sum
                        el_vals = [c.get("min_elapsed_min") for c in pruned_ch if c.get("min_elapsed_min") is not None]
                        if el_vals:
                            new_nd["min_elapsed_min"] = min(el_vals)
                        c_vals = [c.get("min_cost") for c in pruned_ch if c.get("min_cost") is not None]
                        new_nd["min_cost"] = min(c_vals) if c_vals else None
                kept.append(new_nd)
                continue
            pv_nd = china_geo.normalize_province(nd.get("province")) or "未识别省份"
            pv_cross = china_geo.normalize_province(nd.get("cross_province")) if nd.get("cross_province") else ""
            if pv_nd in self.pruned_provinces or (pv_cross and pv_cross in self.pruned_provinces):
                continue
            if kind == "station":
                c = _norm_tree_tok(nd.get("city"))
                c_bare = c[:-1] if c.endswith("市") and len(c) > 1 else c
                k = _norm_tree_tok(nd.get("station_key"))
                n = _norm_tree_tok(nd.get("label"))
                n_bare = n[:-1] if n.endswith("站") and len(n) > 1 else n
                if (c and (c in self.pruned_cities or c_bare in self.pruned_cities)) or (k and k in self.pruned_stations) or (n and (n in self.pruned_stations or n_bare in self.pruned_stations)):
                    continue
                pt_toks = tuple(_norm_tree_tok(x) for x in (nd.get("path") or ()) if _norm_tree_tok(x))
                if any(len(pp) <= len(pt_toks) and all(pp[idx] == pt_toks[idx] for idx in range(len(pp))) for pp in self.pruned_paths):
                    continue
                kept.append(nd)
            else:
                c = _norm_tree_tok(nd.get("city") or nd.get("label"))
                c_bare = c[:-1] if c.endswith("市") and len(c) > 1 else c
                cc = _norm_tree_tok(nd.get("cross_city"))
                cc_bare = cc[:-1] if cc.endswith("市") and len(cc) > 1 else cc
                if (c and (c in self.pruned_cities or c_bare in self.pruned_cities)) or (cc and (cc in self.pruned_cities or cc_bare in self.pruned_cities)):
                    continue
                if self.pruned_stations:
                    ck = _norm_tree_tok(nd.get("cross_station_key"))
                    cn = _norm_tree_tok(nd.get("cross_station_name"))
                    ak = _norm_tree_tok(nd.get("station_key"))
                    an = _norm_tree_tok(nd.get("station_name"))
                    if any(x and x in self.pruned_stations for x in (ck, cn, ak, an)):
                        continue
                pt_toks = tuple(_norm_tree_tok(x) for x in (nd.get("path") or ()) if _norm_tree_tok(x))
                if any(len(pp) <= len(pt_toks) and all(pp[idx] == pt_toks[idx] for idx in range(len(pp))) for pp in self.pruned_paths):
                    continue
                new_nd = dict(nd)
                if nd.get("children"):
                    old_ch = nd["children"]
                    pruned_ch = self._prune_snapshot_nodes_locked(old_ch)
                    if not pruned_ch:
                        continue
                    new_nd["children"] = pruned_ch
                    new_sum = sum(int(c.get("count") or 0) for c in pruned_ch)
                    if nd.get("more_children"):
                        old_sum = sum(int(c.get("count") or 0) for c in old_ch)
                        new_nd["count"] = max(new_sum, int(nd.get("count") or 0) - max(0, old_sum - new_sum))
                    else:
                        new_nd["count"] = new_sum
                        el_vals = [c.get("min_elapsed_min") for c in pruned_ch if c.get("min_elapsed_min") is not None]
                        if el_vals:
                            new_nd["min_elapsed_min"] = min(el_vals)
                        c_vals = [c.get("min_cost") for c in pruned_ch if c.get("min_cost") is not None]
                        new_nd["min_cost"] = min(c_vals) if c_vals else None
                kept.append(new_nd)
        return kept

    def filter_states(self, states: list, chain_fn, prune_stats: dict | None = None) -> list:
        with self.lock:
            if not self._has_rules:
                return states
            kept = []
            dropped = 0
            for st in states:
                if self._is_chain_pruned_locked(chain_fn(st)):
                    dropped += 1
                else:
                    kept.append(st)
            if dropped:
                self.user_pruned_count += dropped
        if dropped:
            _add_prune(prune_stats, "user_tree", dropped)
            gc.collect()
        return kept

    def apply_command(
        self,
        prune_cities=None,
        prune_stations=None,
        prune_paths=None,
        unprune_cities=None,
        unprune_stations=None,
        unprune_paths=None,
        action: str = "prune",
        chain_fn=None,
        prune_provinces=None,
        unprune_provinces=None,
    ) -> dict:
        dropped_now = 0
        with self.lock:
            for x in (prune_provinces or ()):
                p = china_geo.normalize_province(x)
                if p:
                    self.pruned_provinces.add(p)
            for x in (unprune_provinces or ()):
                p = china_geo.normalize_province(x)
                self.pruned_provinces.discard(p)
            for x in (prune_cities or ()):
                t = _norm_tree_tok(x)
                if t:
                    self.pruned_cities.add(t)
            for x in (prune_stations or ()):
                t = _norm_tree_tok(x)
                if t:
                    self.pruned_stations.add(t)
            for p in (prune_paths or ()):
                if isinstance(p, (list, tuple)):
                    pt = tuple(_norm_tree_tok(x) for x in p if _norm_tree_tok(x))
                    if pt:
                        self.pruned_paths.add(pt)
            for x in (unprune_cities or ()):
                t = _norm_tree_tok(x)
                if not t:
                    continue
                self.pruned_cities.discard(t)
                self.pruned_cities.discard(t[:-1] if t.endswith("市") and len(t) > 1 else t + "市")
            for x in (unprune_stations or ()):
                t = _norm_tree_tok(x)
                if not t:
                    continue
                self.pruned_stations.discard(t)
                self.pruned_stations.discard(t[:-1] if t.endswith("站") and len(t) > 1 else t + "站")
            for p in (unprune_paths or ()):
                if isinstance(p, (list, tuple)):
                    pt = tuple(_norm_tree_tok(x) for x in p if _norm_tree_tok(x))
                    if pt:
                        self.pruned_paths.discard(pt)
            self._refresh_has_rules_locked()

            eff_chain_fn = chain_fn or self._paused_chain_fn
            if self.paused and self._paused_frontier is not None and eff_chain_fn is not None:
                if self._has_rules:
                    kept = []
                    for st in self._paused_frontier:
                        if self._is_chain_pruned_locked(eff_chain_fn(st)):
                            dropped_now += 1
                        else:
                            kept.append(st)
                    if dropped_now:
                        self._paused_frontier[:] = kept
                        self.user_pruned_count += dropped_now
                        if self._paused_prune_stats is not None:
                            _add_prune(self._paused_prune_stats, "user_tree", dropped_now)
                        if self._paused_build_fn is not None:
                            self.snapshot = self._paused_build_fn(
                                self._paused_frontier, self.paused_depth, self._paused_max_legs, "paused"
                            )
            elif self.snapshot is not None and self._has_rules:
                # 实时(live)模式下一旦收到人工剪除指令，立即在当前快照树上同步剔除对应省/市/站/路径节点，并重算省汇总与总数
                new_nodes = self._prune_snapshot_nodes_locked(self.snapshot.get("nodes") or [])
                has_provs = "provinces" in self.snapshot
                new_provs = self._prune_snapshot_nodes_locked(self.snapshot.get("provinces") or [])
                prov_by_name = {
                    (china_geo.normalize_province(pn.get("province") or pn.get("label")) or "未识别省份"): pn
                    for pn in new_provs
                }
                new_psummary = []
                for ps in (self.snapshot.get("province_summary") or []):
                    pv_ps = china_geo.normalize_province(ps.get("province")) or "未识别省份"
                    if pv_ps in self.pruned_provinces:
                        continue
                    if has_provs:
                        pn = prov_by_name.get(pv_ps)
                        if pn is None:
                            continue
                        ps2 = dict(ps)
                        ps2["count"] = pn.get("count", ps.get("count", 0))
                        ps2["min_elapsed_min"] = pn.get("min_elapsed_min")
                        ps2["min_cost"] = pn.get("min_cost")
                        new_psummary.append(ps2)
                    else:
                        new_psummary.append(ps)
                if has_provs:
                    new_tot = sum(int(pn.get("count") or 0) for pn in new_provs)
                elif self.snapshot.get("province_summary"):
                    new_tot = sum(int(ps.get("count") or 0) for ps in new_psummary)
                elif not self.snapshot.get("more_top"):
                    new_tot = sum(int(n.get("count") or 0) for n in new_nodes)
                else:
                    new_tot = self.snapshot.get("total_states", 0)
                self.snapshot = {
                    **self.snapshot,
                    "nodes": new_nodes,
                    "provinces": new_provs,
                    "province_summary": new_psummary,
                    "total_states": new_tot,
                }

            act = (action or "prune").strip().lower()
            if act == "continue":
                self.resume_event.set()
            elif act == "continue_all":
                self.pause_after_layer = False
                self.resume_event.set()
            elif act == "pause":
                self.pause_after_layer = True
            snap = self.snapshot
            state_info = self.status_dict_locked()
        if dropped_now:
            gc.collect()
        if self.on_state_change is not None:
            self.on_state_change(state_info)
        return {"ok": True, "dropped_now": dropped_now, **state_info, "tree_snapshot": snap}

    def status_dict_locked(self) -> dict:
        return {
            "tree_paused": self.paused,
            "tree_paused_depth": self.paused_depth,
            "tree_pause_after_layer": self.pause_after_layer,
            "tree_pruned_count": self.user_pruned_count,
            "tree_pruned_rules": {
                "provinces": sorted(self.pruned_provinces),
                "cities": sorted(self.pruned_cities),
                "stations": sorted(self.pruned_stations),
                "paths": [list(p) for p in sorted(self.pruned_paths)],
            },
        }

    def get_status(self) -> dict:
        with self.lock:
            out = self.status_dict_locked()
            out["tree_snapshot"] = self.snapshot
            return out

    def update_live_snapshot(self, states: list, depth: int, max_legs: int, phase_tag: str, build_fn, force: bool = False):
        now = time.monotonic()
        with self.lock:
            if not force and (now - self._last_snap_ts) < self._live_cooldown:
                return
        # 锁外构建树快照，并在构建结束后再刷新 _last_snap_ts 与动态冷却，防止快照阻塞 HTTP 轮询且防止超时雪崩
        t0 = time.monotonic()
        snap = build_fn(states, depth, max_legs, phase_tag)
        dt = time.monotonic() - t0
        with self.lock:
            self.snapshot = snap
            self._last_snap_ts = time.monotonic()
            self._live_cooldown = max(0.5, min(8.0, dt * 4.0))

    def checkpoint(
        self,
        frontier: list,
        depth: int,
        max_legs: int,
        cancel_event,
        chain_fn,
        build_fn,
        prune_stats: dict | None = None,
    ) -> list:
        frontier = self.filter_states(frontier, chain_fn, prune_stats=prune_stats)
        with self.lock:
            self.snapshot = build_fn(frontier, depth, max_legs, "paused" if (self.pause_after_layer and frontier) else "running")
            should_pause = self.pause_after_layer and bool(frontier)
            if should_pause:
                self.paused = True
                self.paused_depth = depth
                self.resume_event.clear()
                self._paused_frontier = frontier
                self._paused_max_legs = max_legs
                self._paused_chain_fn = chain_fn
                self._paused_build_fn = build_fn
                self._paused_prune_stats = prune_stats
                info = self.status_dict_locked()
            else:
                info = None
        if should_pause:
            if self.on_state_change is not None and info is not None:
                self.on_state_change(info)
            try:
                while not self.resume_event.wait(0.1):
                    _check_cancel(cancel_event)
            finally:
                with self.lock:
                    self.paused = False
                    self.paused_depth = 0
                    self._paused_frontier = None
                    self._paused_chain_fn = None
                    self._paused_build_fn = None
                    self._paused_prune_stats = None
                    self.snapshot = build_fn(frontier, depth, max_legs, "running")
                    info_done = self.status_dict_locked()
                if self.on_state_change is not None:
                    self.on_state_change(info_done)
            frontier = self.filter_states(frontier, chain_fn, prune_stats=prune_stats)
        return frontier


def _search_multi(services, fkeys, tkeys, base_min, min_wait, max_wait, max_legs,
                  prices, seats, cal, sno, cities, real_tk=None, n2c=None,
                  cross_extra=60, frontier_k=FRONTIER_K,
                  cancel_event=None, depth_cb=None,
                  prune_cfg=None, prune_stats=None,
                  init_sub_cb=None, depth_sub_cb=None,
                  geo_index=None, cross_cfg=None, truncation_out=None,
                  result_limit: int = 0, total_out: list | None = None,
                  tree_ctrl: TreePruneController | None = None, tk_aux=None) -> list[dict]:
    """BFS 多程换乘，返回 3..max_legs 程的换乘方案（反向可达时间下界剪枝 + 二分时间窗 + Top-K 延迟丰富化 + 交互分支树）。"""
    if max_legs < 3:
        if total_out is not None:
            total_out[:] = [0]
        return []
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
    inc_pfx = prune_cfg.get("inc_pfx") or []
    exc_pfx = prune_cfg.get("exc_pfx") or []
    inc_st = prune_cfg.get("inc_st") or []
    inc_st_mode = prune_cfg.get("inc_st_mode", "or")
    exc_st = prune_cfg.get("exc_st") or []
    eff_detour = float(prune_cfg.get("detour_ratio") or 0.0)
    if cross_cfg is None:
        cross_cfg = geo.resolve_cross_cfg(cross_extra=cross_extra, geo_index=geo_index)
    # WHY: frontier_k==0 或 max_total_min==0 在 unbounded 预设中代表“无限制”，
    # 不可再用 `or MAX_TOTAL_MIN` 回退覆盖 0，否则无限制模式会隐式退化成 48 小时与束宽 8。
    raw_max_total = prune_cfg.get("max_total_min")
    max_total_min = MAX_TOTAL_MIN if raw_max_total is None else int(raw_max_total)
    prune_dominated = bool(prune_cfg.get("prune_dominated", False))
    detour_cap = prune_cfg.get("detour_cap")
    time_limit = max_total_min if max_total_min > 0 else _INF
    if detour_cap and detour_cap < time_limit:
        time_limit = int(detour_cap)
    has_fk = bool(frontier_k and frontier_k > 0)

    real_tk = real_tk or {}
    n2c = n2c or {}
    style_map = _style_map()
    city_of, _, tele_of = cities
    name_of = {tc: nm for nm, tc in tele_of.items()}
    seats_keys = set(seats)

    allow_provs = set(prune_cfg.get("allow_provs") or ())
    exc_provs = set(prune_cfg.get("exc_provs") or ())
    block_unknown_prov = bool(prune_cfg.get("block_unknown_prov", False))
    has_prov_filter = bool(allow_provs or exc_provs or block_unknown_prov)
    prov_of = dict(prune_cfg.get("prov_of") or ())
    if not prov_of and (has_prov_filter or tree_ctrl is not None):
        prov_of = _build_prov_map(cities, geo_index=geo_index)

    def _prov_key(k: str, nm: str, ct: str = "") -> str:
        return _station_province(k, nm, city_of, prov_of, city_hint=ct)

    def _prov_ok(k: str, nm: str) -> bool:
        if not has_prov_filter:
            return True
        return _prov_allowed(_prov_key(k, nm), allow_provs, exc_provs, block_unknown=block_unknown_prov)

    if tree_ctrl is not None and prov_of:
        with tree_ctrl.lock:
            tree_ctrl.prov_lookup.update(prov_of)

    # 1) 过滤有效车次（含车型前缀预筛选）并预计算停站元数据、分级与按发车时刻排序的站车倒排索引
    allowed_sv = []
    sv_meta = []       # (sv_codes, stop_keys, stop_canons, stop_cities, dest_indices, sv_tier)
    by_station_raw = {}
    n_sv = len(services)
    has_pfx_filter = bool(inc_pfx or exc_pfx)
    for si_raw, sv in enumerate(services):
        if (si_raw & 127) == 0 or si_raw + 1 == n_sv:
            _check_cancel(cancel_event)
            if init_sub_cb is not None:
                init_sub_cb(int((si_raw + 1) * 30 / max(1, n_sv)), 100,
                            f"构建多程路网（{n_sv} 车次）：建索引 {si_raw + 1}/{n_sv}")
        is_emu = parsers.is_emu_style(style_map.get(sv["train_no"]))
        if has_pfx_filter and not _code_allowed(sv.get("code"), inc_pfx, exc_pfx, is_emu=is_emu):
            _add_prune(prune_stats, "custom_rule")
            continue
        si = len(allowed_sv)
        allowed_sv.append(sv)
        stops = sv["stops"]
        s_keys = [st["key"] for st in stops]
        s_canons = [tele_of.get(k, k) for k in s_keys]
        s_cities = [_city_of_key(st["key"], st["name"], city_of, tele_of) for st in stops]
        dest_idx = tuple(idx for idx, st in enumerate(stops) if st["enabled"] and st["key"] in tkeys)
        sv_codes = _train_codes(sv.get("code"), is_emu=is_emu)
        sv_tier = 2 if any(c[0] in ("G", "C", "X") for c in sv_codes) else (1 if any(c[0] == "D" for c in sv_codes) else 0)
        sv_meta.append((sv_codes, s_keys, s_canons, s_cities, dest_idx, sv_tier))
        for i, st in enumerate(stops):
            if st["enabled"]:
                by_station_raw.setdefault(st["key"], []).append((st["dep"] % 1440, si, i))

    sv_tiers = [m[5] for m in sv_meta]

    available = set(by_station_raw)
    by_station = {}
    by_dep_mods = {}
    for dk, lst in by_station_raw.items():
        lst.sort(key=lambda x: x[0])
        by_station[dk] = [(x[1], x[2]) for x in lst]
        by_dep_mods[dk] = [x[0] for x in lst]
    del by_station_raw

    # 2) 计算精确反向可达性与可容许最短剩余时间下界 min_lb[rem]（1 <= rem < max_legs）
    #    WHY: Scheme C 支持跨行政区物理半径建边与站对/换乘组动态加时，原先按 city 字符串汇总的
    #    min_lb_city 既会把 200km 远郊车误当同城下界、也会漏掉跨市邻站(如 昆山南->安亭北)。
    #    此处改为按物理站点建立反向异站边 cross_in_edges[dk]=[(sk, extra_min)]，保证多程下界严格可容许。
    cross_in_edges: dict[str, list[tuple[str, int]]] = {}
    if allow_cross:
        for sk in available:
            for dk, em in _dep_edges(sk, name_of.get(sk), available, cities, geo_index, cross_cfg,
                                     allow_cross=True, truncation_out=truncation_out):
                if not em["same"]:
                    cross_in_edges.setdefault(dk, []).append((sk, em["extra_min"]))

    min_lb_direct: dict[int, dict[str, int]] = {}
    min_lb_cross: dict[int, dict[str, int]] = {}
    n_allowed = len(allowed_sv)

    # rem = 1: 至少需乘 1 程直达 tkeys
    edge1: dict[str, int] = {}
    for si, sv in enumerate(allowed_sv):
        dest_idx = sv_meta[si][4]
        if not dest_idx:
            continue
        stops = sv["stops"]
        s_keys = sv_meta[si][1]
        next_t_arr = None
        for idx in range(len(stops) - 1, -1, -1):
            st = stops[idx]
            if not st["enabled"]:
                continue
            k = s_keys[idx]
            if k in tkeys:
                next_t_arr = st["arr"]
            elif next_t_arr is not None:
                ride = next_t_arr - st["dep"]
                prev_e = edge1.get(k)
                if prev_e is None or ride < prev_e:
                    edge1[k] = ride

    d1_dir, d1_cross = {}, {}
    for dk, ride in edge1.items():
        w_same = min_wait + ride
        if dk not in d1_dir or w_same < d1_dir[dk]:
            d1_dir[dk] = w_same
    if allow_cross:
        for dk, w_same in d1_dir.items():
            for sk, extra_m in cross_in_edges.get(dk, ()):
                wc = w_same + extra_m
                prev_c = d1_cross.get(sk)
                if prev_c is None or wc < prev_c:
                    d1_cross[sk] = wc
    min_lb_direct[1] = d1_dir
    min_lb_cross[1] = d1_cross

    def lb_exact(rem: int, key: str) -> int:
        v = min_lb_direct[rem].get(key, _INF)
        if allow_cross:
            vc = min_lb_cross[rem].get(key, _INF)
            if vc < v:
                v = vc
        return v

    for rem in range(2, max_legs):
        edge_r: dict[str, int] = {}
        prev_dir = min_lb_direct[rem - 1]
        prev_cross = min_lb_cross[rem - 1]
        for si, sv in enumerate(allowed_sv):
            stops = sv["stops"]
            s_keys = sv_meta[si][1]
            best_tail = _INF
            for idx in range(len(stops) - 1, -1, -1):
                st = stops[idx]
                if not st["enabled"]:
                    continue
                k = s_keys[idx]
                if k in tkeys:
                    best_tail = _INF
                    continue
                if best_tail < _INF:
                    cost = best_tail - st["dep"]
                    prev_e = edge_r.get(k)
                    if prev_e is None or cost < prev_e:
                        edge_r[k] = cost
                lb_prev = prev_dir.get(k, _INF)
                if allow_cross:
                    vc = prev_cross.get(k, _INF)
                    if vc < lb_prev:
                        lb_prev = vc
                if lb_prev < _INF:
                    cand_tail = st["arr"] + lb_prev
                    if cand_tail < best_tail:
                        best_tail = cand_tail
        dr, cr = {}, {}
        for dk, cost in edge_r.items():
            w_same = min_wait + cost
            if dk not in dr or w_same < dr[dk]:
                dr[dk] = w_same
        if allow_cross:
            for dk, w_same in dr.items():
                for sk, extra_m in cross_in_edges.get(dk, ()):
                    wc = w_same + extra_m
                    prev_c = cr.get(sk)
                    if prev_c is None or wc < prev_c:
                        cr[sk] = wc
        min_lb_direct[rem] = dr
        min_lb_cross[rem] = cr

    def lb_at_depth(depth_cur: int, key: str) -> int:
        """从 depth_cur (1..max_legs-1) 出发完成总程数 >= 3 且 <= max_legs 的最短剩余时间下界。"""
        rem = max_legs - depth_cur
        if depth_cur == 1:
            best = _INF
            for r in range(2, rem + 1):
                v = lb_exact(r, key)
                if v < best:
                    best = v
            return best
        if rem == 1:
            return lb_exact(1, key)
        best = _INF
        for r in range(1, rem + 1):
            v = lb_exact(r, key)
            if v < best:
                best = v
        return best

    real_tk_ods = {(k[0], k[1]) for k in real_tk} if real_tk else set()
    seats_ods = {(k[1], k[2]) for k in seats_keys} if seats_keys else set()
    # 当 tk_aux 仅含同 digits/时刻兜底数据时，若 OD 在 real_tk_ods 或 seats_ods 中再格式化日期，避免热点内海量 _fmt_epoch
    tk_aux_ods = set(real_tk_ods) | seats_ods
    if tk_aux and tk_aux.get("by_sched"):
        for k_s in tk_aux["by_sched"]:
            tk_aux_ods.add((k_s[0], k_s[1]))

    def leg_conflict_raw(sv, a, b, dep_epoch: int) -> bool:
        if not real_tk:
            return False
        ftc = a["telecode"] or n2c.get(a["name"])
        ttc = b["telecode"] or n2c.get(b["name"])
        if not ftc or not ttc or (ftc, ttc) not in real_tk_ods:
            return False
        dep_date = _fmt_epoch(dep_epoch)[:10]
        key = (ftc, ttc, dep_date)
        if key not in real_tk or sv["train_no"] in real_tk[key]:
            return False
        if tk_aux and _resolve_live_ticket(
            sv["train_no"], sv.get("code"), ftc, ttc, dep_date,
            _hm_of(a["dep"]), _hm_of(b["arr"]), tk_aux, style_map=style_map
        ) is not None:
            return False
        return True

    def has_seat_raw(sv, a, b, dep_epoch: int) -> bool:
        if not seats_keys and not tk_aux:
            return False
        ftc, ttc = a["telecode"], b["telecode"]
        if (ftc, ttc) not in tk_aux_ods:
            return False
        dep_date = _fmt_epoch(dep_epoch)[:10]
        if (sv["train_no"], ftc, ttc, dep_date) in seats_keys:
            return True
        if tk_aux:
            return _resolve_live_ticket(
                sv["train_no"], sv.get("code"), ftc, ttc, dep_date,
                _hm_of(a["dep"]), _hm_of(b["arr"]), tk_aux, style_map=style_map
            ) is not None
        return False

    def make_leg_tuple(ref):
        si, i, j, n_shift = ref
        sv = allowed_sv[si]
        a, b = sv["stops"][i], sv["stops"][j]
        dep = base_min + a["dep"] + n_shift * 1440
        arr = base_min + b["arr"] + n_shift * 1440
        dep_date = _fmt_epoch(dep)[:10]
        return _enrich({
            "train": sv["code"], "train_no": sv["train_no"],
            "from": a["name"], "to": b["name"],
            "from_telecode": a["telecode"], "to_telecode": b["telecode"],
            "depart": _fmt_epoch(dep), "arrive": _fmt_epoch(arr),
            "duration_min": b["arr"] - a["dep"],
        }, sv["train_no"], a["no"], b["no"], a["telecode"], b["telecode"],
            dep_date, prices, seats, code=sv["code"],
            duration_min=b["arr"] - a["dep"], cal=cal, sno=sno,
            dep_hm=_hm_of(a["dep"]), arr_hm=_hm_of(b["arr"]), tk_aux=tk_aux, style_map=style_map,
            stop_names=[s["name"] for s in sv["stops"][i : j + 1]])

    est_cache = {}
    seg_cost_cache = {}
    has_price_keys = set(prices) if prices else set()

    def seg_leg_cost(si: int, ii: int, jj: int, dep_date: str = "") -> tuple[float | None, int]:
        ck = (si, ii, jj, dep_date)
        cached = seg_cost_cache.get(ck)
        if cached is not None:
            return cached
        sv = allowed_sv[si]
        sa, sb = sv["stops"][ii], sv["stops"][jj]
        c = _raw_leg_cost(sv["train_no"], sv.get("code"), sa["no"], sb["no"],
                          sa["telecode"], sb["telecode"], sb["arr"] - sa["dep"],
                          prices, sno, cal, est_cache, style_map=style_map, dep_date=dep_date or None)
        res = (c, sv_tiers[si])
        seg_cost_cache[ck] = res
        return res

    def refs_cost_tier(l_refs: tuple) -> tuple[float, bool, int]:
        tot, known, mx_tier = 0.0, True, 0
        for si, ii, jj, _n in l_refs:
            c, t_lv = seg_leg_cost(si, ii, jj)
            if t_lv > mx_tier:
                mx_tier = t_lv
            if c is None:
                known = False
            else:
                tot += c
        return (tot if known else float("inf")), known, mx_tier

    def tr_meta(tr_refs: tuple) -> tuple[tuple, int, int]:
        if not tr_refs:
            return ((), 0, 999999)
        hubs = tuple((tr[1] or tr[0] or "", bool(tr[2])) for tr in tr_refs)
        same_cnt = sum(1 for tr in tr_refs if tr[2])
        min_buf = min((tr[3] if tr[2] else tr[3] - tr[6]["extra_min"]) for tr in tr_refs)
        return hubs, same_cnt, min_buf

    def state_hub_chain(st: dict) -> list[tuple]:
        ch = []
        for tr in st["tr_refs"]:
            nm = tr[0] or tr[1] or ""
            tc = tr[1] or nm
            ct = _city_of_key(tc, nm, city_of, tele_of)
            if tr[2]:
                ch.append((ct, tc, nm, "", "", ""))
            else:
                d_tc = tr[4] or tr[5] or ""
                d_nm = tr[5] or tr[4] or ""
                d_ct = _city_of_key(d_tc, d_nm, city_of, tele_of)
                ch.append((ct, tc, nm, d_ct, d_tc, d_nm))
        ch.append((st["city"], st["station"], st["name"], "", "", ""))
        return ch

    def build_tree_snapshot(states: list, depth_cur: int, m_legs: int, phase_tag: str) -> dict:
        # 同时生成两套互补视图：
        # 1) provinces: 按「首程/当前落地省 -> 城市 -> 站」分层，顶层<=35省永不折叠百城，一眼看清方向并按省秒砍；
        # 2) nodes: 按「城市路径 -> 站」平铺视图 + province_summary 全省汇总条，兼顾原城市习惯。
        def _new_node():
            return {"count": 0, "min_elapsed": _INF, "min_cost": None, "samples": [], "children": {}, "province": ""}

        root = _new_node()
        prov_agg: dict[str, dict] = {}
        prov_root: dict[str, dict] = {}
        for st in states:
            ch = state_hub_chain(st)
            el = max(0, st["arr"] - st["start"])
            scost = st["cost"] if st["cost_known"] else None
            first_sv_code = allowed_sv[st["leg_refs"][-1][0]].get("code") or ""

            # 1) 省份汇总与「按当前落脚省 -> 城市路径 -> 车站」分层树
            last_ct, last_key, last_nm = ch[-1][0], ch[-1][1], ch[-1][2]
            cur_prov = _prov_key(last_key, last_nm, last_ct) or "未识别省份"
            pa = prov_agg.get(cur_prov)
            if pa is None:
                pa = prov_agg[cur_prov] = {"province": cur_prov, "count": 0, "min_elapsed": _INF, "min_cost": None, "samples": []}
            pa["count"] += 1
            if el < pa["min_elapsed"]:
                pa["min_elapsed"] = el
            if scost is not None and (pa["min_cost"] is None or scost < pa["min_cost"]):
                pa["min_cost"] = round(scost, 1)
            if first_sv_code and len(pa["samples"]) < 3 and first_sv_code not in pa["samples"]:
                pa["samples"].append(first_sv_code)

            p_top = prov_root.get(cur_prov)
            if p_top is None:
                p_top = prov_root[cur_prov] = {
                    **_new_node(),
                    "kind": "province", "province": cur_prov, "city": "", "station_key": "",
                    "label": cur_prov, "path": [],
                }
            p_top["count"] += 1
            if el < p_top["min_elapsed"]:
                p_top["min_elapsed"] = el
            if scost is not None and (p_top["min_cost"] is None or scost < p_top["min_cost"]):
                p_top["min_cost"] = round(scost, 1)
            if first_sv_code and len(p_top["samples"]) < 3 and first_sv_code not in p_top["samples"]:
                p_top["samples"].append(first_sv_code)

            # 将状态插入两棵树（城市平铺树 & 当前省份子树）
            for tree_top in (root, p_top):
                cur = tree_top
                if tree_top is root:
                    cur["count"] += 1
                    if el < cur["min_elapsed"]:
                        cur["min_elapsed"] = el
                path_acc = []
                n_ch = len(ch)
                for idx_h, item in enumerate(ch):
                    ct, key, nm = item[0], item[1], item[2]
                    d_ct, d_key, d_nm = (item[3], item[4], item[5]) if len(item) >= 6 else ("", "", "")
                    c_label = ct or nm or key
                    is_cross_hop = bool(d_key and d_key != key)
                    node_label = c_label
                    cross_prov = ""
                    hop_prov = _prov_key(key, nm, ct)
                    if is_cross_hop:
                        node_label = f"{c_label}（{nm or key}→{d_nm or d_key}）" if (not d_ct or d_ct == c_label) else f"{c_label}→{d_ct}（{nm or key}→{d_nm or d_key}）"
                        cross_prov = _prov_key(d_key, d_nm, d_ct)
                    path_acc.append(c_label)
                    c_key = ("city", node_label)
                    c_node = cur["children"].get(c_key)
                    if c_node is None:
                        c_node = {
                            **_new_node(),
                            "kind": "city", "city": c_label, "station_key": key if is_cross_hop else "",
                            "station_name": nm if is_cross_hop else "",
                            "cross_city": d_ct if is_cross_hop else "",
                            "cross_province": cross_prov,
                            "cross_station_key": d_key if is_cross_hop else "",
                            "cross_station_name": d_nm if is_cross_hop else "",
                            "label": node_label, "path": list(path_acc), "province": hop_prov,
                        }
                        cur["children"][c_key] = c_node
                    elif not c_node["province"] and hop_prov:
                        c_node["province"] = hop_prov
                    c_node["count"] += 1
                    if el < c_node["min_elapsed"]:
                        c_node["min_elapsed"] = el
                    if scost is not None and (c_node["min_cost"] is None or scost < c_node["min_cost"]):
                        c_node["min_cost"] = round(scost, 1)
                    if first_sv_code and len(c_node["samples"]) < 3 and first_sv_code not in c_node["samples"]:
                        c_node["samples"].append(first_sv_code)
                    if idx_h == n_ch - 1:
                        s_key = ("station", key)
                        s_node = c_node["children"].get(s_key)
                        if s_node is None:
                            s_node = {
                                **_new_node(),
                                "kind": "station", "city": c_label, "station_key": key,
                                "label": nm or key, "path": path_acc[:-1] + [key], "province": hop_prov,
                            }
                            c_node["children"][s_key] = s_node
                        s_node["count"] += 1
                        if el < s_node["min_elapsed"]:
                            s_node["min_elapsed"] = el
                        if scost is not None and (s_node["min_cost"] is None or scost < s_node["min_cost"]):
                            s_node["min_cost"] = round(scost, 1)
                        if first_sv_code and len(s_node["samples"]) < 3 and first_sv_code not in s_node["samples"]:
                            s_node["samples"].append(first_sv_code)
                    else:
                        cur = c_node

        def _serialize(node: dict, level: int, cap0: int = 45, cap_sub: int = 18) -> tuple[list[dict], int]:
            items = list(node["children"].values())
            items.sort(key=lambda x: (x["min_elapsed"], -x["count"], x["label"]))
            cap = cap0 if level == 0 else cap_sub
            shown = items[:cap]
            hidden = max(0, len(items) - cap)
            out_nodes = []
            for it in shown:
                ch_list, ch_more = _serialize(it, level + 1, cap0=cap0, cap_sub=cap_sub) if it["children"] else ([], 0)
                out_nodes.append({
                    "kind": it["kind"],
                    "province": it.get("province") or "",
                    "city": it["city"],
                    "station_key": it["station_key"],
                    "station_name": it.get("station_name") or "",
                    "cross_city": it.get("cross_city") or "",
                    "cross_province": it.get("cross_province") or "",
                    "cross_station_key": it.get("cross_station_key") or "",
                    "cross_station_name": it.get("cross_station_name") or "",
                    "label": it["label"],
                    "path": it["path"],
                    "count": it["count"],
                    "min_elapsed_min": it["min_elapsed"] if it["min_elapsed"] < _INF else None,
                    "min_cost": it["min_cost"],
                    "samples": it["samples"],
                    "children": ch_list,
                    "more_children": ch_more,
                })
            return out_nodes, hidden

        nodes, more_top = _serialize(root, 0, cap0=45, cap_sub=18)

        prov_nodes = []
        for p_item in sorted(
            prov_root.values(),
            key=lambda x: (1 if x["province"] == "未识别省份" else 0, x["min_elapsed"], -x["count"], x["province"]),
        ):
            p_ch, p_more = _serialize(p_item, 0, cap0=22, cap_sub=12)
            prov_nodes.append({
                "kind": "province",
                "province": p_item["province"],
                "city": "",
                "station_key": "",
                "label": p_item["province"],
                "path": [],
                "count": p_item["count"],
                "min_elapsed_min": p_item["min_elapsed"] if p_item["min_elapsed"] < _INF else None,
                "min_cost": p_item["min_cost"],
                "samples": p_item["samples"],
                "children": p_ch,
                "more_children": p_more,
            })

        prov_summary = [
            {
                "province": pa["province"],
                "count": pa["count"],
                "min_elapsed_min": pa["min_elapsed"] if pa["min_elapsed"] < _INF else None,
                "min_cost": pa["min_cost"],
                "samples": pa["samples"],
            }
            for pa in sorted(
                prov_agg.values(),
                key=lambda x: (1 if x["province"] == "未识别省份" else 0, x["min_elapsed"], -x["count"], x["province"]),
            )
        ]

        return {
            "depth": depth_cur,
            "max_legs": m_legs,
            "phase": phase_tag,
            "total_states": len(states),
            "nodes": nodes,
            "more_top": more_top,
            "provinces": prov_nodes,
            "province_summary": prov_summary,
        }

    # 内存优化：未开启支配剪枝时“入桶即截断”——每站只维护 fast/cheap 两维
    # top-frontier_k 有界列表，落选分支立即释放。与整层建完再排序截断结果完全一致：
    # 稳定排序的平局用插入计数器还原，交替选取至多取每维前 frontier_k 个。
    bounded = (not prune_dominated) and has_fk

    # 3) 初始状态（第 1 程）：结合可达下界剪掉死分支，并同步应用 frontier_k 入桶截断或支配压缩
    init = []
    init_buckets: dict[str, list] = {}
    fk_items = [(si, i) for fk in fkeys for si, i in by_station.get(fk, ())]
    n_fk = len(fk_items)
    for idx, (si, i) in enumerate(fk_items, 1):
        _check_cancel(cancel_event)
        if init_sub_cb is not None:
            init_sub_cb(30 + int(idx * 70 / max(1, n_fk)), 100,
                        f"构建多程路网（{n_allowed} 车次）：初始化首程 {idx}/{n_fk}，分支 {len(init_buckets) if bounded else len(init)} 个")
        sv = allowed_sv[si]
        stops = sv["stops"]
        sv_codes, s_keys, s_canons, s_cities, _, sv_tier = sv_meta[si]
        st = stops[i]
        if s_keys[i] not in fkeys:
            continue
        n0 = -(st["dep"] // 1440)
        dep0 = base_min + st["dep"] + n0 * 1440
        for j in range(i + 1, len(stops)):
            b = stops[j]
            if not b["enabled"]:
                continue
            bk = s_keys[j]
            if bk in fkeys:
                # WHY: 首程停靠出发站集内其余同城站(如 甲北→甲南)后立即下车再换乘属于同城内耗空转，
                # 剪除该落点但不能 break，以免漏掉同一列车继续出城(如 中一)的合法首程。
                _add_prune(prune_stats, "loopback")
                continue
            if bk in tkeys:
                _add_prune(prune_stats, "passed_dest")
                break  # 已到达终点站，无需再走 3 程以上绕行
            dur = b["arr"] - st["dep"]
            if dur <= 0:
                continue
            if time_limit < _INF and dur > time_limit:
                if max_total_min and dur > max_total_min:
                    _add_prune(prune_stats, "total_time")
                else:
                    _add_prune(prune_stats, "detour")
                continue
            b_city = s_cities[j]
            if has_prov_filter and not _prov_ok(bk, b["name"]):
                _add_prune(prune_stats, "province_filter")
                continue
            if exc_st and _match_station_tokens(bk, b["name"], city_of, tele_of, exc_st):
                _add_prune(prune_stats, "custom_rule")
                continue
            if tree_ctrl is not None and tree_ctrl.has_rules() and tree_ctrl.is_expansion_pruned([], b_city, bk, b["name"], count_prune=True):
                _add_prune(prune_stats, "user_tree")
                continue
            lb = lb_at_depth(1, bk)
            if lb >= _INF:
                continue
            if time_limit < _INF and dur + lb > time_limit:
                if max_total_min and dur + lb > max_total_min:
                    _add_prune(prune_stats, "total_time")
                else:
                    _add_prune(prune_stats, "detour")
                continue
            if hub_tokens:
                self_ok = _match_station_tokens(bk, b["name"], city_of, tele_of, hub_tokens)
                if not self_ok:
                    cross_ok = False
                    if allow_cross:
                        c_edges = _dep_edges(bk, b["name"], available, cities, geo_index, cross_cfg,
                                             exclude=frozenset(s_canons[i:j + 1]), allow_cross=True,
                                             truncation_out=truncation_out)
                        cross_ok = any(
                            not em2["same"] and _match_station_tokens(dk2, name_of.get(dk2), city_of, tele_of, hub_tokens)
                            for dk2, em2 in c_edges
                        )
                    if not cross_ok:
                        _add_prune(prune_stats, "hub_filter")
                        continue
            if leg_conflict_raw(sv, st, b, dep0):
                continue
            arr0 = base_min + b["arr"] + n0 * 1440
            passed = frozenset(s_canons[i:j + 1])
            closed_cities = frozenset(s_cities[i:j]) - frozenset((b_city,))
            c_leg0, _ = seg_leg_cost(si, i, j)
            c0 = c_leg0 if c_leg0 is not None else float("inf")
            c0_known = c_leg0 is not None
            st_obj = {
                "arr": arr0, "station": bk, "name": b["name"], "city": b_city,
                "to_tc": b["telecode"],
                "passed": passed, "closed_cities": closed_cities,
                "used_trains": frozenset((sv["train_no"],)), "used_codes": sv_codes,
                "leg_refs": ((si, i, j, n0),), "tr_refs": (),
                "start": dep0, "has_seats": has_seat_raw(sv, st, b, dep0),
                "cost": c0, "cost_known": c0_known, "tier": sv_tier,
                "hubs": (), "same_cnt": 0, "min_buf": 999999,
            }
            if bounded:
                buck = init_buckets.get(bk)
                if buck is None:
                    buck = init_buckets[bk] = [[], [], [], 0]
                buck[3] += 1
                cnt = buck[3]
                hs = 0 if st_obj["has_seats"] else 1
                elapsed0 = arr0 - dep0
                if cnt <= frontier_k:
                    buck[2].append(st_obj)
                elif cnt == frontier_k + 1:
                    buck[2] = None
                buck[0].append(((hs, arr0, elapsed0), cnt, st_obj))
                if len(buck[0]) > frontier_k:
                    buck[0].remove(max(buck[0]))
                buck[1].append(((hs, c0, arr0, elapsed0), cnt, st_obj))
                if len(buck[1]) > frontier_k:
                    buck[1].remove(max(buck[1]))
            else:
                init.append(st_obj)
        if tree_ctrl is not None and tree_ctrl.should_sample_live():
            if bounded:
                live_init = []
                for b_item in init_buckets.values():
                    if b_item[2] is not None:
                        live_init.extend(b_item[2])
                    else:
                        seen_c = set()
                        for fe in b_item[0]:
                            if fe[1] not in seen_c:
                                seen_c.add(fe[1])
                                live_init.append(fe[2])
            else:
                live_init = init
            tree_ctrl.update_live_snapshot(live_init, 1, max_legs, "running", build_tree_snapshot, force=True)

    if bounded:
        frontier = []
        for buck in init_buckets.values():
            n_ins = buck[3]
            if n_ins <= frontier_k:
                frontier.extend(buck[2])
                continue
            by_fast = sorted(buck[0])
            by_cheap = sorted(buck[1])
            chosen, seen_cnt = [], set()
            fi = ci = 0
            while len(chosen) < frontier_k and (fi < len(by_fast) or ci < len(by_cheap)):
                while fi < len(by_fast) and by_fast[fi][1] in seen_cnt:
                    fi += 1
                if fi < len(by_fast) and len(chosen) < frontier_k:
                    seen_cnt.add(by_fast[fi][1])
                    chosen.append(by_fast[fi][2])
                    fi += 1
                while ci < len(by_cheap) and by_cheap[ci][1] in seen_cnt:
                    ci += 1
                if ci < len(by_cheap) and len(chosen) < frontier_k:
                    seen_cnt.add(by_cheap[ci][1])
                    chosen.append(by_cheap[ci][2])
                    ci += 1
            _add_prune(prune_stats, "frontier", n_ins - len(chosen))
            frontier.extend(chosen)
        del init_buckets
    elif prune_dominated and len(init) > 1:
        by_st: dict[str, list] = {}
        for s in init:
            by_st.setdefault(s["station"], []).append(s)
        compacted = []
        for bucket in by_st.values():
            bucket.sort(key=lambda s: (s["arr"], -s["start"], s["cost"]))
            kept = []
            for s in bucket:
                s_cost = s["cost"] if s["cost_known"] else None
                if any(
                    _dominates(
                        1, k["start"], k["arr"], (), 0, 999999,
                        k["cost"] if k["cost_known"] else None, k["tier"],
                        1, s["start"], s["arr"], (), 0, 999999, s_cost, s["tier"],
                        k["passed"], k["closed_cities"], s["passed"], s["closed_cities"],
                    )
                    for k in kept
                ):
                    _add_prune(prune_stats, "dominated")
                    continue
                kept.append(s)
            compacted.extend(kept)
        frontier = compacted
    else:
        frontier = init
    del init

    if tree_ctrl is not None and frontier:
        frontier = tree_ctrl.checkpoint(
            frontier, 1, max_legs, cancel_event, state_hub_chain, build_tree_snapshot, prune_stats=prune_stats
        )

    # 4) BFS 逐层扩展（全程仅维护整数时间戳与紧凑引用，收敛后再丰富化）
    raw_results = []
    for depth in range(2, max_legs + 1):
        _check_cancel(cancel_event)
        if depth_cb is not None:
            depth_cb(depth, len(frontier))
        nxt = {}
        n_front = len(frontier)
        is_last = (depth == max_legs)
        span_all = max_wait
        for s_idx, state in enumerate(frontier, 1):
            _check_cancel(cancel_event)
            if depth_sub_cb is not None:
                depth_sub_cb(depth, s_idx, max(1, n_front), len(raw_results))
            if tree_ctrl is not None:
                if tree_ctrl.has_rules():
                    st_chain = state_hub_chain(state)
                    if tree_ctrl.is_expansion_pruned(st_chain[:-1], *st_chain[-1][:3], count_prune=True):
                        _add_prune(prune_stats, "user_tree")
                        continue
                else:
                    st_chain = None
                if tree_ctrl.should_sample_live():
                    if is_last:
                        if tree_ctrl.has_rules():
                            rem = [st for st in frontier[s_idx - 1:] if not tree_ctrl.is_expansion_pruned(state_hub_chain(st)[:-1], *state_hub_chain(st)[-1][:3])]
                        else:
                            rem = frontier[s_idx - 1:]
                        tree_ctrl.update_live_snapshot(rem, depth, max_legs, "running", build_tree_snapshot, force=True)
                    else:
                        live_list = []
                        if bounded:
                            for b_item in nxt.values():
                                if b_item[2] is not None:
                                    live_list.extend(b_item[2])
                                else:
                                    seen_c = set()
                                    for fe in b_item[0]:
                                        if fe[1] not in seen_c:
                                            seen_c.add(fe[1])
                                            live_list.append(fe[2])
                        else:
                            for b_lst in nxt.values():
                                live_list.extend(b_lst)
                        tree_ctrl.update_live_snapshot(
                            live_list if live_list else frontier, depth, max_legs, "running", build_tree_snapshot, force=True
                        )
            else:
                st_chain = None
            t = state["arr"]
            cur_elapsed = t - state["start"]
            if time_limit < _INF and cur_elapsed > time_limit:
                if max_total_min and cur_elapsed > max_total_min:
                    _add_prune(prune_stats, "total_time")
                else:
                    _add_prune(prune_stats, "detour")
                continue
            cur_city = state["city"]
            for dk, em in _dep_edges(state["station"], state["name"], available, cities,
                                     geo_index, cross_cfg, exclude=state["passed"], allow_cross=allow_cross,
                                     truncation_out=truncation_out):
                same = em["same"]
                pair_extra = em["extra_min"]
                dk_name = state["name"] if same else name_of.get(dk, dk)
                if has_prov_filter and not same and not _prov_ok(dk, dk_name):
                    _add_prune(prune_stats, "province_filter")
                    continue
                if exc_st and not same and _match_station_tokens(dk, dk_name, city_of, tele_of, exc_st):
                    _add_prune(prune_stats, "custom_rule")
                    continue
                if hub_tokens and not (
                    _match_station_tokens(state["station"], state["name"], city_of, tele_of, hub_tokens)
                    or _match_station_tokens(dk, dk_name, city_of, tele_of, hub_tokens)
                ):
                    _add_prune(prune_stats, "hub_filter")
                    continue
                sv_list = by_station.get(dk)
                if not sv_list:
                    continue
                eff_min = min_wait if same else min_wait + pair_extra
                span = span_all - eff_min
                if span < 0:
                    continue
                dk_city = _city_of_key(dk, dk_name, city_of, tele_of)
                if not same and tree_ctrl is not None and tree_ctrl.has_rules():
                    st_chain_edge = st_chain if st_chain is not None else state_hub_chain(state)
                    if tree_ctrl.is_cross_dep_pruned(st_chain_edge, dk_city, dk, dk_name, count_prune=True):
                        _add_prune(prune_stats, "user_tree")
                        continue
                eff_closed_cities = state["closed_cities"] if same else (state["closed_cities"] - frozenset((dk_city,)))

                # 二分查找在允许候车窗口 [t + eff_min, t + max_wait] 内的班次
                if span >= 1439:
                    cand_svs = sv_list
                else:
                    deps = by_dep_mods[dk]
                    lo_mod = (t + eff_min) % 1440
                    hi_mod = (t + max_wait) % 1440
                    if lo_mod <= hi_mod:
                        l_i = bisect.bisect_left(deps, lo_mod)
                        r_i = bisect.bisect_right(deps, hi_mod)
                        cand_svs = sv_list[l_i:r_i]
                    else:
                        l_i = bisect.bisect_left(deps, lo_mod)
                        r_i = bisect.bisect_right(deps, hi_mod)
                        cand_svs = sv_list[l_i:] + sv_list[:r_i]

                for si, i in cand_svs:
                    sv_codes, s_keys, s_canons, s_cities, dest_idx, sv_tier = sv_meta[si]
                    if is_last and not dest_idx:
                        continue
                    sv = allowed_sv[si]
                    if sv["train_no"] in state["used_trains"] or (sv_codes and (sv_codes & state["used_codes"])):
                        _add_prune(prune_stats, "repeat_train")
                        continue
                    stops = sv["stops"]
                    st_i = stops[i]
                    n = (t + eff_min - (base_min + st_i["dep"]) + 1439) // 1440
                    dep = base_min + st_i["dep"] + n * 1440
                    wait = dep - t
                    if wait > max_wait:
                        continue
                    if is_last and i >= dest_idx[-1]:
                        continue

                    if is_last:
                        # 末程直接前向走到目标停站（避免枚举非终点站与重复区间扫描）
                        last_tj = dest_idx[-1]
                        # 过头折返：末程在最后一个目的站之后又折回已走过的站 → 全程成环
                        if any(s_canons[z] in state["passed"]
                               for z in range(last_tj + 1, len(stops))
                               if stops[z]["enabled"]):
                            _add_prune(prune_stats, "loopback")
                            continue
                        dest_pos = 0
                        while dest_pos < len(dest_idx) and dest_idx[dest_pos] <= i:
                            dest_pos += 1
                        if dest_pos >= len(dest_idx):
                            continue
                        left_dk_city = False
                        city_looped = False
                        for y in range(i + 1, last_tj + 1):
                            if not stops[y]["enabled"]:
                                continue
                            b_canon = s_canons[y]
                            if b_canon in state["passed"]:
                                _add_prune(prune_stats, "loopback")
                                break
                            b_city = s_cities[y]
                            if y == dest_idx[dest_pos]:
                                if city_looped:
                                    _add_prune(prune_stats, "city_loop")
                                else:
                                    arr_y = base_min + stops[y]["arr"] + n * 1440
                                    if arr_y > dep:
                                        total = arr_y - state["start"]
                                        if time_limit < _INF and total > time_limit:
                                            if max_total_min and total > max_total_min:
                                                _add_prune(prune_stats, "total_time")
                                            else:
                                                _add_prune(prune_stats, "detour")
                                        elif not leg_conflict_raw(sv, st_i, stops[y], dep):
                                            new_trs = state["tr_refs"] + ((state["name"], state["to_tc"], same, wait, dk, dk_name, em),)
                                            raw_results.append((
                                                total, state["start"], arr_y,
                                                state["leg_refs"] + ((si, i, y, n),), new_trs,
                                            ))
                                dest_pos += 1
                                if dest_pos >= len(dest_idx):
                                    break
                            if b_city != dk_city:
                                left_dk_city = True
                            if (s_keys[y] not in tkeys) and ((b_city in eff_closed_cities) or (left_dk_city and b_city == dk_city)):
                                city_looped = True
                    else:
                        left_dk_city = False
                        city_looped = False
                        seen_dest = False
                        for j in range(i + 1, len(stops)):
                            b = stops[j]
                            if not b["enabled"]:
                                continue
                            b_canon = s_canons[j]
                            if b_canon in state["passed"]:
                                _add_prune(prune_stats, "loopback")
                                break
                            bk = s_keys[j]
                            b_is_dest = bk in tkeys
                            b_city = s_cities[j]
                            if b_is_dest:
                                # 命中终点站后禁止再扩展后续程数；depth >= 3 时按与末程一致口径校验过头折返/绕城并保留同车多终点
                                seen_dest = True
                                if len(state["leg_refs"]) >= 2:
                                    if any(s_canons[z] in state["passed"]
                                           for z in range(j + 1, len(stops))
                                           if stops[z]["enabled"]):
                                        _add_prune(prune_stats, "loopback")
                                    elif city_looped:
                                        _add_prune(prune_stats, "city_loop")
                                    else:
                                        arr_j = base_min + b["arr"] + n * 1440
                                        if arr_j > dep:
                                            total = arr_j - state["start"]
                                            if time_limit < _INF and total > time_limit:
                                                if max_total_min and total > max_total_min:
                                                    _add_prune(prune_stats, "total_time")
                                                else:
                                                    _add_prune(prune_stats, "detour")
                                            elif not leg_conflict_raw(sv, st_i, b, dep):
                                                new_trs = state["tr_refs"] + ((state["name"], state["to_tc"], same, wait, dk, dk_name, em),)
                                                raw_results.append((
                                                    total, state["start"], arr_j,
                                                    state["leg_refs"] + ((si, i, j, n),), new_trs,
                                                ))
                                else:
                                    break
                            if b_city != dk_city:
                                left_dk_city = True
                            if (not b_is_dest) and ((b_city in eff_closed_cities) or (left_dk_city and b_city == dk_city)):
                                city_looped = True
                            if seen_dest:
                                if not dest_idx or j >= dest_idx[-1]:
                                    break
                                continue
                            if city_looped:
                                _add_prune(prune_stats, "city_loop")
                                continue
                            if b_city == dk_city:
                                # WHY: 刚在同城上车尚未离城即在本城下一站(如 北京→北京南)下车再换乘属于内耗空转，
                                # 剪除此换乘落点但不置 city_looped，以免阻断同一列车继续出城到外地枢纽(如 济南)。
                                _add_prune(prune_stats, "city_loop")
                                continue
                            arr_j = base_min + b["arr"] + n * 1440
                            if arr_j <= dep:
                                continue
                            total = arr_j - state["start"]
                            if time_limit < _INF and total > time_limit:
                                if max_total_min and total > max_total_min:
                                    _add_prune(prune_stats, "total_time")
                                else:
                                    _add_prune(prune_stats, "detour")
                                continue
                            if has_prov_filter and not _prov_ok(bk, b["name"]):
                                _add_prune(prune_stats, "province_filter")
                                continue
                            if exc_st and _match_station_tokens(bk, b["name"], city_of, tele_of, exc_st):
                                _add_prune(prune_stats, "custom_rule")
                                continue
                            if tree_ctrl is not None and st_chain is not None and tree_ctrl.is_expansion_pruned(st_chain, b_city, bk, b["name"], count_prune=True):
                                _add_prune(prune_stats, "user_tree")
                                continue
                            lb = lb_at_depth(depth, bk)
                            if lb >= _INF:
                                continue
                            if time_limit < _INF and total + lb > time_limit:
                                if max_total_min and total + lb > max_total_min:
                                    _add_prune(prune_stats, "total_time")
                                else:
                                    _add_prune(prune_stats, "detour")
                                continue
                            if hub_tokens:
                                self_ok = _match_station_tokens(bk, b["name"], city_of, tele_of, hub_tokens)
                                if not self_ok:
                                    cross_ok = False
                                    if allow_cross:
                                        excl_tmp = state["passed"] | frozenset(s_canons[i:j + 1])
                                        c_edges = _dep_edges(bk, b["name"], available, cities, geo_index, cross_cfg,
                                                             exclude=excl_tmp, allow_cross=True,
                                                             truncation_out=truncation_out)
                                        cross_ok = any(
                                            not em2["same"] and _match_station_tokens(dk2, name_of.get(dk2), city_of, tele_of, hub_tokens)
                                            for dk2, em2 in c_edges
                                        )
                                    if not cross_ok:
                                        _add_prune(prune_stats, "hub_filter")
                                        continue
                            if leg_conflict_raw(sv, st_i, b, dep):
                                continue
                            new_passed = state["passed"] | frozenset(s_canons[i:j + 1])
                            new_closed = (state["closed_cities"] | frozenset((cur_city, dk_city))
                                          | frozenset(s_cities[i:j])) - frozenset((b_city,))
                            new_lrefs = state["leg_refs"] + ((si, i, j, n),)
                            new_tr = (state["name"], state["to_tc"], same, wait, dk, dk_name, em)
                            new_trrefs = state["tr_refs"] + (new_tr,)
                            c_leg, _ = seg_leg_cost(si, i, j)
                            c_known = state["cost_known"] and (c_leg is not None)
                            c_tot = (state["cost"] + c_leg) if c_known else float("inf")
                            c_tier = sv_tier if sv_tier > state["tier"] else state["tier"]
                            hubs = state["hubs"] + ((state["to_tc"] or state["name"] or "", bool(same)),)
                            same_cnt = state["same_cnt"] + (1 if same else 0)
                            buf_cur = wait if same else (wait - em["extra_min"])
                            min_buf = buf_cur if buf_cur < state["min_buf"] else state["min_buf"]
                            new_state = {
                                "arr": arr_j, "station": bk, "name": b["name"], "city": b_city,
                                "to_tc": b["telecode"],
                                "passed": new_passed, "closed_cities": new_closed,
                                "used_trains": state["used_trains"] | frozenset((sv["train_no"],)),
                                "used_codes": state["used_codes"] | sv_codes,
                                "leg_refs": new_lrefs,
                                "tr_refs": new_trrefs,
                                "start": state["start"],
                                "has_seats": state["has_seats"] and has_seat_raw(sv, st_i, b, dep),
                                "cost": c_tot, "cost_known": c_known, "tier": c_tier,
                                "hubs": hubs, "same_cnt": same_cnt, "min_buf": min_buf,
                            }
                            if not bounded:
                                nxt.setdefault(bk, []).append(new_state)
                                continue
                            buck = nxt.get(bk)
                            if buck is None:
                                buck = nxt[bk] = [[], [], [], 0]
                            buck[3] += 1
                            cnt = buck[3]
                            hs = 0 if new_state["has_seats"] else 1
                            elapsed = arr_j - new_state["start"]
                            if cnt <= frontier_k:
                                buck[2].append(new_state)
                            elif cnt == frontier_k + 1:
                                buck[2] = None
                            fe = ((hs, arr_j, elapsed), cnt, new_state)
                            buck[0].append(fe)
                            if len(buck[0]) > frontier_k:
                                buck[0].remove(max(buck[0]))
                            ce = ((hs, c_tot, arr_j, elapsed), cnt, new_state)
                            buck[1].append(ce)
                            if len(buck[1]) > frontier_k:
                                buck[1].remove(max(buck[1]))
        if is_last:
            break
        frontier = []
        if bounded:
            for buck in nxt.values():
                n_ins = buck[3]
                if n_ins <= frontier_k:
                    frontier.extend(buck[2])
                    continue
                by_fast = sorted(buck[0])
                by_cheap = sorted(buck[1])
                chosen, seen_cnt = [], set()
                fi = ci = 0
                while len(chosen) < frontier_k and (fi < len(by_fast) or ci < len(by_cheap)):
                    while fi < len(by_fast) and by_fast[fi][1] in seen_cnt:
                        fi += 1
                    if fi < len(by_fast) and len(chosen) < frontier_k:
                        seen_cnt.add(by_fast[fi][1])
                        chosen.append(by_fast[fi][2])
                        fi += 1
                    while ci < len(by_cheap) and by_cheap[ci][1] in seen_cnt:
                        ci += 1
                    if ci < len(by_cheap) and len(chosen) < frontier_k:
                        seen_cnt.add(by_cheap[ci][1])
                        chosen.append(by_cheap[ci][2])
                        ci += 1
                _add_prune(prune_stats, "frontier", n_ins - len(chosen))
                frontier.extend(chosen)
        else:
            for bucket in nxt.values():
                if prune_dominated and len(bucket) > 1:
                    # 同级同枢纽支配剪枝：价格更低或换乘更稳妥时不剪除
                    bucket.sort(key=lambda s: (len(s["leg_refs"]), s["arr"], -s["start"], s["cost"]))
                    kept = []
                    for s in bucket:
                        s_cost = s["cost"] if s["cost_known"] else None
                        if any(
                            _dominates(
                                len(k["leg_refs"]), k["start"], k["arr"], k["hubs"],
                                k["same_cnt"], k["min_buf"], k["cost"] if k["cost_known"] else None, k["tier"],
                                len(s["leg_refs"]), s["start"], s["arr"], s["hubs"],
                                s["same_cnt"], s["min_buf"], s_cost, s["tier"],
                                k["passed"], k["closed_cities"], s["passed"], s["closed_cities"],
                            )
                            for k in kept
                        ):
                            _add_prune(prune_stats, "dominated")
                            continue
                        kept.append(s)
                    bucket = kept
                if has_fk and len(bucket) > frontier_k:
                    # 束宽截断同时保留最快到达与最低费用双维度最优分支
                    by_fast = sorted(
                        range(len(bucket)),
                        key=lambda idx: (0 if bucket[idx]["has_seats"] else 1, bucket[idx]["arr"], bucket[idx]["arr"] - bucket[idx]["start"]),
                    )
                    by_cheap = sorted(
                        range(len(bucket)),
                        key=lambda idx: (0 if bucket[idx]["has_seats"] else 1, bucket[idx]["cost"], bucket[idx]["arr"], bucket[idx]["arr"] - bucket[idx]["start"]),
                    )
                    chosen, seen_idx = [], set()
                    fi = ci = 0
                    while len(chosen) < frontier_k and (fi < len(by_fast) or ci < len(by_cheap)):
                        while fi < len(by_fast) and by_fast[fi] in seen_idx:
                            fi += 1
                        if fi < len(by_fast) and len(chosen) < frontier_k:
                            seen_idx.add(by_fast[fi])
                            chosen.append(bucket[by_fast[fi]])
                            fi += 1
                        while ci < len(by_cheap) and by_cheap[ci] in seen_idx:
                            ci += 1
                        if ci < len(by_cheap) and len(chosen) < frontier_k:
                            seen_idx.add(by_cheap[ci])
                            chosen.append(bucket[by_cheap[ci]])
                            ci += 1
                    _add_prune(prune_stats, "frontier", len(bucket) - len(chosen))
                    frontier.extend(chosen)
                else:
                    frontier.extend(bucket)
        if not frontier:
            break
        if tree_ctrl is not None:
            frontier = tree_ctrl.checkpoint(
                frontier, depth, max_legs, cancel_event, state_hub_chain, build_tree_snapshot, prune_stats=prune_stats
            )
            if not frontier:
                break

    # BFS 结束后立即释放索引与前沿状态，为结果丰富化腾出内存
    del frontier, nxt, by_station, by_dep_mods, sv_meta, seats_keys

    # 5) 先在轻量元组层完成去重、后置规则与绕行/支配过滤，最后仅对优胜 Top-K 调用 make_leg_tuple（避免几十万条全量实例化撑爆 RAM）
    dedup = {}
    for total, st_ep, arr_ep, l_refs, tr_refs in raw_results:
        key = (
            tuple(allowed_sv[r[0]]["train_no"] for r in l_refs),
            tuple(tr[0] for tr in tr_refs),
            tuple(allowed_sv[r[0]]["stops"][r[1]]["name"] for r in l_refs),
            allowed_sv[l_refs[-1][0]]["stops"][l_refs[-1][2]]["name"],
            st_ep,
        )
        if key not in dedup or total < dedup[key][0]:
            dedup[key] = (total, st_ep, arr_ep, l_refs, tr_refs)
    del raw_results

    final_raw = list(dedup.values())
    del dedup

    if (not allow_cross) or inc_st or exc_st:
        kept_filt = []
        for it in final_raw:
            tr_refs = it[4]
            if not allow_cross and any(not tr[2] for tr in tr_refs):
                _add_prune(prune_stats, "custom_rule")
                continue
            pairs = [
                (tr[1] or tr[0], tr[0], tr[4] or tr[5] or (tr[1] or tr[0]), tr[5] or tr[0])
                for tr in tr_refs
            ]
            if exc_st and any(
                _match_station_tokens(sk, sk_n, city_of, tele_of, exc_st)
                or (dk != sk and _match_station_tokens(dk, dk_n, city_of, tele_of, exc_st))
                for sk, sk_n, dk, dk_n in pairs
            ):
                _add_prune(prune_stats, "custom_rule")
                continue
            if inc_st:
                if inc_st_mode == "and":
                    ok_inc = all(
                        any(
                            _match_station_tokens(sk, sk_n, city_of, tele_of, [tok])
                            or _match_station_tokens(dk, dk_n, city_of, tele_of, [tok])
                            for sk, sk_n, dk, dk_n in pairs
                        )
                        for tok in inc_st
                    )
                else:
                    ok_inc = any(
                        _match_station_tokens(sk, sk_n, city_of, tele_of, inc_st)
                        or _match_station_tokens(dk, dk_n, city_of, tele_of, inc_st)
                        for sk, sk_n, dk, dk_n in pairs
                    )
                if not ok_inc:
                    _add_prune(prune_stats, "custom_rule")
                    continue
            kept_filt.append(it)
        final_raw = kept_filt

    if eff_detour > 0 and final_raw:
        base_best = prune_cfg.get("base_best_min")
        cur_min = min(it[0] for it in final_raw)
        best_all = min(base_best, cur_min) if base_best is not None else cur_min
        d_lim = max(DETOUR_FLOOR_MIN, best_all * eff_detour)
        kept_det = []
        for it in final_raw:
            if it[0] > d_lim:
                _add_prune(prune_stats, "detour")
            else:
                kept_det.append(it)
        final_raw = kept_det

    if prune_dominated and len(final_raw) > 1:
        groups: dict[tuple, list] = {}
        for item in final_raw:
            first_ref, last_ref = item[3][0], item[3][-1]
            fn = allowed_sv[first_ref[0]]["stops"][first_ref[1]]["name"]
            tn = allowed_sv[last_ref[0]]["stops"][last_ref[2]]["name"]
            groups.setdefault((fn, tn), []).append(item)
        kept_raw = []
        for grp in groups.values():
            grp.sort(key=lambda it: (len(it[3]), it[2], -it[1]))
            acc = []
            for it in grp:
                l_refs, tr_refs = it[3], it[4]
                p_cost, p_known, p_tier = refs_cost_tier(l_refs)
                p_hubs, p_same, p_buf = tr_meta(tr_refs)
                pc = p_cost if p_known else None
                if any(
                    _dominates(
                        k["lc"], k["start"], k["arr"], k["hubs"],
                        k["same"], k["buf"], k["cost"], k["tier"],
                        len(l_refs), it[1], it[2], p_hubs,
                        p_same, p_buf, pc, p_tier,
                    )
                    for k in acc
                ):
                    _add_prune(prune_stats, "dominated")
                    continue
                acc.append({
                    "it": it, "lc": len(l_refs), "start": it[1], "arr": it[2],
                    "hubs": p_hubs, "same": p_same, "buf": p_buf, "cost": pc, "tier": p_tier,
                })
            kept_raw.extend(x["it"] for x in acc)
        final_raw = kept_raw

    multi_total = len(final_raw)
    if total_out is not None:
        total_out[:] = [multi_total]

    if result_limit > 0 and multi_total > result_limit:
        keep_n = result_limit if not prune_dominated else min(multi_total, result_limit * 4)
        final_raw = heapq.nsmallest(keep_n, final_raw, key=lambda it: it[0])

    out = []
    for total, _st_ep, _arr_ep, l_refs, tr_refs in final_raw:
        legs = [make_leg_tuple(r) for r in l_refs]
        transfers = [{
            "station": tr[0],
            "telecode": tr[1],
            "to_station": None if tr[2] else tr[5],
            "to_telecode": None if tr[2] else tr[4],
            "same": tr[2],
            "wait_min": tr[3],
            **_transfer_edge_fields(tr[6], tr[3], tr[2]),
        } for tr in tr_refs]
        out.append({"total_min": total, "date_assumed": True, "legs": legs, "transfers": transfers})
    return out


def _candidate_trains(fkeys: set[str], tkeys: set[str]) -> set[str]:
    """2 程以内预过滤：只加载经过出发站或到达站（含大屏始发终到）的候选 train_no。"""
    keys = list(fkeys | tkeys)
    if not keys:
        return set()
    ph = ",".join("?" * len(keys))
    out = set()
    conn = db.get_conn()
    for (tn,) in conn.execute(
        f"""SELECT DISTINCT train_no FROM station_trains
            WHERE train_no IS NOT NULL AND (
                station_telecode IN ({ph}) OR station_name IN ({ph})
                OR start_station_telecode IN ({ph}) OR start_station_name IN ({ph})
                OR end_station_telecode IN ({ph}) OR end_station_name IN ({ph})
            )""",
        (*keys, *keys, *keys, *keys, *keys, *keys),
    ):
        out.add(tn)
    for (tn,) in conn.execute(
        f"""SELECT DISTINCT train_no FROM train_stops
            WHERE telecode IN ({ph}) OR station_name IN ({ph})""",
        (*keys, *keys),
    ):
        out.add(tn)
    return out


def _load_sno(target_date: str | None = None) -> dict:
    out = {}
    conn = db.get_conn()
    for tn, tc, sno in conn.execute(
        "SELECT train_no,telecode,station_no FROM train_stops "
        "WHERE telecode IS NOT NULL ORDER BY train_date ASC, id ASC"
    ):
        out[(tn, tc)] = sno
    if target_date:
        for tn, tc, sno in conn.execute(
            "SELECT train_no,telecode,station_no FROM train_stops "
            "WHERE telecode IS NOT NULL AND train_date=?",
            (target_date,),
        ):
            out[(tn, tc)] = sno
    return out


def _build_graph_cache(ver: tuple, cancel_event=None, sub_cb=None) -> dict:
    """将全量车站、空间近邻索引、票价校准、站序、大屏与时刻表路网构建为常驻内存快照。"""
    n2c = {}
    city_of, city_groups, tele_of = {}, {}, {}
    st_rows = []
    for tc, nm, ct, lat, lon, cstatus, cconf, tzone, rdist, rmode, is_hub in db.get_conn().execute(
        """SELECT telecode, name, city, lat, lon, coord_status, coord_confidence,
                  transit_zone_id, nearest_rail_stop_distance_m, rail_access_mode, is_major_hub
           FROM stations"""
    ):
        n2c[nm] = tc
        tele_of[nm] = tc
        if ct:
            city_of[tc] = ct
            city_of[nm] = ct
            city_groups.setdefault(ct, set()).update((tc, nm))
        st_rows.append({
            "telecode": tc, "name": nm, "city": ct or "",
            "lat": lat, "lon": lon, "coord_status": cstatus or "missing",
            "coord_confidence": cconf,
            "transit_zone_id": tzone or "",
            "nearest_rail_stop_distance_m": rdist,
            "rail_access_mode": rmode or "none",
            "is_major_hub": is_hub or 0,
        })
    cities = (city_of, city_groups, tele_of)
    geo_index = geo.StationGeoIndex(
        st_rows,
        db.list_transfer_groups(),
        db.list_transfer_pair_overrides(),
        geo.load_transit_cities(),
    )
    cal = fare.load_calibration()
    sno_base = _load_sno(None)
    screen_all = _load_screen(None, cancel_event=cancel_event)
    tt_all = _load_trains(None, cancel_event=cancel_event, sub_cb=sub_cb)
    # 大屏按拍到该车次的车站分行，同一物理车次途中会改号（厦门北→兰州记作 Z126/Z127，
    # 兰州→厦门北记作 Z125/Z128），所以大屏车次码与时刻表不一致并不代表反向，不能据此剔除。
    # 但大屏只拍到部分停靠站会让“已越过目的站/过头折返”这类按站序的剪枝失明，
    # 故用同 train_no 的时刻表停站补全缺口（标 bridged，票价余票仍各自保留）。
    # 缓存对象跨请求共享，补全必须在倒排索引与 services 构建之前只做一次。
    _complete_screen_from_tt(screen_all, tt_all)
    screen_key_to_tn: dict[str, set[str]] = {}
    for run, t in screen_all.items():
        tn = run[0]
        for k, o in t["obs"].items():
            screen_key_to_tn.setdefault(k, set()).add(tn)
            nm = o.get("name")
            if nm:
                screen_key_to_tn.setdefault(nm, set()).add(tn)

    tt_key_to_tn: dict[str, set[str]] = {}
    for T in tt_all:
        tn = T.train_no
        for s in T.stops:
            tt_key_to_tn.setdefault(s["key"], set()).add(tn)
            nm = s.get("station_name")
            if nm:
                tt_key_to_tn.setdefault(nm, set()).add(tn)

    prov_of = _build_prov_map(cities, geo_index=geo_index)
    services_all = _load_services(screen_all, tt_all, cancel_event=cancel_event)
    return {
        "version": ver,
        "n2c": n2c,
        "cities": cities,
        "geo_index": geo_index,
        "prov_of": prov_of,
        "cal": cal,
        "sno_base": sno_base,
        "screen_all": screen_all,
        "screen_key_to_tn": screen_key_to_tn,
        "tt_all": tt_all,
        "tt_key_to_tn": tt_key_to_tn,
        "services_all": services_all,
    }


def _get_graph_cache(cancel_event=None, sub_cb=None) -> tuple[dict, bool]:
    """获取进程内常驻路网缓存；若数据库有写入或跨进程更新则自动重建。"""
    global _MEM_CACHE
    ver = db.data_version()
    with _CACHE_LOCK:
        cached = _MEM_CACHE if (_MEM_CACHE is not None and _MEM_CACHE["version"] == ver) else None
    if cached is not None:
        return cached, True
    with _BUILD_LOCK:
        ver = db.data_version()
        with _CACHE_LOCK:
            cached = _MEM_CACHE if (_MEM_CACHE is not None and _MEM_CACHE["version"] == ver) else None
        if cached is not None:
            return cached, True
        built = _build_graph_cache(ver, cancel_event=cancel_event, sub_cb=sub_cb)
        with _CACHE_LOCK:
            _MEM_CACHE = built
        return built, False


def _get_date_cache(ver: tuple, target_date: str, sno_base: dict) -> dict:
    """按目标日期缓存票价表、余票表及当日站序覆盖，数据版本变化时自动清空。"""
    global _DATE_CACHE_VER
    with _CACHE_LOCK:
        if _DATE_CACHE_VER != ver:
            _DATE_CACHE.clear()
            _DATE_CACHE_VER = ver
        hit = _DATE_CACHE.get(target_date)
    if hit is not None:
        return hit

    prices = _price_map(target_date)
    seats, real_tk, direct_tickets, tk_aux = _seat_map(target_date)
    overrides = db.get_conn().execute(
        "SELECT train_no,telecode,station_no FROM train_stops "
        "WHERE telecode IS NOT NULL AND train_date=?",
        (target_date,),
    ).fetchall()
    if overrides:
        sno = dict(sno_base)
        for tn, tc, s_no in overrides:
            sno[(tn, tc)] = s_no
    else:
        sno = sno_base
    entry = {"prices": prices, "seats": seats, "real_tk": real_tk,
             "direct_tickets": direct_tickets, "tk_aux": tk_aux, "sno": sno}

    with _CACHE_LOCK:
        if _DATE_CACHE_VER != ver:
            _DATE_CACHE.clear()
            _DATE_CACHE_VER = ver
        if len(_DATE_CACHE) >= _MAX_DATE_CACHE and target_date not in _DATE_CACHE:
            oldest = next(iter(_DATE_CACHE))
            _DATE_CACHE.pop(oldest, None)
        _DATE_CACHE[target_date] = entry
    return entry


def search(from_q: str, to_q: str, date: str,
           min_wait: int = 30, max_wait: int = 360, limit: int = 0,
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
           active_group_ids: list[int] | None = None,
           fallback_ungeocoded: bool = False,
           progress_cb=None, cancel_event=None, sub_cb=None,
           tree_ctrl: TreePruneController | None = None,
           province_mode: str = "off",
           allow_provinces: str | list[str] = "",
           exclude_provinces: str | list[str] = "",
           block_unknown_prov: bool = False) -> dict:
    preset = PRUNE_PRESETS.get(prune_level, PRUNE_PRESETS["balanced"])
    eff_fk = frontier_k if frontier_k is not None else preset["frontier_k"]
    eff_max_total = max_total_min if max_total_min is not None else preset["max_total_min"]
    eff_detour = float(detour_ratio if detour_ratio is not None else preset["detour_ratio"])
    eff_dom = bool(prune_dominated if prune_dominated is not None else preset["prune_dominated"])
    hub_tokens = _resolve_hub_tokens(hub_only, hub_stations)
    inc_pfx = _parse_tokens(allow_prefixes)
    exc_pfx = _parse_tokens(exclude_prefixes)
    inc_st = _parse_tokens(include_stations)
    inc_st_mode = "and" if str(include_stations_mode or "or").strip().lower() == "and" else "or"
    exc_st = _parse_tokens(exclude_stations)

    p_mode = str(province_mode or "off").strip().lower()
    raw_allow_p = _parse_province_tokens(allow_provinces)
    raw_exc_p = _parse_province_tokens(exclude_provinces)
    if p_mode == "allow":
        allow_provs, exc_provs = raw_allow_p, raw_exc_p
    elif p_mode == "exclude":
        allow_provs, exc_provs = set(), raw_exc_p
    elif raw_allow_p or raw_exc_p:
        allow_provs, exc_provs = raw_allow_p, raw_exc_p
        p_mode = "allow" if raw_allow_p else "exclude"
    else:
        allow_provs, exc_provs = set(), set()
        p_mode = "off"
    if p_mode == "off":
        block_unknown_prov = False

    # WHY: 束宽(frontier_k)与支配剪枝在过滤之前按“局部最优”扔分支，会把“尚未经过必经站(inc_st)”的合法分支误剪；
    # 而车型头(inc_pfx/exc_pfx)与排除站(exc_st)已下沉到 _search_multi 搜前/入桶前精准过滤，无需放大束宽 4x 引发状态数 16x 膨胀。
    # NOTE: 中转省份过滤属于主动收缩搜索空间，同样不触发 auto_relaxed，保留 frontier_k 加速收敛。
    auto_relaxed = False
    if inc_st and frontier_k is None and prune_dominated is None:
        if eff_fk or eff_dom:
            if eff_fk:
                eff_fk = max(32, min(64, eff_fk * 4))
            eff_dom = False
            auto_relaxed = True
    # AND 要求每个 token 各命中一个换乘对：2 程仅 1 对，AND 两站恒 0。
    # 与其回空结果不如自动提高程数（上限 5 程），超上限时在 note 说明。
    legs_note = ""
    if inc_st and inc_st_mode == "and" and len(inc_st) > max_legs - 1:
        need = len(inc_st) + 1
        if need <= 5:
            legs_note = f"必经 AND {len(inc_st)} 站需 {len(inc_st)} 次换乘，已自动把程数从 {max_legs} 提高到 {need} · "
            max_legs = need
        else:
            legs_note = f"必经 AND {len(inc_st)} 站超过 5 程(4 次换乘)上限，AND 无法满足，结果可能为 0 · "

    prune_stats = {
        "repeat_train": 0, "loopback": 0, "city_loop": 0, "passed_dest": 0,
        "total_time": 0, "detour": 0, "dominated": 0, "frontier": 0,
        "hub_filter": 0, "province_filter": 0, "custom_rule": 0, "user_tree": 0,
    }
    prune_cfg = {
        "prune_level": prune_level,
        "frontier_k": eff_fk,
        "max_total_min": eff_max_total,
        "detour_ratio": eff_detour,
        "prune_dominated": eff_dom,
        "auto_relaxed": auto_relaxed,
        "allow_cross_station": bool(allow_cross_station),
        "hub_only": bool(hub_only),
        "hub_tokens": hub_tokens,
        "detour_cap": None,
        "base_best_min": None,
        "inc_pfx": inc_pfx,
        "exc_pfx": exc_pfx,
        "inc_st": inc_st,
        "inc_st_mode": inc_st_mode,
        "exc_st": exc_st,
        "province_mode": p_mode,
        "allow_provs": sorted(allow_provs),
        "exc_provs": sorted(exc_provs),
        "block_unknown_prov": bool(block_unknown_prov),
    }

    total_steps = 6 if max_legs < 3 else 5 + (max_legs - 1)
    last_sub_ts = [0.0]

    def step(done: int, msg: str):
        _check_cancel(cancel_event)
        last_sub_ts[0] = 0.0
        if progress_cb is not None:
            progress_cb(done, total_steps, msg)

    def sub_step(s_done: int, s_total: int, text: str, force: bool = False):
        _check_cancel(cancel_event)
        if sub_cb is None:
            return
        now = time.monotonic()
        if force or s_done >= s_total or (now - last_sub_ts[0]) >= 0.15:
            last_sub_ts[0] = now
            sub_cb(s_done, s_total, text)

    step(1, "解析车站与价格/余票索引")
    f_mode = from_city_mode or city_mode
    t_mode = to_city_mode or city_mode
    fstations = db.resolve_stations(from_q, expand_city, f_mode)
    tstations = db.resolve_stations(to_q, expand_city, t_mode)
    if not fstations or not tstations:
        missing = from_q if not fstations else to_q
        miss_mode = f_mode if not fstations else t_mode
        raise ValueError(db.station_resolve_error(missing, miss_mode))
    fkeys = {s["telecode"] for s in fstations} | {s["name"] for s in fstations}
    tkeys = {s["telecode"] for s in tstations} | {s["name"] for s in tstations}
    base = datetime.strptime(date, "%Y-%m-%d")
    day_min = base.toordinal() * 1440

    step(2, "加载大屏与时刻表缓存")
    g, cache_hit = _get_graph_cache(cancel_event=cancel_event, sub_cb=sub_step)
    d_cache = _get_date_cache(g["version"], date, g["sno_base"])
    prices = d_cache["prices"]
    seats = d_cache["seats"]
    real_tk = d_cache["real_tk"]
    direct_tickets = d_cache["direct_tickets"]
    tk_aux = d_cache["tk_aux"]
    sno = d_cache["sno"]
    n2c = g["n2c"]
    cal = g["cal"]
    cities = g["cities"]
    geo_index = g.get("geo_index")
    prov_of = g.get("prov_of") or _build_prov_map(cities, geo_index=geo_index)
    prune_cfg["prov_of"] = prov_of
    cross_cfg = geo.resolve_cross_cfg(
        cross_extra=cross_extra,
        cross_radius_km=cross_radius_km,
        cross_time_mode=cross_time_mode,
        transit_discount_on=transit_discount_on,
        transit_discount_min=transit_discount_min,
        transit_proximity_m=transit_proximity_m,
        active_group_ids=active_group_ids,
        fallback_ungeocoded=fallback_ungeocoded,
        geo_index=geo_index,
    )
    truncation_flag = [False]

    # 基于内存倒排索引筛选接触起终点的候选车次，2程阶段无需遍历无关车次
    cand_scr_tns = set()
    for k in (fkeys | tkeys):
        cand_scr_tns.update(g["screen_key_to_tn"].get(k, ()))
    screen = {run: t for run, t in g["screen_all"].items() if run[0] in cand_scr_tns}

    cand_tt_tns = set()
    for k in (fkeys | tkeys):
        cand_tt_tns.update(g["tt_key_to_tn"].get(k, ()))
    tt = [T for T in g["tt_all"] if T.train_no in cand_tt_tns]
    need_transfers = max_legs >= 2
    step(3, f"计算大屏直达{'/换乘' if need_transfers else ''}（{len(screen)} 班次）")
    d1, t1 = _search_screen(screen, fkeys, tkeys, day_min, date, min_wait, max_wait,
                            prices, seats, cal, sno, cities, cross_extra=cross_extra,
                            cancel_event=cancel_event, prune_cfg=prune_cfg, prune_stats=prune_stats,
                            sub_cb=sub_step, need_transfers=need_transfers,
                            geo_index=geo_index, cross_cfg=cross_cfg, truncation_out=truncation_flag,
                            tk_aux=tk_aux)

    style_map = _style_map()

    def _assumed_conflict(p) -> bool:
        """推算方案（无当日真实数据平移而来）若与实际余票冲突，则以实际余票为准剔除。"""
        if not p.get("date_assumed") or not real_tk:
            return False
        for l in p["legs"]:
            if "seats" in l:
                continue
            ftc = l.get("from_telecode") or n2c.get(l.get("from"))
            ttc = l.get("to_telecode") or n2c.get(l.get("to"))
            dep_date = l["depart"][:10]
            key = (ftc, ttc, dep_date)
            if None in key:
                continue
            if key in real_tk and l["train_no"] not in real_tk[key]:
                if _resolve_live_ticket(
                    l["train_no"], l.get("train"), ftc, ttc, dep_date,
                    l["depart"][11:16], l["arrive"][11:16], tk_aux, style_map=style_map
                ) is not None:
                    continue
                return True
        return False

    non_conflicted_1 = [p["total_min"] for p in d1 + t1 if not _assumed_conflict(p)]
    init_best_min = min(non_conflicted_1) if non_conflicted_1 else None

    step(4, f"计算时刻表直达{'/换乘' if need_transfers else ''}（{len(tt)} 车次）")
    tt_sub_cap = 70 if max_legs >= 3 else 100
    d2, t2 = _search_timetable(tt, fkeys, tkeys, base, min_wait, max_wait,
                               prices, seats, cal, sno, cities, cross_extra=cross_extra,
                               cancel_event=cancel_event, prune_cfg=prune_cfg, prune_stats=prune_stats,
                               sub_cb=sub_step, sub_cap=tt_sub_cap,
                               real_tk=real_tk, n2c=n2c, init_best_min=init_best_min,
                               need_transfers=need_transfers,
                               geo_index=geo_index, cross_cfg=cross_cfg, truncation_out=truncation_flag,
                               tk_aux=tk_aux)

    def rank(p):
        return (0 if any("prices" in l for l in p["legs"]) else 1,
                1 if p.get("date_assumed") else 0)

    def _canon_tn(l: dict) -> str:
        return l.get("live_train_no") or l["train_no"]

    merged_d = {}
    for p in d1 + d2:
        l0 = p["legs"][0]
        key = (_canon_tn(l0), l0["from"], l0["to"], l0["depart"])
        if key not in merged_d or rank(p) < rank(merged_d[key]):
            merged_d[key] = p
    direct = list(merged_d.values())
    merged_t = {}
    for p in t1 + t2:
        l0, l1 = p["legs"][0], p["legs"][1]
        key = (_canon_tn(l0), _canon_tn(l1),
               p["transfer_station"], l1["from"], l0["from"],
               l1["to"], l0["depart"])
        if key not in merged_t or rank(p) < rank(merged_t[key]):
            merged_t[key] = p
    transfers = []
    for p in merged_t.values():
        if "transfers" not in p:
            same = bool(p.get("same_station", True))
            wait_v = p.get("wait_min", 0)
            eff_v = 0 if same else int(p.get("effective_extra", p.get("effective_cross_extra", cross_extra)))
            p["transfers"] = [{
                "station": p.get("transfer_station"),
                "telecode": p["legs"][0].get("to_telecode"),
                "to_station": None if same else p["legs"][1].get("from"),
                "to_telecode": None if same else p["legs"][1].get("from_telecode"),
                "same": same,
                "wait_min": wait_v,
                "base_extra": 0 if same else int(p.get("base_extra", eff_v)),
                "hub_extra": 0 if same else int(p.get("hub_extra", 0)),
                "transit_discount": 0 if same else int(p.get("transit_discount", p.get("transit_discount_min", 0))),
                "transit_discount_min": 0 if same else int(p.get("transit_discount_min", p.get("transit_discount", 0))),
                "effective_extra": eff_v,
                "effective_cross_extra": eff_v,
                "min_buffer": wait_v if same else (wait_v - eff_v),
                "source_rule": "same" if same else (p.get("source_rule") or p.get("cross_source") or "radius"),
                "cross_source": "same" if same else (p.get("cross_source") or p.get("source_rule") or "radius"),
                "distance_km": 0.0 if same else p.get("distance_km", p.get("cross_distance_km")),
                "cross_distance_km": 0.0 if same else p.get("cross_distance_km", p.get("distance_km")),
                "rail_access_mode": "none" if same else (p.get("rail_access_mode") or "none"),
                "transit_reason": "" if same else (p.get("transit_reason") or ""),
                "transit_cities": [] if same else list(p.get("transit_cities") or []),
                "coord_confidence": 1.0 if same else float(p.get("coord_confidence", 0.0)),
                "is_fallback": False if same else bool(p.get("is_fallback", False)),
                "is_suspect": False if same else bool(p.get("is_suspect", False)),
            }]
        transfers.append(p)
    del merged_d, merged_t, d1, d2, t1, t2

    has_post_filter = bool(inc_pfx or exc_pfx or inc_st or exc_st or allow_provs or exc_provs or block_unknown_prov)

    # 引擎 D：多程换乘（3 程及以上，每日开行假设）
    extra_multi_unreturned = 0
    if max_legs >= 3:
        if eff_detour > 0:
            non_conf_2 = [p for p in direct + transfers if not _assumed_conflict(p)]
            if has_post_filter and non_conf_2:
                # 绕行基准只能取自符合过滤约束的合法方案，否则 banned 快速车会压低 detour_cap 误杀合法慢车
                legal_d = filter_plans(
                    [p for p in direct if not _assumed_conflict(p)],
                    allow_prefixes=inc_pfx, exclude_prefixes=exc_pfx,
                    include_stations=inc_st, exclude_stations=exc_st,
                    include_stations_mode=inc_st_mode, allow_cross_station=allow_cross_station,
                    cities=cities, stats=None,
                )
                legal_t = filter_plans(
                    [p for p in transfers if not _assumed_conflict(p)],
                    allow_prefixes=inc_pfx, exclude_prefixes=exc_pfx,
                    include_stations=inc_st, exclude_stations=exc_st,
                    include_stations_mode=inc_st_mode, allow_cross_station=allow_cross_station,
                    cities=cities, stats=None,
                    allow_provinces=allow_provs, exclude_provinces=exc_provs,
                    prov_of=prov_of, block_unknown_prov=bool(block_unknown_prov),
                )
                non_conf_min = [p["total_min"] for p in legal_d + legal_t]
            else:
                non_conf_min = [p["total_min"] for p in non_conf_2]
            if non_conf_min:
                bmin = min(non_conf_min)
                prune_cfg["base_best_min"] = bmin
                prune_cfg["detour_cap"] = max(DETOUR_FLOOR_MIN, bmin * eff_detour)
        sub_step(70, 100, f"复用内存路网服务（{len(g['services_all'])} 车次）…", force=True)
        services = g["services_all"]
        multi_total_box = [0]
        multi_res = _search_multi(
            services, fkeys, tkeys, day_min, min_wait, max_wait,
            max_legs, prices, seats, cal, sno, cities,
            real_tk=real_tk, n2c=n2c,
            cross_extra=cross_extra, frontier_k=eff_fk,
            cancel_event=cancel_event,
            depth_cb=lambda d, n_st: step(4 + (d - 1), f"多程 BFS 搜索第 {d}/{max_legs} 程（状态 {n_st}）"),
            prune_cfg=prune_cfg, prune_stats=prune_stats,
            init_sub_cb=lambda sd, st, txt: sub_step(70 + int(sd * 30 / max(1, st)), 100, txt),
            depth_sub_cb=lambda d, sd, st, hits: sub_step(
                sd, st, f"多程 BFS 搜索第 {d}/{max_legs} 程：分支 {sd}/{st}，已命中 {hits} 方案"
            ),
            geo_index=geo_index, cross_cfg=cross_cfg, truncation_out=truncation_flag,
            result_limit=limit, total_out=multi_total_box,
            tree_ctrl=tree_ctrl, tk_aux=tk_aux,
        )
        extra_multi_unreturned = max(0, multi_total_box[0] - len(multi_res))
        transfers.extend(multi_res)
        del services

    step(total_steps, "汇总余票直达并校验冲突")

    # 引擎 C：仅有余票数据的直达（直接复用内存 _DATE_CACHE['direct_tickets']，零磁盘查询）
    have = set()
    for p in direct:
        l0 = p["legs"][0]
        ftc0, ttc0 = l0.get("from_telecode"), l0.get("to_telecode")
        have.add((l0["train_no"], ftc0, ttc0))
        if l0.get("live_train_no"):
            have.add((l0["live_train_no"], ftc0, ttc0))
    seen_tk = set()
    for fst in fstations:
        for tst in tstations:
            for tn, st_code, fno, tno, st_time, arr_time, dur_str in direct_tickets.get((fst["telecode"], tst["telecode"]), ()):
                tkey = (tn, fst["telecode"], tst["telecode"])
                if tkey in have or tkey in seen_tk:
                    continue
                seen_tk.add(tkey)
                dep, arr = _minutes(st_time), _minutes(arr_time)
                if dep is None or arr is None:
                    continue
                dur = _colon_dur(dur_str)
                if dur is None or dur <= 0:
                    dur = arr - dep
                    if dur < 0:
                        dur += 1440
                if eff_max_total and dur > eff_max_total:
                    _add_prune(prune_stats, "total_time")
                    continue
                direct.append({"total_min": dur, "source_date": date, "legs": [_enrich({
                    "train": st_code, "train_no": tn,
                    "from": fst["name"], "to": tst["name"],
                    "from_telecode": fst["telecode"], "to_telecode": tst["telecode"],
                    "depart": _fmt(base, dep), "arrive": _fmt(base, dep + dur),
                    "duration_min": dur,
                }, tn, fno, tno, fst["telecode"], tst["telecode"], date, prices, seats,
                    code=st_code, duration_min=dur, cal=cal, sno=sno,
                    dep_hm=_hm_of(dep), arr_hm=_hm_of(dep + dur), tk_aux=tk_aux, style_map=style_map)]})

    direct = [p for p in direct if not _assumed_conflict(p)]
    transfers = [p for p in transfers if not _assumed_conflict(p)]
    if len(transfers) > 1:
        dedup_t = {}
        for p in transfers:
            sig = tuple(
                (_canon_tn(l), l.get("from_telecode") or l.get("from"), l.get("to_telecode") or l.get("to"), l["depart"], l["arrive"])
                for l in p["legs"]
            )
            if sig not in dedup_t or rank(p) < rank(dedup_t[sig]):
                dedup_t[sig] = p
        transfers = list(dedup_t.values())

    # 车次头、换乘站与中转省份统一过滤（必须先于绕行基准与支配剪枝执行，确保违禁方案不挤压合法方案）
    if has_post_filter:
        direct = filter_plans(
            direct,
            allow_prefixes=inc_pfx, exclude_prefixes=exc_pfx,
            include_stations=inc_st, exclude_stations=exc_st,
            include_stations_mode=inc_st_mode, allow_cross_station=allow_cross_station,
            cities=cities, stats=prune_stats,
        )
        transfers = filter_plans(
            transfers,
            allow_prefixes=inc_pfx, exclude_prefixes=exc_pfx,
            include_stations=inc_st, exclude_stations=exc_st,
            include_stations_mode=inc_st_mode, allow_cross_station=allow_cross_station,
            cities=cities, stats=prune_stats,
            allow_provinces=allow_provs, exclude_provinces=exc_provs,
            prov_of=prov_of, block_unknown_prov=bool(block_unknown_prov),
        )
    prune_cfg.pop("prov_of", None)

    # 汇总期绕行倍率剪枝（设至少 8 小时保底时长）与同级同枢纽支配剪枝
    detour_limit_min = None
    if eff_detour > 0 and (direct or transfers):
        best_min = min(p["total_min"] for p in direct + transfers)
        detour_limit_min = max(DETOUR_FLOOR_MIN, best_min * eff_detour)
        kept_t = []
        for p in transfers:
            if p["total_min"] > detour_limit_min:
                _add_prune(prune_stats, "detour")
            else:
                kept_t.append(p)
        transfers = kept_t
    elif prune_cfg.get("detour_cap"):
        detour_limit_min = prune_cfg["detour_cap"]

    if eff_dom and len(transfers) > 1:
        groups: dict[tuple, list] = {}
        for p in transfers:
            groups.setdefault((p["legs"][0]["from"], p["legs"][-1]["to"]), []).append(p)
        kept_t = []
        for grp in groups.values():
            grp.sort(key=lambda p: (
                len(p["legs"]),
                _epoch(p["legs"][-1]["arrive"]),
                -_epoch(p["legs"][0]["depart"]),
                (_legs_cost(p["legs"])[0] if _legs_cost(p["legs"])[1] else float("inf")),
            ))
            accepted = []
            for p in grp:
                p_start = _epoch(p["legs"][0]["depart"])
                p_arr = _epoch(p["legs"][-1]["arrive"])
                p_trs = p.get("transfers", [])
                p_hubs = _hub_sig(p_trs)
                p_same, p_buf = _transfer_safety(p_trs, cross_extra)
                p_cost, _ = _legs_cost(p["legs"])
                p_tier = _train_tier(p["legs"], style_map=style_map)
                if any(
                    _dominates(
                        k["legs_cnt"], k["start"], k["arr"], k["hubs"],
                        k["same"], k["buf"], k["cost"], k["tier"],
                        len(p["legs"]), p_start, p_arr, p_hubs,
                        p_same, p_buf, p_cost, p_tier,
                    )
                    for k in accepted
                ):
                    _add_prune(prune_stats, "dominated")
                    continue
                accepted.append({
                    "plan": p, "legs_cnt": len(p["legs"]), "start": p_start, "arr": p_arr,
                    "hubs": p_hubs, "same": p_same, "buf": p_buf, "cost": p_cost, "tier": p_tier,
                })
            kept_t.extend(x["plan"] for x in accepted)
        transfers = kept_t

    direct.sort(key=lambda x: x["total_min"])
    transfers.sort(key=lambda x: x["total_min"])
    direct_total, transfers_total = len(direct), len(transfers) + extra_multi_unreturned
    if limit > 0:
        # limit 为直达与换乘的合计上限，直达优先占用额度（此前两者各自截断会翻倍返回）
        direct = direct[:limit]
        transfers = transfers[:max(0, limit - len(direct))]

    # 截断已前移到下面两处遍历之前：它们只扫真正返回的方案，否则 37 万条量级的枚举结果会被整体再扫两遍。
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

    # 供前端本地后置过滤复用精确站名→城市映射与以“市”结尾的物理站清单（如“南平市”）
    city_of_map, _, tele_of_map = cities
    station_cities = {}
    for p in direct + transfers:
        for l in p.get("legs") or []:
            for k in (l.get("from_telecode"), l.get("from"), l.get("to_telecode"), l.get("to")):
                if k and k in city_of_map:
                    station_cities[k] = city_of_map[k]
        for tr in p.get("transfers") or []:
            for k in (tr.get("telecode"), tr.get("station")):
                if k and k in city_of_map:
                    station_cities[k] = city_of_map[k]
    city_suffix_stations = sorted(nm for nm in tele_of_map if nm.endswith("市"))

    fb_impact = geo.fallback_impact_stats(geo_index) if geo_index else None
    coord_cov = None
    if geo_index and fb_impact:
        suspect_cnt = sum(1 for st in geo_index.status_of.values() if st == "suspect")
        missing_cnt = sum(1 for st in geo_index.status_of.values() if st == "missing")
        coord_cov = {
            "total": fb_impact["total_stations"],
            "ok": fb_impact["ok_stations"],
            "pct": fb_impact["ok_pct"],
            "suspect": suspect_cnt,
            "missing": missing_cnt,
        }
    radius_truncated = bool(truncation_flag[0])
    zero_override_active = bool(geo_index and geo_index.has_zero_override)

    return {
        "from": from_q, "to": to_q, "date": date,
        "max_legs": max_legs,
        "server_now": cn_now_str,
        "from_stations": [s["name"] for s in fstations],
        "to_stations": [s["name"] for s in tstations],
        "from_station_labels": [f"{s['name']}({s['telecode']})" for s in fstations],
        "to_station_labels": [f"{s['name']}({s['telecode']})" for s in tstations],
        "screen_trains": len(screen), "timetable_trains": len(tt),
        "cache_hit": cache_hit,
        "station_cities": station_cities,
        "city_suffix_stations": city_suffix_stations,
        "radius_truncated": radius_truncated,
        "zero_override_active": zero_override_active,
        "fallback_impact": fb_impact,
        "coord_coverage": coord_cov,
        "prune_config": {
            "prune_level": prune_level,
            "frontier_k": eff_fk,
            "max_total_min": eff_max_total,
            "detour_ratio": eff_detour,
            "detour_limit_min": int(round(detour_limit_min)) if detour_limit_min is not None else None,
            "prune_dominated": eff_dom,
            "auto_relaxed": auto_relaxed,
            "allow_cross_station": bool(allow_cross_station),
            "hub_only": bool(hub_only),
            "hub_stations": hub_tokens,
            "allow_prefixes": inc_pfx,
            "exclude_prefixes": exc_pfx,
            "include_stations": inc_st,
            "include_stations_mode": inc_st_mode,
            "exclude_stations": exc_st,
            "province_mode": p_mode,
            "allow_provinces": sorted(allow_provs),
            "exclude_provinces": sorted(exc_provs),
            "block_unknown_prov": bool(block_unknown_prov),
            "cross_radius_km": cross_cfg["radius_km"],
            "cross_time_mode": cross_cfg["time_mode"],
            "cross_extra": cross_cfg["fixed_extra"],
            "transit_discount_on": cross_cfg["transit_discount_on"],
            "transit_discount_min": cross_cfg["transit_discount_min"],
            "transit_proximity_m": cross_cfg["transit_proximity_m"],
            "active_group_ids": sorted(cross_cfg["active_group_ids"]),
            "fallback_ungeocoded": cross_cfg["fallback_ungeocoded"],
            "radius_truncated": radius_truncated,
            "zero_override_active": zero_override_active,
        },
        "prune_stats": prune_stats,
        "note": (
            f"{legs_note}"
            f"{'换乘/车型约束生效：搜前已自动关闭束宽与支配剪枝（其余剪枝参数仍生效） · ' if auto_relaxed else ''}"
            f"时间基准：北京时间(UTC+8) · "
            + (
                f"异站半径{cross_cfg['radius_km']:g}km·按距连续加时"
                + (
                    f"(同圈群≤{cross_cfg['transit_proximity_m']}m重轨估算最高-{cross_cfg['transit_discount_min']}分)"
                    if cross_cfg["transit_discount_min"]
                    else ""
                )
                if cross_cfg["time_mode"] == "smart"
                else f"异站半径{cross_cfg['radius_km']:g}km·固定预留{cross_cfg['fixed_extra']}分钟"
            )
            + (" · [Top-25近邻已截断]" if radius_truncated else "")
            + (" · [含未验证坐标同城回退90m]" if cross_cfg["fallback_ungeocoded"] else "")
            + " · 无当日数据时按最近日期与 running_fig 校验推算并剔除余票冲突车次"
        ),
        # 截断前的全量命中数：limit 生效时前端据此提示“另有 N 条未返回”，避免把上限误当总数
        "direct_total": direct_total,
        "transfers_total": transfers_total,
        "direct": direct,
        "transfers": transfers,
    }
