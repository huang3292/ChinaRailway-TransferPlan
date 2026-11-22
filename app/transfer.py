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
import json
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta

from . import db, fare, parsers

_CACHE_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()
_MEM_CACHE: dict | None = None
_DATE_CACHE_VER = None
_DATE_CACHE: dict[str, dict] = {}
_MAX_DATE_CACHE = 8


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
        # 同席别去重（保留最后写入）
        dedup = {x["seat_type"]: x for x in by_date[chosen_date]}
        out[key] = {"items": list(dedup.values()), "date": chosen_date, "stale": stale}
    return out


def _seat_map(target_date: str | None = None) -> tuple[dict, dict]:
    """一次查询同时构建余票明细映射与 (from,to,date)->train_no 冲突校验集。"""
    out = {}
    real_tk = {}
    sql = ("SELECT t.train_no,t.from_telecode,t.to_telecode,t.train_date,t.seats,t.can_web_buy,"
           "r.fetched_at FROM tickets t LEFT JOIN raw_payloads r ON r.id=t.raw_id")
    params = ()
    if target_date:
        # 多程跨天可能查到次日余票，仍按全日期索引或限定 >= target_date
        sql += " WHERE t.train_date >= ?"
        params = (target_date,)
    for tn, ftc, ttc, td, seats_js, cwb, fetched_at in db.get_conn().execute(sql, params):
        out[(tn, ftc, ttc, td)] = (seats_js, cwb, fetched_at)
        if ftc and ttc:
            real_tk.setdefault((ftc, ttc, td), set()).add(tn)
    return out, real_tk


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
        seats_js, cwb, fetched_at = tk
        leg["seats"] = json.loads(seats_js or "{}")
        leg["can_web_buy"] = cwb
        if fetched_at:
            leg["ticket_updated"] = fetched_at.replace("T", " ")[:16]
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
    """同站 + 同城异站的候选出发站键（按物理站去重，排除指定站）。"""
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
        vals = [float(x["price"]) for x in prices if x.get("price") is not None]
        if vals:
            return min(vals)
    est = leg.get("price_est")
    if est and isinstance(est.get("seats"), dict):
        vals = [float(x["min"]) for x in est["seats"].values() if x.get("min") is not None]
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


def _train_tier(legs: list[dict]) -> int:
    """缺价时按车型做保守分级：普速/混搭(0) < 动车(1) < 高铁/城际(2)。"""
    best = 0
    for leg in legs:
        codes = _train_codes(leg.get("train"))
        if not codes:
            continue
        tier = 2 if any(c[0] in ("G", "C") for c in codes) else (1 if any(c[0] == "D" for c in codes) else 0)
        best = max(best, tier)
    return best


def _transfer_safety(trs: list[dict], cross_extra: int) -> tuple[int, int]:
    """返回 (同站换乘次数, 最小有效换乘余量分钟数)，用于保护更稳妥的换乘方案。"""
    if not trs:
        return (0, 999999)
    same_cnt = sum(1 for t in trs if t.get("same", True))
    min_buf = min(
        (t.get("wait_min", 0) if t.get("same", True) else t.get("wait_min", 0) - cross_extra)
        for t in trs
    )
    return (same_cnt, min_buf)


def _hub_sig(trs: list[dict]) -> tuple:
    """提取中转枢纽路径标识（仅同中转站与同/异站属性完全一致才属同枢纽）。"""
    return tuple((t.get("telecode") or t.get("station") or "", bool(t.get("same", True))) for t in trs)


def _dominates(k_legs: int, k_start: int, k_arr: int, k_hubs: tuple,
               k_same: int, k_buf: int, k_cost: float | None, k_tier: int,
               p_legs: int, p_start: int, p_arr: int, p_hubs: tuple,
               p_same: int, p_buf: int, p_cost: float | None, p_tier: int) -> bool:
    """同级同枢纽支配判定：价格更低或换乘更稳妥时不剪除。"""
    if k_legs > p_legs or k_start < p_start or k_arr > p_arr:
        return False
    if k_hubs != p_hubs:
        return False
    if k_same < p_same:
        return False
    if k_buf < SAFE_WAIT_MIN and p_buf > k_buf:
        return False
    if k_cost is not None and p_cost is not None:
        if k_cost > p_cost + 1e-6:
            return False
    elif k_tier > p_tier:
        return False
    return True


def _raw_leg_cost(train_no: str, code: str | None, fno, tno, ftc, ttc, dur_min: int,
                  prices: dict, sno: dict, cal: dict, cache: dict | None = None) -> float | None:
    """快速提取或估算单程最低参考票价（用于剪枝阶段的价格保护与双路束宽）。"""
    if sno:
        if fno is None and ftc:
            fno = sno.get((train_no, ftc))
        if tno is None and ttc:
            tno = sno.get((train_no, ttc))
    if fno is not None and tno is not None:
        p = prices.get((train_no, str(fno).zfill(2), str(tno).zfill(2)))
        if p and p.get("items"):
            vals = [float(x["price"]) for x in p["items"] if x.get("price") is not None]
            if vals:
                return min(vals)
    if code and dur_min and dur_min > 0:
        ckey = (code, dur_min)
        if cache is not None and ckey in cache:
            return cache[ckey]
        est = fare.estimate(code, duration_min=dur_min, cal=cal)
        res = None
        if est and isinstance(est.get("seats"), dict):
            vals = [float(x["min"]) for x in est["seats"].values() if x.get("min") is not None]
            if vals:
                res = min(vals)
        if cache is not None:
            cache[ckey] = res
        return res
    return None


def _train_codes(code: str | None) -> frozenset[str]:
    """拆分合并车次号（如 G123/G126 -> {'G123', 'G126'}），用于严格判重。"""
    if not code:
        return frozenset()
    return frozenset(p.strip().upper() for p in str(code).split("/") if p.strip())


def _parse_tokens(s: str | None) -> list[str]:
    if not s:
        return []
    return [x.strip().upper() for x in re.split(r"[,，\s]+", str(s)) if x.strip()]


def _match_prefix(code: str | None, prefixes: list[str]) -> bool:
    if not prefixes:
        return True
    parts = _train_codes(code)
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


def _code_allowed(code: str | None, inc_pfx: list[str], exc_pfx: list[str]) -> bool:
    if inc_pfx and not _match_prefix(code, inc_pfx):
        return False
    if exc_pfx and _match_prefix(code, exc_pfx):
        return False
    return True


def _resolve_hub_tokens(hub_only: bool, hub_stations: str | list[str] | None) -> list[str]:
    if not hub_only:
        return []
    toks = _parse_tokens(hub_stations) if not isinstance(hub_stations, (list, tuple)) else [str(x).strip().upper() for x in hub_stations if str(x).strip()]
    return toks if toks else list(MAJOR_HUB_TOKENS)


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
) -> list[dict]:
    """独立后置过滤器：按车型头、必经/排除换乘站（精确站名/城市+OR/AND）、同异站过滤已生成的方案。"""
    inc_pfx = _parse_tokens(allow_prefixes) if not isinstance(allow_prefixes, (list, tuple)) else [str(x).strip().upper() for x in allow_prefixes if str(x).strip()]
    exc_pfx = _parse_tokens(exclude_prefixes) if not isinstance(exclude_prefixes, (list, tuple)) else [str(x).strip().upper() for x in exclude_prefixes if str(x).strip()]
    inc_st = _parse_tokens(include_stations) if not isinstance(include_stations, (list, tuple)) else [str(x).strip().upper() for x in include_stations if str(x).strip()]
    exc_st = _parse_tokens(exclude_stations) if not isinstance(exclude_stations, (list, tuple)) else [str(x).strip().upper() for x in exclude_stations if str(x).strip()]
    inc_mode = "and" if str(include_stations_mode or "or").strip().lower() == "and" else "or"
    if not inc_pfx and not exc_pfx and not inc_st and not exc_st and allow_cross_station:
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

    out = []
    for p in plans:
        legs = p.get("legs") or []
        if (inc_pfx or exc_pfx) and not all(_code_allowed(l.get("train"), inc_pfx, exc_pfx) for l in legs):
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
                    dk_name = nl.get("from")
                    dk_key = nl.get("from_telecode") or dk_name
                else:
                    dk_name, dk_key = sk_name, sk_key
                pairs.append((sk_key, sk_name, dk_key, dk_name))
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
                   prune_cfg=None, prune_stats=None, sub_cb=None, need_transfers=True):
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
    max_total_min = prune_cfg.get("max_total_min", 2880)

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
            code=a.get("code") or t["code"], km=km, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

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
        for fo, shift in fo_cands:
            a_code = fo.get("code") or t["code"]
            a_codes = _train_codes(a_code)
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
                dep_cands = _dep_keys(sk, so.get("name"), available, cities, exclude=passed_canon, allow_cross=allow_cross)
                if hub_tokens:
                    hub_cands = [dk for dk in dep_cands if _match_station_tokens(sk, so.get("name"), city_of, tele_of, hub_tokens) or _match_station_tokens(dk, so.get("name") if dk == sk else name_of.get(dk), city_of, tele_of, hub_tokens)]
                    if not hub_cands:
                        _add_prune(prune_stats, "hub_filter")
                        continue
                    dep_cands = hub_cands
                for dk in dep_cands:
                    same = dk == sk
                    eff_min = min_wait if same else min_wait + cross_extra
                    for bn in by_station.get(dk, ()):
                        if bn not in to_obs:
                            continue
                        b = trains[bn]
                        bs = b["obs"].get(dk)
                        if not bs or "dep" not in bs:
                            continue
                        b_code = bs.get("code") or b["code"]
                        if bn[0] == tn[0] or (a_codes and (a_codes & _train_codes(b_code))):
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
                                # 第二程途经已离开的前序城市非终点站 → 绕圈折返
                                if closed_cities and any(
                                    k2 not in tkeys and o2.get("no") is not None
                                    and bs["no"] < o2["no"] < bt["no"]
                                    and _city_of_key(k2, o2.get("name"), city_of, tele_of) in closed_cities
                                    for k2, o2 in b["obs"].items()
                                ):
                                    _add_prune(prune_stats, "city_loop")
                                    continue
                            total_min = bt["arr"] + n * 1440 - a_dep
                            if max_total_min and total_min > max_total_min:
                                _add_prune(prune_stats, "total_time")
                                continue
                            is_assumed = (assumed or n != 0 or bool(so.get("bridged"))
                                          or bool(bs.get("bridged")) or bool(bt.get("bridged")))
                            transfers.append({
                                "total_min": total_min, "wait_min": wait,
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
        base = _date_min(src.train_date)
        if base is None or abs(sd - base) > 7 * 1440:
            continue
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
                      need_transfers=True):
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
    max_total_min = prune_cfg.get("max_total_min", 2880) or 0
    eff_detour = float(prune_cfg.get("detour_ratio") or 0.0)
    eff_dom = bool(prune_cfg.get("prune_dominated", False))
    real_tk = real_tk or {}
    n2c = n2c or {}
    city_of, _, tele_of = cities
    name_of = {tc: nm for nm, tc in tele_of.items()}

    def _leg_conflict_raw(train_no, a, b, dep_date_str):
        if not real_tk:
            return False
        ftc = a["telecode"] or n2c.get(a["station_name"])
        ttc = b["telecode"] or n2c.get(b["station_name"])
        if not ftc or not ttc:
            return False
        key = (ftc, ttc, dep_date_str)
        return key in real_tk and train_no not in real_tk[key]

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
            m = (_train_codes(T.code), st_keys, st_canons, st_cities)
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
            code=t.code, duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

    # 先算时刻表直达，立即得到最优直达用时用于绕行剪枝上界
    direct = []
    best_min = init_best_min if (init_best_min and init_best_min > 0) else None
    for A, fi, tj in from_cands:
        if not tj:
            continue
        for i in fi:
            shift_a = -(A.stops[i]["dep"] // 1440) * 1440
            dep_date_a = _fmt(base, A.stops[i]["dep"] + shift_a)[:10]
            for j in tj:
                if j > i:
                    dur = A.stops[j]["arr"] - A.stops[i]["dep"]
                    if max_total_min and dur > max_total_min:
                        _add_prune(prune_stats, "total_time")
                        continue
                    l = leg(A, i, j, shift_a)
                    direct.append({"total_min": dur, "date_assumed": True,
                                   "source_date": A.train_date, "legs": [l]})
                    if not _leg_conflict_raw(A.train_no, A.stops[i], A.stops[j], dep_date_a):
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
                if _leg_conflict_raw(A.train_no, A.stops[i], S, dep_date_a):
                    forbidden.add(sk)
                    forbidden_canon.add(a_canons[s_idx])
                    seen_cities.add(sc_city)
                    continue

                closed_cities = seen_cities - {sc_city}
                a_arr_s = S["arr"] + shift_a
                dep_cands = _dep_keys(sk, S["station_name"], available, cities,
                                      exclude=forbidden_canon, allow_cross=allow_cross)
                if hub_tokens:
                    hub_cands = [dk for dk in dep_cands if _match_station_tokens(sk, S["station_name"], city_of, tele_of, hub_tokens) or _match_station_tokens(dk, S["station_name"] if dk == sk else name_of.get(dk), city_of, tele_of, hub_tokens)]
                    if not hub_cands:
                        _add_prune(prune_stats, "hub_filter")
                        forbidden.add(sk)
                        forbidden_canon.add(a_canons[s_idx])
                        seen_cities.add(sc_city)
                        continue
                    dep_cands = hub_cands
                for dk in dep_cands:
                    same = dk == sk
                    b_list = b_at_station.get(dk)
                    if not b_list:
                        continue
                    eff_min = min_wait if same else min_wait + cross_extra
                    span = max_wait - eff_min
                    if span < 0:
                        continue

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
                                if has_city_loop or (yc in closed_cities and yk not in tkeys):
                                    _add_prune(prune_stats, "city_loop")
                                else:
                                    total_min = B.stops[y]["arr"] + b_shift - a_dep
                                    if max_total_min and total_min > max_total_min:
                                        _add_prune(prune_stats, "total_time")
                                    elif detour_cap and total_min > detour_cap:
                                        _add_prune(prune_stats, "detour")
                                    elif not (real_tk and _leg_conflict_raw(B.train_no, B.stops[k], B.stops[y], b_dep_date)):
                                        b_arr_abs = B.stops[y]["arr"] + b_shift
                                        raw_transfers.append((
                                            total_min, wait, a_dep, b_arr_abs,
                                            A, i, s_idx, shift_a, B, k, y, b_shift,
                                            S["station_name"], same, A.train_date or B.train_date,
                                        ))
                                        if best_min is None or total_min < best_min:
                                            best_min = total_min
                                            if eff_detour > 0:
                                                detour_cap = max(DETOUR_FLOOR_MIN, best_min * eff_detour)
                                b_to_ptr += 1
                                if b_to_ptr >= n_b_to:
                                    break
                                next_m = b_to[b_to_ptr]
                            if yc in closed_cities and yk not in tkeys:
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
            # rt = (total_min, wait, a_dep, b_arr_abs, A, i, s_idx, shift_a, B, k, y, b_shift, sname, same, sdate)
            A, i, s_idx, B, k, y = rt[4], rt[5], rt[6], rt[8], rt[9], rt[10]
            groups.setdefault((A.stops[i]["station_name"], B.stops[y]["station_name"]), []).append(rt)
        kept = []
        for grp in groups.values():
            grp.sort(key=lambda r: (r[3], -r[2]))
            accepted = []
            for rt in grp:
                wait, p_start, p_arr, A, i, s_idx, _sa, B, k, y, _sb, t_sname, same, _sd = rt[1:]
                sa1, sa2 = A.stops[i], A.stops[s_idx]
                sb1, sb2 = B.stops[k], B.stops[y]
                c1 = _raw_leg_cost(A.train_no, A.code, sa1["station_no"], sa2["station_no"],
                                   sa1["telecode"], sa2["telecode"], sa2["arr"] - sa1["dep"],
                                   prices, sno, cal, est_cache)
                c2 = _raw_leg_cost(B.train_no, B.code, sb1["station_no"], sb2["station_no"],
                                   sb1["telecode"], sb2["telecode"], sb2["arr"] - sb1["dep"],
                                   prices, sno, cal, est_cache)
                p_cost = (c1 + c2) if (c1 is not None and c2 is not None) else None
                p_tier = _train_tier([{"train": A.code}, {"train": B.code}])
                p_hubs = ((sa2["telecode"] or t_sname or "", bool(same)),)
                p_same = 1 if same else 0
                p_buf = wait if same else wait - cross_extra
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
    for total_min, wait, _ad, _ba, A, i, s_idx, shift_a, B, k, y, b_shift, t_sname, same, sdate in raw_transfers:
        transfers.append({
            "total_min": total_min, "wait_min": wait,
            "transfer_station": t_sname, "same_station": same,
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


def _search_multi(services, fkeys, tkeys, base_min, min_wait, max_wait, max_legs,
                  prices, seats, cal, sno, cities, real_tk=None, n2c=None,
                  cross_extra=60, frontier_k=FRONTIER_K,
                  cancel_event=None, depth_cb=None,
                  prune_cfg=None, prune_stats=None,
                  init_sub_cb=None, depth_sub_cb=None) -> list[dict]:
    """BFS 多程换乘，返回 3..max_legs 程的换乘方案（反向可达时间下界剪枝 + 二分时间窗 + 延迟丰富化）。"""
    if max_legs < 3:
        return []
    prune_cfg = prune_cfg or {}
    hub_tokens = prune_cfg.get("hub_tokens") or []
    allow_cross = prune_cfg.get("allow_cross_station", True)
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
    city_of, _, tele_of = cities
    name_of = {tc: nm for nm, tc in tele_of.items()}
    seats_keys = set(seats)

    # 1) 过滤有效车次并预计算停站元数据与按发车时刻排序的站车倒排索引
    allowed_sv = []
    sv_meta = []       # (sv_codes, stop_keys, stop_canons, stop_cities, dest_indices)
    by_station_raw = {}
    n_sv = len(services)
    for si_raw, sv in enumerate(services):
        if (si_raw & 127) == 0 or si_raw + 1 == n_sv:
            _check_cancel(cancel_event)
            if init_sub_cb is not None:
                init_sub_cb(int((si_raw + 1) * 30 / max(1, n_sv)), 100,
                            f"构建多程路网（{n_sv} 车次）：建索引 {si_raw + 1}/{n_sv}")
        si = len(allowed_sv)
        allowed_sv.append(sv)
        stops = sv["stops"]
        s_keys = [st["key"] for st in stops]
        s_canons = [tele_of.get(k, k) for k in s_keys]
        s_cities = [_city_of_key(st["key"], st["name"], city_of, tele_of) for st in stops]
        dest_idx = tuple(idx for idx, st in enumerate(stops) if st["enabled"] and st["key"] in tkeys)
        sv_meta.append((_train_codes(sv.get("code")), s_keys, s_canons, s_cities, dest_idx))
        for i, st in enumerate(stops):
            if st["enabled"]:
                by_station_raw.setdefault(st["key"], []).append((st["dep"] % 1440, si, i))

    available = set(by_station_raw)
    by_station = {}
    by_dep_mods = {}
    for dk, lst in by_station_raw.items():
        lst.sort(key=lambda x: x[0])
        by_station[dk] = [(x[1], x[2]) for x in lst]
        by_dep_mods[dk] = [x[0] for x in lst]
    del by_station_raw

    # 2) 计算精确反向可达性与可容许最短剩余时间下界 min_lb[rem]（1 <= rem < max_legs）
    #    任意深度 d (rem = max_legs - d) 若 b 不可达或 elapsed + lb(rem, b) > time_limit，即可安全剪除
    min_lb_direct: dict[int, dict[str, int]] = {}
    min_lb_city: dict[int, dict[str, int]] = {}
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

    d1_dir, d1_city = {}, {}
    for dk, ride in edge1.items():
        w_same = min_wait + ride
        if dk not in d1_dir or w_same < d1_dir[dk]:
            d1_dir[dk] = w_same
        if allow_cross:
            c = _city_of_key(dk, None, city_of, tele_of)
            w_cross = min_wait + cross_extra + ride
            if c not in d1_city or w_cross < d1_city[c]:
                d1_city[c] = w_cross
    min_lb_direct[1] = d1_dir
    min_lb_city[1] = d1_city

    def lb_exact(rem: int, key: str, city: str) -> int:
        v = min_lb_direct[rem].get(key, _INF)
        if allow_cross:
            vc = min_lb_city[rem].get(city, _INF)
            if vc < v:
                v = vc
        return v

    for rem in range(2, max_legs):
        edge_r: dict[str, int] = {}
        prev_dir = min_lb_direct[rem - 1]
        prev_city = min_lb_city[rem - 1]
        for si, sv in enumerate(allowed_sv):
            stops = sv["stops"]
            s_keys = sv_meta[si][1]
            s_cities = sv_meta[si][3]
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
                    vc = prev_city.get(s_cities[idx], _INF)
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
                c = _city_of_key(dk, None, city_of, tele_of)
                w_cross = min_wait + cross_extra + cost
                if c not in cr or w_cross < cr[c]:
                    cr[c] = w_cross
        min_lb_direct[rem] = dr
        min_lb_city[rem] = cr

    def lb_at_depth(depth_cur: int, key: str, city: str) -> int:
        """从 depth_cur (1..max_legs-1) 出发完成总程数 >= 3 且 <= max_legs 的最短剩余时间下界。"""
        rem = max_legs - depth_cur
        if depth_cur == 1:
            # 首程后至少还需 2 程才属于 >= 3 程方案
            best = _INF
            for r in range(2, rem + 1):
                v = lb_exact(r, key, city)
                if v < best:
                    best = v
            return best
        if rem == 1:
            return lb_exact(1, key, city)
        best = _INF
        for r in range(1, rem + 1):
            v = lb_exact(r, key, city)
            if v < best:
                best = v
        return best

    def leg_conflict_raw(sv, a, b, dep_epoch: int) -> bool:
        if not real_tk:
            return False
        ftc = a["telecode"] or n2c.get(a["name"])
        ttc = b["telecode"] or n2c.get(b["name"])
        if not ftc or not ttc:
            return False
        key = (ftc, ttc, _fmt_epoch(dep_epoch)[:10])
        return key in real_tk and sv["train_no"] not in real_tk[key]

    def has_seat_raw(sv, a, b, dep_epoch: int) -> bool:
        return bool(seats_keys) and (sv["train_no"], a["telecode"], b["telecode"], _fmt_epoch(dep_epoch)[:10]) in seats_keys

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
            duration_min=b["arr"] - a["dep"], cal=cal, sno=sno)

    est_cache = {}

    def ref_leg_cost(ref) -> tuple[float | None, int]:
        si, ii, jj, _n = ref
        sv = allowed_sv[si]
        sa, sb = sv["stops"][ii], sv["stops"][jj]
        c = _raw_leg_cost(sv["train_no"], sv.get("code"), sa["no"], sb["no"],
                          sa["telecode"], sb["telecode"], sb["arr"] - sa["dep"],
                          prices, sno, cal, est_cache)
        return c, _train_tier([{"train": sv.get("code")}])

    def refs_cost_tier(l_refs: tuple) -> tuple[float, bool, int]:
        tot, known, mx_tier = 0.0, True, 0
        for ref in l_refs:
            c, t_lv = ref_leg_cost(ref)
            mx_tier = max(mx_tier, t_lv)
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
        min_buf = min((tr[3] if tr[2] else tr[3] - cross_extra) for tr in tr_refs)
        return hubs, same_cnt, min_buf

    # 3) 初始状态（第 1 程）：结合可达下界剪掉死分支，并在开启 prune_dominated 时做价格/车型感知首程支配压缩
    init = []
    fk_items = [(si, i) for fk in fkeys for si, i in by_station.get(fk, ())]
    n_fk = len(fk_items)
    for idx, (si, i) in enumerate(fk_items, 1):
        _check_cancel(cancel_event)
        if init_sub_cb is not None:
            init_sub_cb(30 + int(idx * 70 / max(1, n_fk)), 100,
                        f"构建多程路网（{n_allowed} 车次）：初始化首程 {idx}/{n_fk}，分支 {len(init)} 个")
        sv = allowed_sv[si]
        stops = sv["stops"]
        sv_codes, s_keys, s_canons, s_cities, _ = sv_meta[si]
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
            lb = lb_at_depth(1, bk, b_city)
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
                        c_dks = _dep_keys(bk, b["name"], available, cities,
                                          exclude=frozenset(s_canons[i:j + 1]), allow_cross=True)
                        cross_ok = any(
                            dk2 != bk and _match_station_tokens(dk2, name_of.get(dk2), city_of, tele_of, hub_tokens)
                            for dk2 in c_dks
                        )
                    if not cross_ok:
                        _add_prune(prune_stats, "hub_filter")
                        continue
            if leg_conflict_raw(sv, st, b, dep0):
                continue
            arr0 = base_min + b["arr"] + n0 * 1440
            passed = frozenset(s_canons[i:j + 1])
            closed_cities = frozenset(s_cities[i:j]) - frozenset((b_city,))
            c0, c0_known, tier0 = refs_cost_tier(((si, i, j, n0),))
            init.append({
                "arr": arr0, "station": bk, "name": b["name"], "city": b_city,
                "to_tc": b["telecode"],
                "passed": passed, "closed_cities": closed_cities,
                "used_trains": frozenset((sv["train_no"],)), "used_codes": sv_codes,
                "leg_refs": ((si, i, j, n0),), "tr_refs": (),
                "start": dep0, "has_seats": has_seat_raw(sv, st, b, dep0),
                "cost": c0, "cost_known": c0_known, "tier": tier0,
                "hubs": (), "same_cnt": 0, "min_buf": 999999,
            })

    if prune_dominated and len(init) > 1:
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

    # 内存优化：未开启支配剪枝时“入桶即截断”——每站只维护 fast/cheap 两维
    # top-frontier_k 有界列表，落选分支立即释放。与整层建完再排序截断结果完全一致：
    # 稳定排序的平局用插入计数器还原，交替选取至多取每维前 frontier_k 个。
    bounded = (not prune_dominated) and has_fk

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
            t = state["arr"]
            cur_elapsed = t - state["start"]
            if time_limit < _INF and cur_elapsed > time_limit:
                if max_total_min and cur_elapsed > max_total_min:
                    _add_prune(prune_stats, "total_time")
                else:
                    _add_prune(prune_stats, "detour")
                continue
            cur_city = state["city"]
            for dk in _dep_keys(state["station"], state["name"], available, cities,
                                exclude=state["passed"], allow_cross=allow_cross):
                same = dk == state["station"]
                dk_name = state["name"] if same else name_of.get(dk)
                if hub_tokens and not (
                    _match_station_tokens(state["station"], state["name"], city_of, tele_of, hub_tokens)
                    or _match_station_tokens(dk, dk_name, city_of, tele_of, hub_tokens)
                ):
                    _add_prune(prune_stats, "hub_filter")
                    continue
                sv_list = by_station.get(dk)
                if not sv_list:
                    continue
                eff_min = min_wait if same else min_wait + cross_extra
                span = span_all - eff_min
                if span < 0:
                    continue
                dk_city = _city_of_key(dk, None, city_of, tele_of)

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
                    sv_codes, s_keys, s_canons, s_cities, dest_idx = sv_meta[si]
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
                                            new_trs = state["tr_refs"] + ((state["name"], state["to_tc"], same, wait, dk, dk_name),)
                                            raw_results.append((
                                                total, state["start"], arr_y,
                                                state["leg_refs"] + ((si, i, y, n),), new_trs,
                                            ))
                                dest_pos += 1
                                if dest_pos >= len(dest_idx):
                                    break
                            if b_city != dk_city:
                                left_dk_city = True
                            if (b_city in state["closed_cities"]) or (left_dk_city and b_city == dk_city):
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
                                                new_trs = state["tr_refs"] + ((state["name"], state["to_tc"], same, wait, dk, dk_name),)
                                                raw_results.append((
                                                    total, state["start"], arr_j,
                                                    state["leg_refs"] + ((si, i, j, n),), new_trs,
                                                ))
                                else:
                                    break
                            if b_city != dk_city:
                                left_dk_city = True
                            if (b_city in state["closed_cities"]) or (left_dk_city and b_city == dk_city):
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
                            lb = lb_at_depth(depth, bk, b_city)
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
                                        c_dks = _dep_keys(bk, b["name"], available, cities,
                                                          exclude=excl_tmp, allow_cross=True)
                                        cross_ok = any(
                                            dk2 != bk and _match_station_tokens(dk2, name_of.get(dk2), city_of, tele_of, hub_tokens)
                                            for dk2 in c_dks
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
                            new_trrefs = state["tr_refs"] + ((state["name"], state["to_tc"], same, wait, dk, dk_name),)
                            c_tot, c_known, c_tier = refs_cost_tier(new_lrefs)
                            hubs, same_cnt, min_buf = tr_meta(new_trrefs)
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

    # BFS 结束后立即释放索引与前沿状态，为结果丰富化腾出内存
    del frontier, nxt, by_station, by_dep_mods, sv_meta, seats_keys

    # 5) 先完成去重与同级同枢纽支配剪枝，最后仅对优胜方案调用 make_leg_tuple（_enrich + fare.estimate）
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

    out = []
    for total, _st_ep, _arr_ep, l_refs, tr_refs in final_raw:
        legs = [make_leg_tuple(r) for r in l_refs]
        transfers = [{"station": tr[0], "telecode": tr[1], "same": tr[2], "wait_min": tr[3]}
                     for tr in tr_refs]
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
    """将全量车站、票价校准、站序、大屏与时刻表路网构建为常驻内存快照。"""
    n2c = {}
    city_of, city_groups, tele_of = {}, {}, {}
    for tc, nm, ct in db.get_conn().execute("SELECT telecode,name,city FROM stations"):
        n2c[nm] = tc
        tele_of[nm] = tc
        if ct:
            city_of[tc] = ct
            city_of[nm] = ct
            city_groups.setdefault(ct, set()).update((tc, nm))
    cities = (city_of, city_groups, tele_of)
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

    services_all = _load_services(screen_all, tt_all, cancel_event=cancel_event)
    return {
        "version": ver,
        "n2c": n2c,
        "cities": cities,
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
    seats, real_tk = _seat_map(target_date)
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
    entry = {"prices": prices, "seats": seats, "real_tk": real_tk, "sno": sno}

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
           progress_cb=None, cancel_event=None, sub_cb=None) -> dict:
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

    # WHY: 束宽(frontier_k)与支配剪枝在过滤之前按"局部最优"扔分支，会把"尚未经过必经站/
    # 尚未用上允许车型"的合法分支误剪，后置过滤无法补回（实测 3 程必经 balanced=260 vs 无束宽=6314）。
    # 故任一换乘/车型约束生效且用户未显式指定这两项剪枝参数时，搜前自动关闭它们，
    # 让约束方案完整进入后置过滤；硬防死循环规则与用户显式参数仍生效。
    auto_relaxed = False
    if (inc_pfx or exc_pfx or inc_st or exc_st) and frontier_k is None and prune_dominated is None:
        if eff_fk or eff_dom:
            eff_fk, eff_dom = 0, False
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
        "hub_filter": 0, "custom_rule": 0,
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
    fstations = db.resolve_stations(from_q, expand_city)
    tstations = db.resolve_stations(to_q, expand_city)
    if not fstations or not tstations:
        missing = from_q if not fstations else to_q
        raise ValueError(f"车站/城市“{missing}”不存在，请先在「爬取数据」页更新车站站名表")
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
    sno = d_cache["sno"]
    n2c = g["n2c"]
    cal = g["cal"]
    cities = g["cities"]

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
                            sub_cb=sub_step, need_transfers=need_transfers)

    def _assumed_conflict(p) -> bool:
        """推算方案（无当日真实数据平移而来）若与实际余票冲突，则以实际余票为准剔除。"""
        if not p.get("date_assumed") or not real_tk:
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

    non_conflicted_1 = [p["total_min"] for p in d1 + t1 if not _assumed_conflict(p)]
    init_best_min = min(non_conflicted_1) if non_conflicted_1 else None

    step(4, f"计算时刻表直达{'/换乘' if need_transfers else ''}（{len(tt)} 车次）")
    tt_sub_cap = 70 if max_legs >= 3 else 100
    d2, t2 = _search_timetable(tt, fkeys, tkeys, base, min_wait, max_wait,
                               prices, seats, cal, sno, cities, cross_extra=cross_extra,
                               cancel_event=cancel_event, prune_cfg=prune_cfg, prune_stats=prune_stats,
                               sub_cb=sub_step, sub_cap=tt_sub_cap,
                               real_tk=real_tk, n2c=n2c, init_best_min=init_best_min,
                               need_transfers=need_transfers)

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
    del merged_d, merged_t, d1, d2, t1, t2

    # 引擎 D：多程换乘（3 程及以上，每日开行假设）
    if max_legs >= 3:
        if eff_detour > 0:
            non_conf_min = [p["total_min"] for p in direct + transfers if not _assumed_conflict(p)]
            if non_conf_min:
                prune_cfg["detour_cap"] = max(DETOUR_FLOOR_MIN, min(non_conf_min) * eff_detour)
        sub_step(70, 100, f"复用内存路网服务（{len(g['services_all'])} 车次）…", force=True)
        services = g["services_all"]
        transfers.extend(_search_multi(
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
        ))
        del services

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
                dur = _colon_dur(r["duration"])
                if dur is None or dur <= 0:
                    dur = arr - dep
                    if dur < 0:
                        dur += 1440
                if eff_max_total and dur > eff_max_total:
                    _add_prune(prune_stats, "total_time")
                    continue
                direct.append({"total_min": dur, "source_date": date, "legs": [_enrich({
                    "train": r["station_train_code"], "train_no": r["train_no"],
                    "from": fst["name"], "to": tst["name"],
                    "from_telecode": r["from_telecode"], "to_telecode": r["to_telecode"],
                    "depart": _fmt(base, dep), "arrive": _fmt(base, dep + dur),
                    "duration_min": dur,
                }, r["train_no"], r["from_station_no"], r["to_station_no"],
                    r["from_telecode"], r["to_telecode"], date, prices, seats,
                    code=r["station_train_code"], duration_min=dur, cal=cal, sno=sno)]})

    direct = [p for p in direct if not _assumed_conflict(p)]
    transfers = [p for p in transfers if not _assumed_conflict(p)]

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
                p_tier = _train_tier(p["legs"])
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

    # 车次头与换乘站过滤已从图搜索剥离为统一后置过滤（API/测试调用传入时仍自动在末尾生效）
    if inc_pfx or exc_pfx or inc_st or exc_st:
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
        )

    direct.sort(key=lambda x: x["total_min"])
    transfers.sort(key=lambda x: x["total_min"])
    direct_total, transfers_total = len(direct), len(transfers)
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
        },
        "prune_stats": prune_stats,
        "note": f"{legs_note}{'换乘/车型约束生效：搜前已自动关闭束宽与支配剪枝（其余剪枝参数仍生效） · ' if auto_relaxed else ''}时间基准：北京时间(UTC+8) · 异站换乘默认额外预留{cross_extra}分钟 · 无当日数据时按最近日期与 running_fig 校验推算并剔除余票冲突车次",
        # 截断前的全量命中数：limit 生效时前端据此提示“另有 N 条未返回”，避免把上限误当总数
        "direct_total": direct_total,
        "transfers_total": transfers_total,
        "direct": direct,
        "transfers": transfers,
    }
