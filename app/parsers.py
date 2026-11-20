"""解析 12306 各接口的原始返回，格式与官方 API 完全一致。"""
import json
import re

# leftTicket 竖线分隔字段索引（2026-11 实测）
LT = {
    "train_no": 2, "station_train_code": 3,
    "start_station_telecode": 4, "end_station_telecode": 5,
    "from_station_telecode": 6, "to_station_telecode": 7,
    "start_time": 8, "arrive_time": 9, "duration": 10,
    "can_web_buy": 11, "start_train_date": 13,
    "from_station_no": 16, "to_station_no": 17,
    "seat_types": 35,
}
SEAT_FIELDS = {
    20: "yxyz", 21: "gr", 22: "qt", 23: "rw", 24: "rz", 25: "tz", 26: "wz",
    27: "yb", 28: "yw", 29: "yz", 30: "ze", 31: "zy", 32: "swz", 33: "srrb",
}
SEAT_NAMES = {
    "swz": "商务座", "tz": "特等座", "yxyz": "优选一等", "zy": "一等座", "ze": "二等座",
    "gr": "高级软卧", "rw": "软卧", "yw": "硬卧", "rz": "软座",
    "yz": "硬座", "wz": "无座", "srrb": "动卧", "qt": "其他", "yb": "预留",
    "dwo": "动卧", "fwo": "高级动卧", "ydw": "一等卧", "edw": "二等卧",
}
# queryTicketPrice / yp_info_new 返回键 -> 席别
PRICE_KEYS = {
    "A9": "swz", "9": "swz", "P": "tz", "M": "zy", "O": "ze",
    "A6": "gr", "6": "gr", "F": "fwo", "D": "dwo",
    "I": "ydw", "J": "edw",
    "A4": "rw", "4": "rw", "A3": "yw", "3": "yw",
    "A2": "rz", "2": "rz", "A1": "yz", "1": "yz", "WZ": "wz",
}


def norm_date(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def parse_stations(text: str) -> list[dict]:
    m = re.search(r"station_names\s*=\s*'(.*)'", text, re.S)
    if not m:
        raise ValueError("不是有效的 station_name.js 内容")
    out = []
    for item in m.group(1).split("@"):
        if not item.strip():
            continue
        p = item.split("|")
        if len(p) < 5:
            continue
        out.append({
            "telecode": p[2], "name": p[1], "pinyin": p[3], "short": p[4],
            "city": p[7] if len(p) > 7 else None,
        })
    return out


def parse_left_ticket(payload: dict, ctx: dict | None = None) -> list[dict]:
    ctx = ctx or {}
    data = payload.get("data") or {}
    result = data.get("result") or []
    out = []
    for line in result:
        if isinstance(line, dict):  # 部分接口变体 result 内嵌 {"queryLeftNewDTO": ...}
            line = line.get("queryLeftNewDTO", "")
        f = line.split("|")
        if len(f) < 34:
            continue
        seats = {code: f[i] for i, code in SEAT_FIELDS.items() if f[i] != ""}
        out.append({
            "train_no": f[LT["train_no"]],
            "station_train_code": f[LT["station_train_code"]],
            # 乘车日期以查询日期为准；start_train_date 是始发站发车日，长途车会早一两天
            "train_date": norm_date(ctx.get("date") or f[LT["start_train_date"]]),
            "from_telecode": f[LT["from_station_telecode"]],
            "to_telecode": f[LT["to_station_telecode"]],
            "from_station_no": f[LT["from_station_no"]],
            "to_station_no": f[LT["to_station_no"]],
            "start_time": f[LT["start_time"]],
            "arrive_time": f[LT["arrive_time"]],
            "duration": f[LT["duration"]],
            "can_web_buy": f[LT["can_web_buy"]],
            "seat_types": f[LT["seat_types"]] if len(f) > LT["seat_types"] else "",
            "seats": seats,
        })
    return out


def parse_left_ticket_prices(payload: dict, ctx: dict | None = None) -> list[dict]:
    """从 leftTicket 字段[39]（yp_info_new，每10字符=席别1+角价5+余票4）提取实时票价。"""
    ctx = ctx or {}
    data = payload.get("data") or {}
    result = data.get("result") or []
    out = []
    for line in result:
        if isinstance(line, dict):
            line = line.get("queryLeftNewDTO", "")
        f = line.split("|")
        if len(f) <= 39 or not f[39]:
            continue
        yp = f[39].strip()
        if len(yp) < 10 or len(yp) % 10 != 0:
            continue
        train_no = f[LT["train_no"]]
        code = f[LT["station_train_code"]]
        train_date = norm_date(ctx.get("date") or f[LT["start_train_date"]])
        fno, tno = f[LT["from_station_no"]], f[LT["to_station_no"]]
        if not (train_no and train_date and fno and tno):
            continue
        dedup = {}
        for i in range(0, len(yp), 10):
            chunk = yp[i:i + 10]
            key, price_s, cnt_s = chunk[0], chunk[1:6], chunk[6:10]
            if not (price_s.isdigit() and cnt_s.isdigit()):
                continue
            price = int(price_s) / 10.0
            if price <= 0:
                continue
            if int(cnt_s) >= 3000:
                seat = "wz"
            elif key == "D" and not (code or "").upper().startswith("D"):
                seat = "yxyz"
            else:
                seat = PRICE_KEYS.get(key)
            if not seat:
                continue
            dedup[seat] = {
                "train_no": train_no, "station_train_code": code,
                "train_date": train_date, "from_station_no": fno, "to_station_no": tno,
                "seat_type": seat, "seat_name": SEAT_NAMES.get(seat, key), "price": price,
            }
        out.extend(dedup.values())
    return out


def _price_value(v) -> float | None:
    if v is None:
        return None
    s = str(v).replace("¥", "").replace("元", "").strip()
    if not s or s in ("--", "无"):
        return None
    try:
        x = float(s)
    except ValueError:
        return None
    # 纯整数（如 "17440"）单位为角；带小数点的为元
    if re.fullmatch(r"\d+", s):
        return x / 10
    return x


def parse_price(payload: dict, ctx: dict) -> list[dict]:
    data = payload.get("data") or {}
    train_no = data.get("train_no") or ctx.get("train_no")
    if not train_no:
        raise ValueError("票价数据缺少 train_no，请在导入时补充车次编号")
    code = data.get("station_train_code") or ctx.get("station_train_code") or ""
    out = []
    for k, v in data.items():
        if k not in PRICE_KEYS or isinstance(v, (list, dict)):
            continue
        price = _price_value(v)
        if price is None:
            continue
        seat = "yxyz" if (k == "D" and code and not str(code).upper().startswith("D")) else PRICE_KEYS[k]
        out.append({
            "train_no": train_no,
            "station_train_code": code or None,
            "train_date": norm_date(data.get("train_date") or ctx.get("date")),
            "from_station_no": data.get("from_station_no") or ctx.get("from_station_no"),
            "to_station_no": data.get("to_station_no") or ctx.get("to_station_no"),
            "seat_type": seat, "seat_name": SEAT_NAMES.get(seat, k), "price": price,
        })
    # 同一席别可能同时出现 "A9" 与 "9"，去重
    dedup = {}
    for r in out:
        dedup[r["seat_type"]] = r
    return list(dedup.values())


def _find_stop_list(payload: dict) -> list[dict]:
    data = payload.get("data")
    candidates = []
    if isinstance(data, list):
        candidates.append(data)
    elif isinstance(data, dict):
        for key in ("data", "stop_stations", "stations", "result"):
            if isinstance(data.get(key), list):
                candidates.append(data[key])
    for lst in candidates:
        if lst and isinstance(lst[0], dict) and "station_name" in lst[0] \
                and ("arrive_time" in lst[0] or "start_time" in lst[0]):
            return lst
    raise ValueError("未能在返回中找到停站列表（期望 data.data 数组）")


def parse_train_stops(payload: dict, ctx: dict, name2code: dict) -> list[dict]:
    stops = _find_stop_list(payload)
    first = stops[0]
    train_no = ctx.get("train_no") or payload.get("train_no")
    if not train_no:
        raise ValueError("时刻表数据缺少 train_no，请在导入时补充车次编号")
    code = ctx.get("station_train_code") or first.get("station_train_code")
    date = norm_date(ctx.get("date"))
    out = []
    for s in stops:
        try:
            no = int(s.get("station_no"))
        except (TypeError, ValueError):
            continue
        name = s.get("station_name", "")
        out.append({
            "train_no": train_no, "station_train_code": code, "train_date": date,
            "station_no": no, "station_name": name,
            "telecode": s.get("station_telecode") or name2code.get(name),
            "arrive_time": s.get("arrive_time"), "start_time": s.get("start_time"),
            "stopover_time": s.get("stopover_time"),
            "enabled": 1 if s.get("isEnabled", True) in (True, "true", "Y", "1") else 0,
        })
    return out


def parse_station_screen(payload: dict, ctx: dict) -> list[dict]:
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("车站大屏返回应为 data 数组")
    out = []
    for t in data:
        if not isinstance(t, dict) or "station_train_code" not in t:
            continue
        out.append({
            "station_telecode": t.get("station_telecode") or ctx.get("station"),
            "station_name": t.get("station_name"),
            "query_date": norm_date(ctx.get("date")),
            "station_train_code": t["station_train_code"],
            "train_no": t.get("train_no"),
            "station_train_date": norm_date(t.get("station_train_date")),
            "start_train_date": norm_date(t.get("start_train_date")),
            "station_no": int(t["station_no"]) if str(t.get("station_no", "")).isdigit() else None,
            "arrive_time": t.get("arrive_time"), "start_time": t.get("start_time"),
            "stopover_time": t.get("stopover_time"),
            "arrive_day_diff": int(t["arrive_day_diff"]) if str(t.get("arrive_day_diff", "")).isdigit() else None,
            "start_day_diff": int(t["start_day_diff"]) if str(t.get("start_day_diff", "")).isdigit() else None,
            "start_station_name": t.get("start_station_name"),
            "start_station_telecode": t.get("start_station_telecode"),
            "end_station_name": t.get("end_station_name"),
            "end_station_telecode": t.get("end_station_telecode"),
            "train_class_name": t.get("train_class_name"),
            "seat_types": t.get("seat_types"),
            "running_fig": t.get("running_fig"),
            "start_start_time": t.get("start_start_time"),
            "end_arrive_time": t.get("end_arrive_time"),
            "running_time": t.get("running_time"),
            "distance": int(t["distance"]) if str(t.get("distance", "")).isdigit() else None,
        })
    return out


def running_fig_allows(fig: str | None, query_date: str) -> bool | None:
    """根据大屏 running_fig（如 '20260126#1#1#20261124'）判断有效期，不匹配时返回 None。"""
    if not fig or not query_date:
        return None
    m = re.fullmatch(r"(\d{8})#[^#]*#[^#]*#(\d{8})", str(fig).strip())
    if not m:
        return None
    qd = query_date.replace("-", "")
    if len(qd) != 8 or not qd.isdigit():
        return None
    return m.group(1) <= qd <= m.group(2)


def detect_type(text: str) -> str:
    t = text.strip()
    if t.startswith("var station_names"):
        return "stations"
    try:
        payload = json.loads(t)
    except json.JSONDecodeError:
        raise ValueError("无法识别：既不是 station_name.js 也不是合法 JSON")
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict) and isinstance(data.get("result"), list) \
            and data["result"] and isinstance(data["result"][0], str) and "|" in data["result"][0]:
        return "left_ticket"
    if isinstance(data, dict) and "train_no" in data \
            and any(k in data for k in PRICE_KEYS):
        return "price"
    if isinstance(data, list) and data and isinstance(data[0], dict):
        if "station_train_date" in data[0] or "bureau_code" in data[0]:
            return "station_screen"
    try:
        _find_stop_list(payload)
        return "train_stops"
    except ValueError:
        pass
    raise ValueError("无法自动识别数据类型，请手动指定 api_type")
