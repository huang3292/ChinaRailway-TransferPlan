"""本地票价估算。

优先用精确里程（车站大屏 distance 累计里程之差）× 车型费率区间；
无里程时按 历时 × 速度区间 推算里程区间。
已爬取的票价会反推实际费率用于校准同车型席别。
结果均为参考区间，非官方票价。
"""
import math

from . import db, parsers
from .parsers import SEAT_NAMES

# 元/km 参考区间（含线路浮动与递远递减的综合效果）
RATES = {
    "G": {"ze": (0.35, 0.55), "zy": (0.56, 0.88), "swz": (1.05, 1.65)},
    "X": {"ze": (0.35, 0.55), "zy": (0.56, 0.88), "swz": (1.05, 1.65)},
    "C": {"ze": (0.30, 0.50), "zy": (0.48, 0.80)},
    "D": {"ze": (0.25, 0.37), "zy": (0.40, 0.60), "dwo": (0.60, 0.85)},
    "Z": {"yz": (0.08, 0.12), "yw": (0.16, 0.24), "rw": (0.25, 0.35)},
    "T": {"yz": (0.08, 0.12), "yw": (0.16, 0.24), "rw": (0.25, 0.35)},
    "N": {"yz": (0.08, 0.12), "yw": (0.16, 0.24), "rw": (0.25, 0.35)},
    "K": {"yz": (0.08, 0.12), "yw": (0.16, 0.24), "rw": (0.25, 0.35)},
    "V": {"yz": (0.08, 0.12), "yw": (0.16, 0.24), "rw": (0.25, 0.35)},
    "P": {"yz": (0.07, 0.11), "yw": (0.14, 0.22), "rw": (0.22, 0.32)},
}
# km/h 参考区间，用于无里程时按历时推算
SPEED = {
    "G": (200, 320), "X": (200, 320), "C": (160, 260), "D": (140, 210),
    "Z": (90, 140), "T": (80, 120), "N": (80, 120),
    "K": (70, 110), "V": (70, 110), "P": (60, 100),
}


def class_of(code: str, style: str | None = None) -> str:
    s = (code or "P").strip().upper()
    c = s[0] if s else "P"
    # WHY: 2026-11-25 改号后普速 T/K 转为 N/V，非动车底场景将 N→T、V→K 归并以便共享历史校准费率；
    # 若未来出现 CR/CRH 动车底担当的 T(300km/h)/K(200-250km/h) 动车组，按 G/D 费率与速度估算。
    if parsers.is_emu_style(style):
        if c == "T":
            return "G"
        if c == "K":
            return "D"
    else:
        if c == "N":
            return "T"
        if c == "V":
            return "K"
    return c if c in RATES else "P"


def load_calibration() -> dict:
    """(车型, 席别) -> 已爬票价反推的费率样本列表"""
    dist = {}
    code_by_no = {}
    style_by_no = {}
    for r in db.rows("SELECT train_no,station_train_code,station_no,distance,train_style,jiaolu_train_style FROM station_trains"):
        if r["station_train_code"] and r["train_no"] not in code_by_no:
            code_by_no[r["train_no"]] = r["station_train_code"]
        st = r.get("train_style") or r.get("jiaolu_train_style")
        if st and r["train_no"] not in style_by_no:
            style_by_no[r["train_no"]] = st
        if r["distance"] is not None:
            try:
                dist[(r["train_no"], r["station_no"])] = int(r["distance"])
            except (TypeError, ValueError):
                continue
    cal = {}
    for r in db.rows("SELECT train_no,station_train_code,from_station_no,to_station_no,seat_type,price FROM prices"):
        try:
            d1 = dist[(r["train_no"], int(r["from_station_no"]))]
            d2 = dist[(r["train_no"], int(r["to_station_no"]))]
        except (KeyError, ValueError, TypeError):
            continue
        km = d2 - d1
        if km <= 0 or not r["price"]:
            continue
        code = r["station_train_code"] or code_by_no.get(r["train_no"]) or ""
        cal.setdefault((class_of(code, style=style_by_no.get(r["train_no"])), r["seat_type"]), []).append(r["price"] / km)
    return cal


def estimate(code: str, km: int | None = None, duration_min: int | None = None,
             cal: dict | None = None, style: str | None = None) -> dict | None:
    cls = class_of(code, style=style)
    rates = dict(RATES[cls])
    calibrated = False
    for seat in list(rates):
        obs = (cal or {}).get((cls, seat))
        if obs:
            rates[seat] = (min(obs) * 0.9, max(obs) * 1.1)
            calibrated = True
    if km and km > 0:
        klo = khi = km
        basis = f"里程 {km} km"
    elif duration_min and duration_min > 0:
        h = duration_min / 60
        slo, shi = SPEED[cls]
        klo, khi = h * slo, h * shi
        basis = f"按历时推算里程 {klo:.0f}–{khi:.0f} km"
    else:
        return None
    seats = {}
    for seat, (rlo, rhi) in rates.items():
        seats[seat] = {
            "name": SEAT_NAMES.get(seat, seat),
            "min": math.floor(klo * rlo * 2) / 2,
            "max": math.ceil(khi * rhi * 2) / 2,
        }
    return {"seats": seats, "basis": basis + ("（费率已用实际票价校准）" if calibrated else "（参考费率）"),
            "estimated": True}
