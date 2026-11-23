"""Scheme C v2 地理空间与分层城轨邻近换乘模块 + 合规批量地理采集。

职责：
1. 严格单调连续距离计价 + 三档重轨邻近减时（walk <=800m 100% / short_bus 800-1500m 50% / >1500m 0%）+ 枢纽附加
2. 纬度感知动态空间格网索引（StationGeoIndex）与单站 Top-25 物理近邻截断告警
3. MOT 2025 全年口径 ~43 城地铁/轻轨及重轨市域/单轨字典与严格跨市圈群交集（排除京津、排除纯有轨电车）
4. 合规公开地理批量抓取（Overpass 试跑配额门禁 + 分片国铁/重轨双采集 + Wikidata 补盲；剥离 Nominatim 自动批量，降级为单站手工点查）
"""
from __future__ import annotations

import json
import math
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from . import db

GEO_STATE_PATH = Path(__file__).resolve().parent.parent / "data" / "crawl_geo_coords_state.json"

CHINA_BBOX = (17.5, 72.5, 54.5, 136.0)  # (min_lat, min_lon, max_lat, max_lon)
MAX_CROSS_RADIUS_KM = 100.0
MIN_SAFE_CROSS_EXTRA = 20
DEFAULT_CROSS_RADIUS_KM = 20.0
DEFAULT_TRANSIT_DISCOUNT_MIN = 15
DEFAULT_TRANSIT_PROXIMITY_M = 1500
WALK_PROXIMITY_M = 800
DEFAULT_HUB_EXTRA_MIN = 5
FALLBACK_FIXED_EXTRA_MIN = 90
FALLBACK_AUTO_DISABLE_PCT = 98.0
MAX_RADIUS_NEIGHBORS = 25
NOMINATIM_COOLDOWN_SEC = 1.5

# 半开区间分档端点：用于 (0, 3], (3, 10], (10, 20], (20, 35], (35, 100] 连续线性插值
DISTANCE_TIER_BREAKPOINTS: tuple[tuple[float, float, float, float], ...] = (
    # (low_km, high_km, low_min, high_min)
    (0.0, 3.0, 25.0, 32.0),
    (3.0, 10.0, 32.0, 48.0),
    (10.0, 20.0, 48.0, 65.0),
    (20.0, 35.0, 65.0, 85.0),
    (35.0, 100.0, 85.0, 125.0),
)

# 交通运输部 2025 年全年数据口径：约 43 个地铁/轻轨核心城市 + 明确重轨制式市域/单轨（彻底排除纯有轨电车城市如红河、文山、嘉兴市区等）
RAIL_TRANSIT_CITIES: dict[str, str] = {
    # 华北 (5)
    "北京": "地铁/市域快线",
    "天津": "地铁/轻轨",
    "石家庄": "地铁",
    "太原": "地铁",
    "呼和浩特": "地铁",
    # 东北 (4)
    "沈阳": "地铁",
    "大连": "地铁/轻轨",
    "长春": "地铁/轻轨",
    "哈尔滨": "地铁",
    # 华东 (19)
    "上海": "地铁/市域快轨",
    "南京": "地铁/市域S线",
    "无锡": "地铁/市域S1",
    "徐州": "地铁",
    "常州": "地铁",
    "苏州": "地铁/市域轨道",
    "南通": "地铁",
    "杭州": "地铁/市域快轨",
    "宁波": "地铁/市域铁路",
    "温州": "市域铁路S线",
    "绍兴": "地铁/城际",
    "台州": "市域铁路S1",
    "金华": "市域轨道(金义东)",
    "合肥": "地铁",
    "芜湖": "跨座式单轨",
    "滁州": "市域铁路(宁滁S4)",
    "福州": "地铁/市域F1",
    "厦门": "地铁",
    "南昌": "地铁",
    "济南": "地铁",
    "青岛": "地铁",
    # 华中 (4)
    "郑州": "地铁/城郊线",
    "洛阳": "地铁",
    "武汉": "地铁/阳逻线",
    "长沙": "地铁/磁浮",
    # 华南 (5)
    "广州": "地铁/广佛/市域",
    "深圳": "地铁",
    "佛山": "地铁/广佛线",
    "东莞": "地铁",
    "南宁": "地铁",
    # 西南 (5)
    "重庆": "地铁/跨座单轨",
    "成都": "地铁/市域S线",
    "资阳": "市域铁路S3",
    "贵阳": "地铁",
    "昆明": "地铁",
    # 西北 (4)
    "西安": "地铁/西咸线",
    "咸阳": "地铁(西安线网延入)",
    "兰州": "地铁",
    "乌鲁木齐": "地铁",
}

# 跨市互联重轨圈群（仅保留实际具备地铁/轻轨同网贯通运营的圈群；明确排除京津！）
CROSS_CITY_TRANSIT_ZONES: dict[str, str] = {
    "上海": "SH_KS",
    "昆山": "SH_KS",
    "广州": "GZ_FS",
    "佛山": "GZ_FS",
    "西安": "XA_XY",
    "咸阳": "XA_XY",
    "成都": "CD_ZY",
    "资阳": "CD_ZY",
    "南京": "NJ_CZ",
    "滁州": "NJ_CZ",
}

# 昆山南等隶属苏州行政管辖但接壤沪昆走廊的车站名缀归属
STATION_ZONE_OVERRIDES: dict[str, str] = {
    "昆山": "SH_KS",
    "昆山南": "SH_KS",
    "花桥": "SH_KS",
    "阳澄湖": "SH_KS",
    "句容": "NJ_CZ",
    "海宁": "杭州",
    "海宁西": "杭州",
    "三河": "北京",
    "燕郊": "北京",
}

# 默认大型客运枢纽（用于缺省 is_major_hub=0 时兜底识别，并允许数据库列覆盖）
DEFAULT_MAJOR_HUB_NAMES: frozenset[str] = frozenset({
    "北京", "北京西", "北京南", "北京北", "北京朝阳", "北京丰台", "清河",
    "天津", "天津西", "天津南",
    "上海", "上海虹桥", "上海南", "上海西", "上海松江",
    "南京", "南京南", "杭州", "杭州东", "杭州西", "杭州南",
    "广州", "广州南", "广州东", "广州白云", "深圳", "深圳北", "深圳东", "福田",
    "武汉", "汉口", "武昌", "长沙", "长沙南", "郑州", "郑州东",
    "成都东", "成都西", "成都南", "重庆北", "重庆西", "沙坪坝",
    "西安", "西安北", "沈阳", "沈阳北", "沈阳南", "哈尔滨", "哈尔滨西",
    "长春", "大连", "大连北", "济南", "济南西", "青岛", "青岛北",
    "合肥", "合肥南", "南昌", "南昌西", "福州", "福州南", "厦门", "厦门北",
    "贵阳", "贵阳北", "昆明", "昆明南", "南宁", "南宁东", "石家庄", "太原", "太原南",
    "徐州", "徐州东", "宁波", "温州南", "苏州", "苏州北", "无锡", "无锡东",
})

ACCESS_MODE_LABELS: dict[str, str] = {
    "walk": "步行直连(≤800m)",
    "short_bus": "短驳邻近(800–1500m)",
    "long_bus_or_taxi": "公交/打车(>1500m)",
    "none": "无重轨邻近",
}


def norm_city(c: str | None) -> str:
    s = str(c or "").strip()
    if len(s) > 1 and s.endswith("市"):
        return s[:-1]
    return s


def default_zone_for_station(name: str, city: str) -> str:
    nm = (name or "").strip().removesuffix("站")
    if nm in STATION_ZONE_OVERRIDES:
        return STATION_ZONE_OVERRIDES[nm]
    ct = norm_city(city)
    if not ct:
        return ""
    if ct in CROSS_CITY_TRANSIT_ZONES:
        return CROSS_CITY_TRANSIT_ZONES[ct]
    return ct


def load_transit_cities() -> dict[str, str]:
    """合并内置 MOT 2025 重轨城市字典与用户在 settings 中的自定义增删。"""
    merged = dict(RAIL_TRANSIT_CITIES)
    raw = db.get_setting("custom_transit_cities")
    if raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                for k, v in obj.items():
                    ck = norm_city(k)
                    if not ck:
                        continue
                    if v is False or v == "" or v is None:
                        merged.pop(ck, None)
                    else:
                        merged[ck] = str(v)
        except Exception:
            pass
    return merged


def save_transit_city_override(city: str, label: str | None, enabled: bool = True) -> dict[str, str]:
    ck = norm_city(city)
    if not ck:
        raise ValueError("城市名称不能为空")
    raw = db.get_setting("custom_transit_cities")
    cur: dict = {}
    if raw:
        try:
            cur = json.loads(raw) or {}
        except Exception:
            cur = {}
    if enabled:
        cur[ck] = (label or "地铁/市域").strip() or "地铁/市域"
    else:
        cur[ck] = False
    db.set_setting("custom_transit_cities", json.dumps(cur, ensure_ascii=False), bump=True)
    return load_transit_cities()


def reset_transit_cities() -> dict[str, str]:
    db.set_setting("custom_transit_cities", "{}", bump=True)
    return load_transit_cities()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """计算地球表面两点间的球面直线距离（km）。"""
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp * 0.5) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl * 0.5) ** 2
    return round(2.0 * r * math.asin(min(1.0, math.sqrt(max(0.0, a)))), 2)


def in_china_bbox(lat: float, lon: float) -> bool:
    return CHINA_BBOX[0] <= lat <= CHINA_BBOX[2] and CHINA_BBOX[1] <= lon <= CHINA_BBOX[3]


def _base_extra_float(dist_km: float) -> float:
    d = max(0.0, float(dist_km))
    for lo, hi, m_lo, m_hi in DISTANCE_TIER_BREAKPOINTS:
        if d <= hi:
            if hi <= lo:
                return m_hi
            t = max(0.0, min(1.0, (d - lo) / (hi - lo)))
            return m_lo + (m_hi - m_lo) * t
    return 125.0 + max(0.0, d - 100.0) * 0.6


def _round_half_up(x: float) -> int:
    # WHY: Python round() 为偶舍入，28.5->28 而 (28.5+5)=33.5->34 会破坏 base_extra+hub_extra-disc==effective_extra；
    # 半入整舍对整数平移不变，数学上严格保证账本恒等与单调性。
    return int(math.floor(float(x) + 0.5))


def distance_tier_minutes(dist_km: float) -> int:
    """半开区间内连续线性插值的基础地面接驳时间（不含枢纽附加与城轨减时）。"""
    return _round_half_up(_base_extra_float(dist_km))


def compute_smart_extra(dist_km: float, hub_extra: int = 0, raw_discount_cap: int = 0) -> dict:
    """严格随距离单调不减的接驳时间计算器。

    WHY: 3km 以下不触发城轨减时，3km 以上若直接硬减 15m 会导致 3.1km < 2.9km 的倒挂；
    故令减时从 3km 的 0 线性坡升至 8km 达到封顶值，并在整个 (0, 100]km 上取前缀上包络：
      env(d) = max_{0 <= u <= d} (base(u) + hub_extra - ramp_disc(u))
    由于 base(u) 在 (3,8] 线性、8km 后单调增，前缀包络只在 3.0km 处需要比对 max(g(3), g(d))，
    数学上严格保证任意 d2 > d1 必有 effective_extra(d2) >= effective_extra(d1) 且满足账本等式。
    """
    d = max(0.0, float(dist_km))
    hub_i = int(hub_extra)
    base_f = _base_extra_float(d)
    base_i = _round_half_up(base_f)
    cap = max(0, int(raw_discount_cap))

    if d <= 3.0 or cap <= 0:
        eff = max(MIN_SAFE_CROSS_EXTRA, base_i + hub_i)
        return {
            "base_extra": base_i,
            "hub_extra": hub_i,
            "transit_discount": 0,
            "effective_extra": eff,
        }

    ramp = min(1.0, (d - 3.0) / 5.0)
    raw_val = base_f + hub_i - cap * ramp
    floor_3km = _base_extra_float(3.0) + hub_i
    env_val = max(float(MIN_SAFE_CROSS_EXTRA), floor_3km, raw_val)
    eff = _round_half_up(env_val)
    actual_disc = max(0, base_i + hub_i - eff)
    return {
        "base_extra": base_i,
        "hub_extra": hub_i,
        "transit_discount": actual_disc,
        "effective_extra": eff,
    }


def _access_factor(dist_m: int | None, mode: str, proximity_m: int) -> float:
    if dist_m is None or mode == "none":
        return 0.0
    d = int(dist_m)
    prox = max(200, int(proximity_m))
    walk_cap = min(WALK_PROXIMITY_M, prox)
    if d <= walk_cap:
        return 1.0
    if d <= prox:
        return 0.5
    return 0.0


class StationGeoIndex:
    """只读车站空间与异站规则索引（随 db.data_version() 构图重建，跨请求安全共享）。"""

    def __init__(
        self,
        stations: list[dict],
        groups: list[dict],
        pair_overrides: list[dict],
        transit_cities: dict[str, str],
    ):
        self.coords: dict[str, tuple[float, float]] = {}
        self.all_coords: dict[str, tuple[float, float]] = {}
        self.status_of: dict[str, str] = {}
        self.confidence_of: dict[str, float] = {}
        self.zone_of: dict[str, str] = {}
        self.rail_dist_of: dict[str, int | None] = {}
        self.access_mode_of: dict[str, str] = {}
        self.is_hub: dict[str, bool] = {}
        self.name_of: dict[str, str] = {}
        self.city_of: dict[str, str] = {}
        self.tele_of: dict[str, str] = {}
        self.grid: dict[tuple[int, int], list[str]] = {}
        self.transit_cities = transit_cities

        for st in stations:
            tc = st["telecode"]
            nm = st["name"]
            ct = st.get("city") or ""
            cstatus = st.get("coord_status") or "missing"
            self.name_of[tc] = nm
            self.tele_of[nm] = tc
            self.tele_of[tc] = tc
            self.status_of[tc] = cstatus
            self.confidence_of[tc] = float(st.get("coord_confidence") or (0.9 if cstatus in ("ok", "verified") else 0.0))
            if ct:
                self.city_of[tc] = ct
                self.city_of[nm] = ct

            zone = (st.get("transit_zone_id") or "").strip() or default_zone_for_station(nm, ct)
            self.zone_of[tc] = zone
            rdist = st.get("nearest_rail_stop_distance_m")
            self.rail_dist_of[tc] = int(rdist) if rdist is not None else None
            rmode = st.get("rail_access_mode") or (
                db.rail_access_mode_for(rdist) if rdist is not None else "none"
            )
            self.access_mode_of[tc] = rmode
            nm_clean = nm.removesuffix("站")
            self.is_hub[tc] = bool(st.get("is_major_hub")) or (nm in DEFAULT_MAJOR_HUB_NAMES) or (nm_clean in DEFAULT_MAJOR_HUB_NAMES)

            lat, lon = st.get("lat"), st.get("lon")
            if lat is not None and lon is not None and in_china_bbox(float(lat), float(lon)):
                flat, flon = float(lat), float(lon)
                self.all_coords[tc] = (flat, flon)
                # WHY: coord_status='suspect' 的可疑坐标默认隔离，严禁参与物理半径建边
                if cstatus in ("ok", "verified"):
                    self.coords[tc] = (flat, flon)
                    gb = (math.floor(flat * 2.0), math.floor(flon * 2.0))
                    self.grid.setdefault(gb, []).append(tc)

        self.groups_by_id: dict[int, dict] = {}
        self.default_group_ids: tuple[int, ...] = ()
        self.station_groups: dict[str, list[int]] = {}
        def_ids = []
        for g in groups:
            gid = int(g["id"])
            mem_tcs = frozenset(m["telecode"] for m in g.get("members", []))
            self.groups_by_id[gid] = {
                "id": gid,
                "name": g["name"],
                "default_extra_min": g.get("default_extra_min"),
                "enabled_by_default": bool(g.get("enabled_by_default", True)),
                "connect_mode": g.get("connect_mode") or "full_mesh",
                "hub_telecode": (g.get("hub_telecode") or "").strip().upper(),
                "members": mem_tcs,
            }
            if g.get("enabled_by_default", True):
                def_ids.append(gid)
            for tc in mem_tcs:
                self.station_groups.setdefault(tc, []).append(gid)
        self.default_group_ids = tuple(def_ids)

        self.pair_overrides: dict[tuple[str, str], dict] = {}
        self.has_zero_override = False
        for p in pair_overrides:
            a, b = p["a_telecode"], p["b_telecode"]
            key = (a, b) if a <= b else (b, a)
            ex = p.get("custom_extra_min")
            if p["mode"] == "allow" and ex is not None and int(ex) == 0:
                self.has_zero_override = True
            self.pair_overrides[key] = {
                "mode": p["mode"],
                "custom_extra_min": int(ex) if ex is not None else None,
                "note": p.get("note") or "",
            }

        # 纬度感知动态空间格网预构建 <=100km 物理近邻（搜索期切任何 R<=100 均 O(1) 复用，不重建全局路网缓存）
        self.max_neighbors: dict[str, list[tuple[str, float]]] = {}
        for tc, (lat, lon) in self.coords.items():
            lat_bin = math.floor(lat * 2.0)
            lon_bin = math.floor(lon * 2.0)
            d_lat = math.ceil(MAX_CROSS_RADIUS_KM / 55.0) + 1
            cos_lat = max(0.30, abs(math.cos(math.radians(lat))))
            d_lon = math.ceil(MAX_CROSS_RADIUS_KM / (55.0 * cos_lat)) + 1
            cands: list[tuple[str, float]] = []
            for bi in range(lat_bin - d_lat, lat_bin + d_lat + 1):
                for bj in range(lon_bin - d_lon, lon_bin + d_lon + 1):
                    for other in self.grid.get((bi, bj), ()):
                        if other == tc:
                            continue
                        olat, olon = self.coords[other]
                        dist = haversine_km(lat, lon, olat, olon)
                        if dist <= MAX_CROSS_RADIUS_KM:
                            cands.append((other, dist))
            if cands:
                cands.sort(key=lambda x: (x[1], x[0]))
                self.max_neighbors[tc] = cands

    def zone_tokens(self, tc: str) -> set[str]:
        tokens: set[str] = set()
        z = self.zone_of.get(tc)
        if z:
            tokens.add(z)
        ct = norm_city(self.city_of.get(tc))
        if ct and ct in self.transit_cities:
            tokens.add(CROSS_CITY_TRANSIT_ZONES.get(ct, ct))
        return tokens

    def is_transit_city(self, tc: str) -> bool:
        ct = norm_city(self.city_of.get(tc))
        if ct and ct in self.transit_cities:
            return True
        nm = (self.name_of.get(tc) or "").removesuffix("站")
        if nm in STATION_ZONE_OVERRIDES:
            mapped = STATION_ZONE_OVERRIDES[nm]
            return mapped in self.transit_cities or mapped in ("SH_KS", "NJ_CZ")
        return False


def resolve_cross_cfg(
    cross_extra: int = 60,
    cross_radius_km: float = DEFAULT_CROSS_RADIUS_KM,
    cross_time_mode: str = "smart",
    transit_discount_on: bool = True,
    transit_discount_min: int = DEFAULT_TRANSIT_DISCOUNT_MIN,
    transit_proximity_m: int = DEFAULT_TRANSIT_PROXIMITY_M,
    active_group_ids: list[int] | None = None,
    fallback_ungeocoded: bool = False,
    geo_index: StationGeoIndex | None = None,
) -> dict:
    mode = "fixed" if str(cross_time_mode or "smart").lower() == "fixed" else "smart"
    rad = max(0.0, min(MAX_CROSS_RADIUS_KM, float(DEFAULT_CROSS_RADIUS_KM if cross_radius_km is None else cross_radius_km)))
    disc_on = bool(transit_discount_on)
    disc_min = max(0, min(60, int(DEFAULT_TRANSIT_DISCOUNT_MIN if transit_discount_min is None else transit_discount_min)))
    prox_m = max(200, min(5000, int(DEFAULT_TRANSIT_PROXIMITY_M if transit_proximity_m is None else transit_proximity_m)))
    if active_group_ids is None:
        gids = set(geo_index.default_group_ids) if geo_index else set()
    else:
        gids = {int(x) for x in active_group_ids}
    # WHY: R=0 表示关闭按物理距离建边；若同时残留开启 fallback_ungeocoded 会退回老同城误连。
    fb = bool(fallback_ungeocoded) and (rad > 0.0)
    return {
        "fixed_extra": max(0, min(360, int(cross_extra))),
        "radius_km": rad,
        "time_mode": mode,
        "transit_discount_on": disc_on,
        "transit_discount_min": disc_min if disc_on else 0,
        "transit_proximity_m": prox_m,
        "active_group_ids": gids,
        "fallback_ungeocoded": fb,
    }


def min_possible_cross_extra(geo_index: StationGeoIndex | None, cfg: dict) -> int:
    """计算当前配置下所有合法异站边可能取得的真实最小 extra_min，保证多程 BFS 下界严格可容许。"""
    if cfg["time_mode"] == "fixed":
        base = cfg["fixed_extra"]
    else:
        base = MIN_SAFE_CROSS_EXTRA
    if geo_index:
        for ov in geo_index.pair_overrides.values():
            if ov["mode"] == "allow" and ov.get("custom_extra_min") is not None:
                base = min(base, int(ov["custom_extra_min"]))
        for gid in cfg["active_group_ids"]:
            g = geo_index.groups_by_id.get(gid)
            if g and g.get("default_extra_min") is not None:
                base = min(base, int(g["default_extra_min"]))
    return max(0, base)


def _group_edge_match(tc1: str, tc2: str, geo_index: StationGeoIndex, active_group_ids: set[int]) -> tuple[bool, int | None]:
    if not active_group_ids:
        return False, None
    matched = False
    best_extra: int | None = None
    for gid in geo_index.station_groups.get(tc1, ()):
        if gid not in active_group_ids:
            continue
        g = geo_index.groups_by_id.get(gid)
        if not g or tc2 not in g["members"]:
            continue
        if g["connect_mode"] == "hub_star":
            hub = g.get("hub_telecode") or ""
            if tc1 != hub and tc2 != hub:
                continue
        matched = True
        ge = g.get("default_extra_min")
        if ge is not None:
            gi = int(ge)
            if best_extra is None or gi < best_extra:
                best_extra = gi
    return matched, best_extra


def eval_cross_pair(tc_from: str, tc_to: str, geo_index: StationGeoIndex, cfg: dict) -> dict | None:
    """严格按不叠加优先链评估异站换乘：block > allow > active_group > radius(ok/verified) > fallback。"""
    if not tc_from or not tc_to or tc_from == tc_to:
        return None
    pk = (tc_from, tc_to) if tc_from <= tc_to else (tc_to, tc_from)
    ov = geo_index.pair_overrides.get(pk)
    if ov and ov["mode"] == "block":
        return None

    c1 = geo_index.coords.get(tc_from)
    c2 = geo_index.coords.get(tc_to)
    dist_km = haversine_km(c1[0], c1[1], c2[0], c2[1]) if (c1 and c2) else None
    st1 = geo_index.status_of.get(tc_from, "missing")
    st2 = geo_index.status_of.get(tc_to, "missing")
    is_suspect = (st1 == "suspect") or (st2 == "suspect")
    conf = round(min(geo_index.confidence_of.get(tc_from, 0.0), geo_index.confidence_of.get(tc_to, 0.0)), 2)

    # 1) 连接允许判定（严格单赢优先级，不叠加）
    source_rule: str | None = None
    group_matched, group_extra = _group_edge_match(tc_from, tc_to, geo_index, cfg["active_group_ids"])
    if ov and ov["mode"] == "allow":
        source_rule = "override"
    elif group_matched:
        source_rule = "group"
    elif dist_km is not None and cfg["radius_km"] > 0 and dist_km <= cfg["radius_km"]:
        source_rule = "radius"
    elif cfg["fallback_ungeocoded"] and (c1 is None or c2 is None):
        ct1 = geo_index.city_of.get(tc_from)
        ct2 = geo_index.city_of.get(tc_to)
        if ct1 and ct2 and norm_city(ct1) == norm_city(ct2):
            source_rule = "fallback"

    if source_rule is None:
        return None

    # 2) 接驳模式 & 城轨圈群分析（供透明账本展示；仅在 smart + radius 且未指定上层加时时减免）
    m1 = geo_index.access_mode_of.get(tc_from, "none")
    m2 = geo_index.access_mode_of.get(tc_to, "none")
    d1_m = geo_index.rail_dist_of.get(tc_from)
    d2_m = geo_index.rail_dist_of.get(tc_to)
    f1 = _access_factor(d1_m, m1, cfg["transit_proximity_m"])
    f2 = _access_factor(d2_m, m2, cfg["transit_proximity_m"])
    pair_factor = min(f1, f2)
    pair_access_mode = (
        "none" if (m1 == "none" or m2 == "none")
        else ("long_bus_or_taxi" if pair_factor == 0.0 else ("short_bus" if pair_factor < 1.0 else "walk"))
    )

    z1 = geo_index.zone_tokens(tc_from)
    z2 = geo_index.zone_tokens(tc_to)
    shared_zones = sorted(z1 & z2)
    both_transit = geo_index.is_transit_city(tc_from) and geo_index.is_transit_city(tc_to)
    transit_cities: list[str] = []
    if both_transit:
        c1n = norm_city(geo_index.city_of.get(tc_from)) or geo_index.name_of.get(tc_from, "")
        c2n = norm_city(geo_index.city_of.get(tc_to)) or geo_index.name_of.get(tc_to, "")
        transit_cities = list(dict.fromkeys([c1n, c2n]))

    hub_extra = DEFAULT_HUB_EXTRA_MIN if (geo_index.is_hub.get(tc_from) or geo_index.is_hub.get(tc_to)) else 0

    # 3) 计价（严格不叠加规则）
    if source_rule == "override" and ov and ov.get("custom_extra_min") is not None:
        ex = int(ov["custom_extra_min"])
        return {
            "same": False,
            "extra_min": ex,
            "base_extra": ex,
            "hub_extra": 0,
            "transit_discount": 0,
            "transit_discount_min": 0,
            "effective_extra": ex,
            "source_rule": "override",
            "source": "override",
            "distance_km": dist_km,
            "rail_access_mode": pair_access_mode,
            "transit_reason": "站对覆盖指定专属加时（不叠加城轨减时）",
            "transit_cities": [],
            "coord_confidence": conf,
            "is_fallback": False,
            "is_suspect": is_suspect,
        }

    if source_rule == "group" and group_extra is not None:
        ex = int(group_extra)
        return {
            "same": False,
            "extra_min": ex,
            "base_extra": ex,
            "hub_extra": 0,
            "transit_discount": 0,
            "transit_discount_min": 0,
            "effective_extra": ex,
            "source_rule": "group",
            "source": "group",
            "distance_km": dist_km,
            "rail_access_mode": pair_access_mode,
            "transit_reason": "换乘组规则指定加时（不叠加城轨减时）",
            "transit_cities": [],
            "coord_confidence": conf,
            "is_fallback": False,
            "is_suspect": is_suspect,
        }

    if source_rule == "fallback":
        return {
            "same": False,
            "extra_min": FALLBACK_FIXED_EXTRA_MIN,
            "base_extra": FALLBACK_FIXED_EXTRA_MIN,
            "hub_extra": 0,
            "transit_discount": 0,
            "transit_discount_min": 0,
            "effective_extra": FALLBACK_FIXED_EXTRA_MIN,
            "source_rule": "fallback",
            "source": "fallback",
            "distance_km": None,
            "rail_access_mode": "none",
            "transit_reason": "缺少已验证坐标：启用低置信同城保守回退",
            "transit_cities": [],
            "coord_confidence": 0.0,
            "is_fallback": True,
            "is_suspect": is_suspect,
        }

    # source_rule 为 radius 或无专属加时的 override/group：套用固定或智能计价
    if cfg["time_mode"] == "fixed" or dist_km is None:
        ex = cfg["fixed_extra"]
        return {
            "same": False,
            "extra_min": ex,
            "base_extra": ex,
            "hub_extra": 0,
            "transit_discount": 0,
            "transit_discount_min": 0,
            "effective_extra": ex,
            "source_rule": source_rule,
            "source": source_rule,
            "distance_km": dist_km,
            "rail_access_mode": pair_access_mode,
            "transit_reason": "固定异站加时模式",
            "transit_cities": [],
            "coord_confidence": conf,
            "is_fallback": False,
            "is_suspect": is_suspect,
        }

    # Smart 单调连续公式 + 三档邻近 + 圈群交集验证
    raw_cap = 0
    transit_reason = ""
    if cfg["transit_discount_min"] <= 0:
        transit_reason = "用户已关闭城轨减时"
    elif dist_km <= 3.0:
        transit_reason = "距离≤3km 短途步行/短驳，不叠加城轨减时"
    elif not both_transit:
        transit_reason = "非双端重轨/地铁覆盖城市"
    elif not shared_zones:
        transit_reason = "两端不属于同一地铁/轻轨互联圈群（如京津排除跨市城轨减时）"
    elif pair_factor <= 0.0:
        transit_reason = f"双端重轨邻近度不满足（阈值≤{cfg['transit_proximity_m']}m，当前接驳={ACCESS_MODE_LABELS.get(pair_access_mode, pair_access_mode)}）"
    else:
        raw_cap = int(round(cfg["transit_discount_min"] * pair_factor))
        if pair_factor >= 1.0:
            transit_reason = f"同圈群({shared_zones[0]})双端步行直连地铁(≤800m)：全额估算减时"
        else:
            transit_reason = f"同圈群({shared_zones[0]})含短驳接驳(800–{cfg['transit_proximity_m']}m)：短驳折半估算减时"

    calc = compute_smart_extra(dist_km, hub_extra=hub_extra, raw_discount_cap=raw_cap)
    return {
        "same": False,
        "extra_min": calc["effective_extra"],
        "base_extra": calc["base_extra"],
        "hub_extra": calc["hub_extra"],
        "transit_discount": calc["transit_discount"],
        "transit_discount_min": calc["transit_discount"],
        "effective_extra": calc["effective_extra"],
        "source_rule": source_rule,
        "source": source_rule,
        "distance_km": dist_km,
        "rail_access_mode": pair_access_mode,
        "transit_reason": transit_reason,
        "transit_cities": transit_cities if calc["transit_discount"] > 0 else [],
        "coord_confidence": conf,
        "is_fallback": False,
        "is_suspect": is_suspect,
    }


def cross_candidates(
    sk: str,
    s_name: str | None,
    available: set[str],
    cities: tuple,
    geo_index: StationGeoIndex | None,
    cfg: dict,
    exclude: set[str] | frozenset[str] = frozenset(),
    allow_cross: bool = True,
    truncation_out: list[bool] | None = None,
) -> list[tuple[str, dict]]:
    """产出当前车站 sk 的可用换乘候选站 [(dk, meta), ...]，并实施物理半径 Top-25 截断。"""
    same_meta = {
        "same": True,
        "extra_min": 0,
        "base_extra": 0,
        "hub_extra": 0,
        "transit_discount": 0,
        "transit_discount_min": 0,
        "effective_extra": 0,
        "source_rule": "same",
        "source": "same",
        "distance_km": 0.0,
        "rail_access_mode": "none",
        "transit_reason": "",
        "transit_cities": [],
        "coord_confidence": 1.0,
        "is_fallback": False,
        "is_suspect": False,
    }
    out: list[tuple[str, dict]] = [(sk, same_meta)]
    if not allow_cross:
        return out

    city_of, city_groups, tele_of = cities
    sk_tc = tele_of.get(sk) or (tele_of.get(s_name) if s_name else None) or sk

    if geo_index is None:
        if cfg.get("fallback_ungeocoded"):
            c = city_of.get(sk) or city_of.get(s_name or "")
            if c:
                for x in sorted(city_groups[c]):
                    xtc = tele_of.get(x, x)
                    if x in available and xtc != sk_tc and xtc not in exclude:
                        out.append((x, {
                            "same": False,
                            "extra_min": FALLBACK_FIXED_EXTRA_MIN,
                            "base_extra": FALLBACK_FIXED_EXTRA_MIN,
                            "hub_extra": 0,
                            "transit_discount": 0,
                            "transit_discount_min": 0,
                            "effective_extra": FALLBACK_FIXED_EXTRA_MIN,
                            "source_rule": "fallback",
                            "source": "fallback",
                            "distance_km": None,
                            "rail_access_mode": "none",
                            "transit_reason": "无地理索引回退",
                            "transit_cities": [],
                            "coord_confidence": 0.0,
                            "is_fallback": True,
                            "is_suspect": False,
                        }))
        return out

    def _resolve_avail_key(tc: str) -> str | None:
        if tc in available:
            return tc
        nm = geo_index.name_of.get(tc)
        if nm and nm in available:
            return nm
        return None

    # 半径近邻单独按距离升序取 Top-25（固定在路网可用集上，先截断再扣除路径 exclude，保证多程 BFS 反向可达下界严格可容许）
    radius_tcs: list[str] = []
    if cfg["radius_km"] > 0:
        for nb_tc, dist in geo_index.max_neighbors.get(sk_tc, ()):
            if dist > cfg["radius_km"]:
                break
            if _resolve_avail_key(nb_tc) is None:
                continue
            ov_k = (sk_tc, nb_tc) if sk_tc <= nb_tc else (nb_tc, sk_tc)
            ov = geo_index.pair_overrides.get(ov_k)
            if ov and ov["mode"] == "block":
                continue
            radius_tcs.append(nb_tc)
    if len(radius_tcs) > MAX_RADIUS_NEIGHBORS:
        radius_tcs = radius_tcs[:MAX_RADIUS_NEIGHBORS]
        if truncation_out is not None and not truncation_out[0]:
            truncation_out[0] = True

    cand_tcs: set[str] = {tc for tc in radius_tcs if tc not in exclude}

    for gid in cfg["active_group_ids"]:
        g = geo_index.groups_by_id.get(gid)
        if not g:
            continue
        mems = g["members"]
        if sk_tc not in mems:
            continue
        if g["connect_mode"] == "hub_star":
            hub = g.get("hub_telecode") or ""
            if sk_tc == hub:
                cand_tcs.update(mems)
            elif hub:
                cand_tcs.add(hub)
        else:
            cand_tcs.update(mems)

    for (a, b), ov in geo_index.pair_overrides.items():
        if ov["mode"] == "allow":
            if a == sk_tc:
                cand_tcs.add(b)
            elif b == sk_tc:
                cand_tcs.add(a)

    if cfg["fallback_ungeocoded"]:
        c = city_of.get(sk_tc) or city_of.get(s_name or "")
        if c:
            sk_has = sk_tc in geo_index.coords
            for x in city_groups.get(c, ()):
                xtc = tele_of.get(x, x)
                if xtc != sk_tc and (not sk_has or xtc not in geo_index.coords):
                    cand_tcs.add(xtc)

    cand_tcs.discard(sk_tc)
    seen_dk: set[str] = {sk}
    cross_items: list[tuple[str, dict]] = []
    for dk_tc in sorted(cand_tcs):
        if dk_tc in exclude:
            continue
        dk_key = _resolve_avail_key(dk_tc)
        if not dk_key or dk_key in seen_dk:
            continue
        meta = eval_cross_pair(sk_tc, dk_tc, geo_index, cfg)
        if not meta:
            continue
        seen_dk.add(dk_key)
        cross_items.append((dk_key, meta))

    cross_items.sort(key=lambda it: (
        it[1]["distance_km"] if it[1]["distance_km"] is not None else 9999.0,
        it[0],
    ))
    out.extend(cross_items)
    return out


def fallback_impact_stats(geo_index: StationGeoIndex | None = None) -> dict:
    """量化对比 Fallback 开启 vs 关闭时受影响的同城无坐标候选站对数量及主要涉及城市。"""
    if geo_index is None:
        st_rows = db.rows("SELECT telecode, name, city, lat, lon, coord_status, coord_confidence, transit_zone_id, nearest_rail_stop_distance_m, rail_access_mode, is_major_hub FROM stations")
        geo_index = StationGeoIndex(st_rows, [], [], load_transit_cities())
    by_city: dict[str, list[str]] = {}
    for tc, ct in geo_index.city_of.items():
        if tc in geo_index.name_of:  # 仅统计 telecode 键，避免与站名键重复计入
            nc = norm_city(ct)
            if nc:
                by_city.setdefault(nc, []).append(tc)

    disappearing_pairs = 0
    city_counts: list[dict] = []
    for ct, tcs in by_city.items():
        n = len(tcs)
        if n < 2:
            continue
        ok_n = sum(1 for tc in tcs if tc in geo_index.coords)
        miss_n = n - ok_n
        if miss_n == 0:
            continue
        # 无序站对数：全部同城站对 - 双端都有合法坐标的站对
        total_pairs = n * (n - 1) // 2
        ok_pairs = ok_n * (ok_n - 1) // 2
        diff = total_pairs - ok_pairs
        if diff > 0:
            disappearing_pairs += diff
            city_counts.append({
                "city": ct,
                "total_stations": n,
                "missing_or_suspect": miss_n,
                "fallback_pairs": diff,
            })
    city_counts.sort(key=lambda x: (-x["fallback_pairs"], x["city"]))
    total_st = len(geo_index.name_of)
    ok_st = len(geo_index.coords)
    ok_pct = round(ok_st * 100.0 / total_st, 1) if total_st else 0.0
    return {
        "total_stations": total_st,
        "ok_stations": ok_st,
        "ok_pct": ok_pct,
        "auto_disable_recommended": ok_pct >= FALLBACK_AUTO_DISABLE_PCT,
        "fallback_removed_pairs": disappearing_pairs,
        "top_affected_cities": city_counts[:10],
    }


# ==============================================================================
# 公开地理数据源批量抓取管道（Overpass 试跑+双采集 / Wikidata SPARQL / 手工单查 Nominatim）
# ==============================================================================

OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
GEO_UA = "ChinaRailwayTransferPlan/2.0 (+https://github.com/chinarailway-transferplan; open-geo-sync)"

# 覆盖中国全境的 12 块区域切片（按区/省切片，避免单次全境 Overpass 超时并天然支持试跑配额统计与断点续跑）
CHINA_OVERPASS_TILES: tuple[tuple[str, tuple[float, float, float, float]], ...] = (
    ("华南-粤桂琼港", (18.0, 104.5, 26.5, 117.5)),
    ("东南-闽台赣南", (23.0, 113.5, 29.5, 122.5)),
    ("华东-江浙沪皖", (28.0, 115.0, 35.2, 123.0)),
    ("华中-湘鄂赣北", (25.0, 108.5, 33.5, 118.5)),
    ("中原-豫鲁", (31.5, 110.5, 38.5, 123.0)),
    ("京津冀晋", (35.0, 110.0, 42.5, 120.0)),
    ("辽吉黑南", (38.5, 118.5, 45.5, 134.0)),
    ("黑蒙东", (45.0, 116.0, 54.0, 135.5)),
    ("成渝黔滇东", (21.5, 101.0, 32.5, 110.5)),
    ("陕甘宁蒙中", (32.0, 99.0, 42.5, 112.0)),
    ("青藏川西滇西", (21.0, 78.0, 39.5, 102.5)),
    ("新疆蒙西", (35.0, 73.0, 49.5, 106.0)),
)

_nom_lock = threading.Lock()
_last_nom_ts = 0.0


def _norm_station_name(s: str | None) -> str:
    txt = re.sub(r"\s+", "", str(s or "")).strip()
    txt = re.sub(r"[（(][^）)]*[）)]", "", txt)
    if txt.endswith("火车站") and len(txt) > 3:
        txt = txt[:-3]
    elif txt.endswith("铁路车站") and len(txt) > 4:
        txt = txt[:-4]
    elif txt.endswith("站") and len(txt) > 1:
        txt = txt[:-1]
    return txt


def _http_request(url: str, *, params: dict | None = None, data: bytes | None = None,
                  headers: dict | None = None, timeout: int = 60) -> bytes:
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}{'&' if '?' in url else '?'}{qs}"
    hdrs = {"User-Agent": GEO_UA, "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_geo_state(state_path: Path = GEO_STATE_PATH, reset: bool = False) -> dict:
    fresh = {
        "completed_tiles": [],
        "wikidata_done": False,
        "tile_bytes": 0,
        "tile_requests": 0,
        "stats": {"overpass": 0, "wikidata": 0, "rail_stops": 0, "nominatim_manual": 0},
        "last_updated": "",
    }
    if reset or not state_path.exists():
        return fresh
    try:
        obj = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(obj.get("completed_tiles"), list):
            obj["completed_tiles"] = []
        obj.setdefault("wikidata_done", False)
        obj.setdefault("tile_bytes", 0)
        obj.setdefault("tile_requests", 0)
        st = obj.setdefault("stats", dict(fresh["stats"]))
        for k, v in fresh["stats"].items():
            st.setdefault(k, v)
        return obj
    except Exception:
        return fresh


def save_geo_state(state: dict, state_path: Path = GEO_STATE_PATH):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state["last_updated"] = db.cn_now().isoformat(timespec="seconds")
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(state_path)


def geo_coverage_status() -> dict:
    total = db.rows("SELECT COUNT(*) c FROM stations")[0]["c"]
    has_coord = db.rows("SELECT COUNT(*) c FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL")[0]["c"]
    ok_cnt = db.rows("SELECT COUNT(*) c FROM stations WHERE coord_status IN ('ok','verified') AND lat IS NOT NULL AND lon IS NOT NULL")[0]["c"]
    verified_cnt = db.rows("SELECT COUNT(*) c FROM stations WHERE coord_status='verified'")[0]["c"]
    suspect_rows = db.rows(
        """SELECT telecode, name, city, lat, lon, coord_source, coord_status, coord_issue,
                  coord_confidence, nearest_rail_stop_distance_m, rail_access_mode, transit_zone_id, is_major_hub
           FROM stations WHERE coord_status='suspect' ORDER BY name LIMIT 100"""
    )
    missing_cnt = max(0, total - has_coord)
    by_src = {
        (r["coord_source"] or "unknown"): r["c"]
        for r in db.rows(
            "SELECT coord_source, COUNT(*) c FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL GROUP BY coord_source"
        )
    }
    rail_stops_cnt = db.rows("SELECT COUNT(*) c FROM rail_transit_stops")[0]["c"]
    prox_ready_cnt = db.rows("SELECT COUNT(*) c FROM stations WHERE nearest_rail_stop_distance_m IS NOT NULL")[0]["c"]
    transit_cities = load_transit_cities()
    st = load_geo_state()
    impact = fallback_impact_stats()
    return {
        "total": total,
        "with_coords": has_coord,
        "ok_coords": ok_cnt,
        "verified_coords": verified_cnt,
        "missing": missing_cnt,
        "suspect": len(suspect_rows),
        "suspect_sample": suspect_rows[:20],
        "pct": round(ok_cnt * 100.0 / total, 1) if total else 0.0,
        "raw_pct": round(has_coord * 100.0 / total, 1) if total else 0.0,
        "sources": by_src,
        "rail_stops_count": rail_stops_cnt,
        "rail_proximity_ready": prox_ready_cnt,
        "transit_cities_count": len(transit_cities),
        "fallback_impact": impact,
        "attribution": "坐标与重轨站点源自 OpenStreetMap (ODbL) & Wikidata (CC0)；单站手工辅助点查 Nominatim (仅限逐站低频人工核查)",
        "checkpoint": {
            "tiles_done": len(st.get("completed_tiles", [])),
            "tiles_total": len(CHINA_OVERPASS_TILES),
            "wikidata_done": bool(st.get("wikidata_done")),
            "tile_requests": int(st.get("tile_requests") or 0),
            "tile_mb": round((st.get("tile_bytes") or 0) / (1024 * 1024), 2),
            "last_updated": st.get("last_updated") or "",
        },
    }


def _check_timetable_speeds(
    coords: dict[str, tuple[float, float]],
    sources: dict[str, str],
    statuses: dict[str, str],
    max_speed_kmh: float = 420.0,
) -> dict[str, str]:
    """用已入库时刻表相邻停站物理距离与行车时长做异常时速质检（>420km/h 判可疑飞站）。"""
    from . import transfer  # 延迟引用复用 _minutes

    bad_counts: dict[str, list[str]] = {}
    good_counts: dict[str, int] = {}
    recs = db.get_conn().execute(
        """SELECT train_no, train_date, station_no, telecode, arrive_time, start_time, enabled
           FROM train_stops WHERE telecode IS NOT NULL ORDER BY train_no, train_date DESC, station_no"""
    ).fetchall()
    cur_key = None
    prev_tc = None
    prev_dep = None
    prev_day = 0
    seen_tn: set[str] = set()
    for tn, td, _sno, tc, arr_s, dep_s, en in recs:
        run_k = (tn, td)
        if run_k != cur_key:
            if tn in seen_tn:
                cur_key = None
                continue
            seen_tn.add(tn)
            cur_key = run_k
            prev_tc = None
            prev_dep = None
            prev_day = 0
        if not en:
            continue
        arr = transfer._minutes(arr_s)
        dep = transfer._minutes(dep_s)
        if arr is None and dep is None:
            continue
        if arr is None:
            arr = dep
        if prev_dep is not None and arr < (prev_dep % 1440):
            prev_day += 1
        arr_abs = prev_day * 1440 + arr
        if dep is None:
            dep = arr
        elif dep < arr:
            prev_day += 1
        dep_abs = prev_day * 1440 + dep

        if prev_tc and prev_tc != tc and prev_tc in coords and tc in coords and prev_dep is not None:
            dt = arr_abs - prev_dep
            if dt >= 5:
                dist = haversine_km(coords[prev_tc][0], coords[prev_tc][1], coords[tc][0], coords[tc][1])
                kmh = dist / (dt / 60.0)
                if kmh > max_speed_kmh:
                    reason = f"speed_anomaly:{prev_tc}-{tc}={round(kmh)}km/h({dist}km/{dt}m)"
                    bad_counts.setdefault(prev_tc, []).append(reason)
                    bad_counts.setdefault(tc, []).append(reason)
                else:
                    good_counts[prev_tc] = good_counts.get(prev_tc, 0) + 1
                    good_counts[tc] = good_counts.get(tc, 0) + 1
        prev_tc = tc
        prev_dep = dep_abs

    suspects: dict[str, str] = {}
    for tc, reasons in bad_counts.items():
        if sources.get(tc) == "manual" or statuses.get(tc) == "verified":
            continue
        goods = good_counts.get(tc, 0)
        # 若某站异常边占比 >=50% 或被多个邻站独立举报，判为可疑飞站
        if goods == 0 or len(reasons) >= goods:
            suspects[tc] = reasons[0]
    return suspects


def audit_station_coords(outlier_km: float = 150.0, max_speed_kmh: float = 420.0) -> dict:
    """多维度坐标质检：1) 中国边界校验；2) 同城中位离群 >150km；3) 时刻表相邻停站速度 >420km/h。"""
    st_rows = db.rows(
        "SELECT telecode, name, city, lat, lon, coord_source, coord_status FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL"
    )
    coords: dict[str, tuple[float, float]] = {}
    sources: dict[str, str] = {}
    statuses: dict[str, str] = {}
    issues: dict[str, str] = {}
    by_city: dict[str, list[dict]] = {}

    for r in st_rows:
        tc = r["telecode"]
        flat, flon = float(r["lat"]), float(r["lon"])
        sources[tc] = r.get("coord_source") or ""
        statuses[tc] = r.get("coord_status") or "ok"
        if not in_china_bbox(flat, flon):
            issues[tc] = "outside_china_bbox"
            continue
        coords[tc] = (flat, flon)
        ct = norm_city(r.get("city"))
        if ct:
            by_city.setdefault(ct, []).append(r)

    for ct, grp in by_city.items():
        if len(grp) < 3:
            continue
        lats = sorted(float(x["lat"]) for x in grp)
        lons = sorted(float(x["lon"]) for x in grp)
        med_lat = lats[len(lats) // 2]
        med_lon = lons[len(lons) // 2]
        for x in grp:
            tc = x["telecode"]
            if statuses.get(tc) == "verified" or sources.get(tc) == "manual":
                continue
            d = haversine_km(float(x["lat"]), float(x["lon"]), med_lat, med_lon)
            if d > outlier_km:
                issues.setdefault(tc, f"city_outlier:{d}km>{outlier_km}km")

    speed_suspects = _check_timetable_speeds(coords, sources, statuses, max_speed_kmh=max_speed_kmh)
    for tc, msg in speed_suspects.items():
        issues.setdefault(tc, msg)

    with db._write_lock, db.tx(bump=True) as conn:
        # 保留 verified，其余有坐标点默认置 ok 再对 issues 标 suspect
        conn.execute(
            "UPDATE stations SET coord_status='ok', coord_issue='' "
            "WHERE lat IS NOT NULL AND lon IS NOT NULL AND coord_status!='verified'"
        )
        if issues:
            conn.executemany(
                "UPDATE stations SET coord_status='suspect', coord_issue=? WHERE telecode=?",
                [(msg, tc) for tc, msg in issues.items()],
            )
    return {"checked": len(st_rows), "suspect": len(issues)}


def recompute_rail_proximity() -> dict:
    """用已入库 heavy-rail 站点与 43 城重轨映射，重算所有 ok/verified 国铁站的最近重轨距离、接驳方式及大站标记。"""
    transit_cities = load_transit_cities()
    stops = db.list_rail_stops()
    stop_grid: dict[tuple[int, int], list[tuple[float, float, str]]] = {}
    for s in stops:
        lat, lon = float(s["lat"]), float(s["lon"])
        gb = (math.floor(lat * 2.0), math.floor(lon * 2.0))
        stop_grid.setdefault(gb, []).append((lat, lon, s.get("transit_zone_id") or ""))

    st_rows = db.rows(
        "SELECT telecode, name, city, lat, lon, coord_status, transit_zone_id, is_major_hub FROM stations"
    )
    updates: list[dict] = []
    matched_walk = matched_short = matched_long = 0
    for r in st_rows:
        tc = r["telecode"]
        nm = r["name"]
        ct = r.get("city") or ""
        zone = (r.get("transit_zone_id") or "").strip() or default_zone_for_station(nm, ct)
        nm_clean = nm.removesuffix("站")
        is_hub = bool(r.get("is_major_hub")) or (nm in DEFAULT_MAJOR_HUB_NAMES) or (nm_clean in DEFAULT_MAJOR_HUB_NAMES)

        dist_m: int | None = None
        if r["lat"] is not None and r["lon"] is not None and r.get("coord_status") in ("ok", "verified") and stop_grid:
            flat, flon = float(r["lat"]), float(r["lon"])
            lb, jb = math.floor(flat * 2.0), math.floor(flon * 2.0)
            best_km: float | None = None
            for bi in range(lb - 1, lb + 2):
                for bj in range(jb - 1, jb + 2):
                    for slat, slon, _sz in stop_grid.get((bi, bj), ()):
                        dkm = haversine_km(flat, flon, slat, slon)
                        if best_km is None or dkm < best_km:
                            best_km = dkm
            if best_km is not None and best_km <= 30.0:
                dist_m = int(round(best_km * 1000.0))

        # 若该站城市不在重轨城市列表且距离很远或无站点，保持真实测距，access_mode 由距离自动分级
        mode = db.rail_access_mode_for(dist_m)
        if mode == "walk":
            matched_walk += 1
        elif mode == "short_bus":
            matched_short += 1
        elif mode == "long_bus_or_taxi":
            matched_long += 1
        updates.append({
            "telecode": tc,
            "transit_zone_id": zone,
            "nearest_rail_stop_distance_m": dist_m,
            "rail_access_mode": mode,
            "is_major_hub": is_hub,
        })
    db.batch_update_station_rail_proximity(updates)
    return {
        "stations": len(updates),
        "stops": len(stops),
        "walk": matched_walk,
        "short_bus": matched_short,
        "long_bus_or_taxi": matched_long,
    }


def _build_station_name_index(pending_rows: list[dict]) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    by_norm: dict[str, list[dict]] = {}
    by_tc: dict[str, dict] = {}
    for r in pending_rows:
        tc = r["telecode"]
        by_tc[tc] = r
        raw = r["name"].strip()
        for cand in {raw, _norm_station_name(raw)}:
            if cand:
                by_norm.setdefault(cand, []).append(r)
    return by_norm, by_tc


def _split_osm_elements(elements: list[dict], pending_rows: list[dict], transit_cities: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """一次解析 Overpass 切片，拆分产出国铁站点匹配与 43 城重轨站点。"""
    by_norm, by_tc = _build_station_name_index(pending_rows)
    city_anchors: dict[str, tuple[float, float]] = {}
    for r in db.rows("SELECT city, AVG(lat) alat, AVG(lon) alon FROM stations WHERE lat IS NOT NULL AND lon IS NOT NULL GROUP BY city"):
        ct = norm_city(r.get("city"))
        if ct and r["alat"] is not None:
            city_anchors[ct] = (float(r["alat"]), float(r["alon"]))

    best_for_tc: dict[str, tuple[float, float, float, float]] = {}  # tc -> (score, dist_anchor, lat, lon)
    rail_stops: list[dict] = []

    for el in elements:
        tags = el.get("tags") or {}
        lat = el.get("lat") if el.get("lat") is not None else (el.get("center") or {}).get("lat")
        lon = el.get("lon") if el.get("lon") is not None else (el.get("center") or {}).get("lon")
        if lat is None or lon is None:
            continue
        try:
            flat, flon = float(lat), float(lon)
        except (TypeError, ValueError):
            continue
        if not in_china_bbox(flat, flon):
            continue

        st_tag = str(tags.get("station") or "").lower()
        rw_tag = str(tags.get("railway") or "").lower()
        # 严格剔除有轨电车
        if st_tag == "tram" or rw_tag == "tram_stop" or tags.get("tram") == "yes":
            continue

        # 判定是否重轨地铁/轻轨/市域/跨座单轨站点
        is_subway = st_tag == "subway" or tags.get("subway") == "yes"
        is_light = st_tag == "light_rail" or tags.get("light_rail") == "yes"
        is_mono = st_tag == "monorail" or tags.get("monorail") == "yes"
        if is_subway or is_light or is_mono:
            mode = "subway" if is_subway else ("monorail" if is_mono else "light_rail")
            raw_city = norm_city(tags.get("addr:city") or tags.get("is_in:city") or tags.get("city") or "")
            if not raw_city:
                # 退用最近已知城市锚点，仅保留属于 43 城重轨圈群的站点
                best_ct = ""
                best_d = 45.0
                for ct_name, (alat, alon) in city_anchors.items():
                    if ct_name not in transit_cities:
                        continue
                    dkm = haversine_km(flat, flon, alat, alon)
                    if dkm < best_d:
                        best_d = dkm
                        best_ct = ct_name
                raw_city = best_ct
            if raw_city and raw_city in transit_cities:
                stop_name = (tags.get("name:zh") or tags.get("name:zh-Hans") or tags.get("name") or "").strip()
                if stop_name:
                    rail_stops.append({
                        "name": stop_name,
                        "city": raw_city,
                        "transit_zone_id": CROSS_CITY_TRANSIT_ZONES.get(raw_city, raw_city),
                        "mode": mode,
                        "lat": flat,
                        "lon": flon,
                        "source": "overpass",
                    })
            continue

        # 国铁车站匹配
        base_prio = 0.7 if rw_tag == "station" else 0.5
        ref_tc = (tags.get("ref:railway:telecode") or tags.get("ref:CR") or "").strip().upper()
        matched_rows: list[dict] = []
        exact_tele = False
        if ref_tc and ref_tc in by_tc:
            matched_rows = [by_tc[ref_tc]]
            exact_tele = True
        else:
            names = [tags.get(k) for k in ("name:zh", "name:zh-Hans", "name:zh-CN", "name", "alt_name") if tags.get(k)]
            seen_tc = set()
            for nm in names:
                for cand in (nm.strip(), _norm_station_name(nm)):
                    for row in by_norm.get(cand, ()):
                        if row["telecode"] not in seen_tc:
                            seen_tc.add(row["telecode"])
                            matched_rows.append(row)
        if not matched_rows:
            continue
        for row in matched_rows:
            tc = row["telecode"]
            ct = norm_city(row.get("city"))
            anchor_d = haversine_km(flat, flon, *city_anchors[ct]) if ct in city_anchors else 9999.0
            anchor_bonus = 0.2 if anchor_d <= 80.0 else 0.0
            conf = min(1.0, (0.98 if exact_tele else base_prio + anchor_bonus + (0.05 if len(matched_rows) == 1 else 0.0)))
            cand_score = (conf, -anchor_d)
            cur = best_for_tc.get(tc)
            if cur is None or cand_score > (cur[0], cur[1]):
                best_for_tc[tc] = (conf, -anchor_d, flat, flon)

    cr_hits = [
        {"telecode": tc, "lat": v[2], "lon": v[3], "source": "overpass", "status": "ok", "confidence": round(v[0], 2)}
        for tc, v in best_for_tc.items()
    ]
    return cr_hits, rail_stops


def manual_nominatim_lookup(telecode: str, http_fn=None) -> dict:
    """单站手工点查 Nominatim（严格 >=1.5s 冷却门禁，严禁用于自动批量抓取）。"""
    global _last_nom_ts
    tc = (telecode or "").strip().upper()
    row = db.rows("SELECT telecode, name, city FROM stations WHERE telecode=?", (tc,))
    if not row:
        raise ValueError(f"车站电报码“{tc}”不存在")
    st = row[0]
    with _nom_lock:
        now = time.monotonic()
        wait_need = NOMINATIM_COOLDOWN_SEC - (now - _last_nom_ts)
        if wait_need > 0:
            raise ValueError(f"Nominatim 手工点查需遵守公开服务礼貌间隔（请等待 {wait_need:.1f} 秒后再试）")
        _last_nom_ts = now

    fn = http_fn or _http_request
    name = st["name"]
    city = st.get("city") or ""
    queries = [f"{name}火车站 China"]
    if city and norm_city(city) not in name:
        queries.append(f"{norm_city(city)} {name}站")
    for q in queries:
        raw = fn(
            NOMINATIM_SEARCH_URL,
            params={"q": q, "format": "jsonv2", "limit": 5, "countrycodes": "cn", "accept-language": "zh-CN"},
            timeout=25,
        )
        arr = json.loads(raw.decode("utf-8", errors="replace"))
        if not isinstance(arr, list):
            continue
        for item in arr:
            cls = item.get("class")
            tp = item.get("type")
            if cls == "railway" or tp in ("station", "halt", "train_station") or "火车站" in (item.get("display_name") or ""):
                lat, lon = float(item["lat"]), float(item["lon"])
                if in_china_bbox(lat, lon):
                    return {
                        "telecode": tc,
                        "name": name,
                        "lat": round(lat, 6),
                        "lon": round(lon, 6),
                        "display_name": item.get("display_name") or "",
                        "source": "nominatim_manual",
                    }
    raise ValueError(f"Nominatim 未检索到车站 {name}({tc}) 的合法中国境内坐标")


class GeoCoordCrawler:
    """合规地理坐标与重轨站点批量抓取器（Overpass 区域切片+试跑估算 + Wikidata 补盲；支持断点续跑与安全取消）。"""

    def __init__(self, state_path: Path = GEO_STATE_PATH, http_fn=None):
        self.state_path = state_path
        self._http = http_fn or _http_request

    def _sleep(self, sec: float, cancel_event: threading.Event | None):
        if sec <= 0:
            if cancel_event and cancel_event.is_set():
                raise InterruptedError("地理抓取任务已取消")
            return
        if cancel_event is None:
            time.sleep(sec)
        elif cancel_event.wait(sec):
            raise InterruptedError("地理抓取任务已取消")

    def _query_overpass_tile(self, bbox: tuple[float, float, float, float]) -> tuple[list[dict], int]:
        s, w, n, e = bbox
        ql = (
            f"[out:json][timeout:60];"
            f"(node[\"railway\"~\"station|halt\"]({s},{w},{n},{e});"
            f"way[\"railway\"~\"station|halt\"]({s},{w},{n},{e}););"
            f"out center tags;"
        )
        body = urllib.parse.urlencode({"data": ql}).encode("utf-8")
        last_err = None
        for ep in OVERPASS_ENDPOINTS:
            for attempt in range(2):
                try:
                    raw = self._http(ep, data=body, timeout=75)
                    obj = json.loads(raw.decode("utf-8", errors="replace"))
                    return (obj.get("elements") or []), len(raw)
                except Exception as exc:
                    last_err = exc
                    time.sleep(2.0 * (attempt + 1))
        raise RuntimeError(f"Overpass 切片查询失败: {last_err}")

    def _query_wikidata_bulk(self) -> list[dict]:
        sparql = """
SELECT ?stationLabel ?coord ?tele WHERE {
  ?station wdt:P31/wdt:P279* wd:Q55488 ;
           wdt:P17 wd:Q148 ;
           wdt:P625 ?coord .
  OPTIONAL { ?station wdt:P238 ?tele . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "zh-hans,zh-cn,zh,en". }
}
"""
        raw = self._http(
            WIKIDATA_SPARQL_URL,
            params={"format": "json", "query": sparql},
            headers={"Accept": "application/sparql-results+json"},
            timeout=70,
        )
        obj = json.loads(raw.decode("utf-8", errors="replace"))
        return (obj.get("results") or {}).get("bindings") or []

    def run(
        self,
        *,
        force: bool = False,
        reset_checkpoint: bool = False,
        use_overpass: bool = True,
        use_wikidata: bool = True,
        trial_tiles: int | None = None,
        overpass_delay: float = 3.5,
        cancel_event: threading.Event | None = None,
        progress_cb=None,
        log_cb=None,
    ) -> dict:
        """执行合规批量地理抓取（严禁在此批量流中调用 Nominatim）。"""
        state = load_geo_state(self.state_path, reset=reset_checkpoint)
        if force:
            state["completed_tiles"] = []
            state["wikidata_done"] = False
            state["tile_bytes"] = 0
            state["tile_requests"] = 0
            save_geo_state(state, self.state_path)

        def log(msg: str):
            if log_cb:
                log_cb(msg)

        def pending_stations() -> list[dict]:
            if force:
                return db.rows("SELECT telecode, name, city FROM stations WHERE coord_status!='verified' ORDER BY telecode")
            return db.rows(
                "SELECT telecode, name, city FROM stations WHERE (lat IS NULL OR lon IS NULL) AND coord_status!='verified' ORDER BY telecode"
            )

        total_stations = db.rows("SELECT COUNT(*) c FROM stations")[0]["c"]
        if total_stations == 0:
            raise ValueError("站名表为空，请先同步 12306 站名表后再抓取地理坐标")

        tiles = CHINA_OVERPASS_TILES[:max(1, int(trial_tiles))] if trial_tiles else CHINA_OVERPASS_TILES
        effective_wikidata = use_wikidata and (trial_tiles is None)
        total_units = (len(tiles) if use_overpass else 0) + (1 if effective_wikidata else 0)
        done_units = 0
        matched_overpass = 0
        matched_wikidata = 0
        matched_rail_stops = 0
        trial_req_cnt = 0
        trial_bytes = 0
        errors: list[str] = []
        transit_cities = load_transit_cities()

        def emit_prog(cur_msg: str, done_u: int, tot_u: int, sub_d: int = 0, sub_t: int = 0):
            if progress_cb:
                progress_cb(done_u, max(1, tot_u), cur_msg, sub_d, sub_t)

        # Stage 1: Overpass 区域切片同步抓取国铁站 + 43 城重轨站点
        if use_overpass:
            done_tiles = set(state.get("completed_tiles", []))
            for idx, (tile_name, bbox) in enumerate(tiles, 1):
                if cancel_event and cancel_event.is_set():
                    raise InterruptedError("地理抓取任务已取消")
                if tile_name in done_tiles and not trial_tiles:
                    done_units += 1
                    emit_prog(f"跳过已完成切片：{tile_name}（{idx}/{len(tiles)}）", done_units, total_units)
                    continue
                pend = pending_stations()
                emit_prog(f"Overpass 切片采集：{tile_name}（{idx}/{len(tiles)}）", done_units, total_units)
                try:
                    elements, raw_bytes = self._query_overpass_tile(bbox)
                    trial_req_cnt += 1
                    trial_bytes += raw_bytes
                    cr_hits, rail_hits = _split_osm_elements(elements, pend, transit_cities)
                    if cr_hits:
                        db.batch_update_station_coords(cr_hits)
                        matched_overpass += len(cr_hits)
                    if rail_hits:
                        db.batch_upsert_rail_stops(rail_hits)
                        matched_rail_stops += len(rail_hits)
                    if tile_name not in state["completed_tiles"]:
                        state["completed_tiles"].append(tile_name)
                    state["tile_requests"] = int(state.get("tile_requests") or 0) + 1
                    state["tile_bytes"] = int(state.get("tile_bytes") or 0) + raw_bytes
                    state["stats"]["overpass"] = state["stats"].get("overpass", 0) + len(cr_hits)
                    state["stats"]["rail_stops"] = state["stats"].get("rail_stops", 0) + len(rail_hits)
                    save_geo_state(state, self.state_path)
                    log(
                        f"[Overpass] {tile_name}：{raw_bytes / 1024:.1f}KB，解析 {len(elements)} 节点，"
                        f"国铁+{len(cr_hits)} 站，重轨+{len(rail_hits)} 站"
                    )
                except Exception as exc:
                    err_txt = f"Overpass {tile_name}: {exc}"
                    errors.append(err_txt)
                    log(f"[警告] {err_txt}")
                done_units += 1
                if idx < len(tiles):
                    self._sleep(overpass_delay, cancel_event)

        # Stage 2: Wikidata SPARQL 境内全量一次性补盲
        if effective_wikidata:
            if cancel_event and cancel_event.is_set():
                raise InterruptedError("地理抓取任务已取消")
            pend = pending_stations()
            if not pend or state.get("wikidata_done"):
                done_units += 1
                log("跳过 Wikidata SPARQL（已执行或已无缺失车站）")
            else:
                emit_prog("Wikidata SPARQL 批量补漏查询…", done_units, total_units)
                try:
                    rows = self._query_wikidata_bulk()
                    by_norm, _ = _build_station_name_index(pend)
                    hits_map: dict[str, dict] = {}
                    for b in rows:
                        coord_val = ((b.get("coord") or {}).get("value")) or ""
                        m = re.match(r"Point\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)", coord_val)
                        if not m:
                            continue
                        lon_v, lat_v = float(m.group(1)), float(m.group(2))
                        if not in_china_bbox(lat_v, lon_v):
                            continue
                        lbl = ((b.get("stationLabel") or {}).get("value")) or ""
                        norm_lbl = _norm_station_name(lbl)
                        for row in by_norm.get(norm_lbl, ()):
                            if row["telecode"] not in hits_map:
                                hits_map[row["telecode"]] = {
                                    "telecode": row["telecode"],
                                    "lat": lat_v,
                                    "lon": lon_v,
                                    "source": "wikidata",
                                    "status": "ok",
                                    "confidence": 0.85,
                                }
                    hits = list(hits_map.values())
                    if hits:
                        db.batch_update_station_coords(hits)
                        matched_wikidata += len(hits)
                    state["wikidata_done"] = True
                    state["stats"]["wikidata"] = state["stats"].get("wikidata", 0) + len(hits)
                    save_geo_state(state, self.state_path)
                    log(f"[Wikidata] SPARQL 返回 {len(rows)} 条记录，新匹配入库 {len(hits)} 站")
                except Exception as exc:
                    err_txt = f"Wikidata SPARQL: {exc}"
                    errors.append(err_txt)
                    log(f"[警告] {err_txt}")
                done_units += 1

        audit = audit_station_coords()
        prox = recompute_rail_proximity()
        cov = geo_coverage_status()
        est_full_mb = round((trial_bytes / max(1, trial_req_cnt)) * len(CHINA_OVERPASS_TILES) / (1024 * 1024), 2) if trial_req_cnt else 0.0
        if trial_tiles:
            log(
                f"[Overpass 试跑配额估算] 已试跑 {trial_tiles} 区域片：{trial_req_cnt} 次请求，"
                f"累计 {trial_bytes / (1024 * 1024):.2f} MB；外推全国全量约 {len(CHINA_OVERPASS_TILES)} 次请求 / {est_full_mb:.2f} MB（远低于日限）"
            )
        emit_prog("地理坐标与重轨邻近计算完成", total_units, max(1, total_units), 0, 0)
        return {
            "trial_tiles": trial_tiles,
            "trial_requests": trial_req_cnt,
            "trial_mb": round(trial_bytes / (1024 * 1024), 2),
            "estimated_full_requests": len(CHINA_OVERPASS_TILES),
            "estimated_full_mb": est_full_mb,
            "matched_overpass": matched_overpass,
            "matched_wikidata": matched_wikidata,
            "matched_rail_stops": matched_rail_stops,
            "total_matched": matched_overpass + matched_wikidata,
            "audit": audit,
            "proximity": prox,
            "coverage": cov,
            "errors": errors,
        }
