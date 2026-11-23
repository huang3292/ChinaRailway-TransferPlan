import json
import math
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "rail.db"
CN_TZ = ZoneInfo("Asia/Shanghai")

_local = threading.local()
_gen_lock = threading.Lock()
_write_lock = threading.Lock()
_data_gen = 0

MAX_TRANSFER_GROUP_MEMBERS = 30
MIN_TRANSFER_GROUP_EXTRA_MIN = 5


def bump_data_version() -> int:
    """递增进程内数据变更代数，触发内存检索缓存失效重建。"""
    global _data_gen
    with _gen_lock:
        _data_gen += 1
        return _data_gen


def _file_sig(p: Path, ignore_empty: bool = False) -> tuple:
    try:
        st = p.stat()
        if ignore_empty and st.st_size == 0:
            return (0, 0)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return (0, 0)


def data_version() -> tuple:
    """返回兼顾进程内写入、跨进程文件修改与库路径切换的缓存版本键。"""
    with _gen_lock:
        gen = _data_gen
    wal_path = Path(f"{DB_PATH}-wal")
    return (str(DB_PATH), gen, _file_sig(DB_PATH), _file_sig(wal_path, ignore_empty=True))


def cn_now() -> datetime:
    return datetime.now(CN_TZ).replace(tzinfo=None)


def cn_today() -> date:
    return cn_now().date()

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations(
  telecode TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  pinyin TEXT,
  short TEXT,
  city TEXT,
  lat REAL,
  lon REAL,
  coord_source TEXT DEFAULT '',
  coord_updated_at TEXT DEFAULT '',
  coord_status TEXT DEFAULT 'missing',
  coord_grid TEXT DEFAULT '',
  coord_confidence REAL DEFAULT 0,
  coord_issue TEXT DEFAULT '',
  transit_zone_id TEXT DEFAULT '',
  nearest_rail_stop_distance_m INTEGER,
  rail_access_mode TEXT DEFAULT 'none',
  is_major_hub INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_stations_name ON stations(name);

CREATE TABLE IF NOT EXISTS rail_transit_stops(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  city TEXT NOT NULL DEFAULT '',
  transit_zone_id TEXT NOT NULL DEFAULT '',
  mode TEXT NOT NULL DEFAULT 'subway' CHECK(mode IN ('subway','light_rail','suburban','monorail')),
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  grid_key TEXT DEFAULT '',
  source TEXT DEFAULT 'overpass',
  UNIQUE(name, city, mode, lat, lon)
);
CREATE INDEX IF NOT EXISTS idx_rts_grid ON rail_transit_stops(grid_key);
CREATE INDEX IF NOT EXISTS idx_rts_zone ON rail_transit_stops(transit_zone_id);

CREATE TABLE IF NOT EXISTS raw_payloads(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  api_type TEXT NOT NULL,
  params TEXT,
  source TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS station_trains(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_telecode TEXT NOT NULL,
  station_name TEXT,
  query_date TEXT,
  station_train_code TEXT NOT NULL,
  train_no TEXT,
  station_train_date TEXT,
  start_train_date TEXT,
  station_no INTEGER,
  arrive_time TEXT,
  start_time TEXT,
  stopover_time TEXT,
  arrive_day_diff INTEGER,
  start_day_diff INTEGER,
  start_station_name TEXT,
  start_station_telecode TEXT,
  end_station_name TEXT,
  end_station_telecode TEXT,
  train_class_name TEXT,
  seat_types TEXT,
  running_fig TEXT,
  train_style TEXT,
  jiaolu_train_style TEXT,
  start_start_time TEXT,
  end_arrive_time TEXT,
  running_time TEXT,
  distance INTEGER,
  raw_id INTEGER,
  UNIQUE(station_telecode, station_train_code, station_train_date)
);
CREATE INDEX IF NOT EXISTS idx_st_date ON station_trains(station_telecode, station_train_date);

CREATE TABLE IF NOT EXISTS train_stops(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  train_no TEXT NOT NULL,
  station_train_code TEXT,
  train_date TEXT,
  station_no INTEGER NOT NULL,
  station_name TEXT NOT NULL,
  telecode TEXT,
  arrive_time TEXT,
  start_time TEXT,
  stopover_time TEXT,
  enabled INTEGER DEFAULT 1,
  raw_id INTEGER,
  UNIQUE(train_no, train_date, station_no)
);
CREATE INDEX IF NOT EXISTS idx_ts_station ON train_stops(station_name);
CREATE INDEX IF NOT EXISTS idx_ts_code ON train_stops(station_train_code);
CREATE INDEX IF NOT EXISTS idx_ts_telecode_train ON train_stops(telecode, train_no);
CREATE INDEX IF NOT EXISTS idx_st_train_date ON station_trains(train_no, station_train_date);

CREATE TABLE IF NOT EXISTS tickets(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  train_no TEXT NOT NULL,
  station_train_code TEXT,
  train_date TEXT NOT NULL,
  from_telecode TEXT,
  to_telecode TEXT,
  from_station_no TEXT,
  to_station_no TEXT,
  start_time TEXT,
  arrive_time TEXT,
  duration TEXT,
  can_web_buy TEXT,
  seat_types TEXT,
  seats TEXT,
  raw_id INTEGER,
  UNIQUE(train_no, train_date, from_telecode, to_telecode)
);
CREATE INDEX IF NOT EXISTS idx_tk_query ON tickets(from_telecode, to_telecode, train_date);

CREATE TABLE IF NOT EXISTS prices(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  train_no TEXT NOT NULL,
  station_train_code TEXT,
  train_date TEXT,
  from_station_no TEXT,
  to_station_no TEXT,
  seat_type TEXT NOT NULL,
  seat_name TEXT,
  price REAL,
  raw_id INTEGER,
  UNIQUE(train_no, train_date, from_station_no, to_station_no, seat_type)
);
CREATE INDEX IF NOT EXISTS idx_pr_lookup ON prices(train_no, from_station_no, to_station_no, train_date);

CREATE TABLE IF NOT EXISTS settings(
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS transfer_groups(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  default_extra_min INTEGER,
  note TEXT DEFAULT '',
  enabled_by_default INTEGER NOT NULL DEFAULT 1,
  connect_mode TEXT NOT NULL DEFAULT 'full_mesh',
  hub_telecode TEXT DEFAULT '',
  created_at TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS transfer_group_members(
  group_id INTEGER NOT NULL,
  telecode TEXT NOT NULL,
  PRIMARY KEY(group_id, telecode),
  FOREIGN KEY(group_id) REFERENCES transfer_groups(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_tgm_tc ON transfer_group_members(telecode);

CREATE TABLE IF NOT EXISTS transfer_pair_overrides(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  a_telecode TEXT NOT NULL,
  b_telecode TEXT NOT NULL,
  mode TEXT NOT NULL CHECK(mode IN ('allow', 'block')),
  custom_extra_min INTEGER,
  note TEXT DEFAULT '',
  created_at TEXT DEFAULT '',
  updated_at TEXT DEFAULT '',
  UNIQUE(a_telecode, b_telecode)
);
"""


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-131072")  # 128MB 内存页缓存，减少全表扫描换页读盘
        try:
            conn.execute("PRAGMA mmap_size=536870912")  # 512MB 内存映射只读加速
        except sqlite3.Error:
            pass
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


def close_conn():
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


@contextmanager
def tx(bump: bool = True):
    conn = get_conn()
    before = conn.total_changes
    try:
        yield conn
        conn.commit()
        if bump and conn.total_changes != before:
            bump_data_version()
    except Exception:
        conn.rollback()
        raise


def init_db():
    with _write_lock, tx() as conn:
        conn.executescript(SCHEMA)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(station_trains)")}
        added = [c for c in ("start_start_time", "end_arrive_time", "running_time", "distance",
                             "train_style", "jiaolu_train_style") if c not in cols]
        for c in added:
            typ = "INTEGER" if c == "distance" else "TEXT"
            conn.execute(f"ALTER TABLE station_trains ADD COLUMN {c} {typ}")
        st_cols = {r[1] for r in conn.execute("PRAGMA table_info(stations)")}
        st_migrations = (
            ("lat", "REAL"),
            ("lon", "REAL"),
            ("coord_source", "TEXT DEFAULT ''"),
            ("coord_updated_at", "TEXT DEFAULT ''"),
            ("coord_status", "TEXT DEFAULT 'missing'"),
            ("coord_grid", "TEXT DEFAULT ''"),
            ("coord_confidence", "REAL DEFAULT 0"),
            ("coord_issue", "TEXT DEFAULT ''"),
            ("transit_zone_id", "TEXT DEFAULT ''"),
            ("nearest_rail_stop_distance_m", "INTEGER"),
            ("rail_access_mode", "TEXT DEFAULT 'none'"),
            ("is_major_hub", "INTEGER NOT NULL DEFAULT 0"),
        )
        for c, typ in st_migrations:
            if c not in st_cols:
                conn.execute(f"ALTER TABLE stations ADD COLUMN {c} {typ}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stations_coords ON stations(coord_status, lat, lon)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stations_grid ON stations(coord_grid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stations_zone ON stations(transit_zone_id)")

        tg_cols = {r[1] for r in conn.execute("PRAGMA table_info(transfer_groups)")}
        for c, typ in (
            ("connect_mode", "TEXT NOT NULL DEFAULT 'full_mesh'"),
            ("hub_telecode", "TEXT DEFAULT ''"),
        ):
            if c not in tg_cols:
                conn.execute(f"ALTER TABLE transfer_groups ADD COLUMN {c} {typ}")

        tp_cols = {r[1] for r in conn.execute("PRAGMA table_info(transfer_pair_overrides)")}
        if "updated_at" not in tp_cols:
            conn.execute("ALTER TABLE transfer_pair_overrides ADD COLUMN updated_at TEXT DEFAULT ''")
    if added:
        _backfill_screen()
    bump_data_version()


def _backfill_screen():
    """旧库升级：用原始留档重新解析车站大屏，补齐新增字段。"""
    from . import importer, parsers

    for r in rows("SELECT id,params,payload FROM raw_payloads WHERE api_type='station_screen'"):
        try:
            recs = parsers.parse_station_screen(json.loads(r["payload"]), json.loads(r["params"] or "{}"))
            importer._save("station_screen", recs, r["id"])
        except (ValueError, KeyError):
            continue


def rows(sql, params=()):
    return [dict(r) for r in get_conn().execute(sql, params).fetchall()]


def archive_raw(api_type: str, payload: str, params: dict, source: str) -> int:
    with tx() as conn:
        cur = conn.execute(
            "INSERT INTO raw_payloads(api_type,params,source,fetched_at,payload) VALUES(?,?,?,?,?)",
            (api_type, json.dumps(params, ensure_ascii=False), source,
             cn_now().isoformat(timespec="seconds"), payload),
        )
        return cur.lastrowid


def clear_cache(before_date: str | None = None, include_raw: bool = False) -> dict:
    deleted = {}
    with tx() as conn:
        if before_date:
            deleted["station_trains"] = conn.execute(
                "DELETE FROM station_trains WHERE station_train_date < ?", (before_date,)).rowcount
            deleted["train_stops"] = conn.execute(
                "DELETE FROM train_stops WHERE train_date != '' AND train_date < ?", (before_date,)).rowcount
            deleted["tickets"] = conn.execute(
                "DELETE FROM tickets WHERE train_date < ?", (before_date,)).rowcount
            deleted["prices"] = conn.execute(
                "DELETE FROM prices WHERE train_date != '' AND train_date < ?", (before_date,)).rowcount
            if include_raw:
                deleted["raw_payloads"] = conn.execute(
                    "DELETE FROM raw_payloads WHERE substr(fetched_at, 1, 10) < ?", (before_date,)).rowcount
        else:
            for tbl in ("station_trains", "train_stops", "tickets", "prices"):
                deleted[tbl] = conn.execute(f"DELETE FROM {tbl}").rowcount
            if include_raw:
                deleted["raw_payloads"] = conn.execute("DELETE FROM raw_payloads").rowcount
    return deleted


def station_lookup() -> dict:
    return {r["name"]: r["telecode"] for r in rows("SELECT name,telecode FROM stations")}


def resolve_station(q: str) -> dict | None:
    q = (q or "").strip()
    if not q:
        return None
    # 精确电报码、精确站名优先于去“站”后缀匹配，避免同名/近名顺序不确定
    r = rows(
        """SELECT * FROM stations WHERE telecode=? OR name=? OR name=?
           ORDER BY CASE WHEN telecode=? THEN 0 WHEN name=? THEN 1 ELSE 2 END LIMIT 1""",
        (q.upper(), q, q.removesuffix("站"), q.upper(), q),
    )
    if r:
        return r[0]
    m = re.fullmatch(r"(.+?)\s*\(([A-Za-z]{3})\)", q)
    if m:
        tc, nm = m.group(2).upper(), m.group(1).strip()
        r = rows("SELECT * FROM stations WHERE telecode=? OR name=? ORDER BY CASE WHEN telecode=? THEN 0 ELSE 1 END LIMIT 1",
                 (tc, nm, tc))
        if r:
            return r[0]
    return None


# 部分城市的城区/近郊核心枢纽站名不以城市名开头，手动补充
CITY_PROPER = {
    "北京": ("清河", "昌平北", "亦庄", "通州"),
    "天津": ("滨海", "滨海西", "滨海北", "塘沽", "军粮城北"),
    "上海": ("松江", "金山北", "南翔北", "安亭北", "安亭西", "练塘"),
    "广州": ("庆盛", "新塘", "南沙北"),
    "深圳": ("光明城", "坪山"),
    "武汉": ("武昌", "汉口"),
    "重庆": ("沙坪坝", "菜园坝", "复盛"),
    "成都": ("犀浦", "双流机场", "双流西", "天府机场", "安靖"),
    "西安": ("阿房宫", "鄠邑", "引镇"),
    "南京": ("江宁", "仙林", "江浦"),
    "杭州": ("富阳", "富阳西", "钱塘"),
    "郑州": ("航空港", "南曹"),
    "沈阳": ("皇姑屯", "苏家屯"),
    "哈尔滨": ("呼兰", "王岗"),
}


def city_stations(q: str) -> list[dict]:
    """返回某城市（如“厦门”“南昌”，可带“市”后缀）的城区车站，主站优先。

    城市范围只保留“站名以城市名开头”的车站（厦门/厦门北/厦门高崎、南昌/南昌西/南昌东/南昌南），
    另加 CITY_PROPER 别名（武汉=武昌/汉口），避免把郊县站（如南昌的向塘/进贤）误算进市区。
    """
    q = (q or "").strip()
    base = q.removesuffix("市")
    if not base:
        return []
    all_rows = rows("SELECT * FROM stations WHERE city=? OR city=? OR city=?",
                    (q, base, base + "市"))
    proper = set(CITY_PROPER.get(base, ()))
    out = [r for r in all_rows
           if r["name"].startswith(base) or r["name"] in proper or r["name"].removesuffix("站") in proper]
    # WHY: 12306 station_name.js 中少数同名县级站归在地级市名下（如 建德→杭州、上杭→龙岩、兴安→桂林），
    # 而其新建/支线站挂在县级 city 下（建德东/蛟洋/兴安盟）；仅查 city 列会漏掉同名主站甚至错扩到异省同名盟市。
    exact = resolve_station(q)
    if exact and exact["name"] in (q, base, q.removesuffix("站")) and all(
        r["telecode"] != exact["telecode"] for r in out
    ):
        out.insert(0, exact)
    if not out:
        out = all_rows
    out.sort(key=lambda r: (0 if r["name"] in (base, base + "站", q) else 1, r["name"]))
    return out


def city_all_stations(q: str) -> list[dict]:
    """返回 city 列归属该市的全部车站（含郊县站，如泉州→晋江/德化/惠安…）。"""
    q = (q or "").strip()
    base = q.removesuffix("市")
    if not base:
        return []
    all_rows = rows("SELECT * FROM stations WHERE city=? OR city=? OR city=?",
                    (q, base, base + "市"))
    exact = resolve_station(q)
    if exact and exact["name"] in (q, base) and all(
        r["telecode"] != exact["telecode"] for r in all_rows
    ):
        all_rows.insert(0, exact)
    all_rows.sort(key=lambda r: (0 if r["name"].startswith(base) else 1, r["name"]))
    return all_rows


def _split_station_tokens(q: str) -> list[str]:
    norm = q.replace("，", ",").replace("、", ",").replace("；", ",").replace(";", ",")
    return [t.strip() for t in norm.split(",") if t.strip()]


def resolve_stations(q: str, expand_city: bool = True, city_mode: str | None = None) -> list[dict]:
    """解析出发/到达为车站列表。

    WHY: 12306 官网/App 在输入框的地名匹配层会把郊县站引到市内主站
    （如建阳→南平市站），但查询接口本身是按电报码精确查的；口径必须显式
    交给调用方，city_mode="single" 保证绝不换站。
    city_mode（优先于 expand_city）五档，与前端「车站范围」下拉一一对应：
      "single"=仅该站本身（站名/电报码精确，绝不换站、不拆分列表）；
      "name"=市内站名以城市名打头的站（`city_stations` 旧 expand_city=True 行为）；
      "string"=token 写作 "城市:字样"，取该市 city 归属且站名含字样的站
        （南平:延平→延平/延平东/延平西）；有冒号但未命中时严禁回退单站；无冒号时退化为 "all"；
      "mixed"=逗号/顿号/分号分隔的站名或电报码列表取并集，各 token 仅按
        单站精确解析（任一 token 未匹配即整体返回空列表，绝不悄悄忽略或做城市展开）；
      "all"=该地级市 city 归属的全部车站（含郊县站）。
    例外口径：安溪站 city='安溪'（非泉州），"all" 档按 city 字段会漏。
    """
    q = (q or "").strip()
    if not q:
        return []
    mode = city_mode or ("name" if expand_city else "single")
    if mode == "single":
        s = resolve_station(q)
        return [s] if s else []

    toks = _split_station_tokens(q)
    if not toks:
        return []

    out, seen = [], set()
    for tok in toks:
        cands: list[dict] = []
        if mode == "string":
            base, sep, pfx = tok.replace("：", ":").partition(":")
            base, pfx = base.strip(), pfx.strip()
            if sep:
                if base:
                    cands = city_all_stations(base)
                    if pfx:
                        cands = [r for r in cands if pfx in r["name"]]
            else:
                cands = city_all_stations(base)
                if not cands:
                    s = resolve_station(base)
                    if s:
                        cands = [s]
        elif mode == "mixed":
            # mixed 档默认把无冒号 token 严格当单个物理站/电报码（防止把城市名“南平”静默忽略或把“厦门”歧义展开），
            # 若显式带冒号修饰符（name:厦门 / all:泉州 / station:厦门 / 南平:延平）则允许与单站混合组合。
            if ":" in tok or "：" in tok:
                head, _, tail = tok.replace("：", ":").partition(":")
                head, tail = head.strip(), tail.strip()
                hl = head.lower()
                if hl == "name" and tail:
                    cands = city_stations(tail)
                elif hl == "all" and tail:
                    cands = city_all_stations(tail)
                elif hl == "station" and tail:
                    s = resolve_station(tail)
                    cands = [s] if s else []
                elif head:
                    cands = city_all_stations(head)
                    if tail:
                        cands = [r for r in cands if tail in r["name"]]
                if not cands:
                    return []
            else:
                s = resolve_station(tok)
                if not s:
                    return []
                cands = [s]
        elif mode == "name":
            cands = city_stations(tok)
            if not cands:
                s = resolve_station(tok)
                if s:
                    cands = [s]
        elif mode == "all":
            cands = city_all_stations(tok)
            if not cands:
                s = resolve_station(tok)
                if s:
                    cands = [s]
        for r in cands:
            if r["telecode"] not in seen:
                seen.add(r["telecode"])
                out.append(r)
    return out


def station_resolve_error(q: str, city_mode: str | None = None) -> str:
    """生成符合当前 city_mode 语义的车站解析失败提示。"""
    if not rows("SELECT 1 FROM stations LIMIT 1"):
        return "站名表为空，请先在「爬取数据」页更新车站站名表"
    q_s = (q or "").strip()
    if not q_s:
        return "请输入出发/到达车站或城市"
    if city_mode == "mixed":
        missing = []
        for t in _split_station_tokens(q_s):
            if ":" in t or "：" in t:
                if not resolve_stations(t, city_mode="mixed"):
                    missing.append(t)
            elif not resolve_station(t):
                missing.append(t)
        if missing:
            return f"混合多选仅支持精确站名/电报码或「城市:字样」，未识别：“{'、'.join(missing)}”"
    if city_mode == "string":
        for tok in _split_station_tokens(q_s):
            base, sep, pfx = tok.replace("：", ":").partition(":")
            base, pfx = base.strip(), pfx.strip()
            if sep and not base:
                return f"“{tok}”缺少城市名，请先输入城市再配合站名字样筛选"
            if sep and not city_all_stations(base):
                return f"城市“{base}”不存在或未归属任何车站"
            if sep and pfx:
                return f"城市“{base}”下未找到站名含“{pfx}”的车站"
    if city_mode == "single":
        return f"车站“{q_s}”不存在（①仅该站模式不支持城市名或多站），请确认站名或电报码"
    return f"车站/城市“{q_s}”不存在，请确认名称或先更新车站站名表"


def get_setting(key: str, default=None):
    r = rows("SELECT value FROM settings WHERE key=?", (key,))
    return r[0]["value"] if r else default


def set_setting(key: str, value, bump: bool = False):
    with tx(bump=bump) as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


def grid_key_for(lat: float | None, lon: float | None) -> str:
    if lat is None or lon is None:
        return ""
    return f"{math.floor(float(lat) * 2.0)}_{math.floor(float(lon) * 2.0)}"


def rail_access_mode_for(dist_m: int | float | None) -> str:
    if dist_m is None:
        return "none"
    d = float(dist_m)
    if d <= 800.0:
        return "walk"
    if d <= 1500.0:
        return "short_bus"
    return "long_bus_or_taxi"


def update_station_coords(
    telecode: str,
    lat: float | None,
    lon: float | None,
    source: str = "manual",
    status: str = "verified",
    confidence: float | None = None,
    issue: str = "",
    transit_zone_id: str | None = None,
    nearest_rail_stop_distance_m: int | None = -1,
    rail_access_mode: str | None = None,
    is_major_hub: bool | None = None,
):
    tc = (telecode or "").strip().upper()
    if status not in ("ok", "verified", "suspect", "missing"):
        raise ValueError("coord_status 必须为 ok / verified / suspect / missing")
    if lat is not None and lon is not None:
        flat, flon = float(lat), float(lon)
        grid = grid_key_for(flat, flon)
        conf = 1.0 if confidence is None and status == "verified" else float(confidence if confidence is not None else 0.9)
    else:
        flat, flon, grid = None, None, ""
        conf = 0.0
        if status in ("ok", "verified"):
            status = "missing"
    now_str = cn_now().isoformat(timespec="seconds")
    with _write_lock, tx(bump=True) as conn:
        existing = conn.execute(
            "SELECT transit_zone_id, nearest_rail_stop_distance_m, rail_access_mode, is_major_hub FROM stations WHERE telecode=?",
            (tc,),
        ).fetchone()
        if not existing:
            raise ValueError(f"车站电报码“{tc}”不存在")
        zone_val = existing[0] if transit_zone_id is None else str(transit_zone_id).strip()
        dist_val = existing[1] if nearest_rail_stop_distance_m == -1 else (
            None if nearest_rail_stop_distance_m is None else max(0, int(nearest_rail_stop_distance_m))
        )
        if rail_access_mode is not None:
            if rail_access_mode not in ("walk", "short_bus", "long_bus_or_taxi", "none"):
                raise ValueError("rail_access_mode 必须为 walk / short_bus / long_bus_or_taxi / none")
            mode_val = rail_access_mode
        elif nearest_rail_stop_distance_m != -1:
            mode_val = rail_access_mode_for(dist_val)
        else:
            mode_val = existing[2] or "none"
        hub_val = existing[3] if is_major_hub is None else (1 if is_major_hub else 0)
        conn.execute(
            """UPDATE stations
               SET lat=?, lon=?, coord_source=?, coord_updated_at=?, coord_status=?,
                   coord_grid=?, coord_confidence=?, coord_issue=?,
                   transit_zone_id=?, nearest_rail_stop_distance_m=?, rail_access_mode=?, is_major_hub=?
               WHERE telecode=?""",
            (flat, flon, source, now_str, status, grid, conf, (issue or "").strip(),
             zone_val, dist_val, mode_val, hub_val, tc),
        )


def batch_update_station_coords(items: list[dict]) -> int:
    """单写锁 + 单事务批量更新车站地理与质检字段，避免并发锁冲突与长事务抖动。"""
    if not items:
        return 0
    now_str = cn_now().isoformat(timespec="seconds")
    payload = []
    for it in items:
        lat, lon = it.get("lat"), it.get("lon")
        grid = it.get("grid") or grid_key_for(lat, lon)
        conf = float(it.get("confidence", 0.9 if (lat is not None and lon is not None) else 0.0))
        payload.append((
            lat,
            lon,
            it.get("source") or "geo",
            it.get("updated_at") or now_str,
            it.get("status") or ("ok" if lat is not None and lon is not None else "missing"),
            grid,
            conf,
            it.get("issue") or "",
            it["telecode"],
        ))
    with _write_lock, tx(bump=True) as conn:
        conn.executemany(
            """UPDATE stations
               SET lat=?, lon=?, coord_source=?, coord_updated_at=?, coord_status=?,
                   coord_grid=?, coord_confidence=?, coord_issue=?
               WHERE telecode=?""",
            payload,
        )
    return len(payload)


def batch_update_station_rail_proximity(items: list[dict]) -> int:
    """批量写入火车站最近重轨距离、接驳方式、圈群及大站标记。"""
    if not items:
        return 0
    payload = []
    for it in items:
        dist_m = it.get("nearest_rail_stop_distance_m")
        d_val = None if dist_m is None else max(0, int(dist_m))
        mode_val = it.get("rail_access_mode") or rail_access_mode_for(d_val)
        payload.append((
            it.get("transit_zone_id") or "",
            d_val,
            mode_val,
            1 if it.get("is_major_hub") else 0,
            it["telecode"],
        ))
    with _write_lock, tx(bump=True) as conn:
        conn.executemany(
            """UPDATE stations
               SET transit_zone_id=?, nearest_rail_stop_distance_m=?, rail_access_mode=?, is_major_hub=?
               WHERE telecode=?""",
            payload,
        )
    return len(payload)


def batch_upsert_rail_stops(stops: list[dict]) -> int:
    """批量写入 43 城重轨站点（地铁/轻轨/重轨市域/跨座单轨，排除有轨电车）。"""
    if not stops:
        return 0
    payload = []
    for s in stops:
        lat, lon = float(s["lat"]), float(s["lon"])
        mode = s.get("mode") or "subway"
        if mode not in ("subway", "light_rail", "suburban", "monorail"):
            continue
        payload.append((
            str(s.get("name") or "").strip(),
            str(s.get("city") or "").strip(),
            str(s.get("transit_zone_id") or "").strip(),
            mode,
            round(lat, 6),
            round(lon, 6),
            s.get("grid_key") or grid_key_for(lat, lon),
            str(s.get("source") or "overpass"),
        ))
    if not payload:
        return 0
    with _write_lock, tx(bump=True) as conn:
        conn.executemany(
            """INSERT OR IGNORE INTO rail_transit_stops(name, city, transit_zone_id, mode, lat, lon, grid_key, source)
               VALUES(?,?,?,?,?,?,?,?)""",
            payload,
        )
    return len(payload)


def list_rail_stops(city: str = "") -> list[dict]:
    if city:
        return rows("SELECT * FROM rail_transit_stops WHERE city=? ORDER BY name", (city,))
    return rows("SELECT * FROM rail_transit_stops")


def list_transfer_groups() -> list[dict]:
    groups = rows(
        "SELECT id, name, default_extra_min, note, enabled_by_default, connect_mode, hub_telecode, created_at "
        "FROM transfer_groups ORDER BY id ASC"
    )
    if not groups:
        return []
    mem_rows = rows(
        """SELECT m.group_id, m.telecode, s.name, s.city, s.lat, s.lon, s.coord_status
           FROM transfer_group_members m
           LEFT JOIN stations s ON s.telecode = m.telecode
           ORDER BY s.name, m.telecode"""
    )
    by_gid: dict[int, list[dict]] = {}
    for mr in mem_rows:
        by_gid.setdefault(mr["group_id"], []).append({
            "telecode": mr["telecode"],
            "name": mr["name"] or mr["telecode"],
            "city": mr["city"] or "",
            "has_coord": mr["lat"] is not None and mr["lon"] is not None,
            "coord_status": mr["coord_status"] or "missing",
        })
    for g in groups:
        g["enabled_by_default"] = bool(g["enabled_by_default"])
        g["connect_mode"] = g.get("connect_mode") or "full_mesh"
        g["hub_telecode"] = g.get("hub_telecode") or ""
        g["members"] = by_gid.get(g["id"], [])
    return groups


def save_transfer_group(name: str, telecodes: list[str],
                        default_extra_min: int | None = None,
                        note: str = "", enabled_by_default: bool = True,
                        connect_mode: str = "full_mesh",
                        hub_telecode: str = "",
                        group_id: int | None = None) -> dict:
    clean_name = (name or "").strip()
    if not clean_name:
        raise ValueError("换乘组名称不能为空")
    cm = (connect_mode or "full_mesh").strip()
    if cm not in ("full_mesh", "hub_star"):
        raise ValueError("connect_mode 必须为 full_mesh 或 hub_star")
    uniq_tcs = list(dict.fromkeys(str(tc).strip().upper() for tc in telecodes if str(tc).strip()))
    if len(uniq_tcs) < 2:
        raise ValueError("换乘组至少需要包含 2 个车站")
    if len(uniq_tcs) > MAX_TRANSFER_GROUP_MEMBERS:
        raise ValueError(f"换乘组成员数不能超过 {MAX_TRANSFER_GROUP_MEMBERS} 个（防止全连接组合爆炸）")
    if default_extra_min is not None:
        ex_i = int(default_extra_min)
        if cm == "full_mesh" and ex_i < MIN_TRANSFER_GROUP_EXTRA_MIN:
            raise ValueError(f"全连通(full_mesh)换乘组加时不能低于 {MIN_TRANSFER_GROUP_EXTRA_MIN} 分钟")
        if ex_i < 0 or ex_i > 360:
            raise ValueError("换乘组 default_extra_min 必须在 0-360 分钟之间")
        default_extra_min = ex_i
    hub_tc = (hub_telecode or "").strip().upper()
    if cm == "hub_star":
        if not hub_tc or hub_tc not in uniq_tcs:
            raise ValueError("枢纽星型(hub_star)模式必须指定组内的一个核心枢纽站(hub_telecode)")
    else:
        hub_tc = ""

    ph = ",".join("?" * len(uniq_tcs))
    known = {r["telecode"] for r in rows(f"SELECT telecode FROM stations WHERE telecode IN ({ph})", tuple(uniq_tcs))}
    missing = [tc for tc in uniq_tcs if tc not in known]
    if missing:
        raise ValueError(f"换乘组包含未知车站电报码：{', '.join(missing)}")
    now_str = cn_now().isoformat(timespec="seconds")
    en_val = 1 if enabled_by_default else 0
    with _write_lock, tx(bump=True) as conn:
        if group_id is not None:
            cur = conn.execute(
                """UPDATE transfer_groups
                   SET name=?, default_extra_min=?, note=?, enabled_by_default=?, connect_mode=?, hub_telecode=?
                   WHERE id=?""",
                (clean_name, default_extra_min, (note or "").strip(), en_val, cm, hub_tc, group_id),
            )
            if cur.rowcount == 0:
                raise ValueError(f"换乘组 #{group_id} 不存在")
            gid = group_id
            conn.execute("DELETE FROM transfer_group_members WHERE group_id=?", (gid,))
        else:
            existing = conn.execute("SELECT id FROM transfer_groups WHERE name=?", (clean_name,)).fetchone()
            if existing:
                gid = existing[0]
                conn.execute(
                    """UPDATE transfer_groups
                       SET default_extra_min=?, note=?, enabled_by_default=?, connect_mode=?, hub_telecode=?
                       WHERE id=?""",
                    (default_extra_min, (note or "").strip(), en_val, cm, hub_tc, gid),
                )
                conn.execute("DELETE FROM transfer_group_members WHERE group_id=?", (gid,))
            else:
                cur = conn.execute(
                    """INSERT INTO transfer_groups(name, default_extra_min, note, enabled_by_default, connect_mode, hub_telecode, created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (clean_name, default_extra_min, (note or "").strip(), en_val, cm, hub_tc, now_str),
                )
                gid = cur.lastrowid
        conn.executemany(
            "INSERT INTO transfer_group_members(group_id, telecode) VALUES(?,?)",
            [(gid, tc) for tc in uniq_tcs],
        )
    return next(g for g in list_transfer_groups() if g["id"] == gid)


def delete_transfer_group(group_id: int) -> bool:
    with _write_lock, tx(bump=True) as conn:
        cur = conn.execute("DELETE FROM transfer_groups WHERE id=?", (group_id,))
        return cur.rowcount > 0


def list_transfer_pair_overrides() -> list[dict]:
    return rows(
        """SELECT p.id, p.a_telecode, sa.name AS a_name, sa.city AS a_city,
                  p.b_telecode, sb.name AS b_name, sb.city AS b_city,
                  p.mode, p.custom_extra_min, p.note, p.created_at, p.updated_at
           FROM transfer_pair_overrides p
           LEFT JOIN stations sa ON sa.telecode = p.a_telecode
           LEFT JOIN stations sb ON sb.telecode = p.b_telecode
           ORDER BY p.id DESC"""
    )


def save_transfer_pair_override(tc1: str, tc2: str, mode: str,
                                custom_extra_min: int | None = None,
                                note: str = "") -> dict:
    t1 = (tc1 or "").strip().upper()
    t2 = (tc2 or "").strip().upper()
    if not t1 or not t2 or t1 == t2:
        raise ValueError("站对必须包含两个不同的有效车站")
    if mode not in ("allow", "block"):
        raise ValueError("站对规则类型必须为 allow 或 block")
    ex_min = None
    if mode == "allow" and custom_extra_min is not None:
        ex_i = int(custom_extra_min)
        if ex_i < 0 or ex_i > 360:
            raise ValueError("站对自定义加时必须在 0-360 分钟之间")
        ex_min = ex_i
    known = {r["telecode"] for r in rows("SELECT telecode FROM stations WHERE telecode IN (?,?)", (t1, t2))}
    if t1 not in known or t2 not in known:
        raise ValueError(f"存在未知车站电报码：{t1}, {t2}")
    a_tc, b_tc = (t1, t2) if t1 <= t2 else (t2, t1)
    now_str = cn_now().isoformat(timespec="seconds")
    with _write_lock, tx(bump=True) as conn:
        conn.execute(
            """INSERT INTO transfer_pair_overrides(a_telecode, b_telecode, mode, custom_extra_min, note, created_at, updated_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(a_telecode, b_telecode) DO UPDATE SET
                 mode=excluded.mode, custom_extra_min=excluded.custom_extra_min,
                 note=excluded.note, updated_at=excluded.updated_at""",
            (a_tc, b_tc, mode, ex_min, (note or "").strip(), now_str, now_str),
        )
    return next(
        p for p in list_transfer_pair_overrides()
        if p["a_telecode"] == a_tc and p["b_telecode"] == b_tc
    )


def delete_transfer_pair_override(pair_id: int) -> bool:
    with _write_lock, tx(bump=True) as conn:
        cur = conn.execute("DELETE FROM transfer_pair_overrides WHERE id=?", (pair_id,))
        return cur.rowcount > 0
