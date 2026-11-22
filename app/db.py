import json
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
_data_gen = 0


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
  city TEXT
);
CREATE INDEX IF NOT EXISTS idx_stations_name ON stations(name);

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
    with tx() as conn:
        conn.executescript(SCHEMA)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(station_trains)")}
        added = [c for c in ("start_start_time", "end_arrive_time", "running_time", "distance",
                             "train_style", "jiaolu_train_style") if c not in cols]
        for c in added:
            typ = "INTEGER" if c == "distance" else "TEXT"
            conn.execute(f"ALTER TABLE station_trains ADD COLUMN {c} {typ}")
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
    return r[0] if r else None


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
            s = resolve_station(tok)
            if not s:
                # 混合多选要求每个 token 均为有效物理站/电报码，避免漏掉拼写错误或误填的城市名
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
        missing = [t for t in _split_station_tokens(q_s) if not resolve_station(t)]
        if missing:
            return f"混合多选仅支持精确站名或电报码（不支持城市名），未识别：“{'、'.join(missing)}”"
    if city_mode == "string":
        for tok in _split_station_tokens(q_s):
            base, sep, pfx = tok.replace("：", ":").partition(":")
            base, pfx = base.strip(), pfx.strip()
            if sep and not base:
                return f"“{tok}”缺少城市名，请先输入城市再配合站名字样筛选"
            if sep and pfx:
                return f"城市“{base}”下未找到站名含“{pfx}”的车站"
    if city_mode == "single":
        return f"车站“{q_s}”不存在（仅该站模式不支持城市名或多站列表），请确认站名或电报码"
    return f"车站/城市“{q_s}”不存在，请先在「爬取数据」页更新车站站名表"


def get_setting(key: str, default=None):
    r = rows("SELECT value FROM settings WHERE key=?", (key,))
    return r[0]["value"] if r else default


def set_setting(key: str, value):
    with tx(bump=False) as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
