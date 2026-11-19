import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "rail.db"

_local = threading.local()

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
"""


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


@contextmanager
def tx():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    with tx() as conn:
        conn.executescript(SCHEMA)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(station_trains)")}
        added = [c for c in ("start_start_time", "end_arrive_time", "running_time", "distance") if c not in cols]
        for c in added:
            typ = "INTEGER" if c == "distance" else "TEXT"
            conn.execute(f"ALTER TABLE station_trains ADD COLUMN {c} {typ}")
    if added:
        _backfill_screen()


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
    from datetime import datetime

    with tx() as conn:
        cur = conn.execute(
            "INSERT INTO raw_payloads(api_type,params,source,fetched_at,payload) VALUES(?,?,?,?,?)",
            (api_type, json.dumps(params, ensure_ascii=False), source,
             datetime.now().isoformat(timespec="seconds"), payload),
        )
        return cur.lastrowid


def station_lookup() -> dict:
    return {r["name"]: r["telecode"] for r in rows("SELECT name,telecode FROM stations")}


def resolve_station(q: str) -> dict | None:
    q = q.strip()
    r = rows("SELECT * FROM stations WHERE telecode=? OR name=? OR name=? LIMIT 1",
             (q.upper(), q, q.removesuffix("站")))
    return r[0] if r else None