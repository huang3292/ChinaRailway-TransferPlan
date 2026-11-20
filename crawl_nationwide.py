#!/usr/bin/env python
"""全国全量爬取：车站站名表 → 全部车站大屏 → 全部车次时刻表。

用法:
    python crawl_nationwide.py [日期 YYYY-MM-DD]   # 缺省为今天

断点续爬：进度写入 data/nationwide_state.json，重启后自动跳过已完成项。
汇报节奏：速度不稳时每 10 分钟输出一次状态，稳定后每小时一次。
状态行输出到 stdout（供监控），错误明细写入 data/nationwide.log。
"""
import json
import sys
import time
from collections import deque

# 确保 stdout 输出 UTF-8，避免 Windows 管道下中文乱码
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass
from datetime import datetime, time as dtime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import db
from app.crawler import CrawlError, crawler

ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "nationwide_state.json"
LOG = ROOT / "data" / "nationwide.log"

MAINT_START = dtime(23, 30)
MAINT_END = dtime(5, 0)
CHECKPOINT_EVERY = 25


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str):
    line = f"[{ts()}] {msg}"
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def err(line: str):
    with LOG.open("a", encoding="utf-8") as f:
        f.write(f"[{ts()}] {line}\n")


def fmt_sec(sec: float) -> str:
    sec = max(0, int(sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def load_state(date: str) -> dict:
    st = {"date": None, "done_stations": [], "done_trains": [],
          "screen_err": 0, "tt_err": 0}
    if STATE.exists():
        try:
            st = json.loads(STATE.read_text(encoding="utf-8"))
        except Exception:
            pass
    if st.get("date") != date:
        st = {"date": date, "done_stations": [], "done_trains": [],
              "screen_err": 0, "tt_err": 0}
    return st


def save_state(st: dict):
    STATE.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")


def in_maintenance() -> bool:
    t = db.cn_now().time()
    return t >= MAINT_START or t < MAINT_END


def wait_out_maintenance():
    if not in_maintenance():
        return
    log("进入 12306 夜间维护时段（23:30–05:00），暂停并等待到 05:00 后继续…")
    while in_maintenance():
        time.sleep(60)
    log("维护结束，恢复爬取。")


class Tracker:
    def __init__(self, phase: str, total: int):
        self.phase = phase
        self.total = total
        self.done = 0
        self.errors = 0
        self.t0 = time.monotonic()
        self.prev_done = 0
        self.prev_t = self.t0
        self.next_report = self.t0 + 60  # 首报 1 分钟后，确认速度
        self.window_rates = deque(maxlen=3)

    def tick(self, ok: bool):
        self.done += 1
        if not ok:
            self.errors += 1

    def maybe_report(self, force: bool = False):
        now = time.monotonic()
        if not force and now < self.next_report:
            return
        elapsed = now - self.t0
        win = now - self.prev_t
        recent = (self.done - self.prev_done) / win if win > 0 else 0.0
        self.window_rates.append(recent)
        stable = len(self.window_rates) >= 2 and elapsed >= 1200 and (
            max(self.window_rates) <= min(self.window_rates) * 1.25
        )
        eta = (self.total - self.done) / recent if recent > 0 else float("inf")
        eta_str = fmt_sec(eta) if eta != float("inf") else "计算中"
        log(f"[{self.phase}] {self.done}/{self.total} 完成 {self.done / max(1, self.total) * 100:.1f}%"
            f" | 近期速度 {recent * 60:.1f}/分 | 累计 {fmt_sec(elapsed)} | 预计剩余 {eta_str}"
            f" | 失败 {self.errors}")
        self.prev_done = self.done
        self.prev_t = now
        self.next_report = now + (3600 if stable else 600)


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else db.cn_today().isoformat()
    if len(date) == 8 and date.isdigit():
        date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    db.init_db()
    crawler.set_rate("auto")
    st = load_state(date)

    log(f"==== 全国全量爬取开始，目标日期 {date} ====")

    try:
        r = crawler.crawl_stations()
        log(f"车站站名表已更新：解析 {r['parsed']} 站")
    except Exception as e:
        log(f"车站站名表更新失败（继续用库内已有数据）：{e}")

    stations = db.rows("SELECT telecode,name FROM stations ORDER BY telecode")
    log(f"库内车站总数 {len(stations)}")

    # ---- 阶段1：全部车站大屏 ----
    done_st = set(st["done_stations"])
    todo_st = [s for s in stations if s["telecode"] not in done_st]
    log(f"阶段1 车站大屏：待爬 {len(todo_st)} 站（已完成 {len(done_st)}）")
    tr = Tracker("screen", len(todo_st))
    for i, s in enumerate(todo_st, 1):
        wait_out_maintenance()
        ok = True
        try:
            crawler.crawl_station_trains(s["telecode"], date)
        except Exception as e:
            ok = False
            st["screen_err"] += 1
            if "无数据" not in str(e):
                err(f"大屏 {s['telecode']} {s['name']}: {e}")
        tr.tick(ok)
        st["done_stations"].append(s["telecode"])
        if i % CHECKPOINT_EVERY == 0:
            save_state(st)
        tr.maybe_report()
    save_state(st)
    tr.maybe_report(force=True)
    log(f"阶段1 完成：{tr.done} 站，失败 {tr.errors}")

    # ---- 阶段2：全部车次时刻表 ----
    wait_out_maintenance()
    rows = db.rows(
        """SELECT train_no, MAX(start_station_telecode) a, MAX(end_station_telecode) b,
                  MAX(start_train_date) sd, MAX(station_train_date) std, MAX(station_train_code) code
           FROM station_trains WHERE train_no IS NOT NULL AND query_date=?
           GROUP BY train_no""", (date,))
    log(f"阶段2 时刻表：共发现 {len(rows)} 个车次（日期 {date}）")
    done_tt = set(st["done_trains"])
    todo_tt = [r for r in rows if r["train_no"] not in done_tt]
    log(f"阶段2 待爬时刻表 {len(todo_tt)} 个（已完成 {len(done_tt)}）")
    tr = Tracker("timetable", len(todo_tt))
    for i, r in enumerate(todo_tt, 1):
        wait_out_maintenance()
        tn = r["train_no"]
        f, t, d = r["a"], r["b"], r["sd"] or r["std"] or date
        ok = True
        if not f or not t:
            ok = False
            st["tt_err"] += 1
            err(f"时刻表 {tn} {r['code']} 跳过：缺始发/终到电报码")
        else:
            try:
                crawler.crawl_timetable(tn, d, f, t)
            except Exception as e:
                ok = False
                st["tt_err"] += 1
                if "无" not in str(e) and "不存在" not in str(e):
                    err(f"时刻表 {tn} {r['code']}: {e}")
        tr.tick(ok)
        st["done_trains"].append(tn)
        if i % CHECKPOINT_EVERY == 0:
            save_state(st)
        tr.maybe_report()
    save_state(st)
    tr.maybe_report(force=True)
    log(f"阶段2 完成：{tr.done} 车次，失败 {tr.errors}")

    # ---- 汇总 ----
    log("==== 全国全量爬取结束 ====")
    log(f"车站 {len(stations)} | 车站大屏记录 {db.rows('SELECT COUNT(*) c FROM station_trains')[0]['c']}"
        f" | 时刻表经停记录 {db.rows('SELECT COUNT(*) c FROM train_stops')[0]['c']}"
        f" | 已有时刻表车次 {db.rows('SELECT COUNT(DISTINCT train_no) c FROM train_stops')[0]['c']}")
    log(f"本次失败：大屏 {st['screen_err']}，时刻表 {st['tt_err']}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("手动中断（进度已保存，重启可续爬）")
