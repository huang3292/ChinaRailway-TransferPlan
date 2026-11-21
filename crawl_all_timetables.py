#!/usr/bin/env python
"""全国全量车站经由车次与全车次时刻表离线爬取脚本。

流程：
  1. 同步官方站名表 station_name.js -> stations
  2. 遍历全国（或指定范围）车站抓取车站大屏 -> station_trains
  3. 从已入库大屏提取全部去重 train_no，逐趟抓取完整经停时刻表 -> train_stops

全部接口均走 app.crawler 全局限速与 raw_payloads 原始留档，支持断点续爬与增量跳过。

用法示例：
  python crawl_all_timetables.py                        # 默认爬取明天(UTC+8 +1天)全国全量
  python crawl_all_timetables.py --date 2026-11-22      # 指定日期
  python crawl_all_timetables.py --stations 北京,上海    # 仅爬取指定城市/车站相关车次
  python crawl_all_timetables.py --phase timetables     # 仅补齐库中尚缺时刻表的车次
  python crawl_all_timetables.py --all-db-trains        # 扫描 station_trains 全表历史全部车次
"""
import argparse
import json
import sys
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

# Windows 管道模式下 stdout 默认 GBK，必须提前切回 UTF-8 避免中文进度与车站名乱码
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import db
from app.crawler import CrawlCancelled, crawler, station_timetable_context

DEFAULT_STATE = ROOT / "data" / "crawl_all_timetables_state.json"
DEFAULT_LOG = ROOT / "data" / "crawl_all_timetables.log"
MAINT_START = dtime(23, 30)
MAINT_END = dtime(5, 0)
CHECKPOINT_EVERY = 20


def default_tomorrow() -> str:
    return (db.cn_today() + timedelta(days=1)).isoformat()


def normalize_date(s: str | None) -> str:
    if not s:
        return default_tomorrow()
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    datetime.strptime(s, "%Y-%m-%d")
    return s


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fmt_sec(sec: float) -> str:
    if sec == float("inf") or sec < 0:
        return "计算中"
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def in_maintenance() -> bool:
    t = db.cn_now().time()
    return t >= MAINT_START or t < MAINT_END


class Logger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def info(self, msg: str):
        line = f"[{ts()}] {msg}"
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def err(self, msg: str):
        line = f"[{ts()}] [ERROR] {msg}"
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


class ProgressTracker:
    def __init__(self, phase: str, total: int, logger: Logger, report_sec: float = 30.0):
        self.phase = phase
        self.total = total
        self.logger = logger
        self.report_sec = max(1.0, float(report_sec))
        self.done = 0
        self.ok_count = 0
        self.fail_count = 0
        self.t0 = time.monotonic()
        self.next_report = self.t0 + self.report_sec

    def tick(self, ok: bool):
        self.done += 1
        if ok:
            self.ok_count += 1
        else:
            self.fail_count += 1

    def maybe_report(self, force: bool = False):
        now = time.monotonic()
        if not force and now < self.next_report:
            return
        elapsed = max(0.001, now - self.t0)
        rate = self.done / elapsed
        eta = (self.total - self.done) / rate if rate > 0 and self.total > self.done else 0.0
        pct = self.done / max(1, self.total) * 100.0
        self.logger.info(
            f"[{self.phase}] {self.done}/{self.total} ({pct:.1f}%) "
            f"| 成功 {self.ok_count} 失败 {self.fail_count} "
            f"| 速度 {rate * 60:.1f}/分 | 已用 {fmt_sec(elapsed)} | 预计剩余 {fmt_sec(eta)}"
        )
        self.next_report = now + self.report_sec


def load_state(state_path: Path, date: str, reset: bool = False) -> dict:
    fresh = {
        "date": date,
        "done_stations": [],
        "failed_stations": [],
        "done_trains": [],
        "failed_trains": [],
    }
    if reset or not state_path.exists():
        return fresh
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
        if data.get("date") != date:
            return fresh
        for k in ("done_stations", "failed_stations", "done_trains", "failed_trains"):
            if not isinstance(data.get(k), list):
                data[k] = []
        return data
    except Exception:
        return fresh


def save_state(state_path: Path, state: dict):
    """原子替换落盘，防止 Ctrl+C 中断写到一半损坏断点文件。"""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(state_path)


def wait_out_maintenance(logger: Logger, ignore_maint: bool):
    if ignore_maint or not in_maintenance():
        return
    logger.info("进入 12306 夜间维护时段（23:30–05:00），暂停等待恢复…")
    while not ignore_maint and in_maintenance():
        time.sleep(60)
    logger.info("维护时段结束，继续爬取。")


def select_target_stations(station_spec: str, offset: int, limit: int) -> list[dict]:
    if station_spec.strip():
        out, seen = [], set()
        for raw_tok in station_spec.replace("，", ",").split(","):
            tok = raw_tok.strip()
            if not tok:
                continue
            matches = db.resolve_stations(tok, expand_city=True)
            for s in matches:
                if s["telecode"] not in seen:
                    seen.add(s["telecode"])
                    out.append(s)
        stations = out
    else:
        stations = db.rows("SELECT telecode, name, city FROM stations ORDER BY telecode")
    if offset > 0:
        stations = stations[offset:]
    if limit > 0:
        stations = stations[:limit]
    return stations


def collect_timetable_targets(
    date: str,
    target_telecodes: list[str] | None,
    all_db_trains: bool,
    force: bool,
) -> list[dict]:
    """从 station_trains 聚合并按车型排序所有待爬时刻表的去重 train_no。

    WHY: 12306 大屏返回里的 train_no 在不同日期可能不同，时刻表主表 train_stops 也是以
    train_no 为核心关联键；因此统一按 train_no 分组，并优先过滤掉库内已有完整时刻表的车次。
    """
    where = ["train_no IS NOT NULL", "train_no != ''"]
    params: list = []
    if not all_db_trains:
        where.append("(query_date=? OR station_train_date=?)")
        params.extend([date, date])
    if target_telecodes:
        ph = ",".join("?" * len(target_telecodes))
        where.append(f"station_telecode IN ({ph})")
        params.extend(target_telecodes)

    sql = f"""
        SELECT train_no,
               MAX(station_train_code) AS code,
               MAX(start_station_telecode) AS start_tc,
               MAX(end_station_telecode) AS end_tc,
               MAX(start_train_date) AS sd,
               MAX(station_train_date) AS std,
               MAX(query_date) AS qd
        FROM station_trains
        WHERE {' AND '.join(where)}
        GROUP BY train_no
    """
    rows = db.rows(sql, tuple(params))

    existing_tt: set[str] = set()
    if not force:
        existing_tt = {
            r["train_no"]
            for r in db.rows("SELECT DISTINCT train_no FROM train_stops WHERE train_no IS NOT NULL")
        }

    rank_map = {"G": 0, "C": 0, "D": 0, "Z": 1, "T": 1, "K": 1, "S": 3}
    items = [r for r in rows if r["train_no"] not in existing_tt]
    items.sort(key=lambda r: (
        rank_map.get(((r.get("code") or "")[:1]).upper(), 2),
        r.get("code") or "",
        r["train_no"],
    ))
    return items


def run_crawl(
    *,
    date: str | None = None,
    phase: str = "all",
    stations: str = "",
    offset: int = 0,
    limit: int = 0,
    refresh_stations: bool = True,
    rate_mode: str = "auto",
    interval: float | None = None,
    force: bool = False,
    retry_errors: bool = False,
    skip_db_screens: bool = False,
    all_db_trains: bool = False,
    ignore_maintenance: bool = False,
    report_sec: float = 30.0,
    state_path: Path = DEFAULT_STATE,
    log_path: Path = DEFAULT_LOG,
    reset_state: bool = False,
) -> dict:
    target_date = normalize_date(date)
    db.init_db()
    if interval is not None:
        crawler.set_rate("manual", interval)
    else:
        crawler.set_rate(rate_mode)

    logger = Logger(log_path)
    state = load_state(state_path, target_date, reset=reset_state)

    summary = {
        "date": target_date,
        "phase": phase,
        "stations_total": 0,
        "screens_crawled": 0,
        "screens_skipped": 0,
        "screens_failed": 0,
        "timetables_crawled": 0,
        "timetables_skipped": 0,
        "timetables_failed": 0,
        "interrupted": False,
    }

    logger.info(
        f"==== 全国车次与时刻表全量爬取启动 | 日期={target_date} | 阶段={phase} "
        f"| 强制重爬={force} | 全库车次={all_db_trains} ===="
    )

    try:
        # 1. 站名表同步
        if phase in ("all", "stations") and (refresh_stations or phase == "stations"):
            try:
                res = crawler.crawl_stations()
                logger.info(f"[stations] 官方站名表同步完成：解析 {res.get('parsed', 0)} 站，入库 {res.get('saved', 0)} 站")
            except Exception as e:
                logger.err(f"站名表更新失败：{e}")
                if phase == "stations":
                    raise
                logger.info("[stations] 更新失败，降级使用本地库内已有 stations 表继续")

        if phase == "stations":
            summary["stations_total"] = db.rows("SELECT COUNT(*) c FROM stations")[0]["c"]
            return summary

        target_stations = select_target_stations(stations, offset, limit)
        summary["stations_total"] = len(target_stations)
        if not target_stations:
            logger.info("未匹配到任何目标车站，结束。")
            return summary

        logger.info(f"目标车站数：{len(target_stations)}" + (f"（筛选：{stations}）" if stations else "（全国全量）"))

        # 2. 车站大屏（全部经由车次）
        if phase in ("all", "screens"):
            done_st = set() if force else set(state["done_stations"])
            failed_st = set() if (force or retry_errors) else set(state["failed_stations"])
            db_done_st: set[str] = set()
            if skip_db_screens and not force:
                db_done_st = {
                    r["station_telecode"]
                    for r in db.rows(
                        "SELECT DISTINCT station_telecode FROM station_trains WHERE query_date=?",
                        (target_date,),
                    )
                }

            todo_stations = []
            for s in target_stations:
                tc = s["telecode"]
                if tc in done_st or tc in failed_st or tc in db_done_st:
                    summary["screens_skipped"] += 1
                    continue
                todo_stations.append(s)

            logger.info(
                f"[screens] 待爬车站 {len(todo_stations)} 个（跳过已完成/已过滤 {summary['screens_skipped']} 个）"
            )
            tracker = ProgressTracker("screens", len(todo_stations), logger, report_sec=report_sec)
            done_set = set(state["done_stations"])
            fail_set = set(state["failed_stations"])

            try:
                for idx, s in enumerate(todo_stations, 1):
                    wait_out_maintenance(logger, ignore_maintenance)
                    tc, name = s["telecode"], s["name"]
                    ok = True
                    try:
                        crawler.crawl_station_trains(tc, target_date)
                        done_set.add(tc)
                        fail_set.discard(tc)
                        summary["screens_crawled"] += 1
                    except CrawlCancelled:
                        raise
                    except Exception as e:
                        err_txt = str(e)
                        # "无数据/暂无/不存在" 属于合法空站（小站当日无图定停靠），计入已完成避免反复重扫
                        if any(k in err_txt for k in ("无数据", "暂无", "不存在")):
                            done_set.add(tc)
                            fail_set.discard(tc)
                            summary["screens_crawled"] += 1
                        else:
                            ok = False
                            fail_set.add(tc)
                            summary["screens_failed"] += 1
                            logger.err(f"大屏 {name}({tc}) {target_date}: {err_txt}")
                    tracker.tick(ok)
                    if idx % CHECKPOINT_EVERY == 0:
                        state["done_stations"] = sorted(done_set)
                        state["failed_stations"] = sorted(fail_set)
                        save_state(state_path, state)
                    tracker.maybe_report()
            finally:
                state["done_stations"] = sorted(done_set)
                state["failed_stations"] = sorted(fail_set)
                save_state(state_path, state)
            if todo_stations:
                tracker.maybe_report(force=True)

        # 3. 去重车次完整时刻表
        if phase in ("all", "timetables"):
            wait_out_maintenance(logger, ignore_maintenance)
            filter_tcs = [s["telecode"] for s in target_stations] if stations.strip() else None
            targets = collect_timetable_targets(target_date, filter_tcs, all_db_trains, force)

            done_tt = set() if force else set(state["done_trains"])
            failed_tt = set() if (force or retry_errors) else set(state["failed_trains"])
            todo_trains = []
            for r in targets:
                tn = r["train_no"]
                if tn in done_tt or tn in failed_tt:
                    summary["timetables_skipped"] += 1
                    continue
                todo_trains.append(r)

            logger.info(
                f"[timetables] 库内待爬车次 {len(todo_trains)} 个（跳过已存在/已处理 {summary['timetables_skipped']} 个）"
            )
            tracker = ProgressTracker("timetables", len(todo_trains), logger, report_sec=report_sec)
            done_set = set(state["done_trains"])
            fail_set = set(state["failed_trains"])

            try:
                for idx, r in enumerate(todo_trains, 1):
                    wait_out_maintenance(logger, ignore_maintenance)
                    tn = r["train_no"]
                    code = r.get("code") or tn
                    f_tc = r.get("start_tc")
                    t_tc = r.get("end_tc")
                    d = r.get("sd") or r.get("std") or r.get("qd") or target_date
                    if not f_tc or not t_tc:
                        ctx = station_timetable_context(tn) or {}
                        f_tc = f_tc or ctx.get("from")
                        t_tc = t_tc or ctx.get("to")
                        d = ctx.get("date") or d

                    ok = True
                    if not f_tc or not t_tc:
                        ok = False
                        fail_set.add(tn)
                        summary["timetables_failed"] += 1
                        logger.err(f"时刻表 {code}({tn}) 跳过：缺少始发/终到电报码")
                    else:
                        try:
                            crawler.crawl_timetable(tn, d, f_tc, t_tc)
                            done_set.add(tn)
                            fail_set.discard(tn)
                            summary["timetables_crawled"] += 1
                        except CrawlCancelled:
                            raise
                        except Exception as e:
                            err_txt = str(e)
                            if any(k in err_txt for k in ("无数据", "暂无", "不存在")):
                                done_set.add(tn)
                                fail_set.discard(tn)
                                summary["timetables_crawled"] += 1
                            else:
                                ok = False
                                fail_set.add(tn)
                                summary["timetables_failed"] += 1
                                logger.err(f"时刻表 {code}({tn}) {d} {f_tc}->{t_tc}: {err_txt}")

                    tracker.tick(ok)
                    if idx % CHECKPOINT_EVERY == 0:
                        state["done_trains"] = sorted(done_set)
                        state["failed_trains"] = sorted(fail_set)
                        save_state(state_path, state)
                    tracker.maybe_report()
            finally:
                state["done_trains"] = sorted(done_set)
                state["failed_trains"] = sorted(fail_set)
                save_state(state_path, state)
            if todo_trains:
                tracker.maybe_report(force=True)

        st_cnt = db.rows("SELECT COUNT(*) c FROM stations")[0]["c"]
        scr_cnt = db.rows("SELECT COUNT(*) c FROM station_trains")[0]["c"]
        ts_cnt = db.rows("SELECT COUNT(*) c FROM train_stops")[0]["c"]
        ts_trains = db.rows("SELECT COUNT(DISTINCT train_no) c FROM train_stops")[0]["c"]
        logger.info(
            f"==== 爬取结束 | 库内车站={st_cnt} | 大屏记录={scr_cnt} | 时刻表停站={ts_cnt} | 时刻表车次数={ts_trains} ===="
        )
        return summary

    except (KeyboardInterrupt, CrawlCancelled):
        summary["interrupted"] = True
        save_state(state_path, state)
        logger.info("任务已手动中断（断点已保存，重启可自动续爬）")
        return summary
    finally:
        db.close_conn()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="爬取全国全部车站经由车次及所有去重车次时刻表至本地 SQLite")
    p.add_argument("--date", "-d", default=None, help="目标乘车日期 YYYY-MM-DD（缺省为北京时间明天）")
    p.add_argument("--phase", choices=("all", "stations", "screens", "timetables"), default="all",
                   help="执行阶段（默认 all：站名表 -> 车站大屏 -> 车次时刻表）")
    p.add_argument("--stations", default="", help="可选：逗号分隔车站/城市/电报码，只爬该范围（留空则全国全部车站）")
    p.add_argument("--offset", type=int, default=0, help="车站起始偏移量（分批爬取用）")
    p.add_argument("--limit", type=int, default=0, help="最多爬取车站数量（0 表示不限）")
    p.add_argument("--skip-stations-sync", action="store_true", help="跳过启动时联网更新 station_name.js")
    p.add_argument("--rate", choices=("auto", "manual"), default="auto", help="限速模式（默认 auto）")
    p.add_argument("--interval", type=float, default=None, help="指定固定请求间隔秒数（0.4-12.0，传入即启用手动限速）")
    p.add_argument("--force", action="store_true", help="忽略断点与库内已有时刻表，强制重新抓取")
    p.add_argument("--retry-errors", action="store_true", help="续爬时重试上次失败的车站与车次")
    p.add_argument("--skip-db-screens", action="store_true", help="若某站当日已有 station_trains 记录则跳过重抓大屏")
    p.add_argument("--all-db-trains", action="store_true", help="时刻表阶段扫描 station_trains 全部历史日期车次")
    p.add_argument("--ignore-maintenance", action="store_true", help="不因 23:30-05:00 夜间维护时段休眠")
    p.add_argument("--reset-state", action="store_true", help="清空断点状态文件后从头开始")
    p.add_argument("--report-sec", type=float, default=30.0, help="进度与 ETA 汇报间隔秒数（默认 30 秒）")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_crawl(
        date=args.date,
        phase=args.phase,
        stations=args.stations,
        offset=args.offset,
        limit=args.limit,
        refresh_stations=not args.skip_stations_sync,
        rate_mode=args.rate,
        interval=args.interval,
        force=args.force,
        retry_errors=args.retry_errors,
        skip_db_screens=args.skip_db_screens,
        all_db_trains=args.all_db_trains,
        ignore_maintenance=args.ignore_maintenance,
        report_sec=args.report_sec,
        reset_state=args.reset_state,
    )
    return 130 if summary.get("interrupted") else 0


if __name__ == "__main__":
    raise SystemExit(main())
