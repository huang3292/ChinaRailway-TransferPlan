#!/usr/bin/env python
"""初始化全量爬取：官方站名表 → 全部车站大屏 → 全部车次时刻表。

用法:
    python init_crawl.py                 # 冷启动初始化（目标日期 = 北京时间明天）
    python init_crawl.py --dry-run       # 只做开局体检，不发任何请求、不改限速设置
    python init_crawl.py --date 2026-11-25
    python init_crawl.py --interval 1.5  # 固定请求间隔（默认沿用智能自适应限速）
    python init_crawl.py --retry-errors  # 把上次失败的车站/车次重新纳入本次范围
    python init_crawl.py --state data/init_1125.json   # 并行初始化各用一份断点

抓取逻辑全部复用 crawl_all_timetables.run_crawl()，本脚本不重复实现任何请求/入库代码，
只补初始化真正需要的三件事：开工前算清工作量与 ETA、结束后按断点记账核对有没有漏爬、
把 crawl_all_timetables.py 的十几个运维参数收敛成"一条命令跑完"的安全默认值。

断点文件默认 data/crawl_all_timetables_state.json（--state 可换路径）：Ctrl+C 或断电后
重跑同一条命令即自动续爬，日期一变旧断点作废。12306 每日 23:30–05:00 维护时段脚本自动休眠等待。
"""
import argparse
import sys
from pathlib import Path

# Windows 管道下 stdout 默认 GBK，中文进度会乱码，必须早于任何 print 切回 UTF-8
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import db
from app.crawler import MAX_CEIL, MIN_FLOOR, crawler
from crawl_all_timetables import (
    DEFAULT_STATE,
    collect_timetable_targets,
    fmt_sec,
    load_state,
    normalize_date,
    run_crawl,
    select_target_stations,
)


def preflight(date: str, retry_errors: bool = False,
              rate_mode: str = "auto", interval: float | None = None,
              state_path: Path = DEFAULT_STATE) -> dict:
    """开工体检：算清这次到底要发多少个请求、大概多久。"""
    stations = select_target_stations("", 0, 0)
    stations_total = len(stations)
    covered_in_db = {
        r["station_telecode"]
        for r in db.rows(
            "SELECT DISTINCT station_telecode FROM station_trains WHERE query_date=?",
            (date,),
        )
    }
    state = load_state(state_path, date)
    # 跳过口径与 run_crawl 保持一致：断点已完成 + 库内当日已有大屏的站不再抓；
    # failed_* 只有在没带 --retry-errors 时才会被跳过。
    skip = covered_in_db | set(state["done_stations"])
    if not retry_errors:
        skip |= set(state["failed_stations"])
    screens_todo = sum(1 for s in stations if s["telecode"] not in skip)

    # 时刻表待爬量以“库里已有的大屏”为准：冷启动时大屏还没爬，这里必然接近 0。
    # 再用同一函数扫全历史大屏车次，取初始化完成后的工作量上限（口径与 run_crawl 一致：
    # 库内已有时刻表的 train_no 会被排除），避免把 3 千请求误报成初始化的总工作量。
    tt_todo = len(collect_timetable_targets(date, None, all_db_trains=False, force=False))
    tt_done = db.rows("SELECT COUNT(DISTINCT train_no) c FROM train_stops")[0]["c"]
    tt_est = len(collect_timetable_targets(date, None, all_db_trains=True, force=False))
    tt_est = max(tt_est, tt_todo)

    # 估算用的间隔：--interval 只参与内存计算；dry-run 不能把它写进 settings 影响交互端
    rate = crawler.status()
    iv = interval if interval is not None else (rate["manual_interval"] or rate["interval"])
    iv = max(MIN_FLOOR, min(MAX_CEIL, iv))
    return {
        "date": date,
        "stations_total": stations_total,
        "stations_covered": len(covered_in_db),
        "screens_todo": screens_todo,
        "tt_todo": tt_todo,
        "tt_est": tt_est,
        "tt_done": tt_done,
        "requests": screens_todo + tt_todo,
        "requests_est": screens_todo + tt_est,
        "interval": iv,
        "rate_mode": "manual" if interval is not None else rate["mode"],
        "state_path": str(state_path),
    }


def print_preflight(p: dict):
    iv, req = p["interval"], p["requests"]
    print(f"目标乘车日期   : {p['date']}")
    if p["stations_total"] == 0:
        print("站名表车站数   : 0（本地空库，阶段0 会先联网同步官方 station_name.js）")
    else:
        print(f"站名表车站数   : {p['stations_total']}（其中当日已有大屏 {p['stations_covered']}）")
    print(f"阶段1 待爬大屏 : {p['screens_todo']} 站")
    # 全新空库连历史车次都没有，阶段2 的量确实无从推算，只能说明并给出复算办法
    known = p["tt_est"] > 0 or p["screens_todo"] == 0
    if known:
        print(f"阶段2 待爬时刻表: 按当前库内大屏为 {p['tt_todo']} 车次；"
              f"大屏爬完后预计约 {p['tt_est']} 车次（库内已有时刻表车次 {p['tt_done']}）")
    else:
        print("阶段2 待爬时刻表: 全新空库无法预估，阶段1 跑完后重跑 --dry-run 即可算出")
    est = p["requests_est"] if known else req
    print(f"预计请求数     : {req}" + (f"（初始化总工作量约 {est}）" if known else "（不含阶段2）"))
    if est:
        cur = f"按当前量 {fmt_sec(req * iv)}；" if req else ""
        print(f"预计耗时       : {cur}按总工作量 {fmt_sec(est * iv)}"
              f"（{p['rate_mode']}限速 {iv:.2f}s/请求，区间 {fmt_sec(est * MIN_FLOOR)}"
              f"–{fmt_sec(est * MAX_CEIL)}；触发风控会变慢，23:30–05:00 自动休眠）")
    print(f"断点文件       : {p['state_path']}")
    print("中断后重跑同一条命令即可续爬；本脚本不会清空或删除已爬数据。\n")


def postflight(date: str, state_path: Path = DEFAULT_STATE):
    """收尾核对：站名表是否同步成功、本次记账的失败项与仍缺的时刻表。"""
    stations = select_target_stations("", 0, 0)
    covered = {
        r["station_telecode"]
        for r in db.rows(
            "SELECT DISTINCT station_telecode FROM station_trains WHERE query_date=?",
            (date,),
        )
    }
    st = load_state(state_path, date)
    failed_st, failed_tt = len(st["failed_stations"]), len(st["failed_trains"])
    tt_left = len(collect_timetable_targets(date, None, all_db_trains=False, force=False))
    # WHY: 无大屏记录的站不等于失败——小站当日确实可能一趟图定车都不停（爬取时按成功计入断点）。
    # 因此完成与否只看断点记账的失败项和仍然缺少的时刻表，不能拿"记录数 < 车站数"当未完成。
    print(f"覆盖核对：车站大屏有记录 {len(covered)}/{len(stations)} 站，"
          f"无记录 {len(stations) - len(covered)} 站（多为当日无图定车次的小站，属正常）")
    print(f"断点记账：大屏失败 {failed_st} 站，时刻表失败 {failed_tt} 车次，"
          f"库内仍缺时刻表 {tt_left} 车次")
    return len(stations), failed_st, tt_left


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="一次性初始化：爬取全国所有车站大屏 + 所有车次时刻表到本地 SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--date", "-d", default=None, help="目标乘车日期 YYYY-MM-DD（缺省为北京时间明天）")
    ap.add_argument("--dry-run", action="store_true", help="只输出体检结果与 ETA，不联网")
    ap.add_argument("--rate", choices=("auto", "manual"), default="auto",
                    help="限速模式（默认 auto 自适应）")
    ap.add_argument("--interval", type=float, default=None,
                    help=f"固定请求间隔秒数（{MIN_FLOOR}-{MAX_CEIL}，传入即启用手动限速）")
    ap.add_argument("--retry-errors", action="store_true",
                    help="把上次失败的车站/车次重新纳入本次范围")
    ap.add_argument("--report-sec", type=float, default=60.0, help="进度与 ETA 汇报间隔秒数（默认 60）")
    ap.add_argument("--state", type=Path, default=DEFAULT_STATE,
                    help="断点文件路径（默认 data/crawl_all_timetables_state.json；"
                         "并行跑多份初始化时用不同文件）")
    args = ap.parse_args(argv)

    db.init_db()
    try:
        date = normalize_date(args.date)
    except ValueError:
        # 64 = 命令行用法错误（sysexits），与"什么都没爬"的 2 区分开
        print(f"日期格式不对：{args.date}（应为 YYYY-MM-DD 或 YYYYMMDD）", file=sys.stderr)
        return 64
    print("==== 初始化全量爬取 · 开局体检 ====")
    p = preflight(date, args.retry_errors, args.rate, args.interval, args.state)
    print_preflight(p)

    if args.dry_run:
        print("--dry-run：未发起网络请求，也未改动库内限速设置。")
        return 0

    # 限速在 run_crawl 内部落地（它会写 settings 供交互端共用），此处不重复设置
    summary = run_crawl(
        date=date,
        phase="all",
        rate_mode=args.rate,
        interval=args.interval,
        retry_errors=args.retry_errors,
        skip_db_screens=True,
        report_sec=args.report_sec,
        state_path=args.state,
    )
    if summary.get("interrupted"):
        print("\n初始化被中断，断点已保存：重跑 `python init_crawl.py` 从断点继续。")
        return 130

    print("==== 初始化收尾核对 ====")
    stations_total, failed_st, tt_left = postflight(date, args.state)
    # WHY: 冷启动时 stations 表要靠 run_crawl 的站名表阶段联网填充；若结束时仍为 0，
    # 说明 station_name.js 拉取失败（网络不通/接口变更），后面两个阶段全是空跑，
    # 不能报"初始化完成"骗过用户。
    if stations_total == 0:
        print("站名表仍为空：官方 station_name.js 同步失败，本次没有爬取任何车站。", file=sys.stderr)
        print("请确认能访问 kyfw.12306.cn，或先用前端「导入数据」页导入 samples/station_name.js 再重跑。",
              file=sys.stderr)
        return 2
    if failed_st or tt_left:
        print(f"未竟事项：大屏失败 {failed_st} 站、仍缺时刻表 {tt_left} 车次"
              "（缺的时刻表已含失败车次）。重跑 `python init_crawl.py --retry-errors` 补齐。")
    else:
        print("全部车站大屏与全部车次时刻表已覆盖，初始化完成。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
