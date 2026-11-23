#!/usr/bin/env python
"""合规地理坐标与重轨站点批量抓取 CLI（Overpass 区域切片 + Wikidata SPARQL，支持断点续跑与试跑估算）。

严禁在批量管道中使用 Nominatim；单站手工补查仅通过 Web 页或 geo.manual_nominatim_lookup 低频调用。

用法:
    python crawl_geo_coords.py --trial-tiles 5      # 试跑前 5 个 Overpass 区域切片并估算全国配额
    python crawl_geo_coords.py --resume             # 断点续跑全量（默认行为）
    python crawl_geo_coords.py --reset --force      # 清空进度并全量重抓（保留 verified 人工锁定站）
    python crawl_geo_coords.py --audit-only         # 仅执行多维质检 + 重轨邻近距离重算
"""
import argparse
import sys
import time
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app import db, geo


def main():
    ap = argparse.ArgumentParser(description="全国火车站坐标与 43 城重轨站点合规批量抓取工具")
    ap.add_argument("--trial-tiles", type=int, default=None, help="仅试跑前 N 个 Overpass 切片并估算全量配额")
    ap.add_argument("--resume", action="store_true", help="从断点状态继续抓取（默认开启）")
    ap.add_argument("--reset", action="store_true", help="重置 data/geo_crawl_state.json 断点状态")
    ap.add_argument("--force", action="store_true", help="忽略已有 ok 坐标重新抓取（始终保留 verified）")
    ap.add_argument("--audit-only", action="store_true", help="仅运行质检与重轨邻近度重算，不联网")
    ap.add_argument("--delay", type=float, default=3.5, help="Overpass 切片间隔秒数（默认 3.5s）")
    ap.add_argument("--no-overpass", action="store_true", help="跳过 Overpass 区域切片")
    ap.add_argument("--no-wikidata", action="store_true", help="跳过 Wikidata SPARQL 补漏")
    args = ap.parse_args()

    db.init_db()
    if args.audit_only:
        t0 = time.monotonic()
        audit = geo.audit_station_coords()
        prox = geo.recompute_rail_proximity()
        cov = geo.geo_coverage_status()
        print(
            f"[质检完成] 用时 {time.monotonic() - t0:.1f}s | "
            f"有效 {cov['ok_coords']}/{cov['total']} ({cov['pct']}%) | "
            f"可疑 {cov['suspect']} | 缺失 {cov['missing']} | "
            f"重轨站点 {prox['stops']} (walk={prox['walk']}, short_bus={prox['short_bus']}, long={prox['long_bus_or_taxi']})",
            flush=True,
        )
        return

    crawler = geo.GeoCoordCrawler()
    t0 = time.monotonic()

    def on_prog(done: int, total: int, msg: str, _sd: int = 0, _st: int = 0):
        elapsed = time.monotonic() - t0
        eta = (elapsed / done * (total - done)) if done > 0 and total > done else 0.0
        eta_s = f" | ETA {eta:.0f}s" if eta > 0 else ""
        print(f"[{done}/{total}{eta_s}] {msg}", flush=True)

    def on_log(line: str):
        print(f"  {line}", flush=True)

    res = crawler.run(
        force=args.force,
        reset_checkpoint=args.reset,
        use_overpass=not args.no_overpass,
        use_wikidata=not args.no_wikidata,
        trial_tiles=args.trial_tiles,
        overpass_delay=max(1.0, args.delay),
        progress_cb=on_prog,
        log_cb=on_log,
    )
    cov = res["coverage"]
    print(
        f"\n[抓取结束] 总耗时 {time.monotonic() - t0:.1f}s | "
        f"Overpass +{res['matched_overpass']} 站, Wikidata +{res['matched_wikidata']} 站, 重轨 +{res['matched_rail_stops']} 站\n"
        f"  覆盖率：{cov['ok_coords']}/{cov['total']} ({cov['pct']}%) | 可疑 {cov['suspect']} | 缺失 {cov['missing']}\n"
        f"  Fallback 若关闭影响同城无坐标站对数：{cov['fallback_impact']['fallback_removed_pairs']}",
        flush=True,
    )
    if res["errors"]:
        print(f"  告警/失败项 ({len(res['errors'])}): " + " ; ".join(res["errors"][:5]), flush=True)


if __name__ == "__main__":
    main()
