import json
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import db, fare, importer, parsers, transfer


class TempDBTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_db_path = db.DB_PATH
        db.close_conn()
        db.DB_PATH = Path(self._tmp.name) / "test.db"
        db.init_db()

    def tearDown(self):
        db.close_conn()
        db.DB_PATH = self._orig_db_path
        self._tmp.cleanup()


class TestParsersAndSamples(TempDBTestCase):
    def test_station_name_and_city_proper_hubs(self):
        text = (SAMPLES / "station_name.js").read_text(encoding="utf-8")
        res = importer.ingest(text, "stations", source="test")
        self.assertGreater(res["saved"], 1000)

        bj = {s["name"] for s in db.city_stations("北京")}
        for expected in ("北京", "北京南", "北京西", "北京朝阳", "清河"):
            self.assertIn(expected, bj)

        sh = {s["name"] for s in db.city_stations("上海")}
        for expected in ("上海", "上海虹桥", "上海南", "上海松江", "练塘"):
            self.assertIn(expected, sh)

        wh = {s["name"] for s in db.city_stations("武汉")}
        for expected in ("武汉", "汉口", "武昌"):
            self.assertIn(expected, wh)

    def test_left_ticket_embedded_prices_and_new_seats(self):
        lt_text = (SAMPLES / "leftTicket_BJP_SHH.json").read_text(encoding="utf-8")
        res = importer.ingest(lt_text, "left_ticket", ctx={"date": "2026-11-20"}, source="test")
        self.assertEqual(res["api_type"], "left_ticket")
        self.assertGreater(res["saved"], 10)

        # G531 实票价核对 (vs queryTicketPrice_G531.json: swz=1744, zy=836, ze=498, wz=498)
        g531_prices = {
            r["seat_type"]: r["price"]
            for r in db.rows(
                "SELECT seat_type, price FROM prices WHERE train_no='240000G53106' AND train_date='2026-11-20'"
            )
        }
        self.assertEqual(g531_prices["swz"], 1744.0)
        self.assertEqual(g531_prices["zy"], 836.0)
        self.assertEqual(g531_prices["ze"], 498.0)
        self.assertEqual(g531_prices["wz"], 498.0)

        # G547 优选一等 (field[20] & yp_info_new 'D' on non-D train -> yxyz)
        g547_tk = db.rows("SELECT seats FROM tickets WHERE train_no='240000G54700'")[0]
        g547_seats = json.loads(g547_tk["seats"])
        self.assertEqual(g547_seats.get("yxyz"), "19")
        g547_prices = {
            r["seat_type"]: r["price"]
            for r in db.rows("SELECT seat_type, price FROM prices WHERE train_no='240000G54700'")
        }
        self.assertEqual(g547_prices.get("yxyz"), 1277.0)

        # D17 CR200J 一等卧/二等卧 (I -> ydw, J -> edw; 余票复用 rw/yw)
        d17_prices = {
            r["seat_type"]: r["price"]
            for r in db.rows("SELECT seat_type, price FROM prices WHERE train_no='2400000D170O'")
        }
        self.assertEqual(d17_prices.get("ydw"), 697.0)
        self.assertEqual(d17_prices.get("edw"), 516.0)

    def test_running_fig_allows(self):
        self.assertTrue(parsers.running_fig_allows("20260126#1#1#20261124", "2026-11-20"))
        self.assertFalse(parsers.running_fig_allows("20260126#1#1#20261124", "2026-11-25"))
        self.assertFalse(parsers.running_fig_allows("20260126#1#1#20261124", "2026-01-25"))
        self.assertIsNone(parsers.running_fig_allows(None, "2026-11-20"))
        self.assertIsNone(parsers.running_fig_allows("bad-fig", "2026-11-20"))

    def test_query_by_train_no_and_screen_samples(self):
        st_text = (SAMPLES / "station_name.js").read_text(encoding="utf-8")
        importer.ingest(st_text, "stations", source="test")
        tt_text = (SAMPLES / "queryByTrainNo_G531.json").read_text(encoding="utf-8")
        res = importer.ingest(tt_text, "train_stops", ctx={"train_no": "240000G53106", "date": "2026-11-20"}, source="test")
        self.assertEqual(res["saved"], 13)
        scr_text = (SAMPLES / "queryTrainByStation_BJP.json").read_text(encoding="utf-8")
        res2 = importer.ingest(scr_text, "station_screen", ctx={"station": "BJP", "date": "2026-11-20"}, source="test")
        self.assertGreater(res2["saved"], 20)


class TestTransferFixes(TempDBTestCase):
    def _seed_stations(self, items):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode, name, pinyin, short, city) VALUES(?,?,?,?,?)",
                items,
            )

    def test_price_map_date_isolation_and_fallback(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price) VALUES(?,?,?,?,?,?,?,?)",
                [
                    ("T1", "G1", "2026-11-19", "01", "05", "ze", "二等座", 100.0),
                    ("T1", "G1", "2026-11-20", "01", "05", "ze", "二等座", 120.0),
                    ("T2", "G2", "2026-11-18", "01", "03", "ze", "二等座", 80.0),
                    ("T2", "G2", "2026-11-19", "01", "03", "ze", "二等座", 95.0),
                ],
            )
        pm = transfer._price_map("2026-11-20")
        self.assertEqual(len(pm[("T1", "01", "05")]["items"]), 1)
        self.assertEqual(pm[("T1", "01", "05")]["items"][0]["price"], 120.0)
        self.assertEqual(pm[("T1", "01", "05")]["items"][0]["seat_type"], "ze")
        self.assertFalse(pm[("T1", "01", "05")]["stale"])

        # 无当日价时只取最新单日快照，不把 18/19 号堆叠在一起
        self.assertEqual(len(pm[("T2", "01", "03")]["items"]), 1)
        self.assertEqual(pm[("T2", "01", "03")]["items"][0]["price"], 95.0)
        self.assertEqual(pm[("T2", "01", "03")]["date"], "2026-11-19")
        self.assertTrue(pm[("T2", "01", "03")]["stale"])

    def test_screen_loopback_and_running_fig_rejection(self):
        self._seed_stations([
            ("AAA", "甲站", "jiazhan", "jz", "甲市"),
            ("BBB", "乙站", "yizhan", "yz", "乙市"),
            ("CCC", "丙站", "bingzhan", "bz", "丙市"),
        ])
        with db.tx() as conn:
            # 车次1：甲站(01) -> 乙站(02) 2026-11-19（将被平移到 11-20，running_fig 允许到 11-30）
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode,running_fig,start_start_time,end_arrive_time,running_time)
                   VALUES('AAA','甲站','2026-11-19','G10','TN10','2026-11-19','2026-11-19',1,'08:00','08:00',
                     '甲站','AAA','乙站','BBB','20261101#1#1#20261130','08:00','09:00','1小时0分')"""
            )
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode,running_fig,start_start_time,end_arrive_time,running_time)
                   VALUES('BBB','乙站','2026-11-20','G10','TN10','2026-11-20','2026-11-20',2,'09:00','09:00',
                     '甲站','AAA','乙站','BBB','20261101#1#1#20261130','08:00','09:00','1小时0分')"""
            )
            # 车次2：从乙站(01) 经过甲站(02) 到丙站(03) -> 第二程经过出发站甲站，应被折返剔除
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode,running_fig,start_start_time,end_arrive_time,running_time)
                   VALUES('BBB','乙站','2026-11-20','G20','TN20','2026-11-20','2026-11-20',1,'10:00','10:00',
                     '乙站','BBB','丙站','CCC','20261101#1#1#20261130','10:00','12:00','2小时0分')"""
            )
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode,running_fig,start_start_time,end_arrive_time,running_time)
                   VALUES('AAA','甲站','2026-11-20','G20','TN20','2026-11-20','2026-11-20',2,'10:50','10:55',
                     '乙站','BBB','丙站','CCC','20261101#1#1#20261130','10:00','12:00','2小时0分')"""
            )
            # 车次3：已超出 running_fig 有效期的车次 (止于 2026-11-19)，查询 2026-11-20 不应平移命中
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode,running_fig,start_start_time,end_arrive_time,running_time)
                   VALUES('AAA','甲站','2026-11-19','G30','TN30','2026-11-19','2026-11-19',1,'08:30','08:30',
                     '甲站','AAA','丙站','CCC','20261101#1#1#20261119','08:30','11:30','3小时0分')"""
            )

        res = transfer.search("甲站", "丙站", "2026-11-20", expand_city=False)
        direct_nos = [p["legs"][0]["train_no"] for p in res["direct"]]
        self.assertNotIn("TN30", direct_nos)
        # TN20 从甲站出发后续到丙站可直达，但 TN10(甲->乙)+TN20(乙->甲->丙) 的换乘经过甲站，必须被折返过滤剔除
        self.assertEqual(direct_nos, ["TN20"])
        self.assertEqual(len(res["transfers"]), 0)

    def test_timetable_multiple_city_stops_and_cross_extra(self):
        self._seed_stations([
            ("A01", "甲东", "jiadong", "jd", "甲市"),
            ("A02", "甲西", "jiaxi", "jx", "甲市"),
            ("M01", "中站", "zhongzhan", "zz", "中市"),
            ("M02", "中南", "zhongnan", "zn", "中市"),
            ("B01", "乙南", "yinan", "yn", "乙市"),
            ("B02", "乙北", "yibei", "yb", "乙市"),
        ])
        with db.tx() as conn:
            # 列车 A 同城停两站：先甲东再甲西，然后到中站
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("TA", "G101", "2026-11-20", 1, "甲东", "A01", "----", "08:00"),
                    ("TA", "G101", "2026-11-20", 2, "甲西", "A02", "08:18", "08:20"),
                    ("TA", "G101", "2026-11-20", 3, "中站", "M01", "09:20", "09:20"),
                    # 列车 B：中南(异站) 10:05 发 (距 09:20 仅 45 分钟，默认 cross_extra=60 + min_wait=30 = 90 分钟会拒；调 cross_extra=10 可命中)
                    # 且在终点城市先停乙南、再停乙北
                    ("TB", "G202", "2026-11-20", 1, "中南", "M02", "----", "10:05"),
                    ("TB", "G202", "2026-11-20", 2, "乙南", "B01", "11:00", "11:05"),
                    ("TB", "G202", "2026-11-20", 3, "乙北", "B02", "11:20", "11:20"),
                ],
            )

        with db.tx() as conn:
            conn.execute("UPDATE stations SET lat=31.20, lon=121.40, coord_status='verified' WHERE telecode='M01'")
            conn.execute("UPDATE stations SET lat=31.28, lon=121.40, coord_status='verified' WHERE telecode='M02'")

        res_default = transfer.search("甲市", "乙市", "2026-11-20", min_wait=30, max_wait=360, expand_city=True, cross_time_mode="fixed")
        # 默认 cross_extra=60，45 分钟异站换乘不满足 30+60=90
        self.assertEqual(len(res_default["transfers"]), 0)

        res_relaxed = transfer.search("甲市", "乙市", "2026-11-20", min_wait=30, max_wait=360, expand_city=True, cross_extra=10, cross_time_mode="fixed")
        # 应能枚举出 甲东/甲西 × 乙南/乙北（不止 [0]）
        combos = {(p["legs"][0]["from"], p["legs"][1]["to"]) for p in res_relaxed["transfers"]}
        self.assertIn(("甲西", "乙北"), combos)
        self.assertIn(("甲东", "乙南"), combos)

    def test_multi_leg_overnight_first_leg_and_cache_clear(self):
        self._seed_stations([
            ("S01", "始站", "shizhan", "sz", "始市"),
            ("S02", "一转", "yizhuan", "yz", "一市"),
            ("S03", "二转", "erzhuan", "ez", "二市"),
            ("S04", "终站", "zhongzhan", "zz", "终市"),
        ])
        with db.tx() as conn:
            # S1 跨天运行：Day0 18:00 从前站开 -> Day1 01:00 到始站(用户从始站上车!) -> Day1 02:30 到一转
            # 注意 st["dep"] >= 1440, n0 = -1; 第二程当日 03:10 从一转发（如果 S2 首站前日 23:00 始发 -> n 可能是负数！）
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("M1", "K1", "2026-11-20", 1, "始站", "S01", "----", "08:00"),
                    ("M1", "K1", "2026-11-20", 2, "一转", "S02", "09:00", "09:05"),
                    # M2 前一站昨天 23:00 发 -> 今天 09:45 到一转 (dep_min = 1440+585=2025), 用 math.ceil 需要 n=-1 才能接当天 09:45！
                    ("M2", "K2", "2026-11-20", 1, "一转前", "S00", "----", "23:00"),
                    ("M2", "K2", "2026-11-20", 2, "一转", "S02", "09:40", "09:45"),
                    ("M2", "K2", "2026-11-20", 3, "二转", "S03", "11:00", "11:05"),
                    ("M3", "K3", "2026-11-20", 1, "二转", "S03", "----", "11:45"),
                    ("M3", "K3", "2026-11-20", 2, "终站", "S04", "13:00", "13:00"),
                ],
            )
            conn.execute("INSERT INTO stations(telecode,name,pinyin,short,city) VALUES('S00','一转前','y','y','')")

        res = transfer.search("始站", "终站", "2026-11-20", min_wait=30, max_wait=180, max_legs=3, expand_city=False)
        three_leg = [p for p in res["transfers"] if len(p["legs"]) == 3]
        self.assertTrue(three_leg, "应支持 n<0 的跨天次日时刻表衔接 3 程方案")
        self.assertEqual(three_leg[0]["legs"][1]["depart"], "2026-11-20 09:45")
        self.assertEqual(three_leg[0]["transfers"][0]["wait_min"], 45)

        # 验证过期缓存清理
        with db.tx() as conn:
            conn.execute(
                "INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,seats) VALUES('M1','K1','2026-11-15','S01','S02','{}')"
            )
        deleted = db.clear_cache(before_date="2026-11-18", include_raw=False)
        self.assertEqual(deleted["tickets"], 1)
        self.assertEqual(deleted["train_stops"], 0)

    def test_pruning_hard_rules_and_multi_level_custom(self):
        self._seed_stations([
            ("A01", "甲东", "jd", "jd", "甲市"),
            ("A02", "甲西", "jx", "jx", "甲市"),
            ("B01", "乙站", "yz", "yz", "乙市"),
            ("B02", "乙南", "yn", "yn", "乙市"),
            ("C01", "丙站", "bz", "bz", "丙市"),
            ("D01", "丁站", "dz", "dz", "丁市"),
            ("D02", "丁东", "dd", "dd", "丁市"),
        ])
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # G1: 甲东(08:00) -> 乙站(09:00) -> 丁站(11:30)（可直达丁站，用于检验 detour_ratio 与过终点剪枝）
                    ("T_G1", "G1", "2026-11-20", 1, "甲东", "A01", "----", "08:00"),
                    ("T_G1", "G1", "2026-11-20", 2, "乙站", "B01", "09:00", "09:05"),
                    ("T_G1", "G1", "2026-11-20", 3, "丁站", "D01", "11:30", "11:30"),
                    # G2: 乙站(09:40) -> 丙站(10:30)
                    ("T_G2", "G2", "2026-11-20", 1, "乙站", "B01", "----", "09:40"),
                    ("T_G2", "G2", "2026-11-20", 2, "丙站", "C01", "10:30", "10:30"),
                    # T_G1_DUP 与 G1 同车次号（模拟第3程再坐回同名车次 G1，即使 train_no 不同也应按 repeat_train 剪掉）
                    ("T_G1_DUP", "G1", "2026-11-20", 1, "丙站", "C01", "----", "11:05"),
                    ("T_G1_DUP", "G1", "2026-11-20", 2, "丁东", "D02", "12:00", "12:00"),
                    # G3: 丙站(11:10) -> 丁东(12:10)（合法3程：G1 -> G2 -> G3）
                    ("T_G3", "G3", "2026-11-20", 1, "丙站", "C01", "----", "11:10"),
                    ("T_G3", "G3", "2026-11-20", 2, "丁东", "D02", "12:10", "12:10"),
                    # G4: 乙站(09:50) -> 甲西(10:40) -> 丙站(11:30)（经过已离城市甲市，应触发 city_loop 剪枝）
                    ("T_G4", "G4", "2026-11-20", 1, "乙站", "B01", "----", "09:50"),
                    ("T_G4", "G4", "2026-11-20", 2, "甲西", "A02", "10:35", "10:40"),
                    ("T_G4", "G4", "2026-11-20", 3, "丙站", "C01", "11:30", "11:30"),
                    # G5: 丁站(12:10) -> 丁东(12:30)（若前程已到终点丁站，不得继续向丁东换乘绕行）
                    ("T_G5", "G5", "2026-11-20", 1, "丁站", "D01", "----", "12:10"),
                    ("T_G5", "G5", "2026-11-20", 2, "丁东", "D02", "12:30", "12:30"),
                    # K9: 慢车 2 程方案 甲东(08:00)->乙南(10:00) + K10 乙南(11:00)->丁站(17:00)（总耗时 540 分钟 > G1 直达 210 分钟的 2.0 倍）
                    ("T_K9", "K9", "2026-11-20", 1, "甲东", "A01", "----", "08:00"),
                    ("T_K9", "K9", "2026-11-20", 2, "乙南", "B02", "10:00", "10:00"),
                    ("T_K10", "K10", "2026-11-20", 1, "乙南", "B02", "----", "11:00"),
                    ("T_K10", "K10", "2026-11-20", 2, "丁站", "D01", "17:00", "17:00"),
                    # D8: 甲东(08:10) -> 乙站(09:10) + D9 乙站(09:45) -> 丁站(12:30)（被 G1+G2/直达等对比，且比另一同站早到方案慢时用于检验支配剪枝）
                    ("T_D8", "D8", "2026-11-20", 1, "甲东", "A01", "----", "08:10"),
                    ("T_D8", "D8", "2026-11-20", 2, "乙站", "B01", "09:10", "09:10"),
                    ("T_D9", "D9", "2026-11-20", 1, "乙站", "B01", "----", "09:45"),
                    ("T_D9", "D9", "2026-11-20", 2, "丁站", "D01", "12:30", "12:30"),
                    # 另一更优同站换乘：甲东(08:20) -> 乙站(09:10) + D9(09:45 -> 12:30) 晚发同到，支配 D8+D9
                    ("T_D7", "D7", "2026-11-20", 1, "甲东", "A01", "----", "08:20"),
                    ("T_D7", "D7", "2026-11-20", 2, "乙站", "B01", "09:10", "09:10"),
                ],
            )

        # 1) 均衡模式下：校验 3 程硬剪枝（禁止同车次 G1 重复、禁止经过甲市绕回、禁止过终点丁站再转）
        res_bal = transfer.search("甲市", "丁市", "2026-11-20", min_wait=30, max_wait=120, max_legs=3, prune_level="balanced")
        self.assertGreater(res_bal["prune_stats"]["repeat_train"], 0)
        self.assertGreater(res_bal["prune_stats"]["city_loop"], 0)
        self.assertGreater(res_bal["prune_stats"]["passed_dest"], 0)
        for p in res_bal["transfers"]:
            codes = [l["train"] for l in p["legs"]]
            self.assertEqual(len(codes), len(set(codes)), "同一方案内不得出现重复车次")
            self.assertNotIn("G4", codes, "不得出现绕回甲市的方案")
            self.assertNotIn("G5", codes, "不得出现已到丁站再转丁东的方案")
        self.assertTrue(any([l["train"] for l in p["legs"]] == ["G1", "G2", "G3"] for p in res_bal["transfers"]))

        # 2) 严格级别：detour_ratio=2.0 应剪除 540 分钟的 K9+K10，prune_dominated=True 应剪除被 D7+D9 支配的 D8+D9
        res_strict = transfer.search("甲市", "丁市", "2026-11-20", min_wait=30, max_wait=120, max_legs=3, prune_level="strict")
        self.assertEqual(res_strict["prune_config"]["frontier_k"], 4)
        self.assertGreater(res_strict["prune_stats"]["dominated"], 0)
        strict_chains = {[l["train"] for l in p["legs"]].__str__() for p in res_strict["transfers"]}
        self.assertNotIn("['K9', 'K10']", strict_chains)
        self.assertNotIn("['D8', 'D9']", strict_chains)
        self.assertIn("['D7', 'D9']", strict_chains)

        # 3) 自定义剪枝组合：排除丙站中转 + 剪除 K 字头 + 禁止同城异站换乘
        res_custom = transfer.search(
            "甲市", "丁市", "2026-11-20", min_wait=30, max_wait=120, max_legs=3,
            prune_level="custom", exclude_stations="丙站", exclude_prefixes="K", allow_cross_station=False,
        )
        for p in res_custom["transfers"]:
            self.assertTrue(all(tr["same"] for tr in p["transfers"]))
            for l in p["legs"]:
                self.assertFalse(l["train"].startswith("K"))
            for tr in p["transfers"]:
                self.assertNotIn("丙", tr["station"])

        # 4) 必经中转站 + 只含车型头（仅保留含丙站且只乘 G 字头的方案）
        res_inc = transfer.search(
            "甲市", "丁市", "2026-11-20", min_wait=30, max_wait=120, max_legs=3,
            prune_level="custom", allow_prefixes="G", include_stations="丙站",
        )
        self.assertTrue(res_inc["transfers"])
        for p in res_inc["transfers"]:
            self.assertTrue(all(l["train"].startswith("G") for l in p["legs"]))
            self.assertTrue(any("丙" in tr["station"] for tr in p["transfers"]))



class TestCrawlerAndMainStubbed(TempDBTestCase):
    @classmethod
    def setUpClass(cls):
        # app.crawler 与 app.main 已内置无三方库兼容层，无需注入临时桩模块
        pass

    def test_crawler_cancel_and_maint_hint(self):
        from app import crawler as crawler_mod

        c = crawler_mod.Crawler()
        ev = threading.Event()
        ev.set()
        c.set_cancel_event(ev)
        with self.assertRaises(crawler_mod.CrawlCancelled):
            c._sleep(5.0)
        c.set_cancel_event(None)

        # 业务提示（如“预售”）不应触发请求间隔翻倍
        c._interval = 1.0
        with mock.patch.object(c, "ensure_init"), mock.patch.object(
            c, "_json", return_value={"status": False, "messages": ["不在预售期内"]}
        ):
            with self.assertRaises(crawler_mod.CrawlError):
                c.crawl_tickets("BJP", "SHH", "2026-12-31")
        self.assertEqual(c.status()["interval"], 1.0)

    def test_main_job_eviction_cancel_and_plan_tickets_cache(self):
        from app import main as main_mod

        main_mod.jobs.clear()
        main_mod._job_cancels.clear()
        for i in range(main_mod.MAX_JOBS + 5):
            jid, _ = main_mod._create_job({"status": "done", "n": i})
        self.assertLessEqual(len(main_mod.jobs), main_mod.MAX_JOBS)

        jid, ev = main_mod._create_job({"status": "running", "done": 0, "total": 1})
        res = main_mod.cancel_job(jid)
        self.assertEqual(res["status"], "cancelled")
        self.assertTrue(ev.is_set())

        # 校验 _plan_tickets_job 使用北京时间今日并按 train_date 命中票价缓存
        future_date = "2099-01-01"
        with db.tx() as conn:
            conn.execute(
                "INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,from_station_no,to_station_no,seat_types,seats,can_web_buy) "
                "VALUES('T9','G9',?,'BJP','SHH','01','05','OM','{\"ze\":\"有\"}','Y')",
                (future_date,),
            )
            conn.execute(
                "INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price) "
                "VALUES('T9','G9',?,'01','05','ze','二等座',555.0)",
                (future_date,),
            )
            # 仅存在精确 future_date 的票价缓存时才会跳过实时抓价
        jid2, ev2 = main_mod._create_job({"status": "running", "done": 0, "total": 0, "errors": [], "result": {}})
        with mock.patch.object(main_mod.crawler, "crawl_tickets") as m_tk, mock.patch.object(
            main_mod.crawler, "crawl_price"
        ) as m_pr:
            main_mod._plan_tickets_job(
                jid2,
                ev2,
                [{"key": f"T9|BJP|SHH|{future_date}", "train_no": "T9", "from": "BJP", "to": "SHH", "date": future_date}],
            )
            m_tk.assert_called_once_with("BJP", "SHH", future_date)
            m_pr.assert_not_called()

        status = main_mod.job_status(jid2)
        self.assertEqual(status["status"], "done")
        leg_res = status["result"][f"T9|BJP|SHH|{future_date}"]
        self.assertEqual(leg_res["prices"][0]["seat_type"], "ze")
        self.assertEqual(leg_res["price_date"], future_date)
        self.assertNotIn("price_stale", leg_res)

    def test_unified_timetable_and_route_job_labels(self):
        from app import main as main_mod

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [("BJP", "北京", "bj", "bj", "北京"), ("SHH", "上海", "sh", "sh", "上海")],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("5u000D645402", "D6454", "2026-11-20", 1, "北京", None, "----", "08:00"),
                    ("5u000D645402", "D6454", "2026-11-20", 2, "上海", "SHH", "14:00", "14:00"),
                ],
            )
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,start_station_name,start_station_telecode,
                     end_station_name,end_station_telecode)
                   VALUES('BJP','北京','2026-11-20','D6454','5u000D645402','2026-11-20','2026-11-20',1,'北京','BJP','上海','SHH')"""
            )

        # 时刻表接口同时返回代号与车次号，并自动兜底补齐站点电报码
        tt = main_mod.timetable("5u000D645402")
        self.assertEqual(tt["train_no"], "5u000D645402")
        self.assertEqual(tt["station_train_code"], "D6454")
        self.assertEqual(tt["stops"][0]["telecode"], "BJP")
        self.assertEqual(tt["stops"][1]["telecode"], "SHH")

        # 搜索返回值包含带电报码的车站标签
        sr = transfer.search("北京", "上海", "2026-11-20", expand_city=False)
        self.assertEqual(sr["from_station_labels"], ["北京(BJP)"])
        self.assertEqual(sr["to_station_labels"], ["上海(SHH)"])

        # 一键导入任务日志同时记录 站名(电报码) 与 车次(代号)
        jid, ev = main_mod._create_job({"status": "running", "done": 0, "total": 0, "phase": "init", "current": "", "log": [], "errors": [], "result": {}})
        with mock.patch.object(main_mod.crawler, "crawl_station_trains"), \
             mock.patch.object(main_mod.crawler, "crawl_timetable"), \
             mock.patch.object(main_mod.crawler, "crawl_tickets"):
            main_mod._route_job(jid, ev, "北京", "上海", "2026-11-20", True)
        st = main_mod.job_status(jid)
        self.assertEqual(st["status"], "done")
        self.assertIn("车站大屏 北京(BJP)", st["log"])
        self.assertIn("时刻表 D6454(5u000D645402)", st["log"])
        self.assertIn("余票 北京(BJP)→上海(SHH)", st["log"])

    def test_screen_cross_night_bridging_and_same_city_multi_stop(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("JNA", "甲北", "jiabei", "jb", "甲市"),
                    ("JNB", "甲南", "jianan", "jn", "甲市"),
                    ("MID", "中转站", "zhongzhuan", "zz", "中市"),
                    ("DST", "终点站", "zhongdian", "zd", "乙市"),
                ],
            )
            # 跨夜直达车 Z10：11-20 22:00 甲北始发 -> 11-21 06:00 到终点站；
            # 模拟“两端均只抓 11-20 当天大屏”，终点站抓到的是 11-19 始发班次的 11-20 06:00 终到记录
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode)
                   VALUES('JNA','甲北','2026-11-20','Z10','TN_Z10','2026-11-20','2026-11-20',1,'----','22:00',
                     '甲北','JNA','终点站','DST')"""
            )
            conn.execute(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode)
                   VALUES('DST','终点站','2026-11-20','Z10','TN_Z10','2026-11-20','2026-11-19',2,'06:00','06:00',
                     '甲北','JNA','终点站','DST')"""
            )
            # 同城双站停靠车 G50：同在 11-20 先停甲北(08:00) 再停甲南(08:30) 到中转站(10:00)
            # 在中转站接 G60(10:40->12:00) 到终点站，验证不会因遇到甲北后提前 break 而漏掉甲南上车方案
            conn.executemany(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,start_station_name,
                     start_station_telecode,end_station_name,end_station_telecode)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    ("JNA", "甲北", "2026-11-20", "G50", "TN_G50", "2026-11-20", "2026-11-20", 1, "----", "08:00", "甲北", "JNA", "中转站", "MID"),
                    ("JNB", "甲南", "2026-11-20", "G50", "TN_G50", "2026-11-20", "2026-11-20", 2, "08:25", "08:30", "甲北", "JNA", "中转站", "MID"),
                    ("MID", "中转站", "2026-11-20", "G50", "TN_G50", "2026-11-20", "2026-11-20", 3, "10:00", "10:00", "甲北", "JNA", "中转站", "MID"),
                    ("MID", "中转站", "2026-11-20", "G60", "TN_G60", "2026-11-20", "2026-11-20", 1, "----", "10:40", "中转站", "MID", "终点站", "DST"),
                    ("DST", "终点站", "2026-11-20", "G60", "TN_G60", "2026-11-20", "2026-11-20", 2, "12:00", "12:00", "中转站", "MID", "终点站", "DST"),
                ],
            )

        sr = transfer.search("甲市", "终点站", "2026-11-20", expand_city=True)
        # 校验跨夜直达已成功桥接，且标为推算方案
        z10 = [p for p in sr["direct"] if p["legs"][0]["train_no"] == "TN_Z10"]
        self.assertEqual(len(z10), 1)
        self.assertEqual(z10[0]["legs"][0]["depart"], "2026-11-20 22:00")
        self.assertEqual(z10[0]["legs"][0]["arrive"], "2026-11-21 06:00")
        self.assertTrue(z10[0]["date_assumed"])

        # 校验同城多停靠站均被枚举，且大屏换乘方案补齐 source_date
        g50_froms = {p["legs"][0]["from"] for p in sr["transfers"] if p["legs"][0]["train_no"] == "TN_G50"}
        self.assertEqual(g50_froms, {"甲北", "甲南"})
        for p in sr["transfers"]:
            self.assertTrue(p.get("source_date"))

    def test_fare_load_calibration_avoids_n_plus_one(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,station_train_date,station_no,distance) VALUES(?,?,?,?,?,?,?,?)",
                [
                    ("BJP", "北京", "2026-11-20", "G1", "TN1", "2026-11-20", 1, "0"),
                    ("SHH", "上海", "2026-11-20", "G1", "TN1", "2026-11-20", 2, "1000"),
                ],
            )
            conn.executemany(
                "INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price) VALUES(?,?,?,?,?,?,?,?)",
                [
                    ("TN1", "", "2026-11-20", "01", "02", "ze", "二等座", 500.0),
                    ("TN1", "", "2026-11-20", "01", "02", "zy", "一等座", 800.0),
                ],
            )
        with mock.patch.object(db, "rows", wraps=db.rows) as spy_rows:
            cal = fare.load_calibration()
        # 无论 prices 有多少行，固定只发 2 条 SQL（station_trains + prices）
        self.assertEqual(spy_rows.call_count, 2)
        self.assertEqual(cal[("G", "ze")], [0.5])
        self.assertEqual(cal[("G", "zy")], [0.8])

    def test_crawler_reset_session_and_non_json_reinit(self):
        from app import crawler as crawler_mod

        c = crawler_mod.Crawler()
        old_sess = c.session
        c._inited = True
        c._left_ticket_url = "leftTicket/queryZ"
        c.reset_session()
        self.assertIsNot(c.session, old_sess)
        self.assertFalse(c._inited)
        self.assertIsNone(c._left_ticket_url)

        # 模拟第一次请求返回非 JSON 触发自动重置 _inited，第二次重初始化并按 c_url 重定向后成功
        bad_resp = types.SimpleNamespace(json=mock.Mock(side_effect=ValueError("html")))
        init_resp = types.SimpleNamespace(text="var CLeftTicketUrl = 'leftTicket/queryA';")
        c_url_resp = types.SimpleNamespace(
            json=mock.Mock(return_value={"status": False, "c_url": "leftTicket/queryB"})
        )
        ok_resp = types.SimpleNamespace(json=mock.Mock(return_value={"data": {"result": []}}))

        c._inited = True
        c._left_ticket_url = "leftTicket/queryOld"
        with mock.patch.object(c, "_request", side_effect=[bad_resp, init_resp, c_url_resp, ok_resp]):
            res = c.crawl_tickets("BJP", "SHH", "2026-11-20")
        self.assertEqual(res["saved"], 0)
        self.assertEqual(c._left_ticket_url, "leftTicket/queryB")
        self.assertTrue(c._inited)

    def test_route_job_class_priority_truncation_date_validation_and_import_file(self):
        import asyncio
        from app import main as main_mod

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [("BJP", "北京", "bj", "bj", "北京"), ("SHH", "上海", "sh", "sh", "上海")],
            )
            # 插入 210 趟混编车次：含 S 市郊、纯数字普客、K/Z 普速、G/D/C 高铁动车
            rows = []
            for i in range(100):
                rows.append(("BJP", "北京", "2026-11-20", f"S{100+i}", f"TN_S_{i:03d}", "2026-11-20", "2026-11-20", 1, "BJP", "SHH"))
            for i in range(60):
                rows.append(("BJP", "北京", "2026-11-20", f"K{100+i}", f"TN_K_{i:03d}", "2026-11-20", "2026-11-20", 1, "BJP", "SHH"))
            for i in range(50):
                rows.append(("BJP", "北京", "2026-11-20", f"G{100+i}", f"TN_G_{i:03d}", "2026-11-20", "2026-11-20", 1, "BJP", "SHH"))
            conn.executemany(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,start_station_telecode,end_station_telecode)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )

        crawled = []
        jid, ev = main_mod._create_job({"status": "running", "done": 0, "total": 0, "phase": "init", "current": "", "truncated": 0, "log": [], "errors": [], "result": {}})
        with mock.patch.object(main_mod.crawler, "crawl_station_trains"), \
             mock.patch.object(main_mod.crawler, "crawl_timetable", side_effect=lambda tn, d, f, t: crawled.append(tn)), \
             mock.patch.object(main_mod.crawler, "crawl_tickets"):
            main_mod._route_job(jid, ev, "北京", "上海", "2026-11-20", True)

        st = main_mod.job_status(jid)
        self.assertEqual(st["status"], "done")
        self.assertEqual(len(crawled), 200)
        self.assertEqual(st["truncated"], 10)
        # 50 趟 G 与 60 趟 K 必须全部命中，S 字头只补齐剩余 90 趟，最后 10 趟 S 被截断跳过
        self.assertEqual(sum(1 for x in crawled if x.startswith("TN_G_")), 50)
        self.assertEqual(sum(1 for x in crawled if x.startswith("TN_K_")), 60)
        self.assertEqual(sum(1 for x in crawled if x.startswith("TN_S_")), 90)
        self.assertTrue(any("截断提示" in line and "10 趟" in line for line in st["log"]))

        # 日期非法格式拦截
        with self.assertRaises(main_mod.HTTPException) as ctx:
            main_mod.station_trains("北京", "2026-02-30")
        self.assertEqual(ctx.exception.status_code, 400)
        with self.assertRaises(main_mod.HTTPException) as ctx2:
            main_mod.station_trains("北京", "2026-11-2")
        self.assertEqual(ctx2.exception.status_code, 400)

        # 文件导入端点传递 station_train_code（区分 G/D 复用票价编码）
        sample_price = (SAMPLES / "queryTicketPrice_G531.json").read_bytes()

        class FakeUpload:
            async def read(self):
                return sample_price

        imp_res = asyncio.run(
            main_mod.do_import_file(
                file=FakeUpload(),
                api_type="price",
                train_no="2400000D170O",
                station_train_code="D17",
                date="2026-11-20",
            )
        )
        self.assertGreater(imp_res["saved"], 0)

    def test_search_progress_cancel_and_plan_tickets_partial_preserve(self):
        from app import main as main_mod

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("BJP", "北京", "bj", "bj", "北京"),
                    ("SHH", "上海", "sh", "sh", "上海"),
                    ("TJP", "天津", "tj", "tj", "天津"),
                ],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("TN1", "G1", "2026-11-20", 1, "北京", "BJP", "----", "08:00"),
                    ("TN1", "G1", "2026-11-20", 2, "上海", "SHH", "12:00", "12:00"),
                    ("TN2", "G2", "2026-11-20", 1, "北京", "BJP", "----", "09:00"),
                    ("TN2", "G2", "2026-11-20", 2, "天津", "TJP", "09:35", "09:35"),
                ],
            )
            # 为第一段预先写入已有的余票与票价缓存，第二段不写，模拟“查完第1段后取消”
            conn.execute(
                "INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,from_station_no,to_station_no,seat_types,seats,can_web_buy) "
                "VALUES('TN1','G1','2099-02-01','BJP','SHH','01','02','O','{\"ze\":\"无\"}','Y')"
            )
            conn.execute(
                "INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price) "
                "VALUES('TN1','G1','2099-02-01','01','02','ze','二等座',553.0)"
            )

        # 1) 验证 transfer.search 的 progress_cb 回调与 cancel_event 打断
        prog_steps = []
        sr = transfer.search(
            "北京", "上海", "2026-11-20",
            expand_city=False,
            progress_cb=lambda done, total, msg: prog_steps.append((done, total, msg)),
        )
        self.assertEqual(len(sr["direct"]), 1)
        self.assertGreaterEqual(len(prog_steps), 4)
        self.assertEqual(prog_steps[-1][0], prog_steps[-1][1])

        ev_stop = threading.Event()
        ev_stop.set()
        with self.assertRaises(transfer.SearchCancelled):
            transfer.search("北京", "上海", "2026-11-20", expand_city=False, cancel_event=ev_stop)

        # 2) 验证 _transfer_search_job 正常完成与取消状态流转
        req = main_mod.TransferSearchReq(from_station="北京", to_station="上海", date="2026-11-20", expand_city=False)
        jid_ok, ev_ok = main_mod._create_job({"status": "running", "phase": "search", "done": 0, "total": 6, "current": "", "errors": [], "result": {}})
        main_mod._transfer_search_job(jid_ok, ev_ok, req)
        st_ok = main_mod.job_status(jid_ok)
        self.assertEqual(st_ok["status"], "done")
        self.assertEqual(len(st_ok["result"]["direct"]), 1)

        jid_can, ev_can = main_mod._create_job({"status": "running", "phase": "search", "done": 0, "total": 6, "current": "", "errors": [], "result": {}})
        ev_can.set()
        main_mod._transfer_search_job(jid_can, ev_can, req)
        self.assertEqual(main_mod.job_status(jid_can)["status"], "cancelled")

        # 3) 验证 _plan_tickets_job 更新 current，并在第 2 段中途取消时保留第 1 段已查结果
        jid_pt, ev_pt = main_mod._create_job({"status": "running", "phase": "tickets", "done": 0, "total": 0, "current": "", "errors": [], "result": {}})
        seen_currents = []

        def fake_crawl_tickets(f, t, d):
            seen_currents.append(main_mod.job_status(jid_pt)["current"])
            if f == "BJP" and t == "TJP":
                ev_pt.set()
                raise main_mod.CrawlCancelled("用户取消")

        with mock.patch.object(main_mod.crawler, "crawl_tickets", side_effect=fake_crawl_tickets):
            main_mod._plan_tickets_job(
                jid_pt,
                ev_pt,
                [
                    {"key": "TN1|BJP|SHH|2099-02-01", "train_no": "TN1", "train": "G1", "from": "BJP", "to": "SHH", "date": "2099-02-01"},
                    {"key": "TN2|BJP|TJP|2099-02-01", "train_no": "TN2", "train": "G2", "from": "BJP", "to": "TJP", "date": "2099-02-01"},
                ],
            )

        st_pt = main_mod.job_status(jid_pt)
        self.assertEqual(st_pt["status"], "cancelled")
        self.assertEqual(st_pt["done"], 1)
        self.assertTrue(any("北京(BJP)→上海(SHH)" in c for c in seen_currents))
        # 第 1 段已入库的余票与票价必须保留，不能因第 2 段取消而清空
        self.assertIn("TN1|BJP|SHH|2099-02-01", st_pt["result"])
        self.assertEqual(st_pt["result"]["TN1|BJP|SHH|2099-02-01"]["seats"], {"ze": "无"})
        self.assertEqual(st_pt["result"]["TN1|BJP|SHH|2099-02-01"]["prices"][0]["price"], 553.0)
        self.assertNotIn("TN2|BJP|TJP|2099-02-01", st_pt["result"])

        # 4) 验证：已有当日 tickets + raw_payloads 时，search 会将原本来自时刻表的方案转为“实测日期”并附带 ticket_updated
        with db.tx() as conn:
            cur = conn.execute(
                "INSERT INTO raw_payloads(api_type,params,source,fetched_at,payload) VALUES('left_ticket','{}','crawl','2099-02-01T10:30:00','{}')"
            )
            rid = cur.lastrowid
            conn.execute(
                "UPDATE tickets SET raw_id=? WHERE train_no='TN1' AND train_date='2099-02-01'",
                (rid,),
            )
        sr_verified = transfer.search("北京", "上海", "2099-02-01", expand_city=False)
        self.assertEqual(len(sr_verified["direct"]), 1)
        self.assertFalse(sr_verified["direct"][0]["date_assumed"])
        self.assertEqual(sr_verified["direct"][0]["source_date"], "2099-02-01")
        self.assertEqual(sr_verified["direct"][0]["ticket_updated"], "2099-02-01 10:30")

        # 5) 验证 _plan_tickets_job 区分“余票请求失败”与“当日无此车次”并正确透传 ticket_updated
        jid_err, ev_err = main_mod._create_job({"status": "running", "phase": "tickets", "done": 0, "total": 0, "current": "", "errors": [], "result": {}})

        def fake_crawl_err(f, t, d):
            if f == "BJP" and t == "TJP":
                raise RuntimeError("网络超时")

        with mock.patch.object(main_mod.crawler, "crawl_tickets", side_effect=fake_crawl_err):
            main_mod._plan_tickets_job(
                jid_err,
                ev_err,
                [
                    {"key": "TN1|BJP|SHH|2099-02-01", "train_no": "TN1", "train": "G1", "from": "BJP", "to": "SHH", "date": "2099-02-01"},
                    {"key": "TN2|BJP|TJP|2099-02-01", "train_no": "TN2", "train": "G2", "from": "BJP", "to": "TJP", "date": "2099-02-01"},
                ],
            )
        st_err = main_mod.job_status(jid_err)
        self.assertEqual(st_err["status"], "done")
        self.assertEqual(st_err["result"]["TN1|BJP|SHH|2099-02-01"]["ticket_updated"], "2099-02-01 10:30")
        self.assertIn("余票请求失败：网络超时", st_err["result"]["TN2|BJP|TJP|2099-02-01"]["error"])

        # 6) 验证非法剪枝参数在接口层被拦截并返回 400
        with self.assertRaises(main_mod.HTTPException) as ctx_p:
            main_mod.transfers("北京", "上海", "2026-11-20", prune_level="bad_level")
        self.assertEqual(ctx_p.exception.status_code, 400)
        with self.assertRaises(main_mod.HTTPException) as ctx_t:
            main_mod.start_transfers(
                main_mod.TransferSearchReq(from_station="北京", to_station="上海", date="2026-11-20", max_total_min=10)
            )
        self.assertEqual(ctx_t.exception.status_code, 400)

    def test_search_sub_progress_and_large_timetable_index(self):
        import time
        from app import main as main_mod

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("BJP", "北京", "bj", "bj", "北京"),
                    ("M01", "中一", "zy", "zy", "中一市"),
                    ("M02", "中二", "ze", "ze", "中二市"),
                    ("SHH", "上海", "sh", "sh", "上海"),
                    ("X01", "旁站甲", "pxa", "pxa", "旁甲市"),
                    ("X02", "旁站乙", "pxb", "pxb", "旁乙市"),
                ],
            )
            rows = [
                ("L1", "G101", "2026-11-20", 1, "北京", "BJP", "----", "08:00"),
                ("L1", "G101", "2026-11-20", 2, "中一", "M01", "09:00", "09:05"),
                ("L2", "G102", "2026-11-20", 1, "中一", "M01", "----", "09:40"),
                ("L2", "G102", "2026-11-20", 2, "中二", "M02", "10:40", "10:45"),
                ("L3", "G103", "2026-11-20", 1, "中二", "M02", "----", "11:20"),
                ("L3", "G103", "2026-11-20", 2, "上海", "SHH", "12:30", "12:30"),
            ]
            # 构造 1,500 趟不经过终点或旁路车次，检验倒排索引优化与子进度回调（避免全量下 O(N^2) 卡死）
            for i in range(1500):
                tn = f"NOISE_{i:04d}"
                cd = f"K{1000 + i}"
                if i < 60:
                    # 部分从北京始发但不到上海的列车，确保触发外层匹配并需走索引过滤
                    rows.append((tn, cd, "2026-11-20", 1, "北京", "BJP", "----", "07:00"))
                    rows.append((tn, cd, "2026-11-20", 2, "旁站甲", "X01", "08:00", "08:00"))
                else:
                    rows.append((tn, cd, "2026-11-20", 1, "旁站甲", "X01", "----", "08:00"))
                    rows.append((tn, cd, "2026-11-20", 2, "旁站乙", "X02", "09:30", "09:30"))
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                rows,
            )

        prog_steps, sub_steps = [], []
        t0 = time.monotonic()
        sr = transfer.search(
            "北京", "上海", "2026-11-20",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False,
            progress_cb=lambda d, tot, m: prog_steps.append((d, tot, m)),
            sub_cb=lambda sd, st, m: sub_steps.append((sd, st, m)),
        )
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 5.0, f"全量索引搜索耗时过长: {elapsed:.2f}s")
        self.assertEqual(len(sr["transfers"]), 1)
        self.assertEqual([l["train"] for l in sr["transfers"][0]["legs"]], ["G101", "G102", "G103"])
        self.assertTrue(any("计算时刻表直达/换乘" in m for _, _, m in sub_steps))
        self.assertTrue(any("多程 BFS" in m or "构建多程路网" in m for _, _, m in sub_steps))
        for sd, st, _ in sub_steps:
            self.assertGreaterEqual(sd, 0)
            self.assertGreater(st, 0)
            self.assertLessEqual(sd, st)

        # 验证 _transfer_search_job 正确维护与重置 sub_done / sub_total
        req = main_mod.TransferSearchReq(from_station="北京", to_station="上海", date="2026-11-20", max_legs=3, expand_city=False)
        jid, ev = main_mod._create_job({"status": "running", "phase": "search", "done": 0, "total": 7, "sub_done": 0, "sub_total": 0, "current": "", "errors": [], "result": {}})
        main_mod._transfer_search_job(jid, ev, req)
        st_job = main_mod.job_status(jid)
        self.assertEqual(st_job["status"], "done")
        self.assertEqual(st_job["sub_done"], 0)
        self.assertEqual(st_job["sub_total"], 0)
        self.assertEqual(len(st_job["result"]["transfers"]), 1)


class TestStdlibHTTPServer(TempDBTestCase):
    def test_http_routes_multipart_and_nested_models(self):
        import json
        import threading
        import time
        import urllib.error
        import urllib.request
        from app import server as server_mod

        srv = server_mod.create_server("127.0.0.1", 0)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        base = f"http://127.0.0.1:{port}"

        def req(method, path, body=None, headers=None):
            h = dict(headers or {})
            data = body
            if isinstance(body, (dict, list)):
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                h.setdefault("Content-Type", "application/json")
            r = urllib.request.Request(base + path, data=data, headers=h, method=method)
            try:
                with urllib.request.urlopen(r, timeout=5) as resp:
                    return resp.status, resp.read()
            except urllib.error.HTTPError as e:
                return e.code, e.read()

        try:
            # 1) 首页与统计接口
            st, raw = req("GET", "/")
            self.assertEqual(st, 200)
            self.assertIn("中国铁路12306 · 智能中转", raw.decode("utf-8"))

            st, raw = req("GET", "/api/stats")
            self.assertEqual(st, 200)
            stats = json.loads(raw)
            self.assertIn("stations", stats)

            # 2) multipart/form-data 文件导入站点并校验入库
            boundary = "----TestBoundary123"
            stations_js = "var station_names ='@bjp|北京|BJP|beijing|bj|0|0357|北京|||@shh|上海|SHH|shanghai|sh|1|0712|上海|||';".encode("utf-8")
            mp_body = (
                f"--{boundary}\r\n"
                f"Content-Disposition: form-data; name=\"api_type\"\r\n\r\n"
                f"stations\r\n"
                f"--{boundary}\r\n"
                f"Content-Disposition: form-data; name=\"file\"; filename=\"station_name.js\"\r\n"
                f"Content-Type: application/javascript\r\n\r\n"
            ).encode("utf-8") + stations_js + f"\r\n--{boundary}--\r\n".encode("utf-8")
            st, raw = req("POST", "/api/import/file", body=mp_body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
            self.assertEqual(st, 200, raw.decode("utf-8", errors="replace"))
            self.assertGreaterEqual(json.loads(raw)["saved"], 2)

            # 3) 异步换乘搜索与参数校验（含 0 分钟参数不回退默认值 + 400 错误校验）
            st, raw = req("POST", "/api/transfers/start", {
                "from_station": "北京", "to_station": "上海", "date": "2026-11-20",
                "min_wait": 0, "cross_extra": 0,
                "max_legs": 2, "expand_city": False, "prune_level": "strict",
            })
            self.assertEqual(st, 200)
            jid = json.loads(raw)["job_id"]
            for _ in range(40):
                st_j, raw_j = req("GET", f"/api/jobs/{jid}")
                self.assertEqual(st_j, 200)
                jobj = json.loads(raw_j)
                if jobj.get("status") != "running":
                    break
                time.sleep(0.05)
            self.assertEqual(jobj["status"], "done")
            # 3b) HTTP 换乘默认带返回上限（两段枚举不设限会返回数十万条），且给出截断前命中数
            res = jobj["result"]
            self.assertIn("direct_total", res)
            self.assertIn("transfers_total", res)
            self.assertLessEqual(len(res["direct"]) + len(res["transfers"]), 1000)
            self.assertEqual(jobj["result"]["max_legs"], 2)

            st_bad, _ = req("POST", "/api/transfers/start", {
                "from_station": "北京", "to_station": "上海", "date": "2026-11-20", "prune_level": "invalid"
            })
            self.assertEqual(st_bad, 400)

            # 4) 嵌套 BaseModel (list[PlanLeg]) 校验
            with mock.patch("app.main.crawler.crawl_tickets"):
                st_p, raw_p = req("POST", "/api/plan_tickets", {
                    "legs": [{"train_no": "T1", "train": "G1", "date": "2099-01-01", "from_telecode": "BJP", "to_telecode": "SHH"}]
                })
                self.assertEqual(st_p, 200)
                self.assertEqual(json.loads(raw_p)["legs"], 1)

            # 5) DELETE 清理缓存路由
            st_d, raw_d = req("DELETE", "/api/cache?before_date=2099-01-01&include_raw=false")
            self.assertEqual(st_d, 200)
            self.assertIn("deleted", json.loads(raw_d))

            # 5b) 缺省 before_date 必须拒绝，否则 db.clear_cache(None) 会全表删光已爬数据
            st_all, _ = req("DELETE", "/api/cache")
            self.assertEqual(st_all, 400)
        finally:
            srv.shutdown()
            srv.server_close()


class TestStrictPruningEnhancements(TempDBTestCase):
    def test_8h_detour_floor_price_safety_dominance_and_dual_frontier(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("XKS", "厦门北", "xmb", "xmb", "厦门"),
                    ("FZS", "福州", "fz", "fz", "福州"),
                    ("SHS", "三明北", "smb", "smb", "三明"),
                    ("YPS", "延平", "yp", "yp", "南平"),
                    ("NOS", "南平市", "nps", "nps", "南平"),
                ],
            )
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # 最快高铁（福州中转）：G3756 07:18 -> 08:40 + G1698 08:44 -> 09:46（148分钟，仅4分钟极短换乘）
                    ("T_G3756", "G3756", "2026-11-21", 1, "厦门北", "XKS", "----", "07:18"),
                    ("T_G3756", "G3756", "2026-11-21", 2, "福州", "FZS", "08:40", "08:40"),
                    ("T_G1698", "G1698", "2026-11-21", 1, "福州", "FZS", "----", "08:44"),
                    ("T_G1698", "G1698", "2026-11-21", 2, "南平市", "NOS", "09:46", "09:46"),
                    # 同福州中转的稳妥方案：G3750 07:10 -> 08:30 + G1700 09:15 -> 10:20（会被更晚发车的 G3756+G1700 35分钟稳妥方案合法支配）
                    ("T_G3750", "G3750", "2026-11-21", 1, "厦门北", "XKS", "----", "07:10"),
                    ("T_G3750", "G3750", "2026-11-21", 2, "福州", "FZS", "08:30", "08:30"),
                    ("T_G1700", "G1700", "2026-11-21", 1, "福州", "FZS", "----", "09:15"),
                    ("T_G1700", "G1700", "2026-11-21", 2, "南平市", "NOS", "10:20", "10:20"),
                    # 同福州中转的低价动车方案（早发晚到但便宜一半，绝不能被 G3756+G1700 支配）
                    ("T_D375", "D375", "2026-11-21", 1, "厦门北", "XKS", "----", "07:12"),
                    ("T_D375", "D375", "2026-11-21", 2, "福州", "FZS", "08:32", "08:32"),
                    ("T_D170", "D170", "2026-11-21", 1, "福州", "FZS", "----", "09:15"),
                    ("T_D170", "D170", "2026-11-21", 2, "南平市", "NOS", "10:30", "10:30"),
                    # 同福州中转既更慢又更贵的冗余高铁：G3748 07:05 -> 08:30 + G1702 09:15 -> 10:35（应被 G3756+G1700 支配）
                    ("T_G3748", "G3748", "2026-11-21", 1, "厦门北", "XKS", "----", "07:05"),
                    ("T_G3748", "G3748", "2026-11-21", 2, "福州", "FZS", "08:30", "08:30"),
                    ("T_G1702", "G1702", "2026-11-21", 1, "福州", "FZS", "----", "09:15"),
                    ("T_G1702", "G1702", "2026-11-21", 2, "南平市", "NOS", "10:35", "10:35"),
                    # 3 程高价高速快线占满三明北早到达槽位（验证 frontier_k=2 时不会把 Z126 挤出）
                    ("T_GF1", "G101", "2026-11-21", 1, "厦门北", "XKS", "----", "07:30"),
                    ("T_GF1", "G101", "2026-11-21", 2, "三明北", "SHS", "09:00", "09:00"),
                    ("T_GF2", "G102", "2026-11-21", 1, "厦门北", "XKS", "----", "07:40"),
                    ("T_GF2", "G102", "2026-11-21", 2, "三明北", "SHS", "09:10", "09:10"),
                    ("T_GF3", "G103", "2026-11-21", 1, "厦门北", "XKS", "----", "07:50"),
                    ("T_GF3", "G103", "2026-11-21", 2, "三明北", "SHS", "09:20", "09:20"),
                    # 用户案例同款 3 程低价普速接动车（6小时56分=416分 <= 480分8小时保底，且到达三明北最晚但最便宜）：
                    ("T_Z126", "Z126", "2026-11-21", 1, "厦门北", "XKS", "----", "07:25"),
                    ("T_Z126", "Z126", "2026-11-21", 2, "三明北", "SHS", "10:08", "10:08"),
                    ("T_D6318", "D6318", "2026-11-21", 1, "三明北", "SHS", "----", "12:22"),
                    ("T_D6318", "D6318", "2026-11-21", 2, "延平", "YPS", "12:51", "12:51"),
                    ("T_D2352", "D2352", "2026-11-21", 1, "延平", "YPS", "----", "13:39"),
                    ("T_D2352", "D2352", "2026-11-21", 2, "南平市", "NOS", "14:21", "14:21"),
                    # 超出 8 小时保底上限（9小时30分=570分 > 480分）的超慢绕行，应记入 detour 剪枝
                    ("T_SLOW1", "K901", "2026-11-21", 1, "厦门北", "XKS", "----", "06:00"),
                    ("T_SLOW1", "K901", "2026-11-21", 2, "福州", "FZS", "11:00", "11:00"),
                    ("T_SLOW2", "K902", "2026-11-21", 1, "福州", "FZS", "----", "11:30"),
                    ("T_SLOW2", "K902", "2026-11-21", 2, "南平市", "NOS", "15:30", "15:30"),
                ],
            )
            conn.executemany(
                "INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price) VALUES(?,?,?,?,?,?,?,?)",
                [
                    ("T_G3756", "G3756", "2026-11-21", "01", "02", "ze", "二等座", 140.0),
                    ("T_G1698", "G1698", "2026-11-21", "01", "02", "ze", "二等座", 110.0),
                    ("T_G3750", "G3750", "2026-11-21", "01", "02", "ze", "二等座", 140.0),
                    ("T_G1700", "G1700", "2026-11-21", "01", "02", "ze", "二等座", 110.0),
                    ("T_D375", "D375", "2026-11-21", "01", "02", "ze", "二等座", 70.0),
                    ("T_D170", "D170", "2026-11-21", "01", "02", "ze", "二等座", 55.0),
                    ("T_G3748", "G3748", "2026-11-21", "01", "02", "ze", "二等座", 150.0),
                    ("T_G1702", "G1702", "2026-11-21", "01", "02", "ze", "二等座", 120.0),
                    ("T_GF1", "G101", "2026-11-21", "01", "02", "ze", "二等座", 160.0),
                    ("T_GF2", "G102", "2026-11-21", "01", "02", "ze", "二等座", 160.0),
                    ("T_GF3", "G103", "2026-11-21", "01", "02", "ze", "二等座", 160.0),
                    ("T_Z126", "Z126", "2026-11-21", "01", "02", "yz", "硬座", 50.5),
                    ("T_D6318", "D6318", "2026-11-21", "01", "02", "ze", "二等座", 22.0),
                    ("T_D2352", "D2352", "2026-11-21", "01", "02", "ze", "二等座", 32.0),
                ],
            )

        res = transfer.search(
            "厦门北", "南平市", "2026-11-21",
            min_wait=0, max_wait=180, max_legs=3,
            frontier_k=2, prune_level="strict",
            max_total_min=1440, detour_ratio=2.0, prune_dominated=True,
        )
        chains = [tuple(l["train"] for l in p["legs"]) for p in res["transfers"]]
        # 1) 8小时保底生效（148*2=296 -> 保底 480 分）且 570分慢车被计入 detour 统计
        self.assertEqual(res["prune_config"]["detour_limit_min"], 480)
        self.assertGreater(res["prune_stats"]["detour"], 0)
        self.assertNotIn(("K901", "K902"), chains)
        # 2) 多程束宽=2 同时保留最便宜支线 Z126，416分的 Z126+D6318+D2352 在严格模式下成功保留
        self.assertIn(("Z126", "D6318", "D2352"), chains)
        # 3) 4分钟高风险换乘(G3756+G1698)不支配35分钟稳妥换乘(G3756+G1700)，高价快车也不支配同站低价动车(D375+D170)；
        #    而更贵且更慢的冗余同站方案(G3748+G1702)仍被支配剪除
        self.assertIn(("G3756", "G1698"), chains)
        self.assertIn(("G3756", "G1700"), chains)
        self.assertIn(("D375", "D170"), chains)
        self.assertNotIn(("G3748", "G1702"), chains)
        self.assertGreater(res["prune_stats"]["dominated"], 0)


class TestInMemoryCacheAndBatching(TempDBTestCase):
    def test_memory_cache_hit_and_invalidation(self):
        import sqlite3
        from concurrent.futures import ThreadPoolExecutor

        transfer.invalidate_cache()
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("XKS", "厦门北", "xiamenbei", "xmb", "厦门"),
                    ("FZS", "福州", "fuzhou", "fz", "福州"),
                    ("NOS", "南平市", "nanpingshi", "nps", "南平"),
                ],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("T1", "G1", "2026-11-21", 1, "厦门北", "XKS", "----", "08:00"),
                    ("T1", "G1", "2026-11-21", 2, "福州", "FZS", "09:20", "09:20"),
                    ("T2", "G2", "2026-11-21", 1, "福州", "FZS", "----", "10:00"),
                    ("T2", "G2", "2026-11-21", 2, "南平市", "NOS", "11:00", "11:00"),
                ],
            )
        # 刷出一次检查点后，确认单线程只读 close_conn() 后下一次搜索仍命中内存缓存
        db.close_conn()
        r0 = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertFalse(r0["cache_hit"])
        db.close_conn()
        r0_hit = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertTrue(r0_hit["cache_hit"])
        db.close_conn()
        transfer.invalidate_cache()

        # 1) 并发冷启动检索应安全完成
        with ThreadPoolExecutor(max_workers=3) as pool:
            futs = [
                pool.submit(transfer.search, "厦门北", "南平市", "2026-11-21", max_legs=2)
                for _ in range(3)
            ]
            results = [f.result() for f in futs]
        db.close_conn()
        for r in results:
            self.assertEqual([tuple(l["train"] for l in p["legs"]) for p in r["transfers"]], [("G1", "G2")])

        # 2) 新线程（新 SQLite 连接）热命中内存缓存（含 3 程复用 services_all）
        r_warm = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=3)
        self.assertTrue(r_warm["cache_hit"])
        db.close_conn()
        r_warm2 = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertTrue(r_warm2["cache_hit"])

        # 3) 进程内写入后缓存自动失效并立即可见新直达车次
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("T3", "G3", "2026-11-21", 1, "厦门北", "XKS", "----", "08:30"),
                    ("T3", "G3", "2026-11-21", 2, "南平市", "NOS", "10:10", "10:10"),
                ],
            )
        r_after_tx = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertFalse(r_after_tx["cache_hit"])
        self.assertEqual([p["legs"][0]["train"] for p in r_after_tx["direct"]], ["G3"])

        r_warm3 = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertTrue(r_warm3["cache_hit"])
        db.close_conn()

        # 4) 模拟外部独立进程（如 crawl_nationwide.py）直接写库，应能通过文件变更感知失效并刷新
        ext = sqlite3.connect(db.DB_PATH, timeout=30)
        try:
            ext.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("T4", "G4", "2026-11-21", 1, "厦门北", "XKS", "----", "09:00"),
                    ("T4", "G4", "2026-11-21", 2, "南平市", "NOS", "10:30", "10:30"),
                ],
            )
            ext.commit()
        finally:
            ext.close()
        r_after_ext = transfer.search("厦门北", "南平市", "2026-11-21", max_legs=2)
        self.assertFalse(r_after_ext["cache_hit"])
        self.assertEqual({p["legs"][0]["train"] for p in r_after_ext["direct"]}, {"G3", "G4"})


class TestAuditFixesAndIncludeModes(TempDBTestCase):
    def _seed_stations(self, items):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode, name, pinyin, short, city) VALUES(?,?,?,?,?)",
                items,
            )

    def test_cross_midnight_timetable_screen_and_terminal_quirk(self):
        self._seed_stations([
            ("AAA", "甲站", "jz", "jz", "甲市"),
            ("MID", "夜转站", "yzz", "yzz", "夜市"),
            ("BBB", "乙站", "yz", "yz", "乙市"),
        ])
        with db.tx() as conn:
            # 1) 时刻表在 夜转站 23:55 到达、00:05 发车：到达应在 Day0，发车应在 Day1
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("TT_MID", "K100", "2026-11-20", 1, "甲站", "AAA", "----", "20:00"),
                    ("TT_MID", "K100", "2026-11-20", 2, "夜转站", "MID", "23:55", "00:05"),
                    ("TT_MID", "K100", "2026-11-20", 3, "乙站", "BBB", "04:05", "04:05"),
                ],
            )
            # 2) 大屏缺 day_diff 时的 23:55/00:05 午夜跨天（应回退 arr） + 终到站 13:06/13:00 小幅倒挂（不应回退 1440）
            conn.executemany(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,arrive_day_diff,start_day_diff,
                     start_station_name,start_station_telecode,end_station_name,end_station_telecode)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    ("AAA", "甲站", "2026-11-20", "Z88", "SCR_M", "2026-11-20", "2026-11-20", 1, "----", "20:00", None, None, "甲站", "AAA", "乙站", "BBB"),
                    ("MID", "夜转站", "2026-11-21", "Z88", "SCR_M", "2026-11-21", "2026-11-20", 2, "23:55", "00:05", None, None, "甲站", "AAA", "乙站", "BBB"),
                    ("BBB", "乙站", "2026-11-21", "Z88", "SCR_M", "2026-11-21", "2026-11-20", 3, "13:06", "13:00", None, None, "甲站", "AAA", "乙站", "BBB"),
                ],
            )

        res = transfer.search("甲站", "乙站", "2026-11-20", max_legs=1, expand_city=False)
        by_tn = {p["legs"][0]["train_no"]: p for p in res["direct"]}
        self.assertEqual(by_tn["TT_MID"]["total_min"], 485)
        self.assertEqual(by_tn["TT_MID"]["legs"][0]["arrive"], "2026-11-21 04:05")
        # 终到站 13:06/13:00 不应被误减 1440 分钟；次日 13:06 - 前日 20:00 = 1026 分钟
        self.assertEqual(by_tn["SCR_M"]["total_min"], 1026)

        # 验证大屏夜转站观测 arr 早于 dep（11-20 23:55 vs 11-21 00:05）
        scr_loaded = transfer._load_screen()
        obs_mid = scr_loaded[("SCR_M", "2026-11-20")]["obs"]["MID"]
        self.assertEqual(obs_mid["dep"] - obs_mid["arr"], 10)

    def test_exact_station_filtering_no_spread_and_include_or_and(self):
        from app import main as main_mod

        self._seed_stations([
            ("ORG", "起点站", "qd", "qd", "起点市"),
            ("BJP", "北京", "bj", "bj", "北京"),
            ("VNP", "北京南", "bjn", "bjn", "北京"),
            ("FZS", "福州", "fz", "fz", "福州"),
            ("FYS", "福州南", "fzn", "fzn", "福州"),
            ("NCG", "南昌", "nc", "nc", "南昌"),
            ("YPS", "延平", "yp", "yp", "南平"),
            ("NOS", "南平市", "nps", "nps", "南平"),
            ("DST", "终点站", "zd", "zd", "终点市"),
        ])
        with db.tx() as conn:
            conn.execute("UPDATE stations SET lat=26.113, lon=119.320, coord_status='verified' WHERE telecode='FZS'")
            conn.execute("UPDATE stations SET lat=26.000, lon=119.390, coord_status='verified' WHERE telecode='FYS'")
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # 经北京(BJP)换乘 vs 经北京南(VNP)换乘
                    ("T1", "G1", "2026-11-20", 1, "起点站", "ORG", "----", "08:00"),
                    ("T1", "G1", "2026-11-20", 2, "北京", "BJP", "09:00", "09:00"),
                    ("T2", "G2", "2026-11-20", 1, "北京", "BJP", "----", "09:40"),
                    ("T2", "G2", "2026-11-20", 2, "终点站", "DST", "11:00", "11:00"),
                    ("T3", "G3", "2026-11-20", 1, "起点站", "ORG", "----", "08:10"),
                    ("T3", "G3", "2026-11-20", 2, "北京南", "VNP", "09:10", "09:10"),
                    ("T4", "G4", "2026-11-20", 1, "北京南", "VNP", "----", "09:50"),
                    ("T4", "G4", "2026-11-20", 2, "终点站", "DST", "11:10", "11:10"),
                    # 延平(YPS, 所属城市“南平”) 与 南平市(NOS, 所属城市“南平”)
                    ("T5", "G5", "2026-11-20", 1, "起点站", "ORG", "----", "08:20"),
                    ("T5", "G5", "2026-11-20", 2, "延平", "YPS", "09:20", "09:20"),
                    ("T6", "G6", "2026-11-20", 1, "延平", "YPS", "----", "10:00"),
                    ("T6", "G6", "2026-11-20", 2, "终点站", "DST", "11:20", "11:20"),
                    # 3 程方案（异站 福州->福州南，再经 南昌 转 终点站），以及仅过南昌的 2 程方案
                    ("TA1", "D10", "2026-11-20", 1, "起点站", "ORG", "----", "07:00"),
                    ("TA1", "D10", "2026-11-20", 2, "福州", "FZS", "08:00", "08:00"),
                    ("TA2", "D11", "2026-11-20", 1, "福州南", "FYS", "----", "09:30"),
                    ("TA2", "D11", "2026-11-20", 2, "南昌", "NCG", "11:00", "11:00"),
                    ("TA3", "D12", "2026-11-20", 1, "南昌", "NCG", "----", "11:40"),
                    ("TA3", "D12", "2026-11-20", 2, "终点站", "DST", "13:00", "13:00"),
                    ("TB1", "D20", "2026-11-20", 1, "起点站", "ORG", "----", "07:30"),
                    ("TB1", "D20", "2026-11-20", 2, "南昌", "NCG", "09:00", "09:00"),
                ],
            )

        # 1) 排除“北京南”不得扩散到“北京”，排除“南平市”不得扩散到同城“延平”
        r_exc = transfer.search(
            "起点站", "终点站", "2026-11-20",
            min_wait=30, max_wait=180, max_legs=2, expand_city=False,
            prune_level="custom", exclude_stations="北京南,南平市",
        )
        tr_stations = {tr["station"] for p in r_exc["transfers"] for tr in p["transfers"]}
        self.assertIn("北京", tr_stations)
        self.assertIn("延平", tr_stations)
        self.assertNotIn("北京南", tr_stations)

        # 2) OR 模式与 AND 模式 + 引擎 D 异站下车站“福州南”命中 include_stations
        r_or = transfer.search(
            "起点站", "终点站", "2026-11-20",
            min_wait=30, max_wait=180, max_legs=3, expand_city=False,
            prune_level="custom", include_stations="福州南,南昌", include_stations_mode="or",
        )
        or_chains = {tuple(l["train"] for l in p["legs"]) for p in r_or["transfers"]}
        self.assertIn(("D20", "D12"), or_chains)
        self.assertIn(("D10", "D11", "D12"), or_chains)

        r_and = transfer.search(
            "起点站", "终点站", "2026-11-20",
            min_wait=30, max_wait=180, max_legs=3, expand_city=False,
            prune_level="custom", include_stations="福州南,南昌", include_stations_mode="and",
        )
        and_chains = {tuple(l["train"] for l in p["legs"]) for p in r_and["transfers"]}
        self.assertEqual(and_chains, {("D10", "D11", "D12")})
        self.assertEqual(r_and["prune_config"]["include_stations_mode"], "and")

        with self.assertRaises(main_mod.HTTPException) as ctx_mode:
            main_mod.transfers("起点站", "终点站", "2026-11-20", include_stations_mode="xor")
        self.assertEqual(ctx_mode.exception.status_code, 400)

    def test_multi_leg_dedup_mid_depth_dest_and_max_legs_1(self):
        self._seed_stations([
            ("A1", "甲北", "jb", "jb", "甲市"),
            ("A2", "甲南", "jn", "jn", "甲市"),
            ("M1", "中一", "zy", "zy", "中一市"),
            ("M2", "中二", "ze", "ze", "中二市"),
            ("B1", "乙南", "yn", "yn", "乙市"),
            ("B2", "乙北", "yb", "yb", "乙市"),
        ])
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # G10：同城停 甲北(08:00) -> 甲南(08:15) -> 中一(09:10) -> 乙南(13:00 亦可直达)
                    ("T10", "G10", "2026-11-20", 1, "甲北", "A1", "----", "08:00"),
                    ("T10", "G10", "2026-11-20", 2, "甲南", "A2", "08:12", "08:15"),
                    ("T10", "G10", "2026-11-20", 3, "中一", "M1", "09:10", "09:15"),
                    # G20：中一(09:50) -> 中二(10:50)
                    ("T20", "G20", "2026-11-20", 1, "中一", "M1", "----", "09:50"),
                    ("T20", "G20", "2026-11-20", 2, "中二", "M2", "10:50", "10:50"),
                    # G30：中二(11:30) -> 同城双终到 乙南(12:20) -> 乙北(12:40)
                    ("T30", "G30", "2026-11-20", 1, "中二", "M2", "----", "11:30"),
                    ("T30", "G30", "2026-11-20", 2, "乙南", "B1", "12:15", "12:20"),
                    ("T30", "G30", "2026-11-20", 3, "乙北", "B2", "12:40", "12:40"),
                    # G31：中二(11:35) -> 乙南(12:25) -> 之后过头折返 中一(13:10)，在 max_legs=4 (not is_last) 下必须被 loopback 剔除
                    ("T31", "G31", "2026-11-20", 1, "中二", "M2", "----", "11:35"),
                    ("T31", "G31", "2026-11-20", 2, "乙南", "B1", "12:20", "12:25"),
                    ("T31", "G31", "2026-11-20", 3, "中一", "M1", "13:10", "13:10"),
                    # 直达对比用
                    ("TD", "G99", "2026-11-20", 1, "甲北", "A1", "----", "07:00"),
                    ("TD", "G99", "2026-11-20", 2, "乙北", "B2", "10:00", "10:00"),
                ],
            )

        # 1) max_legs=4 时 depth=3 (not is_last)：必须枚举出同车 乙南/乙北 双终到、保留 甲北/甲南 双始发去重分支，并剔除过头折返 G31
        res4 = transfer.search("甲市", "乙市", "2026-11-20", min_wait=30, max_wait=120, max_legs=4, expand_city=True)
        g30_od = {(p["legs"][0]["from"], p["legs"][-1]["to"])
                  for p in res4["transfers"]
                  if tuple(l["train"] for l in p["legs"]) == ("G10", "G20", "G30")}
        self.assertEqual(g30_od, {("甲北", "乙南"), ("甲北", "乙北"), ("甲南", "乙南"), ("甲南", "乙北")})
        self.assertFalse(any("G31" in [l["train"] for l in p["legs"]] for p in res4["transfers"]))

        # 2) max_legs=1 仅直达，不生成换乘方案
        res1 = transfer.search("甲市", "乙市", "2026-11-20", max_legs=1, expand_city=True)
        self.assertEqual(len(res1["direct"]), 1)
        self.assertEqual(res1["transfers"], [])

    def test_engine_c_long_duration_engine_a_city_loop_and_price_zfill(self):
        from app import main as main_mod

        self._seed_stations([
            ("A1", "甲东", "jd", "jd", "甲市"),
            ("A2", "甲西", "jx", "jx", "甲市"),
            ("B1", "乙站", "yz", "yz", "乙市"),
            ("C1", "丙站", "bz", "bz", "丙市"),
        ])
        with db.tx() as conn:
            # 1) 引擎 C 超 24 小时余票直达（duration='28:30' -> 1710 分钟）
            conn.execute(
                """INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,
                     from_station_no,to_station_no,start_time,arrive_time,duration,seats,can_web_buy)
                   VALUES('TC_LONG','Z28','2026-11-20','A1','C1','1','5','08:00','12:30','28:30','{"yz":"有"}','Y')"""
            )
            # 2) 引擎 A 城际环绕剪枝：G1 甲东(08:00)->乙站(09:00)；G2 乙站(09:40)->经甲西(10:20)->丙站(11:30)
            conn.executemany(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,
                     start_station_name,start_station_telecode,end_station_name,end_station_telecode)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    ("A1", "甲东", "2026-11-20", "G1", "TA1", "2026-11-20", "2026-11-20", 1, "----", "08:00", "甲东", "A1", "乙站", "B1"),
                    ("B1", "乙站", "2026-11-20", "G1", "TA1", "2026-11-20", "2026-11-20", 2, "09:00", "09:00", "甲东", "A1", "乙站", "B1"),
                    ("B1", "乙站", "2026-11-20", "G2", "TA2", "2026-11-20", "2026-11-20", 1, "----", "09:40", "乙站", "B1", "丙站", "C1"),
                    ("A2", "甲西", "2026-11-20", "G2", "TA2", "2026-11-20", "2026-11-20", 2, "10:15", "10:20", "乙站", "B1", "丙站", "C1"),
                    ("C1", "丙站", "2026-11-20", "G2", "TA2", "2026-11-20", "2026-11-20", 3, "11:30", "11:30", "乙站", "B1", "丙站", "C1"),
                ],
            )

        res = transfer.search("甲东", "丙站", "2026-11-20", min_wait=30, max_wait=120, max_legs=2, expand_city=False)
        z28 = [p for p in res["direct"] if p["legs"][0]["train_no"] == "TC_LONG"]
        self.assertEqual(len(z28), 1)
        self.assertEqual(z28[0]["total_min"], 1710)
        self.assertEqual(res["transfers"], [])
        self.assertGreater(res["prune_stats"]["city_loop"], 0)

        # 3) importer 价格站序零填充（非空 "1"->"01"，空串保持 ""）+ _route_job zfill 查重跳过已缓存票价
        importer.ingest(
            json.dumps({"data": {"liveBeijing": False, "OT": [], "O": "300.0"}}),
            "price",
            ctx={"train_no": "TC_LONG", "station_train_code": "Z28", "date": "2026-11-20",
                 "from_station_no": "1", "to_station_no": "5"},
            source="test",
        )
        importer.ingest(
            json.dumps({"data": {"O": "100.0"}}),
            "price",
            ctx={"train_no": "TC_EMPTY", "date": "2026-11-20", "from_station_no": "", "to_station_no": ""},
            source="test",
        )
        p_rows = db.rows("SELECT from_station_no, to_station_no FROM prices WHERE train_no='TC_LONG'")
        self.assertEqual((p_rows[0]["from_station_no"], p_rows[0]["to_station_no"]), ("01", "05"))
        pe_rows = db.rows("SELECT from_station_no, to_station_no FROM prices WHERE train_no='TC_EMPTY'")
        self.assertEqual((pe_rows[0]["from_station_no"], pe_rows[0]["to_station_no"]), ("", ""))

        jid, ev = main_mod._create_job({"status": "running", "done": 0, "total": 0, "phase": "init",
                                       "current": "", "truncated": 0, "log": [], "errors": [], "result": {}})
        with mock.patch.object(main_mod.crawler, "crawl_station_trains"), \
             mock.patch.object(main_mod.crawler, "crawl_timetable"), \
             mock.patch.object(main_mod.crawler, "crawl_tickets"), \
             mock.patch.object(main_mod.crawler, "crawl_price") as m_cp:
            main_mod._route_job(jid, ev, "甲东", "丙站", "2026-11-20", True)
            m_cp.assert_not_called()

    def test_unbounded_hub_narrowing_engine_d_guards_and_post_filter(self):
        from app import main as main_mod

        self._seed_stations([
            ("ORG", "起点站", "qd", "qd", "起点市"),
            ("BJS", "北京", "bj", "bj", "北京"),
            ("VNP", "北京南", "bjn", "bjn", "北京"),
            ("TJP", "天津", "tj", "tj", "天津"),
            ("JNK", "济南", "jn", "jn", "济南"),
            ("NCG", "南昌", "nc", "nc", "南昌"),
            ("XHZ", "小站", "xz", "xz", "小市"),
            ("DST", "终点站", "zd", "zd", "终点市"),
        ])
        with db.tx() as conn:
            # 1) 测试城内中转同城空转跳过但不阻断后续出城（起点->北京 -> 北京南 -> 济南 -> 终点站）
            #    以及超过48h(50h)的多程车次验证 unbounded(0=不限) 不回退到 48h
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("L1", "G10", "2026-11-20", 1, "起点站", "ORG", "----", "08:00"),
                    ("L1", "G10", "2026-11-20", 2, "北京", "BJS", "09:00", "09:00"),
                    # L2 同城先停 北京 再停 北京南，之后出城到 济南：应跳过 北京南 换乘落点，但放行 济南
                    ("L2", "G20", "2026-11-20", 1, "北京", "BJS", "----", "09:40"),
                    ("L2", "G20", "2026-11-20", 2, "北京南", "VNP", "09:55", "10:00"),
                    ("L2", "G20", "2026-11-20", 3, "济南", "JNK", "11:30", "11:30"),
                    # 若误在 北京南 换乘可接 D21->终点站，应被 intra-city 规则剪除
                    ("L2_BAD", "D21", "2026-11-20", 1, "北京南", "VNP", "----", "10:35"),
                    ("L2_BAD", "D21", "2026-11-20", 2, "终点站", "DST", "12:00", "12:00"),
                    ("L3", "G30", "2026-11-20", 1, "济南", "JNK", "----", "12:10"),
                    ("L3", "G30", "2026-11-20", 2, "终点站", "DST", "13:30", "13:30"),
                    # L_DUR0: 零耗时脏数据（09:00 -> 09:00），在 3 程引擎应被 dur<=0 防护忽略
                    ("L_DUR0", "K99", "2026-11-20", 1, "济南", "JNK", "----", "12:15"),
                    ("L_DUR0", "K99", "2026-11-20", 2, "终点站", "DST", "12:15", "12:15"),
                    # 2) 超 48 小时慢车链（总历时约 51 小时 > 2880m，仅 unbounded 与 relaxed(96h) 保留，balanced(48h) 剪除）
                    #    注：走“小站 -> 南昌 -> 终点站”3 程
                    ("SL1", "K101", "2026-11-20", 1, "起点站", "ORG", "----", "06:00"),
                    ("SL1", "K101", "2026-11-20", 2, "小站", "XHZ", "18:00", "18:00"),
                    ("SL2", "K102", "2026-11-20", 1, "小站", "XHZ", "----", "19:00"),
                    ("SL2", "K102", "2026-11-20", 2, "起点前", "PRE", "23:00", "23:10"),
                    ("SL2", "K102", "2026-11-20", 3, "南昌", "NCG", "07:00", "07:00"),
                    ("SL3", "K103", "2026-11-20", 1, "南昌", "NCG", "----", "07:40"),
                    ("SL3", "K103", "2026-11-20", 2, "中继", "ZJB", "23:55", "00:05"),
                    ("SL3", "K103", "2026-11-20", 3, "终点站", "DST", "09:00", "09:00"),
                ],
            )
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [("PRE", "起点前", "qdq", "qdq", "前市"), ("ZJB", "中继", "zj", "zj", "中市")],
            )
            # 大屏 A：起点站->小站(XHZ)->终点站 与 起点站->南昌(NCG)->终点站（验证引擎 A 的 hub_only 裁剪）
            conn.executemany(
                """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                     station_train_date,start_train_date,station_no,arrive_time,start_time,
                     start_station_name,start_station_telecode,end_station_name,end_station_telecode)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    ("ORG", "起点站", "2026-11-20", "D50", "SA1", "2026-11-20", "2026-11-20", 1, "----", "08:00", "起点站", "ORG", "小站", "XHZ"),
                    ("XHZ", "小站", "2026-11-20", "D50", "SA1", "2026-11-20", "2026-11-20", 2, "09:00", "09:00", "起点站", "ORG", "小站", "XHZ"),
                    ("XHZ", "小站", "2026-11-20", "D51", "SA2", "2026-11-20", "2026-11-20", 1, "----", "09:40", "小站", "XHZ", "终点站", "DST"),
                    ("DST", "终点站", "2026-11-20", "D51", "SA2", "2026-11-20", "2026-11-20", 2, "10:40", "10:40", "小站", "XHZ", "终点站", "DST"),
                    ("ORG", "起点站", "2026-11-20", "G60", "SA3", "2026-11-20", "2026-11-20", 1, "----", "08:10", "起点站", "ORG", "南昌", "NCG"),
                    ("NCG", "南昌", "2026-11-20", "G60", "SA3", "2026-11-20", "2026-11-20", 2, "09:10", "09:10", "起点站", "ORG", "南昌", "NCG"),
                    ("NCG", "南昌", "2026-11-20", "G61", "SA4", "2026-11-20", "2026-11-20", 1, "----", "09:50", "南昌", "NCG", "终点站", "DST"),
                    ("DST", "终点站", "2026-11-20", "G61", "SA4", "2026-11-20", "2026-11-20", 2, "10:50", "10:50", "南昌", "NCG", "终点站", "DST"),
                ],
            )

        # 1) balanced (48h=2880m) 会剪掉 51h 的 K101+K102+K103，同时放行 G10+G20+G30（跳过北京南、不留K99零耗时）
        r_bal = transfer.search("起点站", "终点站", "2026-11-20", min_wait=30, max_wait=120, max_legs=3, prune_level="balanced", expand_city=False)
        bal_chains = {tuple(l["train"] for l in p["legs"]) for p in r_bal["transfers"]}
        self.assertIn(("G10", "G20", "G30"), bal_chains)
        self.assertNotIn(("G10", "G20", "D21"), bal_chains)
        self.assertNotIn(("G10", "G20", "K99"), bal_chains)
        self.assertNotIn(("K101", "K102", "K103"), bal_chains)
        self.assertIn("station_cities", r_bal)
        self.assertIn("city_suffix_stations", r_bal)

        # 2) unbounded (frontier_k=0, max_total_min=0) 不触发 48h 剪枝，K101+K102+K103 保留
        r_unb = transfer.search("起点站", "终点站", "2026-11-20", min_wait=30, max_wait=120, max_legs=3, prune_level="unbounded", expand_city=False)
        unb_chains = {tuple(l["train"] for l in p["legs"]) for p in r_unb["transfers"]}
        self.assertIn(("K101", "K102", "K103"), unb_chains)
        self.assertEqual(r_unb["prune_config"]["frontier_k"], 0)
        self.assertEqual(r_unb["prune_config"]["max_total_min"], 0)

        # 3) hub_only=True 默认省会大枢纽：引擎 A 仅保留 南昌(G60+G61)，引擎 D 仅保留 北京+济南(G10+G20+G30)
        #    并跳过非枢纽“小站”相关的 2 程与 3 程换乘
        r_hub = transfer.search("起点站", "终点站", "2026-11-20", min_wait=30, max_wait=120, max_legs=3,
                                prune_level="unbounded", hub_only=True, expand_city=False)
        hub_chains = {tuple(l["train"] for l in p["legs"]) for p in r_hub["transfers"]}
        self.assertIn(("G60", "G61"), hub_chains)
        self.assertIn(("G10", "G20", "G30"), hub_chains)
        self.assertNotIn(("D50", "D51"), hub_chains)
        self.assertNotIn(("K101", "K102", "K103"), hub_chains)
        self.assertGreater(r_hub["prune_stats"]["hub_filter"], 0)

        # 自定义枢纽列表增删（只保留 北京,济南 且禁异站 -> 连 南昌 与 北京->北京南 也被排除，仅剩 G10+G20+G30）
        r_custom_hub = transfer.search("起点站", "终点站", "2026-11-20", min_wait=30, max_wait=120, max_legs=3,
                                       hub_only=True, hub_stations="北京,济南", allow_cross_station=False, expand_city=False)
        self.assertEqual({tuple(l["train"] for l in p["legs"]) for p in r_custom_hub["transfers"]},
                         {("G10", "G20", "G30")})

        # 4) 独立后置过滤 filter_plans()：无需重跑 search 即可按车型头和换乘站过滤
        post_g = transfer.filter_plans(r_unb["transfers"], allow_prefixes="G")
        self.assertEqual({tuple(l["train"] for l in p["legs"]) for p in post_g},
                         {("G60", "G61"), ("G10", "G20", "G30")})
        post_and = transfer.filter_plans(r_unb["transfers"], include_stations="北京,济南", include_stations_mode="and")
        self.assertEqual({tuple(l["train"] for l in p["legs"]) for p in post_and},
                         {("G10", "G20", "G30")})

        # 5) /api/hubs 与接口参数 unbounded(0) 支持
        hubs_res = main_mod.hubs()
        self.assertIn("北京", hubs_res["hubs"])
        self.assertIn("三明北", hubs_res["hubs"])
        api_res = main_mod.transfers("起点站", "终点站", "2026-11-20", max_legs=3,
                                     prune_level="unbounded", frontier_k=0, max_total_min=0,
                                     hub_only=True, hub_stations="南昌", expand_city=False)
        self.assertEqual([tuple(l["train"] for l in p["legs"]) for p in api_res["transfers"]],
                         [("G60", "G61")])


class TestNationwideTimetablesAndTicketsAPI(TempDBTestCase):
    def test_tickets_api_enriches_prices_with_zfill2_and_date_isolation(self):
        from app import main as main_mod

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [("BJP", "北京", "bj", "bj", "北京"), ("SHH", "上海", "sh", "sh", "上海")],
            )
            conn.execute(
                """INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,
                     from_station_no,to_station_no,start_time,arrive_time,duration,seats,can_web_buy)
                   VALUES('T_G1','G1','2026-11-21','BJP','SHH','1','5','08:00','12:30','04:30','{"ze":"有"}','Y')"""
            )
            conn.execute(
                """INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,
                     from_station_no,to_station_no,start_time,arrive_time,duration,seats,can_web_buy)
                   VALUES('T_G2','G2','2026-11-21','BJP','SHH','01','06','09:00','13:30','04:30','{"ze":"5"}','Y')"""
            )
            conn.executemany(
                """INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,seat_type,seat_name,price)
                   VALUES(?,?,?,?,?,?,?,?)""",
                [
                    ("T_G1", "G1", "2026-11-20", "01", "05", "ze", "二等座", 530.0),
                    ("T_G1", "G1", "2026-11-21", "01", "05", "ze", "二等座", 553.0),
                    ("T_G2", "G2", "2026-11-19", "01", "06", "ze", "二等座", 510.0),
                ],
            )

        res = main_mod.tickets("北京", "上海", "2026-11-21")
        self.assertEqual(len(res), 2)
        by_no = {r["train_no"]: r for r in res}
        # T_G1 站序 '1'->'01'/'5'->'05' 对齐且只取当日 2026-11-21 实测价
        self.assertEqual(by_no["T_G1"]["from_name"], "北京")
        self.assertEqual(by_no["T_G1"]["to_name"], "上海")
        self.assertEqual(len(by_no["T_G1"]["prices"]), 1)
        self.assertEqual(by_no["T_G1"]["prices"][0]["price"], 553.0)
        self.assertFalse(by_no["T_G1"]["price_stale"])
        # T_G2 无当日价时回退最近快照并标记 price_stale=True
        self.assertEqual(by_no["T_G2"]["prices"][0]["price"], 510.0)
        self.assertTrue(by_no["T_G2"]["price_stale"])

    def test_crawl_all_timetables_script_workflow_resume_and_ui_defaults(self):
        from datetime import timedelta
        import crawl_all_timetables as cat

        expected_tomorrow = (db.cn_today() + timedelta(days=1)).isoformat()
        self.assertEqual(cat.default_tomorrow(), expected_tomorrow)

        state_file = Path(self._tmp.name) / "cat_state.json"
        log_file = Path(self._tmp.name) / "cat.log"

        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("BJP", "北京", "bj", "bj", "北京"),
                    ("SHH", "上海", "sh", "sh", "上海"),
                    ("XHZ", "小站", "xz", "xz", "小市"),
                ],
            )
            # T_EXIST 已有完整时刻表，非 force 模式应自动跳过
            conn.execute(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) "
                "VALUES('T_EXIST','G999',?,1,'北京','BJP','----','07:00',1)",
                (expected_tomorrow,),
            )

        fail_once = {"SHH": True}
        seen_screens, seen_tts = [], []

        def fake_crawl_stations():
            return {"parsed": 3, "saved": 0}

        def fake_crawl_screen(tc, d):
            seen_screens.append((tc, d))
            if tc == "SHH" and fail_once.pop("SHH", False):
                raise RuntimeError("临时超时")
            if tc == "XHZ":
                raise cat.crawler.CrawlError("暂无数据") if hasattr(cat.crawler, "CrawlError") else RuntimeError("暂无数据")
            with db.tx() as conn:
                conn.executemany(
                    """INSERT OR REPLACE INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                         station_train_date,start_train_date,station_no,start_station_telecode,end_station_telecode)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    [
                        (tc, tc, d, "G101", "T_G101", d, d, 1, "BJP", "SHH"),
                        (tc, tc, d, "G999", "T_EXIST", d, d, 1, "BJP", "SHH"),
                    ],
                )
            return {"saved": 2}

        def fake_crawl_tt(tn, d, f, t):
            seen_tts.append((tn, d, f, t))
            with db.tx() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) "
                    "VALUES(?,?,?,1,'北京','BJP','----','08:00',1)",
                    (tn, "G101", d),
                )
            return {"saved": 1}

        with mock.patch.object(cat.crawler, "crawl_stations", side_effect=fake_crawl_stations), \
             mock.patch.object(cat.crawler, "crawl_station_trains", side_effect=fake_crawl_screen), \
             mock.patch.object(cat.crawler, "crawl_timetable", side_effect=fake_crawl_tt), \
             mock.patch.object(cat.crawler, "set_rate"):
            # 第1次运行：SHH 报临时超时进入 failed_stations；XHZ 暂无数据记为 done；T_EXIST 跳过；T_G101 抓取
            s1 = cat.run_crawl(
                ignore_maintenance=True,
                report_sec=999,
                state_path=state_file,
                log_path=log_file,
            )
            self.assertEqual(s1["date"], expected_tomorrow)
            self.assertEqual(s1["screens_crawled"], 2)
            self.assertEqual(s1["screens_failed"], 1)
            self.assertEqual(s1["timetables_crawled"], 1)
            self.assertEqual([x[0] for x in seen_tts], ["T_G101"])

            # 第2次运行（--retry-errors）：仅重抓上次失败的 SHH，其余已完成车站与已有时刻表均跳过
            seen_screens.clear()
            seen_tts.clear()
            s2 = cat.run_crawl(
                retry_errors=True,
                ignore_maintenance=True,
                report_sec=999,
                state_path=state_file,
                log_path=log_file,
            )
            self.assertEqual([x[0] for x in seen_screens], ["SHH"])
            self.assertEqual(s2["screens_crawled"], 1)
            self.assertEqual(s2["screens_failed"], 0)
            self.assertEqual(seen_tts, [])

        # 校验前端页面包含默认明天(+1天)与国铁 12306 席别网格及展开方法
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn("const tomorrow=new Date(Date.now()+8*3600e3+86400e3).toISOString().slice(0,10)", html_text)
        self.assertIn("SEAT_COLS", html_text)
        self.assertIn("priceDrawerHtml", html_text)
        self.assertIn("toggleTT", html_text)
        self.assertIn('id="tab-display"', html_text)
        self.assertIn("KNOWN_COEXIST_PAIRS", html_text)
        self.assertIn("'swz|tz'", html_text)
        self.assertIn("computeEffectiveColumns", html_text)
        self.assertIn("renderMobileLegHtml", html_text)
        self.assertIn("tt-track", html_text)
        self.assertIn("actualDurTxt", html_text)

    def test_job_status_stage_local_eta_and_frozen_elapsed(self):
        from app import main as main_mod

        fake_now = [1000.0]
        with mock.patch.object(main_mod.time, "time", side_effect=lambda: fake_now[0]):
            # 1) 搜索任务：前 4 步仅用 0.1s，第 5 步 BFS 进入重循环后不得用前 4 步外推全局 eta_sec
            jid, _ = main_mod._create_job({
                "status": "running", "phase": "search", "done": 0, "total": 7,
                "sub_done": 0, "sub_total": 0, "current": "", "errors": [], "result": {},
            })
            fake_now[0] = 1000.1
            main_mod._job_update(jid, done=5, total=7, sub_done=0, sub_total=2000, current="BFS 2/3")
            st0 = main_mod.job_status(jid)
            self.assertEqual(st0["elapsed_sec"], 0.1)
            self.assertNotIn("eta_sec", st0)
            self.assertNotIn("sub_eta_sec", st0)

            # 第 5 步跑了 10s 完成 500/2000 -> 阶段局部 sub_eta_sec 应约为 30.0s
            fake_now[0] = 1010.1
            main_mod._job_update(jid, sub_done=500, sub_total=2000)
            st1 = main_mod.job_status(jid)
            self.assertEqual(st1["elapsed_sec"], 10.1)
            self.assertAlmostEqual(st1["sub_eta_sec"], 30.0, places=1)
            self.assertNotIn("eta_sec", st1)

            # 完成后 elapsed_sec 冻结在 finish 时刻，不再随后续时间推进变化
            fake_now[0] = 1015.5
            main_mod._job_update(jid, status="done", phase="done", done=7, total=7, sub_done=0, sub_total=0)
            fake_now[0] = 1099.0
            st_done = main_mod.job_status(jid)
            self.assertEqual(st_done["status"], "done")
            self.assertEqual(st_done["elapsed_sec"], 15.5)

            # 2) 多阶段抓包任务：切换 phase 后重置阶段测速基准，不被前序轻量阶段污染
            fake_now[0] = 2000.0
            jid2, _ = main_mod._create_job({
                "status": "running", "phase": "screen", "done": 0, "total": 2,
                "current": "", "errors": [], "result": {},
            })
            fake_now[0] = 2000.2
            main_mod._job_update(jid2, done=2)
            # 切入慢速 price 阶段（2 条已完成耗时 4.0s，还剩 6 条 -> 约 12.0s）
            main_mod._job_update(jid2, phase="price", total=10)
            fake_now[0] = 2004.2
            main_mod._job_update(jid2, done=4)
            st_p = main_mod.job_status(jid2)
            self.assertAlmostEqual(st_p["eta_sec"], 12.0, places=1)
            fake_now[0] = 2006.0
            main_mod.cancel_job(jid2)
            fake_now[0] = 2050.0
            st_can = main_mod.job_status(jid2)
            self.assertEqual(st_can["status"], "cancelled")
            self.assertAlmostEqual(st_can["elapsed_sec"], 6.0, places=1)

    def test_station_selection_modes_autocomplete_and_online_route_mode(self):
        from app import main as main_mod

        text = (SAMPLES / "station_name.js").read_text(encoding="utf-8")
        importer.ingest(text, "stations", source="test")

        # 1) 空串不误匹配 city=''，single 不拆分逗号、不扩城市、不换站
        self.assertEqual(db.resolve_stations("", city_mode="name"), [])
        self.assertEqual(db.resolve_stations("", city_mode="all"), [])
        self.assertEqual([s["name"] for s in db.resolve_stations("武汉", city_mode="single")], ["武汉"])
        self.assertEqual(db.resolve_stations("南平", city_mode="single"), [])
        self.assertEqual(db.resolve_stations("厦门，厦门北", city_mode="single"), [])

        # 2) name 档修复同名县级站碰撞（建德/上杭/兴安）与常规城市前缀展开
        self.assertEqual([s["name"] for s in db.resolve_stations("南平", city_mode="name")], ["南平市"])
        jd_names = [s["name"] for s in db.resolve_stations("建德", city_mode="name")]
        self.assertEqual(jd_names[0], "建德")
        self.assertIn("建德南", jd_names)
        self.assertEqual([s["name"] for s in db.resolve_stations("上杭", city_mode="name")], ["上杭"])
        self.assertEqual([s["name"] for s in db.resolve_stations("兴安", city_mode="name")], ["兴安"])

        # 3) string 档：有冒号未命中不得回退单站；空城市不得匹配空 city
        self.assertEqual(
            [s["name"] for s in db.resolve_stations("南平:延平", city_mode="string")],
            ["延平", "延平东", "延平西"],
        )
        self.assertEqual(db.resolve_stations("北京:不存在", city_mode="string"), [])
        self.assertEqual(db.resolve_stations(":延平", city_mode="string"), [])
        with self.assertRaises(ValueError) as ctx_str:
            transfer.search("北京:不存在", "上海", "2026-11-21", city_mode="string")
        self.assertIn("未找到站名含“不存在”", str(ctx_str.exception))

        # 4) mixed 档：无冒号 token 仅精确单站取并集，显式冒号支持混合城市/字样（如 name:上海、南平:延平）；任一未知 token 整体失败
        self.assertEqual(
            [s["name"] for s in db.resolve_stations("厦门，延平", city_mode="mixed")],
            ["厦门", "延平"],
        )
        mix_combo = [s["name"] for s in db.resolve_stations("延平，name:厦门", city_mode="mixed")]
        self.assertEqual(mix_combo[:2], ["延平", "厦门"])
        self.assertIn("厦门北", mix_combo)
        self.assertEqual(db.resolve_stations("南平，厦门", city_mode="mixed"), [])
        with self.assertRaises(ValueError) as ctx_mix:
            transfer.search("南平，厦门", "上海", "2026-11-21", city_mode="mixed")
        self.assertIn("南平", str(ctx_mix.exception))

        # 5) all 档：全市含郊县站
        qz_all = {s["name"] for s in db.resolve_stations("泉州", city_mode="all")}
        for expected in ("泉州", "泉州东", "晋江", "惠安", "德化"):
            self.assertIn(expected, qz_all)

        # 6) /api/stations：简拼排序优先、only_cities=1 与城市排序（sh→上海优先、xm→厦门、bj→北京）
        st_bj = main_mod.stations(q="bj", limit=8, include_cities=False)
        self.assertIn(st_bj[0]["name"], ("北京", "宝鸡"))
        st_sh_only = main_mod.stations(q="sh", limit=8, only_cities=True)
        self.assertEqual(st_sh_only[0]["name"], "上海")
        self.assertEqual(st_sh_only[0]["kind"], "city")
        st_xm_cities = main_mod.stations(q="xm", limit=10, include_cities=True)
        self.assertTrue(any(x.get("kind") == "city" and x["name"] == "厦门" for x in st_xm_cities))
        st_np_cities = main_mod.stations(q="np", limit=10, include_cities=True)
        self.assertTrue(any(x.get("kind") == "city" and x["name"] == "南平" for x in st_np_cities))

        # 7) /api/stations/resolve 实时预览接口
        pv_ok = main_mod.resolve_stations_preview(q="南平:延平", city_mode="string")
        self.assertTrue(pv_ok["ok"])
        self.assertEqual([s["name"] for s in pv_ok["stations"]], ["延平", "延平东", "延平西"])
        pv_err = main_mod.resolve_stations_preview(q="南平", city_mode="single")
        self.assertFalse(pv_err["ok"])
        self.assertIn("仅该站", pv_err["error"])
        pv_blank = main_mod.resolve_stations_preview(q="", city_mode="name")
        self.assertFalse(pv_blank["ok"])
        self.assertFalse(pv_blank["empty_db"])

        # 8) 两端独立口径（from_city_mode != to_city_mode）透传到 search / /api/transfers / /api/transfers/start / /api/import/route
        sr_asym = transfer.search("延平", "厦门", "2026-11-21", from_city_mode="single", to_city_mode="name")
        self.assertEqual(sr_asym["from_stations"], ["延平"])
        self.assertEqual(set(sr_asym["to_stations"]), {"厦门", "厦门北", "厦门高崎"})
        api_asym = main_mod.transfers("南平:延平", "泉州", "2026-11-21", from_city_mode="string", to_city_mode="all")
        self.assertEqual(set(api_asym["from_stations"]), {"延平", "延平东", "延平西"})
        self.assertIn("晋江", api_asym["to_stations"])

        with mock.patch("threading.Thread") as m_thr:
            m_thr.return_value.start = lambda: None
            st_job = main_mod.start_transfers(main_mod.TransferSearchReq(
                from_station="延平", to_station="厦门", date="2026-11-21",
                from_city_mode="single", to_city_mode="name",
            ))
            self.assertIn("job_id", st_job)

        # 9) 在线导入 /api/import/route 与 _route_job 支持独立 from_city_mode / to_city_mode
        with self.assertRaises(main_mod.HTTPException) as ctx_bad:
            main_mod.import_route(main_mod.RouteImportReq(
                from_city="厦门", to_city="上海", date="2026-11-21", deep=False, city_mode="bad",
            ))
        self.assertEqual(ctx_bad.exception.status_code, 400)

        seen_screens, seen_tickets = [], []
        with mock.patch.object(main_mod.crawler, "crawl_station_trains",
                               side_effect=lambda tc, d: seen_screens.append(tc) or {"parsed": 1}), \
             mock.patch.object(main_mod.crawler, "crawl_tickets",
                               side_effect=lambda f, t, d: seen_tickets.append((f, t)) or {"saved": 0}), \
             mock.patch("threading.Thread") as m_thr:
            m_thr.return_value.start = lambda: None
            res = main_mod.import_route(main_mod.RouteImportReq(
                from_city="南平:延平", to_city="厦门", date="2026-11-21", deep=False,
                from_city_mode="string", to_city_mode="name",
            ))
            self.assertEqual(set(res["from_stations"]), {"延平", "延平东", "延平西"})
            self.assertEqual(set(res["to_stations"]), {"厦门", "厦门北", "厦门高崎"})
            jid = res["job_id"]
            ev = main_mod._job_cancels[jid]
            main_mod._route_job(jid, ev, "南平:延平", "厦门", "2026-11-21", False, None, "string", "name")
        self.assertEqual(set(seen_screens), {"ADS", "YPS", "YWS", "XMS", "XKS", "XBS"})
        self.assertEqual(len(seen_tickets), 9)

        # 10) 前端静态校验：两端独立选择器、芯片多选、实时预览、无旧 #s-city 与 station-ac 冲突
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="s-from-mode"', html_text)
        self.assertIn('id="s-to-mode"', html_text)
        self.assertNotIn('id="s-city"', html_text)
        self.assertNotIn('id="s-from" class="station-ac"', html_text)
        self.assertIn("initSidePicker", html_text)
        self.assertIn("addMixedStations", html_text)
        self.assertIn("/api/stations/resolve", html_text)
        self.assertIn("only_cities=1", html_text)
        self.assertIn("from_city_mode:fcm,to_city_mode:tcm", html_text)


class TestSchemeCv2(TempDBTestCase):
    def _seed_station(self, tc: str, name: str, city: str, *, lat=None, lon=None,
                      status: str = "missing", zone: str = "", rdist=None,
                      rmode: str | None = None, hub: int = 0, src: str = "manual"):
        with db.tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO stations(telecode,name,city,pinyin,short) VALUES(?,?,?,?,?)",
                (tc, name, city, tc.lower(), tc.lower()),
            )
        if lat is not None and lon is not None:
            db.update_station_coords(
                tc, lat, lon, source=src, status=status, confidence=0.95,
                transit_zone_id=zone if zone else None,
                nearest_rail_stop_distance_m=rdist if rdist is not None else -1,
                rail_access_mode=rmode,
                is_major_hub=bool(hub),
            )

    def _add_service(self, train_no: str, code: str, stops: list[tuple[str, str, str, str]]):
        """stops: [(telecode, name, arr, dep), ...]"""
        with db.tx() as conn:
            for idx, (tc, nm, arr, dep) in enumerate(stops, 1):
                conn.execute(
                    """INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,
                                               station_name,telecode,arrive_time,start_time,enabled)
                       VALUES(?,?,?,?,?,?,?,?,1)""",
                    (train_no, code, "2026-11-21", idx, nm, tc, arr, dep),
                )

    def test_five_locked_benchmark_pairs(self):
        from app import geo

        # 1) 上海虹桥 — 上海南 (~10.5km, SH_KS, 双端 walk, 虹桥枢纽 +5)
        self._seed_station("AOH", "上海虹桥", "上海", lat=31.1942, lon=121.3189, status="verified", zone="SH_KS", rdist=180, hub=1)
        self._seed_station("SNH", "上海南", "上海", lat=31.1544, lon=121.4293, status="verified", zone="SH_KS", rdist=150, hub=1)
        # 2) 深圳北 — 深圳 (~11.9km, 深圳, 双端 walk)
        self._seed_station("IOQ", "深圳北", "深圳", lat=22.6097, lon=114.0294, status="verified", zone="深圳", rdist=120, hub=1)
        self._seed_station("SZQ", "深圳", "深圳", lat=22.5326, lon=114.1173, status="verified", zone="深圳", rdist=160, hub=1)
        # 3) 昆山南 — 安亭北 (~19.8km, 同属 SH_KS 圈群，但双端距11号线>1500m，不触发减时)
        self._seed_station("KNH", "昆山南", "苏州", lat=31.3529, lon=120.9539, status="verified", zone="SH_KS", rdist=3400, hub=0)
        self._seed_station("ASH", "安亭北", "上海", lat=31.3064, lon=121.1544, status="verified", zone="SH_KS", rdist=2400, hub=0)
        # 4) 廊坊 — 北京南 (~50km, 京津排除跨市城轨减时 + 廊坊无重轨邻近)
        self._seed_station("LJP", "廊坊", "廊坊", lat=39.5094, lon=116.7042, status="verified", zone="廊坊", rdist=None, hub=0)
        self._seed_station("VNP", "北京南", "北京", lat=39.8651, lon=116.3789, status="verified", zone="北京", rdist=140, hub=1)
        # 5) 万州北 — 重庆北 (~245km, 同写重庆市但物理相距遥远)
        self._seed_station("WZB", "万州北", "重庆", lat=30.8586, lon=108.3992, status="verified", zone="万州", rdist=None, hub=0)
        self._seed_station("CUW", "重庆北", "重庆", lat=29.6095, lon=106.5495, status="verified", zone="重庆", rdist=150, hub=1)

        st_rows = db.rows("SELECT * FROM stations")
        idx = geo.StationGeoIndex(st_rows, [], [], geo.load_transit_cities())
        cfg20 = geo.resolve_cross_cfg(cross_radius_km=20.0, fallback_ungeocoded=False, geo_index=idx)

        # Case 1: 上海虹桥 — 上海南
        e1 = geo.eval_cross_pair("AOH", "SNH", idx, cfg20)
        self.assertIsNotNone(e1)
        self.assertEqual(e1["source_rule"], "radius")
        self.assertEqual(e1["rail_access_mode"], "walk")
        self.assertEqual(e1["hub_extra"], 5)
        self.assertEqual(e1["transit_discount"], 15)
        self.assertEqual(e1["base_extra"] + e1["hub_extra"] - e1["transit_discount"], e1["effective_extra"])
        self.assertGreaterEqual(e1["effective_extra"], 20)
        self.assertIn("估算", e1["transit_reason"])

        # Case 2: 深圳北 — 深圳
        e2 = geo.eval_cross_pair("IOQ", "SZQ", idx, cfg20)
        self.assertIsNotNone(e2)
        self.assertEqual(e2["source_rule"], "radius")
        self.assertEqual(e2["rail_access_mode"], "walk")
        self.assertEqual(e2["transit_discount"], 15)
        self.assertGreaterEqual(e2["effective_extra"], 20)

        # Case 3: 昆山南 — 安亭北（反向邻近度拒绝测试）
        e3 = geo.eval_cross_pair("KNH", "ASH", idx, cfg20)
        self.assertIsNotNone(e3)
        self.assertEqual(e3["source_rule"], "radius")
        self.assertEqual(e3["rail_access_mode"], "long_bus_or_taxi")
        self.assertEqual(e3["transit_discount"], 0)
        self.assertEqual(e3["effective_extra"], e3["base_extra"])
        self.assertIn("邻近度不满足", e3["transit_reason"])

        # Case 4: 廊坊 — 北京南（R=60km 下连通，但京津不享跨市城轨减时）
        cfg60 = geo.resolve_cross_cfg(cross_radius_km=60.0, fallback_ungeocoded=False, geo_index=idx)
        e4 = geo.eval_cross_pair("LJP", "VNP", idx, cfg60)
        self.assertIsNotNone(e4)
        self.assertEqual(e4["source_rule"], "radius")
        self.assertEqual(e4["transit_discount"], 0)
        self.assertEqual(e4["effective_extra"], e4["base_extra"] + e4["hub_extra"])

        # Case 5: 万州北 — 重庆北（默认 fallback=False 且 R=100km 下绝不误连）
        cfg100 = geo.resolve_cross_cfg(cross_radius_km=100.0, fallback_ungeocoded=False, geo_index=idx)
        self.assertIsNone(geo.eval_cross_pair("WZB", "CUW", idx, cfg100))

        # 并验证万州北→重庆北仍可通过万州北→达州→重庆北（同站铁路换乘）联通而不产生 fallback 边
        self._seed_station("DZW", "达州", "达州", lat=31.2100, lon=107.4700, status="verified")
        self._add_service("T_WD", "G101", [("WZB", "万州北", "08:00", "08:00"), ("DZW", "达州", "08:40", "08:45")])
        self._add_service("T_DC", "G102", [("DZW", "达州", "09:30", "09:30"), ("CUW", "重庆北", "10:40", "10:40")])
        res = transfer.search("万州北", "重庆北", "2026-11-21", cross_radius_km=20.0, fallback_ungeocoded=False)
        self.assertTrue(any(p["transfers"][0]["same"] for p in res["transfers"]))
        self.assertFalse(any(tr.get("is_fallback") for p in res["transfers"] for tr in p["transfers"]))

    def test_monotonicity_and_arithmetic_identity(self):
        from app import geo

        for hub in (0, 5):
            prev = -1
            for step in range(1, 2001):
                d = step * 0.05  # 0.05..100.0 km
                c = geo.compute_smart_extra(d, hub_extra=hub, raw_discount_cap=15)
                self.assertGreaterEqual(c["effective_extra"], prev, f"非单调 d={d}, hub={hub}")
                self.assertGreaterEqual(c["effective_extra"], geo.MIN_SAFE_CROSS_EXTRA)
                self.assertEqual(
                    c["base_extra"] + c["hub_extra"] - c["transit_discount"],
                    c["effective_extra"],
                    f"账本等式破坏 d={d}: {c}",
                )
                prev = c["effective_extra"]

        # 验证半开分档边界连续且无倒挂 (2.9km vs 3.1km with discount cap 15)
        v29 = geo.compute_smart_extra(2.9, hub_extra=0, raw_discount_cap=15)
        v31 = geo.compute_smart_extra(3.1, hub_extra=0, raw_discount_cap=15)
        self.assertGreaterEqual(v31["effective_extra"], v29["effective_extra"])

    def test_five_tier_precedence_groups_and_validation(self):
        from app import geo, main as main_mod

        # 两站相距 ~8km，均步行直连地铁且同城（radius 本应给减时）
        self._seed_station("STA", "甲站", "测试市", lat=31.200, lon=121.400, status="verified", zone="测试市", rdist=100)
        self._seed_station("STB", "乙站", "测试市", lat=31.270, lon=121.400, status="verified", zone="测试市", rdist=100)
        self._seed_station("STC", "丙站", "测试市", lat=31.250, lon=121.460, status="verified", zone="测试市", rdist=100)
        self._seed_station("STD", "丁站", "测试市", lat=None, lon=None, status="missing")

        # 组约束检验：>30 失败；full_mesh < 5 失败；hub_star 无组内枢纽失败
        too_many = [f"X{i:02d}" for i in range(31)]
        for tc in too_many:
            self._seed_station(tc, f"站{tc}", "测城")
        with self.assertRaises(ValueError):
            db.save_transfer_group("超大组", too_many, default_extra_min=30, connect_mode="full_mesh")
        with self.assertRaises(ValueError):
            db.save_transfer_group("太短组", ["STA", "STB"], default_extra_min=4, connect_mode="full_mesh")
        with self.assertRaises(ValueError):
            db.save_transfer_group("缺枢纽", ["STA", "STB", "STC"], default_extra_min=20, connect_mode="hub_star", hub_telecode="STD")

        # 有效 hub_star 组：核心 STA 连通 STB/STC，但 STB-STC 不直接组连接
        grp = db.save_transfer_group("星型组", ["STA", "STB", "STC"], default_extra_min=38, connect_mode="hub_star", hub_telecode="STA")
        gid = grp["id"]

        # 未加 pair override 时，STA-STB 命中 group(38)，不叠加城轨减时
        idx = geo.StationGeoIndex(db.rows("SELECT * FROM stations"), db.list_transfer_groups(), db.list_transfer_pair_overrides(), geo.load_transit_cities())
        cfg = geo.resolve_cross_cfg(cross_radius_km=20.0, active_group_ids=[gid], fallback_ungeocoded=True, geo_index=idx)
        eg = geo.eval_cross_pair("STA", "STB", idx, cfg)
        self.assertEqual(eg["source_rule"], "group")
        self.assertEqual(eg["effective_extra"], 38)
        self.assertEqual(eg["transit_discount"], 0)
        # STB-STC 在 R=0 下因 hub_star 不互相连通
        cfg_r0 = geo.resolve_cross_cfg(cross_radius_km=0.0, active_group_ids=[gid], fallback_ungeocoded=False, geo_index=idx)
        self.assertIsNone(geo.eval_cross_pair("STB", "STC", idx, cfg_r0))

        # Allow override (0m) 压过 group，不叠加减时
        p_allow = db.save_transfer_pair_override("STA", "STB", "allow", custom_extra_min=0, note="地下同厅极速")
        idx2 = geo.StationGeoIndex(db.rows("SELECT * FROM stations"), db.list_transfer_groups(), db.list_transfer_pair_overrides(), geo.load_transit_cities())
        eo = geo.eval_cross_pair("STA", "STB", idx2, cfg)
        self.assertEqual(eo["source_rule"], "override")
        self.assertEqual(eo["effective_extra"], 0)
        self.assertEqual(eo["transit_discount"], 0)
        self.assertTrue(idx2.has_zero_override)

        # Block override 压过一切（含 allow/group/radius/fallback）
        db.save_transfer_pair_override("STA", "STB", "block", note="施工禁换")
        idx3 = geo.StationGeoIndex(db.rows("SELECT * FROM stations"), db.list_transfer_groups(), db.list_transfer_pair_overrides(), geo.load_transit_cities())
        self.assertIsNone(geo.eval_cross_pair("STA", "STB", idx3, cfg))
        db.delete_transfer_pair_override(p_allow["id"])

        # Fallback tier (90m) 仅在缺失坐标且同城且 fallback_ungeocoded=True 时生效
        ef = geo.eval_cross_pair("STA", "STD", idx, cfg)
        self.assertEqual(ef["source_rule"], "fallback")
        self.assertEqual(ef["effective_extra"], 90)
        self.assertTrue(ef["is_fallback"])
        cfg_nofb = geo.resolve_cross_cfg(cross_radius_km=20.0, active_group_ids=[], fallback_ungeocoded=False, geo_index=idx)
        self.assertIsNone(geo.eval_cross_pair("STA", "STD", idx, cfg_nofb))

        # HTTP 校验：R=0 且 fallback_ungeocoded=True 返回 400
        with self.assertRaises(main_mod.HTTPException) as ctx1:
            main_mod.transfers("STA", "STB", "2026-11-21", cross_radius_km=0.0, fallback_ungeocoded=True)
        self.assertEqual(ctx1.exception.status_code, 400)
        with self.assertRaises(main_mod.HTTPException) as ctx2:
            main_mod.start_transfers(main_mod.TransferSearchReq(
                from_station="STA", to_station="STB", date="2026-11-21", cross_radius_km=0.0, fallback_ungeocoded=True
            ))
        self.assertEqual(ctx2.exception.status_code, 400)

    def test_high_low_latitude_grid_accuracy_and_top25_truncation(self):
        from app import geo

        # 高纬度 (50°N) 与低纬度 (20°N) 集群，检验格网邻域绝不漏查边界点
        anchors = [("HI0", 50.1, 127.5, "黑河"), ("LO0", 20.02, 110.35, "海口")]
        for tc, lat, lon, ct in anchors:
            self._seed_station(tc, f"中心{tc}", ct, lat=lat, lon=lon, status="verified")
        pts = [
            # 50°N 周向不同方位 8km..98km 点
            ("HI1", 50.1, 127.62, "黑河"),
            ("HI2", 50.1, 128.85, "黑河"),  # ~96km
            ("HI3", 50.95, 127.5, "黑河"),  # ~94.5km
            ("HI4", 50.70, 128.60, "黑河"),
            # 20°N
            ("LO1", 20.02, 110.50, "海口"),
            ("LO2", 20.85, 110.35, "海口"),  # ~92km
            ("LO3", 20.60, 111.05, "海口"),  # ~97km
        ]
        for tc, lat, lon, ct in pts:
            self._seed_station(tc, f"站{tc}", ct, lat=lat, lon=lon, status="verified")

        st_rows = db.rows("SELECT * FROM stations")
        idx = geo.StationGeoIndex(st_rows, [], [], geo.load_transit_cities())
        for R in (10.0, 20.0, 50.0, 100.0):
            for center_tc, c_lat, c_lon, _ in anchors:
                expected = sorted(
                    r["telecode"]
                    for r in st_rows
                    if r["telecode"] != center_tc and geo.haversine_km(c_lat, c_lon, r["lat"], r["lon"]) <= R
                )
                got = sorted(nb for nb, d in idx.max_neighbors.get(center_tc, ()) if d <= R)
                self.assertEqual(got, expected, f"纬度格网漏检 center={center_tc}, R={R}")

        # 单站物理邻域 > 25 时触发 Top-25 截断与 radius_truncated 标志
        for i in range(28):
            tc = f"N{i:02d}"
            self._seed_station(tc, f"密集{i}", "密城", lat=31.20 + i * 0.004, lon=121.40 + (i % 3) * 0.003, status="verified")
        idx_dense = geo.StationGeoIndex(db.rows("SELECT * FROM stations"), [], [], geo.load_transit_cities())
        avail = {r["telecode"] for r in db.rows("SELECT telecode FROM stations")}
        cities = ({}, {}, {r["telecode"]: r["telecode"] for r in db.rows("SELECT telecode FROM stations")})
        cfg = geo.resolve_cross_cfg(cross_radius_km=25.0, geo_index=idx_dense)
        trunc = [False]
        cands = geo.cross_candidates("N00", "密集0", avail, cities, idx_dense, cfg, truncation_out=trunc)
        self.assertTrue(trunc[0])
        # same(1) + Top-25 radius = 26
        self.assertEqual(len(cands), 26)

    def test_random_cyclic_graph_multi_leg_parity_and_city_loop(self):
        from app import geo

        # 1) 真实跨市接驳与绕城检测：昆山南→安亭北→上海、廊坊→北京南→上海、咸阳→西安→北京
        self._seed_station("KNH", "昆山南", "苏州", lat=31.3529, lon=120.9539, status="verified", zone="SH_KS", rdist=3400)
        self._seed_station("ASH", "安亭北", "上海", lat=31.3064, lon=121.1544, status="verified", zone="SH_KS", rdist=2400)
        self._seed_station("SNQ", "上海西", "上海", lat=31.263, lon=121.401, status="verified")
        self._seed_station("SHH", "上海", "上海", lat=31.249, lon=121.455, status="verified")
        self._seed_station("LJP", "廊坊", "廊坊", lat=39.5094, lon=116.7042, status="verified")
        self._seed_station("VNP", "北京南", "北京", lat=39.8651, lon=116.3789, status="verified", rdist=140)
        self._seed_station("BXP", "北京西", "北京", lat=39.8949, lon=116.3212, status="verified")
        self._seed_station("SZP", "苏州", "苏州", lat=31.33, lon=120.61, status="verified")
        self._seed_station("XAY", "咸阳", "咸阳", lat=34.33, lon=108.70, status="verified", zone="XA_XY", rdist=300)
        self._seed_station("XAYN", "西安", "西安", lat=34.277, lon=108.963, status="verified", zone="XA_XY", rdist=250)
        self._seed_station("BJP", "北京", "北京", lat=39.902, lon=116.427, status="verified")
        self._seed_station("CDW", "成都", "成都", lat=30.69, lon=104.07, status="verified")

        # 合法跨市：昆山南 -> (安亭北跨站) -> 上海西 -> 上海
        self._add_service("TK1", "G701", [("SZP", "苏州", "08:00", "08:00"), ("KNH", "昆山南", "08:15", "08:17")])
        self._add_service("TK2", "G702", [("ASH", "安亭北", "09:55", "09:55"), ("SNQ", "上海西", "10:12", "10:14"), ("SHH", "上海", "10:25", "10:25")])
        r_ka = transfer.search("苏州", "上海", "2026-11-21", from_city_mode="single", to_city_mode="single", cross_radius_km=20.0)
        self.assertTrue(any(p["legs"][0]["to"] == "昆山南" and p["legs"][1]["from"] == "安亭北" for p in r_ka["transfers"]))

        # 廊坊 -> 北京南 -> 上海
        self._add_service("TL1", "G801", [("LJP", "廊坊", "07:00", "07:00"), ("BXP", "北京西", "07:25", "07:25")])
        self._add_service("TL2", "G802", [("VNP", "北京南", "09:30", "09:30"), ("SHH", "上海", "14:00", "14:00")])
        r_lb = transfer.search("廊坊", "上海", "2026-11-21", from_city_mode="single", to_city_mode="single", cross_radius_km=20.0)
        self.assertTrue(any(p["legs"][0]["to"] == "北京西" and p["legs"][1]["from"] == "北京南" for p in r_lb["transfers"]))

        # 咸阳 -> 西安 -> 北京
        self._add_service("TX1", "C901", [("XAY", "咸阳", "08:00", "08:00"), ("CDW", "成都", "12:00", "12:00")])
        self._add_service("TX2", "G902", [("XAYN", "西安", "09:30", "09:30"), ("BJP", "北京", "14:30", "14:30")])
        # 咸阳 -> 成都 车次不该连上，造 咸阳 -> 西安南(同城) 或 咸阳(上)直接西安跨站：建 宝鸡->咸阳 & 西安->北京
        self._seed_station("BJY", "宝鸡", "宝鸡", lat=34.36, lon=107.14, status="verified")
        self._add_service("TX0", "K900", [("BJY", "宝鸡", "06:30", "06:30"), ("XAY", "咸阳", "07:50", "07:50")])
        r_xx = transfer.search("宝鸡", "北京", "2026-11-21", from_city_mode="single", to_city_mode="single", cross_radius_km=30.0)
        self.assertTrue(any(p["legs"][0]["to"] == "咸阳" and p["legs"][1]["from"] == "西安" for p in r_xx["transfers"]))

        # 2) 随机有环小图暴力 DFS vs 3-4程无界 BFS 一致性校验（含 0m 覆盖、换乘组与跨市边，严防误剪枝漏解）
        nodes = [
            ("G_S", "起点站", "城1", 31.00, 121.00),
            ("G_A", "中转A", "城2", 31.05, 121.00),
            ("G_B", "中转B", "城3", 31.08, 121.02),
            ("G_C", "中转C", "城2", 31.12, 121.05),
            ("G_D", "中转D", "城4", 31.18, 121.10),
            ("G_T", "终点站", "城5", 31.40, 121.40),
        ]
        for tc, nm, ct, lt, ln in nodes:
            self._seed_station(tc, nm, ct, lat=lt, lon=ln, status="verified")
        # 注入 0 分钟 override 与换乘组，检验 min_lb 不高估
        db.save_transfer_pair_override("G_A", "G_B", "allow", custom_extra_min=0)
        grp = db.save_transfer_group("小图组", ["G_B", "G_C"], default_extra_min=15, connect_mode="full_mesh")
        # 构建有环服务（时刻单调、允许跨日）
        services = [
            ("MV1", "M1", [("G_S", "起点站", "08:00", "08:00"), ("G_A", "中转A", "08:30", "08:35"), ("G_C", "中转C", "08:50", "08:50")]),
            ("MV2", "M2", [("G_B", "中转B", "09:05", "09:05"), ("G_C", "中转C", "09:20", "09:25"), ("G_D", "中转D", "09:50", "09:50")]),
            ("MV3", "M3", [("G_C", "中转C", "10:15", "10:15"), ("G_D", "中转D", "10:40", "10:45"), ("G_T", "终点站", "11:30", "11:30")]),
            ("MV4", "M4", [("G_D", "中转D", "11:20", "11:20"), ("G_T", "终点站", "12:00", "12:00")]),
            ("MV5", "M5", [("G_C", "中转C", "11:10", "11:10"), ("G_A", "中转A", "11:30", "11:30")]),  # 反向制造环
        ]
        for tn, code, stps in services:
            self._add_service(tn, code, stps)

        # 用 transfer.search (unbounded, max_legs=4, max_total_min=2000) 搜索
        bfs_res = transfer.search(
            "起点站", "终点站", "2026-11-21",
            from_city_mode="single", to_city_mode="single",
            max_legs=4, min_wait=30, max_wait=180,
            cross_radius_km=12.0, active_group_ids=[grp["id"]],
            prune_level="unbounded", max_total_min=240,
        )
        # 紧 max_total_min=240 恰好测试 0m override 下 min_lb 不可误杀 3/4程紧致方案 (08:00 -> 11:30 = 210m, 08:00 -> 12:00 = 240m)
        self.assertTrue(bfs_res["zero_override_active"])
        combos = {tuple(l["train_no"] for l in p["legs"]) for p in bfs_res["transfers"]}
        # G_S --(MV1)--> G_A --(0m override, wait=35m>=30)--> G_B --(MV2)--> G_D --(MV4)--> G_T = 240m!
        self.assertIn(("MV1", "MV2", "MV4"), combos)
        # G_S --(MV1)--> G_A --(0m override)--> G_B --(MV2)--> G_C --(MV3)--> G_T = 210m!
        self.assertIn(("MV1", "MV2", "MV3"), combos)

    def test_scale_latency_crawler_audit_endpoints_and_ui(self):
        import time
        from app import geo, main as main_mod

        # 1) 3,400 站合成空间索引构造与单站 R=20/50/100 检索耗时验证
        st_synth = [
            {
                "telecode": f"S{i:04d}",
                "name": f"测试站{i}",
                "city": f"市{i // 10}",
                "lat": 20.0 + (i % 60) * 0.5,
                "lon": 100.0 + (i // 60) * 0.5,
                "coord_status": "ok",
                "coord_confidence": 0.9,
                "transit_zone_id": "",
                "nearest_rail_stop_distance_m": None,
                "rail_access_mode": "none",
                "is_major_hub": 0,
            }
            for i in range(3400)
        ]
        t0 = time.monotonic()
        idx_big = geo.StationGeoIndex(st_synth, [], [], geo.load_transit_cities())
        build_s = time.monotonic() - t0
        self.assertLess(build_s, 10.0)
        avail = {s["telecode"] for s in st_synth}
        cities = ({}, {}, {})
        t1 = time.monotonic()
        for rkm in (20.0, 50.0, 100.0):
            geo.cross_candidates("S0500", "测试站500", avail, cities, idx_big, geo.resolve_cross_cfg(cross_radius_km=rkm, geo_index=idx_big))
        self.assertLess(time.monotonic() - t1, 0.5)

        # 2) 合规抓取管道试跑(trial_tiles=3)、断点续跑(不重复抓取)及 Nominatim 手工 1.5s 限频
        self._seed_station("VNP", "北京南", "北京")
        self._seed_station("SHH", "上海", "上海")
        self._seed_station("BAD", "飞站", "北京")
        state_file = Path(self._tmp.name) / "geo_state.json"

        overpass_calls = []
        wikidata_calls = []

        def fake_http(url, *, params=None, data=None, headers=None, timeout=60):
            if "overpass" in url:
                overpass_calls.append(url)
                payload = {
                    "elements": [
                        {"type": "node", "lat": 39.865, "lon": 116.379, "tags": {"railway": "station", "ref:railway:telecode": "VNP", "name": "北京南站"}},
                        {"type": "node", "lat": 39.864, "lon": 116.378, "tags": {"railway": "station", "station": "subway", "name": "北京南站地铁", "addr:city": "北京"}},
                        # 有轨电车严格剔除
                        {"type": "node", "lat": 39.900, "lon": 116.400, "tags": {"railway": "tram_stop", "station": "tram", "name": "前门有轨"}},
                    ]
                }
                return json.dumps(payload).encode("utf-8")
            if "wikidata" in url:
                wikidata_calls.append(url)
                wd = {
                    "results": {
                        "bindings": [
                            {"stationLabel": {"value": "上海站"}, "coord": {"value": "Point(121.455 31.249)"}},
                        ]
                    }
                }
                return json.dumps(wd).encode("utf-8")
            if "nominatim" in url:
                return json.dumps([{"class": "railway", "type": "station", "lat": "39.865", "lon": "116.379", "display_name": "北京南站"}]).encode("utf-8")
            return b"{}"

        logs = []
        cr = geo.GeoCoordCrawler(state_path=state_file, http_fn=fake_http)
        trial_res = cr.run(trial_tiles=3, overpass_delay=0.0, log_cb=logs.append)
        self.assertEqual(trial_res["trial_tiles"], 3)
        self.assertEqual(trial_res["trial_requests"], 3)
        self.assertEqual(len(overpass_calls), 3)
        self.assertEqual(len(wikidata_calls), 0)
        self.assertTrue(any("试跑配额估算" in line for line in logs))
        # 验证有轨电车未被入库
        self.assertEqual(db.rows("SELECT COUNT(*) c FROM rail_transit_stops WHERE name='前门有轨'")[0]["c"], 0)
        self.assertGreaterEqual(db.rows("SELECT COUNT(*) c FROM rail_transit_stops WHERE name='北京南站地铁'")[0]["c"], 1)

        # 断点续跑全量：已完成的前 3 片不再重复请求 Overpass
        full_res = cr.run(trial_tiles=None, overpass_delay=0.0)
        self.assertEqual(len(overpass_calls), len(geo.CHINA_OVERPASS_TILES))
        self.assertEqual(len(wikidata_calls), 1)
        self.assertGreaterEqual(full_res["matched_wikidata"], 1)

        # 手工 Nominatim 冷却检查
        with mock.patch.object(geo, "_last_nom_ts", 0.0):
            nom_ok = geo.manual_nominatim_lookup("VNP", http_fn=fake_http)
            self.assertEqual(nom_ok["source"], "nominatim_manual")
            with self.assertRaises(ValueError) as nom_err:
                geo.manual_nominatim_lookup("SHH", http_fn=fake_http)
            self.assertIn("礼貌间隔", str(nom_err.exception))

        # 3) >420km/h 异常时速飞站检测 & China bbox / 同城离群隔离
        self._seed_station("A_ST", "起点A", "某市", lat=31.20, lon=121.40, status="ok", src="overpass")
        self._seed_station("B_ST", "正常B", "某市", lat=31.80, lon=121.40, status="ok", src="overpass")
        # 飞到 1500km 外的错误坐标且仅用 10 分钟到达 -> >9000km/h
        self._seed_station("BAD", "飞站", "北京", lat=23.10, lon=113.20, status="ok", src="overpass")
        self._add_service("SPD1", "G555", [("A_ST", "起点A", "08:00", "08:00"), ("B_ST", "正常B", "08:30", "08:35"), ("BAD", "飞站", "08:45", "08:45")])
        aud = geo.audit_station_coords()
        self.assertGreaterEqual(aud["suspect"], 1)
        bad_row = db.rows("SELECT coord_status, coord_issue FROM stations WHERE telecode='BAD'")[0]
        self.assertEqual(bad_row["coord_status"], "suspect")
        self.assertIn("speed_anomaly", bad_row["coord_issue"])
        # suspect 站禁止进入 radius 索引
        idx_aud = geo.StationGeoIndex(db.rows("SELECT * FROM stations"), [], [], geo.load_transit_cities())
        self.assertNotIn("BAD", idx_aud.coords)

        # 4) Fallback 量化统计 & auto_disable_recommended
        imp = geo.fallback_impact_stats(idx_aud)
        self.assertIn("fallback_removed_pairs", imp)
        self.assertIn("auto_disable_recommended", imp)

        # 5) 后端端点联调
        st_api = main_mod.geo_status()
        self.assertEqual(st_api["max_radius_km"], 100.0)
        st_list = main_mod.geo_stations(status="suspect")
        self.assertTrue(any(s["telecode"] == "BAD" for s in st_list["stations"]))
        upd = main_mod.geo_update_station("BAD", main_mod.GeoStationReq(lat=39.90, lon=116.40, coord_status="verified"))
        self.assertEqual(upd["station"]["coord_status"], "verified")
        aud_api = main_mod.geo_audit()
        self.assertIn("audit", aud_api)

        g_api = main_mod.save_transfer_group_api(main_mod.TransferGroupReq(
            name="HTTP测试组", members=["VNP", "SHH"], default_extra_min=45, connect_mode="full_mesh",
        ))
        self.assertEqual(g_api["group"]["name"], "HTTP测试组")
        p_api = main_mod.save_transfer_pair_api(main_mod.TransferPairReq(
            a_telecode="VNP", b_telecode="SHH", mode="allow", custom_extra_min=50, note="联调",
        ))
        self.assertEqual(p_api["pair"]["mode"], "allow")
        pv_api = main_mod.preview_transfer_pair(a="VNP", b="SHH", cross_radius_km=20.0)
        self.assertTrue(pv_api["connected"])
        self.assertEqual(pv_api["eval"]["source_rule"], "override")
        main_mod.delete_transfer_pair_api(p_api["pair"]["id"])
        main_mod.delete_transfer_group_api(g_api["group"]["id"])

        tc_res = main_mod.save_transit_city_api(main_mod.TransitCityReq(city="烟台", label="测试轻轨", enabled=True))
        self.assertTrue(any(c["city"] == "烟台" for c in tc_res["cities"]))
        tc_rst = main_mod.reset_transit_cities_api()
        self.assertFalse(any(c["city"] == "烟台" for c in tc_rst["cities"]))

        # 6) 旧库升级回归：模拟无 Scheme C 新增列的旧版 stations 表调用 init_db() 不崩溃
        import sqlite3 as _sqlite3
        leg_db = Path(self._tmp.name) / "legacy.db"
        c_leg = _sqlite3.connect(leg_db)
        c_leg.execute("CREATE TABLE stations(telecode TEXT PRIMARY KEY, name TEXT NOT NULL, pinyin TEXT, short TEXT, city TEXT)")
        c_leg.execute("INSERT INTO stations VALUES('BJP','北京','beijing','bj','北京')")
        c_leg.commit()
        c_leg.close()
        orig_p = db.DB_PATH
        try:
            db.close_conn()
            db.DB_PATH = leg_db
            db.init_db()
            migrated_cols = {r["name"] for r in db.rows("PRAGMA table_info(stations)")}
            for col in ("lat", "lon", "coord_status", "coord_grid", "transit_zone_id", "is_major_hub"):
                self.assertIn(col, migrated_cols)
        finally:
            db.close_conn()
            db.DB_PATH = orig_p

        # 7) 前端静态接线完整性校验
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        for marker in (
            'id="s-radius"', 'id="s-cross-mode"', 'id="s-transit-on"',
            'id="s-fallback"', 'id="s-groups-box"', 'id="s-hide-fallback"',
            'id="s-hide-suspect"', 'id="geo-card"', 'id="groups-card"',
            'id="pairs-card"', 'id="transit-card"', 'transferLedgerHtml',
            'id="s-cross-preview"',
            'id="tab-geo"', 'data-tab="geo"', 'id="s-summary-bar"',
            'id="s-mode-readout"', 'id="s-param-chips"', 'id="s-stale-banner"',
            'id="s-scope-seg"', 'id="scope-sheet"', 'id="pr-pv-radius"',
            'id="grp-member-add"', 'deriveSearchConfig', 'validateSearchConfig',
            'transferSearch:v2', 'transferRulesVersion:v1',
        ):
            self.assertIn(marker, html_text, f"前端缺失控件: {marker}")
        for removed in (
            'id="s-allow-cross"', 'id="s-same"', 'id="s-prune-toggle"',
            'scheduleCondResearch',
        ):
            self.assertNotIn(removed, html_text, f"前端残留已移除控件: {removed}")


class TestMemoryAndInteractiveTreePrune(TempDBTestCase):
    def _seed_multi_corridor(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("ORI", "起点站", "qdz", "qdz", "起市"),
                    ("MA1", "中甲站", "zjz", "zjz", "中甲市"),
                    ("MA2", "次甲站", "cjz", "cjz", "次甲市"),
                    ("MB1", "中乙站", "zyz", "zyz", "中乙市"),
                    ("MB2", "次乙站", "cyz", "cyz", "次乙市"),
                    ("DST", "终点站", "zdz", "zdz", "终市"),
                ],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # 路线 A (更快，经 中甲市 -> 中乙市): G101 -> G102 -> G103
                    ("TA1", "G101", "2026-11-21", 1, "起点站", "ORI", "----", "08:00"),
                    ("TA1", "G101", "2026-11-21", 2, "中甲站", "MA1", "09:00", "09:00"),
                    ("TA2", "G102", "2026-11-21", 1, "中甲站", "MA1", "----", "09:35"),
                    ("TA2", "G102", "2026-11-21", 2, "中乙站", "MB1", "10:35", "10:35"),
                    ("TA3", "G103", "2026-11-21", 1, "中乙站", "MB1", "----", "11:10"),
                    ("TA3", "G103", "2026-11-21", 2, "终点站", "DST", "12:00", "12:00"),
                    # 路线 B (稍慢，经 次甲市 -> 次乙市): G201 -> G202 -> G203
                    ("TB1", "G201", "2026-11-21", 1, "起点站", "ORI", "----", "08:10"),
                    ("TB1", "G201", "2026-11-21", 2, "次甲站", "MA2", "09:20", "09:20"),
                    ("TB2", "G202", "2026-11-21", 1, "次甲站", "MA2", "----", "10:00"),
                    ("TB2", "G202", "2026-11-21", 2, "次乙站", "MB2", "11:10", "11:10"),
                    ("TB3", "G203", "2026-11-21", 1, "次乙站", "MB2", "----", "11:50"),
                    ("TB3", "G203", "2026-11-21", 2, "终点站", "DST", "13:00", "13:00"),
                ],
            )
            conn.executemany(
                "INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,from_station_no,to_station_no,seat_types,seats,can_web_buy) VALUES(?,?,?,?,?,?,?,?,?,?)",
                [
                    ("TD_DIR", "D999", "2026-11-21", "ORI", "DST", "01", "02", "O", '{"ze":"有"}', "Y"),
                ],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("TD_DIR", "D999", "2026-11-21", 1, "起点站", "ORI", "----", "07:00"),
                    ("TD_DIR", "D999", "2026-11-21", 2, "终点站", "DST", "11:30", "11:30"),
                ],
            )

    def test_stations_memory_cache_and_topk_enrichment(self):
        self._seed_multi_corridor()
        # 首次调用预热站点内存缓存
        res1 = db.resolve_stations("起点站", True, "name")
        self.assertEqual([r["telecode"] for r in res1], ["ORI"])
        # 再次解析应 0 次 SQLite 查询
        with mock.patch.object(db, "rows", wraps=db.rows) as spy:
            res2 = db.resolve_stations("中甲市", True, "all")
            self.assertEqual([r["telecode"] for r in res2], ["MA1"])
            self.assertEqual(spy.call_count, 0)

        # 验证 Top-K 延迟丰富化：limit=2 时直达占 1 席 + 换乘占 1 席，且 transfers_total 仍准确记录 2 条
        sr_lim = transfer.search("起点站", "终点站", "2026-11-21", min_wait=30, max_wait=120, max_legs=3, limit=2, allow_cross_station=False)
        self.assertEqual([p["legs"][0]["train"] for p in sr_lim["direct"]], ["D999"])
        self.assertEqual(len(sr_lim["transfers"]), 1)
        self.assertEqual(sr_lim["transfers_total"], 2)
        self.assertEqual([l["train"] for l in sr_lim["transfers"][0]["legs"]], ["G101", "G102", "G103"])

    def test_step_and_live_tree_prune_and_result_eviction(self):
        import time
        from app import main as main_mod

        self._seed_multi_corridor()
        main_mod.jobs.clear()
        main_mod._job_cancels.clear()
        main_mod._job_trees.clear()

        # 1) 逐程暂停(step)模式：第1程结束暂停 -> 人工砍掉"中甲市" -> 内存前沿立即减少并唤醒完成
        req = main_mod.TransferSearchReq(
            from_station="起点站", to_station="终点站", date="2026-11-21",
            min_wait=30, max_wait=120, max_legs=3, allow_cross_station=False,
            tree_prune_mode="step",
        )
        start = main_mod.start_transfers(req)
        jid = start["job_id"]
        st = None
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st.get("tree_paused"):
                break
            time.sleep(0.05)
        self.assertTrue(st and st.get("tree_paused"), f"未触发逐程暂停: {st}")
        self.assertEqual(st.get("tree_paused_depth"), 1)
        snap = st.get("tree_snapshot") or {}
        top_cities = {n["city"] for n in snap.get("nodes", [])}
        self.assertIn("中甲市", top_cities)
        self.assertIn("次甲市", top_cities)

        pr_res = main_mod.prune_job_tree(jid, main_mod.TreePruneReq(prune_cities=["中甲市"], action="prune"))
        self.assertGreaterEqual(pr_res["dropped_now"], 1)
        self.assertGreaterEqual(pr_res["tree_pruned_count"], 1)
        self.assertNotIn("中甲市", {n["city"] for n in pr_res["tree_snapshot"]["nodes"]})

        main_mod.prune_job_tree(jid, main_mod.TreePruneReq(action="continue_all"))
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st["status"] != "running":
                break
            time.sleep(0.05)
        self.assertEqual(st["status"], "done")
        self.assertFalse(st.get("tree_paused"))
        self.assertIsNotNone(st.get("tree_snapshot"))
        self.assertGreaterEqual(st.get("tree_pruned_count", 0), 1)
        chains = [tuple(l["train"] for l in p["legs"]) for p in st["result"]["transfers"]]
        self.assertNotIn(("G101", "G102", "G103"), chains)
        self.assertIn(("G201", "G202", "G203"), chains)
        self.assertGreaterEqual(st["result"]["prune_stats"].get("user_tree", 0), 1)

        # 1b) 实时热砍(live)模式：运行中即时快照剔除 + 中途切换 pause -> step 审阅 -> continue_all
        ctrl_live = transfer.TreePruneController(pause_after_layer=False)
        snap_mock = {
            "depth": 1, "max_legs": 3, "phase": "running", "total_states": 2, "more_top": 0,
            "nodes": [
                {"kind": "city", "label": "中甲市", "city": "中甲市", "path": ["中甲市"], "count": 1, "min_elapsed_min": 60, "samples": ["G101"], "children": []},
                {"kind": "city", "label": "次甲市", "city": "次甲市", "path": ["次甲市"], "count": 1, "min_elapsed_min": 80, "samples": ["G201"], "children": []},
            ],
        }
        with ctrl_live.lock:
            ctrl_live.snapshot = snap_mock
        live_pr = ctrl_live.apply_command(prune_cities=["中甲市"], action="prune")
        self.assertFalse(live_pr["tree_paused"])
        self.assertEqual([n["city"] for n in live_pr["tree_snapshot"]["nodes"]], ["次甲市"])
        self.assertEqual(live_pr["tree_snapshot"]["total_states"], 1)
        sw = ctrl_live.apply_command(action="pause")
        self.assertTrue(sw["tree_pause_after_layer"])
        sw2 = ctrl_live.apply_command(action="continue_all")
        self.assertFalse(sw2["tree_pause_after_layer"])
        req_live = main_mod.TransferSearchReq(
            from_station="起点站", to_station="终点站", date="2026-11-21",
            min_wait=30, max_wait=120, max_legs=3, allow_cross_station=False,
            tree_prune_mode="live",
        )
        jid_live = main_mod.start_transfers(req_live)["job_id"]
        st_live = None
        for _ in range(80):
            st_live = main_mod.job_status(jid_live)
            if st_live["status"] != "running":
                break
            time.sleep(0.05)
        self.assertEqual(st_live["status"], "done")
        self.assertFalse(st_live.get("tree_paused"))
        self.assertIsNotNone(st_live.get("tree_snapshot"))

        # 2) 暂停状态下 cancel_job 能立即唤醒并取消
        start_c = main_mod.start_transfers(req)
        jid_c = start_c["job_id"]
        for _ in range(80):
            if main_mod.job_status(jid_c).get("tree_paused"):
                break
            time.sleep(0.05)
        main_mod.cancel_job(jid_c)
        for _ in range(40):
            if main_mod.job_status(jid_c)["status"] == "cancelled":
                break
            time.sleep(0.05)
        self.assertEqual(main_mod.job_status(jid_c)["status"], "cancelled")

        # 3) 搜索结果超 MAX_SEARCH_RESULTS_KEPT 时淘汰旧 result 与旧 tree_snapshot，保留任务元数据与 MAX_JOBS 上限
        created_jids = []
        for idx in range(main_mod.MAX_SEARCH_RESULTS_KEPT + 3):
            j_ev, _ = main_mod._create_job({"status": "running", "phase": "search", "done": 0, "total": 1, "result": {}})
            main_mod._job_update(j_ev, status="done", result={"direct": [], "transfers": [{"idx": idx}]}, tree_snapshot={"depth": 1, "nodes": []})
            created_jids.append(j_ev)
        with main_mod._jobs_lock:
            self.assertTrue(main_mod.jobs[created_jids[0]]["result"].get("evicted"))
            self.assertNotIn("tree_snapshot", main_mod.jobs[created_jids[0]])
            self.assertIn("tree_snapshot", main_mod.jobs[created_jids[-1]])
            self.assertEqual(main_mod.jobs[created_jids[-1]]["result"]["transfers"], [{"idx": main_mod.MAX_SEARCH_RESULTS_KEPT + 2}])

        # 4) 前端分支树面板控件校验
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        for marker in (
            'id="s-tree-mode"', 'id="s-tree-panel"', 'prune_tree', 'renderTreePanel',
            'initTreePanelEvents', 'id="s-tree-exp-all"', 'id="s-tree-col-all"', 'toggle-body',
        ):
            self.assertIn(marker, html_text, f"缺失分支树 UI 标记: {marker}")


class TestProvinceFilterAndTreeGrouping(TempDBTestCase):
    def _seed_corridor_provinces(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(?,?,?,?,?)",
                [
                    ("XMN", "厦门", "xm", "xm", "厦门"),
                    ("JYB", "建阳", "jy", "jy", "南平"),
                    ("GZS", "中山", "zs", "zs", "中山"),
                    ("HZC", "杭州东", "hzd", "hzd", "杭州"),
                    ("JND", "济南西", "jnx", "jnx", "济南"),
                    ("WHI", "威海", "wh", "wh", "威海"),
                    ("UNK", "神秘小站", "smxz", "smxz", "神秘城"),
                ],
            )
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    # 2程绕广东方案: 厦门 -> 中山(广东) -> 威海(山东)
                    ("P2_GD1", "G8001", "2026-11-25", 1, "厦门", "XMN", "----", "07:00"),
                    ("P2_GD1", "G8001", "2026-11-25", 2, "中山", "GZS", "09:00", "09:00"),
                    ("P2_GD2", "G8002", "2026-11-25", 1, "中山", "GZS", "----", "09:40"),
                    ("P2_GD2", "G8002", "2026-11-25", 2, "威海", "WHI", "15:00", "15:00"),
                    # 2程走廊内方案: 厦门 -> 杭州东(浙江) -> 威海(山东)
                    ("P2_ZJ1", "G8003", "2026-11-25", 1, "厦门", "XMN", "----", "07:10"),
                    ("P2_ZJ1", "G8003", "2026-11-25", 2, "杭州东", "HZC", "10:10", "10:10"),
                    ("P2_ZJ2", "G8004", "2026-11-25", 1, "杭州东", "HZC", "----", "10:50"),
                    ("P2_ZJ2", "G8004", "2026-11-25", 2, "威海", "WHI", "16:00", "16:00"),
                    # 2程未知省小站方案: 厦门 -> 神秘小站(未知) -> 威海(山东)
                    ("P2_UK1", "G8005", "2026-11-25", 1, "厦门", "XMN", "----", "07:20"),
                    ("P2_UK1", "G8005", "2026-11-25", 2, "神秘小站", "UNK", "09:20", "09:20"),
                    ("P2_UK2", "G8006", "2026-11-25", 1, "神秘小站", "UNK", "----", "10:00"),
                    ("P2_UK2", "G8006", "2026-11-25", 2, "威海", "WHI", "16:20", "16:20"),
                    # 3程绕广东方案: 厦门 -> 中山(广东) -> 济南西(山东) -> 威海(山东)
                    ("P3_GD2", "G8012", "2026-11-25", 1, "中山", "GZS", "----", "09:45"),
                    ("P3_GD2", "G8012", "2026-11-25", 2, "济南西", "JND", "13:00", "13:00"),
                    # 3程走廊内方案: 厦门 -> 杭州东(浙江) -> 济南西(山东) -> 威海(山东)
                    ("P3_ZJ2", "G8013", "2026-11-25", 1, "杭州东", "HZC", "----", "10:55"),
                    ("P3_ZJ2", "G8013", "2026-11-25", 2, "济南西", "JND", "13:30", "13:30"),
                    ("P3_SD3", "G8014", "2026-11-25", 1, "济南西", "JND", "----", "14:10"),
                    ("P3_SD3", "G8014", "2026-11-25", 2, "威海", "WHI", "16:40", "16:40"),
                    # 直达车次必须免疫任何省份黑/白名单过滤（即使白名单不含起点福建或终点山东）
                    ("P1_DIR", "G9000", "2026-11-25", 1, "厦门", "XMN", "----", "06:30"),
                    ("P1_DIR", "G9000", "2026-11-25", 2, "威海", "WHI", "17:00", "17:00"),
                ],
            )

    def test_china_geo_mapping_and_corridor_recommend(self):
        from app import china_geo

        self.assertEqual(len(china_geo.ALL_PROVINCES), 34)
        self.assertEqual(china_geo.infer_station_province("厦门北", "厦门"), "福建")
        self.assertEqual(china_geo.infer_station_province("建阳", "南平"), "福建")
        self.assertEqual(china_geo.infer_station_province("武夷山北", ""), "福建")
        self.assertEqual(china_geo.infer_station_province("威海北", "威海"), "山东")
        self.assertEqual(china_geo.infer_station_province("荣成", ""), "山东")
        self.assertEqual(china_geo.infer_station_province("青海海南", "海南州"), "青海")
        self.assertEqual(china_geo.infer_station_province("神秘小站", "神秘城"), "")
        # 无城市名时利用经纬度兜底识别（济南坐标 -> 山东）
        self.assertEqual(china_geo.infer_station_province("不知名郊县站", "", lat=36.65, lon=117.0), "山东")

        # 厦门 -> 威海 走廊不得包含广东、台湾、辽宁
        rec_std = china_geo.recommend_corridor_provinces("厦门", "威海", widen="standard")
        self.assertIn("福建", rec_std["provinces"])
        self.assertIn("浙江", rec_std["provinces"])
        self.assertIn("山东", rec_std["provinces"])
        self.assertNotIn("广东", rec_std["provinces"])
        self.assertNotIn("台湾", rec_std["provinces"])
        self.assertNotIn("辽宁", rec_std["provinces"])

        rec_wide = china_geo.recommend_corridor_provinces("建阳", "威海", widen="wide")
        self.assertTrue(len(rec_wide["provinces"]) >= len(rec_std["provinces"]))
        self.assertIn("福建", rec_wide["provinces"])
        self.assertIn("山东", rec_wide["provinces"])

    def test_province_filter_2leg_and_3leg_and_endpoints(self):
        from app import main as main_mod

        self._seed_corridor_provinces()

        # 1) 黑名单屏蔽广东：2程 G8001/G8002 与 3程 G8001/G8012/G8014 被彻底屏蔽，直达 G9000 与浙江/未知中转放行
        r_exc = transfer.search(
            "厦门", "威海", "2026-11-25",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False, allow_cross_station=False,
            province_mode="exclude", exclude_provinces="广东",
        )
        self.assertFalse(r_exc["prune_config"]["auto_relaxed"])
        self.assertEqual(len(r_exc["direct"]), 1)
        chains_exc = {tuple(l["train"] for l in p["legs"]) for p in r_exc["transfers"]}
        self.assertNotIn(("G8001", "G8002"), chains_exc)
        self.assertNotIn(("G8001", "G8012", "G8014"), chains_exc)
        self.assertIn(("G8003", "G8004"), chains_exc)
        self.assertIn(("G8003", "G8013", "G8014"), chains_exc)
        self.assertIn(("G8005", "G8006"), chains_exc)
        self.assertGreaterEqual(r_exc["prune_stats"].get("province_filter", 0), 1)

        # 2) 白名单仅限 浙江,山东：未知小站默认放行(block_unknown_prov=False)，开启 block_unknown_prov=True 后被拦截
        r_allow_open = transfer.search(
            "厦门", "威海", "2026-11-25",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False, allow_cross_station=False,
            province_mode="allow", allow_provinces="浙江,山东", block_unknown_prov=False,
        )
        chains_open = {tuple(l["train"] for l in p["legs"]) for p in r_allow_open["transfers"]}
        self.assertIn(("G8003", "G8004"), chains_open)
        self.assertIn(("G8005", "G8006"), chains_open)
        self.assertNotIn(("G8001", "G8002"), chains_open)
        self.assertEqual(len(r_allow_open["direct"]), 1)

        r_allow_strict = transfer.search(
            "厦门", "威海", "2026-11-25",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False, allow_cross_station=False,
            province_mode="allow", allow_provinces="浙江,山东", block_unknown_prov=True,
        )
        chains_strict = {tuple(l["train"] for l in p["legs"]) for p in r_allow_strict["transfers"]}
        self.assertIn(("G8003", "G8004"), chains_strict)
        self.assertNotIn(("G8005", "G8006"), chains_strict)

        # 3) API 端点 /api/provinces/meta 与 /api/provinces/recommend 验证 + 400 校验
        meta = main_mod.provinces_meta()
        self.assertEqual(len(meta["provinces"]), 34)
        self.assertEqual(len(meta["regions"]), 8)
        rec_api = main_mod.recommend_provinces(from_station="厦门", to_station="威海", widen="standard")
        self.assertIn("浙江", rec_api["provinces"])
        self.assertNotIn("广东", rec_api["provinces"])

        with self.assertRaises(main_mod.HTTPException) as ctx_bad_pm:
            main_mod.transfers("厦门", "威海", "2026-11-25", province_mode="invalid")
        self.assertEqual(ctx_bad_pm.exception.status_code, 400)

    def test_tree_province_grouping_and_live_province_prune(self):
        import time
        from app import main as main_mod

        self._seed_corridor_provinces()
        main_mod.jobs.clear()
        main_mod._job_cancels.clear()
        main_mod._job_trees.clear()

        req = main_mod.TransferSearchReq(
            from_station="厦门", to_station="威海", date="2026-11-25",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False, allow_cross_station=False,
            tree_prune_mode="step",
        )
        jid = main_mod.start_transfers(req)["job_id"]
        st = None
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st.get("tree_paused"):
                break
            time.sleep(0.05)
        self.assertTrue(st and st.get("tree_paused"))
        snap = st["tree_snapshot"]
        # 按省分层树应包含 广东 -> 中山 -> 中山，浙江 -> 杭州 -> 杭州东，以及活跃省份汇总栏
        prov_labels = [n["label"] for n in snap.get("provinces", [])]
        self.assertIn("广东", prov_labels)
        self.assertIn("浙江", prov_labels)
        sum_provs = [x["province"] for x in snap.get("province_summary", [])]
        self.assertIn("广东", sum_provs)

        # 一键整省屏蔽 广东 并继续完成
        pr = main_mod.prune_job_tree(jid, main_mod.TreePruneReq(prune_provinces=["广东"], action="prune"))
        self.assertIn("广东", pr["tree_pruned_rules"]["provinces"])
        self.assertNotIn("广东", [n["label"] for n in pr["tree_snapshot"].get("provinces", [])])
        self.assertNotIn("中山", [n["city"] for n in pr["tree_snapshot"].get("nodes", [])])

        main_mod.prune_job_tree(jid, main_mod.TreePruneReq(action="continue_all"))
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st["status"] != "running":
                break
            time.sleep(0.05)
        self.assertEqual(st["status"], "done")
        chains = {tuple(l["train"] for l in p["legs"]) for p in st["result"]["transfers"]}
        self.assertNotIn(("G8001", "G8012", "G8014"), chains)
        self.assertIn(("G8003", "G8013", "G8014"), chains)

        # 前端省份抽屉与双视图分支树控件检查
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        for marker in (
            'id="s-drawer-prov-btn"', 'id="s-prov-drawer"', 'id="s-prov-mode-seg"',
            'id="s-prov-rec-btn"', 'id="s-prov-rec-exc-btn"', 'id="s-prov-widen"',
            'id="s-prov-block-unknown"', 'id="s-tree-view-seg"', 'id="s-tree-prov-bar"',
            'id="s-tree-cut-prov"', 'data-cut-province=', '/api/provinces/recommend',
        ):
            self.assertIn(marker, html_text, f"缺失省份过滤 UI 标记: {marker}")

    def test_all_real_stations_province_coverage(self):
        from app import china_geo, parsers

        js_path = ROOT / "samples" / "station_name.js"
        stations = parsers.parse_stations(js_path.read_text(encoding="utf-8"))
        laos_cities = {"万象", "老挝万荣", "琅勃拉邦", "磨丁", "孟赛", "纳堆"}
        for s in stations:
            ct = (s.get("city") or "").strip()
            if ct in laos_cities:
                continue
            pw = china_geo.infer_station_province(s["name"], ct)
            pn = china_geo.infer_station_province(s["name"], "")
            self.assertTrue(pw, f"station_name.js 归省失败(含city): {s['name']}/{ct}")
            self.assertEqual(pw, pn, f"station_name.js 无city归省不一致: {s['name']} ({ct}->{pw} vs {pn})")

        # 重点抽检曾落入 未识别省份 或前缀歧义的站点
        for nm, ct, exp in (
            ("武穴北", "武穴", "湖北"),
            ("蕲春南", "蕲春", "湖北"),
            ("浠水南", "浠水", "湖北"),
            ("汉川北", "汉川", "湖北"),
            ("仪征", "仪征", "江苏"),
            ("芦溪", "芦溪", "江西"),
            ("资溪", "资溪", "江西"),
            ("来舟", "来舟", "福建"),
            ("乌兰浩特", "兴安", "内蒙古"),
            ("兴安", "桂林", "广西"),
            ("海北", "绥化", "黑龙江"),
            ("平湖", "深圳", "广东"),
            ("汉口", "", "湖北"),
            ("桃村", "", "山东"),
        ):
            self.assertEqual(china_geo.infer_station_province(nm, ct), exp)

    def test_unknown_province_sorts_last_and_prunable(self):
        import time
        from app import main as main_mod

        self._seed_corridor_provinces()
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("P3_UK1F", "G8015", "2026-11-25", 1, "厦门", "XMN", "----", "07:30"),
                    ("P3_UK1F", "G8015", "2026-11-25", 2, "神秘小站", "UNK", "08:30", "08:30"),
                    ("P3_UK2", "G8016", "2026-11-25", 1, "神秘小站", "UNK", "----", "09:10"),
                    ("P3_UK2", "G8016", "2026-11-25", 2, "济南西", "JND", "13:10", "13:10"),
                ],
            )
        main_mod.jobs.clear()
        main_mod._job_cancels.clear()
        main_mod._job_trees.clear()

        req = main_mod.TransferSearchReq(
            from_station="厦门", to_station="威海", date="2026-11-25",
            min_wait=30, max_wait=120, max_legs=3, expand_city=False, allow_cross_station=False,
            tree_prune_mode="step",
        )
        jid = main_mod.start_transfers(req)["job_id"]
        st = None
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st.get("tree_paused"):
                break
            time.sleep(0.05)
        self.assertTrue(st and st.get("tree_paused"))
        snap = st["tree_snapshot"]
        prov_labels = [n["label"] for n in snap.get("provinces", [])]
        self.assertIn("未识别省份", prov_labels)
        self.assertEqual(prov_labels[-1], "未识别省份")
        sum_provs = [x["province"] for x in snap.get("province_summary", [])]
        self.assertEqual(sum_provs[-1], "未识别省份")

        pr = main_mod.prune_job_tree(jid, main_mod.TreePruneReq(prune_provinces=["未识别省份"], action="prune"))
        self.assertNotIn("未识别省份", [n["label"] for n in pr["tree_snapshot"].get("provinces", [])])
        main_mod.prune_job_tree(jid, main_mod.TreePruneReq(action="continue_all"))
        for _ in range(80):
            st = main_mod.job_status(jid)
            if st["status"] != "running":
                break
            time.sleep(0.05)
        self.assertEqual(st["status"], "done")
        chains = {tuple(l["train"] for l in p["legs"]) for p in st["result"]["transfers"]}
        self.assertNotIn(("G8015", "G8016", "G8014"), chains)
        self.assertIn(("G8003", "G8013", "G8014"), chains)


class TestZnvZtkCompat(TempDBTestCase):
    def test_parsers_znv_ztk_conversion_and_emu_guard(self):
        # 普速 T↔N, K/纯数字↔V, Z↔Z 及复合斜杠车次
        self.assertEqual(parsers.convert_train_code("K8278/K8275", "znv"), "V8278/V8275")
        self.assertEqual(parsers.convert_train_code("V8278/V8275", "ztk"), "K8278/K8275")
        self.assertEqual(parsers.convert_train_code("T181", "znv"), "N181")
        self.assertEqual(parsers.convert_train_code("N181", "ztk"), "T181")
        self.assertEqual(parsers.convert_train_code("Z104", "znv"), "Z104")
        self.assertEqual(parsers.convert_train_code("6061", "znv"), "V6061")
        self.assertEqual(parsers.convert_train_code("6061", "ztk"), "6061")

        # 未来高速动车组 T/K（CR400/CRH/ZL 车底）不与普速 N/V 互通
        self.assertTrue(parsers.is_emu_style("CR400BF-Z_重联"))
        self.assertFalse(parsers.is_emu_style("25G"))
        self.assertEqual(parsers.convert_train_code("T10", "znv", is_emu=True), "T10")
        self.assertEqual(parsers.train_code_variants("T10", is_emu=True), frozenset({"T10"}))
        self.assertEqual(parsers.train_code_variants("K411", is_emu=False), frozenset({"K411", "V411"}))

        # 大类分族防 K10 串车 G10
        self.assertEqual(parsers.train_class_family("K10"), "KV")
        self.assertEqual(parsers.train_class_family("V10"), "KV")
        self.assertEqual(parsers.train_class_family("6061"), "KV")
        self.assertEqual(parsers.train_class_family("G10"), "G")
        self.assertEqual(parsers.train_class_family("T10", is_emu=True), "T")
        self.assertEqual(parsers.train_class_family("N10"), "TN")

        # 12 位 train_no 提取主车次（折角车 K884/V881 同根）
        self.assertEqual(parsers.train_no_root_code("270000V88400"), "V884")
        self.assertEqual(parsers.train_no_root_code("47000K827850"), "K8278")
        self.assertEqual(parsers.train_no_root_code("2400000D170O"), "D17")

    def test_fare_estimate_and_calibration_across_nv_and_emu(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO station_trains(station_telecode,station_name,station_train_code,train_no,station_train_date,station_no,distance) VALUES(?,?,?,?,?,?,?)",
                [
                    ("JNK", "济南", "K411", "K411_TN", "2026-11-24", 1, 0),
                    ("QDK", "青岛", "K411", "K411_TN", "2026-11-24", 2, 300),
                    ("JNK", "济南", "T100", "T100_TN", "2026-11-24", 1, 0),
                    ("QDK", "青岛", "T100", "T100_TN", "2026-11-24", 2, 300),
                ],
            )
            conn.executemany(
                "INSERT INTO prices(train_no,train_date,from_station_no,to_station_no,seat_type,seat_name,price) VALUES(?,?,?,?,?,?,?)",
                [
                    ("K411_TN", "2026-11-24", "01", "02", "yz", "硬座", 80.0),
                    ("T100_TN", "2026-11-24", "01", "02", "yz", "硬座", 90.0),
                ],
            )
        self.assertEqual(fare.class_of("V411"), "K")
        self.assertEqual(fare.class_of("N100"), "T")
        self.assertEqual(fare.class_of("T10", style="CR400BF-Z"), "G")
        self.assertEqual(fare.class_of("K20", style="CRH380A"), "D")

        cal = fare.load_calibration()
        self.assertIn(("K", "yz"), cal)
        self.assertIn(("T", "yz"), cal)
        est_v = fare.estimate("V411", km=300, cal=cal)
        est_k = fare.estimate("K411", km=300, cal=cal)
        self.assertIsNotNone(est_v)
        self.assertIn("已用实际票价校准", est_v["basis"])
        self.assertEqual(est_v["seats"]["yz"]["min"], est_k["seats"]["yz"]["min"])

        est_n = fare.estimate("N100", duration_min=240, cal=cal)
        self.assertIn("yz", est_n["seats"])
        self.assertIn("已用实际票价校准", est_n["basis"])

        # 动车组底 T/K 按 G/D 估算席别，而非硬座
        est_emu_t = fare.estimate("T10", duration_min=120, cal=cal, style="CR400BF-Z")
        self.assertIn("ze", est_emu_t["seats"])
        self.assertNotIn("yz", est_emu_t["seats"])

    def _seed_k8278_to_v4278_scenario(self):
        with db.tx() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO stations(telecode,name,city,pinyin,short) VALUES(?,?,?,?,?)",
                [
                    ("XKS", "厦门北", "厦门", "xiamenbei", "xmb"),
                    ("UCK", "聊城", "聊城", "liaocheng", "lc"),
                    ("JAK", "大明湖", "济南", "daminghu", "dmh"),
                    ("WFK", "潍坊", "潍坊", "weifang", "wf"),
                    ("QHK", "青岛北", "青岛", "qingdaobei", "qdb"),
                    ("WKK", "威海", "威海", "weihai", "wh"),
                ],
            )
            # 改号前本地图谱时刻表：Z104 -> K8278(47000K827850) -> K411(240000K41102) 及 K884(270000K8842F)
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("5n0000Z1040F", "Z104", "2026-11-24", 1, "厦门北", "XKS", "----", "06:45"),
                    ("5n0000Z1040F", "Z104", "2026-11-24", 8, "聊城", "UCK", "23:36", "23:42"),
                    ("47000K827850", "K8278", "2026-11-24", 2, "聊城", "UCK", "23:58", "00:06"),
                    ("47000K827850", "K8278", "2026-11-24", 3, "大明湖", "JAK", "01:53", "02:01"),
                    ("240000K41102", "K411", "2026-11-24", 5, "大明湖", "JAK", "06:06", "06:14"),
                    ("240000K41102", "K411", "2026-11-24", 11, "威海", "WKK", "14:25", "----"),
                    ("270000K8842F", "K884", "2026-11-24", 7, "潍坊", "WFK", "06:10", "06:16"),
                    ("270000K8842F", "K884", "2026-11-24", 8, "青岛北", "QHK", "07:41", "07:50"),
                ],
            )
            # 改号后 12306 实时余票（V4278 47000V427810; V411 240000V41100; 折角车 V881 270000V88400 即使微调 1 分钟也同根命中）
            conn.executemany(
                """INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,
                                       from_station_no,to_station_no,start_time,arrive_time,duration,can_web_buy,seats,seat_types)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?, ?,?)""",
                [
                    ("5n0000Z1040F", "Z104", "2026-11-26", "XKS", "UCK", "01", "08", "06:45", "23:36", "16:51", "Y",
                     json.dumps({"yz": "候补", "rw": "有", "yw": "有", "wz": "有"}), "134"),
                    ("47000V427810", "V4278", "2026-11-27", "UCK", "JAK", "02", "03", "00:06", "01:53", "01:47", "Y",
                     json.dumps({"yz": "有", "yw": "有", "rw": "5", "wz": "有"}), "134"),
                    ("240000V41100", "V411", "2026-11-27", "JAK", "WKK", "04", "10", "06:14", "14:25", "08:11", "Y",
                     json.dumps({"yz": "有", "rw": "6", "yw": "有", "wz": "候补"}), "134"),
                    ("270000V88400", "V881", "2026-11-27", "WFK", "QHK", "07", "08", "06:16", "07:42", "01:26", "Y",
                     json.dumps({"yz": "有", "rw": "15", "yw": "有", "wz": "有"}), "134"),
                    # 同区间干扰动车 G4278（同数字不同大类）不应被 K8278 匹配
                    ("24000G427800", "G4278", "2026-11-27", "UCK", "JAK", "02", "03", "00:06", "01:53", "01:47", "Y",
                     json.dumps({"ze": "有"}), "OM"),
                ],
            )
            conn.executemany(
                "INSERT INTO prices(train_no,train_date,from_station_no,to_station_no,seat_type,seat_name,price) VALUES(?,?,?,?,?,?,?)",
                [
                    ("5n0000Z1040F", "2026-11-26", "01", "08", "yz", "硬座", 206.0),
                    ("47000V427810", "2026-11-27", "02", "03", "yz", "硬座", 19.5),
                    ("240000V41100", "2026-11-27", "04", "10", "yz", "硬座", 81.0),
                    ("270000V88400", "2026-11-27", "07", "08", "yz", "硬座", 24.5),
                ],
            )

    def test_transfer_search_and_plan_tickets_resolve_k8278_to_v4278(self):
        from app import main as main_mod

        self._seed_k8278_to_v4278_scenario()
        transfer.invalidate_cache()
        res = transfer.search(
            "厦门北", "威海", "2026-11-26",
            min_wait=20, max_wait=360, max_legs=3,
            expand_city=False, allow_cross_station=False,
        )
        chains = {tuple(l["train"] for l in p["legs"]): p for p in res["transfers"]}
        self.assertIn(("Z104", "K8278", "K411"), chains)
        plan = chains[("Z104", "K8278", "K411")]
        self.assertFalse(plan.get("date_assumed"))
        leg2, leg3 = plan["legs"][1], plan["legs"][2]
        self.assertEqual(leg2.get("live_train"), "V4278")
        self.assertEqual(leg2.get("live_train_no"), "47000V427810")
        self.assertEqual(leg2["seats"]["yz"], "有")
        self.assertEqual(leg2["prices"][0]["price"], 19.5)
        self.assertEqual(leg3.get("live_train"), "V411")
        self.assertEqual(leg3.get("live_train_no"), "240000V41100")
        self.assertEqual(leg3["prices"][0]["price"], 81.0)

        # 前缀过滤跨新旧号生效：筛选 V 或 K 都能保留 Z/K/V 组合，而剔除 V 不会误杀动车组 T/K
        kept_v = transfer.filter_plans([plan], allow_prefixes="Z,V")
        self.assertEqual(len(kept_v), 1)
        kept_k = transfer.filter_plans([plan], allow_prefixes="Z,K")
        self.assertEqual(len(kept_k), 1)
        emu_plan = {"legs": [{"train": "T10", "train_style": "CR400BF-Z"}], "transfers": []}
        self.assertEqual(len(transfer.filter_plans([emu_plan], exclude_prefixes="N")), 1)
        self.assertEqual(len(transfer.filter_plans([emu_plan], allow_prefixes="N")), 0)

        # _plan_tickets_job: K8278->V4278, K411->V411, 以及折角车 K884(270000K8842F)->V881(270000V88400) 均能命中实时余票与新票价
        main_mod.jobs.clear()
        main_mod._job_cancels.clear()
        req = main_mod.PlanTicketsReq(legs=[
            main_mod.PlanLeg(train_no="47000K827850", train="K8278", date="2026-11-27",
                             from_telecode="UCK", to_telecode="JAK", from_name="聊城", to_name="大明湖",
                             depart="2026-11-27 00:06", arrive="2026-11-27 01:53",
                             from_station_no="02", to_station_no="03"),
            main_mod.PlanLeg(train_no="240000K41102", train="K411", date="2026-11-27",
                             from_telecode="JAK", to_telecode="WKK", from_name="大明湖", to_name="威海",
                             depart="2026-11-27 06:14", arrive="2026-11-27 14:25",
                             from_station_no="05", to_station_no="11"),
            main_mod.PlanLeg(train_no="270000K8842F", train="K884", date="2026-11-27",
                             from_telecode="WFK", to_telecode="QHK", from_name="潍坊", to_name="青岛北",
                             depart="2026-11-27 06:16", arrive="2026-11-27 07:41",
                             from_station_no="07", to_station_no="08"),
        ])
        with mock.patch("app.main.crawler.crawl_tickets") as m_ct, mock.patch("app.main.crawler.crawl_price") as m_cp:
            jid = main_mod.plan_tickets(req)["job_id"]
            import time
            st = None
            for _ in range(80):
                st = main_mod.job_status(jid)
                if st["status"] != "running":
                    break
                time.sleep(0.05)
        self.assertEqual(st["status"], "done")
        r_map = st["result"]
        k2 = "47000K827850|UCK|JAK|2026-11-27"
        k3 = "240000K41102|JAK|WKK|2026-11-27"
        k4 = "270000K8842F|WFK|QHK|2026-11-27"
        self.assertEqual(r_map[k2]["live_train"], "V4278")
        self.assertEqual(r_map[k2]["live_train_no"], "47000V427810")
        self.assertEqual(r_map[k2]["seats"]["yz"], "有")
        self.assertEqual(r_map[k2]["prices"][0]["price"], 19.5)
        self.assertEqual(r_map[k3]["live_train"], "V411")
        self.assertEqual(r_map[k3]["prices"][0]["price"], 81.0)
        self.assertEqual(r_map[k4]["live_train"], "V881")
        self.assertEqual(r_map[k4]["live_train_no"], "270000V88400")
        self.assertEqual(r_map[k4]["seats"]["rw"], "15")
        self.assertEqual(r_map[k4]["prices"][0]["price"], 24.5)
        self.assertEqual(m_ct.call_count, 3)
        m_cp.assert_not_called()

    def test_trains_search_timetable_compat_and_ui_mode_markers(self):
        from app import main as main_mod

        self._seed_k8278_to_v4278_scenario()
        with db.tx() as conn:
            conn.executemany(
                "INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,telecode,arrive_time,start_time,enabled) VALUES(?,?,?,?,?,?,?,?,1)",
                [
                    ("47000V427810", "V4278", "2026-11-27", 2, "聊城", "UCK", "23:58", "00:06"),
                    ("47000V427810", "V4278", "2026-11-27", 3, "大明湖", "JAK", "01:53", "02:01"),
                    ("240000V41100", "V411", "2026-11-27", 4, "大明湖", "JAK", "06:06", "06:14"),
                    ("240000V41100", "V411", "2026-11-27", 10, "威海", "WKK", "14:25", "----"),
                ],
            )
            conn.execute(
                "INSERT INTO station_trains(station_telecode,station_name,station_train_code,train_no,station_train_date,start_station_telecode,end_station_telecode,start_train_date,station_no,arrive_time,start_time) VALUES('UCK','聊城','K8278','47000K827850','2026-11-24','LCK','QDK','2026-11-24',2,'23:58','00:06')"
            )

        # /api/trains 搜旧车次 K411 应同时命中共存的 V411；搜单字母 V 也能命中 K 系列
        hit_k = {r["station_train_code"] for r in main_mod.trains(q="K411")}
        self.assertEqual(hit_k, {"K411", "V411"})
        hit_v = {r["station_train_code"] for r in main_mod.trains(q="V")}
        self.assertIn("K411", hit_v)

        # /api/timetable 优先回落 live_train_no
        tt = main_mod.timetable(train_no="99999K827800", live_train_no="47000V427810")
        self.assertEqual(tt["train_no"], "47000V427810")
        self.assertEqual(tt["station_train_code"], "V4278")

        # _crawl_timetable_compat：live_train_no 先试失败时，旧号 >=2026-11-25 返空会回退到 2026-11-24
        calls = []
        def fake_crawl_tt(tn, d, f, t):
            calls.append((tn, d, f, t))
            if tn == "47000V427810":
                raise main_mod.CrawlError("接口返回停站为空")
            if tn == "47000K827850" and d >= "2026-11-25":
                raise main_mod.CrawlError("接口返回停站为空")
            return {"parsed": 2}

        with mock.patch("app.main.crawler.crawl_timetable", side_effect=fake_crawl_tt):
            res_tt = main_mod._crawl_timetable_compat(
                "47000K827850", live_train_no="47000V427810",
                date="2026-11-27", from_telecode="UCK", to_telecode="JAK",
            )
        self.assertEqual(res_tt, {"parsed": 2})
        self.assertEqual(
            calls,
            [
                ("47000V427810", "2026-11-27", "UCK", "JAK"),
                ("47000K827850", "2026-11-27", "UCK", "JAK"),
                ("47000K827850", "2026-11-24", "UCK", "JAK"),
            ],
        )

        # 前端 ZNV / ZTK 切换按钮与函数标记检查
        html_text = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
        for marker in (
            "setTrainCodeMode('znv')", "setTrainCodeMode('ztk')", 'id="d-code-mode"',
            'setTrainCodeMode', 'displayTrainCode', 'legAliasNote', 'convertTrainCode',
        ):
            self.assertIn(marker, html_text, f"缺失 ZNV/ZTK 前端标记: {marker}")


if __name__ == "__main__":
    unittest.main()





