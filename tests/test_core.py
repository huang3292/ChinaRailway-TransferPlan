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

        res_default = transfer.search("甲市", "乙市", "2026-11-20", min_wait=30, max_wait=360, expand_city=True)
        # 默认 cross_extra=60，45 分钟异站换乘不满足 30+60=90
        self.assertEqual(len(res_default["transfers"]), 0)

        res_relaxed = transfer.search("甲市", "乙市", "2026-11-20", min_wait=30, max_wait=360, expand_city=True, cross_extra=10)
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


class TestCrawlerAndMainStubbed(TempDBTestCase):
    @classmethod
    def setUpClass(cls):
        # Lightweight stubs for third-party modules absent in container so crawler & main logic can be unit tested.
        if "requests" not in sys.modules:
            req_mod = types.ModuleType("requests")

            class RequestException(Exception):
                pass

            class Session:
                def __init__(self):
                    self.headers = {}

                def request(self, *args, **kwargs):
                    raise RequestException("stub")

            req_mod.RequestException = RequestException
            req_mod.Session = Session
            sys.modules["requests"] = req_mod

        if "fastapi" not in sys.modules:
            fa = types.ModuleType("fastapi")
            fa_resp = types.ModuleType("fastapi.responses")
            fa_stat = types.ModuleType("fastapi.staticfiles")
            pyd = types.ModuleType("pydantic")

            class HTTPException(Exception):
                def __init__(self, status_code, detail=""):
                    super().__init__(detail)
                    self.status_code = status_code
                    self.detail = detail

            class FastAPI:
                def __init__(self, *a, **kw):
                    pass

                def on_event(self, *a, **kw):
                    return lambda f: f

                def get(self, *a, **kw):
                    return lambda f: f

                def post(self, *a, **kw):
                    return lambda f: f

                def delete(self, *a, **kw):
                    return lambda f: f

                def mount(self, *a, **kw):
                    pass

            class UploadFile:
                pass

            class BaseModel:
                def __init__(self, **kw):
                    for k, v in kw.items():
                        setattr(self, k, v)

                def model_dump(self, exclude=None):
                    ex = exclude or set()
                    return {k: v for k, v in self.__dict__.items() if k not in ex}

            fa.FastAPI = FastAPI
            fa.HTTPException = HTTPException
            fa.UploadFile = UploadFile
            fa_resp.FileResponse = lambda *a, **kw: None
            fa_stat.StaticFiles = lambda *a, **kw: None
            pyd.BaseModel = BaseModel
            sys.modules["fastapi"] = fa
            sys.modules["fastapi.responses"] = fa_resp
            sys.modules["fastapi.staticfiles"] = fa_stat
            sys.modules["pydantic"] = pyd

    def test_crawler_cancel_and_maint_hint(self):
        from app import crawler as crawler_mod

        c = crawler_mod.Crawler()
        ev = threading.Event()
        ev.set()
        c.set_cancel_event(ev)
        with self.assertRaises(crawler_mod.CrawlCancelled):
            c._sleep(5.0)
        c.set_cancel_event(None)

        # Business message ("预售") should not double interval
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

        # Verify _plan_tickets_job uses Beijing today and checks train_date in prices cache
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
            # An older price row shouldn't block crawling if date differed, whereas exact future_date price should satisfy cache!
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


if __name__ == "__main__":
    unittest.main()
