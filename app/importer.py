"""导入模块：原始 12306 API 返回 -> 留档 + 解析入库。爬虫与手动导入共用。"""
import json

from . import db, parsers

API_TYPES = ("stations", "left_ticket", "price", "train_stops", "station_screen")


def ingest(text: str, api_type: str | None = None, ctx: dict | None = None,
           source: str = "import") -> dict:
    ctx = {k: v for k, v in (ctx or {}).items() if v}
    api_type = api_type or parsers.detect_type(text)
    if api_type not in API_TYPES:
        raise ValueError(f"未知类型 {api_type}，可选：{API_TYPES}")

    if api_type == "stations":
        records = parsers.parse_stations(text)
    else:
        payload = json.loads(text)
        if api_type == "left_ticket":
            records = parsers.parse_left_ticket(payload, ctx)
        elif api_type == "price":
            records = parsers.parse_price(payload, ctx)
        elif api_type == "train_stops":
            records = parsers.parse_train_stops(payload, ctx, db.station_lookup())
        else:
            records = parsers.parse_station_screen(payload, ctx)

    raw_id = db.archive_raw(api_type, text, ctx, source)
    count = _save(api_type, records, raw_id)
    return {"api_type": api_type, "parsed": len(records), "saved": count, "raw_id": raw_id}


def _save(api_type: str, records: list[dict], raw_id: int) -> int:
    if not records:
        return 0
    with db.tx() as conn:
        if api_type == "stations":
            conn.executemany(
                """INSERT INTO stations(telecode,name,pinyin,short,city) VALUES(:telecode,:name,:pinyin,:short,:city)
                   ON CONFLICT(telecode) DO UPDATE SET name=:name,pinyin=:pinyin,short=:short,city=:city""",
                records)
        elif api_type == "left_ticket":
            for r in records:
                r = dict(r, seats=json.dumps(r["seats"], ensure_ascii=False), raw_id=raw_id)
                conn.execute(
                    """INSERT INTO tickets(train_no,station_train_code,train_date,from_telecode,to_telecode,
                         from_station_no,to_station_no,start_time,arrive_time,duration,can_web_buy,seat_types,seats,raw_id)
                       VALUES(:train_no,:station_train_code,:train_date,:from_telecode,:to_telecode,
                         :from_station_no,:to_station_no,:start_time,:arrive_time,:duration,:can_web_buy,:seat_types,:seats,:raw_id)
                       ON CONFLICT(train_no,train_date,from_telecode,to_telecode) DO UPDATE SET
                         station_train_code=:station_train_code,start_time=:start_time,arrive_time=:arrive_time,
                         duration=:duration,can_web_buy=:can_web_buy,seat_types=:seat_types,seats=:seats,raw_id=:raw_id""",
                    r)
        elif api_type == "price":
            for r in records:
                # 唯一键列不允许 NULL，否则 SQLite 视为互不相同导致重复
                r = dict(r, train_date=r["train_date"] or "",
                         from_station_no=r["from_station_no"] or "",
                         to_station_no=r["to_station_no"] or "")
                conn.execute(
                    """INSERT INTO prices(train_no,station_train_code,train_date,from_station_no,to_station_no,
                         seat_type,seat_name,price,raw_id)
                       VALUES(:train_no,:station_train_code,:train_date,:from_station_no,:to_station_no,
                         :seat_type,:seat_name,:price,:raw_id)
                       ON CONFLICT(train_no,train_date,from_station_no,to_station_no,seat_type) DO UPDATE SET
                         station_train_code=:station_train_code,seat_name=:seat_name,price=:price,raw_id=:raw_id""",
                    dict(r, raw_id=raw_id))
        elif api_type == "train_stops":
            # 整份时刻表以最后一次爬取为准：先清掉该车次该日期的旧停站
            for tn, d in {(r["train_no"], r["train_date"] or "") for r in records}:
                conn.execute("DELETE FROM train_stops WHERE train_no=? AND train_date=?", (tn, d))
            for r in records:
                r = dict(r, train_date=r["train_date"] or "")
                conn.execute(
                    """INSERT INTO train_stops(train_no,station_train_code,train_date,station_no,station_name,
                         telecode,arrive_time,start_time,stopover_time,enabled,raw_id)
                       VALUES(:train_no,:station_train_code,:train_date,:station_no,:station_name,
                         :telecode,:arrive_time,:start_time,:stopover_time,:enabled,:raw_id)
                       ON CONFLICT(train_no,train_date,station_no) DO UPDATE SET
                         station_train_code=:station_train_code,station_name=:station_name,telecode=:telecode,
                         arrive_time=:arrive_time,start_time=:start_time,stopover_time=:stopover_time,
                         enabled=:enabled,raw_id=:raw_id""",
                    dict(r, raw_id=raw_id))
        else:  # station_screen
            # 该站该日期的经由车次以最后一次爬取为准：先清旧记录（含已停运/改车次号的）
            for st, d in {(r["station_telecode"], r["station_train_date"]) for r in records}:
                conn.execute("DELETE FROM station_trains WHERE station_telecode=? AND station_train_date=?",
                             (st, d))
            for r in records:
                conn.execute(
                    """INSERT INTO station_trains(station_telecode,station_name,query_date,station_train_code,train_no,
                         station_train_date,start_train_date,station_no,arrive_time,start_time,stopover_time,
                         arrive_day_diff,start_day_diff,start_station_name,start_station_telecode,
                         end_station_name,end_station_telecode,train_class_name,seat_types,running_fig,
                         start_start_time,end_arrive_time,running_time,distance,raw_id)
                       VALUES(:station_telecode,:station_name,:query_date,:station_train_code,:train_no,
                         :station_train_date,:start_train_date,:station_no,:arrive_time,:start_time,:stopover_time,
                         :arrive_day_diff,:start_day_diff,:start_station_name,:start_station_telecode,
                         :end_station_name,:end_station_telecode,:train_class_name,:seat_types,:running_fig,
                         :start_start_time,:end_arrive_time,:running_time,:distance,:raw_id)
                       ON CONFLICT(station_telecode,station_train_code,station_train_date) DO UPDATE SET
                         station_name=:station_name,query_date=:query_date,
                         train_no=:train_no,start_train_date=:start_train_date,station_no=:station_no,
                         arrive_time=:arrive_time,start_time=:start_time,stopover_time=:stopover_time,
                         arrive_day_diff=:arrive_day_diff,start_day_diff=:start_day_diff,
                         start_station_name=:start_station_name,start_station_telecode=:start_station_telecode,
                         end_station_name=:end_station_name,end_station_telecode=:end_station_telecode,
                         train_class_name=:train_class_name,seat_types=:seat_types,running_fig=:running_fig,
                         start_start_time=:start_start_time,end_arrive_time=:end_arrive_time,
                         running_time=:running_time,distance=:distance,raw_id=:raw_id""",
                    dict(r, raw_id=raw_id))
    return len(records)