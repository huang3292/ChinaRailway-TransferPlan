# 12306 智能中转

本地运行的火车票**直达 / 中转换乘**规划工具。抓取并留档 12306 公开查询接口的原始返回，
入库后在本地做换乘组合搜索、票价估算与实时票价/余票查询。全部数据落在本地 SQLite，
可离线复算；联网仅用于按需抓取。

> 仅访问 12306 公开查询接口，全局限速，原始返回全部留档。请勿用于高频抢票/刷票。

---

## 1. 技术栈与启动

- 后端：Python + FastAPI + Uvicorn，SQLite（WAL 模式，线程局部连接；连接建好即设
  `synchronous=NORMAL`、`temp_store=MEMORY`、`cache_size=-131072`≈128MB 页缓存、
  `mmap_size=512MB` 只读内存映射）。写入统一走 `db.tx()`，事务内实际变更后会
  `bump_data_version()` 让 §6.1 的内存缓存失效重建。
- 前端：单文件原生 HTML/JS（`static/index.html`），无构建步骤。
- 依赖：`fastapi`、`uvicorn`、`requests`、`python-multipart`（见 `requirements.txt`）。

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8306
# 浏览器打开 http://127.0.0.1:8306/
```

开发时建议加 `--reload`，否则改后端代码需手动重启进程才会生效：

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8306 --reload
```

数据库文件：`data/rail.db`（首次启动自动建表，见 `app/db.py` 的 `SCHEMA` 与 `init_db`）。

---

## 2. 目录结构

```
app/
  main.py      FastAPI 路由 + 后台任务（jobs）调度
  crawler.py   12306 抓取 + 自适应/手动限速
  parsers.py   各接口原始返回 -> 结构化记录（字段索引与官方一致）
  importer.py  留档(raw_payloads) + 解析 + 入库（爬虫与手动导入共用）
  db.py        SQLite 连接/事务/建表/车站与城市解析/设置
  transfer.py  换乘搜索引擎（A 大屏 / B 时刻表 / C 余票 / D 多程）
  fare.py      本地票价估算 + 用实际票价反推费率校准
static/
  index.html   前端单页（换乘搜索 / 爬取数据 / 导入数据 / 数据浏览 四个页签）
data/rail.db   本地数据库
samples/       示例原始返回（可用于手动导入测试）
diagnostics/   本地一次性诊断/基准脚本（被 .gitignore 忽略，不入库，见 §13）
```

数据流总览：

```
12306 接口 ──crawler──▶ 原始文本 ──importer.ingest──▶ raw_payloads(留档)
                                   └─parsers.parse_*─▶ 结构化记录 ─▶ 业务表
业务表 ──transfer.search──▶ 直达/换乘方案 ──前端渲染──▶ 排序/筛选/自动查价
```

---

## 3. 数据模型（`app/db.py`）

| 表 | 含义 | 唯一键 | 来源接口 |
|---|---|---|---|
| `stations` | 车站站名表（电报码/拼音/城市） | `telecode` | `station_name.js` |
| `station_trains` | **车站大屏**：某站某日经由的所有车次（含到发时刻/跨天/累计里程） | `(station_telecode, station_train_code, station_train_date)` | `bigScreen/queryTrainByStation` |
| `train_stops` | **车次时刻表**：某车次全部经停站 | `(train_no, train_date, station_no)` | `czxx/queryByTrainNo` |
| `tickets` | **站间余票**：某 OD 某日的车次与席别余票 | `(train_no, train_date, from_telecode, to_telecode)` | `leftTicket/query` |
| `prices` | **票价**：某车次某区间某席别价格 | `(train_no, train_date, from_station_no, to_station_no, seat_type)` | `leftTicket/queryTicketPrice` |
| `raw_payloads` | 原始返回留档（api_type/params/source/fetched_at/payload） | 自增 id | 所有 |
| `settings` | 键值设置（如限速模式） | `key` | — |

### 两个极易混淆的标识（二次修改务必注意）

- **`station_train_code`**：人类可读车次号，如 `G1274`、`Z126`。
- **`train_no`**：12306 内部哈希编号，如 `58000G12740B`、`5n0000Z1260J`。
  **同一车次在不同日期/不同接口里 `train_no` 可能不同**；跨表关联、查票价/时刻表都用 `train_no`，
  展示用 `station_train_code`。前端 `legKey()` 用 `train_no` 拼键。
- **`station_no`**：车次的经停序号（始发=1）。票价接口要求**两位零填充**字符串（`str(no).zfill(2)`），
  见 `transfer._price_map`、`main._plan_tickets_job`。`from_station_no`/`to_station_no` 在 `tickets`/`prices`
  里是 TEXT，比较前统一 `zfill(2)`。

### 入库覆盖策略（`importer._save`）

- `stations`：按 `telecode` upsert。
- `tickets`/`prices`：按唯一键 upsert（保留最新）。
- `train_stops`：**整份时刻表以最后一次抓取为准**——先 `DELETE` 该车次该日期旧停站再插入。
- `station_trains`：**该站该日经由车次以最后一次抓取为准**——先 `DELETE` 该站该日旧记录再插入
  （含已停运/改号的）。
- `prices` 唯一键列不允许 NULL（SQLite 中 NULL 互不相等会导致重复），入库前把空值置 `""`。

---

## 4. 抓取与限速（`app/crawler.py`）

单例 `crawler`，`requests.Session` + 全局锁串行限速。

### 接口清单

| 方法 | URL | 入参 | 入库类型 |
|---|---|---|---|
| `crawl_stations` | `kyfw/otn/resources/js/framework/station_name.js` | — | `stations` |
| `crawl_station_trains` | `mobile/wxxcx/wechat/bigScreen/queryTrainByStation` | `train_start_date`(YYYYMMDD), `train_station_code` | `station_screen` |
| `crawl_timetable` | `kyfw/otn/czxx/queryByTrainNo` | `train_no`, `from/to_station_telecode`, `depart_date` | `train_stops` |
| `crawl_tickets` | `kyfw/otn/{leftTicketUrl}` | `leftTicketDTO.train_date/from_station/to_station`, `purpose_codes=ADULT` | `left_ticket` |
| `crawl_price` | `kyfw/otn/leftTicket/queryTicketPrice` | `train_no`, `from/to_station_no`, `seat_types`, `train_date` | `price` |

- `ensure_init()`：先访问 `leftTicket/init` 解析出动态 `CLeftTicketUrl`；余票查询若返回
  `status:false + c_url` 会自动切换到新 URL 重试。
- `crawl_timetable` 网页接口失败时退回小程序 `getStopStation` 接口。
- 每个方法抓取后调用 `importer.ingest(..., source="crawl")`，原始返回全部留档。

### 限速（自适应 / 手动）

- 常量：`MIN_INTERVAL=1.0`（默认）、`MIN_FLOOR=0.4`（最快）、`MAX_CEIL=12.0`（最慢）。
- **自适应**：连续成功 3 次 → 间隔 ×0.8（不低于 floor）；请求异常/非 JSON/返回异常 →
  间隔 ×2+0.3（不高于 ceiling）。见 `_note_ok`/`_note_throttled`/`_throttle`。
- **手动**：`set_rate("manual", interval)` 固定间隔，写入 `settings`，重启后 `_load_rate` 恢复。
- 请求层 `_request` 自带 3 次重试（退避 2/4/6 秒）。
- 路由：`GET/POST /api/rate`。

> 修改限速行为：调 `MIN_FLOOR/MAX_CEIL/MIN_INTERVAL` 或 `_note_ok/_note_throttled` 的系数。

---

## 5. 解析（`app/parsers.py`）

- `detect_type(text)`：自动识别 `stations / left_ticket / price / station_screen / train_stops`。
- `parse_left_ticket`：竖线分隔字段，索引见 `LT` 字典；席别余票索引见 `SEAT_FIELDS`。
  **乘车日期以查询日期 `ctx.date` 为准**（`start_train_date` 是始发发车日，长途车会早一两天）。
- `parse_price`：键映射见 `PRICE_KEYS`（如 `O→二等座 ze`、`M→一等座 zy`、`A9/9→商务座 swz`）。
  `_price_value`：纯整数单位为**角**（÷10），带小数点为**元**；`--`/`无` 视为 None。
- `parse_train_stops`：`_find_stop_list` 兼容 `data.data / stop_stations / stations / result` 多种嵌套；
  `train_no` 取自 `ctx`（抓取时注入），电报码缺失时用站名表回填。
- `parse_station_screen`：解析大屏字段，含 `distance`（累计里程）、`running_time`（始发至本站历时）、
  `start_start_time`/`end_arrive_time`（始发发车/终到到达时刻）——这些是引擎 A 推导始发终到与里程的关键。
- 席别中文名统一在 `SEAT_NAMES`。

> 12306 改版时优先改这里：字段索引 `LT`/`SEAT_FIELDS`、价格键 `PRICE_KEYS`、`_find_stop_list`。

---

## 6. 换乘搜索引擎（`app/transfer.py`，核心）

入口 `search(from_q, to_q, date, min_wait=30, max_wait=360, limit=0, max_legs=2, expand_city=True)`。

### 6.1 准备

- `db.resolve_stations` 把出发/到达解析为车站列表；`expand_city=True` 时城市名展开为同城车站
  （`db.city_stations`：站名以城市名开头 + `CITY_PROPER` 别名，如武汉=武昌/汉口）。
- `fkeys`/`tkeys`：出发/到达站的**电报码与站名集合**（兼容不同数据源用码或用名）。
- 常驻缓存（`_get_graph_cache` / `_get_date_cache`）：全量路网只建一次并跨请求复用——
  `_build_graph_cache` 装入 `n2c`、`cities`（城市分组，用于同城异站换乘）、`cal`
  （`fare.load_calibration()` 费率校准）、`sno_base`（train_no+电报码→station_no）、`screen_all`
  （大屏）、`tt_all`（时刻表）、`services_all`（引擎 D 车次服务），并为每次查询附
  `screen_key_to_tn`/`tt_key_to_tn` 两张「站→车次」倒排索引，`search()` 靠它挑候选，
  不再遍历无关车次；`_get_date_cache` 再按日期缓存 `_price_map`（票价）、`_seat_map`（余票）、
  `real_tk` 与当日 `sno` 覆盖。失效口径为 `db.data_version()`（进程内写入代数 + 库文件与
  `-wal` 的 mtime/size），变更后自动重建；响应里的 `cache_hit` 标明本次是否复用。
- `real_tk`：当日已爬到的权威余票 OD→车次集合，用于剔除与实际冲突的推算方案。
- `_complete_screen_from_tt(screen, tt)`：大屏只记录"那天拍到的几个站"，行残缺。用 `train_no`
  （匹配不到再退回 `(车次码, 始发站, 终到站)`）找到停站更全的时刻表班次，把大屏缺的站补进 `obs`
  并标 `bridged`，票价/余票仍各自按大屏保留。缺站会让"已越过目的站""过头折返"这类按 `station_no`
  的剪枝失明，补全后才会正确触发（反向判定见 §12 第 8 条）。因 `screen_all` 是跨请求共享的缓存对象，
  补全放在 `_build_graph_cache` 里、且在倒排索引与 `services_all` 构建**之前**只做一次。

### 6.2 四个引擎（结果合并去重）

- **A 车站大屏 `_search_screen`**：`_load_screen` 把大屏按 `(train_no, 开行日)` 聚合成"开行实例"，
  每站观测含到/发时刻（绝对分钟）、station_no、累计里程；并用 `running_time`+`end_arrive_time`
  推导始发/终到时刻。**真实开行日期优先**；无当日数据时取最近班次按"每日开行假设"平移到查询日
  （`shift`，`date_assumed=True`）。
- **B 车次时刻表 `_search_timetable`**：`_load_trains` 读 `train_stops`，按到发时刻递增推算跨天，
  统一为相对始发的分钟数。全程按"每日开行假设"推算日期（`date_assumed=True`）。
- **C 站间余票**：仅有余票数据、A/B 未覆盖的直达，按精确日期补充（`search` 末段）。
- **D 多程换乘 `_search_multi`**（`max_legs>=3` 时启用）：`_load_services` 把大屏+时刻表统一成
  "车次服务"，BFS 逐层扩展，`FRONTIER_K=4` 每站每层保留最早到达的 4 个状态剪枝，
  `MAX_TOTAL_MIN=2880`（48h）总行程上限防组合爆炸。
  未开启支配剪枝时走"入桶即截断"的有界桶：每个中转站只维护"最快到达/最低费用"两维 top-K，
  落选分支立即释放，结果与"整层建完再排序截断"一致（稳定排序的平局用插入序号还原），只为压内存峰值。

### 6.3 换乘约束与剪枝

- 换乘等待 `wait` 须满足 `min_wait <= wait <= max_wait`；**同城异站换乘额外 +30 分钟**
  （`eff_min = min_wait if same else min_wait+30`，见 `_dep_keys`）。
- 跨天：`n = ceil((到达 + eff_min - 第二程发车) / 1440)` 自动取下一班。
- **剔除折返/绕回**：第二程重新经过出发站、第一程走过的站、或再次回到换乘站 → 丢弃。
  **过头折返**同计入 `loopback`：第二程/末程在最后一个目的站**之后**又停到第一程已走过的站
  （全程成环），A/B/D 三个引擎都判这一条。
- `_assumed_conflict`：推算方案（`date_assumed`）若其任一程的 OD+日期已有权威余票、但该车次不在
  当日余票里（即当日不开行）→ 剔除，以实际余票为准。

### 6.4 合并与排序

- 直达按 `(首程train_no, 首程depart)` 去重；换乘按 `(首程no, 二程no, 换乘站, 二程from, 首程depart)` 去重。
- `rank(p)`：**有真实票价的优先**，其次非推算（真实日期）优先。
- 最终按 `total_min` 升序。返回含 `direct`/`transfers`/`screen_trains`/`timetable_trains`/`cache_hit`/`note`。

### 6.5 票价/余票富化 `_enrich`

每程 leg 依次尝试：
1. 有 `from/to_station_no`（或由 `sno` 回填）→ 查 `_price_map` 得真实 `prices`。
2. 无真实票价 → `fare.estimate` 得 `price_est`（估算区间）。
3. 查 `_seat_map`（train_no+OD+日期）得 `seats`/`can_web_buy`。

> 修改换乘行为的关键参数：`min_wait/max_wait`（API 传入）、异站 +30（`_search_screen`/`_search_timetable`/
> `_search_multi` 的 `eff_min`）、`MAX_TOTAL_MIN`、`FRONTIER_K`、`MAX_TIMETABLES`（main.py 向导封顶）。

---

## 7. 票价估算与校准（`app/fare.py`）

- `RATES`：各车型（G/C/D/Z/T/K/P）各席别的**元/km 参考区间**；`SPEED`：各车型 km/h 区间。
- `estimate(code, km, duration_min, cal)`：
  - 有精确里程 `km`（大屏 `distance` 之差）→ 直接用；
  - 否则按 `历时 × 速度区间` 推算里程区间；
  - 每席别 `min=floor(klo*rlo*2)/2`、`max=ceil(khi*rhi*2)/2`（取整到 0.5 元）。
- `load_calibration()`：用**已爬实际票价 ÷ 里程**反推费率样本，按 (车型,席别) 聚合，
  估算时用 `(min(obs)*0.9, max(obs)*1.1)` 覆盖参考费率（结果标注"已校准"）。
- 结果均为**参考区间，非官方票价**；前端以"估"标记。

> 调票价估算：改 `RATES`/`SPEED`；校准口径改 `load_calibration`。

---

## 8. 后台任务（`app/main.py` 的 `jobs`）

`jobs: dict[job_id, dict]`，线程执行，前端轮询 `GET /api/jobs/{id}`。任务字典含
`status/done/total/errors/phase/current/log/result` 等。

- **`_price_job`**：`/api/crawl/tickets?with_prices=true` 触发，按已入库 tickets 逐车次抓票价。
- **`_route_job`**（一键导入/在线模式）：`/api/import/route` 触发，阶段
  `screen→timetable→tickets→price`。先抓出发/到达全部车站大屏；`deep` 时再抓最多
  `MAX_TIMETABLES=200` 个车次时刻表、各 OD 余票、各车次票价。
- **`_plan_tickets_job`**（方案实时查价/余票）：`/api/plan_tickets` 触发，见下节。

### 方案票价/余票实时查询 `/api/plan_tickets`

1. 入参 `legs[]`（train_no/date/from_telecode/to_telecode/from_name/to_name），按
   `_leg_ident` 去重，生成 `key = "train_no|from|to|date"`（与前端 `legKey()` 完全一致）。
2. 对每个唯一 OD+日期：**日期 >= 今天**才实时 `crawl_tickets`（过去日期只用缓存）。
3. 从入库 tickets 取余票；station_no 优先用 tickets 的 `from/to_station_no`，缺失用 `sno` 回填。
4. 票价：已有缓存跳过，否则 `crawl_price`（同样仅未来日期）。
5. 输出 `result[key] = {seats, can_web_buy, prices:[{seat,price}]}`；无余票则 `error`。

> 前端 `autoQuery()` 取**当前排序+筛选后**的前 50 个未发车方案的腿，POST 后轮询，
> `applyTickets()` 按 key 回填。切换排序/筛选会 `refreshAutoQuery()` 防抖 900ms 重查。

---

## 9. 时刻表查看 `/api/timetable`（含大屏退回）

`GET /api/timetable?train_no=X` 返回 `{stops, complete, source}`：

1. `train_stops` 有该车次 → 取最新日期全部停站，`complete=true, source="train_stops"`。
2. 否则**退回车站大屏**：把 `station_trains` 中该车次已抓取各站（按 `station_no` 去重、取最新日期）
   拼成停站列表，`complete=false, source="station_screen"`（可能不完整）。
3. 两者皆无 → 404。

前端 `toggleTT`/`renderTT`：`complete=false` 时显示已知停站 + "抓取完整时刻表"按钮
（`crawlTTPlan` → `/api/crawl/timetable`）。`/api/crawl/timetable` 用 `station_timetable_context`
（crawler.py）从大屏/余票回填始发终到电报码与日期。

> 注意：**抓车站大屏 ≠ 抓车次时刻表**。大屏只给"该站这一 stop"，完整时刻表需 `queryByTrainNo`。
> 退回逻辑让已抓大屏的车次也能展示部分经停，而不是直接"未入库"。

---

## 10. API 端点速查（`app/main.py`）

| 方法 路径 | 作用 |
|---|---|
| `GET /` | 前端页面 |
| `GET /api/stats` | 各表计数 |
| `GET /api/stations?q=&limit=` | 车站搜索（站名/电报码/拼音/简拼） |
| `GET/POST /api/rate` | 读取/设置限速 |
| `POST /api/import` | 手动导入原始文本（可指定 api_type 与 ctx） |
| `POST /api/import/file` | 上传文件导入 |
| `POST /api/crawl/stations` | 抓站名表 |
| `POST /api/crawl/station_trains` | 抓车站大屏 `{station,date}` |
| `POST /api/crawl/timetable` | 抓车次时刻表 `{train_no,date?,from?,to?}` |
| `POST /api/crawl/tickets` | 抓站间余票 `{from_station,to_station,date,with_prices?}` |
| `POST /api/crawl/price` | 抓单票价 `{train_no,from_station_no,to_station_no,seat_types,date}` |
| `POST /api/import/route` | 一键导入向导 `{from_city,to_city,date,deep}` → job |
| `GET /api/jobs/{id}` | 任务进度 |
| `GET /api/station_trains?station=&date=` | 浏览大屏车次 |
| `GET /api/timetable?train_no=` | 时刻表（含大屏退回） |
| `GET /api/trains?q=&limit=` | 按车次号搜时刻表 |
| `GET /api/tickets?from_station=&to_station=&date=` | 浏览余票 |
| `GET /api/prices?train_no=` | 浏览票价 |
| `GET /api/transfers?...` | 换乘搜索（见 §6 参数） |
| `POST /api/plan_tickets` | 方案实时查价/余票 → job |

---

## 11. 前端逻辑（`static/index.html`）

四个页签：换乘搜索 / 爬取数据 / 导入数据 / 数据浏览。核心 JS：

- 搜索：`#s-go` → （在线模式先 `runRouteImport`）→ `GET /api/transfers` → 给方案打 `_k/_type` → `render()`。
- `legKey(l)`：`train_no|from_telecode|to_telecode|depart[:10]`，与后端 `_leg_ident` 对齐。
- `priceKey(p)`：返回 `[miss?1:0, est?1:0, sum]`——**实际价优先**（有真实价排前，按实际价升序），
  其次有估价，最后按金额。`sortPlans` 价格模式据此排序。
- `filterPlans`：方案类型/仅换乘/席位/换乘站/换乘类型/车次包含/车次头/出发到达时间等筛选。
  （"直达 0"通常是开了"仅换乘"或换乘站筛选所致，非 bug。）
- 服务端剪枝可发现性：`refreshPruneToggleBadge()` 把仍生效的筛选值**直接写在折叠的"自定义规则"按钮上**
  （`自定义规则 ▾ ⚠生效1：Z,D`，超 34 字截断，完整口径在 `title`），并给面板里非空的那四个框描红边、
  淡红底（`customFilterRaw` 是这四个值的唯一来源，`customFilterParts`/按钮/明细都从它取）；输入时
  `input` 事件即时刷新，不用等失焦。`pruneBreakdownItems`/`appliedFilterParts` 把拦截明细写成
  `自定义规则 N（已生效：只含车型头「Z,D」…）`，取值优先用响应里的 `prune_config` 回显；
  只要有筛选生效，诊断区就给"清除自定义筛选并重搜"（`clearCustomFilters`）。
  注意可见的"车次筛选"`#s-filter` 是**前端显示过滤**（支持 `Z126` 这类完整车次号、任一腿命中即保留），
  与面板里的服务端 `#s-inc-pfx`（只认车型头前缀、搜索期逐腿剪枝）是两个不同的框。
- `autoQuery()`：`sortPlans(filterPlans(...)).filter(!departed).slice(0,50)` → flatMap 腿 →
  `POST /api/plan_tickets` → `pollPlanJob` → `applyTickets` 按 key 回填真实价/余票。
- `refreshAutoQuery()`：排序/筛选变化后防抖 900ms 重查（仅勾选"自动查询票价/余票"时）。
- 时刻表展开：`toggleTT`/`renderTT`/`ttTable`/`crawlTTPlan`（见 §9）。
- 爬取页：`crawlTT`/`showTimetable`（模态框）、`renderTickets` 等。

> 改前端排序/筛选/自动查询逻辑都在 `index.html` 的 `priceKey/sortPlans/filterPlans/autoQuery` 几处。

---

## 12. 二次修改常见入口

| 想改什么 | 去哪改 |
|---|---|
| 12306 字段/接口改版 | `parsers.py`（`LT`/`SEAT_FIELDS`/`PRICE_KEYS`/`_find_stop_list`）、`crawler.py`（URL/参数） |
| 限速快慢/风控策略 | `crawler.py`（`MIN_FLOOR/MAX_CEIL`、`_note_ok/_note_throttled`） |
| 换乘最短/最长等待、异站预留 | API `min_wait/max_wait`；`transfer.py` 各处 `eff_min`（异站 +30） |
| 多程剪枝/总时长上限 | `transfer.py`（`FRONTIER_K`、`MAX_TOTAL_MIN`）；`main.py`（`MAX_TIMETABLES`） |
| 票价估算费率/速度 | `fare.py`（`RATES`/`SPEED`/`load_calibration`） |
| 城市展开/别名 | `db.py`（`city_stations`、`CITY_PROPER`） |
| 常驻路网/日期缓存、候选筛选 | `transfer.py`（`_build_graph_cache`/`_get_graph_cache`/`_get_date_cache`、`screen_key_to_tn`/`tt_key_to_tn`）、`db.py`（`data_version`/`bump_data_version`） |
| SQLite 读盘性能（页缓存/mmap/同步级别） | `db.py` 连接初始化处的 `PRAGMA` |
| 排序"实际价优先"口径 | 前端 `priceKey`；后端 `transfer.rank` |
| 时刻表退回/完整性 | `main.py` `/api/timetable`；前端 `renderTT` |
| 新增一类接口 | `parsers.parse_x` + `importer.ingest/_save` + `crawler.crawl_x` + `main` 路由 + `db` 建表 |

### 必须遵守的约定（踩坑点）

1. 跨表关联用 `train_no`（哈希），展示用 `station_train_code`；二者不可混用。
2. 票价/余票的 `station_no` 比较前 `zfill(2)`；`prices` 唯一键列空值置 `""` 而非 NULL。
3. 余票乘车日期以**查询日期**为准，勿用 `start_train_date`（始发日）。
4. 大屏/时刻表入库是"先删后插"的整份覆盖；余票/票价是按唯一键 upsert。
5. 推算方案 `date_assumed=True`，会被 `_assumed_conflict` 用当日权威余票校验剔除。
6. 改后端代码后需重启 uvicorn（或启用 `--reload`）；前端改 `index.html` 刷新浏览器即可。
7. 所有抓取原始返回都进 `raw_payloads`，排查解析问题先查留档（`source` 区分 crawl/import/test）。
8. 一个 `train_no` = 一趟物理车次的**单一方向**，途中会换显示车次码（厦门北→兰州记 `Z126/Z127`，
   兰州→厦门北是另一个 `train_no`，记 `Z125/Z128`，且停站表不同）。因此"大屏车次码与时刻表车次码
   不一致"**不能**当作反向证据剔除，反向/缺站只能靠 `train_no` 与 `station_no` 顺序判断。
9. "自定义规则"面板折叠后，`搜索只含车型头`/`剪除车型头`/`必经中转站`/`排除中转站` 四个文本框
   **仍然生效**，且切换剪枝预设（严格/均衡/宽松）不会清空它们。现在这四个值会同时出现在三处：按钮上
   `⚠生效N：Z,D`、展开时框体的红边淡红底、拦截明细 `自定义规则 N（已生效：…）`；结果异常少时先看这三处，
   别急着怀疑算法或数据。尤其别把可见的"车次筛选"框（前端显示过滤）当成它——那个是空的也可能有生效值。
10. `screen_all`/`tt_all`/`services_all` 和两张倒排索引是**进程内跨请求共享**的缓存对象，只读：
    不要在 `search()` 里原地改写（会污染下一个查询），也别指望 `del screen, tt` 省内存（删的只是
    本地引用，删早了末尾统计就取不到值）。要调整停靠站口径就改 `_build_graph_cache`，一次构建全生效。
11. 缓存过期只看 `db.data_version()`：进程内 `db.tx()` 写入代数 + `data/rail.db` 与 `-wal` 的
    mtime/size。用外部脚本直接改库文件而没动这两项时，需重启 uvicorn 才会重建。

---

## 13. 本地诊断脚本（`diagnostics/`，不入库）

排查线上现象留下的一次性脚本，已写进 `.gitignore`，仓库里看不到；是否保留由本机自行决定。

- 一律从仓库根执行：`python diagnostics/<脚本名>.py`。`diagnostics/_diag_env.py` 负责把仓库根加入
  `sys.path` 并 `chdir` 到仓库根，所以脚本内仍按原样写 `data/rail.db`、`from app import transfer`。
- 按依赖分三类，各脚本 docstring 的"用法"行已标明是哪一类：
  1. **打 HTTP 接口**（`diag_1121*.py`、`diag_api_filters.py`、`diag_fingerprint.py`、`diag_fp2/3.py`）：
     需先在 `127.0.0.1:8306` 起服务，端口可作第 1 个参数。
  2. **进程内跑引擎**（`verify_complete.py`、`verify_fix_xmb_pt.py`、`_abfilter*.py`、`bench_*.py`、
     `diag_z12x_*.py`、`diag_price_rank.py`、`diag_1122_sm.py` 等）：直接 import `app.transfer`，
     不需要起服务，改完后端可先用它自检，再重启 uvicorn 验证接口。
  3. **只查库/带参数**（`diag_sm_b_22.py` 只读 SQLite；`ab_counts.py`、`ab_z126.py`、`ab_bounded.py`
     接受 `<module>` 参数，用于对比 HEAD 版本与当前版本的引擎结果）。
- `diagnostics/_premove_backup/` 是从仓库根搬来的原件备份；`diagnostics/_scratch/` 是脚本输出与
  临时服务日志。两者都可随时清理。
- 留档案例（2026-11-21）：查"厦门北→南平市 只有莆田换乘、没有三明北"，`diag_fingerprint.py` 用截图
  指纹反查出请求里残留的"搜索只含车型头 = Z,D"，算出的 `自定义规则 447115` 与截图逐位相同——
  三明北→南平市 的车次全是 G 字头，被白名单剪光，只剩 D 字头经莆田。根因是折叠的文本筛选，
  不是算法或数据问题；这也催生了 §11/§12 那两处可发现性改动。
