# 12306 智能中转

本地运行的火车票**直达 / 中转换乘**规划工具。抓取并留档 12306 公开查询接口的原始返回，
入库后在本地做换乘组合搜索、票价估算与实时票价/余票查询。全部数据落在本地 SQLite，
可离线复算；联网仅用于按需抓取。

> 仅访问 12306 公开查询接口，全局限速，原始返回全部留档。请勿用于高频抢票/刷票。

---

## 0. 阅读分层（先选你的深度）

本仓库读者分两类，全文各章节按 **[概念]** 与 **[内核]** 标注分层：

- **[概念] 查票原理与表面设计** —— 面向一般专业人员：工具怎么用、数据怎么抓、一次换乘查询/爬取的
  完整链路、接口与前端如何组织。不含算法细节。
- **[内核] 搜索算法与底层设计** —— 面向二次开发/深度排障：组合搜索的算法内核（图遍历、多程 BFS、
  剪枝、支配）、跨表关联口径、缓存失效机制、根因级踩坑约定。

> 编号与所有交叉引用（如 §6.6 引 §12 第 10 条）保持不变，仅在标题尾追加 `[概念]`/`[内核]` 标签。
> 只想看懂「如何得到直达/中转 + 票价/余票」→ 通读 **[概念]** 即可；要改算法或排查「结果偏少」→ 从 §6、§12 入手。

| 层级 | 章节 |
|---|---|
| **[概念]** 查票原理与表面设计 | §1 技术栈与启动、§2 目录结构、§4 抓取与限速、§8 后台任务、§9 时刻表查看、§10 API 速查、§11 前端逻辑 |
| **[内核]** 搜索算法与底层设计 | §3 数据模型标识、§5 解析、§6 换乘搜索引擎（图遍历/BFS/剪枝/缓存）、§7 票价估算校准、§12 二次修改入口与踩坑点、§13 诊断脚本 |

---

## 1. 技术栈与启动 [概念]

- 后端：Python（FastAPI + Uvicorn，并内置基于标准库 `http.server` 的无三方依赖兜底服务端 `app/server.py`），SQLite（WAL 模式，线程局部连接；连接建好即设
  `synchronous=NORMAL`、`temp_store=MEMORY`、`cache_size=-131072`≈128MB 页缓存、
  `mmap_size=512MB` 只读内存映射）。写入统一走 `db.tx()`，事务内实际变更后会
  `bump_data_version()` 让 §6.1 的内存缓存失效重建。
- 前端：单文件原生 HTML/JS（`static/index.html`），无构建步骤。
- 依赖：`fastapi`、`uvicorn`、`requests`、`python-multipart`（见 `requirements.txt`）；离线/精简环境也可直接用标准库运行 `app/server.py`。

```bash
pip install -r requirements.txt
python -m app.server --port 3000
# 或直接使用 uvicorn（需端口自定义时调整 --port）：
python -m uvicorn app.main:app --host 127.0.0.1 --port 8306
```

开发时若用 uvicorn 建议加 `--reload`，否则改后端代码需手动重启进程才会生效：

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8306 --reload
```

数据库文件：`data/rail.db`（首次启动自动建表，见 `app/db.py` 的 `SCHEMA` 与 `init_db`）。

### 1.1 新库第一次要有数据：一条命令初始化

不用开前端，直接跑（细节与参数见 §4「初始化全量爬取」）：

```bash
python init_crawl.py --dry-run   # 先看要发多少请求、大概多久（不联网，也不改限速设置）
python init_crawl.py             # 站名表 → 全部车站大屏 → 全部车次时刻表
```

目标日期缺省为北京时间**明天**（与前端各处日期口径一致）。中断后重跑同一条命令自动续爬；
脚本只增不减，不会删除或清空任何已爬数据。

---

## 2. 目录结构 [概念]

```
app/
  main.py            路由定义 + 参数校验 + 后台任务（jobs）调度（含无 FastAPI 兼容层）
  server.py          标准库 ThreadedHTTPServer 入口（优先 uvicorn，无三方库时兜底启动）
  crawler.py         12306 抓取 + 自适应/手动限速
  parsers.py         各接口原始返回 -> 结构化记录（字段索引与官方一致）
  importer.py        留档(raw_payloads) + 解析 + 入库（爬虫与手动导入共用）
  db.py              SQLite 连接/事务/建表/车站与城市解析/换乘组与站对规则/设置
  geo.py             方案 C 地理邻接索引（0.5° 动态空间格网）、连续单调计价、3 档重轨邻近减时、合规抓取与质检
  transfer.py        换乘搜索引擎（A 大屏 / B 时刻表 / C 余票 / D 多程，含地理半径+5 层优先级异站模型）
  fare.py            本地票价估算 + 用实际票价反推费率校准
static/
  index.html         前端单页（换乘搜索 / 爬取数据 / 导入数据 / 数据浏览 / 显示设置 五个页签）
tests/
  test_core.py       核心单元测试（解析、剪枝、缓存失效、方案 C 计价/格网/优先级/抓取质检、HTTP 路由回归）
crawl_nationwide.py     全国全量离线爬取脚本（旧版简洁版）
crawl_all_timetables.py  全国全量车站经由车次 + 去重全车次时刻表爬取脚本（默认明天、支持分阶段/按站筛选/已有时刻表跳过/断点重试）
crawl_geo_coords.py      车站地理坐标与 43 城重轨站点合规批量抓取 CLI（Overpass 切片+Wikidata 补盲+多维质检，支持试跑与断点续跑）
init_crawl.py          一键初始化（体检→复用 crawl_all_timetables 跑完全部车站大屏+全部车次时刻表→收尾核对），见 §4「初始化全量爬取」
package.json             dev/start 启动脚本（调用 python3 -m app.server --port 3000）
data/rail.db         本地数据库
samples/             示例原始返回（可用于手动导入测试）
diagnostics/         本地一次性诊断/基准脚本（被 .gitignore 忽略，不入库，见 §13）
```

数据流总览：

```
12306 接口 ──crawler──▶ 原始文本 ──importer.ingest──▶ raw_payloads(留档)
                                   └─parsers.parse_*─▶ 结构化记录 ─▶ 业务表
OSM Overpass / Wikidata ──geo/crawl_geo_coords──▶ stations 经纬度 + rail_transit_stops ─▶ 多维质检
业务表 + 地理/规则表 ──transfer.search──▶ 直达/换乘方案 ──前端渲染──▶ 排序/筛选/自动查价
```

---

## 3. 数据模型（`app/db.py`）[内核]

| 表 | 含义 | 唯一键 | 来源接口 |
|---|---|---|---|
| `stations` | 车站站名表（电报码/拼音/城市 + 经纬度、coord_status 质检态、coord_grid、transit_zone_id、重轨距 `nearest_rail_stop_distance_m` / `rail_access_mode`、`is_major_hub`） | `telecode` | `station_name.js` + Overpass/Wikidata |
| `rail_transit_stops` | **城市重轨站点**：全国 ~43 城地铁/轻轨/市域/单轨站点（已剔除纯有轨电车） | `(name, city, mode, lat, lon)` | Overpass 切片同步采集 |
| `station_trains` | **车站大屏**：某站某日经由的所有车次（含到发时刻/跨天/累计里程） | `(station_telecode, station_train_code, station_train_date)` | `bigScreen/queryTrainByStation` |
| `train_stops` | **车次时刻表**：某车次全部经停站 | `(train_no, train_date, station_no)` | `czxx/queryByTrainNo` |
| `tickets` | **站间余票**：某 OD 某日的车次与席别余票 | `(train_no, train_date, from_telecode, to_telecode)` | `leftTicket/query` |
| `prices` | **票价**：某车次某区间某席别价格 | `(train_no, train_date, from_station_no, to_station_no, seat_type)` | `leftTicket/queryTicketPrice` |
| `transfer_groups` / `transfer_group_members` | **自定义换乘组**：用户手动维护的多站换乘组（上限 30 站，`full_mesh` 最小 `>=5m` / `hub_star` 放射） | `name` / `(group_id, telecode)` | UI / API |
| `transfer_pair_overrides` | **站对强控规则**：任意两站强制连通（`allow`，可设 `custom_extra_min>=0`）或禁止换乘（`block`） | `(a_telecode, b_telecode)`（有序去重） | UI / API |
| `raw_payloads` | 原始返回留档（api_type/params/source/fetched_at/payload） | 自增 id | 所有 |
| `settings` | 键值设置（如限速模式、`transit_city_overrides` 等） | `key` | — |

> **旧库迁移顺序不变量（`db.init_db`）**：`SCHEMA` 字符串内**不可包含**依赖新增列（如 `coord_status`、`coord_grid`、`transit_zone_id`）的 `CREATE INDEX`；旧版 `rail.db` 升库时必须先在 `init_db()` 跑完 `ALTER TABLE stations ADD COLUMN ...` 迁移并填好默认值后，再创建 `idx_stations_coords` / `idx_stations_grid` / `idx_stations_zone`，否则旧库首次启动即因列不存在报错（见 §12 第 18 条）。

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

## 4. 抓取与限速（`app/crawler.py`）[概念]

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

### 初始化全量爬取（`init_crawl.py`）

脚本自己不发任何请求，抓取与入库全部复用 `crawl_all_timetables.run_crawl(phase="all")`；
它只补初始化真正需要的三件事：开工前体检（工作量 + ETA）、收尾核对（是否真覆盖全）、
把 `crawl_all_timetables.py` 那十几个运维参数收敛成一条安全默认的命令。

| 阶段 | 动作 | 落到哪 |
|---|---|---|
| 0 | `crawl_stations()` 同步官方 `station_name.js` | `stations`；失败则降级用库内已有站表，库也空 → 退出码 2 |
| 1 | 逐站 `crawl_station_trains(telecode, date)` | `station_trains`，全国约 3.4 千站 |
| 2 | 从大屏按 `train_no` 聚合去重，逐趟 `crawl_timetable(train_no, date, from, to)` | `train_stops`，约 1.2 万车次 |

- **顺序不能反**：阶段 2 的车次清单来自阶段 1 入库的大屏记录，所以体检里阶段 2 的数字
  在大屏爬完之前必然偏小（全新库甚至是 0），这是设计而非漏统计。
- **安全默认**：`skip_db_screens=True` 且不传 `force` —— 当日已有大屏的站跳过、库内已有
  时刻表的车次跳过，重复执行只增不减。确需推倒重爬请显式用
  `python crawl_all_timetables.py --force`（会重发全量请求，脚本不提供这个开关）。
- **想先小规模试跑**：本脚本固定全国全量；只爬某几个站/某个城市请用
  `python crawl_all_timetables.py --stations 厦门 --date 2026-11-23`（或 `--limit 20`）。
  两者共用同一份断点文件，所以试跑成功的站在正式初始化时不会重复爬。
- **断点**：`data/crawl_all_timetables_state.json`，按 `date` 分组，日期一变旧断点自动作废。
  Ctrl+C / 断电 / 进程被杀后重跑**同一条命令**即续爬；`--state PATH` 可给并行任务各配一份断点，
  互不干扰。
- **"无记录站"不等于失败**：小站当日确实可能一趟图定车都不停，接口返回的
  `无数据/暂无/不存在` 按成功计入断点（关键词与 §12 第 12 条同一套）。收尾只看断点里
  记账的 `failed_stations/failed_trains` 和仍缺的时刻表来判定完成与否。
- **限速**：默认沿用自适应间隔；`--interval 1.5` 切固定间隔，会写进 `settings` 供交互端共用
  （`--dry-run` 不写）。12306 每日 23:30–05:00 维护，脚本自动休眠等到 05:00 继续。
- **耗时**：全量约 1.5 万次请求，按 1 秒/请求约 4 小时起；一旦触发风控，自适应间隔会成倍拉长
  （上限 `MAX_CEIL=12` 秒/请求），体检行同时给出 `MIN_FLOOR`–`MAX_CEIL` 的耗时区间，
  据此决定要不要挂机跑。
- **退出码**：`0` 跑完（仍有失败项时打印待补数量）；`130` 手动中断（断点已保存）；
  `2` 站名表为空，本次实际什么都没爬。
- 只爬车站大屏与车次时刻表，**不碰余票与票价**（那两类按 §8 的接口即时查询），
  与"联网只按需抓取公开接口"的产品口径一致。

### 车站地理坐标与重轨站点合规抓取（`app/geo.py` + `crawl_geo_coords.py`）

为支撑方案 C 物理距离异站换乘与 3 档重轨邻近减时，提供合规、可断点续跑的 CLI 工具与后台任务 `/api/geo/crawl/start`：

```bash
python3 crawl_geo_coords.py --trial-tiles 2     # 先试跑 2 个切片估算配额与带宽，不做 Wikidata 全量
python3 crawl_geo_coords.py                     # 12 个 Overpass 区域切片 → Wikidata 补漏 → 重轨距离重算 → 多维质检
python3 crawl_geo_coords.py --audit-only        # 仅离线运行多维质检与重轨距离重算（不联网）
```

- **两阶段合规批量流水线**：
  1. **Overpass 12 区域切片**（`CHINA_OVERPASS_TILES`）：按中国大陆 12 块 BBox 串行拉取 `railway~station|halt`（默认请求间隔 `3.5s`，支持备用端点切换与 `--delay` 调整），同一次响应**同步采集国铁车站与交通运输部 2025 口径约 43 个核心城市重轨站点**（地铁/轻轨/市域铁路/单轨写入 `rail_transit_stops`，**显式剔除纯有轨电车城市**如红河、文山、嘉兴市区等，避免低频电车误触发重轨减时）。
  2. **Wikidata SPARQL 一次性补漏**：针对仍无坐标的国铁站，单次 SPARQL 查询境内车站（`wd:Q55488` + `wdt:P17 wd:Q148` + `wdt:P625`）批量补盲。
- **Nominatim 合规硬约束**：**严禁**在任何批量抓取流中调用 Nominatim（触发 OSM 1 req/s 使用政策封禁）；`POST /api/geo/nominatim/{telecode}` 仅开放给 UI「疑似/缺坐标站维护」表格的单站人工点查，内置强制 `≥1.5s` 全局冷却锁（`NOMINATIM_COOLDOWN_SEC=1.5`）。
- **断点与人工保护**：断点落 `data/crawl_geo_coords_state.json`（记录已完成切片、`wikidata_done`、流量统计），`--reset`/`--force` 可重置；`coord_status='verified'` 的人工校准站永久受保护，不会被批量流覆盖。
- **多维坐标质检（`audit_station_coords`）**：入库后自动跑三道校验并标出 `coord_status='suspect'`（搜前自动隔离，仅展示于维护队列）：
  1. 中国边界盒检验（`CHINA_BBOX`）；
  2. 同城中位离群检验（同 `city` 站距城市坐标中位数 `>150km` 标疑，附带 12306「站挂邻县市」豁免意识提示）；
  3. **`train_stops` 物理速度校验**：遍历同车次相邻经停站，若 `球面距离 / 时刻差 > 420km/h`（飞站异常）自动标记相关站点 `suspect` 与 `coord_issue` 供人工核查。

---

## 5. 解析（`app/parsers.py`）[内核]

- `detect_type(text)`：自动识别 `stations / left_ticket / price / station_screen / train_stops`。
- `parse_left_ticket`：竖线分隔字段，索引见 `LT` 字典；席别余票索引见 `SEAT_FIELDS`。
  **乘车日期以查询日期 `ctx.date` 为准**（`start_train_date` 是始发发车日，长途车会早一两天）。
- `parse_left_ticket_prices`：从 `leftTicket` 字段 `[39]`（`yp_info_new`）每 10 字符一段解析实时票价与无座等嵌入价格，与余票同步写入 `prices`（`from_station_no`/`to_station_no` 非空时在入库阶段统一 `zfill(2)`）。
- `parse_price`：键映射见 `PRICE_KEYS`（如 `O→二等座 ze`、`M→一等座 zy`、`A9/9→商务座 swz`）。
  `_price_value`：纯整数单位为**角**（÷10），带小数点为**元**；`--`/`无` 视为 None。
- `parse_train_stops`：`_find_stop_list` 兼容 `data.data / stop_stations / stations / result` 多种嵌套；
  `train_no` 取自 `ctx`（抓取时注入），电报码缺失时用站名表回填。
- `parse_station_screen`：解析大屏字段，含 `distance`（累计里程）、`running_time`（始发至本站历时）、
  `arrive_day_diff`/`start_day_diff`（相对始发日跨天偏移）、`start_start_time`/`end_arrive_time`（始发发车/终到到达时刻）——这些是引擎 A 推导始发终到与里程的关键。
- `running_fig_allows(fig, target_date)`：解析运行图区段（`起始#周期#步长#截止`）校验某日是否开行。
- 席别中文名统一在 `SEAT_NAMES`。

> 12306 改版时优先改这里：字段索引 `LT`/`SEAT_FIELDS`、价格键 `PRICE_KEYS`、`_find_stop_list`。

---

## 6. 换乘搜索引擎（`app/transfer.py`，核心）[内核]

入口 `search(from_q, to_q, date, min_wait=30, max_wait=360, limit=0, max_legs=2, expand_city=True, cross_extra=60, frontier_k=None, prune_level="balanced", max_total_min=None, detour_ratio=None, prune_dominated=None, allow_cross_station=True, hub_only=False, hub_stations="", allow_prefixes="", exclude_prefixes="", include_stations="", exclude_stations="", include_stations_mode="or", city_mode=None, from_city_mode=None, to_city_mode=None, cross_radius_km=20.0, cross_time_mode="smart", transit_discount_on=True, transit_discount_min=15, transit_proximity_m=1500, active_group_ids=None, fallback_ungeocoded=False, progress_cb=None, cancel_event=None, sub_cb=None)`；独立后置过滤器 `filter_plans(...)`。

### 6.1 准备

- `db.resolve_stations(q, expand_city=True, city_mode=None)` 把出发/到达解析为车站列表，`transfer.search()` 与 HTTP 接口同时支持 `from_city_mode`/`to_city_mode`（缺省回退 `city_mode`，前端出发/到达两端可独立选择①–⑤）：
  `single`=仅该站本身（站名或电报码精确，绝不换站）；`name`=市内站名以城市名打头的站
  （`db.city_stations` + `CITY_PROPER` 别名，如武汉=武昌/汉口，即旧 `expand_city=True` 行为）；
  `string`=token 写作 `城市:字样`，取该市 `city` 归属且站名含字样的站（`南平:延平`→
  延平/延平东/延平西；有冒号未命中时不回退单站，无冒号退化为 `all`）；`mixed`=逗号/顿号/分号分隔列表
  取并集，无冒号 token 仅按单站精确解析、亦支持 `name:/all:/城市:字样` 混合组合（任一 token 未匹配即整体失败）；
  `all`=该地级市 `city` 列归属的全部车站（含晋江/惠安/德化等郊县站，`db.city_all_stations`）。
  **WHY**：12306 的"同城聚合"是可勾选扩展（查询页有同城车站勾选项），但官网/App 输入框的
  地名→车站匹配层会把郊县站引到市内主站（建阳→南平市站）；本站把口径显式交给用户，
  `single` 档保证绝不换站。前端配套：每个端点内置独立下拉（`#s-from-mode`/`#s-to-mode`）与实时解析预览（调用 `/api/stations/resolve` 展示已匹配站点明细）；③档左侧只检索城市（`only_cities=1`）、右侧聚焦下拉该市全站并按字样筛选；④档为真实标签/芯片多选，点选单站或展开城市名站/全市所有站即时累加，支持一键删签/清空；在线模式同步下发 `from_city_mode`/`to_city_mode`。已知例外：站表 `city` 列源自 12306
  station_name.js，个别郊县站挂县级市名（如「安溪」站 city=安溪），`all` 档按 city 匹配会漏。
  `/api/stations` 支持 `include_cities=1` 与 `only_cities=1`（按精确城市名/代表站简拼/全拼+车站数排序）。
- **担当车型（车底）显示**：车型仅存在于车站大屏原始报文——`train_style`（动车组，如
  CR400BF-Z/CRH380A）与 `jiaolu_train_style`（担当车底；普速 K/Z/T 的 train_style 为空、
  车底写在此字段，如 25G）。解析入库后由 `transfer._style_map()`（train_no→车型，按
  query_date 最新者胜，`db.data_version()` 为失效键的只读共享缓存）在 `_enrich()` 单点注入
  到每条 leg；`/api/station_trains` 经 `SELECT *` 直接带出两列。前端 `styleBadges()` 渲染：
  `CR数字`/ZL200J=复兴号徽标，`CR4xx…-Z/-AZ/-BZ` 后缀=智能动车（公开接口无标志位，按型号
  判定），另附灰色车型标签；普速直接显示 25G/25B 等具体车底。旧库升级 `init_db()` 自动
  ALTER + `_backfill_screen()` 从已存档报文回填，**零联网**。
- `fkeys`/`tkeys`：出发/到达站的**电报码与站名集合**（兼容不同数据源用码或用名）。
- 常驻缓存（`_get_graph_cache` / `_get_date_cache`）：全量路网只建一次并跨请求复用——
  `_build_graph_cache` 装入 `n2c`、`cities`（城市分组，用于无坐标兜底回退）、`geo_index`（`geo.StationGeoIndex`：
  含全国站坐标与状态、0.5°×0.5° 动态空间格网 `k=ceil(max_r / (111*cos(lat)*0.5))+1` 高低纬适配邻域、
  预构建 `≤100km` 物理近邻与站对强控/活跃换乘组映射）、`cal`
  （`fare.load_calibration()` 费率校准）、`sno_base`（train_no+电报码→station_no）、`screen_all`
  （大屏）、`tt_all`（时刻表）、`services_all`（引擎 D 车次服务），并为每次查询附
  `screen_key_to_tn`/`tt_key_to_tn` 两张「站→车次」倒排索引，`search()` 靠它挑候选，
  不再遍历无关车次；`_get_date_cache` 再按日期缓存 `_price_map`（票价）、`_seat_map`（余票）、
  `real_tk` 与当日 `sno` 覆盖。失效口径为 `db.data_version()`（进程内写入代数 + 库文件与
  `-wal` 的 mtime/size），站坐标/重轨距/换乘组/站对规则修改后均经 `db.tx()` 自增版本触发重建；响应里的 `cache_hit` 标明本次是否复用。
- `real_tk`：当日已爬到的权威余票 OD→车次集合，用于剔除与实际冲突的推算方案。
- `_complete_screen_from_tt(screen, tt)`：大屏只记录"那天拍到的几个站"，行残缺。用 `train_no`
  （匹配不到再退回 `(车次码, 始发站, 终到站)`）找到停站更全的时刻表班次，把大屏缺的站补进 `obs`
  并标 `bridged`，票价/余票仍各自按大屏保留。缺站会让"已越过目的站""过头折返"这类按 `station_no`
  的剪枝失明，补全后才会正确触发（反向判定见 §12 第 8 条）。因 `screen_all` 是跨请求共享的缓存对象，
  补全放在 `_build_graph_cache` 里、且在倒排索引与 `services_all` 构建**之前**只做一次。

### 6.2 四个引擎（结果合并去重）

- **A 车站大屏 `_search_screen`**：`_load_screen` 把大屏按 `(train_no, 开行日)` 聚合成"开行实例"，
  每站观测含到/发时刻（优先按 `start_train_date + arrive_day_diff / start_day_diff` 计算绝对分钟，缺 `day_diff` 时仅对 `>12h` 倒挂回退跨天，并按 `running_time`+`end_arrive_time` 推导始发/终到时刻）。**真实开行日期优先**；无当日数据时取最近班次按"每日开行假设"平移到查询日（`shift`，`date_assumed=True`）。
- **B 车次时刻表 `_search_timetable`**：`_load_trains` 读 `train_stops`，先记录 `arr_day` 再判断是否过午夜翻日计算 `dep`，统一为相对始发的分钟数（解决跨零点停站如 `23:55` 到、`00:05` 发同日错位）。全程按"每日开行假设"推算日期（`date_assumed=True`）。
- **C 站间余票**：仅有余票数据、A/B 未覆盖的直达，按精确日期补充（`search` 末段）；耗时优先用 `_colon_dur(r["duration"])` 解析 `HH:MM`（兼容超过 24h 的长途直达）。
- **D 多程换乘 `_search_multi`**（`max_legs>=3` 时启用）：`_load_services` 把大屏+时刻表统一成
  "车次服务"，BFS 逐层扩展。四档预设 `PRUNE_PRESETS` 对 A/B/D 都在用，但**每个开关的实际作用域不同**
  （详见 §6.7）：
  - `unbounded`（无限制，仅防死循环）：`frontier_k=0 / max_total_min=0 / detour_ratio=0 / prune_dominated=False`；
  - `relaxed`（高度宽松）：`frontier_k=16 / max_total_min=5760(96h) / detour_ratio=0 / prune_dominated=False`；
  - `balanced`（均衡默认）：`frontier_k=8 / max_total_min=2880(48h) / detour_ratio=0 / prune_dominated=False`；
  - `strict`（严格精简）：`frontier_k=4 / max_total_min=2160(36h) / detour_ratio=2.0 / prune_dominated=True`。
  另有 `DETOUR_FLOOR_MIN=480`（8 小时保底上限，防短途高铁误杀普速与低价中转）与 `SAFE_WAIT_MIN=30`（安全缓冲支配保护）；`frontier_k>0` 时每站每层保留"最快到达 + 最低费用"双路 top-`frontier_k`（未开启支配剪枝时走"入桶即截断"的有界桶压内存峰值）。非末程与末程统一按 `not is_last` / `is_last` 做过终点折返（`loopback`）、城市绕回、同城空转（如 `北京→北京南` 再换乘）与 `dur<=0` 防护校验，同时允许同车多个目的站展开。
- 当 `max_legs=1` 时，`need_transfers=False` 跳过所有 2 程与多程扩展，仅返回直达方案（`transfers=[]`）。

### 6.3 换乘约束、方案 C 异站建边、搜前枢纽收窄与后置过滤

- **五层非叠加异站规则（方案 C v2，`geo.eval_cross_pair`）**：同站换乘额外缓冲恒为 `0`；异站换乘仅当 `allow_cross_station=True` 时生效，按**严格非叠加优先级**仲裁一对 `(到达站 A, 出发站 B)`：
  1. `block`（站对黑名单 `transfer_pair_overrides`）：一票否决，即使同城或在半径内也不连通；
  2. `allow`（站对白名单，优先于半径）：强制连通，使用 `custom_extra_min`（`≥0`；若某激活规则设 `0m`，`search()` 标记 `zero_override_active=True` 并按最小实际缓冲计算可达下界）；
  3. `active_group`（启用中的自定义换乘组）：同组两站强制连通，`full_mesh` 模式下额外加时保底 `≥5m`（`hub_star` 仅中心枢纽 ↔ 成员站连通）；
  4. `radius`（物理直线距离半径 `0 ≤ cross_radius_km ≤ 100`）：仅接纳 `coord_status IN ('ok','verified')` 车站（**自动隔离 `suspect`/`missing`**），且每站邻近最多保留距其最近的 **Top-25**（`MAX_RADIUS_NEIGHBORS=25`）——若发生截断，响应标注 `radius_truncated=True` 并在 UI 提示收窄半径；
  5. `fallback`（无坐标旧版同城兜底，**默认关闭 `fallback_ungeocoded=False`**）：开启后**仅对至少一端无可用坐标的同城站对**以固定 `+90m`（`FALLBACK_FIXED_EXTRA_MIN`）回退，并标 `is_fallback=True`；**当 `R=0` 时接口直接返回 HTTP 400 拒绝同时开启回退**（防误以为零半径禁绝异站却仍暗自连同城）。当站表 `ok` 覆盖率 `≥98%` 或回退命中过多时，响应附带 `fallback_impact.auto_disable_recommended` 劝关提示。
- **单调连续距离计价与 3 档重轨邻近减时（`cross_time_mode="smart"` 默认 vs `"fixed"`）**：
  - `fixed` 模式：按传统固定 `cross_extra`（默认 +60 分钟）；
  - `smart` 模式：基于 `DISTANCE_TIER_BREAKPOINTS` `(0,3]→25–32m`, `(3,10]→32–48m`, `(10,20]→48–65m`, `(20,35]→65–85m`, `(35,100]→85–125m` 半开区间连续线性插值得到 `base_extra`，**若任一端为大型枢纽（`is_major_hub=1` 或在 `DEFAULT_MAJOR_HUB_NAMES`）加 `hub_extra=+5m`**，再按 `_round_half_up`（半入整舍，保证对整数平移不变）计算最终额外加时：
    - **3 档重轨邻近减时（均标「估算」）**：需同时满足 `transit_discount_on=True`、两端属于同一可联通轨道圈群（支持如 `SH_KS` 沪昆、`GZ_FS` 广佛、`XA_XY` 西咸、`CD_ZY` 成资、`NJ_CZ` 宁滁等同城/圈群交集；**明确排除京津跨市地铁减时**），且依据 `nearest_rail_stop_distance_m` 决定折减系数：`walk`（≤800m）折算系数 1.0、`short_bus`（800m ~ `transit_proximity_m` 默认 1500m）折算系数 0.5、`>proximity_m` 归 0。**仅双端均满足邻近度**才取两端最小因子折扣（双端 `walk` 满减 15m、混配减半；例如**昆山南—安亭北**因安亭北距 11 号线安亭站 `>1500m`，反向验证不触发减时、`transit_reason="双端邻近度不满足"`）。
    - **全局单调防倒挂**：`d ≤ 3km` 不减时；`3 < d ≤ 8km` 减时随距离从 0 线性坡升至上限，并对 `d=3.0km` 处取值做前缀上包络且保底 `MIN_SAFE_CROSS_EXTRA=20m`，数学上严格保证任意 `d2 > d1` 必有 `effective_extra(d2) >= effective_extra(d1)` 且恒等式 `base_extra + hub_extra - transit_discount == effective_extra` 成立。
- **多程下界可容许性与同城跨站反剪枝**：
  - 引擎 A/B/D 的异站边统一由 `geo.cross_candidates(geo_index, cfg)` 在搜索前物化；引擎 D 反向最短可达时间 `rev_dist`（剪枝下界）通过 `min_possible_cross_extra(geo_index, cfg)` 动态取同站 `0` 与活跃异站边最小额外加时（若存在 `0m` 自定义站对，下界精确降为 `0` 而不致漏解）。
  - 经过 `A → B`（如 `昆山南(苏州) → 安亭北(上海)`）异站换乘抵达 `B` 城后，允许在 `B` 站上车继续走完 `B` 城路线——多程状态剔除「抵达城市」标签避免后续合法同市延伸被误判 `city_loop`（见 §12 第 15 条）。
- 换乘等待 `wait` 须满足 `min_wait <= wait <= max_wait`；`eff_min = min_wait + effective_extra`。跨天：`n = ceil((到达 + eff_min - 第二程发车) / 1440)` 自动取下一班。
- **防死循环与无意义分支硬剪枝**：重复车次（`repeat_train`）、重访出发/已乘车站及过终点后再绕回已乘车站的**过头折返**（`loopback`）、绕回已离开城市（`city_loop`）、刚上车未出城即在同城下一站换乘的**同城内耗空转**（`city_loop` / 首程 `loopback`）、已到目的站后再继续换乘（`passed_dest`）、以及 `dur <= 0` 脏停站边。
- **指定枢纽站搜前收窄（`hub_only` / `hub_stations`）**：独立开关，开启后在图搜索阶段（引擎 A/B/D）仅允许经 `MAJOR_HUBS`（31 个直辖市/省会/首府 + 23 个重要干线铁路枢纽，共 54 处）或用户增删后的 `hub_stations` 换乘，拦截数计入 `prune_stats["hub_filter"]`。
- **车次头与换乘站精确后置过滤（`filter_plans()`）**：`allow_prefixes`/`exclude_prefixes` 按车次头过滤；`include_stations`/`exclude_stations` 通过 `_match_station_tokens` 精确匹配**电报码、完整站名（含可选单后缀`站`）或所属城市名**（不做子串扩散，例如排除「北京南」不扩散到全北京，「南平市」不匹配同城「延平」），多个 `include_stations` 支持 `or`/`and`。该过滤已与图搜索解耦：前端在返回结果后即时本地执行，修改无需重搜；`search()` 响应附带 `station_cities` 与 `city_suffix_stations` 供前端精准复用。
- **后置过滤组合口径**：各条件组（只含/排除车型头、车次包含、必经/排除换乘站、同异站、席位/时间）之间为
  **AND 交集**；组内多 token 默认 **OR**——只含车型头要求**全程每一程**都命中任一 token，排除车型头/排除换乘站为
  **任一程/任一换乘对命中任一 token 即排除**，车次包含为任一程命中；必经换乘站默认 OR、可切 AND
  （每个 token 须各被一个换乘对命中；站数超过换乘次数时 `search()` **自动提高 `max_legs`**，上限 5 程，
  并在 `note` 说明，前端同步提高程数下拉）。排除判定先于必经，等价于 `必经 ∧ ¬排除`。前端 `filterPlans` 与后端逐条同口径。
- **约束生效时搜前自动放宽（`prune_config.auto_relaxed`）**：必经/排除换乘站、只含/排除车型头任一生效且用户未显式传
  `frontier_k`/`prune_dominated` 时，`search()` 搜前把束宽置不限并关闭支配剪枝——这两类剪枝在过滤之前按局部最优扔分支，
  会误剪"尚未经过必经站"的合法分支且后置过滤补不回（实测 3 程必经上海虹桥：balanced=260 vs 无束宽=6314）。
  硬防死循环规则、总历时/绕行上限与用户显式参数仍生效；前端修改这些条件后 600ms 自动重搜。
- `_assumed_conflict`：推算方案（`date_assumed`）若其任一程的 OD+日期已有权威余票、但该车次不在
  当日余票里（即当日不开行）→ 剔除，以实际余票为准。

### 6.4 合并与排序

- 直达按 `(首程train_no, 首程from, 首程to, 首程depart)` 去重；2 程换乘按 `(首程no, 二程no, 换乘站, 二程from, 首程from, 二程to, 首程depart)` 去重；多程按 `(完整train_no元组, 各换乘站名元组, 首程from, 末程to, 首程depart)` 去重，保留同站与异站或同城不同起讫站的合法分支。
- `rank(p)`：**有真实票价的优先**，其次非推算（真实日期）优先。
- 最终按 `total_min` 升序。`limit > 0` 是**直达+换乘的合计上限**（直达优先占额度），截断发生在
  `departed`/`date_assumed` 归一与 `station_cities` 汇总**之前**，因此这两处逐方案统计只作用于返回的方案。
  返回含 `direct`/`transfers`/`direct_total`/`transfers_total`（截断前命中数）/`screen_trains`/`timetable_trains`/`cache_hit`/`prune_config`/`prune_stats`/`radius_truncated`/`zero_override_active`/`fallback_impact`/`coord_coverage`/`note`。每条 `transfers[i]` 的换乘节点对象附带透明计价账本（`base_extra`/`hub_extra`/`transit_discount`/`effective_extra`/`source_rule`/`distance_km`/`rail_access_mode`/`transit_reason`/`is_fallback`/`is_suspect`）。
- **默认值分两层**：`search()` 本身 `limit=0`（不限，脚本自行负责）；HTTP 层 `/api/transfers` 与
  `/api/transfers/start` 默认 `limit=1000`，前端不传即用该值。原因：两段枚举不设上限，实测
  深圳→广州命中 37.4 万条换乘，全量 JSON 数百 MB 会同时打死服务端与浏览器。要全量请显式传 `limit=0`。

### 6.5 票价/余票富化 `_enrich`

每程 leg 依次尝试：
1. 有 `from/to_station_no`（或由 `sno` 回填）→ 查 `_price_map` 得真实 `prices`。
2. 无真实票价 → `fare.estimate` 得 `price_est`（估算区间）。
3. 查 `_seat_map`（train_no+OD+日期）得 `seats`/`can_web_buy`。

> 修改换乘行为的关键参数：`min_wait/max_wait/cross_extra`（固定模式默认 +60）、`cross_radius_km`（默认 20km，上限 100km）、`cross_time_mode`（`smart`/`fixed`）、`transit_discount_on`/`transit_discount_min`/`transit_proximity_m`、`fallback_ungeocoded`（默认 `False`）；`geo.py` 中的 `DISTANCE_TIER_BREAKPOINTS`/`RAIL_TRANSIT_CITIES`/`CROSS_CITY_TRANSIT_ZONES`/`MAX_RADIUS_NEIGHBORS`；`transfer.py` 中的 `PRUNE_PRESETS`、`DETOUR_FLOOR_MIN`、`SAFE_WAIT_MIN`、`MAX_TIMETABLES`。

### 6.6 缓存与过期策略

**三层缓存（无 TTL，只随「数据版本键」失效）**

1. 常驻路网缓存 `_MEM_CACHE`（`_get_graph_cache`）—— 全量 `n2c`/`cities`/`cal`/`sno_base`/
   `screen_all`/`tt_all`/`services_all` 与两张倒排索引，进程内只建一次、跨请求复用；`_BUILD_LOCK`
   加双检锁保证并发下只构建一份。
2. 按日期缓存 `_DATE_CACHE`（`_get_date_cache`）—— 每查询日期缓存 `prices`/`seats`/`real_tk`/
   当日 `sno` 覆盖；最多 `_MAX_DATE_CACHE=8` 个日期，超限按插入序淘汰最早的一个。
3. 票价/余票本身落库（`prices`/`tickets`），`_price_map`/`_seat_map` 每次按需查库后塞进日期缓存。

**失效版本键 `db.data_version()`**

返回 `(str(DB_PATH), 进程内写入代数, _file_sig(db), _file_sig("-wal", ignore_empty=True))`，
综合覆盖三类变化：

- 进程内写入代数 `_data_gen`：`db.tx()` 提交后若 `total_changes` 有变就 `bump_data_version()` 自增
  （写库即失效）；`init_db()` 结束也自增一次。
- `_file_sig(p) = (st_mtime_ns, st_size)`：探测 db 主文件与 `-wal` 的**跨进程**修改；空文件在
  `ignore_empty=True` 下恒记 `(0,0)`——即**空的 `-wal` 不计入版本**，避免其 mtime 抖动误触发重建。
- 库路径切换：`DB_PATH` 变化直接体现在键首项。

**失效时机（惰性，读时校验）**

- `_get_graph_cache`：每次取 `data_version()` 与缓存内 `version` 比对，不一致才重建；
  响应里的 `cache_hit` 标明本次是否复用。
- `_get_date_cache`：版本号变化即整清空；日期超 8 个做淘汰。

**手动失效**

- `transfer.invalidate_cache()`：显式清空 `_MEM_CACHE` + `_DATE_CACHE`（测试用；正常写库已自动失效）。
- `DELETE /api/cache`（`main.py` → `db.clear_cache`）：删的是**库表数据**（`station_trains`/
  `train_stops`/`tickets`/`prices`，可选 `raw_payloads`），经 `tx()` 提交触发 `bump_data_version()`，
  缓存随版本键自动重建。`before_date` 为**必填**：缺省时 `db.clear_cache(None)` 会全表删光已爬数据，
  所以 HTTP 入口直接返回 400；要全量清空请传远期日期（如 `2100-01-01`），或在脚本里调用
  `db.clear_cache(None)`。

**「过期」的两层含义（易混淆）**

- **缓存过期**：即上面的版本键失效——跟时间无关，只跟数据是否变化有关。
- **票价「历史缓存」stale**：`_price_map(target_date)` 无目标日票价时回退到库内最新日期并置
  `stale`，落到 leg 上即 `price_stale`/`price_date`，前端显示「旧/（历史缓存）/价日」。
  这是数据兜底，不是缓存键过期。

**约束**

- `screen_all`/`tt_all`/`services_all` 与两张倒排索引是进程内跨请求共享的**只读**对象，别在
  `search()` 里原地改（见 §12 第 10 条）。
- 外部脚本正常写库会改变 db 文件签名并在下次查询自动重建；仅当文件 `mtime_ns`/`size` 恰好未变时
  才漏检，需重启或 `invalidate_cache()` 兜底（见 §12 第 11 条）。

### 6.7 剪枝档位：四个开关各自作用在哪个引擎（易误解，改动前务必读）

档位表 `PRUNE_PRESETS`（`transfer.py:357-361`）：

| 档位 | `frontier_k` | `max_total_min` | `detour_ratio` | `prune_dominated` |
|---|---|---|---|---|
| `unbounded` 无限制（仅防死循环） | 0 | 0 | 0.0 | False |
| `relaxed` 高度宽松 | 16 | 5760（96h） | 0.0 | False |
| `balanced` 均衡（**默认**） | 8 | 2880（48h） | 0.0 | False |
| `strict` 严格精简 | 4 | 2160（36h） | 2.0 | True |

参数优先级：`search()` 先取档位值，显式传入的参数逐个覆盖（`transfer.py:2296-2300`）；**显式传 `0`
表示"不限"**，不会被档位默认值回填（`transfer.py:1384-1387` 有 WHY 注释）。前端
`static/index.html` 的 `PRUNE_PRESETS`（`index.html:1137-1142`）是同一份数值的**手工副本**，且
`maxh` 单位是小时（后端是分钟）——改后端档位必须同步改前端，否则 UI 显示的档位口径与实际下发不一致。

**作用域矩阵（同一个开关在不同引擎的生效阶段完全不同，这是所有性能误解的根源）**

| 开关 | A 大屏 `_search_screen` | B 时刻表 `_search_timetable` | D ≥3 程 BFS `_search_multi` | `search()` 汇总阶段 |
|---|---|---|---|---|
| `max_total_min` | ✅ 枚举循环内（`749`/`835`） | ✅ 枚举循环内（`1077`/`1221`） | ✅ 循环内 + 反向可达下界剪枝（`1640`/`1650`/`1915`） | — |
| `detour_ratio` | ❌ **不读** | ✅ 循环内随 `best_min` 动态收紧（`1123`/`1206`/`1234-1235`）+ 元组阶段终筛（`1248`） | ✅ 由汇总前算出的 `prune_cfg["detour_cap"]`（`2440` 写入）收紧 `time_limit`（`1389-1392`） | ✅ 事后对**已建好**的方案再筛（`2498-2509`） |
| `prune_dominated` | ❌ **不读** | ✅ 在**轻量元组**上先做（`1258`），`_enrich` 只跑存活者 | ✅ 首程压缩（`1686`）、逐层桶（`2009-2027`）、终点终筛（`2079`） | ✅ 对合并结果再做一遍（`2511-2548`） |
| `frontier_k` | ❌ **不读** | ❌ **不读** | ✅ 每站每层"最快到达 + 最低费用"两榜交替取 top-K；`prune_dominated=False` 时走**入桶即截断**的有界桶压内存峰值（`1716`、`1960-2006`），开启支配则改为层末截断（`2028-2054`） | — |
| `hub_only`/`hub_stations` | ✅ 搜前收窄中转候选（`783-787`） | ✅ 同样收窄（`1144-1151`） | — | — |

三条推论：

1. **UI 默认的"最多 2 程"搜索不受 `frontier_k` 约束**，也享受不到 B 的元组级早筛：大屏引擎 A 会把
   凡是过了 `max_total_min`（默认 48 小时）的每组合作为**已 `_enrich` 的 dict** 直接 `append` 进
   `transfers`（`840`）。实测深圳→广州（`balanced`）换乘命中 **37.4 万条**。
2. **换档位降不了两段搜索的耗时**：`strict` 只让引擎 B 少建方案、并在汇总阶段多筛一遍；引擎 A 的
   内层循环组合数不变（档位对它的唯一影响是 `max_total_min` 从 48h 收到 36h，少落一点方案），
   且它落进 `transfers` 的每条都已经付过 `_enrich`。而 `prune_dominated` 在汇总阶段是按
   `(首程出发站, 末程到达站)` 分组后做 `any(accepted)` 两两比较（`2532-2539`），组内规模大时是
   平方级——大 OD 上选"严格精简"反而更慢。真正兜住返回体积的只有 HTTP 层的 `limit`（见 §6.4）。
3. `balanced`/`relaxed` 的 `detour_ratio=0.0` 是**有意的**：绕行倍率会误杀"慢但便宜"的普速中转。
   相关常量：`DETOUR_FLOOR_MIN=480`（8 小时保底，`transfer.py:377`）、
   `SAFE_WAIT_MIN=30`（支配比较里的稳妥等待缓冲，低于它才判被支配，`transfer.py:447`）、
   `MAX_TOTAL_MIN=2880` / `FRONTIER_K=8`（引擎 D 的默认值，`transfer.py:1319-1320`）。

---

## 7. 票价估算与校准（`app/fare.py`）[内核]

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

## 8. 后台任务（`app/main.py` 的 `jobs`）[概念]

`jobs: dict[job_id, dict]`，线程执行，前端轮询 `GET /api/jobs/{id}`。任务字典含
`status/done/total/errors/phase/current/log/result` 等，响应实时附带 `elapsed_sec`（已完成/取消/报错时冻结为总实际用时）；`running` 时针对子阶段附带局部 `sub_eta_sec`（如搜索 BFS 各深度独立计时），非搜索的同类抓包阶段按当前 `phase` 均速附带 `eta_sec`。

- **`_price_job`**：`/api/crawl/tickets?with_prices=true` 触发，按已入库 tickets 逐车次抓票价。
- **`_route_job`**（一键导入/在线模式）：`/api/import/route` 触发，阶段
  `screen→timetable→tickets→price`。先抓出发/到达全部车站大屏；`deep` 时再抓最多
  `MAX_TIMETABLES=200` 个车次时刻表、各 OD 余票、各车次票价。
  - **大屏空站短路区间余票**：screen 阶段逐站记录「当日无任何经由车次」的车站——判据为
    `crawl_station_trains` 返回 `parsed==0`（`data` 为空列表），或抛 `CrawlError` 且 errorMsg
    含「无数据/暂无/不存在」（`data` 非列表）。tickets 阶段的 OD 笛卡尔积**跳过以这些空站为任一端点的组合**，
    避免对停办客运站（如按电报码的厦门高崎、漳州东）或季节性停运（如永定仅 K230、只在非汛期/临时开行）
    发起必然为空的余票查询。**网络/风控类失败（不含上述关键词）的车站不跳过**，仍照常查询，
    以免偶发失败漏掉真实车次（与 `crawl_all_timetables.py` 的空站判定口径一致，见 §12 第 12 条）。
- **`_transfer_search_job`**（异步换乘搜索）：`POST /api/transfers/start` 触发，支持实时进度/子进度（`sub_done`/`sub_total`）与中断。
- **`_plan_tickets_job`**（方案实时查价/余票）：`/api/plan_tickets` 触发，见下节。

### 方案票价/余票实时查询 `/api/plan_tickets`

1. 入参 `legs[]`（train_no/date/from_telecode/to_telecode/from_name/to_name），按
   `_leg_ident` 去重，生成 `key = "train_no|from|to|date"`（与前端 `legKey()` 完全一致）。
2. 对每个唯一 OD+日期：**日期 >= 今天**才实时 `crawl_tickets`（过去日期只用缓存）。
3. 从入库 tickets 取余票；station_no 优先用 tickets 的 `from/to_station_no`，缺失用 `sno` 回填。
4. 票价：已有缓存跳过，否则 `crawl_price`（同样仅未来日期）。
5. 输出 `result[key] = {seats, can_web_buy, prices:[{seat,price}]}`；无余票或失败则 `error`。

> 前端 `autoQuery()` 取**当前页**（`sortPlans(filterPlans(...)).slice((page-1)*size, page*size).filter(!departed)`）的行程段，POST 后轮询，`applyTickets()` 按 key 回填。切换排序/筛选/翻页会 `refreshAutoQuery()` 防抖 900ms 重查。

---

## 9. 时刻表查看 `/api/timetable`（含大屏退回）[概念]

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

## 10. API 端点速查（`app/main.py`）[概念]

| 方法 路径 | 作用 |
|---|---|
| `GET /` | 前端页面 |
| `GET /api/stats` | 各表计数 |
| `GET /api/stations?q=&limit=&include_cities=&only_cities=` | 车站与城市联想搜索（站名/电报码/拼音/简拼，城市按精确拼写与站数排序） |
| `GET /api/stations/resolve?q=&city_mode=` | 实时预览五档选站解析结果与精准报错 |
| `GET /api/hubs` | 默认全国核心枢纽清单（`MAJOR_HUBS`，31 省会直辖市 + 干线大枢纽） |
| `GET/POST /api/rate` | 读取/设置限速 |
| `POST /api/crawler/reset` | 重置 12306 会话与动态查询 URL |
| `POST /api/import` | 手动导入原始文本（可指定 api_type 与 ctx） |
| `POST /api/import/file` | 上传文件导入 |
| `POST /api/crawl/stations` | 抓站名表 |
| `POST /api/crawl/station_trains` | 抓车站大屏 `{station,date}` |
| `POST /api/crawl/timetable` | 抓车次时刻表 `{train_no,date?,from?,to?}` |
| `POST /api/crawl/tickets` | 抓站间余票 `{from_station,to_station,date,with_prices?}` |
| `POST /api/crawl/price` | 抓单票价 `{train_no,from_station_no,to_station_no,seat_types,date}` |
| `POST /api/import/route` | 一键导入向导 `{from_city,to_city,date,deep}` → job |
| `GET /api/jobs/{id}` | 任务进度（含子进度 `sub_done`/`sub_total`） |
| `POST /api/jobs/{id}/cancel` | 中断运行中的后台任务 |
| `GET /api/station_trains?station=&date=` | 浏览大屏车次 |
| `GET /api/timetable?train_no=` | 时刻表（含大屏退回） |
| `GET /api/trains?q=&limit=` | 按车次号搜时刻表 |
| `GET /api/tickets?from_station=&to_station=&date=` | 浏览余票（批量附带匹配实测票价 `prices`/`price_date`/`price_stale`） |
| `GET /api/prices?train_no=` | 浏览票价 |
| `DELETE /api/cache` | 清理过期数据与内存缓存（`?before_date=必填&include_raw=`） |
| `GET /api/transfers?...` | 同步换乘搜索（见 §6 参数，含 `cross_radius_km`/`cross_time_mode`/`transit_*`/`active_group_ids`/`fallback_ungeocoded`） |
| `POST /api/transfers/start` | 异步换乘搜索 → job（支持进度查询与中断） |
| `POST /api/plan_tickets` | 方案实时查价/余票 → job |
| `GET /api/geo/status` | 站坐标覆盖率（`ok`/`verified`/`suspect`/`missing`）、重轨停靠站总数与断点状态 |
| `GET /api/geo/stations?status=&q=&limit=` | 按坐标状态（如 `suspect`/`missing`/`ok`/`verified`）分页浏览车站维护列表 |
| `POST /api/geo/station/{telecode}` | 人工更新/校准车站坐标与 `is_major_hub`/`transit_zone_id`/`verified` 状态并自动重算重轨邻近度 |
| `POST /api/geo/nominatim/{telecode}` | **仅 UI 单站人工点查** Nominatim（带 `≥1.5s` 强制冷却锁，返回候选供确认） |
| `POST /api/geo/audit` | 立即重算全部站重轨邻近度并运行多维坐标质检 |
| `POST /api/geo/crawl/start` | 后台启动 Overpass/Wikidata 批量地理采集 → job（支持 `trial_tiles`/`force`/`reset_checkpoint`） |
| `GET/POST /api/transfer_groups` | 查询/新建或更新自定义换乘组（上限 30 站，`full_mesh` 最小 `>=5m` / `hub_star`） |
| `DELETE /api/transfer_groups/{group_id}` | 删除自定义换乘组 |
| `GET/POST /api/transfer_pairs` | 查询/保存任意两站强制连通（`allow`）或禁止（`block`）规则 |
| `DELETE /api/transfer_pairs/{pair_id}` | 删除站对覆盖规则 |
| `GET /api/transfer_pairs/preview?a=&b=&...` | 实时预览任意两站按当前半径/规则算出的直线距离、3 档重轨邻近度与计价明细 |
| `GET/POST /api/transit_cities` | 读取 ~43 个内置核心重轨城市列表及自定义覆盖 |
| `POST /api/transit_cities/reset` | 重置重轨城市覆盖配置为默认口径 |

---

## 11. 前端逻辑（`static/index.html`）[概念]

五个页签：换乘搜索 / 爬取数据 / 导入数据 / 数据浏览 / 显示设置。核心 JS：

- 搜索：`#s-go` → （在线模式先 `runRouteImport`）→ `POST /api/transfers/start` + 轮询 `/api/jobs/{id}` → 给方案打 `_k/_type` → `render()`；任务运行中实时展示 `已用` 与阶段 `预计剩余`（`sub_eta_sec`/`eta_sec`），结束后固定展示 `实际用时`（同时写入搜索结果顶栏）。
- `legKey(l)`：`train_no|from_telecode|to_telecode|depart[:10]`，与后端 `_leg_ident` 对齐。
- `priceKey(p)`：返回 `[miss?1:0, est?1:0, sum, waitlist?1:0, allSoldOut?1:0]`——**实际价优先**（有真实价排前，按实际价升序；同价位优先有票而非候补），其次有估价。`sortPlans` 价格模式据此排序。
- `filterPlans`：统一后置本地过滤（即时生效、不重跑图搜索）——只含/排除车型头（`#s-inc-pfx`/`#s-exc-pfx`）、车次包含（`#s-filter`）、必经/排除换乘站（`#s-inc-tst` + `#s-inc-mode` OR/AND、`#s-exc-tst`，利用响应附带的 `station_cities` 与 `city_suffix_stations` 复现精确站名/城市匹配）、换乘类型（`#s-same`）、席位（`#s-seat`）、出发到达时间等。
  （"直达 0"通常是开了"仅换乘"、"同/异站换乘"或"必经换乘站"筛选所致，非 bug。）
- 剪枝与指定枢纽收窄（搜前）：`#s-prune` 提供均衡 / 严格 / 高度宽松 / 无限制（仅防死循环） / 自定义五种策略，`#s-hub-only` 与 `#s-hubs`（可点「填入省会+大枢纽预设」自由增删）控制搜前仅经指定枢纽换乘；`#s-prune-panel` 仅保留纯数值/图参数（束宽 0=不限、总历时 0=不限、绕行倍率、异站分支、支配剪枝），不再混入后置过滤框。`pruneDiagHtml` 按需提供扩展 3 程、放宽至高度宽松/无限制、关闭仅枢纽重搜，以及一键 `清除后置筛选`（0ms 本地生效）。
- **方案 C 异站配置条（`#s-cross-strip`）与透明账本**：搜索主面板集成物理搜寻半径预设（0 / 10 / 20 / 35 / 50 / 100 km）、计价模式（智能连续+重轨减时 / 固定 `+Xm`）、3 档重轨估算减时开关与邻近阈值（默认 1500m）、无坐标回退复选框（**默认关闭**；当 `R=0` 时联动禁用，开启时触发琥珀色低置信提示；当 `ok>=98%` 时展示劝关提醒）与已启用换乘组多选标签。结果列表新增 `#s-hide-fallback` / `#s-hide-suspect` 本地筛选，顶栏按需告警 `radius_truncated`（单站超 25 个邻近截断）与 `zero_override_active`（0m 强控规则放宽了 BFS 下界）；桌面端与手机端方案卡片统一渲染 `transferLedgerHtml`，就地展开 `基础 Xm + 枢纽 Ym - 重轨估算 Zm = 有效 Wm` 与规则来源徽标（规则/换乘组/半径+距离/低置信回退）。
- **日期、双端视图与动态席位设置（`#tab-display`）**：所有查询/爬取日期输入项默认取北京时间**明天（+1 天）**（缓存清理基线维持今天避免误清）。结果区支持**国铁 12306 网页多列席别表格**与**铁路 12306 手机紧凑卡片**切换/自适应（`viewMode`）；第 5 页签「显示设置」支持配置：
  - **整页无数据冷门席位自动折叠**（`foldEmpty`）及手动单席位隐藏（支持在结果栏一键临时展开全部列）；
  - **互斥席位合并**（预设 `硬座-二等座`、`软座-一等座`、`动卧-二等卧(硬卧)`、`高级软卧-商务座` 及自定义合并），其中 **`商务座` 与 `特等座` 强制拆分为独立席位**；`KNOWN_COEXIST_PAIRS` 与 `computeEffectiveColumns()` 双重防线会在配置与渲染阶段拦截或自动拆分任何同车并存的席位组合。
- `autoQuery()`：`sortPlans(filterPlans(...)).slice((page-1)*size, page*size).filter(!departed)` → flatMap 腿 →
  `POST /api/plan_tickets` → `pollPlanJob` → `applyTickets` 按 key 回填真实价/余票。
- `refreshAutoQuery()`：排序/筛选/翻页变化后防抖 900ms 重查（仅勾选"自动查询票价/余票"时）。
- 时刻表与票价展开：`toggleTT`/`renderTT`/`ttTable`（升级为纵向圆点时间线 `●──○──●`，自动淡化上/下车之外经停站）与 `togglePriceRow`/`priceDrawerHtml`（见 §9）。
- 爬取页（`#tab-crawl`）：除大屏/时刻表/余票抓取外，集成 4 个方案 C 管理面板——`#geo-card`（覆盖率统计、Overpass/Wikidata 抓包与试跑、`suspect/missing` 人工维护 + Nominatim 单站校准）、`#groups-card`（自定义换乘组）、`#pairs-card`（站对允许/禁止与实时公式预览）、`#transit-card`（~43 城重轨名单维护）及 `crawlTT`/`showTimetable`（模态框）、`renderTickets`/`drawTicketsTable`（复用动态席位折叠与合并规则）。

> 改前端排序/筛选/自动查询逻辑都在 `index.html` 的 `priceKey/sortPlans/filterPlans/autoQuery` 几处；异站规则条与账本渲染在 `syncCrossStripUI/renderGroupPills/transferLedgerHtml`。

---

## 12. 二次修改常见入口 [内核]

| 想改什么 | 去哪改 |
|---|---|
| 12306 字段/接口改版 | `parsers.py`（`LT`/`SEAT_FIELDS`/`PRICE_KEYS`/`_find_stop_list`）、`crawler.py`（URL/参数） |
| 限速快慢/风控策略 | `crawler.py`（`MIN_FLOOR/MAX_CEIL`、`_note_ok/_note_throttled`） |
| 换乘最短/最长等待、方案 C 异站建边与计价 | `geo.py`（`DISTANCE_TIER_BREAKPOINTS`/`compute_smart_extra`/`eval_cross_pair`/`StationGeoIndex`）；`transfer.py`（`eff_min`/`cross_candidates`）；API 半径与减时参数 |
| 地理坐标采集、43 城重轨名单与多维质检 | `geo.py`（`GeoCoordCrawler`/`RAIL_TRANSIT_CITIES`/`CROSS_CITY_TRANSIT_ZONES`/`audit_station_coords`/`manual_nominatim_lookup`）；`crawl_geo_coords.py` |
| 自定义换乘组、站对强制/禁止规则 | `db.py`（`transfer_groups`/`transfer_group_members`/`transfer_pair_overrides`）；`main.py`（`/api/transfer_groups*`/`/api/transfer_pairs*`） |
| 多程剪枝/总时长上限/预设/核心枢纽 | `transfer.py`（`PRUNE_PRESETS`、`MAJOR_HUBS`、`DETOUR_FLOOR_MIN`、`SAFE_WAIT_MIN`、`filter_plans`）；`main.py`（`MAX_TIMETABLES`） |
| 票价估算费率/速度 | `fare.py`（`RATES`/`SPEED`/`load_calibration`） |
| 城市展开/别名/五档选站（含③城市+字样、④芯片多选、实时预览） | `db.py`（`city_stations`、`city_all_stations`、`CITY_PROPER`、`resolve_stations(city_mode)`）；`main.py`（`/api/stations/resolve`、`only_cities`、`from_city_mode`/`to_city_mode`）；前端 `index.html`（`initSidePicker`、`odState`、`schedulePreview`） |
| 担当车型徽标（复兴号/智能动车/车底） | `parsers.parse_station_screen`（train_style/jiaolu_train_style）→ `transfer._style_map`/`_enrich` → 前端 `styleBadges()` |
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
6. 改后端代码后需重启服务（uvicorn 可启用 `--reload`）；前端改 `index.html` 刷新浏览器即可。
7. 所有抓取原始返回都进 `raw_payloads`，排查解析问题先查留档（`source` 区分 crawl/import/test）。
8. 一个 `train_no` = 一趟物理车次的**单一方向**，途中会换显示车次码（厦门北→兰州记 `Z126/Z127`，
   兰州→厦门北是另一个 `train_no`，记 `Z125/Z128`，且停站表不同）。因此"大屏车次码与时刻表车次码
   不一致"**不能**当作反向证据剔除，反向/缺站只能靠 `train_no` 与 `station_no` 顺序判断。
9. **搜前与后置边界**：仅`仅经枢纽/指定站换乘`（`hub_only`/`hub_stations`）、防死循环硬规则与`unbounded/relaxed/balanced/strict`剪枝参数参与搜前裁剪；车型头与必经/排除换乘站位于主面板可见区，作为搜索条件下发后端（触发 `auto_relaxed` 与 AND 自动提程数），同时保留同口径后置过滤兜底；前端修改这些条件后 600ms 自动重搜，其余条件（席位/时间/车次包含等）仍即时本地过滤。排查结果偏少时先区分「剪枝拦截明细（如 `非枢纽剪除`/`总耗时超限`）」与「后置过滤条」。后置过滤各条件组之间为 AND、组内默认 OR（排除类任一命中即生效），口径见 §6.3。约束生效时搜前自动关闭束宽/支配剪枝（`auto_relaxed`，见 §6.3），避免必经分支"还没机会必经"就被误剪（实测放宽前 3 程必经 balanced=260 vs 无束宽=6314）；总历时/绕行上限仍生效，结果偏少先查这两项与拦截明细。2 程下后置必经与搜前枢纽收窄结果完全一致（实测 4=4、202=202）。
10. `screen_all`/`tt_all`/`services_all` 和两张倒排索引是**进程内跨请求共享**的缓存对象，只读：
    不要在 `search()` 里原地改写（会污染下一个查询），也别指望 `del screen, tt` 省内存（删的只是
    本地引用，删早了末尾统计就取不到值）。要调整停靠站口径就改 `_build_graph_cache`，一次构建全生效。
11. 缓存过期只看 `db.data_version()`：进程内 `db.tx()` 写入代数 + `data/rail.db` 与 `-wal` 的
    `mtime_ns`/`size`。外部脚本正常写库会改变这两项并在下次查询时自动触发缓存重建；仅当文件元数据未变化时才需重启服务。
12. **一键导入的区间余票由大屏结果 gate**（`_route_job`）：screen 阶段把「当日无任何经由车次」的车站
    （`parsed==0` 或 errorMsg 含「无数据/暂无/不存在」）收集起来，tickets 阶段跳过以其为端点的 OD——
    这些站发不出也接不到车，`leftTicket` 必返空。**判定三词必须与 `crawl_all_timetables.py` 保持一致**；
    网络/风控失败（不含三词）的车站保守保留仍查，别把偶发失败误判为空站而漏票（见 §8）。
13. **剪枝档位不是全局生效的，别指望换档位能救两段搜索的性能**：`frontier_k` 只作用于 ≥3 程 BFS，
    `detour_ratio`/`prune_dominated` 在时刻表引擎是元组级早筛、在大屏引擎只在汇总阶段事后筛（且支配
    比较是平方级）。UI 上"束宽 8"对默认的最多 2 程根本不起作用。完整作用域矩阵与推论见 §6.7，
    返回体积的兜底靠 HTTP 层 `limit`（§6.4），不是靠档位。
14. **重轨减时“双端邻近”反向样本与 Nominatim 批量禁令**：**昆山南—安亭北**虽跨入 `SH_KS` 圈群且距 11 号线不远，但安亭北距最近重轨站（安亭站）实际 >1500m，默认参数下**必须拒绝减时**（`transit_reason="双端邻近度不满足"`，不得为了凑减时放宽阈值）；京津虽有高铁城际但无同网地铁直连，明确排除跨市减时。此外 **Nominatim 严禁接入任何批量地理抓取脚本/队列**，只允许前端单站人工点击。
15. **多程跨城换乘不可被误杀为 `city_loop`**：经 `A → B` 异站进入 `B` 城后（如 `昆山南→安亭北→上海`），状态剪枝计算已闭合城市集时必须排除落脚站 `B` 所在城市（`eff_closed = closed_cities - {dk_city}`），否则刚从异站接入 `B` 的后续合法出发车次会被误判绕回；且多程到达下界 `min_lb` 必须用 `min_possible_cross_extra()` 动态计入 `0m` 自定义放行规则。
16. **智能计价必须用半入整舍 `_round_half_up`**：Python 内置 `round()` 是银行家偶舍入，在 `.5` 边界会导致 `round(base + hub) - round(base) != hub`（如 `28.5→28` 而 `33.5→34`），破坏 `base_extra + hub_extra - transit_discount == effective_extra` 账本恒等式与单调性；改 `geo.py` 时务必保持 `_round_half_up` + `3→8km` 前缀上包络。
17. **默认关闭 `fallback_ungeocoded` 且与 `R=0` 互斥**：旧版仅靠 `city` 列绑定换乘正是万州北↔重庆北等百公里行政边界误连的根源，迁移期默认保持 `False`；且 `_validate_search_params` 在 `cross_radius_km == 0` 时禁止同时开启回退（HTTP 400）。
18. **`init_db()` 旧库升库必须“先加列、再建索引”**：`SCHEMA` 常量里禁止放引用新地理列（`coord_status`/`coord_grid`/`transit_zone_id`）的索引；所有新增列索引须写在 `db.init_db()` 执行完 `ALTER TABLE` 迁移之后，保证携带历史数据的 `data/rail.db` 无痛升级。

---

## 13. 本地诊断脚本（`diagnostics/`，不入库）[内核]

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
