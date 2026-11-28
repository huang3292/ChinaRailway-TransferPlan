"""全国主要铁路干线站序拓扑与运行线路推断（零联网、保守消歧）。

WHY:
12306 所有公开接口（leftTicket / 车站大屏 / 时刻表 / 票价）均不返回列车物理运行线路名称。
本模块按“线路 → 有序车站序列 + 牵引制式兼容性”做边级匹配，并利用整段乘车停站
（train_stops 上下游中间站）收窄共廊区间的候选集合：
- 若唯一命中一条线路（如 福州→延平 仅命中 合福高铁），标为确定（certain=True）；
- 若存在多线共廊且无中间站消歧（如 厦门北→漳州 直达，同在 杭深线 / 龙厦线 / 甬广高铁），
  保留完整候选列表并标为待定（certain=False），绝不猜测拍板；
- 未收录的冷门支线返回 None，留空不误标。
"""
import re

# (线路展示名, 类别 hsr|mixed|conv, 是否允许普速机车牵引 Z/T/N/K/V, 路径元组列表)
# 类别口径：
# - hsr: 250-350km/h 高速客专/城际，仅允许 G/C/D（严禁普速 Z/T/N/K/V）
# - mixed: 160-200km/h 客货共线或快铁，allows_conv=True 时亦允许普速机车牵引
# - conv: 普速干线，严禁 G；允许 Z/T/N/K/V 以及 CR200J 动力集中动车组（或无动车车型冲突的跨线 D/C）
LINE_DEFS: tuple[tuple[str, str, bool, tuple[tuple[str, ...], ...]], ...] = (
    # -------- 东南 / 福建及周边 --------
    (
        "合福高铁",
        "hsr",
        False,
        (
            (
                "合肥南", "长临河", "巢湖东", "无为", "铜陵北", "南陵", "泾县", "旌德",
                "绩溪北", "歙县北", "黄山北", "婺源", "德兴", "上饶", "五府山",
                "武夷山北", "南平市", "建瓯西", "延平", "古田北", "闽清北", "福州",
            ),
        ),
    ),
    (
        "甬广高铁(福厦段)",
        "hsr",
        False,
        (
            ("福州", "福州南", "福清西", "莆田", "泉州东", "泉州南", "厦门北", "漳州"),
        ),
    ),
    (
        "杭深线",
        "mixed",
        False,
        (
            (
                "杭州东", "杭州南", "绍兴北", "余姚北", "庄桥", "宁波", "奉化", "宁海",
                "三门县", "临海", "台州西", "温岭", "雁荡山", "乐清东", "乐清",
                "温州北", "温州南", "瑞安", "平阳", "苍南", "福鼎", "太姥山",
                "霞浦", "福安", "宁德", "罗源", "连江", "福州", "福州南",
                "福清", "涵江", "莆田", "仙游", "惠安", "泉州", "晋江",
                "厦门北", "角美", "漳州", "漳浦", "云霄", "诏安", "饶平",
                "潮汕", "潮阳", "普宁", "葵潭", "陆丰", "汕尾", "鲘门",
                "惠东", "惠阳", "深圳坪山", "深圳北",
            ),
        ),
    ),
    (
        "龙厦线",
        "mixed",
        True,
        (
            ("龙岩", "古田会址", "南靖", "漳州", "角美", "厦门北"),
            ("龙岩", "古田会址", "南靖", "漳州", "角美", "厦门高崎", "厦门"),
        ),
    ),
    (
        "昌福线(向莆)",
        "mixed",
        False,
        (
            (
                "南昌西", "抚州北", "抚州", "南城", "南丰", "建宁县北", "泰宁",
                "将乐", "三明北", "尤溪", "永泰", "福州",
            ),
            (
                "南昌西", "抚州北", "抚州", "南城", "南丰", "建宁县北", "泰宁",
                "将乐", "三明北", "尤溪", "永泰", "涵江", "莆田",
            ),
        ),
    ),
    (
        "南龙线",
        "mixed",
        True,
        (
            ("延平", "延平西", "三明北", "三明", "永安南", "双洋", "漳平西", "雁石南", "龙岩"),
        ),
    ),
    (
        "赣瑞龙线",
        "mixed",
        True,
        (
            ("赣州", "于都", "会昌北", "瑞金", "长汀南", "冠豸山", "古田会址", "龙岩"),
        ),
    ),
    (
        "兴泉线",
        "mixed",
        True,
        (
            (
                "兴国", "宁都", "石城东", "宁化", "清流", "明溪", "三明北",
                "永安南", "大田北", "德化", "永春", "安溪东", "南安北", "泉州",
            ),
        ),
    ),
    (
        "衢宁线",
        "mixed",
        True,
        (
            ("衢州", "遂昌", "松阳", "龙泉市", "庆元", "松溪", "政和", "建瓯东", "屏南", "周宁", "宁德"),
        ),
    ),
    (
        "鹰厦线",
        "conv",
        True,
        (
            (
                "鹰潭", "资溪", "光泽", "邵武", "顺昌", "延平西", "来舟",
                "沙县", "三明", "永安", "漳平", "漳州东", "角美", "厦门高崎", "厦门",
            ),
        ),
    ),
    (
        "峰福线",
        "conv",
        True,
        (
            ("来舟", "延平西", "延平东", "福州"),
        ),
    ),
    (
        "杭台高铁",
        "hsr",
        False,
        (
            ("杭州东", "绍兴北", "上虞南", "嵊州北", "嵊州新昌", "天台山", "临海", "台州", "温岭", "玉环"),
        ),
    ),
    (
        "金温新双线",
        "hsr",
        False,
        (
            ("金华", "金华南", "武义北", "永康南", "缙云西", "丽水", "青田", "温州南"),
        ),
    ),
    (
        "杭昌高铁",
        "hsr",
        False,
        (
            (
                "杭州东", "杭州南", "富阳", "桐庐", "建德", "千岛湖", "三阳",
                "绩溪北", "歙县北", "黄山北", "黟县东", "祁门南", "浮梁东",
                "景德镇北", "鄱阳", "余干", "进贤北", "南昌东",
            ),
        ),
    ),
    # -------- 全国核心高铁 / 普速干线 --------
    (
        "京沪高铁",
        "hsr",
        False,
        (
            (
                "北京南", "廊坊", "天津南", "沧州西", "德州东", "济南西", "泰安",
                "曲阜东", "滕州东", "枣庄", "徐州东", "宿州东", "蚌埠南", "定远",
                "滁州", "南京南", "镇江南", "丹阳北", "常州北", "无锡东", "苏州北",
                "昆山南", "上海虹桥",
            ),
        ),
    ),
    (
        "京沪线",
        "conv",
        True,
        (
            (
                "北京", "廊坊北", "天津西", "沧州", "泊头", "东光", "吴桥",
                "德州", "济南", "泰山", "兖州", "邹城", "滕州", "枣庄西",
                "徐州", "宿州", "固镇", "蚌埠", "明光", "滁州北", "南京",
                "镇江", "丹阳", "常州", "无锡", "苏州", "昆山", "上海",
            ),
        ),
    ),
    (
        "京津城际",
        "hsr",
        False,
        (
            ("北京南", "亦庄", "武清", "天津", "军粮城北", "塘沽", "滨海"),
        ),
    ),
    (
        "沪宁城际",
        "hsr",
        False,
        (
            (
                "南京", "仙林", "宝华山", "镇江", "丹徒", "丹阳", "常州",
                "戚墅堰", "惠山", "无锡", "无锡新区", "苏州新区", "苏州",
                "苏州园区", "阳澄湖", "昆山南", "花桥", "安亭北", "南翔北",
                "上海西", "上海",
            ),
        ),
    ),
    (
        "沪昆高铁",
        "hsr",
        False,
        (
            (
                "上海虹桥", "上海松江", "金山北", "嘉善南", "嘉兴南", "桐乡",
                "海宁西", "临平南", "杭州东", "杭州南", "诸暨", "义乌", "金华",
                "龙游", "衢州", "江山", "玉山南", "上饶", "弋阳", "鹰潭北",
                "抚州东", "进贤南", "南昌西", "高安", "新余北", "宜春",
                "萍乡北", "醴陵东", "长沙南", "湘潭北", "韶山南", "娄底南",
                "邵阳北", "新化南", "溆浦南", "怀化南", "芷江", "新晃西",
                "铜仁南", "三穗", "凯里南", "贵定北", "贵阳东", "贵阳北",
                "平坝南", "安顺西", "关岭", "普安县", "盘州", "富源北",
                "曲靖北", "嵩明", "昆明南",
            ),
        ),
    ),
    (
        "沪昆线",
        "conv",
        True,
        (
            (
                "上海", "上海松江", "嘉兴", "海宁", "杭州南", "诸暨", "义乌",
                "金华", "龙游", "衢州", "江山", "玉山", "上饶", "弋阳",
                "鹰潭", "东乡", "进贤", "向塘", "丰城", "樟树", "新余",
                "分宜", "宜春", "萍乡", "醴陵", "株洲", "湘潭", "娄底",
                "怀化", "镇远", "凯里", "贵阳", "安顺", "六枝", "六盘水",
                "宣威", "曲靖", "昆明",
            ),
        ),
    ),
    (
        "宁杭高铁",
        "hsr",
        False,
        (
            ("南京南", "江宁", "句容西", "溧水", "瓦屋山", "溧阳", "宜兴", "长兴", "湖州", "德清", "杭州东"),
        ),
    ),
    (
        "商合杭高铁",
        "hsr",
        False,
        (
            (
                "商丘", "商丘东", "亳州南", "古城东", "太和东", "阜阳西",
                "颍上北", "凤台南", "寿县", "淮南南", "水家湖", "合肥北城",
                "合肥", "肥东", "柘皋", "巢湖东", "含山南", "芜湖北", "芜湖",
                "芜湖南", "湾沚南", "宣城", "郎溪南", "广德南", "安吉", "湖州",
                "德清", "杭州东",
            ),
        ),
    ),
    (
        "京广高铁",
        "hsr",
        False,
        (
            (
                "北京西", "北京丰台", "涿州东", "高碑店东", "保定东", "定州东",
                "正定机场", "石家庄", "高邑西", "邢台东", "邯郸东", "安阳东",
                "鹤壁东", "新乡东", "郑州东", "许昌东", "漯河西", "驻马店西",
                "明港东", "信阳东", "孝感北", "武汉", "咸宁北", "赤壁北",
                "岳阳东", "汨罗东", "长沙南", "株洲西", "衡山西", "衡阳东",
                "耒阳西", "郴州西", "乐昌东", "韶关", "英德西", "清远",
                "广州北", "广州南",
            ),
        ),
    ),
    (
        "广深港高铁",
        "hsr",
        False,
        (
            ("广州南", "虎门", "光明城", "深圳北", "福田", "香港西九龙"),
        ),
    ),
    (
        "京广线",
        "conv",
        True,
        (
            (
                "北京西", "北京丰台", "涿州", "高碑店", "保定", "定州",
                "石家庄", "邢台", "邯郸", "安阳", "鹤壁", "新乡", "郑州",
                "许昌", "临颍", "漯河", "西平", "驻马店", "确山", "信阳",
                "广水", "花园", "孝感", "汉口", "武昌", "咸宁", "赤壁",
                "岳阳", "汨罗", "长沙", "株洲", "衡山", "衡阳", "耒阳",
                "郴州", "坪石", "乐昌", "韶关东", "英德", "源潭", "广州北",
                "广州白云", "广州",
            ),
        ),
    ),
    (
        "京九线",
        "conv",
        True,
        (
            (
                "北京西", "北京丰台", "霸州", "任丘", "衡水", "聊城", "菏泽",
                "商丘南", "亳州", "阜阳", "潢川", "麻城", "黄州", "蕲春",
                "武穴", "九江", "南昌", "向塘", "吉安", "兴国", "赣州",
                "龙川", "河源", "惠州", "东莞东", "深圳",
            ),
        ),
    ),
    (
        "赣深高铁",
        "hsr",
        False,
        (
            (
                "赣州西", "信丰西", "龙南东", "定南南", "和平北", "龙川西",
                "河源北", "河源东", "博罗北", "惠州北", "仲恺", "东莞南",
                "光明城", "深圳北",
            ),
        ),
    ),
    (
        "宁蓉线",
        "mixed",
        False,
        (
            (
                "南京南", "全椒", "合肥南", "合肥", "六安", "金寨", "麻城北",
                "汉口", "汉川", "天门南", "仙桃西", "潜江", "荆州", "枝江北",
                "宜昌东", "巴东", "建始", "恩施", "利川", "石柱县", "丰都",
                "涪陵北", "重庆北",
            ),
        ),
    ),
    (
        "徐兰高铁",
        "hsr",
        False,
        (
            (
                "徐州东", "萧县北", "永城北", "商丘", "民权北", "兰考南",
                "开封北", "郑州东", "郑州西", "巩义南", "洛阳龙门", "渑池南",
                "三门峡南", "灵宝西", "华山北", "渭南北", "西安北", "咸阳西",
                "杨陵南", "岐山", "宝鸡南", "天水南", "秦安", "通渭",
                "定西北", "榆中", "兰州西",
            ),
        ),
    ),
    (
        "陇海线",
        "conv",
        True,
        (
            (
                "连云港东", "连云港", "徐州", "商丘", "开封", "郑州", "洛阳",
                "三门峡", "潼关", "渭南", "西安", "咸阳", "杨陵", "宝鸡",
                "天水", "陇西", "定西", "兰州",
            ),
        ),
    ),
    (
        "西成高铁",
        "hsr",
        False,
        (
            (
                "西安北", "鄠邑", "佛坪", "洋县西", "城固北", "汉中",
                "宁强南", "朝天", "广元", "剑门关", "青川", "江油北",
                "江油", "绵阳", "德阳", "广汉北", "新都东", "成都东",
            ),
        ),
    ),
    (
        "成渝高铁",
        "hsr",
        False,
        (
            (
                "成都东", "简阳南", "资阳北", "资中北", "内江北", "隆昌北",
                "荣昌北", "大足南", "永川东", "璧山", "沙坪坝", "重庆西",
            ),
            (
                "成都东", "简阳南", "资阳北", "资中北", "内江北", "隆昌北",
                "荣昌北", "大足南", "永川东", "璧山", "沙坪坝", "重庆",
            ),
        ),
    ),
    (
        "渝贵线",
        "mixed",
        False,
        (
            ("重庆西", "綦江东", "赶水东", "桐梓北", "桐梓东", "娄山关南", "遵义", "息烽", "修文县", "贵阳北"),
        ),
    ),
    (
        "贵广高铁",
        "hsr",
        False,
        (
            (
                "贵阳北", "贵阳东", "龙洞堡", "龙里北", "贵定县", "都匀东",
                "三都县", "榕江", "从江", "三江南", "五通", "桂林西",
                "阳朔", "恭城", "钟山西", "贺州", "怀集", "广宁",
                "肇庆东", "三水南", "佛山西", "广州南",
            ),
        ),
    ),
    (
        "京哈高铁",
        "hsr",
        False,
        (
            (
                "北京朝阳", "顺义西", "怀柔南", "密云", "兴隆县西", "安匠",
                "承德南", "承德县北", "平泉北", "牛河梁", "喀左", "辽宁朝阳",
                "北票", "阜新", "黑山北", "新民北", "沈阳西", "沈阳北",
                "铁岭西", "开原西", "昌图西", "四平东", "公主岭南", "长春西",
                "德惠西", "扶余北", "双城北", "哈尔滨西",
            ),
        ),
    ),
    (
        "哈大高铁",
        "hsr",
        False,
        (
            (
                "哈尔滨西", "双城北", "扶余北", "德惠西", "长春西", "公主岭南",
                "四平东", "昌图西", "开原西", "铁岭西", "沈阳北", "辽阳",
                "鞍山西", "海城西", "营口东", "盖州西", "鲅鱼圈", "瓦房店西",
                "大连北",
            ),
        ),
    ),
    (
        "济青高铁",
        "hsr",
        False,
        (
            (
                "济南东", "章丘北", "邹平", "淄博北", "临淄北", "青州市北",
                "潍坊北", "高密北", "胶州北", "青岛机场", "红岛", "青岛北",
            ),
        ),
    ),
    (
        "青荣城际",
        "hsr",
        False,
        (
            (
                "青岛", "青岛北", "即墨北", "莱西", "莱阳", "海阳北",
                "桃村北", "烟台南", "牟平", "威海北", "威海", "文登东", "荣成",
            ),
        ),
    ),
    (
        "中老铁路(昆磨/磨万)",
        "mixed",
        True,
        (
            (
                "昆明", "昆明南", "玉溪", "峨山", "化念", "元江", "墨江",
                "宁洱", "普洱", "野象谷", "西双版纳", "橄榄坝", "勐腊",
                "磨憨", "磨丁", "纳堆", "孟赛", "琅勃拉邦", "万荣", "万象",
            ),
        ),
    ),
)

# 跨线直跳防误判表：当相邻两停站之间没有经停中间站且存在多条截然不同的通道时，
# 即使单条线碰巧也连通两端，仍需标明多通道候选而非绝对确定。
AMBIGUOUS_NONSTOP_PAIRS: dict[frozenset[str], tuple[str, ...]] = {
    frozenset(("杭州东", "福州")): ("沪昆+合福", "杭深线"),
    frozenset(("杭州东", "福州南")): ("沪昆+合福", "杭深线"),
    frozenset(("杭州南", "福州")): ("沪昆+合福", "杭深线"),
    frozenset(("杭州南", "福州南")): ("沪昆+合福", "杭深线"),
    frozenset(("杭州东", "温州南")): ("杭深线", "杭台+甬台温", "沪昆+金温"),
    frozenset(("福州", "南昌西")): ("昌福线(向莆)", "合福+沪昆"),
    frozenset(("福州南", "南昌西")): ("昌福线(向莆)", "合福+沪昆"),
    frozenset(("上饶", "杭州东")): ("沪昆高铁",),
}

_CR200J_RE = re.compile(r"^(?:CR200J|ZL200J)", re.I)
_HS_CRH_RE = re.compile(r"^(?:CRH|CR4\d{2}|CR3\d{2})", re.I)


def _line_index():
    by_station: dict[str, list[tuple[str, str, bool]]] = {}
    for name, kind, allows_conv, paths in LINE_DEFS:
        seen_st = set()
        for path in paths:
            for st in path:
                if st not in seen_st:
                    seen_st.add(st)
                    by_station.setdefault(st, []).append((name, kind, allows_conv))
    return by_station


_STATION_LINES = _line_index()


def _line_allowed_for_train(kind: str, allows_conv: bool, train_code: str, train_style: str) -> bool:
    code = (train_code or "").strip().upper()
    pfx = code[:1] if code else ""
    st = (train_style or "").strip().split("_")[0].strip()
    is_cr200j = bool(_CR200J_RE.match(st))
    is_fast_emu = bool(_HS_CRH_RE.match(st))

    is_conv_code = pfx in ("Z", "T", "N", "K", "V", "Y", "L") or (code != "" and code[0].isdigit())
    if is_conv_code and not is_fast_emu and not is_cr200j:
        return kind == "conv" or (kind == "mixed" and allows_conv)
    if pfx == "G":
        return kind in ("hsr", "mixed")
    if pfx in ("D", "C", "X"):
        if is_fast_emu:
            return kind in ("hsr", "mixed")
        if is_cr200j:
            return kind in ("conv", "mixed")
        # 车型未知的 D/C 列车：可走 hsr/mixed，亦保留可跑动集的 conv 作为防漏候选
        return True
    return True


def infer_hop_lines(
    u: str,
    v: str,
    train_code: str = "",
    train_style: str = "",
) -> tuple[list[str], bool]:
    """推断相邻两停站 (u, v) 所属候选线路列表与是否唯一确定。"""
    u_s = (u or "").strip()
    v_s = (v or "").strip()
    if not u_s or not v_s or u_s == v_s:
        return ([], False)

    ov = AMBIGUOUS_NONSTOP_PAIRS.get(frozenset((u_s, v_s)))
    if ov:
        cands = list(ov)
        return (cands, len(cands) == 1)

    lu = _STATION_LINES.get(u_s)
    lv = _STATION_LINES.get(v_s)
    if not lu or not lv:
        return ([], False)

    right_names = {name for name, _, _ in lv}
    raw_cands: list[tuple[str, str]] = []
    seen = set()
    for name, kind, allows_conv in lu:
        if name not in right_names or name in seen:
            continue
        if _line_allowed_for_train(kind, allows_conv, train_code, train_style):
            seen.add(name)
            raw_cands.append((name, kind))

    if not raw_cands:
        return ([], False)

    # 若车型未知的 D/C 列车同时命中 hsr/mixed 与 conv（如 南京->上海 的 沪宁城际 vs 京沪线）：
    # - 已知是 CRH/CR400/CR300 时已在 _line_allowed_for_train 剔除 conv；
    # - 已知是 CR200J 时剔除 hsr，只留 mixed/conv；
    # - 二者皆无时诚实保留多线候选。
    names = [n for n, _ in raw_cands]
    return (names, len(names) == 1)


def _slice_leg_stops(from_name: str, to_name: str, stop_names: list[str] | None) -> list[str]:
    fn = (from_name or "").strip()
    tn = (to_name or "").strip()
    if not fn or not tn:
        return []
    if stop_names and len(stop_names) >= 2:
        fi = next((idx for idx, s in enumerate(stop_names) if s == fn), -1)
        if fi >= 0:
            ti = next((idx for idx in range(fi + 1, len(stop_names)) if stop_names[idx] == tn), -1)
            if ti > fi:
                seq = []
                for s in stop_names[fi : ti + 1]:
                    if s and (not seq or seq[-1] != s):
                        seq.append(s)
                if len(seq) >= 2:
                    return seq
    return [fn, tn] if fn != tn else []


# 同城枢纽短接（始发/通过同一城市相邻大站进入正线，如 福州→福州南、杭州东→杭州南）：
# 仅当同城短接的候选集包含紧邻干线的唯一线路时并入该干线，绝不对跨城共廊区间（如 厦门北→漳州）盲目消歧。
SAME_HUB_APPROACHES: frozenset[frozenset[str]] = frozenset((
    frozenset(("福州", "福州南")),
    frozenset(("杭州东", "杭州南")),
    frozenset(("北京西", "北京丰台")),
    frozenset(("青岛", "青岛北")),
    frozenset(("昆明", "昆明南")),
    frozenset(("广州", "广州北")),
    frozenset(("广州白云", "广州")),
    frozenset(("广州白云", "广州北")),
    frozenset(("厦门", "厦门高崎")),
    frozenset(("重庆", "沙坪坝")),
))


def infer_leg_lines(
    from_name: str,
    to_name: str,
    train_code: str = "",
    train_style: str = "",
    stop_names: list[str] | None = None,
) -> dict | None:
    """推断整段乘车区间所属铁路线路：区分绝对确定与多线候选。"""
    seq = _slice_leg_stops(from_name, to_name, stop_names)
    if len(seq) < 2:
        return None

    raw_hops: list[dict] = []
    for i in range(len(seq) - 1):
        u, v = seq[i], seq[i + 1]
        lines, certain = infer_hop_lines(u, v, train_code=train_code, train_style=train_style)
        raw_hops.append({"from": u, "to": v, "lines": lines, "certain": certain})

    # 同城枢纽短接并入邻近已确定正线（前后扫描各一遍以覆盖首尾接入）
    for _ in range(2):
        for i, h in enumerate(raw_hops):
            if h["certain"] or frozenset((h["from"], h["to"])) not in SAME_HUB_APPROACHES:
                continue
            for nb_idx in (i + 1, i - 1):
                if 0 <= nb_idx < len(raw_hops):
                    nb = raw_hops[nb_idx]
                    if nb["certain"] and nb["lines"] and (not h["lines"] or nb["lines"][0] in h["lines"]):
                        h["lines"] = list(nb["lines"])
                        h["certain"] = True
                        break

    segments: list[dict] = []
    for h in raw_hops:
        if (
            segments
            and segments[-1]["lines"] == h["lines"]
            and segments[-1]["certain"] == h["certain"]
        ):
            segments[-1]["to"] = h["to"]
        else:
            segments.append(dict(h))

    if not any(seg["lines"] for seg in segments):
        return None

    all_mapped = all(bool(seg["lines"]) for seg in segments)
    all_certain = all_mapped and all(seg["certain"] for seg in segments)
    has_certain = any(seg["certain"] for seg in segments)

    full_parts = []
    for seg in segments:
        if not seg["lines"]:
            full_parts.append("未收录")
        elif seg["certain"]:
            full_parts.append(seg["lines"][0])
        else:
            full_parts.append("/".join(seg["lines"]) + "?")

    certain_summary = ""
    if all_certain:
        certain_summary = " → ".join(full_parts)
    elif has_certain:
        c_parts: list[str] = []
        for seg in segments:
            lbl = seg["lines"][0] if seg["certain"] else "待定"
            if lbl == "待定" and c_parts and c_parts[-1] == "待定":
                continue
            c_parts.append(lbl)
        certain_summary = " → ".join(c_parts)

    return {
        "summary": " → ".join(full_parts),
        "certain_summary": certain_summary,
        "certain": all_certain,
        "segments": segments,
    }


def infer_timetable_hops(
    stops: list[dict],
    train_code: str = "",
    train_style: str = "",
) -> list[dict | None]:
    """为时刻表每一停站生成从前一站到本站的区间线路判定（首站为 None）。"""
    out: list[dict | None] = []
    for i, s in enumerate(stops):
        if i == 0:
            out.append(None)
            continue
        u = (stops[i - 1].get("station_name") or "").strip()
        v = (s.get("station_name") or "").strip()
        lines, certain = infer_hop_lines(u, v, train_code=train_code, train_style=train_style)
        if not lines:
            out.append(None)
        else:
            out.append({
                "lines": lines,
                "certain": certain,
                "label": lines[0] if certain else ("/".join(lines) + "?"),
            })
    return out
