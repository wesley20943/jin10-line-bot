"""Protocol and filter engine shared with Colab WebSocket 2.3."""
import json, re, logging, struct

import anyio

import httpx2

JSON_CODES = {1000, 1001, 1002, 1003, 1005, 1007, 1100, 1110, 4002}

class SetupError(Exception):
    pass

import html

import json

import re

import unicodedata

from collections import Counter

from datetime import datetime

from functools import lru_cache

from urllib.parse import urlsplit, urlunsplit

FILTER_VERSION = '1.1-preview'

KEYWORD_SPEC = {'source': '新聞篩選關鍵字_v1.json', 'entities': {'policy_people': ['川普', '特朗普', 'Trump', 'Donald Trump', '貝森特', '贝森特', 'Bessent', 'Warsh', '沃什', '華許', '沃勒', 'Waller', '植田', '植田和男', 'Ueda', '片山皋月', '片山さつき', 'Katayama'], 'ai_companies': ['NVIDIA', 'NVDA', '英伟达', '英偉達', '輝達', 'Microsoft', 'MSFT', '微软', '微軟', 'Alphabet', 'Google', 'GOOG', 'GOOGL', '谷歌', 'Amazon', 'AMZN', '亚马逊', '亞馬遜', 'AWS', 'Meta', 'Meta Platforms', 'OpenAI', 'Anthropic', 'SoftBank', '软银', '軟銀', 'Oracle', 'ORCL', '甲骨文', 'Broadcom', 'AVGO', '博通'], 'chip_companies': ['NVIDIA', 'NVDA', '英伟达', '英偉達', '輝達', 'TSMC', 'TSM', '台积电', '台積電', 'AMD', '超微', 'Intel', 'INTC', '英特尔', '英特爾', 'Micron', 'MU', '美光', 'SK hynix', '海力士', 'SKHY', 'Samsung', '三星电子', '三星電子', 'Kioxia', '铠侠', '鎧俠', '凱俠', 'SanDisk', 'SNDK', '闪迪', '閃迪', 'Broadcom', 'AVGO', '博通', 'Qualcomm', 'QCOM', '高通', 'Arm', '安謀', '安谋', 'Apple', 'AAPL', '苹果', '蘋果'], 'consumer_companies': ['Walmart', 'WMT', '沃尔玛', '沃爾瑪', 'Costco', 'COST', '好市多']}, 'topics': [{'id': 'bonds', 'title': '美債、全球長債與財政操作', 'subjects': ['美債', '美债', '美國國債', '美国国债', '美国公债', '美國財政部', '美国财政部', 'Treasury', 'US2Y', 'US10Y', 'US20Y', 'US30Y', '日本國債', '日本国债', '日債', '日债', 'JGB', '英國國債', '英国国债', '德國國債', '德国国债', '法國國債', '法国国债', '長端', '长端', '長債', '长债', '貝森特', '贝森特', 'Bessent'], 'triggers': ['殖利率', '收益率', 'yield', '拍賣', '拍卖', '競拍', '竞拍', '投標倍數', '投标倍数', '得標利率', '得标利率', '尾差', '間接投標', '间接投标', '回購', '回购', 'buyback', '季度再融資', '季度再融资', '發行規模', '发行规模', '國庫券', '国库券', 'T-bills', '期限溢價', '期限溢价', 'term premium', '收益率曲線', '收益率曲线', '熊陡', '財政赤字', '财政赤字', '債務上限', '债务上限', 'TIC', '外資淨買入', '外资净买入', '外國央行持有', '外国央行持有'], 'entity_sets': []}, {'id': 'monetary_policy', 'title': '央行政策與美元流動性', 'subjects': ['聯準會', '美聯儲', '美联储', 'Fed', 'FOMC', '紐約聯儲', '纽约联储', '日本央行', '日本銀行', 'BOJ', '歐洲央行', '欧洲央行', 'ECB', '英國央行', '英国央行', 'BOE', '澳洲聯儲', '澳洲联储', 'RBA', 'Warsh', '沃什', '華許', '沃勒', 'Waller', '植田', 'Ueda', 'TGA', 'SOFR', 'RMP'], 'triggers': ['利率決議', '利率决议', '升息', '加息', '降息', '維持利率', '维持利率', '投票', '反對票', '反对票', '貨幣政策', '货币政策', '會議紀要', '会议纪要', '記者會', '记者会', 'Jackson Hole', '傑克遜霍爾', '杰克逊霍尔', '通膨目標', '通胀目标', '資產負債表', '资产负债表', '準備金', '准备金', '流動性', '流动性', '國庫券購買', '国库券购买', '回購利率', '回购利率', '融資成本', '融资成本', '一般帳戶', '一般账户', '財政總帳戶', '财政总账户'], 'entity_sets': []}, {'id': 'macro_data', 'title': '通膨、就業與經濟需求', 'subjects': ['CPI', '核心CPI', 'PPI', 'PCE', '核心PCE', 'Core PCE', '非農', '非农', 'NFP', '失業率', '失业率', '勞動參與率', '劳动参与率', 'LFPR', '平均時薪', '平均时薪', '每週工時', '每周工时', '初請失業金', '初请失业金', '續請失業金', '续请失业金', 'ADP', 'JOLTS', 'JOLTs', '職位空缺', '职位空缺', '挑戰者裁員', '挑战者裁员', '挑戰者企業裁員', '挑战者企业裁员', 'PMI', 'ISM', 'GDP', '零售銷售', '零售销售', '個人支出', '个人支出', '工業產出', '工业产出', '消費者信心', '消费者信心', '通膨預期', '通胀预期', '短觀', '短观', '日本工資', '日本工资', '勞工現金收入', '劳工现金收入', '實際薪資', '实际薪资', '家庭支出'], 'triggers': ['公布', '公佈', '錄得', '录得', '預期', '预期', '前值', '修正', '修訂', '修订', '初值', '終值', '终值', '月率', '年率', '同店銷售', '同店销售', '可比銷售', '可比销售', '業績指引', '业绩指引'], 'entity_sets': ['consumer_companies']}, {'id': 'japan_fx', 'title': '日圓、匯率干預與日本資金流', 'subjects': ['日圓', '日元', '日幣', '日币', 'JPY', 'USD/JPY', 'USDJPY', 'AUD/JPY', 'AUDJPY', '日本央行', 'BOJ', '日本財務省', '日本财务省', '日本財務大臣', '日本财务大臣', '植田', 'Ueda', '片山皋月', 'GPIF', '日本政府養老投資基金', '日本政府养老投资基金', 'carry trade'], 'triggers': ['干預', '干预', '匯率檢查', '汇率检查', '匯率核查', '汇率核查', 'rate check', '拋售美元', '抛售美元', '出售美元', '買入日元', '买入日元', '套息交易', '套利交易', '拆倉', '拆仓', '平倉', '平仓', 'unwind', '升息', '加息', '外匯存底', '外汇储备', '買進外國債券', '买进外国债券', '買進外國股票', '买进外国股票', '外資買進日債', '外资买进日债', '外資買進日股', '外资买进日股', '資金回流', '资金回流', '投資配置', '投资配置', '互換', '互换', 'swap'], 'entity_sets': []}, {'id': 'energy', 'title': '原油、成品油與實際供應', 'subjects': ['原油', '石油', 'WTI', 'Brent', '布倫特', '布伦特', '柴油', '汽油', '航煤', '成品油', '天然氣', '天然气', 'LNG', '沙特阿美', 'Saudi Aramco', 'OPEC', 'OPEC+', '歐佩克', '欧佩克', '霍爾木茲', '霍尔木兹', '荷莫茲', '曼德海峽', '曼德海峡', '紅海', '红海', '蘇伊士', '苏伊士', '油輪', '油轮', '煉油廠', '炼油厂', '煉廠', '炼厂', '輸油管道', '输油管道', '戰略石油儲備', '战略石油储备', 'SPR'], 'triggers': ['減產', '减产', '增產', '增产', '停產', '停产', '停運', '停运', '恢復', '恢复', '重啟', '重启', '出口禁令', '禁止出口', '配額', '配额', '封鎖', '封锁', '開放', '开放', '通航', '通行', '襲擊', '袭击', '受損', '受损', '供應中斷', '供应中断', '釋放儲備', '释放储备', '放油', '補庫', '补库', '庫存', '库存', '裂解價差', '裂解价差', 'crack spread', '官方售價', '官方售价', 'OSP', '現貨升水', '现货升水', '運費', '运费', '航運保險', '航运保险'], 'entity_sets': []}, {'id': 'geopolitics_trade', 'title': '地緣變化、關稅與出口限制', 'subjects': ['川普', '特朗普', 'Trump', '白宮', '白宫', '貝森特', '贝森特', 'Bessent', '伊朗', '以色列', '革命衛隊', '革命卫队', '胡塞', '胡賽', '巴基斯坦', '卡塔爾', '卡塔尔', '阿曼', '俄羅斯', '俄罗斯', '烏克蘭', '乌克兰', '委內瑞拉', '委内瑞拉', '中美', '美中', '美加', '歐盟', '欧盟', '加拿大', '商務部', '商务部'], 'triggers': ['停火', '和談', '和谈', '談判', '谈判', '協議', '协议', '斡旋', '打擊', '打击', '襲擊', '袭击', '封鎖', '封锁', '制裁', '二級制裁', '二级制裁', '解除制裁', '制裁豁免', '關稅', '关税', 'tariff', '出口管制', '出口限制', '出口許可', '出口许可', '禁運', '禁运', '生效', '撤回', '取消', '延期', '否認', '否认', '闢謠', '辟谣', '更正'], 'entity_sets': []}, {'id': 'ai_finance', 'title': 'AI 現金流、融資與信用', 'subjects': ['人工智慧', '人工智能', 'AI', '資料中心', '数据中心', '算力', 'GPU', 'Hyperscaler', '雲端', '云计算'], 'triggers': ['資本支出', '资本支出', 'CapEx', '自由現金流', '自由现金流', 'FCF', '營運現金流', '经营现金流', '毛利率', '財報', '财报', '業績指引', '业绩指引', '營收', '营收', 'ARR', '融資', '融资', '發債', '发债', '貸款', '贷款', '財務擔保', '财务担保', '抵押', '殘值', '残值', '表外', 'SPV', '採購承諾', '采购承诺', '租賃', '租赁', '折舊', '折旧', '循環融資', '循环融资', '供應商融資', '供应商融资', 'vendor financing', 'CDS', '信用利差', '融資成本', '融资成本', 'IPO', '招股書', '招股说明书', '推遲上市', '推迟上市', '違約', '违约', '保險', '保险'], 'entity_sets': ['ai_companies']}, {'id': 'chips_competition', 'title': '半導體供應鏈與 AI 商業競爭', 'subjects': ['半導體', '半导体', '晶片', '芯片', '記憶體', '存储芯片', 'HBM', 'DRAM', 'NAND', 'GPU', '先進封裝', '先进封装', 'CoWoS', '晶圓代工', '晶圆代工', 'AI模型', 'AI 模型', '人工智能模型', '人工智慧模型', '雲服務', '云服务', '推理', '開源模型', '开源模型'], 'triggers': ['漲價', '涨价', '降價', '降价', '定價', '定价', '價格戰', '价格战', '售價', '售价', '產能', '产能', '擴產', '扩产', '減產', '减产', '缺貨', '缺货', '供應限制', '供应限制', '訂單', '订单', '出貨', '出货', '財報', '财报', '營收', '营收', '毛利', '指引', '出口許可', '出口许可', '出口管制', '開源', '开源', '免費', '免费', '收費', '收费', '成本', '商業化', '商业化', '取消發布', '取消发布', '延後發布', '推迟发布', '監管限制', '监管限制', '重大中斷', '重大中断'], 'entity_sets': ['chip_companies', 'ai_companies']}, {'id': 'metals', 'title': '黃金、白銀與銅的供需', 'subjects': ['黃金', '黄金', '白銀', '白银', '貴金屬', '贵金属', 'Gold', 'Silver', 'XAU', 'XAG', '金價', '金价', '銀價', '银价', '銅價', '铜价', '銅礦', '铜矿', '銅產量', '铜产量', 'Copper', 'COMEX', 'LME', '智利國家銅業', '智利国家铜业', 'Codelco'], 'triggers': ['央行購金', '央行购金', '增持', '減持', '减持', '黃金儲備', '黄金储备', '資產凍結', '资产冻结', '去美元化', '美元信用', '黃金掛鉤', '黄金挂钩', 'ETF', '資金流入', '资金流入', '資金流出', '资金流出', '庫存', '库存', '升水', '折價', '折价', '短缺', '交割', '擠兌', '挤兑', '產量', '产量', '發貨量', '发货量', '關稅', '关税', '出口限制', '保證金', '保证金', '持倉', '持仓', '結算', '结算'], 'entity_sets': []}, {'id': 'flows_structure', 'title': '槓桿、資金流與市場制度', 'subjects': ['槓桿ETF', '杠杆ETF', '槓桿產品', '杠杆产品', '養老金', '养老金', 'GPIF', 'ETF', 'CTA', '對沖基金', '对冲基金', '韓國', '韩国', 'KOSPI', '日經', '日经', 'Nikkei', '納指', '纳指', 'Nasdaq', '標普', '标普', 'S&P 500', 'Russell', '羅素', '罗素', '道指', 'Dow', '台指', '台股', '費半', '费半', 'SOX', 'ADR', '期貨', '期货', '交易所'], 'triggers': ['保證金', '保证金', '槓桿上限', '杠杆上限', '降低槓桿', '降低杠杆', '強制平倉', '强制平仓', '去槓桿', '去杠杆', '再平衡', 'rebalancing', '淨買入', '净买入', '淨賣出', '净卖出', '配置調整', '配置调整', '回購', '回购', '庫藏股', '库藏股', '贖回', '赎回', '申購', '申购', '溢價', '溢价', '折價', '折价', '特別氣配', '特别气配', '特別報價', '特别报价', '延遲開盤', '延迟开盘', '停牌', '熔斷', '熔断', '成分調整', '成分调整', '到期', '換倉', '换仓', '移倉', '移仓', '結算', '结算', '休市'], 'entity_sets': ['chip_companies']}], 'corrections': ['否認', '否认', '闢謠', '辟谣', '澄清', '更正', '修正', '撤回', '撤銷', '撤销', '取消', '不屬實', '不属实', '不實', '不实'], 'uncertain': ['據悉', '据悉', '消息人士', '市場消息', '市场消息', '可能', '考慮', '考虑', '擬', '拟', '計劃', '计划', '尚未證實', '尚未证实'], 'conditional_watch': [{'name': '印度／巴西等央行匯率干預', 'aliases': ['印度央行', 'RBI', '盧比', '卢比', 'INR', '巴西央行']}, {'name': '新增公司與模型', 'aliases': ['SpaceX', 'Palantir', 'PLTR', 'Marvell', 'MRVL', 'DeepSeek', 'Kimi']}]}

CONTENT_EXCLUSION_RULES = {'專題追蹤': ['期货热点追踪', '期貨熱點追蹤'], '新聞彙總': ['金十数据整理', '金十數據整理', '重要新闻汇总', '重要新聞彙總', '重要新聞匯總', '重要新聞彙整', '要闻汇总', '要聞彙總', '要聞匯總', '快讯汇总', '快訊彙總', '快訊匯總']}

class PreviewError(ValueError):
    """可直接向使用者顯示的格式／步驟錯誤。"""

def normalize(value):
    return re.sub('\\s+', ' ', unicodedata.normalize('NFKC', value).casefold()).strip()

@lru_cache(maxsize=2048)
def alias_regex(alias):
    text = normalize(alias)
    escaped = re.escape(text).replace('\\ ', '\\s+')
    if re.match('[a-z0-9_]', text):
        escaped = '(?<![a-z0-9_])' + escaped
    if re.search('[a-z0-9_]$', text):
        escaped += '(?![a-z0-9_])'
    return re.compile(escaped)

def find_aliases(text, aliases):
    matched = []
    seen = set()
    for alias in aliases:
        token = normalize(alias)
        if token in seen or not alias_regex(alias).search(text):
            continue
        if token in {'mu', 'arm', 'cost'}:
            if not re.search('公司|股|財報|财报|營收|营收|晶片|芯片|半導體|半导体|earnings|stock|\\([a-z]+\\.[a-z]+\\)', text):
                continue
        seen.add(token)
        matched.append(alias)
    return matched

def canonical_url(value):
    if not isinstance(value, str):
        return ''
    value = value.strip()
    match = re.fullmatch('\\[[^\\]]*\\]\\((https?://[^\\s]+)\\)', value)
    if match:
        value = match.group(1)
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {'https', 'http'} or not parsed.netloc:
            return ''
        if parsed.username or parsed.password:
            return ''
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.query, ''))
    except ValueError:
        return ''

def text_only(value):
    value = re.sub('<br\\s*/?>', '\n', value, flags=re.I)
    value = re.sub('<[^>]*>', '', value)
    return html.unescape(value).strip()

def extract_items(raw):
    if raw is None:
        raise PreviewError('找不到 RAW_RESULT。請先執行第 5 步「一次性資料查詢」，成功取得 list_flash 快訊後，再執行本格。')
    if not isinstance(raw, dict):
        raise PreviewError('RAW_RESULT 格式不同。請回報最外層欄位名稱，不用貼 Token。')
    if raw.get('is_error') is True:
        raise PreviewError('上一次金十查詢回傳錯誤，不能把錯誤訊息當快訊篩選。請先處理第 5 步的錯誤。')
    payload = raw.get('structured_content')
    if payload is None:
        candidates = []
        blocks = raw.get('text_content', [])
        if not isinstance(blocks, list):
            raise PreviewError('text_content 格式不同，請回報欄位名稱。')
        for block in blocks:
            if not isinstance(block, str):
                continue
            try:
                parsed = json.loads(block)
            except (ValueError, TypeError):
                continue
            if isinstance(parsed, dict) and isinstance(parsed.get('data'), dict):
                if isinstance(parsed['data'].get('items'), list):
                    candidates.append(parsed)
        if len(candidates) != 1:
            raise PreviewError('找不到唯一且完整的快訊 JSON。請使用第 5 步記憶體中的 RAW_RESULT，不要使用畫面截短的文字。')
        payload = candidates[0]
    if not isinstance(payload, dict):
        raise PreviewError('structured_content 不是物件；請回報欄位名稱。')
    status = payload.get('status')
    if status is not None and status not in (200, '200'):
        raise PreviewError('資料服務回傳非成功狀態，未進行篩選。')
    data = payload.get('data')
    if not isinstance(data, dict) or not isinstance(data.get('items'), list):
        raise PreviewError('未找到 structured_content → data → items。請確認第 4 步選的是 list_flash。')
    return (data['items'], data.get('has_more') is True)

def event_segments(text):
    pattern = '(?:^|\\n)\\s*\\d{1,3}[.、．)]\\s*'
    if len(re.findall(pattern, text)) >= 2:
        return ([x.strip() for x in re.split(pattern, text) if x.strip()], True)
    return ([text], False)

def headline_text(text):
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), '')
    if first_line.startswith('【') and '】' in first_line[:260]:
        return first_line[1:first_line.index('】')]
    head = re.split('金十(?:数据|數據)[0-9年月日\\s./-]{0,24}[讯訊]', first_line, maxsplit=1)[0]
    return (head or first_line)[:260].strip('【】 []')

def content_exclusion(text):
    headline = normalize(headline_text(text))
    for content_type, tags in CONTENT_EXCLUSION_RULES.items():
        hits = [tag for tag in tags if normalize(tag) in headline]
        if hits:
            return (content_type, '依你的偏好排除：標題包含「' + hits[0] + '」')
    full = normalize(text)
    report_form = bool(re.search('金十(?:数据|數據)[0-9年月日\\s./-]{0,24}[讯訊]', full))
    executive = bool(re.search('新任(?:命)?.{0,8}(?:ceo|首席执行官|首席執行官|執行長|执行长)', headline))
    reform = bool(re.search('改革|重组|重組|组织架构|組織架構|精简|精簡', headline))
    hard_event = bool(re.search('财报|財報|业绩|業績|营收|營收|利润|利潤|指引|停产|停產|停运|停運|供应中断|供應中斷', headline))
    if report_form and executive and reform and (len(full) >= 180) and (not hard_event):
        return ('公司背景整理', '依你的偏好排除：新任主管改革的展開報導，與你標示的蘋果背景整理同類')
    return None

def release_hint(text):
    numeric = bool(re.search('\\d', text))
    actual = bool(re.search('公布|公佈|錄得|录得|實際|实际|前值|修正|修訂|修订|初值|終值|终值|超預期|超预期|不及預期|不及预期|低於預期|低于预期|高於預期|高于预期', text))
    direct_value = bool(re.search('(?:cpi|ppi|pce|pmi|gdp|nfp|指數|指数|月率|年率|人數|人数|失業率|失业率)[\\s:：為为]*[-+]?\\d', text))
    forward = bool(re.search('前瞻|預計|预计|預測|预测|即將|即将|將於|将于|今晚|明晚', text[:130]))
    if forward and (not re.search('錄得|录得|前值由|修正為|修正为|已公布', text[:130])):
        return False
    return numeric and (actual or direct_value)

def classify(text):
    excluded = content_exclusion(text)
    if excluded:
        content_type, reason = excluded
        return {'decision': '排除', 'topics': [], 'subject_hits': [], 'trigger_hits': [], 'reason': reason, 'status_hint': '內容類型排除', 'content_type': content_type}
    segments, is_digest = event_segments(text)
    entity_sets = KEYWORD_SPEC['entities']
    matched_topics, subject_topics = ({}, {})
    corrections, uncertain = ([], [])
    for raw_segment in segments:
        segment = normalize(raw_segment)
        segment_corrections = find_aliases(segment, KEYWORD_SPEC['corrections'])
        corrections.extend(segment_corrections)
        uncertain.extend(find_aliases(segment, KEYWORD_SPEC['uncertain']))
        for watch in KEYWORD_SPEC['conditional_watch']:
            watch_hits = find_aliases(segment, watch['aliases'])
            if watch_hits:
                subject_topics.setdefault(watch['name'] + '（延伸觀察）', []).extend(watch_hits)
        for topic in KEYWORD_SPEC['topics']:
            subject_terms = list(topic['subjects'])
            for ref in topic['entity_sets']:
                subject_terms.extend(entity_sets[ref])
            subjects = find_aliases(segment, subject_terms)
            if topic['id'] == 'bonds':
                subjects.extend(re.findall('(?:美国|美國|日本|英國|英国|德國|德国|法國|法国)[0-9一二三四五六七八九十百年月期超長长短\\s]{0,12}(?:国债|國債|公债|公債)', segment))
            if not subjects:
                continue
            if topic['id'] == 'metals' and (not re.search('黃金|黄金|白銀|白银|貴金屬|贵金属|金價|金价|銀價|银价|銅|铜|(?<![a-z])(?:gold|silver|copper|xau|xag|codelco)(?![a-z])', segment)):
                continue
            triggers = find_aliases(segment, topic['triggers'])
            subject_topics.setdefault(topic['title'], []).extend(subjects)
            qualifies = bool(triggers or segment_corrections)
            detail = '主題與事件詞同段命中'
            if topic['id'] == 'macro_data':
                company_hit = bool(find_aliases(segment, entity_sets.get('consumer_companies', [])))
                consumer_detail = bool(re.search('同店|可比|指引|財報|财报|營收|营收', segment))
                qualifies = release_hint(segment) or (company_hit and consumer_detail)
                detail = '有數據／財報線索，需核對公布值與預期'
            if qualifies:
                entry = matched_topics.setdefault(topic['title'], {'subjects': [], 'triggers': [], 'reason': detail})
                entry['subjects'].extend(subjects)
                entry['triggers'].extend(triggers + segment_corrections)
    corrections = list(dict.fromkeys(corrections))
    uncertain = list(dict.fromkeys(uncertain))
    subjects = list(dict.fromkeys((x for values in subject_topics.values() for x in values)))
    triggers = list(dict.fromkeys((x for row in matched_topics.values() for x in row['triggers'])))
    if matched_topics:
        decision = '候選'
        reason = '；'.join(dict.fromkeys((row['reason'] for row in matched_topics.values())))
    elif subject_topics:
        decision = '觀察'
        reason = '命中關注主題，尚缺明確事件詞／公布值線索'
    else:
        decision = '未命中'
        reason = '目前詞庫未命中，仍列出供檢查漏網消息'
    if is_digest:
        reason += '；彙整文已按編號分段比對'
    if corrections:
        reason += '；含否認／更正類用詞，需核對它指向的事件'
    statuses = []
    if uncertain:
        statuses.append('有推測／傳聞用詞')
    if corrections:
        statuses.append('有否認／更正用詞')
    return {'decision': decision, 'topics': list(matched_topics or subject_topics), 'subject_hits': subjects, 'trigger_hits': triggers, 'reason': reason, 'status_hint': '；'.join(statuses) or '來源與狀態待核對'}

def build_report(raw, *, tool_name=None):
    if tool_name not in (None, '', 'list_flash'):
        raise PreviewError('本格是 list_flash 的預覽器。請先在第 4 步選 list_flash，再執行第 5 步。')
    items, has_more = extract_items(raw)
    rows, seen = ([], {})
    for index, item in enumerate(items, 1):
        if not isinstance(item, dict):
            rows.append({'number': index, 'decision': '格式待查', 'time': '', 'text': '', 'url': '', 'topics': [], 'subject_hits': [], 'trigger_hits': [], 'reason': '此筆不是資料物件；未悄悄丟棄', 'status_hint': '格式待查'})
            continue
        title = item.get('title', '')
        body = item.get('content', '')
        parts = [text_only(x) for x in [title, body] if isinstance(x, str) and x.strip()]
        text = '\n'.join(dict.fromkeys(parts))
        url = canonical_url(item.get('url'))
        timestamp = item.get('time', '')
        timestamp = timestamp if isinstance(timestamp, str) else ''
        row = {'number': index, 'time': timestamp, 'text': text, 'url': url}
        if not text:
            row.update({'decision': '格式待查', 'topics': [], 'subject_hits': [], 'trigger_hits': [], 'reason': '沒有可讀取的 title／content；請核對資料格式', 'status_hint': '格式待查'})
        else:
            row.update(classify(text))
            key = (url or timestamp, normalize(text))
            if row['decision'] == '排除':
                pass
            elif key in seen:
                row.update({'decision': '完全重複', 'reason': f'與第 {seen[key]} 則相同；本次預覽合併標記'})
            else:
                seen[key] = index
        rows.append(row)
    return {'version': FILTER_VERSION, 'total': len(items), 'has_more': has_more, 'counts': dict(Counter((row['decision'] for row in rows))), 'rows': rows, 'scope': '僅此批 RAW_RESULT；不代表完整歷史或全部最新消息', 'line_sent': 0, 'api_calls': 0}

def render_report(report, redactor=None):
    from IPython.display import HTML, display

    def safe(value):
        value = str(value)
        if callable(redactor):
            value = redactor(value)
        return html.escape(value, quote=True)

    def local_time(value):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                return value + '（未標示時區）'
            from datetime import timezone, timedelta
            return dt.astimezone(timezone(timedelta(hours=8))).strftime('%m/%d %H:%M:%S')
        except (ValueError, AttributeError):
            return value or '未提供時間'
    labels = ['候選', '觀察', '未命中', '排除', '完全重複', '格式待查']
    print('篩選預覽', report['version'], '完成｜共', report['total'], '則｜' + '・'.join((f"{k} {report['counts'].get(k, 0)}" for k in labels)))
    print('只比對現有 RAW_RESULT；本格金十請求 0 次、LINE 發送 0 則。')
    print('候選只是詞面命中，仍需你判斷是否重要；相似但不同文字的新聞尚未做語意去重。')
    if report['has_more']:
        print('這是單頁快訊，服務標示還有其他分頁；本格不自動翻頁。')
    if not report['rows']:
        print('金十回傳空清單，沒有可預覽的新聞。')
        return
    visible = [row for row in report['rows'] if row['decision'] != '排除']
    excluded = [row for row in report['rows'] if row['decision'] == '排除']
    if excluded:
        print(f'已排除 {len(excluded)} 則整理／專題內容，放在下方收合區。')
    table = ['<div style="overflow-x:auto"><table style="border-collapse:collapse;width:100%;font-size:14px">', '<thead><tr>' + ''.join((f'<th style="text-align:left;border-bottom:2px solid #888;padding:8px">{safe(h)}</th>' for h in ['編號', '時間（台北）', '結果', '主題／命中詞', '新聞', '理由'])) + '</tr></thead><tbody>']
    colors = {'候選': '#0b715b', '觀察': '#936800', '未命中': '#596579', '完全重複': '#596579', '格式待查': '#a12d31'}
    for row in visible:
        hits = '、'.join(row['subject_hits'][:6] + row['trigger_hits'][:6])
        categories = '、'.join(row['topics']) or '—'
        short = row['text'][:170] + ('…' if len(row['text']) > 170 else '')
        article = safe(short)
        if len(row['text']) > 170:
            article += '<details><summary>展開原文</summary><p style="white-space:pre-wrap">' + safe(row['text']) + '</p></details>'
        if row['url']:
            article += '<br><a target="_blank" rel="noopener noreferrer" href="' + safe(row['url']) + '">原始快訊</a>'
        values = [safe(row['number']), safe(local_time(row['time'])), '<b style="color:' + colors[row['decision']] + '">' + safe(row['decision']) + '</b>', safe(categories) + '<br><small>' + safe(hits or '—') + '</small>', article, safe(row['reason']) + '<br><small>' + safe(row['status_hint']) + '</small>']
        table.append('<tr>' + ''.join(('<td style="vertical-align:top;padding:10px 8px;border-bottom:1px solid #ddd;min-width:65px">' + v + '</td>' for v in values)) + '</tr>')
    table.append('</tbody></table></div>')
    if excluded:
        table.append(f'<details style="margin-top:16px"><summary>查看已排除的 {len(excluded)} 則與原因</summary><ul>')
        for row in excluded:
            table.append('<li><b>第 ' + safe(row['number']) + ' 則：</b>' + safe(headline_text(row['text'])) + '<br><small>' + safe(row['reason']) + '</small></li>')
        table.append('</ul></details>')
    display(HTML(''.join(table)))
    print('請回覆例子：「第 2 則想保留」「第 5 則不用」「第 8 和第 9 則希望合併」。')

def preview(raw=None, *, tool_name=None, redactor=None):
    try:
        report = build_report(raw, tool_name=tool_name)
    except PreviewError as exc:
        print(str(exc))
        return None
    render_report(report, redactor=redactor)
    return report

import asyncio

import sqlite3

import time as time_module

from datetime import datetime, timezone, timedelta

from hashlib import sha256

from pathlib import Path

from uuid import uuid4

TAIPEI = timezone(timedelta(hours=8))

class BotError(ValueError):
    pass

def news_datetime(value):
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError()
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError, TypeError):
        raise BotError('快訊時間格式不完整，已停止；請回報欄位格式。') from None

def event_key(row):
    identity = [row.get('url') or row['time'], normalize(row['text'])]
    return sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()

def utf16_length(text):
    return len(text.encode('utf-16-le')) // 2

def clip_utf16(text, limit):
    if utf16_length(text) <= limit:
        return text
    suffix = '\n（原文較長，完整內容請開啟來源）'
    return text.encode('utf-16-le')[:(limit - utf16_length(suffix)) * 2].decode('utf-16-le', errors='ignore') + suffix

def format_news(row):
    clock = news_datetime(row['time']).astimezone(TAIPEI).strftime('%m/%d %H:%M:%S')
    title = '、'.join(row['topics'][:3]) or '關注快訊'
    header = f'【我的關注快訊】\n{clock}（台北）\n主題：{title}\n'
    status = row.get('status_hint', '')
    if status and status != '來源與狀態待核對':
        header += f'提醒：{status}\n'
    url = row.get('url', '')
    footer = '\n來源：金十\n' + url if url and utf16_length(url) < 1000 else '\n來源：金十'
    body = clip_utf16(row['text'], min(2800, 4700 - utf16_length(header + footer)))
    return header + '\n' + body + footer

class NewsBot:

    def __init__(self, state, fetch_page, push_message, db_path, *, redactor=str, now=None, sleep=asyncio.sleep, monotonic=time_module.monotonic):
        if not isinstance(state, dict) or not all((state.get(k) for k in ('bot_id', 'user_id', 'token'))):
            raise BotError('請先完成 LINE 連線與收件人檢查。')
        self.state = dict(state)
        self.scope = sha256((state['bot_id'] + ':' + state['user_id']).encode()).hexdigest()
        self.fetch_page = fetch_page
        self.push_message = push_message
        self.redactor = redactor
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep
        self.monotonic = monotonic
        self.lock = asyncio.Lock()
        self.max_age = timedelta(minutes=15)
        self.stop_requested = False
        self.query_count = 0
        self.acceptance_count = 0
        self.last_stats = {}
        if db_path != ':memory:':
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path))
        self.db.row_factory = sqlite3.Row
        self.db.execute("CREATE TABLE IF NOT EXISTS outbox (\n            scope TEXT NOT NULL, event_key TEXT NOT NULL, text TEXT NOT NULL,\n            news_time REAL NOT NULL, status TEXT NOT NULL DEFAULT 'queued',\n            retry_key TEXT NOT NULL, first_attempt REAL, last_attempt REAL,\n            accepted_at REAL, last_status INTEGER,\n            PRIMARY KEY(scope, event_key))")
        self.db.commit()

    def log(self, message):
        print(self.redactor(message))

    def get(self, key):
        return self.db.execute('SELECT * FROM outbox WHERE scope=? AND event_key=?', (self.scope, key)).fetchone()

    def pending(self):
        return self.db.execute("SELECT * FROM outbox WHERE scope=? AND status='pending' ORDER BY first_attempt", (self.scope,)).fetchall()

    def eligible(self, row):
        age = self.now() - news_datetime(row['time'])
        return timedelta(seconds=-60) <= age <= self.max_age and row['decision'] == '候選'

    def make_plan(self, rows, *, max_messages=3, include_pending=True, oldest_first=False):
        if not 1 <= max_messages <= 10:
            raise BotError('每次傳送上限請設為 1～10 則。')
        keys = [r['event_key'] for r in self.pending()] if include_pending else []
        eligible = sorted((r for r in rows if self.eligible(r)), key=lambda r: news_datetime(r['time']), reverse=not oldest_first)
        for row in eligible:
            key = self.queue(row)
            if self.get(key)['status'] == 'queued' and key not in keys:
                keys.append(key)
        return {'scope': self.scope, 'keys': tuple(keys[:max_messages]), 'remaining': max(0, len(keys) - max_messages), 'created_at': self.now().timestamp()}

    def show_plan(self, plan):
        self.log(f"LINE 收件人：{self.state.get('recipient_name', '你設定的帳號')}｜官方帳號：{self.state.get('bot_name', '')}")
        self.log(f"本次預計傳送 {len(plan['keys'])} 則；超出上限尚有 {plan['remaining']} 則。")
        for i, key in enumerate(plan['keys'], 1):
            record = self.get(key)
            hint = '（前次結果待確認：沿用原訊息重試）' if record['status'] == 'pending' else ''
            self.log(f"\n── 第 {i} 則{hint} ──\n{record['text']}\n")
        if not plan['keys']:
            self.log('目前沒有可傳送項目：可能未命中、已傳過，或快訊超過 15 分鐘。可以執行第 5 步等新消息。')

    def show_feed_time(self, rows):
        if not rows:
            self.log('金十本次回傳空清單，無法比較快訊時間。')
            return
        newest = news_datetime(rows[0]['time'])
        age = max(0, (self.now() - newest).total_seconds())
        self.log('金十回傳第一則時間：' + newest.astimezone(TAIPEI).strftime('%m/%d %H:%M:%S') + '（台北）｜本次檢查約早於現在 ' + f'{age / 60:.1f}' + ' 分鐘。')
        if age > 3 * 60:
            self.log('第一則較早；可能是這段時間沒有新快訊，也可能是此接口提供資料較晚。請對照同時刻金十網站／App。')

    async def _deliver(self, plan, *, deadline=None):
        if plan.get('scope') != self.scope:
            raise BotError('收件人已變更，請重新預覽。')
        accepted = 0
        for key in plan['keys']:
            if self.stop_requested or (deadline is not None and self.monotonic() >= deadline):
                break
            record = self.get(key)
            if record is None:
                raise BotError('找不到傳送記錄，請重新預覽。')
            if record['status'] == 'accepted':
                continue
            stamp = self.now().timestamp()
            if record['first_attempt'] is not None and stamp - record['first_attempt'] >= 23 * 3600:
                raise BotError('有一則先前傳送結果待確認且已超過 23 小時；已停止，避免換新編號造成重送。請保留記錄並回報。')
            if record['status'] == 'queued' and stamp - record['news_time'] > self.max_age.total_seconds():
                self.log('略過一則已超過 15 分鐘的待傳快訊。')
                continue
            if record['last_attempt'] is not None and stamp - record['last_attempt'] < 5:
                raise BotError('距離前次嘗試太近，請等至少 5 秒再執行。')
            with self.db:
                self.db.execute("UPDATE outbox SET status='pending',\n                    first_attempt=COALESCE(first_attempt,?),last_attempt=?\n                    WHERE scope=? AND event_key=?", (stamp, stamp, self.scope, key))
            response = await self.push_message(self.state, record['text'], record['retry_key'])
            status = response.status_code
            confirmed = status == 200 or (status == 409 and bool(response.headers.get('x-line-accepted-request-id')))
            with self.db:
                self.db.execute('UPDATE outbox SET last_status=? WHERE scope=? AND event_key=?', (status, self.scope, key))
                if confirmed:
                    self.db.execute("UPDATE outbox SET status='accepted',accepted_at=? WHERE scope=? AND event_key=?", (stamp, self.scope, key))
            if not confirmed:
                guidance = {400: '請回報訊息格式錯誤。', 401: '請檢查 LINE Channel access token。', 403: '請檢查 LINE 權限。', 429: '請確認 LINE 額度／速率限制，停止連續重跑。'}.get(status, '請稍後確認服務狀態，再回第 3、4 步處理原訊息。')
                raise BotError(f'LINE HTTP {status}，本輪已停止。{guidance} 原傳送記錄已保留。')
            accepted += 1
            self.acceptance_count += 1
            self.log('LINE API 已接受 1 則。' if status == 200 else 'LINE 確認先前已接受這一則，沒有另發一則。')
        return accepted

    async def send(self, plan):
        if self.lock.locked():
            raise BotError('另一個傳送／追蹤正在執行，請先停止。')
        async with self.lock:
            self.stop_requested = False
            count = await self._deliver(plan)
            self.log(f'本次完成 {count} 則接受確認；實際收到請看手機 LINE。')
            return count

    def stop(self):
        self.stop_requested = True

from functools import lru_cache

from opencc import OpenCC

_TO_SIMPLIFIED = OpenCC('t2s')

DEFAULT_USER_FILTERS = {'version': 1, 'exclude': ['持倉報告', '主題'], 'include': [], 'conflict': 'exclude'}

def keyword_text(value):
    return normalize(_TO_SIMPLIFIED.convert(value))

@lru_cache(maxsize=1024)
def keyword_pattern(value):
    return alias_regex(keyword_text(value))

def validate_user_filters(value):
    if not isinstance(value, dict) or set(value) != {'version', 'exclude', 'include', 'conflict'}:
        raise BotError('設定檔格式不符，請使用「下載設定備份」產生的檔案。')
    if value['version'] != 1 or value['conflict'] not in ('exclude', 'include'):
        raise BotError('設定檔版本或優先順序不正確。')
    result = {'version': 1, 'conflict': value['conflict']}
    for name in ('exclude', 'include'):
        keywords = value[name]
        if not isinstance(keywords, list) or len(keywords) > 300:
            raise BotError('每欄最多 300 個關鍵字。')
        cleaned, seen = ([], set())
        for keyword in keywords:
            if not isinstance(keyword, str) or '\n' in keyword or '\r' in keyword:
                raise BotError('關鍵字請一行一個。')
            keyword = keyword.strip()
            if not keyword:
                continue
            if len(keyword) > 100:
                raise BotError('單個關鍵字／片語最多 100 字。')
            folded = keyword_text(keyword)
            if folded not in seen:
                seen.add(folded)
                cleaned.append(keyword)
        result[name] = cleaned
    return result

def classify_with_user_filters(text, rules=None):
    rules = USER_FILTERS if rules is None else validate_user_filters(rules)
    folded = keyword_text(text)
    excluded = [k for k in rules['exclude'] if keyword_pattern(k).search(folded)]
    included = [k for k in rules['include'] if keyword_pattern(k).search(folded)]
    base = classify(text)
    blocked = bool(excluded) or base['decision'] == '排除'
    force = bool(included) and (rules['conflict'] == 'include' or not blocked)
    if force:
        result = dict(base)
        result.update(decision='候選', topics=base['topics'] or ['自訂收錄'], reason='一定收錄命中：' + '、'.join(included), subject_hits=list(dict.fromkeys(base['subject_hits'] + included)))
        if base['decision'] == '排除':
            result['status_hint'] = '依自訂收錄優先；原本屬於排除類型'
        if blocked:
            result['reason'] += '；同時命中排除，依「收錄優先」處理'
    elif excluded:
        result = {'decision': '排除', 'topics': [], 'subject_hits': [], 'trigger_hits': [], 'reason': '額外排除命中：' + '、'.join(excluded), 'status_hint': '內容類型排除', 'content_type': '自訂文字排除'}
    else:
        result = dict(base)
    if blocked and included and (not force):
        result['reason'] += '；同時命中一定收錄，依「排除優先」處理'
    result['include_hits'] = included
    result['exclude_hits'] = excluded
    return result

class ProtocolError(Exception):
    pass

def xor_bytes(payload, key):
    if not key or not key.isascii():
        raise ProtocolError('握手格式不符。')
    seed = ord(key[0])
    return bytes((value ^ ord(key[(i + seed) % len(key)]) for i, value in enumerate(payload)))

def wire_string(value):
    raw = value.encode('utf-8')
    return struct.pack('<H', len(raw)) + raw

def guest_login(key):
    body = struct.pack('<hi', 4002, 0) + wire_string('') + wire_string('chrome') + struct.pack('<i', 0) + wire_string('web') + wire_string('')
    return xor_bytes(body, key)

def handshake_key(frame):
    if not isinstance(frame, bytes) or not 12 <= len(frame) <= 64:
        raise ProtocolError('首次握手不是預期格式；已停止。')
    _, second, third = struct.unpack_from('<III', frame)
    return f'{third}.{second}'

def decode_packet(frame, key):
    if not isinstance(frame, bytes) or len(frame) < 2:
        raise ProtocolError('收到無法辨識的封包。')
    data = xor_bytes(frame, key)
    code, = struct.unpack_from('<h', data)
    offset = 2

    def read_json():
        nonlocal offset
        if offset + 2 > len(data):
            raise ProtocolError('封包長度不足。')
        size, = struct.unpack_from('<H', data, offset)
        offset += 2
        if offset + size > len(data):
            raise ProtocolError('封包內容不完整。')
        raw = data[offset:offset + size]
        offset += size
        try:
            return json.loads(raw.decode('utf-8'))
        except (ValueError, UnicodeError):
            raise ProtocolError('封包 JSON 格式改變。') from None
    if code in JSON_CODES:
        return (code, read_json())
    if code == 1200:
        if len(data) < 6:
            raise ProtocolError('歷史列表長度不足。')
        count, = struct.unpack_from('<i', data, 2)
        offset = 6
        if not 0 <= count <= 2000:
            raise ProtocolError('歷史列表筆數異常。')
        return (code, [read_json() for _ in range(count)])
    return (code, None)

def published_time(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            number = float(value)
            if number >= 1000000000000.0:
                number /= 1000
            return datetime.fromtimestamp(number, timezone.utc)
        parsed = datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=TAIPEI)).astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None

from collections import OrderedDict, Counter

BOT_VERSION = '2.3-websocket'

WS_URL = 'wss://wss-flash-2.jin10.com/'

WS_NEWS_CODES = {1000, 1001, 1002, 1100, 1110}

class NewsRecordError(BotError):
    """An individual source record cannot be safely used as news."""

def flag(value):
    return value is True or value == 1 or value == '1'

def expand_ws_item(item, previous=None):
    if not isinstance(item, dict):
        raise NewsRecordError('WebSocket 快訊不是物件。')
    origin = item.get('_origin') if isinstance(item.get('_origin'), dict) else {}
    if origin.get('id') is not None and str(origin['id']) != str(item.get('id')):
        raise NewsRecordError('快訊原文與更新的 ID 不一致。')
    previous = previous or {}
    merged = {**origin, **previous, **item}
    merged.pop('_origin', None)
    parts = [part.get('data') for part in (origin, previous, item)]
    dictionaries = [part for part in parts if isinstance(part, dict)]
    if dictionaries and ('data' not in item or isinstance(item.get('data'), dict)):
        merged['data'] = {}
        for part in dictionaries:
            merged['data'].update(part)
    return merged

def ws_news_row(item, *, received=None, changed=False, recovered=False):
    """Normalize complete source records, then apply the user's existing rules."""
    if not isinstance(item, dict):
        raise NewsRecordError('WebSocket 快訊不是物件。')
    item = expand_ws_item(item)
    news_id = str(item.get('id') or '')
    stamp = published_time(item.get('time'))
    if not news_id.isdigit() or stamp is None:
        raise NewsRecordError('WebSocket 快訊缺少有效 ID／時間。')
    data = item.get('data')
    if not isinstance(data, dict):
        raise NewsRecordError('WebSocket 快訊缺少 data 物件。')
    extras = item.get('extras') if isinstance(item.get('extras'), dict) else {}
    if flag(extras.get('ad')) or flag(data.get('lock')):
        return None
    kind = item.get('type', 0)
    star = None
    if kind == 1:
        actual = data.get('actual')
        if actual is None or str(actual).strip() in ('', '--', '-', '待公布'):
            return None
        title = ''.join((str(data.get(k) or '') for k in ('country', 'time_period', 'name')))
        if not title:
            raise NewsRecordError('數據快訊缺少項目名稱。')
        unit = str(data.get('unit') or '')
        parts = [title, f'公布值：{actual}{unit}']
        for field, label in (('consensus', '預期'), ('previous', '前值'), ('revised', '修正前值')):
            if data.get(field) is not None:
                parts.append(f'{label}：{data[field]}{unit}')
        text = '\n'.join(parts)
        star = data.get('star')
    elif kind in (0, 2):
        title = text_only(data.get('title', '')) if isinstance(data.get('title'), str) else ''
        content = text_only(data.get('content', '')) if isinstance(data.get('content'), str) else ''
        text = content if title and title in content else '\n'.join((x for x in (title, content) if x))
        if not text:
            return None
    else:
        return None
    row = {'id': news_id, 'url': f'https://flash.jin10.com/detail/{news_id}', 'time': stamp.isoformat(), 'text': text, 'important': flag(item.get('important')) if item.get('important') is not None else None, 'star': star, 'changed': changed, 'recovered': recovered, 'received': received or datetime.now(timezone.utc)}
    row.update(classify_with_user_filters(text))
    return row

def format_ws_news(row):
    original = format_news(row)
    additions = []
    if row.get('important') is True:
        additions.append('🔴 金十標記：重要')
    elif row.get('important') is False:
        additions.append('金十標記：一般')
    if row.get('star') is not None:
        additions.append(f"數據星級：{row['star']}")
    if row.get('changed'):
        additions.append('消息更新：來源修改了此則內容')
    if row.get('recovered'):
        additions.append('重連後補回')
    return original.replace('【我的關注快訊】', '【我的關注快訊】' + ('\n' + '\n'.join(additions) if additions else ''), 1)

def ws_connect():
    from websockets.asyncio.client import connect
    return connect(WS_URL, origin='https://www.jin10.com', user_agent_header='Jin10PersonalNewsBot/2.3', ping_interval=None, open_timeout=10, close_timeout=2, max_size=2 * 1024 * 1024)

class WSNewsBot(NewsBot):

    def __init__(self, state, push_message, db_path, *, connector=ws_connect, **kwargs):
        super().__init__(state, None, push_message, db_path, **kwargs)
        self.connector = connector
        self._clear_stream()

    def _clear_stream(self):
        self.cache = OrderedDict()
        self.seen_versions = set()
        self.backlog = OrderedDict()
        self.changed_event = asyncio.Event()
        self.initialized = False
        self.stats = Counter()
        self.currently_sending = None
        self.invalid_samples = []

    def reject_news(self, item, error, *, packet_code=None):
        record = item if isinstance(item, dict) else {}
        news_id = str(record.get('id') or '')
        if news_id:
            self.backlog.pop(news_id, None)
        self.stats['invalid_records'] += 1
        if record.get('action') == 2:
            self.stats['incomplete_edit'] += 1
        sample = {'code': packet_code, 'reason': str(error), 'shape': type(item).__name__, 'fields': list(record)[:15], 'id': str(record.get('id', ''))[:80], 'time': str(record.get('time', ''))[:80]}
        self.invalid_samples.append(sample)
        self.invalid_samples = self.invalid_samples[-5:]
        count = self.stats['invalid_records']
        if count <= 3 or count % 50 == 0:
            self.log('略過一筆無法辨識的資料，繼續接收；累計 ' + str(count) + ' 筆。診斷：' + json.dumps(sample, ensure_ascii=False))

    def accept_news(self, item, *, baseline=False, recovered=False, packet_code=None):
        try:
            self.handle_news(item, baseline=baseline, recovered=recovered)
            return True
        except NewsRecordError as exc:
            self.reject_news(item, exc, packet_code=packet_code)
            return False

    def queue(self, row):
        key = event_key(row)
        message = self.redactor(format_ws_news(row))
        if utf16_length(message) > 5000:
            raise BotError('LINE 訊息過長，未排入。')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO outbox\n                (scope,event_key,text,news_time,retry_key) VALUES (?,?,?,?,?)', (self.scope, key, message, news_datetime(row['time']).timestamp(), str(uuid4())))
        return key

    def cache_item(self, item):
        news_id = str(item.get('id') or '')
        previous = self.cache.get(news_id, {})
        merged = expand_ws_item(item, previous)
        self.cache[news_id] = merged
        self.cache.move_to_end(news_id)
        if len(self.cache) > 5000:
            self.cache.popitem(last=False)
        return merged

    def handle_news(self, item, *, baseline=False, recovered=False):
        if not isinstance(item, dict):
            raise NewsRecordError('快訊封包不是新聞物件。')
        news_id = str(item.get('id') or '')
        action = item.get('action', 1)
        if action not in (1, 2, 3):
            self.stats['control'] += 1
            return
        if not news_id.isdigit():
            raise NewsRecordError('WebSocket 快訊缺少有效 ID／時間。')
        if action == 3:
            self.stats['deleted'] += 1
            self.backlog.pop(news_id, None)
            self.cache.pop(news_id, None)
            self.log('金十撤回快訊：' + news_id + '；已移除尚未發送的項目。')
            if self.currently_sending == news_id:
                self.log('這則正在傳送中，撤回可能已無法阻止 LINE 接受。')
            return
        if action == 2:
            self.stats['edited'] += 1
        merged = expand_ws_item(item, self.cache.get(news_id))
        row = ws_news_row(merged, received=self.now(), changed=action == 2, recovered=recovered)
        self.cache_item(item)
        if row is None:
            self.backlog.pop(news_id, None)
            self.stats['unsupported'] += 1
            return
        key = event_key(row)
        if baseline:
            self.seen_versions.add(key)
            self.stats['baseline'] += 1
            return
        queued = self.backlog.get(news_id)
        if queued is not None and event_key(queued) != key:
            self.backlog.pop(news_id, None)
        elif queued is not None:
            queued['important'] = row['important']
            queued['star'] = row['star']
        self.stats['received'] += 1
        if key in self.seen_versions:
            self.stats['duplicate'] += 1
            return
        self.seen_versions.add(key)
        if len(self.seen_versions) > 100000:
            raise BotError('本次接收量已達記憶體上限，請重新啟動追蹤。')
        self.stats[row['decision']] += 1
        if not self.eligible(row):
            if row['decision'] == '候選':
                self.stats['stale'] += 1
            return
        accepted = self.get(key)
        if accepted and accepted['status'] == 'accepted':
            self.stats['duplicate'] += 1
            return
        self.backlog[news_id] = row
        if len(self.backlog) > 200:
            raise BotError('LINE 待送佇列超過 200 則，已停止，請檢查傳送速度。')
        if recovered:
            self.stats['recovered'] += 1
        lag = (row['received'] - news_datetime(row['time'])).total_seconds()
        self.log(f"候選快訊｜收到比發布晚 {lag:.1f} 秒｜{row['text'][:100]}")
        self.changed_event.set()

    def handle_snapshot(self, items):
        if not isinstance(items, list):
            raise BotError('歷史快照格式改變，已停止。')
        is_first = not self.initialized
        previous_ids = set(self.cache)
        snapshot_ids = {str(x.get('id')) for x in items if isinstance(x, dict)}
        if not is_first and previous_ids and items and (not previous_ids.intersection(snapshot_ids)):
            self.log('提醒：重連快照與中斷前資料沒有重疊，斷線期間可能漏訊；本次只能補回快照包含的資料。')
            self.stats['possible_gap'] += 1
        valid = sorted(items, key=lambda x: published_time(x.get('time')) or datetime.min.replace(tzinfo=timezone.utc) if isinstance(x, dict) else datetime.min.replace(tzinfo=timezone.utc))
        for item in valid:
            self.accept_news(item, baseline=is_first, recovered=not is_first, packet_code=1200)
        self.initialized = True
        self.log(f'收到 {len(items)} 則快照：' + ('已建立起點，等待後續新快訊。' if is_first else '已核對斷線期間的新增快訊。'))

    async def _session(self, ws, deadline, *, snapshot_only=False):
        key = handshake_key(await asyncio.wait_for(ws.recv(), min(10, max(0.01, deadline - self.monotonic()))))
        await ws.send(guest_login(key))
        login_ok = False
        snapshot_ok = False
        setup_deadline = min(deadline, self.monotonic() + 20)
        while not self.stop_requested and self.monotonic() < deadline:
            timeout = min(45, deadline - self.monotonic())
            if not login_ok or not snapshot_ok:
                timeout = min(timeout, setup_deadline - self.monotonic())
            if timeout <= 0:
                raise BotError('WebSocket 未完成訪客確認／起始快照，已停止。')
            try:
                frame = await asyncio.wait_for(ws.recv(), timeout)
            except TimeoutError:
                if self.monotonic() >= deadline:
                    return None
                if not login_ok or not snapshot_ok:
                    raise BotError('WebSocket 未完成訪客確認／起始快照，已停止。') from None
                raise
            code, item = decode_packet(frame, key)
            self.stats['frames'] += 1
            if code == 1201:
                await ws.send(b'')
            elif code == 4002:
                if not isinstance(item, dict) or item.get('status') not in (1, 100, 101):
                    raise BotError('WebSocket 訪客狀態未確認；請向客服核對接入方式。')
                login_ok = True
                self.log('WebSocket 已連線，訪客接收已接受。')
            elif code == 1200:
                if not login_ok:
                    raise BotError('尚未取得訪客接收確認。')
                snapshot_ok = True
                if snapshot_only:
                    return item
                self.handle_snapshot(item)
            elif code in WS_NEWS_CODES:
                if not login_ok or not snapshot_ok:
                    raise BotError('快訊出現在起始確認前，需重新核對協定。')
                if not snapshot_only:
                    self.accept_news(item, packet_code=code)
            else:
                self.stats['control_frames'] += 1
        return None

    async def prepare(self):
        if self.lock.locked():
            raise BotError('追蹤仍在執行，請先按停止。')
        async with self.lock:
            self.stop_requested = False
            if self.pending():
                plan = self.make_plan([])
                self.show_plan(plan)
                return (None, {'rows': []}, plan)
            deadline = self.monotonic() + 30
            async with self.connector() as ws:
                items = await self._session(ws, deadline, snapshot_only=True)
            if items is None:
                raise BotError('未收到 WebSocket 快照，請回報畫面上的狀態。')
            rows = []
            for item in items:
                if isinstance(item, dict) and item.get('action', 1) == 3:
                    continue
                try:
                    row = ws_news_row(item, received=self.now())
                except NewsRecordError as exc:
                    self.reject_news(item, exc, packet_code=1200)
                    continue
                if row is not None:
                    rows.append(row)
            rows.sort(key=lambda r: news_datetime(r['time']), reverse=True)
            counts = Counter((r['decision'] for r in rows))
            self.log('WebSocket 預覽：' + '、'.join((f'{k} {v}' for k, v in counts.items())))
            self.show_feed_time(rows)
            plan = self.make_plan(rows)
            self.show_plan(plan)
            return (items, {'rows': rows, 'counts': dict(counts)}, plan)

    async def _receive_loop(self, deadline):
        from websockets.exceptions import ConnectionClosed, InvalidStatus
        failures = 0
        while not self.stop_requested and self.monotonic() < deadline:
            opened = self.monotonic()
            try:
                async with self.connector() as ws:
                    self.stats['connections'] += 1
                    await self._session(ws, deadline)
                return
            except (ProtocolError, BotError):
                raise
            except InvalidStatus as exc:
                status = exc.response.status_code
                if status < 500:
                    raise BotError(f'WebSocket HTTP {status}；已停止，請核對權限或服務限制。') from None
            except ConnectionClosed as exc:
                if getattr(getattr(exc, 'rcvd', None), 'code', None) == 1008:
                    raise BotError('WebSocket 因存取政策中斷（1008），已停止。') from None
            except (OSError, TimeoutError):
                pass
            if self.monotonic() - opened > 60:
                failures = 0
            failures += 1
            if failures >= 5:
                raise BotError('WebSocket 連續 5 次中斷；已停止，請稍後檢查連線。')
            delay = min(5 * 2 ** (failures - 1), 60, max(0, deadline - self.monotonic()))
            if delay:
                self.stats['reconnects'] += 1
                self.log(f'WebSocket 中斷，{delay:.0f} 秒後重連。')
                await self.sleep(delay)

    async def _send_loop(self, deadline, max_messages, send_to_line):
        while not self.stop_requested and self.monotonic() < deadline:
            if not self.backlog:
                self.changed_event.clear()
                try:
                    await asyncio.wait_for(self.changed_event.wait(), min(30, max(0.01, deadline - self.monotonic())))
                except TimeoutError:
                    self.log(f"持續接收中｜本次新增／更新 {self.stats['received']} 則｜候選 {self.stats['候選']} 則｜LINE 接受 {self.stats['accepted']} 則｜格式略過 {self.stats['invalid_records']} 筆")
                continue
            news_id, row = self.backlog.popitem(last=False)
            if not self.eligible(row):
                self.stats['stale'] += 1
                continue
            if not send_to_line:
                self.log('【預覽模式】\n' + format_ws_news(row))
                self.stats['previewed'] += 1
                continue
            plan = self.make_plan([row], max_messages=1, include_pending=False)
            if not plan['keys']:
                continue
            self.currently_sending = news_id
            try:
                self.stats['accepted'] += await self._deliver(plan, deadline=deadline)
            finally:
                self.currently_sending = None
            if self.stats['accepted'] >= max_messages:
                self.log('已到這次 LINE 傳送上限。')
                return

    async def monitor(self, *, minutes=5, max_messages=10, send_to_line=True):
        if self.lock.locked():
            raise BotError('已有追蹤／傳送正在執行。')
        if not isinstance(minutes, int) or not 1 <= minutes <= 120:
            raise BotError('追蹤時間請填 1～120 分鐘。')
        if not isinstance(max_messages, int) or not 1 <= max_messages <= 100:
            raise BotError('LINE 上限請填 1～100 則。')
        if self.pending() and send_to_line:
            raise BotError('有前次傳送結果待確認。請先執行第 3、4 步處理。')
        async with self.lock:
            self.stop_requested = False
            self._clear_stream()
            deadline = self.monotonic() + minutes * 60
            self.log(f'開始持續接收，最長 {minutes} 分鐘；' + (f'最多傳送 {max_messages} 則 LINE。' if send_to_line else '目前為預覽模式。'))
            tasks = [asyncio.create_task(self._receive_loop(deadline)), asyncio.create_task(self._send_loop(deadline, max_messages, send_to_line))]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.last_stats = dict(self.stats)
                self.log(f"追蹤已停止｜新增／更新 {self.stats['received']} 則｜候選 {self.stats['候選']} 則｜排除 {self.stats['排除']} 則｜LINE 接受 {self.stats['accepted']} 則。")
                if self.backlog:
                    self.log(f'尚有 {len(self.backlog)} 則未送；下次啟動不會補發這批等待項目。')
                if self.pending():
                    self.log('有 LINE 傳送結果待確認，請回第 3、4 步沿用原訊息處理。')

USER_FILTERS = validate_user_filters(DEFAULT_USER_FILTERS)
