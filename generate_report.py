'''
巴菲特/蒙格每日選股報告產生器
產出 JSON 格式報告至 docs/data/ 目錄（GitHub Pages）
包含：財報篩選 + 即時新聞風險評估
'''
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import warnings
from datetime import datetime

import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings('ignore')

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(PROJECT_DIR, 'docs', 'data')
FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_TOKEN = os.environ.get('FINMIND_TOKEN', '')

# ============================================================
# 候選清單
# ============================================================
TW_CANDIDATES = {
    '2330': '台積電',
    '2317': '鴻海',
    '2454': '聯發科',
    '2412': '中華電',
    '2882': '國泰金',
    '2881': '富邦金',
    '1301': '台塑',
    '1303': '南亞',
    '2308': '台達電',
    '2886': '兆豐金',
    '2891': '中信金',
    '3711': '日月光投控',
    '2884': '玉山金',
    '5880': '合庫金',
    '2357': '華碩',
    '1216': '統一',
    '2207': '和泰車',
    '9910': '豐泰',
    '2327': '國巨',
    '8454': '富邦媒',
}

US_CANDIDATES = [
    'AAPL', 'MSFT', 'GOOGL', 'BRK-B', 'JNJ',
    'KO', 'PG', 'V', 'MA', 'UNH',
    'COST', 'WMT', 'MCD', 'AXP', 'HD',
    'AVGO', 'ABBV', 'PEP', 'LLY', 'AMZN',
]

# 護城河資料（質化，手動維護）
MOAT_DATA = {
    'MSFT': {
        'moat_type': '轉換成本 + 網路效應',
        'moat_desc': 'Office/Azure 生態系深度綁定企業客戶；Teams/LinkedIn 網路效應',
        'moat_width': 'wide',
    },
    'GOOGL': {
        'moat_type': '網路效應 + 數據優勢',
        'moat_desc': '搜尋引擎全球 90%+ 市佔；YouTube/Android 數據飛輪',
        'moat_width': 'wide',
    },
    'V': {
        'moat_type': '網路效應 + 規模壁壘',
        'moat_desc': '全球最大支付網路，商家/消費者雙邊網路效應',
        'moat_width': 'wide',
    },
    'JNJ': {
        'moat_type': '品牌 + 專利/法規',
        'moat_desc': '百年醫療品牌；製藥專利保護和 FDA 審批壁壘',
        'moat_width': 'wide',
    },
    'AVGO': {
        'moat_type': '技術優勢 + 轉換成本',
        'moat_desc': 'AI 客製晶片技術領先；深度整合客戶設計流程',
        'moat_width': 'narrow',
    },
    'KO': {
        'moat_type': '品牌 + 分銷網路',
        'moat_desc': '全球最強消費品牌；200+ 國家分銷網路',
        'moat_width': 'wide',
    },
    'PG': {
        'moat_type': '品牌 + 規模經濟',
        'moat_desc': '多品牌矩陣覆蓋日用品全品類；規模帶來的成本優勢',
        'moat_width': 'wide',
    },
    'AAPL': {
        'moat_type': '轉換成本 + 品牌',
        'moat_desc': '封閉生態系鎖定用戶；全球最有價值消費品牌',
        'moat_width': 'wide',
    },
    'MA': {
        'moat_type': '網路效應 + 規模壁壘',
        'moat_desc': '全球第二大支付網路；與 Visa 形成雙寡頭壟斷',
        'moat_width': 'wide',
    },
    '2330': {
        'moat_type': '技術優勢 + 規模經濟',
        'moat_desc': '先進製程全球獨霸；$300B+ 年資本支出形成進入門檻',
        'moat_width': 'wide',
    },
    '2412': {
        'moat_type': '法規壁壘 + 規模',
        'moat_desc': '電信業牌照壁壘；固網基礎設施壟斷',
        'moat_width': 'narrow',
    },
    '2454': {
        'moat_type': '技術優勢',
        'moat_desc': 'IC 設計龍頭；5G/AI 晶片技術領先',
        'moat_width': 'narrow',
    },
}

# 出場條件（質化，手動維護）
EXIT_CONDITIONS = {
    'MSFT': 'Azure 市佔大幅下降；淨利率跌破 25%；AI 競爭失利',
    'GOOGL': 'AI 搜尋搶走 >20% 流量；反壟斷導致業務拆分',
    'V': 'CBDC 大規模取代卡片支付；淨利率跌破 40%',
    'JNJ': '重要藥品專利到期且無新藥填補；訴訟風險失控',
    'AVGO': 'AI 資本支出週期結束；毛利率跌破 50%',
    'KO': '含糖飲料需求持續萎縮；毛利率跌破 50%',
    'PG': '自有品牌大幅搶佔市佔；淨利率跌破 12%',
    'AAPL': 'iPhone 市佔跌破 15%；生態系鎖定效應減弱',
    'MA': '與 V 相同的支付顛覆風險；D/E 持續惡化',
    '2330': 'Intel/三星追上先進製程；台海地緣風險急劇升高',
    '2412': '5G 競爭加劇導致 ARPU 持續下滑',
    '2454': '高通/三星搶走市佔；手機晶片 ASP 下滑',
}


# ============================================================
# 新聞拉取（Google News RSS）
# ============================================================
def fetch_news(query, max_results=5):
    '''透過 Google News RSS 拉取即時新聞標題'''
    try:
        encoded_q = requests.utils.quote(query)
        url = (
            f'https://news.google.com/rss/search?q={encoded_q}'
            f'&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'
        )
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return []

        # 簡易 XML 解析（避免額外依賴）
        items = []
        titles = re.findall(r'<title>(.*?)</title>', resp.text)
        pub_dates = re.findall(r'<pubDate>(.*?)</pubDate>', resp.text)
        links = re.findall(r'<link/>(.*?)</', resp.text)

        # 跳過第一個 title（feed 本身的標題）
        for i, title in enumerate(titles[2 : 2 + max_results]):
            item = {'title': title.strip()}
            if i < len(pub_dates):
                item['date'] = pub_dates[i].strip()
            if i < len(links):
                item['url'] = links[i].strip()
            items.append(item)

        return items
    except Exception as e:
        print(f'  [WARN] News fetch error for "{query}": {e}')
        return []


def assess_news_risk(news_items, stock_id):
    '''根據新聞標題關鍵字進行簡易風險評估'''
    if not news_items:
        return 'unknown', '無法取得新聞資料'

    red_keywords = [
        'lawsuit', 'sue', '訴訟', 'fraud', '詐欺', 'scandal', '醜聞',
        'crash', '崩盤', 'bankruptcy', '破產', 'recall', '召回',
        'investigation', '調查', 'ban', '禁止', 'sanction', '制裁',
        'war', '戰爭', 'downgrade', '降評',
    ]
    yellow_keywords = [
        'decline', '下滑', 'miss', '未達', 'cut', '裁員',
        'tariff', '關稅', 'antitrust', '反壟斷', 'delay', '延遲',
        'concern', '擔憂', 'risk', '風險', 'slow', '放緩',
        'layoff', 'restructur', '重組',
    ]
    green_keywords = [
        'beat', '超越', 'record', '新高', 'upgrade', '升評',
        'growth', '成長', 'profit', '獲利', 'dividend', '股息',
        'innovation', '創新', 'expand', '擴張',
    ]

    red_count = 0
    yellow_count = 0
    green_count = 0
    all_titles = ' '.join(
        item.get('title', '').lower() for item in news_items
    )

    for kw in red_keywords:
        if kw.lower() in all_titles:
            red_count += 1
    for kw in yellow_keywords:
        if kw.lower() in all_titles:
            yellow_count += 1
    for kw in green_keywords:
        if kw.lower() in all_titles:
            green_count += 1

    if red_count >= 2:
        level = 'red'
        summary = f'偵測到 {red_count} 個重大風險信號'
    elif red_count >= 1 or yellow_count >= 3:
        level = 'yellow'
        summary = f'偵測到 {red_count} 紅色 + {yellow_count} 黃色信號，建議關注'
    elif yellow_count >= 1:
        level = 'yellow'
        summary = f'偵測到 {yellow_count} 個需關注信號'
    elif green_count >= 2:
        level = 'green'
        summary = f'近期新聞正面 ({green_count} 個正面信號)'
    else:
        level = 'green'
        summary = '近期無重大負面新聞'

    return level, summary


# ============================================================
# 台股財報
# ============================================================
def fetch_tw_data(dataset, stock_id, start_date='2015-01-01'):
    params = {
        'dataset': dataset,
        'data_id': stock_id,
        'start_date': start_date,
    }
    if FINMIND_TOKEN:
        params['token'] = FINMIND_TOKEN
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=30)
        data = resp.json()
        if data.get('status') == 200 and data.get('data'):
            return pd.DataFrame(data['data'])
    except Exception as e:
        print(f'  [WARN] {stock_id} {dataset}: {e}')
    return pd.DataFrame()


def pivot_tw(df):
    if df.empty:
        return pd.DataFrame()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    pivoted = df.pivot_table(
        index='date', columns='type', values='value', aggfunc='first'
    )
    pivoted.index = pd.to_datetime(pivoted.index)
    return pivoted.sort_index()


def _find_col(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def analyze_tw_stock(stock_id, name):
    print(f'  [TW] {stock_id} {name}')
    income = pivot_tw(
        fetch_tw_data('TaiwanStockFinancialStatements', stock_id)
    )
    time.sleep(1)
    balance = pivot_tw(fetch_tw_data('TaiwanStockBalanceSheet', stock_id))
    time.sleep(1)
    cashflow = pivot_tw(
        fetch_tw_data('TaiwanStockCashFlowsStatement', stock_id)
    )
    time.sleep(1)

    r = {
        'stock_id': stock_id,
        'name': name,
        'market': 'TW',
        'currency': 'TWD',
    }

    # ROE
    if not income.empty and not balance.empty:
        ni_col = _find_col(
            income, ['NetIncome', 'ProfitLossAttributableToOwnersOfParent']
        )
        eq_col = _find_col(
            balance, ['Equity', 'EquityAttributableToOwnersOfParent']
        )
        if ni_col and eq_col:
            try:
                a_ni = income[ni_col].resample('YE').sum()
                a_eq = balance[eq_col].resample('YE').last()
                common = a_ni.index.intersection(a_eq.index)
                if len(common) >= 3:
                    roe = (a_ni[common] / a_eq[common] * 100).dropna()
                    r['roe_avg'] = round(roe.mean(), 2)
                    r['roe_latest'] = round(roe.iloc[-1], 2)
                    r['roe_years'] = len(roe)
            except Exception:
                pass

    # Net Margin
    if not income.empty:
        rev_col = _find_col(income, ['Revenue', 'TotalOperatingRevenue'])
        ni_col = _find_col(
            income, ['NetIncome', 'ProfitLossAttributableToOwnersOfParent']
        )
        if rev_col and ni_col:
            try:
                a_rev = income[rev_col].resample('YE').sum()
                a_ni = income[ni_col].resample('YE').sum()
                common = a_rev.index.intersection(a_ni.index)
                if len(common) >= 3:
                    margin = (a_ni[common] / a_rev[common] * 100).dropna()
                    r['net_margin_avg'] = round(margin.mean(), 2)
                    r['net_margin_latest'] = round(margin.iloc[-1], 2)
            except Exception:
                pass

    # Gross Margin
    if not income.empty and 'GrossProfit' in income.columns:
        rev_col = _find_col(income, ['Revenue', 'TotalOperatingRevenue'])
        if rev_col:
            try:
                a_gp = income['GrossProfit'].resample('YE').sum()
                a_rev = income[rev_col].resample('YE').sum()
                common = a_gp.index.intersection(a_rev.index)
                if len(common) >= 3:
                    gm = (a_gp[common] / a_rev[common] * 100).dropna()
                    r['gross_margin_avg'] = round(gm.mean(), 2)
            except Exception:
                pass

    # EPS
    if not income.empty and 'EPS' in income.columns:
        try:
            a_eps = income['EPS'].resample('YE').sum().dropna()
            if len(a_eps) >= 5:
                r['eps_positive_years'] = int((a_eps > 0).sum())
                r['eps_total_years'] = len(a_eps)
                r['eps_latest'] = round(a_eps.iloc[-1], 2)
                r['eps_5yr_ago'] = round(a_eps.iloc[-5], 2)
                if a_eps.iloc[-5] > 0:
                    r['eps_cagr_5y'] = round(
                        ((a_eps.iloc[-1] / a_eps.iloc[-5]) ** 0.2 - 1) * 100,
                        2,
                    )
        except Exception:
            pass

    # D/E
    if not balance.empty:
        liab_col = _find_col(
            balance, ['TotalLiabilities', 'Liabilities']
        )
        eq_col = _find_col(
            balance, ['Equity', 'EquityAttributableToOwnersOfParent']
        )
        if liab_col and eq_col:
            try:
                l_val = balance[liab_col].dropna().iloc[-1]
                e_val = balance[eq_col].dropna().iloc[-1]
                if e_val > 0:
                    r['debt_to_equity'] = round(l_val / e_val, 2)
            except Exception:
                pass

    # OCF
    if not cashflow.empty:
        ocf_col = _find_col(
            cashflow,
            ['CashFlowsFromOperatingActivities', 'CashGeneratedFromOperations'],
        )
        if ocf_col:
            try:
                a_ocf = cashflow[ocf_col].resample('YE').sum().dropna()
                if len(a_ocf) >= 3:
                    r['ocf_positive_years'] = int((a_ocf > 0).sum())
                    r['ocf_total_years'] = len(a_ocf)
            except Exception:
                pass

    return r


# ============================================================
# 美股財報
# ============================================================
def analyze_us_stock(ticker_str):
    print(f'  [US] {ticker_str}')
    try:
        t = yf.Ticker(ticker_str)
        info = t.info or {}
        income = t.income_stmt
        balance = t.balance_sheet
        cashflow = t.cashflow

        r = {
            'stock_id': ticker_str,
            'name': info.get('shortName', ticker_str),
            'market': 'US',
            'currency': 'USD',
        }

        # ROE
        if income is not None and not income.empty:
            if balance is not None and not balance.empty:
                ni_row = next(
                    (
                        x
                        for x in [
                            'Net Income',
                            'Net Income Common Stockholders',
                        ]
                        if x in income.index
                    ),
                    None,
                )
                eq_row = next(
                    (
                        x
                        for x in [
                            'Stockholders Equity',
                            'Total Stockholder Equity',
                            'Common Stock Equity',
                        ]
                        if x in balance.index
                    ),
                    None,
                )
                if ni_row and eq_row:
                    try:
                        ni = income.loc[ni_row].dropna().sort_index()
                        eq = balance.loc[eq_row].dropna().sort_index()
                        common = ni.index.intersection(eq.index)
                        if len(common) >= 2:
                            roe = ni[common] / eq[common] * 100
                            r['roe_avg'] = round(roe.mean(), 2)
                            r['roe_latest'] = round(
                                roe.sort_index().iloc[-1], 2
                            )
                            r['roe_years'] = len(roe)
                    except Exception:
                        pass

        # Net Margin
        if income is not None and not income.empty:
            rev_row = next(
                (
                    x
                    for x in ['Total Revenue', 'Operating Revenue']
                    if x in income.index
                ),
                None,
            )
            ni_row = next(
                (
                    x
                    for x in ['Net Income', 'Net Income Common Stockholders']
                    if x in income.index
                ),
                None,
            )
            if rev_row and ni_row:
                try:
                    rev = income.loc[rev_row].dropna().sort_index()
                    ni = income.loc[ni_row].dropna().sort_index()
                    common = rev.index.intersection(ni.index)
                    if len(common) >= 2:
                        margin = ni[common] / rev[common] * 100
                        r['net_margin_avg'] = round(margin.mean(), 2)
                        r['net_margin_latest'] = round(
                            margin.sort_index().iloc[-1], 2
                        )
                except Exception:
                    pass

        # Gross Margin
        if income is not None and not income.empty:
            if 'Gross Profit' in income.index:
                rev_row = next(
                    (
                        x
                        for x in ['Total Revenue', 'Operating Revenue']
                        if x in income.index
                    ),
                    None,
                )
                if rev_row:
                    try:
                        gp = (
                            income.loc['Gross Profit'].dropna().sort_index()
                        )
                        rev = income.loc[rev_row].dropna().sort_index()
                        common = gp.index.intersection(rev.index)
                        if len(common) >= 2:
                            gm = gp[common] / rev[common] * 100
                            r['gross_margin_avg'] = round(gm.mean(), 2)
                    except Exception:
                        pass

        # Info-based metrics
        if info.get('trailingEps'):
            r['eps_latest'] = round(info['trailingEps'], 2)
        if info.get('debtToEquity') is not None:
            r['debt_to_equity'] = round(info['debtToEquity'] / 100, 2)
        if info.get('currentRatio'):
            r['current_ratio'] = round(info['currentRatio'], 2)
        if info.get('trailingPE'):
            r['pe_ratio'] = round(info['trailingPE'], 2)
        if info.get('forwardPE'):
            r['forward_pe'] = round(info['forwardPE'], 2)
        if info.get('marketCap'):
            r['market_cap_b'] = round(info['marketCap'] / 1e9, 1)
        if info.get('dividendYield'):
            r['dividend_yield'] = round(info['dividendYield'] * 100, 2)
        if info.get('fiftyTwoWeekHigh'):
            r['high_52w'] = round(info['fiftyTwoWeekHigh'], 2)
        if info.get('fiftyTwoWeekLow'):
            r['low_52w'] = round(info['fiftyTwoWeekLow'], 2)
        if info.get('currentPrice'):
            r['current_price'] = round(info['currentPrice'], 2)

        # FCF
        if cashflow is not None and not cashflow.empty:
            try:
                if 'Free Cash Flow' in cashflow.index:
                    fcf = cashflow.loc['Free Cash Flow'].dropna().sort_index()
                    r['fcf_positive_years'] = int((fcf > 0).sum())
                    r['fcf_total_years'] = len(fcf)
                    r['fcf_latest_b'] = round(
                        float(fcf.iloc[-1]) / 1e9, 2
                    )
                else:
                    ocf_row = next(
                        (
                            x
                            for x in [
                                'Operating Cash Flow',
                                'Cash Flow From Continuing Operating Activities',
                            ]
                            if x in cashflow.index
                        ),
                        None,
                    )
                    capex_row = next(
                        (
                            x
                            for x in [
                                'Capital Expenditure',
                                'Capital Expenditures',
                            ]
                            if x in cashflow.index
                        ),
                        None,
                    )
                    if ocf_row and capex_row:
                        ocf = (
                            cashflow.loc[ocf_row].dropna().sort_index()
                        )
                        capex = (
                            cashflow.loc[capex_row].dropna().sort_index()
                        )
                        common = ocf.index.intersection(capex.index)
                        fcf = ocf[common] + capex[common]
                        r['fcf_positive_years'] = int((fcf > 0).sum())
                        r['fcf_total_years'] = len(fcf)
                        r['fcf_latest_b'] = round(
                            float(fcf.iloc[-1]) / 1e9, 2
                        )
            except Exception:
                pass

        return r
    except Exception as e:
        print(f'  [WARN] {ticker_str}: {e}')
        return {
            'stock_id': ticker_str,
            'name': ticker_str,
            'market': 'US',
            'currency': 'USD',
        }


# ============================================================
# 巴菲特評分
# ============================================================
def buffett_score(stock):
    score = 0
    max_score = 0
    details = []

    # 1. ROE (20)
    max_score += 20
    roe = stock.get('roe_avg')
    if roe is not None:
        if roe >= 20:
            score += 20
            details.append({'metric': 'ROE', 'value': roe, 'grade': 'pass'})
        elif roe >= 15:
            score += 15
            details.append({'metric': 'ROE', 'value': roe, 'grade': 'ok'})
        elif roe >= 10:
            score += 8
            details.append({'metric': 'ROE', 'value': roe, 'grade': 'warn'})
        else:
            details.append({'metric': 'ROE', 'value': roe, 'grade': 'fail'})
    else:
        details.append({'metric': 'ROE', 'value': None, 'grade': 'nodata'})

    # 2. Net Margin (15)
    max_score += 15
    nm = stock.get('net_margin_avg')
    if nm is not None:
        if nm >= 20:
            score += 15
            details.append(
                {'metric': '淨利率', 'value': nm, 'grade': 'pass'}
            )
        elif nm >= 10:
            score += 10
            details.append(
                {'metric': '淨利率', 'value': nm, 'grade': 'ok'}
            )
        elif nm >= 5:
            score += 5
            details.append(
                {'metric': '淨利率', 'value': nm, 'grade': 'warn'}
            )
        else:
            details.append(
                {'metric': '淨利率', 'value': nm, 'grade': 'fail'}
            )
    else:
        details.append(
            {'metric': '淨利率', 'value': None, 'grade': 'nodata'}
        )

    # 3. Gross Margin (10)
    max_score += 10
    gm = stock.get('gross_margin_avg')
    if gm is not None:
        if gm >= 40:
            score += 10
            details.append(
                {'metric': '毛利率', 'value': gm, 'grade': 'pass'}
            )
        elif gm >= 30:
            score += 7
            details.append(
                {'metric': '毛利率', 'value': gm, 'grade': 'ok'}
            )
        elif gm >= 20:
            score += 4
            details.append(
                {'metric': '毛利率', 'value': gm, 'grade': 'warn'}
            )
        else:
            details.append(
                {'metric': '毛利率', 'value': gm, 'grade': 'fail'}
            )
    else:
        details.append(
            {'metric': '毛利率', 'value': None, 'grade': 'nodata'}
        )

    # 4. D/E (15)
    max_score += 15
    de = stock.get('debt_to_equity')
    if de is not None:
        if de <= 0.5:
            score += 15
            details.append(
                {'metric': '負債權益比', 'value': de, 'grade': 'pass'}
            )
        elif de <= 1.0:
            score += 10
            details.append(
                {'metric': '負債權益比', 'value': de, 'grade': 'ok'}
            )
        elif de <= 1.5:
            score += 5
            details.append(
                {'metric': '負債權益比', 'value': de, 'grade': 'warn'}
            )
        else:
            details.append(
                {'metric': '負債權益比', 'value': de, 'grade': 'fail'}
            )
    else:
        details.append(
            {'metric': '負債權益比', 'value': None, 'grade': 'nodata'}
        )

    # 5. EPS/FCF consistency (15)
    max_score += 15
    eps_pos = stock.get('eps_positive_years')
    eps_tot = stock.get('eps_total_years')
    fcf_pos = stock.get('fcf_positive_years')
    fcf_tot = stock.get('fcf_total_years')

    if eps_pos is not None and eps_tot and eps_tot > 0:
        ratio = eps_pos / eps_tot
        if ratio >= 0.9:
            score += 15
            details.append(
                {
                    'metric': 'EPS 持續性',
                    'value': f'{eps_pos}/{eps_tot}',
                    'grade': 'pass',
                }
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                {
                    'metric': 'EPS 持續性',
                    'value': f'{eps_pos}/{eps_tot}',
                    'grade': 'ok',
                }
            )
        else:
            score += 5
            details.append(
                {
                    'metric': 'EPS 持續性',
                    'value': f'{eps_pos}/{eps_tot}',
                    'grade': 'warn',
                }
            )
    elif fcf_pos is not None and fcf_tot and fcf_tot > 0:
        ratio = fcf_pos / fcf_tot
        if ratio >= 0.9:
            score += 15
            details.append(
                {
                    'metric': 'FCF 持續性',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'pass',
                }
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                {
                    'metric': 'FCF 持續性',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'ok',
                }
            )
        else:
            score += 5
            details.append(
                {
                    'metric': 'FCF 持續性',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'warn',
                }
            )
    else:
        details.append(
            {'metric': 'EPS/FCF', 'value': None, 'grade': 'nodata'}
        )

    # 6. P/E (10)
    pe = stock.get('pe_ratio')
    if pe is not None:
        max_score += 10
        if 0 < pe <= 15:
            score += 10
            details.append(
                {'metric': 'P/E', 'value': pe, 'grade': 'pass'}
            )
        elif pe <= 20:
            score += 7
            details.append(
                {'metric': 'P/E', 'value': pe, 'grade': 'ok'}
            )
        elif pe <= 30:
            score += 4
            details.append(
                {'metric': 'P/E', 'value': pe, 'grade': 'warn'}
            )
        else:
            details.append(
                {'metric': 'P/E', 'value': pe, 'grade': 'fail'}
            )

    # 7. Cash Flow (15)
    max_score += 15
    ocf_pos = stock.get('ocf_positive_years')
    ocf_tot = stock.get('ocf_total_years')

    if ocf_pos is not None and ocf_tot and ocf_tot > 0:
        ratio = ocf_pos / ocf_tot
        if ratio >= 0.9:
            score += 15
            details.append(
                {
                    'metric': '現金流',
                    'value': f'{ocf_pos}/{ocf_tot}',
                    'grade': 'pass',
                }
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                {
                    'metric': '現金流',
                    'value': f'{ocf_pos}/{ocf_tot}',
                    'grade': 'ok',
                }
            )
        else:
            score += 5
            details.append(
                {
                    'metric': '現金流',
                    'value': f'{ocf_pos}/{ocf_tot}',
                    'grade': 'warn',
                }
            )
    elif fcf_pos is not None and fcf_tot and fcf_tot > 0:
        ratio = fcf_pos / fcf_tot
        if ratio >= 0.9:
            score += 15
            details.append(
                {
                    'metric': 'FCF',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'pass',
                }
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                {
                    'metric': 'FCF',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'ok',
                }
            )
        else:
            score += 5
            details.append(
                {
                    'metric': 'FCF',
                    'value': f'{fcf_pos}/{fcf_tot}',
                    'grade': 'warn',
                }
            )
    else:
        details.append(
            {'metric': '現金流', 'value': None, 'grade': 'nodata'}
        )

    final = round(score / max_score * 100, 1) if max_score > 0 else 0
    return final, details


def get_grade(score):
    if score >= 85:
        return 'S'
    if score >= 70:
        return 'A'
    if score >= 55:
        return 'B'
    return 'C'


# ============================================================
# 主流程
# ============================================================
def main():
    today = datetime.now().strftime('%Y-%m-%d')
    print(f'=== 巴菲特選股報告 {today} ===')
    print()

    stocks = []

    # 台股
    print('[台股]')
    for sid, name in TW_CANDIDATES.items():
        data = analyze_tw_stock(sid, name)
        score, details = buffett_score(data)
        data['buffett_score'] = score
        data['grade'] = get_grade(score)
        data['score_details'] = details
        data['moat'] = MOAT_DATA.get(sid, {})
        data['exit_conditions'] = EXIT_CONDITIONS.get(sid, '')
        stocks.append(data)
        print(f'    {name} ({sid}): {score} ({data["grade"]})')

    print()
    print('[美股]')
    for ticker_str in US_CANDIDATES:
        data = analyze_us_stock(ticker_str)
        score, details = buffett_score(data)
        data['buffett_score'] = score
        data['grade'] = get_grade(score)
        data['score_details'] = details
        data['moat'] = MOAT_DATA.get(ticker_str, {})
        data['exit_conditions'] = EXIT_CONDITIONS.get(ticker_str, '')
        stocks.append(data)
        print(f'    {data["name"]} ({ticker_str}): {score} ({data["grade"]})')
        time.sleep(0.5)

    # 新聞
    print()
    print('[新聞風險評估]')
    for s in stocks:
        sid = s['stock_id']
        name = s['name']
        if s['market'] == 'TW':
            query = f'{name} 股票 新聞'
        else:
            query = f'{sid} stock news'

        news = fetch_news(query, max_results=5)
        risk_level, risk_summary = assess_news_risk(news, sid)
        s['news'] = {
            'items': news,
            'risk_level': risk_level,
            'risk_summary': risk_summary,
        }
        icon = {'green': '🟢', 'yellow': '🟡', 'red': '🔴'}.get(
            risk_level, '⚪'
        )
        print(f'    {icon} {name} ({sid}): {risk_summary}')
        time.sleep(0.3)

    # 排序
    stocks.sort(key=lambda x: x['buffett_score'], reverse=True)

    # 組裝報告
    report = {
        'generated_at': datetime.now().isoformat(),
        'date': today,
        'methodology': {
            'name': '巴菲特/蒙格價值投資篩選',
            'criteria': [
                {
                    'metric': 'ROE',
                    'threshold': '>= 20%',
                    'weight': 20,
                    'description': '股東權益報酬率，衡量管理層效率',
                },
                {
                    'metric': '淨利率',
                    'threshold': '>= 20%',
                    'weight': 15,
                    'description': '反映定價能力與營運效率',
                },
                {
                    'metric': '毛利率',
                    'threshold': '>= 40%',
                    'weight': 10,
                    'description': '護城河寬度指標',
                },
                {
                    'metric': '負債權益比',
                    'threshold': '<= 0.5',
                    'weight': 15,
                    'description': '財務穩健度',
                },
                {
                    'metric': 'EPS/FCF 持續性',
                    'threshold': '90%+ 年度為正',
                    'weight': 15,
                    'description': '盈利可預測性',
                },
                {
                    'metric': 'P/E',
                    'threshold': '<= 15',
                    'weight': 10,
                    'description': '估值合理性',
                },
                {
                    'metric': '現金流健康度',
                    'threshold': '持續為正',
                    'weight': 15,
                    'description': '真實盈利能力',
                },
            ],
            'grades': {
                'S': '85-100 分，完美符合巴菲特標準',
                'A': '70-84 分，大部分指標優異',
                'B': '55-69 分，部分指標良好',
                'C': '< 55 分，不符合標準',
            },
        },
        'summary': {
            'total_screened': len(stocks),
            'grade_s': len([s for s in stocks if s['grade'] == 'S']),
            'grade_a': len([s for s in stocks if s['grade'] == 'A']),
            'grade_b': len([s for s in stocks if s['grade'] == 'B']),
            'grade_c': len([s for s in stocks if s['grade'] == 'C']),
            'top_picks': [
                {
                    'stock_id': s['stock_id'],
                    'name': s['name'],
                    'market': s['market'],
                    'score': s['buffett_score'],
                    'grade': s['grade'],
                }
                for s in stocks[:10]
            ],
            'news_alerts': [
                {
                    'stock_id': s['stock_id'],
                    'name': s['name'],
                    'risk_level': s['news']['risk_level'],
                    'risk_summary': s['news']['risk_summary'],
                }
                for s in stocks
                if s.get('news', {}).get('risk_level') in ('red', 'yellow')
            ],
        },
        'stocks': stocks,
    }

    # 寫檔
    os.makedirs(REPORTS_DIR, exist_ok=True)
    filepath = os.path.join(REPORTS_DIR, f'{today}.json')
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # 同時寫一份 latest.json 供前端直接讀取
    latest_path = os.path.join(REPORTS_DIR, 'latest.json')
    with open(latest_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # 更新報告索引（供前端歷史切換）
    index_path = os.path.join(REPORTS_DIR, 'index.json')
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            index = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        index = {'reports': []}
    if today not in index['reports']:
        index['reports'].insert(0, today)
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print()
    print(f'報告已儲存至 {filepath}')
    print(f'最新報告連結 {latest_path}')

    # Git commit + push（自動部署 GitHub Pages）
    git_push(today)

    return report


def git_push(today):
    '''自動 commit 並 push 報告到 GitHub，觸發 Pages 部署'''
    try:
        os.chdir(PROJECT_DIR)

        # 確認是 git repo
        result = subprocess.run(
            ['git', 'rev-parse', '--is-inside-work-tree'],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print('[GIT] Not a git repo, skipping push.')
            return

        # Stage report files
        subprocess.run(
            ['git', 'add', 'docs/data/'],
            capture_output=True,
            text=True,
        )

        # Check if there are changes to commit
        result = subprocess.run(
            ['git', 'diff', '--cached', '--quiet'],
            capture_output=True,
        )
        if result.returncode == 0:
            print('[GIT] No changes to commit.')
            return

        # Commit
        msg = f'report: {today} daily buffett screener'
        subprocess.run(
            ['git', 'commit', '-m', msg],
            capture_output=True,
            text=True,
        )
        print(f'[GIT] Committed: {msg}')

        # Push
        result = subprocess.run(
            ['git', 'push'],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print('[GIT] Pushed to remote. GitHub Pages will auto-deploy.')
        else:
            print(f'[GIT] Push failed: {result.stderr}')

    except Exception as e:
        print(f'[GIT] Error: {e}')


if __name__ == '__main__':
    main()
