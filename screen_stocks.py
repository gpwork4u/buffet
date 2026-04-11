'''
巴菲特/蒙格選股篩選器
使用 FinMind (台股) 和 yfinance (美股) 拉取歷史財報，
套用價值投資篩選標準進行分析。
'''
from __future__ import annotations

import json
import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings('ignore')

# ============================================================
# 台股候選清單（大型權值股 + 知名價值股）
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

# ============================================================
# 美股候選清單（巴菲特持倉 + 經典護城河股）
# ============================================================
US_CANDIDATES = [
    'AAPL', 'MSFT', 'GOOGL', 'BRK-B', 'JNJ',
    'KO', 'PG', 'V', 'MA', 'UNH',
    'COST', 'WMT', 'MCD', 'AXP', 'HD',
    'AVGO', 'ABBV', 'PEP', 'LLY', 'AMZN',
]

FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'


# ============================================================
# 台股財報拉取
# ============================================================
def fetch_tw_data(dataset, stock_id, start_date='2015-01-01'):
    params = {
        'dataset': dataset,
        'data_id': stock_id,
        'start_date': start_date,
    }
    try:
        resp = requests.get(FINMIND_URL, params=params, timeout=30)
        data = resp.json()
        if data.get('status') == 200 and data.get('data'):
            return pd.DataFrame(data['data'])
    except Exception as e:
        print(f'  [WARN] {stock_id} {dataset} fetch error: {e}')
    return pd.DataFrame()


def pivot_tw_statement(df):
    '''將 FinMind 長格式轉為 date x type 的寬格式'''
    if df.empty:
        return pd.DataFrame()
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    pivoted = df.pivot_table(
        index='date', columns='type', values='value', aggfunc='first'
    )
    pivoted.index = pd.to_datetime(pivoted.index)
    return pivoted.sort_index()


def analyze_tw_stock(stock_id, name):
    '''分析單一台股'''
    print(f'  拉取 {stock_id} {name} ...')

    income_raw = fetch_tw_data(
        'TaiwanStockFinancialStatements', stock_id
    )
    time.sleep(1)
    balance_raw = fetch_tw_data('TaiwanStockBalanceSheet', stock_id)
    time.sleep(1)
    cashflow_raw = fetch_tw_data(
        'TaiwanStockCashFlowsStatement', stock_id
    )
    time.sleep(1)

    income = pivot_tw_statement(income_raw)
    balance = pivot_tw_statement(balance_raw)
    cashflow = pivot_tw_statement(cashflow_raw)

    result = {
        'stock_id': stock_id,
        'name': name,
        'market': 'TW',
    }

    # --- ROE ---
    if not income.empty and not balance.empty:
        try:
            # 年度化：取每年 Q4 或最後一季
            net_income_col = None
            for col in ['NetIncome', 'ProfitLossAttributableToOwnersOfParent']:
                if col in income.columns:
                    net_income_col = col
                    break

            equity_col = None
            for col in ['Equity', 'EquityAttributableToOwnersOfParent']:
                if col in balance.columns:
                    equity_col = col
                    break

            if net_income_col and equity_col:
                # 取年度資料
                annual_income = income[net_income_col].resample('YE').sum()
                annual_equity = balance[equity_col].resample('YE').last()

                # 對齊
                common_years = annual_income.index.intersection(
                    annual_equity.index
                )
                if len(common_years) >= 3:
                    roe_series = (
                        annual_income[common_years]
                        / annual_equity[common_years]
                        * 100
                    )
                    roe_series = roe_series.dropna()
                    result['roe_avg'] = round(roe_series.mean(), 2)
                    result['roe_latest'] = round(roe_series.iloc[-1], 2)
                    result['roe_years'] = len(roe_series)
        except Exception:
            pass

    # --- Net Margin ---
    if not income.empty:
        try:
            rev_col = None
            for col in ['Revenue', 'TotalOperatingRevenue']:
                if col in income.columns:
                    rev_col = col
                    break

            ni_col = None
            for col in ['NetIncome', 'ProfitLossAttributableToOwnersOfParent']:
                if col in income.columns:
                    ni_col = col
                    break

            if rev_col and ni_col:
                annual_rev = income[rev_col].resample('YE').sum()
                annual_ni = income[ni_col].resample('YE').sum()
                common = annual_rev.index.intersection(annual_ni.index)
                if len(common) >= 3:
                    margin_series = annual_ni[common] / annual_rev[common] * 100
                    margin_series = margin_series.dropna()
                    result['net_margin_avg'] = round(margin_series.mean(), 2)
                    result['net_margin_latest'] = round(
                        margin_series.iloc[-1], 2
                    )
        except Exception:
            pass

    # --- Gross Margin ---
    if not income.empty and 'GrossProfit' in income.columns:
        try:
            rev_col = None
            for col in ['Revenue', 'TotalOperatingRevenue']:
                if col in income.columns:
                    rev_col = col
                    break
            if rev_col:
                annual_gp = income['GrossProfit'].resample('YE').sum()
                annual_rev = income[rev_col].resample('YE').sum()
                common = annual_gp.index.intersection(annual_rev.index)
                if len(common) >= 3:
                    gm = annual_gp[common] / annual_rev[common] * 100
                    gm = gm.dropna()
                    result['gross_margin_avg'] = round(gm.mean(), 2)
        except Exception:
            pass

    # --- EPS 成長 ---
    if not income.empty and 'EPS' in income.columns:
        try:
            annual_eps = income['EPS'].resample('YE').sum()
            annual_eps = annual_eps.dropna()
            if len(annual_eps) >= 5:
                positive_years = (annual_eps > 0).sum()
                result['eps_positive_years'] = int(positive_years)
                result['eps_total_years'] = len(annual_eps)
                result['eps_latest'] = round(annual_eps.iloc[-1], 2)
                result['eps_5yr_ago'] = round(annual_eps.iloc[-5], 2)
                if annual_eps.iloc[-5] > 0:
                    cagr = (
                        (annual_eps.iloc[-1] / annual_eps.iloc[-5])
                        ** (1 / 5)
                        - 1
                    ) * 100
                    result['eps_cagr_5y'] = round(cagr, 2)
        except Exception:
            pass

    # --- Debt to Equity ---
    if not balance.empty:
        try:
            liab_col = None
            for col in ['TotalLiabilities', 'Liabilities']:
                if col in balance.columns:
                    liab_col = col
                    break

            eq_col = None
            for col in ['Equity', 'EquityAttributableToOwnersOfParent']:
                if col in balance.columns:
                    eq_col = col
                    break

            if liab_col and eq_col:
                latest_liab = balance[liab_col].dropna().iloc[-1]
                latest_eq = balance[eq_col].dropna().iloc[-1]
                if latest_eq > 0:
                    result['debt_to_equity'] = round(
                        latest_liab / latest_eq, 2
                    )
        except Exception:
            pass

    # --- Operating Cash Flow ---
    if not cashflow.empty:
        try:
            ocf_col = None
            for col in [
                'CashFlowsFromOperatingActivities',
                'CashGeneratedFromOperations',
            ]:
                if col in cashflow.columns:
                    ocf_col = col
                    break
            if ocf_col:
                annual_ocf = cashflow[ocf_col].resample('YE').sum()
                annual_ocf = annual_ocf.dropna()
                if len(annual_ocf) >= 3:
                    positive_ocf = (annual_ocf > 0).sum()
                    result['ocf_positive_years'] = int(positive_ocf)
                    result['ocf_total_years'] = len(annual_ocf)
        except Exception:
            pass

    return result


# ============================================================
# 美股財報分析
# ============================================================
def analyze_us_stock(ticker_str):
    '''分析單一美股'''
    print(f'  拉取 {ticker_str} ...')
    try:
        ticker = yf.Ticker(ticker_str)
        info = ticker.info or {}

        income = ticker.income_stmt
        balance = ticker.balance_sheet
        cashflow = ticker.cashflow
        income_q = ticker.quarterly_income_stmt

        result = {
            'stock_id': ticker_str,
            'name': info.get('shortName', ticker_str),
            'market': 'US',
        }

        # --- ROE ---
        if income is not None and not income.empty:
            if balance is not None and not balance.empty:
                try:
                    ni_row = None
                    for r in ['Net Income', 'Net Income Common Stockholders']:
                        if r in income.index:
                            ni_row = r
                            break
                    eq_row = None
                    for r in [
                        'Stockholders Equity',
                        'Total Stockholder Equity',
                        'Common Stock Equity',
                    ]:
                        if r in balance.index:
                            eq_row = r
                            break
                    if ni_row and eq_row:
                        ni_vals = income.loc[ni_row].dropna().sort_index()
                        eq_vals = balance.loc[eq_row].dropna().sort_index()
                        common = ni_vals.index.intersection(eq_vals.index)
                        if len(common) >= 2:
                            roe = ni_vals[common] / eq_vals[common] * 100
                            result['roe_avg'] = round(roe.mean(), 2)
                            result['roe_latest'] = round(
                                roe.sort_index().iloc[-1], 2
                            )
                            result['roe_years'] = len(roe)
                except Exception:
                    pass

        # --- Net Margin ---
        if income is not None and not income.empty:
            try:
                rev_row = None
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in income.index:
                        rev_row = r
                        break
                ni_row = None
                for r in ['Net Income', 'Net Income Common Stockholders']:
                    if r in income.index:
                        ni_row = r
                        break
                if rev_row and ni_row:
                    rev = income.loc[rev_row].dropna().sort_index()
                    ni = income.loc[ni_row].dropna().sort_index()
                    common = rev.index.intersection(ni.index)
                    if len(common) >= 2:
                        margin = ni[common] / rev[common] * 100
                        result['net_margin_avg'] = round(margin.mean(), 2)
                        result['net_margin_latest'] = round(
                            margin.sort_index().iloc[-1], 2
                        )
            except Exception:
                pass

        # --- Gross Margin ---
        if income is not None and not income.empty:
            try:
                if 'Gross Profit' in income.index:
                    rev_row = None
                    for r in ['Total Revenue', 'Operating Revenue']:
                        if r in income.index:
                            rev_row = r
                            break
                    if rev_row:
                        gp = income.loc['Gross Profit'].dropna().sort_index()
                        rev = income.loc[rev_row].dropna().sort_index()
                        common = gp.index.intersection(rev.index)
                        if len(common) >= 2:
                            gm = gp[common] / rev[common] * 100
                            result['gross_margin_avg'] = round(gm.mean(), 2)
            except Exception:
                pass

        # --- EPS from info ---
        trailing_eps = info.get('trailingEps')
        if trailing_eps:
            result['eps_latest'] = round(trailing_eps, 2)

        # --- Debt to Equity ---
        de = info.get('debtToEquity')
        if de is not None:
            result['debt_to_equity'] = round(de / 100, 2)

        # --- Free Cash Flow ---
        if cashflow is not None and not cashflow.empty:
            try:
                fcf_row = 'Free Cash Flow'
                if fcf_row in cashflow.index:
                    fcf_vals = cashflow.loc[fcf_row].dropna().sort_index()
                    positive_fcf = (fcf_vals > 0).sum()
                    result['fcf_positive_years'] = int(positive_fcf)
                    result['fcf_total_years'] = len(fcf_vals)
                    result['fcf_latest'] = float(fcf_vals.iloc[-1])
                else:
                    # 計算 FCF = Operating CF - CapEx
                    ocf_row = None
                    for r in [
                        'Operating Cash Flow',
                        'Cash Flow From Continuing Operating Activities',
                    ]:
                        if r in cashflow.index:
                            ocf_row = r
                            break
                    capex_row = None
                    for r in [
                        'Capital Expenditure',
                        'Capital Expenditures',
                    ]:
                        if r in cashflow.index:
                            capex_row = r
                            break
                    if ocf_row and capex_row:
                        ocf = cashflow.loc[ocf_row].dropna().sort_index()
                        capex = cashflow.loc[capex_row].dropna().sort_index()
                        common = ocf.index.intersection(capex.index)
                        fcf = ocf[common] + capex[common]  # capex is negative
                        positive_fcf = (fcf > 0).sum()
                        result['fcf_positive_years'] = int(positive_fcf)
                        result['fcf_total_years'] = len(fcf)
                        result['fcf_latest'] = float(fcf.iloc[-1])
            except Exception:
                pass

        # --- Current Ratio ---
        cr = info.get('currentRatio')
        if cr:
            result['current_ratio'] = round(cr, 2)

        # --- P/E ---
        pe = info.get('trailingPE')
        if pe:
            result['pe_ratio'] = round(pe, 2)

        # --- Market Cap ---
        mc = info.get('marketCap')
        if mc:
            result['market_cap_b'] = round(mc / 1e9, 1)

        return result
    except Exception as e:
        print(f'  [WARN] {ticker_str} error: {e}')
        return {
            'stock_id': ticker_str,
            'name': ticker_str,
            'market': 'US',
        }


# ============================================================
# 巴菲特篩選標準
# ============================================================
def buffett_score(stock):
    '''
    根據巴菲特/蒙格標準打分 (0-100)
    每項通過得分，加權計算總分
    '''
    score = 0
    max_score = 0
    details = []

    # 1. ROE > 15% (權重 20)
    max_score += 20
    roe = stock.get('roe_avg')
    if roe is not None:
        if roe >= 20:
            score += 20
            details.append(f'ROE {roe}% >= 20% ✓✓')
        elif roe >= 15:
            score += 15
            details.append(f'ROE {roe}% >= 15% ✓')
        elif roe >= 10:
            score += 8
            details.append(f'ROE {roe}% >= 10% △')
        else:
            details.append(f'ROE {roe}% < 10% ✗')
    else:
        details.append('ROE 數據不足 ✗')

    # 2. Net Margin > 20% (權重 15)
    max_score += 15
    nm = stock.get('net_margin_avg')
    if nm is not None:
        if nm >= 20:
            score += 15
            details.append(f'淨利率 {nm}% >= 20% ✓✓')
        elif nm >= 10:
            score += 10
            details.append(f'淨利率 {nm}% >= 10% ✓')
        elif nm >= 5:
            score += 5
            details.append(f'淨利率 {nm}% >= 5% △')
        else:
            details.append(f'淨利率 {nm}% < 5% ✗')
    else:
        details.append('淨利率數據不足 ✗')

    # 3. Gross Margin > 40% (權重 10)
    max_score += 10
    gm = stock.get('gross_margin_avg')
    if gm is not None:
        if gm >= 40:
            score += 10
            details.append(f'毛利率 {gm}% >= 40% ✓✓')
        elif gm >= 30:
            score += 7
            details.append(f'毛利率 {gm}% >= 30% ✓')
        elif gm >= 20:
            score += 4
            details.append(f'毛利率 {gm}% >= 20% △')
        else:
            details.append(f'毛利率 {gm}% < 20% ✗')
    else:
        details.append('毛利率數據不足 ✗')

    # 4. D/E < 0.5 (權重 15)
    max_score += 15
    de = stock.get('debt_to_equity')
    if de is not None:
        if de <= 0.5:
            score += 15
            details.append(f'負債權益比 {de} <= 0.5 ✓✓')
        elif de <= 1.0:
            score += 10
            details.append(f'負債權益比 {de} <= 1.0 ✓')
        elif de <= 1.5:
            score += 5
            details.append(f'負債權益比 {de} <= 1.5 △')
        else:
            details.append(f'負債權益比 {de} > 1.5 ✗')
    else:
        details.append('負債權益比數據不足 ✗')

    # 5. EPS 持續正成長 (權重 15)
    max_score += 15
    eps_pos = stock.get('eps_positive_years')
    eps_total = stock.get('eps_total_years')
    if eps_pos is not None and eps_total is not None and eps_total > 0:
        ratio = eps_pos / eps_total
        if ratio >= 0.9:
            score += 15
            details.append(
                f'EPS 正值 {eps_pos}/{eps_total} 年 (>90%) ✓✓'
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                f'EPS 正值 {eps_pos}/{eps_total} 年 (>70%) ✓'
            )
        else:
            score += 5
            details.append(
                f'EPS 正值 {eps_pos}/{eps_total} 年 △'
            )
    else:
        # 美股用 FCF 替代
        fcf_pos = stock.get('fcf_positive_years')
        fcf_total = stock.get('fcf_total_years')
        if fcf_pos is not None and fcf_total is not None and fcf_total > 0:
            ratio = fcf_pos / fcf_total
            if ratio >= 0.9:
                score += 15
                details.append(
                    f'FCF 正值 {fcf_pos}/{fcf_total} 年 (>90%) ✓✓'
                )
            elif ratio >= 0.7:
                score += 10
                details.append(
                    f'FCF 正值 {fcf_pos}/{fcf_total} 年 (>70%) ✓'
                )
            else:
                score += 5
                details.append(
                    f'FCF 正值 {fcf_pos}/{fcf_total} 年 △'
                )
        else:
            details.append('EPS/FCF 數據不足 ✗')

    # 6. P/E 合理 (權重 10，僅美股)
    max_score += 10
    pe = stock.get('pe_ratio')
    if pe is not None:
        if 0 < pe <= 15:
            score += 10
            details.append(f'P/E {pe} <= 15 ✓✓')
        elif pe <= 20:
            score += 7
            details.append(f'P/E {pe} <= 20 ✓')
        elif pe <= 30:
            score += 4
            details.append(f'P/E {pe} <= 30 △')
        else:
            details.append(f'P/E {pe} > 30 ✗')
    else:
        details.append('P/E 數據不足')
        max_score -= 10  # 不扣分

    # 7. 現金流健康 (權重 15)
    max_score += 15
    ocf_pos = stock.get('ocf_positive_years')
    ocf_total = stock.get('ocf_total_years')
    fcf_pos = stock.get('fcf_positive_years')
    fcf_total = stock.get('fcf_total_years')

    if ocf_pos is not None and ocf_total is not None and ocf_total > 0:
        ratio = ocf_pos / ocf_total
        if ratio >= 0.9:
            score += 15
            details.append(
                f'營業現金流正值 {ocf_pos}/{ocf_total} 年 ✓✓'
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                f'營業現金流正值 {ocf_pos}/{ocf_total} 年 ✓'
            )
        else:
            score += 5
            details.append(
                f'營業現金流正值 {ocf_pos}/{ocf_total} 年 △'
            )
    elif fcf_pos is not None and fcf_total is not None and fcf_total > 0:
        ratio = fcf_pos / fcf_total
        if ratio >= 0.9:
            score += 15
            details.append(
                f'FCF 正值 {fcf_pos}/{fcf_total} 年 ✓✓'
            )
        elif ratio >= 0.7:
            score += 10
            details.append(
                f'FCF 正值 {fcf_pos}/{fcf_total} 年 ✓'
            )
        else:
            score += 5
            details.append(f'FCF 正值 {fcf_pos}/{fcf_total} 年 △')
    else:
        details.append('現金流數據不足 ✗')

    final_score = round(score / max_score * 100, 1) if max_score > 0 else 0
    return final_score, details


# ============================================================
# Main
# ============================================================
def main():
    all_results = []

    print('=' * 60)
    print('台股財報分析')
    print('=' * 60)
    for sid, name in TW_CANDIDATES.items():
        result = analyze_tw_stock(sid, name)
        score, details = buffett_score(result)
        result['buffett_score'] = score
        result['details'] = details
        all_results.append(result)
        print(f'  → {name} ({sid}): 巴菲特分數 = {score}')

    print()
    print('=' * 60)
    print('美股財報分析')
    print('=' * 60)
    for ticker_str in US_CANDIDATES:
        result = analyze_us_stock(ticker_str)
        score, details = buffett_score(result)
        result['buffett_score'] = score
        result['details'] = details
        all_results.append(result)
        print(f'  → {result["name"]} ({ticker_str}): 巴菲特分數 = {score}')
        time.sleep(0.5)

    # 儲存結果
    output_path = '/Users/gpwang/project/buffet/screening_results.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)

    print(f'\n結果已儲存至 {output_path}')

    # 排名
    print('\n' + '=' * 60)
    print('巴菲特分數排名 (前 15 名)')
    print('=' * 60)
    ranked = sorted(all_results, key=lambda x: x['buffett_score'], reverse=True)
    for i, r in enumerate(ranked[:15], 1):
        print(
            f'  {i:2d}. {r["name"]:20s} ({r["market"]}/{r["stock_id"]:6s}) '
            f'分數: {r["buffett_score"]:5.1f}'
        )


if __name__ == '__main__':
    main()
