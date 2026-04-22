"""季節性規律偵測"""
import numpy as np
import pandas as pd

MONTH_ZH = {1:'1月', 2:'2月', 3:'3月', 4:'4月', 5:'5月', 6:'6月',
            7:'7月', 8:'8月', 9:'9月', 10:'10月', 11:'11月', 12:'12月'}

YEARS = (2023, 2024, 2025)


def _ym(base_year: int, start_month: int, offset: int):
    """(base_year, start_month, offset) → (actual_year, actual_month)"""
    total = (start_month - 1) + offset
    return base_year + total // 12, total % 12 + 1


def scan_all(monthly: pd.DataFrame, stocks: pd.DataFrame,
             window: int, rise_pct: float, drop_pct: float,
             years_required: int) -> pd.DataFrame:
    """
    window      : 上漲窗口月數（首月收盤 → 末月收盤）
    rise_pct    : 漲幅門檻（百分比，如 15 代表 15%）
    drop_pct    : 跌幅門檻（百分比，如 5 代表 5%）
    years_required : 幾年都要符合
    """
    results = []
    rise_thr = rise_pct / 100
    drop_thr = -drop_pct / 100

    for _, stock in stocks.iterrows():
        ticker = stock['ticker']
        if ticker not in monthly.columns:
            continue
        prices = monthly[ticker].dropna()
        if len(prices) < 20:
            continue

        for start_m in range(1, 13):
            year_rows = []

            for year in YEARS:
                y1, m1 = _ym(year, start_m, 0)           # 首月
                yw, mw = _ym(year, start_m, window - 1)  # 末月
                yn, mn = _ym(year, start_m, window)       # 隔月

                def get_p(y, m):
                    mask = (prices.index.year == y) & (prices.index.month == m)
                    return float(prices[mask].iloc[-1]) if mask.any() else None

                p1, pw, pn = get_p(y1, m1), get_p(yw, mw), get_p(yn, mn)
                if None in (p1, pw, pn) or p1 <= 0 or pw <= 0:
                    break  # 資料缺，跳過這個窗口

                year_rows.append({
                    'year': year,
                    'rise': (pw - p1) / p1,
                    'drop': (pn - pw) / pw,
                    'p1': p1, 'pw': pw, 'pn': pn,
                    'y1': y1, 'm1': m1,
                    'yw': yw, 'mw': mw,
                    'yn': yn, 'mn': mn,
                })
            else:
                # 全年資料齊全，檢查條件
                passing = [r for r in year_rows
                           if r['rise'] >= rise_thr and r['drop'] <= drop_thr]
                if len(passing) >= years_required:
                    m1_lbl = MONTH_ZH[start_m]
                    mw_lbl = MONTH_ZH[(start_m - 1 + window - 1) % 12 + 1]
                    mn_lbl = MONTH_ZH[(start_m - 1 + window)     % 12 + 1]

                    row = {
                        'code':         stock['code'],
                        'name':         stock['name'],
                        'market':       stock['market'],
                        'window':       f'{m1_lbl}~{mw_lbl}漲 → {mn_lbl}跌',
                        'start_month':  start_m,
                        'years_passed': len(passing),
                        'avg_rise_%':   round(np.mean([r['rise'] for r in year_rows]) * 100, 2),
                        'avg_drop_%':   round(np.mean([r['drop'] for r in year_rows]) * 100, 2),
                        'min_rise_%':   round(min(r['rise'] for r in year_rows) * 100, 2),
                        '_year_rows':   year_rows,
                    }
                    for r in year_rows:
                        row[f"{r['year']}_rise_%"] = round(r['rise'] * 100, 2)
                        row[f"{r['year']}_drop_%"] = round(r['drop'] * 100, 2)
                    results.append(row)

    return pd.DataFrame(results) if results else pd.DataFrame()
