#!/usr/bin/env python3
"""
台股季節性漲跌規律掃描器
條件：任意連續3個月漲幅>15%，且隔月跌幅>5%，2023/2024/2025三年皆符合
"""

import yfinance as yf
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import warnings
warnings.filterwarnings('ignore')

# ── 設定 ──────────────────────────────────────────────────────────────────────
YEARS          = [2023, 2024, 2025]
RISE_THRESHOLD =  0.15   # 3個月漲幅門檻
DROP_THRESHOLD = -0.05   # 隔月跌幅門檻
OUTPUT_FILE    = 'seasonal_patterns.csv'
BATCH_SIZE     = 40
SLEEP_SEC      = 0.8

MONTH_ZH = {1:'1月',2:'2月',3:'3月',4:'4月',5:'5月',6:'6月',
            7:'7月',8:'8月',9:'9月',10:'10月',11:'11月',12:'12月'}


# ── 1. 取得股票清單 ───────────────────────────────────────────────────────────
def get_tw_stocks() -> pd.DataFrame:
    stocks = []
    configs = [('2', 'TWSE', '.TW'), ('4', 'TPEX', '.TWO')]
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    for mode, market, suffix in configs:
        url = f'https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}'
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.encoding = 'big5'
            soup = BeautifulSoup(resp.text, 'html.parser')
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if not tds:
                    continue
                text = tds[0].get_text()
                if '　' in text:          # 全形空白分隔代號與名稱
                    parts = text.split('　')
                    code = parts[0].strip()
                    name = parts[1].strip() if len(parts) > 1 else ''
                    if code.isdigit() and len(code) == 4:
                        stocks.append({
                            'code': code, 'name': name,
                            'market': market, 'ticker': code + suffix
                        })
        except Exception as e:
            print(f'  [警告] 無法取得 {market} 清單：{e}')

    df = pd.DataFrame(stocks).drop_duplicates('code').reset_index(drop=True)
    return df


# ── 2. 下載並轉為月收盤 ────────────────────────────────────────────────────────
def download_monthly(tickers: list, start='2022-12-01', end='2026-02-01') -> pd.DataFrame:
    try:
        raw = yf.download(
            tickers, start=start, end=end,
            auto_adjust=True, progress=False, threads=True
        )
        if raw.empty:
            return pd.DataFrame()

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw['Close']
        else:
            close = raw[['Close']]
            close.columns = tickers

        return close.resample('ME').last()
    except Exception:
        return pd.DataFrame()


# ── 3. 對單一股票偵測季節性規律 ────────────────────────────────────────────────
def detect_patterns(monthly: pd.DataFrame, ticker: str) -> list:
    if ticker not in monthly.columns:
        return []

    prices = monthly[ticker].dropna()
    if len(prices) < 24:          # 至少需要2年資料才值得分析
        return []

    found = []

    for start_m in range(1, 13):  # 12種可能的3個月視窗
        year_rows = []

        for year in YEARS:
            # 把「起始月 + offset」轉成實際 (year, month)
            def actual_ym(offset):
                total = (start_m - 1) + offset
                return year + total // 12, total % 12 + 1

            y1, m1 = actual_ym(0)  # 首月
            y3, m3 = actual_ym(2)  # 末月
            y4, m4 = actual_ym(3)  # 隔月

            def get_price(y, m):
                mask = (prices.index.year == y) & (prices.index.month == m)
                return prices[mask].iloc[-1] if mask.any() else None

            p1, p3, p4 = get_price(y1, m1), get_price(y3, m3), get_price(y4, m4)
            if p1 is None or p3 is None or p4 is None or p1 <= 0 or p3 <= 0:
                break  # 任一年資料缺，整個視窗放棄

            rise = (p3 - p1) / p1
            drop = (p4 - p3) / p3
            year_rows.append({'year': year, 'rise': rise, 'drop': drop})

        if len(year_rows) < len(YEARS):
            continue

        if (all(r['rise'] > RISE_THRESHOLD for r in year_rows) and
                all(r['drop'] < DROP_THRESHOLD for r in year_rows)):

            y1, m1 = (start_m - 1 + 0) // 12, (start_m - 1 + 0) % 12 + 1
            y3, m3 = (start_m - 1 + 2) // 12, (start_m - 1 + 2) % 12 + 1
            y4, m4 = (start_m - 1 + 3) // 12, (start_m - 1 + 3) % 12 + 1
            m1_label = MONTH_ZH[(start_m - 1) % 12 + 1]
            m3_label = MONTH_ZH[(start_m + 1) % 12 + 1]
            m4_label = MONTH_ZH[(start_m + 2) % 12 + 1]
            window_label = f'{m1_label}~{m3_label}漲 → {m4_label}跌'

            row = {
                'window':      window_label,
                'start_month': start_m,
                'avg_rise_%':  round(np.mean([r['rise'] for r in year_rows]) * 100, 1),
                'avg_drop_%':  round(np.mean([r['drop'] for r in year_rows]) * 100, 1),
                'min_rise_%':  round(min(r['rise'] for r in year_rows) * 100, 1),
                'max_drop_%':  round(max(r['drop'] for r in year_rows) * 100, 1),
            }
            for r in year_rows:
                row[f"{r['year']}_rise_%"] = round(r['rise'] * 100, 1)
                row[f"{r['year']}_drop_%"] = round(r['drop'] * 100, 1)

            found.append(row)

    return found


# ── 主流程 ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('=' * 65)
    print('  台股季節性漲跌規律掃描器')
    print('  條件：連續3個月漲>15%，隔月跌>5%，2023~2025三年皆符合')
    print('=' * 65)

    # Step 1
    print('\n[1/4] 取得上市櫃股票清單...')
    stocks_df = get_tw_stocks()
    twse_n = (stocks_df.market == 'TWSE').sum()
    tpex_n = (stocks_df.market == 'TPEX').sum()
    print(f'  → 共 {len(stocks_df)} 支（上市 {twse_n}，上櫃 {tpex_n}）')

    # Step 2
    print('\n[2/4] 分批下載月線資料（2023~2025）...')
    tickers   = stocks_df['ticker'].tolist()
    batches   = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    monthly_frames = []

    for i, batch in enumerate(batches, 1):
        print(f'  批次 {i:3d}/{len(batches)}  {batch[0]}~{batch[-1]}', end='\r')
        frame = download_monthly(batch)
        if not frame.empty:
            monthly_frames.append(frame)
        time.sleep(SLEEP_SEC)

    print(f'\n  → 下載完成，共 {len(batches)} 批')

    # Step 3
    print('\n[3/4] 合併月線資料...')
    if not monthly_frames:
        print('  錯誤：沒有取到任何價格資料，請檢查網路連線')
        raise SystemExit(1)

    combined = pd.concat(monthly_frames, axis=1)
    combined = combined.loc[:, ~combined.columns.duplicated()]
    print(f'  → {combined.shape[1]} 支股票 × {combined.shape[0]} 個月份')

    # Step 4
    print('\n[4/4] 掃描季節性規律...')
    all_results = []

    for i, (_, stock) in enumerate(stocks_df.iterrows()):
        if i % 200 == 0:
            print(f'  進度：{i:4d}/{len(stocks_df)}', end='\r')
        for pat in detect_patterns(combined, stock['ticker']):
            all_results.append({'code': stock['code'], 'name': stock['name'],
                                'market': stock['market'], **pat})

    print(f'\n  → 掃描完成')

    # 輸出
    print('\n' + '=' * 65)
    if not all_results:
        print('  未找到符合條件的股票')
    else:
        result_df = (pd.DataFrame(all_results)
                     .sort_values(['avg_rise_%'], ascending=False)
                     .reset_index(drop=True))
        result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f'  找到 {len(result_df)} 個規律，儲存至 {OUTPUT_FILE}')
        print()
        pd.set_option('display.max_rows', 50)
        pd.set_option('display.width', 120)
        cols_show = ['code', 'name', 'market', 'window',
                     'avg_rise_%', 'avg_drop_%', 'min_rise_%',
                     '2023_rise_%', '2023_drop_%',
                     '2024_rise_%', '2024_drop_%',
                     '2025_rise_%', '2025_drop_%']
        print(result_df[cols_show].to_string(index=False))
    print('=' * 65)
