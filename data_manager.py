"""資料下載與快取管理"""
import pickle
import time
import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

CACHE_DIR = Path(__file__).parent / 'cache'
CACHE_DIR.mkdir(exist_ok=True)

STOCK_LIST_CACHE = CACHE_DIR / 'stock_list.pkl'
PRICE_CACHE      = CACHE_DIR / 'monthly_prices.parquet'

BATCH_SIZE = 40
SLEEP_SEC  = 0.8
START_DATE = '2022-12-01'
END_DATE   = '2026-02-01'


def get_stock_list(force_refresh: bool = False) -> pd.DataFrame:
    if not force_refresh and STOCK_LIST_CACHE.exists():
        return pickle.loads(STOCK_LIST_CACHE.read_bytes())

    stocks = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    for mode, market, suffix in [('2', 'TWSE', '.TW'), ('4', 'TPEX', '.TWO')]:
        url = f'https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}'
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.encoding = 'big5'
            for tr in BeautifulSoup(resp.text, 'html.parser').find_all('tr'):
                tds = tr.find_all('td')
                if not tds:
                    continue
                text = tds[0].get_text()
                if '　' in text:
                    parts = text.split('　')
                    code = parts[0].strip()
                    name = parts[1].strip() if len(parts) > 1 else ''
                    if code.isdigit() and len(code) == 4:
                        stocks.append({'code': code, 'name': name,
                                       'market': market, 'ticker': code + suffix})
        except Exception as e:
            print(f'[WARN] {market}: {e}')

    df = pd.DataFrame(stocks).drop_duplicates('code').reset_index(drop=True)
    STOCK_LIST_CACHE.write_bytes(pickle.dumps(df))
    return df


def download_all_prices(tickers: list, progress_fn=None) -> pd.DataFrame:
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    frames = []

    for i, batch in enumerate(batches):
        if progress_fn:
            progress_fn(i / len(batches), f'批次 {i + 1}/{len(batches)}　({batch[0]})')
        try:
            raw = yf.download(batch, start=START_DATE, end=END_DATE,
                              auto_adjust=True, progress=False, threads=True)
            if not raw.empty:
                if isinstance(raw.columns, pd.MultiIndex):
                    close = raw['Close']
                else:
                    close = raw[['Close']]
                    close.columns = batch
                frames.append(close.resample('ME').last())
        except Exception:
            pass
        time.sleep(SLEEP_SEC)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1)
    combined = combined.loc[:, ~combined.columns.duplicated()]
    combined.to_parquet(PRICE_CACHE)
    if progress_fn:
        progress_fn(1.0, '完成')
    return combined


def load_prices() -> pd.DataFrame:
    return pd.read_parquet(PRICE_CACHE) if PRICE_CACHE.exists() else pd.DataFrame()


def cache_info() -> dict:
    if PRICE_CACHE.exists():
        return {'exists': True, 'mtime': datetime.fromtimestamp(PRICE_CACHE.stat().st_mtime)}
    return {'exists': False, 'mtime': None}
