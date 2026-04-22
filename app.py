"""台股季節性規律掃描器 — Streamlit 介面"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from data_manager import (get_stock_list, download_all_prices,
                          load_prices, cache_info, PRICE_CACHE)
from pattern_scanner import scan_all, MONTH_ZH

# ── 頁面設定 ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title='台股季節性規律掃描器',
    page_icon='📈',
    layout='wide',
    initial_sidebar_state='expanded',
)

st.title('📈 台股季節性規律掃描器')
st.caption('掃描上市櫃股票，找出每年固定時段漲幅顯著、且隔月明顯回落的規律')

# ── 側欄：參數設定 ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.header('⚙️ 掃描參數')

    c1, c2 = st.columns(2)
    window_months  = c1.number_input('窗口長度（月）', 1, 6, 3,
                                     help='連續幾個月為一個上漲窗口')
    years_required = c2.number_input('需符合年數', 1, 3, 3,
                                     help='2023/2024/2025 中需有幾年同時符合')

    rise_pct = st.slider('上漲門檻 (%)', 5, 80, 15, step=1,
                         help='窗口首月→末月收盤漲幅需超過此值')
    drop_pct = st.slider('回落門檻 (%)', 1, 30, 5, step=1,
                         help='末月→下一個月收盤跌幅需超過此值')

    markets = st.multiselect(
        '市場',
        ['TWSE', 'TPEX'],
        default=['TWSE', 'TPEX'],
        format_func=lambda x: '上市 (TWSE)' if x == 'TWSE' else '上櫃 (TPEX)',
    )

    st.divider()
    st.subheader('📦 資料管理')

    info = cache_info()
    if info['exists']:
        st.caption(f"快取更新：{info['mtime'].strftime('%Y-%m-%d %H:%M')}")
        monthly_preview = load_prices()
        st.caption(f"涵蓋 {monthly_preview.shape[1]} 支股票")
    else:
        st.warning('尚未下載資料，請先點下方按鈕')

    btn_download = st.button('🔄 下載／更新資料', use_container_width=True,
                             help='首次使用約需 15～20 分鐘')
    btn_scan = st.button('🔍 開始掃描', use_container_width=True,
                         type='primary', disabled=not info['exists'])

# ── 下載資料 ───────────────────────────────────────────────────────────────────
if btn_download:
    with st.spinner('正在取得股票清單...'):
        stocks_all = get_stock_list(force_refresh=True)

    twse_n = (stocks_all.market == 'TWSE').sum()
    tpex_n = (stocks_all.market == 'TPEX').sum()
    st.info(f'股票清單：共 {len(stocks_all)} 支（上市 {twse_n}，上櫃 {tpex_n}）')

    pbar    = st.progress(0.0, text='準備下載...')
    pstatus = st.empty()

    def _progress(pct, msg):
        pbar.progress(min(float(pct), 1.0), text=msg)
        pstatus.caption(msg)

    monthly_dl = download_all_prices(stocks_all['ticker'].tolist(), progress_fn=_progress)

    if monthly_dl.empty:
        st.error('下載失敗，請檢查網路連線後再試')
    else:
        st.success(f'✅ 下載完成：{monthly_dl.shape[1]} 支股票，{monthly_dl.shape[0]} 個月份')
        st.rerun()

# ── 掃描 ───────────────────────────────────────────────────────────────────────
if btn_scan:
    if not markets:
        st.warning('請至少選擇一個市場')
        st.stop()

    stocks_all = get_stock_list()
    monthly    = load_prices()
    stocks_sel = stocks_all[stocks_all['market'].isin(markets)]

    with st.spinner(f'掃描中... {len(stocks_sel)} 支股票，請稍候'):
        results = scan_all(
            monthly=monthly,
            stocks=stocks_sel,
            window=int(window_months),
            rise_pct=float(rise_pct),
            drop_pct=float(drop_pct),
            years_required=int(years_required),
        )

    st.session_state['results']       = results
    st.session_state['monthly']       = monthly
    st.session_state['window_months'] = int(window_months)
    st.session_state['rise_pct']      = float(rise_pct)
    st.session_state['drop_pct']      = float(drop_pct)

# ── 顯示結果 ───────────────────────────────────────────────────────────────────
if 'results' not in st.session_state:
    st.info('設定好參數後，點左側「🔍 開始掃描」按鈕')
    st.stop()

results:       pd.DataFrame = st.session_state['results']
monthly:       pd.DataFrame = st.session_state['monthly']
win:           int          = st.session_state['window_months']
cur_rise_pct:  float        = st.session_state['rise_pct']
cur_drop_pct:  float        = st.session_state['drop_pct']

if results.empty:
    st.warning('未找到符合條件的股票，試試降低漲幅門檻或回落門檻，或將「需符合年數」改為 2')
    st.stop()

st.success(f'找到 **{len(results)}** 個規律')

# 顯示表格（隱藏 _year_rows）
DISPLAY_COLS = ['code', 'name', 'market', 'window', 'years_passed',
                'avg_rise_%', 'avg_drop_%', 'min_rise_%',
                '2023_rise_%', '2023_drop_%',
                '2024_rise_%', '2024_drop_%',
                '2025_rise_%', '2025_drop_%']
disp_df = results[[c for c in DISPLAY_COLS if c in results.columns]].copy()

COL_RENAME = {
    'code': '代號', 'name': '名稱', 'market': '市場',
    'window': '規律窗口', 'years_passed': '符合年數',
    'avg_rise_%': '平均漲%', 'avg_drop_%': '平均跌%', 'min_rise_%': '最小漲%',
    '2023_rise_%': '23漲%', '2023_drop_%': '23跌%',
    '2024_rise_%': '24漲%', '2024_drop_%': '24跌%',
    '2025_rise_%': '25漲%', '2025_drop_%': '25跌%',
}
disp_df = disp_df.rename(columns=COL_RENAME)

PCT_COLS = ['平均漲%', '平均跌%', '最小漲%',
            '23漲%', '23跌%', '24漲%', '24跌%', '25漲%', '25跌%']
col_cfg = {c: st.column_config.NumberColumn(c, format="%.2f")
           for c in PCT_COLS if c in disp_df.columns}

st.dataframe(
    disp_df,
    column_config=col_cfg,
    use_container_width=True,
    hide_index=True,
    height=min(400, 40 + len(disp_df) * 35),
)

# 匯出 CSV
export_df = results.drop(columns=['_year_rows'], errors='ignore')
st.download_button(
    '⬇️ 匯出 CSV',
    export_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
    'seasonal_patterns.csv',
    'text/csv',
    use_container_width=False,
)

st.divider()

# ── 走勢圖 ─────────────────────────────────────────────────────────────────────
st.subheader('📊 個股走勢圖')

options = [f"{r['code']} {r['name']} ｜ {r['window']}"
           for _, r in results.iterrows()]

selected = st.selectbox('選擇股票', options, label_visibility='collapsed')
if not selected:
    st.stop()

sel_idx   = options.index(selected)
row       = results.iloc[sel_idx]
ticker    = row['code'] + ('.TW' if row['market'] == 'TWSE' else '.TWO')
year_rows = row['_year_rows']

if ticker not in monthly.columns:
    st.warning(f'{ticker} 無價格資料')
    st.stop()

prices = monthly[ticker].dropna()

# ── 繪圖 ──────────────────────────────────────────────────────────────────────
RISE_STYLES = [
    ('rgba(76,175,80,0.18)',  '#2E7D32', '2E7D32'),
    ('rgba(33,150,243,0.18)', '#1565C0', '1565C0'),
    ('rgba(255,152,0,0.18)',  '#E65100', 'E65100'),
]
DROP_COLOR = 'rgba(229,57,53,0.15)'

fig = go.Figure()

fig.add_trace(go.Scatter(
    x=prices.index,
    y=prices.values,
    mode='lines+markers',
    name=f'{row["code"]} {row["name"]}',
    line=dict(color='#37474F', width=2),
    marker=dict(size=5, color='#37474F'),
    hovertemplate='%{x|%Y-%m}<br>收盤 %{y:.2f}<extra></extra>',
))

for i, yr in enumerate(year_rows):
    fill_c, line_c, _ = RISE_STYLES[i % len(RISE_STYLES)]

    def find_date(y, m):
        mask = (prices.index.year == y) & (prices.index.month == m)
        return prices.index[mask][0] if mask.any() else None

    d_start = find_date(yr['y1'], yr['m1'])
    d_end   = find_date(yr['yw'], yr['mw'])
    d_next  = find_date(yr['yn'], yr['mn'])

    rise_ok = yr['rise'] >= cur_rise_pct / 100
    drop_ok = yr['drop'] <= -cur_drop_pct / 100

    # 上漲窗口（綠/藍/橘底色）
    if d_start and d_end:
        fig.add_vrect(
            x0=d_start, x1=d_end,
            fillcolor=fill_c, opacity=1,
            layer='below', line_width=1, line_color=line_c,
            annotation_text=f"{yr['year']} +{yr['rise']*100:.1f}% {'✓' if rise_ok else '✗'}",
            annotation_position='top left',
            annotation_font=dict(color=line_c, size=11),
        )

    # 回落月（紅底色）
    if d_end and d_next:
        fig.add_vrect(
            x0=d_end, x1=d_next,
            fillcolor=DROP_COLOR, opacity=1,
            layer='below', line_width=1, line_color='rgba(229,57,53,0.5)',
            annotation_text=f"{yr['drop']*100:.1f}% {'✓' if drop_ok else '✗'}",
            annotation_position='top right',
            annotation_font=dict(color='#C62828', size=11),
        )

fig.update_layout(
    title=dict(text=f"{row['code']} {row['name']}　{row['window']}", font_size=15),
    xaxis_title='月份',
    yaxis_title='股價（還原）',
    height=460,
    hovermode='x unified',
    showlegend=False,
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis=dict(gridcolor='#ECEFF1', showgrid=True),
    yaxis=dict(gridcolor='#ECEFF1', showgrid=True),
    margin=dict(t=50, b=40, l=60, r=20),
)
st.plotly_chart(fig, use_container_width=True)

# ── 各年數字摘要 ──────────────────────────────────────────────────────────────
st.caption('各年度數據')
summary_rows = []
for yr in year_rows:
    rise_ok = yr['rise'] >= cur_rise_pct / 100
    drop_ok = yr['drop'] <= -cur_drop_pct / 100
    summary_rows.append({
        '年份':   yr['year'],
        '首月':   f"{MONTH_ZH[yr['m1']]}  {yr['p1']:.2f}",
        '末月':   f"{MONTH_ZH[yr['mw']]}  {yr['pw']:.2f}",
        '漲幅':   f"{yr['rise']*100:+.2f}%",
        '漲幅達標': '✅' if rise_ok else '❌',
        '隔月':   f"{MONTH_ZH[yr['mn']]}  {yr['pn']:.2f}",
        '跌幅':   f"{yr['drop']*100:+.2f}%",
        '跌幅達標': '✅' if drop_ok else '❌',
    })
st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)
