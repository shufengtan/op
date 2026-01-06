#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import yfinance as yf
import re
import sys
import os
LAB_DIR=os.path.expanduser('~/lab')
sys.path.append(LAB_DIR)

def gather_tickers():
    symlist = pd.read_csv(os.path.join(LAB_DIR, 'components/sp500.csv')).symbol.to_list()
    #### yf uses '-' instead of '.'
    symlist = [_.replace('.', '-') for _ in symlist]
    print(f'gather_tickers: {len(symlist)} symbols from SP500')
    nasdaq100 = pd.read_csv(os.path.join(LAB_DIR, 'components/nasdaq100.csv')).Ticker.to_list()
    for ticker in nasdaq100:
        if ticker not in symlist:
            symlist.append(ticker)
    print(f'gather_tickers: {len(symlist)} symbols after adding NASDAQ 100')

    with open(os.path.join(LAB_DIR, 'symbols.txt')) as fo:
        for x in fo:
            s = x.rstrip()
            if s not in symlist:
                symlist.append(s)
    print(f'gather_tickers: {len(symlist)} symbols with symbols.txt')

    for x in 'VOO VEU IXUS SVIX XLB XLC XLE XLF XLI XLK XLP XLU XLV XLY VNQ SMCI ARM HIMS PLTR'.split(' '):
        if x not in symlist:
            symlist.append(x)
    print(f'gather_tickers: {len(symlist)} symbols finally')
    return symlist

def select_date_range(end_date=None, days_offset=265):
    today = pd.Timestamp.today()
    if end_date is None:
        end_date = today
        if end_date.hour < 16:
            print('end_date is today, before market close:', end_date, end=' ')
            end_date -= pd.to_timedelta('1d')
            print('adjusted to', end_date)
    else:
        end_date = pd.to_datetime(end_date)
    if end_date.weekday() >= 5:
        print('end_date is on weekend:', end_date.weekday())
        end_date = end_date - pd.tseries.offsets.BDay(1)
    print('End date:', end_date.strftime('%F %A'))
    start_date = end_date - pd.tseries.offsets.BDay(days_offset)
    print('Start date:', start_date.strftime('%F %A'))
    return start_date, end_date

def drop_nan_columns(df, days):
    nan_columns = df.columns[df.isna().any()].to_list()
    if len(nan_columns) > 0:
        print(f'Dropping symbols for days={days}:', nan_columns)
        return df.drop(columns=nan_columns)
    return df

def compute_hv(df_yf):
    dodf = {}
    for days in [10, 20, 30, 60, 90, 120, 180]:
        df_close = df_yf.Close.tail(days + 1)
        df_close = drop_nan_columns(df_close, days)
        df_return = np.log(df_close/df_close.shift(1)).dropna()
        dodf[days] = df_return.iloc[range(-days, 0)].std()*np.sqrt(252)
    return pd.DataFrame(dodf)

def compute_ma(df_yf):
    d_o_ma = {}
    for days in [200, 50, 10]:
        df_close = df_yf.Close.tail(days)
        df_close = drop_nan_columns(df_close, days)
        d_o_ma[f'MA{days}'] = df_close.mean()
    return pd.DataFrame(d_o_ma)

def calculate_bollinger_bands(prices, window=20, num_std=2):
    """
    Calculate Bollinger Bands.
    
    :param prices: Pandas Series of closing prices.
    :param window: Moving average window size (default 20).
    :param num_std: Number of standard deviations (default 2).
    :return: DataFrame with columns: Close, MA, Upper Band, Lower Band
    """
    ma = prices.rolling(window).mean()
    std = prices.rolling(window).std()
    upper_band = ma + (num_std * std)
    lower_band = ma - (num_std * std)

    bollinger_df = pd.DataFrame({
        'Close': prices,
        'MA': ma,
        'UB': upper_band,
        'LB': lower_band
    })
    return bollinger_df

def assemble_indicator_df(df_yf):
    dfma = compute_ma(df_yf)

    _df20 = df_yf.tail(20)
    _dfv = _df20.Volume * (_df20.Open + _df20.Close)/2
    dfv = pd.concat([_dfv.mean(), _dfv.std()], axis=1)
    dfv.columns = ['Volume_Avg', 'Volume_Std']
    dfv['Volume_Std'] = dfv.Volume_Std / dfv.Volume_Avg

    l_o_df_b = []
    for symbol in df_yf.Close.columns:
        _dfc = df_yf.Close[symbol].dropna()
        if _dfc.shape[0] == 0:
            print(f'Skipping {symbol}: no close data.')
            continue
        if _dfc.index[-1] != df_yf.index[-1]:
            print(f'Skipping {symbol} as its last date {_dfc.index[-1]} != {df_yf.index[-1]}')
            continue
        _df20 = calculate_bollinger_bands(_dfc.tail(30), window=20, num_std=2).tail(1).copy().drop(columns=['Close'])
        _df30 = calculate_bollinger_bands(_dfc.tail(45), window=30, num_std=2).tail(1).copy().drop(columns=['Close'])
        _df20['symbol'] = symbol
        _df30['symbol'] = symbol
        _df = _df20.reset_index().set_index(['Date', 'symbol']).join(_df30.reset_index().set_index(['Date', 'symbol']), how='inner', lsuffix='20', rsuffix='30')
        l_o_df_b.append(_df)
    _dfb = pd.concat(l_o_df_b)

    #### Make sure the index has one Timestamp

    if len(set([_[0] for _ in _dfb.index])) == 1:
        _dfb.index = [_[1] for _ in _dfb.index]
    else:
        raise Exception(f'Unexpected:the index has multiple timestamp {set([_[0] for _ in _dfb.index])}')

    df = _dfb.join(dfma).join(dfv)
    cols = df.columns
    cols1 = sorted([c for c in cols if re.search(r'[23]0$', c)], key=lambda x: x[2:]+x[:2])
    cols2 = sorted([c for c in cols if re.search(r'(?:10|50|200)', c)], key=lambda x: (x[0], int(re.findall(r'(\d+)', x)[0])))
    columns = cols1 + cols2 + [c for c in cols if c not in cols1+cols2]
    df = df[columns]
    df = df.reset_index().rename(columns={'index': 'symbol'})
    return df

if __name__ == '__main__':
    symlist = gather_tickers()
    end_date = sys.argv[1] if len(sys.argv) >= 2 else None
    print('end_date:', end_date)
    start_date, end_date = select_date_range(end_date=end_date, days_offset=265)
    df_yf = yf.download(symlist, start=start_date, end=end_date, progress=False, auto_adjust=True)

    dfhv = compute_hv(df_yf)
    hv_csv_file = end_date.strftime(f'{LAB_DIR}/output/hv-%F.csv')
    print('HV output file:', hv_csv_file)
    dfhv.map(lambda x: f'{x:.4f}').to_csv(hv_csv_file)

    df = assemble_indicator_df(df_yf)
    csv_file = end_date.strftime(f'{LAB_DIR}/output/bollinger_bands-%F.csv')
    print('Bollinger bands output file:', csv_file)
    for col in df.columns:
        if col == 'Volume_Avg':
            df[col] = df[col].apply(lambda x: f'{x:.0f}')
        elif col != 'symbol':
            df[col] = df[col].apply(lambda x: f'{x:.2f}')
    df.to_csv(csv_file, index=None)
