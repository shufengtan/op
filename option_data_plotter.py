import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def check_data_age(_df):
    now = pd.Timestamp.now()
    _g = pd.concat([_df._.symbol, pd.to_datetime(_df.__.quote_dt, format='%m/%d/%Y %I:%M:%S%p'), pd.to_datetime(_df.__.load_dt)], axis=1).groupby('symbol')
    age = (now - _g.max()).map(lambda x: x.seconds).rename(columns={'quote_dt': 'quote_age', 'load_dt': 'load_age'})
    age_range = (_g.max() - _g.min()).map(lambda x: x.seconds)
    return age.join(age_range)

def add_moneyness_columns(df, atm_offset=0.01, otm_offset=0.1):
    df['moneyness'] = df['strike'] / df['lastPrice']
    bins = [0, 1-otm_offset, 1 - atm_offset, 1 + atm_offset, 1 + otm_offset, np.inf]
    labels = ['deep_otm_put', 'otm_put', 'atm', 'otm_call', 'deep_otm_call']
    df['cluster'] = pd.cut(df['moneyness'], bins=bins, labels=labels)
    return df

def put_call_ratios_by_moneyness(df, metric='Volume', by_dte=True):
    # Assume df already has moneyness cluster, option type
    idx_cols = ['symbol', 'dte', 'cluster'] if by_dte else ['symbol', 'cluster']
    pivot = df.pivot_table(index=idx_cols, columns='type', values=metric, aggfunc='sum', observed=True, fill_value=0)
    return pd.DataFrame({'put_call_ratio': pivot['P'] / pivot['C']}).unstack(level=2 if by_dte else 1).put_call_ratio

def plot_put_call_ratios_by_moneyness(df_moneyness):
    _df_v = put_call_ratios_by_moneyness(df_moneyness, metric='Volume', by_dte=True).reset_index()
    _df_o = put_call_ratios_by_moneyness(df_moneyness, metric='OpenInterest', by_dte=True).reset_index()
    _df = pd.merge(_df_v, _df_o, on=['symbol', 'dte'], how='inner', suffixes=('_v', '_oi'))
    sym_list = sorted(_df.symbol.unique())
    nr = len(sym_list)
    nc = 3
    plt.rcParams['figure.figsize'] = (20, nr*5)
    fig, ax = plt.subplots(nr, nc)
    for i, symbol in enumerate(sym_list):
        _dfi = _df[_df.symbol == symbol].drop(columns='symbol').set_index('dte')
        for j, moneyness in enumerate(['otm_put', 'atm', 'otm_call']):
            _title = f"{symbol} {moneyness} put/call ratios"
            cols = [c for c in _dfi.columns if c[:len(moneyness)] == moneyness]
            if nr == 1:
                _dfi.loc[:, cols].plot(kind='bar', ax=ax[j], title=_title)
            else:
                _dfi.loc[:, cols].plot(kind='bar', ax=ax[i, j], title=_title, xlabel="")
    return _df

def plot_option_details_by_dte(df_cp, symbol, metric, delta_lb=0.5, delta_ub=0.9, nr=4, nc=2):
    _df_cp = df_cp[df_cp.symbol == symbol]
    dte_list = sorted(_df_cp.dte.unique())
    spot = _df_cp.lastPrice.min()
    plt.rcParams['figure.figsize'] = (24, 5*nr)
    fig, ax = plt.subplots(nr, nc)
    print('orange: put, blue: call')
    for i, _dte in enumerate(dte_list[:nr*nc]):
        df_c = _df_cp[(_df_cp.type == 'C') & (_df_cp.dte == _dte) & (_df_cp.Delta >= delta_lb) & (_df_cp.Delta <= delta_ub)]
        strike_min = df_c.strike.min()
        strike_max = df_c.strike.max()
        df_p = _df_cp[(_df_cp.type == 'P') & (_df_cp.dte == _dte) & (_df_cp.strike <= strike_max) & (_df_cp.strike >= strike_min)]
        df_p.plot(kind='scatter', x='strike', y=metric, ax=ax[i//nc, i%nc], color='orange')
        df_c.plot(kind='scatter', x='strike', y=metric, ax=ax[i//nc, i%nc], title=f'{symbol} {spot} dte: {_dte}')

def plot_option_details_by_strike(df_cp, symbol, metric, moneyness_range):
    _df_cp = df_cp[df_cp.symbol == symbol]
    spot = _df_cp.lastPrice.min()
    strike_lb = spot * moneyness_range[0]
    strike_ub = spot * moneyness_range[-1]
    _df_cp = _df_cp[(_df_cp.strike >= strike_lb) & (_df_cp.strike <= strike_ub)]
    strike_list = sorted(_df_cp.strike.unique())
    nr = 1 if len(strike_list) <= 4 else 1 + len(strike_list)//4
    nc = int(np.ceil(len(strike_list)/nr))
    plt.rcParams['figure.figsize'] = (24, 5*nr)
    fig, ax = plt.subplots(nr, nc)
    print('orange: put, blue: call')
    for i, strike in enumerate(strike_list):
        df_c = _df_cp[(_df_cp.type == 'C') & (_df_cp.strike == strike)]
        df_p = _df_cp[(_df_cp.type == 'P') & (_df_cp.strike == strike)]
        df_p.plot(kind='scatter', x='dte', y=metric, ax=ax[i//nc, i%nc], color='orange')
        df_c.plot(kind='scatter', x='dte', y=metric, ax=ax[i//nc, i%nc], title=f'{symbol} {_df_cp.lastPrice.min()} strike: {strike}')