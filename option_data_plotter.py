import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def check_data_age(_df):
    now = pd.Timestamp.now()
    _g = pd.concat([_df._.symbol, pd.to_datetime(_df.__.quote_dt, format='%m/%d/%Y %I:%M:%S%p'), pd.to_datetime(_df.__.load_dt)], axis=1).groupby('symbol')
    age = (now - _g.max()).map(lambda x: x.seconds).rename(columns={'quote_dt': 'quote_age', 'load_dt': 'load_age'})
    age_range = (_g.max() - _g.min()).map(lambda x: x.seconds)
    return age.join(age_range)

def plot_option_volume_openinterest(df):
    plt.rcParams['figure.figsize'] = (20, 4)
    fig, ax = plt.subplots(1, 2)
    grouper = ['symbol', 'type']
    metric_list = ['Volume', 'OpenInterest']
    _df = df.loc[:, grouper + metric_list].groupby(grouper).sum().unstack(level=1)
    for i, metric in enumerate(metric_list):
        _df.loc[:, metric].plot(kind='bar', ax=ax[i], xlabel="", title=metric)
    return _df

def overall_put_call_ratios(df, dte_range=None):
    if dte_range is not None:
        df = df[(df.dte >= dte_range[0]) & (df.dte <= dte_range[1])]
    values = ['Volume', 'OpenInterest']
    p = df.pivot_table(index='symbol', columns='type', values=values, aggfunc='sum', observed=True, fill_value=0)
    return pd.DataFrame(dict([(_v, p.loc[:, (_v, 'P')]/p.loc[:, (_v, 'C')]) for _v in values]))
        
def plot_put_call_ratios(df, dte_range):
    plt.rcParams['figure.figsize'] = (20, 4)
    fig, ax = plt.subplots(1, 2)
    df_all = overall_put_call_ratios(df)
    df1 = overall_put_call_ratios(df, dte_range)
    df_all.sort_values(by='Volume').plot(kind='bar', ax=ax[0], title='All DTE')
    df1.sort_values(by='Volume').plot(kind='bar', ax=ax[1], title=f'DTE {dte_range}')
    return df_all.join(df1, lsuffix='_all')

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
    pivot = pivot[pivot.C + pivot.P >= 10]
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
    print('red: put, green: call')
    dte2expdt = _df_cp.loc[:, ['dte', 'expDt']].groupby('dte').first()['expDt'].to_dict()
    for i, _dte in enumerate(dte_list[:nr*nc]):
        df_c = _df_cp[(_df_cp.type == 'C') & (_df_cp.dte == _dte) & (_df_cp.Delta >= delta_lb) & (_df_cp.Delta <= delta_ub)]
        strike_min = df_c.strike.min()
        strike_max = df_c.strike.max()
        df_p = _df_cp[(_df_cp.type == 'P') & (_df_cp.dte == _dte) & (_df_cp.strike <= strike_max) & (_df_cp.strike >= strike_min)]
        df_p.plot(kind='scatter', x='strike', y=metric, ax=ax[i//nc, i%nc], color='red')
        df_c.plot(kind='scatter', x='strike', y=metric, ax=ax[i//nc, i%nc], color='green', title=f'{symbol} {dte2expdt[_dte]} {spot} dte: {_dte}')

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
    print('red: put, green: call')
    for i, strike in enumerate(strike_list):
        df_c = _df_cp[(_df_cp.type == 'C') & (_df_cp.strike == strike)]
        df_p = _df_cp[(_df_cp.type == 'P') & (_df_cp.strike == strike)]
        df_p.plot(kind='scatter', x='dte', y=metric, ax=ax[i//nc, i%nc], color='red')
        df_c.plot(kind='scatter', x='dte', y=metric, ax=ax[i//nc, i%nc], color='green', title=f'{symbol} {_df_cp.lastPrice.min()} strike: {strike}')

def plot_leverage_overpaid(df, delta_lb=0.5, overpaid_ub=0.1, price_lb=5, spread_ub=10, leverage_lb=2, openinterest_lb=10):
    _filter = (df.Delta >= delta_lb) & (df.overpaid <= overpaid_ub) & (df.mid >= price_lb) & (df.pctSpread <= spread_ub)
    _filter = _filter & (df.leverage >= leverage_lb) & (df.OpenInterest >= openinterest_lb)
    _df = df[_filter].set_index(['symbol', 'expDt']).sort_values(by='leverage', ascending=False)
    _symlist = list(_df.index.get_level_values('symbol').unique())
    nr = 1 if len(_symlist) <= 4 else int(np.ceil(len(_symlist)/4))
    nc = int(np.ceil(len(_symlist)/nr))
    plt.rcParams['figure.figsize'] = (nc*5, nr*5)
    fig, ax = plt.subplots(nr, nc)
    for i, symbol in enumerate(_symlist):
        _dte = sorted(_df.loc[symbol].dte.unique())
        print(symbol, len(_dte), 'DTEs', _dte)
        _title = f'{symbol} dte: {min(_dte)}~{max(_dte)}'
        if nr == 1:
            _df.xs(symbol, level='symbol').plot(kind='scatter', x='overpaid', y='leverage', ax=ax if nc==1 else ax[i%nc], title=_title)
        else:
            _df.xs(symbol, level='symbol').plot(kind='scatter', x='overpaid', y='leverage', ax=ax[i//nc, i%nc], title=_title)
    return _df
