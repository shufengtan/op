import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def check_data_age(_df):
    now = pd.Timestamp.now()
    _g = pd.concat([_df._.symbol, pd.to_datetime(_df.__.quote_dt, format='%m/%d/%Y %I:%M:%S%p'), pd.to_datetime(_df.__.load_dt)], axis=1).groupby('symbol')
    age = (now - _g.max()).map(lambda x: x.seconds).rename(columns={'quote_dt': 'quote_age', 'load_dt': 'load_age'})
    return age

def plot_option_volume_openinterest(df):
    metric_list = ['Volume', 'OpenInterest']
    fig = make_subplots(rows=1, cols=len(metric_list), subplot_titles=metric_list)
    grouper = ['symbol', 'type']
    _df = df.loc[:, grouper + metric_list].groupby(grouper).sum().reset_index()
    for i, metric in enumerate(metric_list):
        chart = px.bar(_df, x='symbol', y=metric, color='type')
        for trace in chart.data:
            fig.add_trace(trace, row=1, col=i+1)
    fig.show()
    return _df

def overall_put_call_ratios(df, dte_range=None):
    if dte_range is not None:
        df = df[(df.dte >= dte_range[0]) & (df.dte <= dte_range[1])]
    values = ['Volume', 'OpenInterest']
    p = df.pivot_table(index='symbol', columns='type', values=values, aggfunc='sum', observed=True, fill_value=0)
    return pd.DataFrame(dict([(_v, p.loc[:, (_v, 'P')]/p.loc[:, (_v, 'C')]) for _v in values]))

def plot_put_call_ratios(df, dte_range):
    fig = make_subplots(rows=1, cols=2, subplot_titles=['All DTEs', f'DTE {dte_range}'])
    df_all = overall_put_call_ratios(df)
    df1 = overall_put_call_ratios(df, dte_range)
    chart1 = px.bar(df_all.sort_values(by='Volume'))
    chart2 = px.bar(df1.sort_values(by='Volume'))
    for trace in chart1.data:
        fig.add_trace(trace, row=1, col=1)
    for trace in chart2.data:
        fig.add_trace(trace, row=1, col=2)
    fig.show()
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
    symbol = list(df_moneyness.symbol)[0]
    _df = df_moneyness[df_moneyness.symbol==symbol]
    _df_v = put_call_ratios_by_moneyness(_df, metric='Volume', by_dte=True).reset_index()
    _df_o = put_call_ratios_by_moneyness(_df, metric='OpenInterest', by_dte=True).reset_index()
    _df = pd.merge(_df_v, _df_o, on=['symbol', 'dte'], how='inner', suffixes=('-Volume', '-OpenInterest'))
    _df = _df.set_index(['symbol', 'dte'])
    _df.columns = pd.MultiIndex.from_tuples([tuple(_.split('-')) for _ in _df.columns], names=['moneyness', 'metric'])
    _df = _df.stack(level=1, future_stack=True)
    clusters = df_moneyness.cluster.unique()[1:-1]
    _df = _df[clusters].reset_index()
    titles = [f'{symbol} {_} put/call ratio' for _ in clusters]
    fig = make_subplots(rows=1, cols=len(clusters), subplot_titles=titles, shared_yaxes=True)
    fig.update_layout(showlegend=False)
    for j, moneyness in enumerate(clusters):
        chart = px.bar(_df, x='dte', y=moneyness, barmode='group', color='metric')
        for trace in chart.data:
            fig.add_trace(trace, row=1, col=j+1)
    fig.show()
    return _df

def plot_option_details_by(df_cp, symbol, metric, by, offset=0, delta_lb=0.5, delta_ub=0.9, nr=4, nc=2):
    _df_cp = df_cp[df_cp.symbol == symbol]
    by_list = sorted(_df_cp[by].unique())
    spot = _df_cp.lastPrice.min()
    if by == 'dte':
        by2expdt = _df_cp.loc[:, ['dte', 'expDt']].groupby('dte').first()['expDt'].to_dict()
        by_list = by_list[offset:offset+nr*nc]
        other = 'strike'
    elif by == 'strike':
        by2expdt = {}
        idx0 = by_list.index(max([_ for _ in by_list if _ <= spot])) - nr*nc//2 + offset
        idx0 = 0 if idx0 < 0 else idx0
        by_list = by_list[idx0:idx0+nr*nc]
        other = 'dte'
    else:
        raise Exception("Can only handle by='strike' or by='dte'")
    print(by, by_list)
    titles=[f'{metric} vs {other} of {symbol} @{spot} {by}: {_} {by2expdt.get(_, "")}' for _ in by_list]
    fig = make_subplots(rows=nr, cols=nc, subplot_titles=titles, horizontal_spacing=0.05, vertical_spacing=0.05)
    fig.update_layout(height=nr*300)
    for i, _by in enumerate(by_list):
        c_filter = (_df_cp.type == 'C') & (_df_cp[by] == _by)# & (_df_cp.Delta >= delta_lb) & (_df_cp.Delta <= delta_ub)
        df_c = _df_cp[c_filter]
        other_min = df_c[other].min()
        other_max = df_c[other].max()
        #print('other =', other, df_c.shape, other_min, other_max)
        p_filter = (_df_cp.type == 'P') & (_df_cp[by] == _by) & (_df_cp[other] <= other_max) & (_df_cp[other] >= other_min)
        _df = _df_cp[c_filter | p_filter]
        chart = px.scatter(_df, x=other, y=metric, color='type')
        for trace in chart.data:
            fig.add_trace(trace, row=i//nc + 1, col=i%nc+1)
    fig.show()
    return _df_cp

def plot_leverage_overpaid(df, delta_lb=0.5, overpaid_ub=0.1, price_lb=5, spread_ub=10, leverage_lb=2, openinterest_lb=10):
    _filter = (df.Delta >= delta_lb) & (df.overpaid <= overpaid_ub) & (df.mid >= price_lb) & (df.pctSpread <= spread_ub)
    _filter = _filter & (df.leverage >= leverage_lb) & (df.OpenInterest >= openinterest_lb)
    _df = df[_filter].set_index(['symbol', 'expDt']).sort_values(by='leverage', ascending=False)
    _symlist = list(_df.index.get_level_values('symbol').unique())
    nr = 1 if len(_symlist) <= 4 else int(np.ceil(len(_symlist)/4))
    nc = int(np.ceil(len(_symlist)/nr))
    #plt.rcParams['figure.figsize'] = (nc*5, nr*5)
    #fig, ax = plt.subplots(nr, nc)
    titles=[f'{symbol} {"~".join((lambda x: [x[0], x[-1]])(sorted(_df.loc[symbol].dte.astype(str).unique())))}' for symbol in _symlist]
    fig = make_subplots(rows=nr, cols=nc, subplot_titles=titles, horizontal_spacing=0.05, vertical_spacing=0.05)
    fig.update_layout(height=nr*300)
    for i, symbol in enumerate(_symlist):
        _dte = sorted(_df.loc[symbol].dte.unique())
        print(symbol, len(_dte), 'DTEs', _dte)
        chart = px.scatter(_df.xs(symbol, level='symbol'), x='overpaid', y='leverage')
        for trace in chart.data:
            fig.add_trace(trace, row=i//nc+1, col=i%nc+1)
    fig.show()
    return _df
