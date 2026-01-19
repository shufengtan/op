import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import sys

def check_data_age(_df):
    now = pd.Timestamp.now()
    _g = pd.concat([_df._.symbol, pd.to_datetime(_df.__.quote_dt, format='%m/%d/%Y %I:%M:%S%p'), pd.to_datetime(_df.__.load_dt)], axis=1).groupby('symbol')
    age = (now - _g.max()).map(lambda x: x.seconds).rename(columns={'quote_dt': 'quote_age', 'load_dt': 'load_age'})
    return age

def days_from_earning_reports(df_quotes):
    '''df_quotes from OptionFinder.get_quote_df()
    returns df with earningQtrReportDate and earningDays'''
    _df = df_quotes.loc[:, ['symbol', 'earningQtrReportDate']]
    today = pd.Timestamp.today().normalize()
    _df['earningDays'] = (pd.to_datetime(_df.earningQtrReportDate) - today).dt.days
    _df = _df[(_df.earningDays <= 60) & (_df.earningDays >= 0)].sort_values(by='earningDays')
    _df['earningDays'] = _df.earningDays.astype(int)
    return _df.set_index('symbol')

def plot_iv(dfcp, cluster_re=r'^otm_short', dte_lb=5, dte_ub=365):
    types = ['Calls', 'Puts']
    cluster_name = cluster_re.replace('^', '').replace('_', ' ')
    titles = [f'IV of {cluster_name} {_}' for _ in types]
    fig = make_subplots(rows=2, cols=1, subplot_titles=titles, shared_yaxes=True)
    fig.update_layout(height=800)
    for _i, _type in enumerate(types):
        _filter = (dfcp.type==_type[0]) & dfcp.cluster.str.contains(cluster_re) & (dfcp.dte >= dte_lb) & (dfcp.dte <= dte_ub)
        _dfiv = dfcp[_filter].groupby(['symbol']).agg(
            p10_iv = ('ImpVola', lambda x: x.quantile(0.1)),
            mean_iv = ('ImpVola', 'mean'),
            p90_iv = ('ImpVola', lambda x: x.quantile(0.9))).reset_index().sort_values(by='mean_iv', ascending=False)
        chart = px.bar(_dfiv, x='symbol', y=['p10_iv', 'mean_iv', 'p90_iv'], barmode='group')
        for trace in chart.data:
            fig.add_trace(trace, row=_i+1, col=1)
    fig.show()

def plot_theta_of_symbol(dfcp, symbol, dte_lb=5, dte_ub=365):
    _df = dfcp[(dfcp.symbol == symbol)]
    _df = _df[(_df.dte >= dte_lb) & (_df.dte <= dte_ub)]
    groupers = ['type', 'dte', 'cluster']
    metric = 'Theta'
    agg_list = ['mean', 'min', 'max']
    type_list = ['Calls', 'Puts']
    _dftheta = _df.groupby(groupers, observed=False)[metric].agg(agg_list).reset_index()
    titles=[f'{_a} theta of {symbol} {_t}' for _t in type_list for _a in agg_list]
    fig = make_subplots(rows=2, cols=3, subplot_titles=titles, shared_yaxes=True)#, horizontal_spacing=0.02)
    fig.update_layout(height=800)
    for _i, _type in enumerate(type_list):
        __df = _dftheta[_dftheta.type == _type[0]]
        for _j, _agg_col in enumerate(agg_list):
            chart = px.scatter(__df, x=groupers[1], y=_agg_col, color=groupers[2])
            for trace in chart.data:
                fig.add_trace(trace, row=_i+1, col=_j+1)
    fig.show()

def get_closest_value_in_column(df, col, target_value):
    _df = df.loc[:, [col]].drop_duplicates()
    tmp_diff_col = '__diff'
    _df[tmp_diff_col] = (_df[col] - target_value).abs()
    idx = _df[tmp_diff_col].idxmin()
    return _df.loc[idx, col].item()

def get_theta_curves(dfcp, symbol, strike):
    _df = dfcp[(dfcp.symbol == symbol) & (dfcp.strike == strike)].loc[:, ['type', 'dte', 'Theta']]
    _df = _df.pivot_table(values='Theta', index='dte', columns=['type'])
    return _df

def compute_dtz(dfcp, symbol, opt_type, dte, strike, debug=False):
    _df = dfcp[(dfcp.symbol == symbol) & (dfcp.dte==dte) & (dfcp['type']==opt_type) & (dfcp.strike==strike)].iloc[0]
    premium = _df.mid
    theta_curve = get_theta_curves(dfcp[(dfcp.dte <= dte)], symbol, strike)[opt_type]
    # Special case: only one dte
    if theta_curve.shape[0] == 1:
        theta = theta_curve.iloc[0]
        if theta == 0:
            return np.inf, np.inf, premium, premium
        dtz = premium/(0 - theta)
        dth = dtz/2
        if debug:
            print(f'Single dte {dte}: premium: {premium}, theta {theta}, dtz {dtz}, dth {dth}')
        if dtz <= dte:
            return dth, dtz, 0, premium
        resid = premium *(dte/dtz - 1)
        if dth <= dte:
            return dth, dte, resid, premium
        return np.nan, dte, resid, premium
    df_thc = theta_curve[theta_curve.index <= dte].T.reset_index().dropna().rename(columns={opt_type: 'theta'})
    df_thc['diff_dte'] = df_thc['dte'].diff()
    df_thc['avg_theta'] = df_thc['theta'].rolling(window=2).mean()
    df_thc['theta_decay'] = df_thc['diff_dte'] * df_thc['avg_theta']
    total_decay = df_thc['theta_decay'].sum()
    df_thc['decay_cumsum']  = df_thc['theta_decay'][::-1].cumsum()[::-1]
    df_thc['dt_'] = df_thc['diff_dte'][::-1].cumsum()[::-1]
    df_thc['half_resid'] = premium/2 + df_thc['decay_cumsum']
    df_thc['resid'] = premium + df_thc['decay_cumsum']
    def find_zero(resid_col):
        # resid_col is always ascending
        if df_thc[resid_col].dropna().iloc[0] >= 0:
            # all positive
            if debug:
                print(f'find_zero: {resid_col} all positive', df_thc[resid_col].to_dict())
            return dte
        if df_thc.iloc[-1][resid_col] <= 0:
            # all negative, must use the last row
            row = df_thc.iloc[-1]
            adj = row['diff_dte'] * row[resid_col]/row['theta_decay']
            if debug:
                print(f'find_zero: {resid_col} all negative: dt_ {row["dt_"]} - adj {adj}', _df.loc[:, ['dt_', 'resid']].set_index('dt_').to_dict())
            return row['dt_'] - adj
        else:
            neg_idx = df_thc[df_thc[resid_col] <= 0]['dte'].idxmax()
            pos_idx = df_thc[df_thc[resid_col] >= 0]['dte'].idxmin()
            if neg_idx == pos_idx:
                # Interpolation is not needed
                print(f'find_zero {resid_col} hit the jackpot on', symbol, opt_type, dte, strike)
                return df_thc.loc[neg_idx]['dt_']
            neg_row = df_thc.loc[neg_idx]
            pos_row = df_thc.loc[pos_idx]
            delta_dt_ = pos_row['dt_'] - neg_row['dt_']
            delta_resid = pos_row[resid_col] - neg_row[resid_col]
            adj = neg_row[resid_col] * delta_dt_ / delta_resid
            if debug:
                print(f'find_zero: {resid_col}: interpolate between neg_idx.max {neg_idx} and pos_idx.min {pos_idx}')
                print('delta dt_:', delta_dt_, f'delta {resid_col}:', delta_resid, 'base dt_:', neg_row['dt_'], 'adj:', adj)
            return neg_row['dt_'] - adj
    dth = find_zero('half_resid')
    if premium + total_decay > 0:
        if debug:
            print('dtz > dte', 'premium:', premium, 'dth:', dth, 'resid:', (premium + total_decay)/premium)
        else:
            return dth, dte, (premium + total_decay)/premium, premium
    else:
        dtz = find_zero('resid')
        if debug:
            print('dth =', dth, 'premium:', premium, 'dtz:', dtz)
        else:
            return dth, dtz, 0, premium
    return df_thc

def compute_all_dtz_for_symbol(dfcp, symbol, opt_type):
    df = dfcp[(dfcp.symbol==symbol) & (dfcp.type==opt_type)]
    dte_list = df.dte.unique()
    res = []
    for dte in dte_list:
        if dte == 0:
            continue
        dte = dte.item()
        strike_list = df[df.dte==dte].strike.unique()
        for strike in strike_list:
            strike = strike.item()
            _filter = (df.dte==dte) & (df.strike==strike)
            if df[_filter]['Bid'].iloc[0] == 0 or df[_filter]['OpenInterest'].iloc[0] == 0:
                #print('ignored', df[_filter].iloc[0, :-4].to_dict())
                continue
            if len(res) % 100 == 0:
                print('.', end='')
            dth, dtz, resid, premium = compute_dtz(df, symbol, opt_type, dte, strike)
            res.append({'symbol': symbol, 'dte': dte, 'strike': strike, 'dth': dth, 'dtz': dtz, 'resid': resid, 'premium': premium})
    return pd.DataFrame(res)

def get_rows_with_closest_value_in_column(df, col, target_value):
    groupers = [c for c in df.columns if c != col]
    df = df.drop_duplicates(subset=groupers + [col])
    tmp_diff_col = '__diff'
    df[tmp_diff_col] = (df[col] - target_value).abs()
    idx = df.groupby(groupers)[tmp_diff_col].idxmin() if len(groupers) > 0 else df.loc[:, [tmp_diff_col]].idxmin()
    return df.loc[idx].drop(columns=[tmp_diff_col])

def select_pds_deltas(dfcp, dte_lb, dte_ub):
    _df = dfcp[(dfcp.type=='P') & (dfcp.dte >= dte_lb) & (dfcp.dte <= dte_ub)]
    dfstrike_atm = get_rows_with_closest_value_in_column(_df.loc[:, ['symbol', 'dte', 'strike', 'lastPrice']], 'strike', _df.lastPrice).set_index(['symbol', 'dte'])
    _df = _df.loc[:, ['symbol', 'dte', 'Delta']]
    dfdelta_25 = get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.25], 'Delta', -0.25).set_index(['symbol', 'dte'])
    dfdelta_50 = get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.6],  'Delta', -0.5).set_index(['symbol', 'dte'])
    dfdelta_5  = get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.06], 'Delta', -0.05).set_index(['symbol', 'dte'])
    df_join = dfstrike_atm.join(dfdelta_25).join(dfdelta_50, rsuffix='_50').join(dfdelta_5, rsuffix='_5')
    return df_join.rename(columns={'strike': 'atm_strike', 'Delta': 'Delta_25'})

def put_debit_spread(dfpds, symbol, dte, dfcp, cols=['strike', 'Delta', 'mid', 'OpenInterest', 'ImpVola']):
    delta_dict = dfpds.loc[(symbol, dte)].to_dict()
    atm_strike = delta_dict.pop('atm_strike')
    deltas = list(delta_dict.values())
    _f = (dfcp.strike==atm_strike) & (dfcp.type=='P')
    for _d in deltas:
        _f = _f | (dfcp.Delta==_d)
    return dfcp[(dfcp.symbol==symbol) & (dfcp.dte==dte) & (_f)].loc[:, cols]

def plot_metric_subtotals_in_one_row(df, grouper, metric_list, shared_y=True, log_y_threshold=500, horizontal_spacing=0.02):
    fig = make_subplots(rows=1, cols=len(metric_list), subplot_titles=metric_list, shared_yaxes=shared_y, horizontal_spacing=horizontal_spacing)
    fig.update_layout(height=400)
    _df = df.loc[:, grouper + metric_list].groupby(grouper).sum().reset_index()
    if shared_y and _df.loc[:, metric_list].max().max() >= log_y_threshold*_df.loc[:, metric_list].quantile(0.01).min():
        fig.update_yaxes(type="log")
        sys.stderr.write('y axes in log\n')
    for j, metric in enumerate(metric_list):
        if len(grouper) > 1:
            chart = px.bar(_df, x=grouper[0], y=metric, color=grouper[-1])
        else:
            chart = px.bar(_df, x=grouper[0], y=metric)
        for trace in chart.data:
            fig.add_trace(trace, row=1, col=j+1)
    fig.show()
    return _df

def plot_metrics_in_one_row(df, groupers, metric_list, shared_y=True, log_y_threshold=500, horizontal_spacing=0.02):
    fig = make_subplots(rows=1, cols=len(metric_list), subplot_titles=metric_list, shared_yaxes=shared_y, horizontal_spacing=horizontal_spacing)
    fig.update_layout(height=400)
    if shared_y and df.loc[:, metric_list].max().max() >= log_y_threshold*df.loc[:, metric_list].quantile(0.01).min():
        fig.update_yaxes(type="log")
        sys.stderr.write('y axes in log\n')
    for j, metric in enumerate(metric_list):
        if len(groupers) > 1:
            chart = px.bar(df, x=groupers[0], y=metric, color=groupers[-1])
        else:
            chart = px.bar(df, x=groupers[0], y=metric)
        for trace in chart.data:
            fig.add_trace(trace, row=1, col=j+1)
    fig.show()

def calc_overall_put_call_ratios(df):
    metrics = ['OpenInterest', 'Volume']
    p = df.pivot_table(index='symbol', columns='type', values=metrics, aggfunc='sum', observed=False, fill_value=0)
    return pd.DataFrame(dict([(_v + '_P_C_ratio', p.loc[:, (_v, 'P')]/p.loc[:, (_v, 'C')]) for _v in metrics])).reset_index()

def calc_cluster_put_call_ratios(dfcp):
    groupers = ['symbol', 'cluster', 'dte_cluster', 'type']
    metrics = ['OpenInterest', 'Volume']
    _df = dfcp.loc[:, groupers + metrics].groupby(groupers, observed=False).sum().reset_index()
    _df = _df.pivot(columns='type', index=groupers[:-1])
    _df = pd.DataFrame({'OpenInterest_P_C_ratio': _df.OpenInterest.P/_df.OpenInterest.C, 'Volume_P_C_ratio': _df.Volume.P/_df.Volume.C})
    return _df.reset_index()
    
def plot_metrics_by_moneyness_and_dte_clusters(df, height=240, vertical_spacing=0.03, shared_yaxes=True):
    symlist = df.symbol.unique()
    metrics = [c for c in df.columns if c not in ['symbol', 'cluster', 'dte_cluster', 'type']]
    titles = [f'{symbol} {metric}' for symbol in symlist for metric in metrics]
    nr = len(symlist)
    nc = len(metrics)
    fig = make_subplots(rows=nr, cols=nc, subplot_titles=titles, horizontal_spacing=0.01, vertical_spacing=vertical_spacing, shared_yaxes=shared_yaxes)
    fig.update_layout(showlegend=False, height=nr*height)
    for i, symbol in enumerate(symlist):
        for j, metric in enumerate(metrics):
            #print(symbol, i, metric, j)
            chart = px.bar(df[df.symbol == symbol], x='dte_cluster', y=metric, barmode='group', color='cluster')
            for trace in chart.data:
                fig.add_trace(trace, row=i+1, col=j+1)
    fig.show()

def add_moneyness_columns(df, atm_offset=0.01, otm_offset=0.1):
    df['moneyness'] = df['strike'] / df['lastPrice']
    bins = [0, 1-otm_offset, 1 - atm_offset, 1 + atm_offset, 1 + otm_offset, np.inf]
    labels = ['deep_otm_short', 'otm_short', 'atm', 'otm_long', 'deep_otm_long']
    df['cluster'] = pd.cut(df['moneyness'], bins=bins, labels=labels)
    return df

def bucketize_dte(df, bins=[0, 7, 56, 91, 182, 364, np.inf]):
    labels = ['1wk', '8wk', '13wk', '6mo', '1yr', '>1yr']
    df['dte_cluster'] = pd.cut(df['dte'], bins=bins, labels=labels, right=False)
    return df

def aggregate_metrics_by_moneyness_and_dte_clusters(df, metrics, method='mean', oi_lb=None, bid_lb=0):
    groupers = ['symbol', 'cluster', 'dte_cluster']
    if 'type' in df.columns:
        groupers.append('type')
    _filter  = df.Bid >= bid_lb
    if oi_lb:
        _filter = _filter & (df.OpenInterest >= oi_lb)
    if type(metrics) is str:
        metrics = [metrics]
    g = df.loc[:, groupers + metrics][_filter].groupby(groupers, observed=False)
    if method == 'mean':
        return g.mean().reset_index()
    elif method == 'sum':
        return g.sum().reset_index()
    elif method == 'median':
        return g.quantile(0.5).reset_index()
    else:
        raise Exception('Unknown aggregation method.')

def put_call_ratios_by_moneyness(df, metric='Volume', by_dte=True):
    # Assume df already has moneyness cluster, option type
    idx_cols = ['symbol', 'dte', 'cluster'] if by_dte else ['symbol', 'cluster']
    pivot = df.pivot_table(index=idx_cols, columns='type', values=metric, aggfunc='sum', observed=False, fill_value=0)
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
    titles=[f'{symbol} {"~".join((lambda x: [str(x[0]), str(x[-1])])(sorted(_df.loc[symbol].dte.unique())))} DTE' for symbol in _symlist]
    fig = make_subplots(rows=nr, cols=nc, subplot_titles=titles, horizontal_spacing=0.01, vertical_spacing=0.05, shared_xaxes=True, shared_yaxes=True)
    fig.update_layout(height=nr*300, showlegend=False)
    __df = _df.reset_index()
    for i, symbol in enumerate(_symlist):
        _dte = sorted(_df.loc[symbol].dte.unique())
        print(symbol, len(_dte), 'DTEs', [int(_) for _ in _dte])
        chart = px.scatter(__df[__df.symbol==symbol], x='overpaid', y='leverage', color='expDt')
        for trace in chart.data:
            fig.add_trace(trace, row=i//nc+1, col=i%nc+1)
    fig.show()
    return _df
