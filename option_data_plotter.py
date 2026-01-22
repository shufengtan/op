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

def count_days_from_earning_reports(df_quotes):
    '''df_quotes from OptionFinder.get_quote_df()
    returns df with earningQtrReportDate and earningDays'''
    _df = df_quotes.loc[:, ['symbol', 'earningQtrReportDate']]
    today = pd.Timestamp.today().normalize()
    _df['earningDays'] = (pd.to_datetime(_df.earningQtrReportDate) - today).dt.days
    _df = _df[(_df.earningDays <= 60) & (_df.earningDays >= 0)].sort_values(by='earningDays')
    _df['earningDays'] = _df.earningDays.astype(int)
    return _df.set_index('symbol')

def plot_iv_statistics(dfcp, cluster_re=r'^otm_short', dte_lb=5, dte_ub=365):
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

def plot_theta_curves_by_moneyness_cluster(dfcp, symbol, dte_lb=5, dte_ub=365):
    _df = dfcp[(dfcp.symbol == symbol)]
    _df = _df[(_df.dte >= dte_lb) & (_df.dte <= dte_ub)]
    groupers = ['type', 'dte', 'cluster']
    metric = 'Theta'
    agg_list = ['mean', 'min', 'max']
    type_list = ['Calls', 'Puts'] # Full words for title, first character for actual search
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

def get_theta_curve(dfcp, symbol, strike, opt_type):
    _df = dfcp[(dfcp.symbol == symbol) & (dfcp.strike == strike) & (dfcp.type == opt_type)].loc[:, ['dte', 'Theta']]
    df_thc = _df.pivot_table(values='Theta', index='dte').reset_index()
    return df_thc

def prepare_theta_curve(df_thc, dte, premium):
    df_thc['diff_dte'] = df_thc['dte'].diff()
    df_thc['avg_theta'] = df_thc['Theta'].rolling(window=2).mean()
    df_thc['theta_decay'] = df_thc['diff_dte'] * df_thc['avg_theta']
    df_thc['decay_cumsum']  = df_thc['theta_decay'][::-1].cumsum()[::-1]
    df_thc['dt_'] = df_thc['diff_dte'][::-1].cumsum()[::-1]
    df_thc['half_dte'] = df_thc['dt_'] - np.ceil(dte/2)
    df_thc['half_resid'] = premium/2 + df_thc['decay_cumsum']
    df_thc['resid'] = premium + df_thc['decay_cumsum']
    return df_thc

def find_zero_resid(df_thc, resid_col, dte, debug=False):
    # resid_col is always ascending
    if df_thc[resid_col].iloc[0] >= 0:
        # all positive
        if debug:
            print(f'find_zero_resid: {resid_col} all positive', df_thc[resid_col].to_dict())
        return dte
    if df_thc.iloc[-1][resid_col] <= 0:
        # all negative, must use the last row
        row = df_thc.iloc[-1]
        adj = row['diff_dte'] * row[resid_col]/row['theta_decay']
        if debug:
            print(f'find_zero_resid: {resid_col} all negative: dt_ {row["dt_"]} - adj {adj}', df_thc.loc[:, ['dt_', 'resid']].set_index('dt_').to_dict())
        return row['dt_'] - adj
    else:
        days_to_zero = trap_zero(df_thc, resid_col, 'dt_', debug)
        return days_to_zero

def trap_zero(df_thc, col_to_zero, col_to_return, debug):
    '''
    Example 1: col_to_zero: 'resid', col_to_return: 'dt_'
    Example 2: col_to_zero: 'half_dte', col_to_return: 'resid'
    '''
    if df_thc[df_thc[col_to_zero]==0].shape[0] > 0:
        return df_thc[df_thc[col_to_zero]==0][col_to_return].iloc[0]
    p_first = df_thc[col_to_zero].iloc[0]
    p_last  = df_thc[col_to_zero].iloc[-1]
    if p_last > p_first and p_last >= 0 and p_first < 0:
        # col_to_zero is ascending and sign switch: interpolation
        neg_idx = df_thc[df_thc[col_to_zero] <= 0]['dte'].idxmax()
        pos_idx = df_thc[df_thc[col_to_zero] >= 0]['dte'].idxmin()
    elif p_last < p_first and p_first >= 0 and p_last < 0:
        # col_to_zero is descending and sign switch:  interpolation
        neg_idx = df_thc[df_thc[col_to_zero] <= 0]['dte'].idxmin()
        pos_idx = df_thc[df_thc[col_to_zero] >= 0]['dte'].idxmax()
    else:
        # No sign switch: extrapolation
        idx = df_thc[col_to_zero].abs().idxmin()
        if debug:
            print(f'trap_zero no sign switch on col_to_zero {col_to_zero}', df_thc.loc[:, [col_to_zero, col_to_return]].set_index(col_to_zero).to_dict())
        return None #df_thc[idx]
    if neg_idx == pos_idx:
        # Interpolation is not needed
        print(f'trap_zero {col_to_zero} hit the jackpot on col_to_zero {col_to_zero} and col_to_return {col_to_return}!',
              df_thc.loc[:, ['dte', col_to_zero, col_to_return]].to_dict())
        return df_thc.loc[neg_idx][col_to_return]
    neg_row = df_thc.loc[neg_idx]
    pos_row = df_thc.loc[pos_idx]
    delta_y = pos_row[col_to_return] - neg_row[col_to_return]
    delta_x = pos_row[col_to_zero] - neg_row[col_to_zero]
    '''
        print(f'trap_zero: col_to_zero {col_to_zero}: interpolate between neg_idx.max {neg_idx} and pos_idx.min {pos_idx}')
        print('delta_x:', delta_x, 'delta_y:', delta_y, 'base_y:', neg_row[col_to_return], 'dx:', neg_row[col_to_zero])
        '''
    return neg_row[col_to_return] - neg_row[col_to_zero] * delta_y / delta_x

def calc_half_dte_resid(df_thc, symbol, dte, strike, premium, debug):
    half_dte = np.ceil(dte/2)
    if df_thc.shape[0] == 1:
        row = df_thc.iloc[-1]
        hdte_resid = 1 + half_dte*row['avg_theta']/premium
        if debug:
            print(f'hdte_resid from single avg_theta: {symbol} dte {dte} half_dte {half_dte} strike {strike}:', hdte_resid)
    elif half_dte < df_thc.iloc[0]['dte'] and df_thc.iloc[0]['half_dte'] < 0:
        # half_dte is beyond the first available dte
        row = df_thc.iloc[0]
        hdte_resid = (row['resid'] + row['half_dte']*row['avg_theta'])/premium
        if debug:
            print(f'hdte_resid uses extrapolation on first row: {symbol} dte {dte} strike {strike}', hdte_resid)
    elif df_thc.iloc[-1]['half_dte'] >= 0:
        # The last row completely cover half_dte
        row = df_thc.iloc[-1]
        hdte_resid = (premium + half_dte*row['avg_theta'])/premium
        if debug:
            print(f'hdte_resid uses interpolation on last row: {symbol} dte {dte} strike {strike}', hdte_resid)
    else:
        hdte_resid = trap_zero(df_thc, 'half_dte', 'resid', debug)/premium
        if hdte_resid is None:
            print(f'# trap_zero failed on hdte_resid of {symbol} {opt_type} dte {dte} strike {strike}')
    return hdte_resid

def compute_time_decay_metrics(dfcp, symbol, opt_type, dte, strike, debug=False):
    '''Returns tuples: days_to_half, days_to_zero, half_dte_resid, expired_resid, premium'''
    _df = dfcp[(dfcp.symbol == symbol) & (dfcp.dte==dte) & (dfcp['type']==opt_type) & (dfcp.strike==strike)].iloc[0]
    premium = _df.mid
    if dte == 0:
        theta = _df.Theta
        dth = np.nan
        dtz = np.nan
        hdte_resid = premium + theta/2
        resid = premium + theta
        return np.nan, np.nan, hdte_resid, resid, premium
    theta_curve = get_theta_curve(dfcp[(dfcp.dte <= dte)], symbol, strike, opt_type)
    # Special case: theta_curve has only one dte
    if theta_curve.shape[0] == 1:
        theta = theta_curve.iloc[0]['Theta']
        if theta == 0:
            if debug:
                print(f'Single dte with 0 Theta, boring case.')
            return np.inf, np.inf, 1, 1, premium
        dtz = -premium/theta # theta is negative
        dth = dtz/2
        hdte_resid = 1 + theta*dte/2/premium
        if dtz <= dte:
            resid = 1 + dte * theta / premium
        else: # dtz - dte > 0
            resid = (dtz - dte)/dtz
            if dth > dte:
                dth = np.nan
        if debug:
            print(f'Single dte {dte}: premium: {premium}, theta: {theta}, dth: {dth}, dtz: {dtz}, hdte_resid: {hdte_resid}, resid: {resid}')
        return dth, dte, hdte_resid, resid, premium
    df_thc = prepare_theta_curve(theta_curve, dte, premium).dropna()
    dth = find_zero_resid(df_thc, 'half_resid', dte, debug)
    hdte_resid = calc_half_dte_resid(df_thc, symbol, dte, strike, premium, debug)
    if hdte_resid is None:
        raise Exception(f"Unhandled case: {symbol}, {dte}, {strike}")
    # Compute dtz and resid
    total_decay = df_thc['theta_decay'].sum()
    total_days = df_thc['diff_dte'].sum()
    if dte > total_days:
        # We should use theta from the smallest dte, not average
        theta = theta_curve.iloc[0]['Theta']
        resid = (premium + total_decay + (dte - total_days)*theta)/premium
        dtz = dte
        if debug:
            print(f'resid {symbol} dte {dte} strike {strike} total decay only cover {total_days} days: total_decay {total_decay} premium: {premium} dth: {dth} half dte resid: {hdte_resid} resid: {resid}')
    else:
        resid = 1 + total_decay/premium
        dtz = find_zero_resid(df_thc, 'resid', dte, debug)
        if debug:
            print(f'resid {symbol} dte {dte} strike {strike} total decay {total_decay} cover dte {dte}, dth: {dth} premium: {premium} dtz: {dtz} half dte resid: {hdte_resid}')
    return dth, dtz, hdte_resid, resid, premium

def compute_all_time_decay_metrics_for_symbol(dfcp, symbol, opt_type, oi_lb=0):
    '''Returns df with symbol, dte, strike, dth, dtz, half_dte_resid, resid, premium columns'''
    df = dfcp[(dfcp.symbol==symbol) & (dfcp.type==opt_type)]
    dte_list = df.dte.unique()
    res = []
    ignore_count = 0
    print(symbol, end='')
    for dte in dte_list:
        dte = dte.item()
        strike_list = df[df.dte==dte].strike.unique()
        for strike in strike_list:
            strike = strike.item()
            _filter = (df.dte==dte) & (df.strike==strike)
            if df[_filter]['Bid'].iloc[0] == 0 or df[_filter]['OpenInterest'].iloc[0] <= oi_lb:
                ignore_count += 1
                continue
            if len(res) % 100 == 0:
                print('.', end='')
            dth, dtz, hdte_resid, resid, premium = compute_time_decay_metrics(df, symbol, opt_type, dte, strike)
            res.append({'symbol': symbol, 'dte': dte, 'strike': strike, 'dth': dth, 'dtz': dtz, 'hdte_resid': hdte_resid, 'resid': resid, 'premium': premium})
    print(len(res), 'options loaded', ignore_count, 'ignored')
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
