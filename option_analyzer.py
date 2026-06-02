import pandas as pd
import numpy as np
import plotly.express as px
import plotly.io as pio
from plotly.subplots import make_subplots
import plotly.graph_objects as pgo
import sys
import os
from glob import glob
import logging
import time
from datetime import datetime, timedelta
import json
from multiprocessing import Process
from subprocess import getoutput
from dataclasses import dataclass, field

@dataclass(slots=True)
class OptionAnalyzer:
    quotes_dir: str
    chain_dir: str
    last_price: dict = field(default_factory=dict)
    d2e: dict = field(default_factory=dict)
    logger: logging.Logger = logging.getLogger('OptionAnalyzer')
    pio_template: str = 'none'
    def __post_init__(self):
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.setLevel(logging.INFO)
        pio.templates.default  = self.pio_template

    def get_updated_symbol_list(self, age_ub=60):
        age_dict = dict([(os.path.basename(f), time.time() - os.path.getmtime(f)) for f in glob(os.path.join(self.chain_dir, '*'))])
        symlist = sorted([k for k, v in age_dict.items() if v <= age_ub], key=age_dict.get)
        if len(symlist) > 0:
            self.logger.info(f'get_updated_symbol_list: {len(symlist)} symbols, age: {age_dict[symlist[0]]:.01f}" {age_dict[symlist[-1]]:.01f}", {" ".join(symlist)}')
        else:
            age_limit = min(age_dict.values()) + age_ub
            symlist = [os.path.basename(f) for f, age in age_dict.items() if age <= age_limit]
            self.logger.warning(f'get_updated_symbol_list: no update in the past {age_ub} seconds.')
        return symlist

    def get_quote_df(self, symlist):
        '''Updates self.last_price and returns three dataframes: quote, short interest, volatility'''
        dod = []
        dod_s = {}
        log = self.logger
        for symbol in symlist:
            quote_file = os.path.join(self.quotes_dir, symbol)
            if not os.path.exists(quote_file):
                log.warning(f'get_quote_df: quote file for {symbol} not found.')
                continue
            with open(quote_file) as fo:
                try:
                    quote_dict = json.load(fo)
                except json.JSONDecodeError:
                    log.warning(f'get_quote_df: failed to decode quote file {quote_file}')
                    continue
                resp_list = quote_dict.get('quotes')
                for resp_dict in resp_list:
                    if resp_dict.get('status', {}).get('errorCode') != 0:
                        log.warning(f"get_quote_df: skipped file {quote_file} due to error status {resp_dict.get('status')}")
                        continue
                    symbol = resp_dict['requestSymbol'].replace('/', '-')
                    q_data = resp_dict.get('quoteData')
                    quote_dt = f"{q_data.get('lastDate', '')} {q_data.get('lastTime', '')}"
                    self.last_price[symbol] = (float(q_data.get('lastPrice')),  quote_dt)
                    if 'short' in q_data:
                        dod_s[symbol] = q_data.pop('short')
                    else:
                        log.info(f'get_quote_df: {symbol} has no short interest')
                    if 'symbol' not in q_data:
                        log.warning(f'get_quote_df: quoteData does not include symbol.')
                        q_data['symbol'] = symbol
                    dod.append(q_data)
        df_s = pd.DataFrame(dod_s).T
        for _col in ['shortInterestChg', 'shortInterestRatioChg']:
            if _col in df_s.columns:
                df_s = df_s.drop(_col, axis=1)
        for col in df_s.columns:
            if col[-2:] != 'Dt':
                df_s[col] = pd.to_numeric(df_s[col], errors='coerce')
        df_q = pd.DataFrame(dod)
        v_cols = [c for c in df_q.columns if c.find('volatility') == 0]
        v_cols.sort(key=lambda c: int(c[10:-3]))
        for col in v_cols:
            df_q[col] = pd.to_numeric(df_q[col], errors='coerce')
        df_v = df_q[['symbol'] + v_cols]
        df_q = df_q.drop(v_cols, axis=1)
        numeric_cols = ['volume', 'cashDivRate',  'askPrice', 'bidPrice', 'lastPrice', 'netChgToday', 'openPrice', 'previousClose',
                        'annualizedDividend', 'askSize', 'avgVol10Day', 'avgVol90Day', 'bidSize', 'cumulativeValue', 'dayHigh', 'dayLow',
                        'earningsPerShare', 'eps', 'lastSize', 'marketCap', 'pctChgToday', 'peRatio', 'prevClosePrice', 'yearHighPrice',
                        'yearLowPrice', 'yield', 'equitySummaryScore', 'sharesOutstanding', 'stockDivRate', 'etfMidPremiumDiscount',
                        'etfMidPrice', 'etfNavPriceOffer', 'specDivRate']
        for col in numeric_cols:
            if col in df_q.columns:
                df_q[col] = pd.to_numeric(df_q[col], errors='coerce')
        mono_cols = [col for col in df_q.columns if df_q[col].nunique() == 1]
        #print('Monotonic:', mono_cols)
        #print(df_q.loc[:, ['symbol', 'sector']].groupby('sector').count())
        ignored_cols = 'companyName cusip instrumentSubtype instrumentType reportingExchange scCode3 industryGroup industry subIndustry'.split(' ')
        ignored_cols = [c for c in ignored_cols if c in df_q.columns]
        df_q = df_q.drop(mono_cols + ignored_cols, axis=1)
        return df_q, df_s, df_v

    def count_days_from_earning_reports(self, df_quotes):
        '''df_quotes from OptionFinder.get_quote_df()
        returns df with earningQtrReportDate and earningDays'''
        for col in ['symbol', 'earningQtrReportDate']:
            if col not in df_quotes.columns:
                return
        _df = df_quotes.loc[:, ['symbol', 'earningQtrReportDate']]
        today = pd.Timestamp.today().normalize()
        _df['earningDays'] = (pd.to_datetime(_df.earningQtrReportDate) - today).dt.days
        _df = _df[(_df.earningDays <= 60) & (_df.earningDays >= 0)].sort_values(by='earningDays')
        _df['earningDays'] = _df.earningDays.astype(int)
        if _df.shape[0] == 0:
            self.logger.warning('No earning date in df_quotes')
        _df = _df.set_index('symbol')
        self.d2e = _df['earningDays'].to_dict()
        return _df

    def build_option_df(self, symlist):
        log = self.logger
        df_list = [self.create_option_chain_df(symbol) for symbol in symlist]
        df_list = [_f for _f in df_list if _f is not None]
        if len(df_list) == 0:
            return
        df = pd.concat(df_list)
        monotonic_cols = [col for col in df.columns if col not in ['symbol', 'lastPrice', 'load_dt', 'quote_dt'] and df[col].nunique() == 1]
        log.debug(f'build_option_df dropped monotonic cols: {monotonic_cols}')
        df = df.drop(columns=monotonic_cols + ['adj']).rename(columns={'daysToExpiration': 'dte'})
        layered_cols = [('put', c[3:]) if c[:3]=='put' else ('call', c[4:]) if c[:4]=='call' else ('_', c) for c in df.columns]
        undesired_cols = ['expDt', 'optionPeriodicity', 'load_dt', 'quote_dt', 'Selection']
        layered_cols = [(c[0]+'_', c[1]) if c[1] in undesired_cols else c for c in layered_cols]
        df.columns = pd.MultiIndex.from_tuples(layered_cols)
        df = self._partially_reorder_columns(df)
        return df

    def _partially_reorder_columns(self, df):
        first_cols = {
            '_': 'symbol dte strike lastPrice'.split(' '),
            'c_p': 'Delta Bid Ask Last Change Volume ImpliedVolatility'.split(' ')
        }
        old_common_cols = [c for c in df.columns if c[0] == '_']
        old_call_cols = [c for c in df.columns if c[0] == 'call']
        old_put_cols =  [c for c in df.columns if c[0] == 'put']
        new_cols = [('_', c) for c in first_cols['_'] if ('_', c) in old_common_cols]
        new_cols += [('call', c) for c in first_cols['c_p'] if ('call', c) in old_call_cols]
        new_cols += [c for c in old_call_cols if c not in new_cols]
        new_cols += [('put', c) for c in first_cols['c_p'] if ('put', c) in old_put_cols]
        new_cols += [c for c in old_put_cols if c not in new_cols]
        new_cols += [c for c in old_common_cols if c not in new_cols]
        new_cols += [c for c in df.columns if len(c[0]) >= 2 and c[0][-1] == '_']
        return df[new_cols]

    def create_option_chain_df(self, symbol):
        '''Requires self.last_price'''
        log  = self.logger
        data_file = os.path.join(self.chain_dir, symbol)
        if not os.path.exists(data_file) or os.path.getsize(data_file) == 0:
            return None
        option_chains = None
        for attempt in range(1, 4):
            with open(data_file) as fo:
                try:
                    option_chains = json.load(fo)
                    break
                except json.JSONDecodeError:
                    log.warning(f'create_option_chain_df {symbol} JSONDecodeError on attempt {attempt}')
                    time.sleep(1)
                    continue
        if option_chains is None:
            log.warning(f'creat_option_chain_df {symbol} failed.')
            return None
        mtime = datetime.fromtimestamp(os.path.getmtime(data_file)).strftime('%F %T')
        cnp_list = option_chains['callsAndPuts']
        for cnp in cnp_list:
            exp_data_key = 'expirationData'
            if exp_data_key in cnp:
                exp_data = cnp.pop(exp_data_key)
                for k, v in exp_data.items():
                    if k == 'date':
                        cnp['expDt'] = v #datetime.strptime(v, '%b %d %Y').date()
                    else:
                        cnp[k] = v
        df = pd.DataFrame(cnp_list)
        str_cols = ['callSelection', 'putSelection', 'contractType', 'expDt', 'optionPeriodicity', 'settlementType']
        for col in df.columns:
            if col in str_cols:
                df[col] = df[col].astype(str)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df['symbol'] = symbol
        last_price, quote_dt = self.last_price.get(symbol, (None, None))
        if last_price is None:
            log.warning(f'create_option_chain_df failed to get last_price of {symbol}')
        else:
            df['lastPrice'] = last_price
            df['quote_dt'] = quote_dt # format='%m/%d/%Y %I:%M:%S%p'
        df['load_dt'] = mtime
        return df

    def get_data_timestamps(self, df_raw):
        _df = pd.concat([df_raw._.symbol, df_raw.__.quote_dt, df_raw.__.load_dt], axis=1).drop_duplicates()
        _df['quote_dt'] = pd.to_datetime(_df.quote_dt, format='%m/%d/%Y %I:%M:%S%p')
        _df['load_dt']  = pd.to_datetime(_df.load_dt)
        return _df

    def check_data_age(self, _df):
        now = pd.Timestamp.now()
        _g = self.get_data_timestamps(_df).groupby('symbol')
        df_age = (now - _g.max()).map(lambda x: x.seconds).rename(columns={'quote_dt': 'quote_age', 'load_dt': 'load_age'})
        df_age = df_age.sort_values(by='load_age')
        return df_age

    def select_options_by_type(self, df_raw, opt_type):
        ignored_cols = ['Change', 'Last', 'BidSize', 'AskSize']
        df = pd.concat([df_raw._, df_raw[opt_type], df_raw.__.loc[:, ['expDt']]], axis=1)
        df['mid'] = (df.Bid + df.Ask)/2
        df['pctSpread'] = 100*(df.Ask - df.Bid)/df.mid
        df['expDt'] = pd.to_datetime(df.expDt, format='mixed').dt.strftime('%F')
        df = df.rename(columns={'ImpliedVolatility': 'ImpVola'})
        ignored_cols = [c for c in df.columns if c in ignored_cols]
        if len(ignored_cols) > 0:
            df = df.drop(columns=ignored_cols)
        df = self.add_moneyness_columns(df)
        df = self.bucketize_dte(df)
        if opt_type[0].upper() == 'C':
            df['overpaid'] = (df.strike + df.mid)/df.lastPrice - 1
            df['leverage'] = df.lastPrice/df.mid*df.Delta
        return df

    def concat_put_call_options(self, df_raw):
        dfc = self.select_options_by_type(df_raw, 'call')
        dfc['type'] = 'C'
        dfp = self.select_options_by_type(df_raw, 'put')
        dfp['type'] = 'P'
        dfcp = pd.concat([dfc, dfp], ignore_index=True)#.drop(columns=['Bid', 'Ask'])
        return dfcp

    def add_moneyness_columns(self, df, atm_offset=0.01, otm_offset=0.1):
        df['moneyness'] = df['strike'] / df['lastPrice']
        bins = [0, 1-otm_offset, 1 - atm_offset, 1 + atm_offset, 1 + otm_offset, np.inf]
        labels = ['deep_otm_short', 'otm_short', 'atm', 'otm_long', 'deep_otm_long']
        df['cluster'] = pd.cut(df['moneyness'], bins=bins, labels=labels)
        return df

    def bucketize_dte(self, df, bins=[0, 7, 56, 91, 182, 364, np.inf]):
        labels = ['1wk', '8wk', '13wk', '6mo', '1yr', '>1yr']
        df['dte_cluster'] = pd.cut(df['dte'], bins=bins, labels=labels, right=False)
        return df

    def plot_option_stats(self, dfcp, metrics = ['pctSpread', 'ImpVola']):
        cols = 'type' if 'type' in dfcp.columns else None
        aggfuncs = ['mean', 'median']
        _df = dfcp[(dfcp.Bid > 0) & (dfcp.OpenInterest > 0)]
        df_pivot = pd.pivot_table(_df, index='symbol', columns=cols, values=metrics, aggfunc=aggfuncs)
        #return df_pivot
        titles = [f'{agg} {col}' for col in metrics for agg in aggfuncs]
        fig = make_subplots(rows=len(metrics), cols=2, subplot_titles=titles, shared_yaxes=True)
        fig.update_layout(height=len(metrics)*300)#, showlegend=False)
        for _i, metric in enumerate(metrics):
            for _j, agg in enumerate(aggfuncs):
                _df = df_pivot[agg]
                if cols is not None:
                    _df = _df.stack(level=1, future_stack=True)
                    _df = _df.loc[:, [metric]].reset_index().sort_values(by=['type', metric], ascending=False)
                    chart = px.bar(_df, x='symbol', y=metric, color=cols)
                else:
                    _df = _df.loc[:, [metric]].reset_index().sort_values(by=metric, ascending=False)
                    chart = px.bar(_df, x='symbol', y=metric)
                for trace in chart.data:
                    fig.add_trace(trace, row=_i+1, col=_j+1)
        fig.show()

    def plot_iv_statistics(self, dfcp, cluster_re=r'^otm_short', dte_lb=5, dte_ub=365):
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

    def plot_theta_curves_by_moneyness_cluster(self, dfcp, symbol, dte_lb=5, dte_ub=365):
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

    def get_closest_value_in_column(self, df, col, target_value):
        _df = df.loc[:, [col]].drop_duplicates()
        tmp_diff_col = '__diff'
        _df[tmp_diff_col] = (_df[col] - target_value).abs()
        idx = _df[tmp_diff_col].idxmin()
        return _df.loc[idx, col].item()

    def get_theta_curve(self, df, symbol, strike):
        _df = df[(df.symbol == symbol) & (df.strike == strike)].loc[:, ['dte', 'Theta']]
        df_thc = _df.pivot_table(values='Theta', index='dte').reset_index()
        return df_thc

    def setup_trapezoidal_decay(self, df_thc):
        df_thc['diff_dte'] = df_thc['dte'].diff()
        df_thc['avg_theta'] = df_thc['Theta'].rolling(window=2).mean()
        df_thc['theta_decay'] = df_thc['diff_dte'] * df_thc['avg_theta']
        return df_thc

    def prepare_theta_curve(self, df_thc, dte, premium):
        df_thc = df_thc[df_thc.dte <= dte]
        df_thc['decay_cumsum']  = df_thc['theta_decay'][::-1].cumsum()[::-1]
        df_thc['dt_'] = df_thc['diff_dte'][::-1].cumsum()[::-1]
        df_thc['half_dte'] = df_thc['dt_'] - np.ceil(dte/2)
        df_thc['half_resid'] = premium/2 + df_thc['decay_cumsum']
        df_thc['resid'] = premium + df_thc['decay_cumsum']
        return df_thc

    def find_zero_resid(self, df_thc, resid_col, dte, debug=False):
        log = self.logger
        # resid_col is always ascending
        if df_thc[resid_col].iloc[0] >= 0:
            # all positive
            log.debug(f'find_zero_resid: {resid_col} all positive: {df_thc[resid_col].to_dict()}')
            return dte
        if df_thc.iloc[-1][resid_col] <= 0:
            # all negative, must use the last row
            row = df_thc.iloc[-1]
            adj = row['diff_dte'] * row[resid_col]/row['theta_decay']
            log.debug(f'find_zero_resid: {resid_col} all negative: dt_ {row["dt_"]} - adj {adj}: {df_thc.loc[:, ['dt_', 'resid']].set_index('dt_').to_dict()}')
            return row['dt_'] - adj
        else:
            days_to_zero = self.trap_zero(df_thc, resid_col, 'dt_', debug)
            return days_to_zero
    
    def trap_zero(self, df_thc, col_to_zero, col_to_return, debug):
        '''
        Example 1: col_to_zero: 'resid', col_to_return: 'dt_'
        Example 2: col_to_zero: 'half_dte', col_to_return: 'resid'
        '''
        log = self.logger
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
            log.debug(f'trap_zero no sign switch on col_to_zero {col_to_zero}', df_thc.loc[:, [col_to_zero, col_to_return]].set_index(col_to_zero).to_dict())
            return None #df_thc[idx]
        if neg_idx == pos_idx:
            # Interpolation is not needed
            log.info(f'trap_zero {col_to_zero} hit the jackpot on col_to_zero {col_to_zero} and col_to_return {col_to_return}!',
                             df_thc.loc[:, ['dte', col_to_zero, col_to_return]].to_dict())
            return df_thc.loc[neg_idx][col_to_return]
        neg_row = df_thc.loc[neg_idx]
        pos_row = df_thc.loc[pos_idx]
        delta_y = pos_row[col_to_return] - neg_row[col_to_return]
        delta_x = pos_row[col_to_zero] - neg_row[col_to_zero]
        log.debug(f'trap_zero {col_to_zero}: {neg_row[col_to_return]} - {neg_row[col_to_zero]} * {delta_y} / {delta_x}')
        return neg_row[col_to_return] - neg_row[col_to_zero] * delta_y / delta_x

    def calc_half_dte_resid(self, df_thc, symbol, dte, strike, premium, theta_0, debug):
        half_dte = np.ceil(dte/2)
        total_days = df_thc['diff_dte'].sum()
        total_decay = df_thc['theta_decay'].sum()
        log = self.logger
        if half_dte > total_days:
            hdte_resid = (premium + total_decay + (half_dte - total_days)*theta_0)/premium
            log.debug(f'hdte_resid {symbol} dte {dte} strike {strike} total decay {total_decay} only cover {total_days} days: half_dte {half_dte} premium: {premium} half dte resid: {hdte_resid}')
        elif df_thc.shape[0] == 1:
            row = df_thc.iloc[-1]
            hdte_resid = 1 + half_dte*row['avg_theta']/premium
            log.debug(f'hdte_resid from single avg_theta: {symbol} dte {dte} half_dte {half_dte} strike {strike}: {hdte_resid}')
        elif half_dte < df_thc.iloc[0]['dte'] and df_thc.iloc[0]['half_dte'] < 0:
            # half_dte is beyond the first available dte
            row = df_thc.iloc[0]
            hdte_resid = (row['resid'] + row['half_dte']*row['avg_theta'])/premium
            log.debug(f'hdte_resid uses extrapolation on first row: {symbol} dte {dte} strike {strike}: {hdte_resid}')
        elif df_thc.iloc[-1]['half_dte'] >= 0:
            # The last row completely cover half_dte
            row = df_thc.iloc[-1]
            hdte_resid = (premium + half_dte*row['avg_theta'])/premium
            log.debug(f'hdte_resid uses interpolation on last row: {symbol} dte {dte} strike {strike}: {hdte_resid}')
        else:
            hdte_resid = self.trap_zero(df_thc, 'half_dte', 'resid', debug)/premium
            if hdte_resid is None:
                log.warning(f'# trap_zero failed on hdte_resid of {symbol} {opt_type} dte {dte} strike {strike}')
        return hdte_resid

    def compute_all_time_decay_metrics_for_symbols(self, df, sym_list, ignore_no_bid=True, exclude_0dte=True, oi_lb=0):
        '''Returns df with symbol, dte, strike, dth, dtz, half_dte_resid, resid, premium columns
        input df must be either put or call, not both'''
        assert 'type' not in df.columns or df.type.nunique() == 1
        opt_type = 'P' if df.Delta.min() < 0 else 'C'
        print(f'Assume option type is {opt_type} based on delta')
        res = []
        t00 = time.time()
        for symbol in sym_list:
            df_sym = df[(df.symbol==symbol)]
            strike_list = df_sym.strike.unique()
            load_count = 0
            ignore_count = 0
            print(symbol, end='')
            t0 = time.time()
            for strike in strike_list:
                df_thc = self.get_theta_curve(df_sym, symbol, strike)
                df_thc = self.setup_trapezoidal_decay(df_thc)
                strike_filter = df_sym.strike == strike
                for dte in sorted(df_thc.dte, reverse=True):
                    _df = df_sym[strike_filter & (df_sym.dte == dte)]
                    if (exclude_0dte and dte == 0) or (ignore_no_bid and (_df['Bid'].iloc[0] == 0 or _df['OpenInterest'].iloc[0] <= oi_lb)):
                        ignore_count += 1
                        continue
                    load_count += 1
                    if load_count % 100 == 0:
                        print('.', end='')
                    premium = _df.iloc[0].mid
                    dth, dtz, hdte_resid, resid = self.compute_time_decay_metrics(df_thc, symbol, strike, dte, premium)
                    res.append({'symbol': symbol, 'dte': dte, 'strike': strike, 'dth': dth, 'dtz': dtz, 'hdte_resid': hdte_resid, 'resid': resid})
            print(load_count, 'options loaded,', ignore_count, 'ignored,', '%.1f seconds' % (time.time() - t0))
        print('Total time:', int(time.time() - t00), 'seconds')
        df_res = pd.DataFrame(res)
        df_res = self.finalize_time_decay_df(df_res, df, opt_type)
        return df_res

    def finalize_time_decay_df(self, df_res, df, opt_type):
        df_res['dthr'] = df_res.dth/df_res.dte
        df_res['dtzr'] = df_res.dtz/df_res.dte
        _index = ['symbol', 'dte', 'strike']
        df_res = df_res.set_index(_index)
        add_cols = ['Bid', 'mid', 'lastPrice', 'pctSpread', 'Delta', 'Theta', 'moneyness', 'expDt', 'ImpVola', 'OpenInterest']
        if opt_type == 'C':
            add_cols += ['overpaid', 'leverage']
        df_res = df_res.join(df.set_index(_index).loc[:, add_cols]).reset_index()
        # Profits for selling put and call
        # Fidelity charges $0.65 per option at STO, 0 at BTC if premium < 1
        fidelity_fee = 0.0065
        dte_profit = df_res.Bid - fidelity_fee # Use bid price so we don't have to offset spread
        df_res['dteProfit'] = dte_profit / (df_res.strike if opt_type == 'P' else df_res.lastPrice)*100/df_res.dte*365
        dth_profit = df_res.Bid/2 - df_res.mid.apply(lambda x: fidelity_fee if x <= 1.3 else 2*fidelity_fee)
        df_res['dthProfit'] = dth_profit / (df_res.strike if opt_type == 'P' else df_res.lastPrice)*100/np.ceil(df_res.dth)*365
        df_res['E'] = df_res.symbol.apply(self.d2e.get)
        if opt_type == 'P':
            df_res['dthStrikeMargin'] = 100 * (df_res.lastPrice*(1 - df_res.ImpVola * np.sqrt(df_res.dth/365)) - df_res.strike + df_res.mid) / df_res.strike
        elif opt_type == 'C':
            df_res['dthStrikeMargin'] = 100 * (df_res.strike - df_res.lastPrice*(1 + df_res.ImpVola * np.sqrt(df_res.dth/365)) + df_res.mid) / df_res.strike
        reordered_cols = [c for c in df_res.columns if c[:2] != 'dt'] + [c for c in df_res.columns if c[:2] == 'dt']
        return df_res[reordered_cols]

    def shortcut_time_decay(self, df_thc, symbol, strike, dte, premium):
        if np.isnan(df_thc.iloc[0]['avg_theta']):
            theta = df_thc.iloc[0]['Theta']
        else:
            theta = df_thc.iloc[0]['avg_theta']
        if theta == 0:
            self.logger.debug(f'Single dte with 0 Theta: {symbol} strike={strike} dte={dte}')
            return np.inf, np.inf, 1, 1
        dtz = -premium/theta
        dth = dtz/2
        hdte_resid = 1 + theta*dte/2/premium
        if dtz <= dte:
            resid = 1 + dte * theta / premium
        else: # dtz - dte > 0
            resid = (dtz - dte)/dtz
        self.logger.debug(f'Single dte {symbol} strike={strike} dte={dte}: premium: {premium}, theta: {theta}, dth: {dth}, dtz: {dtz}, hdte_resid: {hdte_resid}, resid: {resid}')
        return dth, dtz, hdte_resid, resid

    def compute_time_decay_metrics(self, df_thc, symbol, strike, dte, premium):
        '''Returns tuples: days_to_half, days_to_zero, half_dte_resid, expired_resid, premium'''
        df_thc_dte = df_thc[df_thc.dte <= dte].copy()
        _df_thc_dte = self.prepare_theta_curve(df_thc_dte, dte, premium).dropna()
        if _df_thc_dte.shape[0] == 0:
            return self.shortcut_time_decay(df_thc_dte, symbol, strike, dte, premium)
        theta_0 = df_thc_dte.iloc[0]['Theta'] # Save this Theta from the original theta curve
        df_thc = _df_thc_dte # I am lazy here, since this code was copied from old code that used df_thc
        dth = self.find_zero_resid(df_thc, 'half_resid', dte, debug=True)
        hdte_resid = self.calc_half_dte_resid(df_thc, symbol, dte, strike, premium, theta_0, debug=True)
        if hdte_resid is None:
            raise Exception(f"Unhandled case: {symbol}, {dte}, {strike}")
        # Compute dtz and resid
        total_decay = df_thc['theta_decay'].sum()
        total_days = df_thc['diff_dte'].sum()
        if dte > total_days:
            # We should use theta from the smallest dte, not average
            resid = (premium + total_decay + (dte - total_days)*theta_0)/premium
            dtz = dte
            self.logger.debug(f'resid {symbol} dte {dte} strike {strike} total decay only cover {total_days} days: total_decay {total_decay} premium: {premium} dth: {dth} half dte resid: {hdte_resid} resid: {resid}')
        else:
            resid = 1 + total_decay/premium
            dtz = self.find_zero_resid(df_thc, 'resid', dte, debug=True)
            self.logger.debug(f'resid {symbol} dte {dte} strike {strike} total decay {total_decay} cover dte {dte}, dth: {dth} premium: {premium} dtz: {dtz} half dte resid: {hdte_resid}')
        return dth, dtz, hdte_resid, resid

    def compute_time_decay_metrics_for_positions(self, df_pos, dfcp):
        res = {}
        df_dict = {}
        for opt_type in df_pos.type.unique():
            res[opt_type] = []
            ss2dte_mid = (df_pos[df_pos.type == opt_type].loc[:, ['symbol', 'strike', 'dte', 'mid']].set_index(['symbol', 'strike']).to_dict())
            for ss, dte in ss2dte_mid['dte'].items():
                symbol, strike = ss
                premium = ss2dte_mid['mid'][ss]
                df_thc = self.get_theta_curve(dfcp[dfcp.dte <= dte], symbol, strike)
                df_thc = self.setup_trapezoidal_decay(df_thc)
                dth, dtz, hdte_resid, resid = self.compute_time_decay_metrics(df_thc, symbol, strike, dte, premium)
                self.logger.debug(f'time_decay_metrics for {opt_type} {ss} DTE {dte}: {dth}, {dtz}, {hdte_resid}, {resid}')
                res[opt_type].append({'type': opt_type, 'symbol': symbol, 'dte': dte, 'strike': strike, 'dth': dth, 'dtz': dtz, 'hdte_resid': hdte_resid, 'resid': resid})
            df_res = pd.DataFrame(res[opt_type])
            df_dict[opt_type] = self.finalize_time_decay_df(df_res, dfcp[dfcp.type==opt_type], opt_type)
        return df_dict

    def rank_put_spreads(self, dfp_leg_1, risk_limit=100_000, max_oi_ratio=0.1):
        symlist = dfp_leg_1.symbol.unique()
        df_quotes, df_shortint, df_vola = self.get_quote_df(symlist)
        df_raw = self.build_option_df(symlist)
        leg_2_cols = ['symbol', 'dte', 'strike', 'mid', 'Delta', 'OpenInterest']
        dfp_leg_2 = self.select_options_by_type(df_raw, 'put').loc[:, leg_2_cols]
        dfp_leg_2 = dfp_leg_2.rename(columns=dict([(c, c+'_2') for c in leg_2_cols[2:]]))
        _df = dfp_leg_1.merge(dfp_leg_2, how='left', on=['symbol', 'dte'])
        _df = _df[(_df.strike_2 < _df.strike)]
        _df = _df.sort_values(by=['dthProfit', 'symbol', 'dte', 'strike', 'strike_2'], ascending=[False, True, True, True, False])
        _df['max_gain'] = 100*(_df['mid'] - _df['mid_2']) - 1.3
        _df['max_loss'] = 100*(_df['strike'] - _df['strike_2']) - _df['max_gain']
        _df['n_options'] = np.minimum(np.floor(risk_limit/_df['max_loss']), np.floor(_df.loc[:, ['OpenInterest_2', 'OpenInterest']] * max_oi_ratio).min(axis=1)).astype(int)
        _df['gain_limit'] = _df.n_options * _df.max_gain
        _df['gain_limit_pd'] = _df.gain_limit/_df.dte
        _df['dthProfit_2'] = _df.max_gain/2/(_df.max_loss + 100 * _df.mid_2)*100/_df.dth*365
        _df['SymbolExpDt'] = _df.symbol + ':' + _df.expDt
        lead_cols = ['SymbolExpDt', 'strike',  'strike_2', 'n_options', 'gain_limit', 'dte', 'gain_limit_pd', 'dthProfit_2', 'dthStrikeMargin', 'lastPrice', 'mid', 'mid_2', 'OpenInterest', 'OpenInterest_2']
        cols = lead_cols + [_ for _ in _df.columns if _ not in lead_cols]
        return _df.loc[:, cols].sort_values(by='gain_limit_pd', ascending=False)

    def get_rows_with_closest_value_in_column(self, df, col, target_value):
        groupers = [c for c in df.columns if c != col]
        df = df.drop_duplicates(subset=groupers + [col])
        tmp_diff_col = '__diff'
        df[tmp_diff_col] = (df[col] - target_value).abs()
        idx = df.groupby(groupers)[tmp_diff_col].idxmin() if len(groupers) > 0 else df.loc[:, [tmp_diff_col]].idxmin()
        return df.loc[idx].drop(columns=[tmp_diff_col])

    def select_pds_deltas(self, dfcp, dte_lb, dte_ub):
        _df = dfcp[(dfcp.type=='P') & (dfcp.dte >= dte_lb) & (dfcp.dte <= dte_ub)]
        dfstrike_atm = self.get_rows_with_closest_value_in_column(_df.loc[:, ['symbol', 'dte', 'strike', 'lastPrice']], 'strike', _df.lastPrice).set_index(['symbol', 'dte'])
        _df = _df.loc[:, ['symbol', 'dte', 'Delta']]
        dfdelta_25 = self.get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.25], 'Delta', -0.25).set_index(['symbol', 'dte'])
        dfdelta_50 = self.get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.6],  'Delta', -0.5).set_index(['symbol', 'dte'])
        dfdelta_5  = self.get_rows_with_closest_value_in_column(_df[_df.Delta >= -0.06], 'Delta', -0.05).set_index(['symbol', 'dte'])
        df_join = dfstrike_atm.join(dfdelta_25).join(dfdelta_50, rsuffix='_50').join(dfdelta_5, rsuffix='_5')
        return df_join.rename(columns={'strike': 'atm_strike', 'Delta': 'Delta_25'})

    def put_debit_spread(self, dfpds, symbol, dte, dfcp, cols=['strike', 'Delta', 'mid', 'OpenInterest', 'ImpVola']):
        delta_dict = dfpds.loc[(symbol, dte)].to_dict()
        atm_strike = delta_dict.pop('atm_strike')
        deltas = list(delta_dict.values())
        _f = (dfcp.strike==atm_strike) & (dfcp.type=='P')
        for _d in deltas:
            _f = _f | (dfcp.Delta==_d)
        return dfcp[(dfcp.symbol==symbol) & (dfcp.dte==dte) & (_f)].loc[:, cols]

    def plot_metric_subtotals_in_one_row(self, df, grouper, metric_list, shared_y=True, log_y_threshold=500, horizontal_spacing=0.02):
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

    def plot_metrics_in_one_row(self, df, groupers, metric_list, shared_y=True, log_y_threshold=500, horizontal_spacing=0.02):
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

    def calc_overall_put_call_ratios(self, df):
        metrics = ['OpenInterest', 'Volume']
        p = df.pivot_table(index='symbol', columns='type', values=metrics, aggfunc='sum', observed=False, fill_value=0)
        return pd.DataFrame(dict([(_v + '_P_C_ratio', p.loc[:, (_v, 'P')]/p.loc[:, (_v, 'C')]) for _v in metrics])).reset_index()

    def calc_cluster_put_call_ratios(self, dfcp):
        groupers = ['symbol', 'cluster', 'dte_cluster', 'type']
        metrics = ['OpenInterest', 'Volume']
        _df = dfcp.loc[:, groupers + metrics].groupby(groupers, observed=False).sum().reset_index()
        _df = _df.pivot(columns='type', index=groupers[:-1])
        _df = pd.DataFrame({'OpenInterest_P_C_ratio': _df.OpenInterest.P/_df.OpenInterest.C, 'Volume_P_C_ratio': _df.Volume.P/_df.Volume.C})
        return _df.reset_index()

    def plot_metrics_by_moneyness_and_dte_clusters(self, df, height=240, vertical_spacing=0.03, shared_yaxes=True):
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

    def aggregate_metrics_by_moneyness_and_dte_clusters(self, df, metrics, method='mean', oi_lb=None, bid_lb=0):
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

    def put_call_ratios_by_moneyness(self, df, metric='Volume', by_dte=True):
        # Assume df already has moneyness cluster, option type
        idx_cols = ['symbol', 'dte', 'cluster'] if by_dte else ['symbol', 'cluster']
        pivot = df.pivot_table(index=idx_cols, columns='type', values=metric, aggfunc='sum', observed=False, fill_value=0)
        pivot = pivot[pivot.C + pivot.P >= 10]
        return pd.DataFrame({'put_call_ratio': pivot['P'] / pivot['C']}).unstack(level=2 if by_dte else 1).put_call_ratio

    def plot_put_call_ratios_by_moneyness(self, df_moneyness):
        symbol = list(df_moneyness.symbol)[0]
        _df = df_moneyness[df_moneyness.symbol==symbol]
        _df_v = self.put_call_ratios_by_moneyness(_df, metric='Volume', by_dte=True).reset_index()
        _df_o = self.put_call_ratios_by_moneyness(_df, metric='OpenInterest', by_dte=True).reset_index()
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

    def plot_option_details_by(self, df_cp, symbol, metric, by, offset=0, delta_lb=0.5, delta_ub=0.9, nr=4, nc=2):
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

    def plot_leverage_overpaid(self, df, delta_lb=0.5, overpaid_ub=0.1, price_lb=5, spread_ub=10, leverage_lb=2, openinterest_lb=10):
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

@dataclass(slots=True)
class ParallelOptionCalculator:
    df: pd.DataFrame
    optana: OptionAnalyzer
    buffer_dir: str
    opt_type: str = ''
    exclude_0dte: bool = True
    ignore_no_bid: bool = True
    oi_lb: int = 10
    proc_dict: dict = field(default_factory=dict)
    logger: logging.Logger = field(init=False)
    def __post_init__(self):
        import resource
        if not os.path.exists(self.buffer_dir):
            os.mkdir(self.buffer_dir)
        self.logger = self.optana.logger
        assert 'type' not in self.df.columns or self.df.type.nunique() == 1
        self.opt_type = 'P' if self.df.Delta.min() < 0 else 'C'
        self.logger.debug(f'Assume option type is {self.opt_type} based on delta')
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        self.logger.debug(f"Current file limits - Soft: {soft}, Hard: {hard}")
        if soft < hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            self.logger.debug(f"New file limits - Soft: {new_soft}, Hard: {new_hard}")

    def do_theta_curves(self, symbol, strike, df_sym):
        optana = self.optana
        df_thc = optana.get_theta_curve(df_sym, symbol, strike)
        try:
            df_thc = optana.setup_trapezoidal_decay(df_thc)
        except KeyError:
            self.logger.info(f'do_theta_curves {symbol}~{strike} is not ready.')
            return
        self.logger.debug(f'do_theta_curves {symbol}~{strike} df_sym shape: {df_sym.shape}, df_thc shape: {df_thc.shape}')
        strike_filter = (df_sym.strike == strike)
        ignore_count = 0
        res = []
        for dte in sorted(df_thc.dte, reverse=True):
            dte_row = df_sym[strike_filter & (df_sym.dte == dte)]
            assert dte_row.shape[0] == 1
            dte_row = dte_row.iloc[0]
            if (self.exclude_0dte and dte == 0) or (self.ignore_no_bid and (dte_row['Bid'] == 0 or dte_row['OpenInterest'] <= self.oi_lb)):
                ignore_count += 1
                continue
            dth, dtz, hdte_resid, resid = optana.compute_time_decay_metrics(df_thc, symbol, strike, dte, dte_row['mid'])
            res.append({'symbol': symbol, 'strike': strike, 'dte': dte, 'dth': dth, 'dtz': dtz, 'hdte_resid': hdte_resid, 'resid': resid})
        if len(res) > 0:
            output_file = self.get_output_file_name(symbol, strike)
            with open(output_file, 'w') as wfo:
                pd.DataFrame(res).to_csv(wfo, index=None)
            self.logger.debug(f'do_theta_curves wrote {len(res)} rows to {output_file}, ignored {ignore_count} DTEs.')
        else:
            self.logger.debug(f'do_theta_curves {symbol}~{strike} ignored {ignore_count} DTEs.')

    def get_output_file_name(self, symbol, strike):
        return os.path.join(self.buffer_dir, f'{symbol}~{strike}.csv')

    def do_all_theta_curves(self, symlist):
        df = self.df
        opt_type = self.opt_type
        proc_dict = self.proc_dict
        start_time = time.time()
        for symbol in symlist:
            df_sym = df[(df.symbol==symbol)]
            if df_sym.shape[0] == 0:
                print(symbol, 'not found in df')
                continue
            strike_list = df_sym.strike.unique()
            t0 = time.perf_counter()
            for strike in strike_list:
                proc = Process(target=self.do_theta_curves, args=(symbol, strike, df_sym))
                proc_dict[(symbol, strike)] = proc
                proc.start()
            print(symbol, len(strike_list), 'theta curves processed in %.1f seconds' % (time.perf_counter() - t0))
        for i in range(1, 100):
            if self.count_zombies() == 0:
                self.logger.debug(f'zombie count = 0 after {i} kills')
                break
            self.kill_zombies()
            time.sleep(0.1)
        print(f'Completed {len(symlist)} symbols in %.1f seconds' % (time.time() - start_time))
        return self.get_output_files(symlist, start_time)

    def assemble_time_decay_df(self, output_files):
        df_res = pd.concat([pd.read_csv(f) for f in output_files])
        return self.optana.finalize_time_decay_df(df_res, self.df, self.opt_type)

    def get_output_files(self, symlist, mtime_lb):
        output_list = []
        for symbol in symlist:
            output_list += [f for f in glob(self.get_output_file_name(symbol, '*')) if os.path.getmtime(f) >= mtime_lb and os.path.getsize(f) > 0]
        return output_list

    def count_zombies(self):
        return len([x for x in getoutput(f'/usr/bin/ps --ppid {os.getpid()} -oargs').splitlines() if x.find('<defunct>') > 0])

    def kill_zombies(self):
        n_alive = 0
        killed = 0
        key_list = list(self.proc_dict)
        for key in key_list:
            proc = self.proc_dict[key]
            if proc.is_alive():
                n_alive += 1
            elif proc.exitcode is not None:
                proc.join(0.1)
                proc.close()
                killed += 1
                self.proc_dict.pop(key)
        self.logger.debug(f'killed {killed} zombies, left {n_alive} children alive.')

    def list_open_files(self):
        import psutil
        proc = psutil.Process(os.getpid())
        open_files = proc.open_files()
        return [file.path for file in open_files]

def get_rotating_logger(log_name, log_file):
    import logging
    from logging.handlers import RotatingFileHandler
    logger = logging.getLogger(log_name)
    handler = RotatingFileHandler(log_file, maxBytes=299792458, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.level = logging.INFO
    return logger

def run_now(symlist_file):
    log_name = os.path.basename(sys_argv[0]).replace('.py', '')
    log_file = os.path.join(os.path.expanduser('~/logs/'),  log_name + '~test.log')
    logger = get_rotating_logger(log_name, log_file)
    app_dir = os.path.dirname(symlist_file)
    quotes_dir = os.path.join(app_dir, 'quotes')
    chain_dir = os.path.join(app_dir, 'chain')
    print('Log file:', log_file, 'chain dir:', chain_dir, 'quotes dir:', quotes_dir)
    self = OptionAnalyzer(quotes_dir, chain_dir, logger=logger)
    with open(symlist_file) as fo:
        symlist = [_.rstrip() for _ in fo]
    this_symlist = self.get_updated_symbol_list(age_ub=60)
    sym_added = set(this_symlist) - set(symlist)
    sym_missing = set(symlist) - set(this_symlist)
    if len(sym_missing) > 0:
            print(f'Option data missing for {sym_missing}. Continue any way.')
    df_quotes, df_shortint, df_vola = self.get_quote_df(symlist)
    df_raw = self.build_option_df(symlist)
    df_ts = self.get_data_timestamps(df_raw)
    load_dt_diff = (df_ts.load_dt.max() - df_ts.load_dt.min()).total_seconds()
    if load_dt_diff > 60:
        print(f'Option data load_dt difference {load_dt_diff} is over 60 seconds. Continue any way.')
    data_ts = df_ts.load_dt.max()
    data_dir = os.path.expanduser('~/lab/data')
    print('df_raw.shape:', df_raw.shape, 'data_ts:', data_ts.strftime('%F %T'))
    df_earning = self.count_days_from_earning_reports(df_quotes)
    print('Days to E:', self.d2e)
    for opt_type in ['put', 'call']:
        csv_file = os.path.join(app_dir, data_ts.strftime(f'{data_dir}/{opt_type}~{hostname}~%F_%T.csv'))
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
            print(f'{csv_file} already exists: {os.path.getsize(csv_file)} bytes')
            continue
        df_type = self.select_options_by_type(df_raw, opt_type)
        poc = ParallelOptionCalculator(df_type, self, f'/run/user/{os.getuid()}/time_decay~{opt_type}', oi_lb=100)
        theta_curve_files = poc.do_all_theta_curves(symlist)
        df_type = poc.assemble_time_decay_df(theta_curve_files)
        df_type.to_csv(csv_file, index=None)
        print(csv_file, df_type.shape, os.path.getsize(csv_file), 'bytes')

def main(sys_argv):
    import socket
    hostname = socket.gethostname()
    from market_data_timer import wait_till_market_open, wait_to_open_symbol_file
    if len(sys_argv) == 1:
        sys.stderr.write(f'Usage: python {sys_argv[0]} symlist_file\n')
        return
    log_name = os.path.basename(sys_argv[0]).replace('.py', '')
    log_file = os.path.join(os.path.expanduser('~/logs/'),  log_name + '.log')
    #print('Log file:', log_file)
    logger = get_rotating_logger(log_name, log_file)
    symlist_file = sys.argv[1]
    app_dir = os.path.dirname(symlist_file)
    quotes_dir = os.path.join(app_dir, 'quotes')
    chain_dir = os.path.join(app_dir, 'chain')
    #print(symlist_file, chain_dir, quotes_dir)
    self = OptionAnalyzer(quotes_dir, chain_dir, logger=logger)
    wait_till_market_open(logger)
    wait_to_open_symbol_file(symlist_file)
    with open(symlist_file) as fo:
        symlist = [_.rstrip() for _ in fo]
    while True:
        this_symlist = self.get_updated_symbol_list(age_ub=60)
        #print(sorted(this_symlist))
        #print(sorted(symlist))
        sym_added = set(this_symlist) - set(symlist)
        #if len(sym_added) > 0:
        #    print(f'Detected option data for new symbols: {sym_added}')
        #    return
        sym_missing = set(symlist) - set(this_symlist)
        if len(sym_missing) > 0:
            print(f'Option data missing for {sym_missing}. Sleep 1 second.')
            time.sleep(1)
            continue
        df_quotes, df_shortint, df_vola = self.get_quote_df(symlist)
        df_raw = self.build_option_df(symlist)
        df_ts = self.get_data_timestamps(df_raw)
        load_dt_diff = (df_ts.load_dt.max() - df_ts.load_dt.min()).total_seconds()
        if load_dt_diff > 60:
            print(f'Option data load_dt difference {load_dt_diff} is over 60 seconds. Sleep 5 seconds.')
            time.sleep(5)
            continue
        quote_dt_load_dt_diff = np.abs((df_ts.load_dt - df_ts.quote_dt).max().total_seconds())
        if quote_dt_load_dt_diff > 45:
            hours = (pd.Timestamp.now() - pd.Timestamp.now().normalize()).seconds/3600
            if hours >= 16:
                if hours <= 16.25:
                    break # Ok to proceed
                else:
                    return # No new data for the day
            print(f'Option data and quote data are out of sync: {quote_dt_load_dt_diff}. Sleep 5 seconds.')
            time.sleep(5)
            continue
        else:
            break
    data_ts = df_ts.load_dt.max()
    data_dir = os.path.expanduser('~/lab/data')
    print('df_raw.shape:', df_raw.shape, 'data_ts:', data_ts.strftime('%F %T'))
    df_earning = self.count_days_from_earning_reports(df_quotes)
    print('Days to E:', self.d2e)
    for opt_type in ['put', 'call']:
        csv_file = os.path.join(app_dir, data_ts.strftime(f'{data_dir}/{opt_type}~{hostname}~%F_%T.csv'))
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
            print(f'{csv_file} already exists: {os.path.getsize(csv_file)} bytes')
            continue
        df_type = self.select_options_by_type(df_raw, opt_type)
        poc = ParallelOptionCalculator(df_type, self, f'/run/user/{os.getuid()}/time_decay~{opt_type}', oi_lb=100)
        theta_curve_files = poc.do_all_theta_curves(symlist)
        df_type = poc.assemble_time_decay_df(theta_curve_files)
        df_type.to_csv(csv_file, index=None)
        print(csv_file, df_type.shape, os.path.getsize(csv_file), 'bytes')
    
if __name__ == '__main__':
    main(sys.argv)
    '''This script can be run in a loop:
    cd ~/lab
    while true; do sync; python option_analyzer.py symbols-$(hostname).txt;sleep 1; done
    '''