#!/usr/bin/env python

import requests
import json
import os
import sys
import re
import time
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from glob import glob
import logging
from logging.handlers import RotatingFileHandler

def get_rotating_logger(log_name, log_file):
    logger = logging.getLogger(log_name)
    handler = RotatingFileHandler(log_file, maxBytes=299792458, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.level = logging.INFO
    return logger

class OptionFinder(object):
    numeric_cols = ['volume', 'cashDivRate',  'askPrice', 'bidPrice', 'lastPrice', 'netChgToday', 'openPrice', 'previousClose',
                    'annualizedDividend', 'askSize', 'avgVol10Day', 'avgVol90Day', 'bidSize', 'cumulativeValue', 'dayHigh', 'dayLow',
                    'earningsPerShare', 'eps', 'lastSize', 'marketCap', 'pctChgToday', 'peRatio', 'prevClosePrice', 'yearHighPrice',
                    'yearLowPrice', 'yield', 'equitySummaryScore', 'sharesOutstanding', 'stockDivRate', 'etfMidPremiumDiscount',
                    'etfMidPrice', 'etfNavPriceOffer', 'specDivRate']
    def __init__(self, logger, chain_dir='chain', quotes_dir='quotes', report_dir = 'data', max_file_age=60):
        self.chain_dir = chain_dir
        self.quotes_dir = quotes_dir
        self.logger = logger
        self.report_dir = report_dir
        self.max_file_age = max_file_age
        self.last_price = {}
        self.last_mtime = {}

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
                resp_list = quote_dict.get('quoteResponse')
                for resp_dict in resp_list:
                    if resp_dict.get('status', {}).get('errorCode') != 0:
                        log.warning(f"get_quote_df: skipped file {quote_file} due to error status {resp_dict.get('status')}")
                        continue
                    symbol = resp_dict['requestSymbol']
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
        for col in self.numeric_cols:
            if col in df_q.columns:
                df_q[col] = pd.to_numeric(df_q[col], errors='coerce')
        mono_cols = [col for col in df_q.columns if df_q[col].nunique() == 1]
        #print('Monotonic:', mono_cols)
        #print(df_q.loc[:, ['symbol', 'sector']].groupby('sector').count())
        ignored_cols = 'companyName cusip instrumentSubtype instrumentType reportingExchange scCode3 industryGroup industry subIndustry'.split(' ')
        ignored_cols = [c for c in ignored_cols if c in df_q.columns]
        df_q = df_q.drop(mono_cols + ignored_cols, axis=1)
        return df_q, df_s, df_v

    def build_option_df(self, symlist):
        log = self.logger
        df_list = [self.create_option_chain_df(symbol) for symbol in symlist]
        df_list = [_f for _f in df_list if _f is not None]
        if len(df_list) == 0:
            return
        df = pd.concat(df_list)
        monotonic_cols = [col for col in df.columns if col not in ['symbol', 'lastPrice', 'load_dt', 'quote_dt'] and df[col].nunique() == 1]
        log.info(f'build_option_df dropped monotonic cols: {monotonic_cols}')
        df = df.drop(columns=monotonic_cols + ['adj']).rename(columns={'daysToExpiration': 'dte'})
        layered_cols = [('put', c[3:]) if c[:3]=='put' else ('call', c[4:]) if c[:4]=='call' else ('_', c) for c in df.columns]
        undesired_cols = ['expirationDate', 'optionPeriodicity', 'load_dt', 'quote_dt', 'Selection']
        layered_cols = [(c[0]+'_', c[1]) if c[1] in undesired_cols else c for c in layered_cols]
        df.columns = pd.MultiIndex.from_tuples(layered_cols)
        for cp in ['call', 'put']:
            df_cp = df[cp]
            df.loc[:, (cp, 'pctSpread')] = 100*(df_cp.Ask - df_cp.Bid)/df_cp.Bid
            df.loc[:, (cp, 'pctProfit')] = 100*df_cp.Bid/(df._.strike if cp == 'put' else df._.lastPrice if 'lastPrice' in df._.columns else np.nan)
        df = self.partially_reorder_columns(df)
        return df

    def partially_reorder_columns(self, df):
        first_cols = {
            '_': 'symbol dte strike lastPrice'.split(' '),
            'c_p': 'pctProfit pctSpread Delta Bid Ask Last Change Volume ImpliedVolatility'.split(' ')
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
                        cnp['expirationDate'] = v #datetime.strptime(v, '%b %d %Y').date()
                    else:
                        cnp[k] = v
        df = pd.DataFrame(cnp_list)
        str_cols = ['callSelection', 'putSelection', 'contractType', 'expirationDate', 'optionPeriodicity', 'settlementType']
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
            df['quote_dt'] = quote_dt
        df['load_dt'] = mtime
        return df

    def read_csv_file(self, csv_file):
        return pd.read_csv(csv_file, header=[0, 1], dtype={('__', 'optionPeriodicity'): str, ('__', 'quote_dt'): str})

    def get_lower_strike_ranges(self, strike):
        x = math.floor(strike/100)*100
        if x >= 100:
            return list(range(x, 0, -100)) + [50, 0]
        elif strike >= 50:
            return [50, 0]
        else:
            return []

    def search_put_options(self, df, dte_ub=56):
        report_dir = self.report_dir
        put_filter = lambda r: (1 <= r.dte <= dte_ub) and r.Bid > 0 and r.OpenInterest > 0 and r.strike  < r.lastPrice
        res = []
        delta_ranges = [(-0.20, 0), (-0.4, -0.20), (-0.5, -0.4)]
        for min_delta, max_delta in delta_ranges:
            df_delta = df[df.apply(lambda r: put_filter(r) and (min_delta < r.Delta <= max_delta), axis=1)]
            df_delta = df_delta.sort_values(by='pctProfit', ascending=False).head(10)
            top_strike = df_delta.iloc[0].strike
            res.append(df_delta)
            strike_range = self.get_lower_strike_ranges(top_strike)
            #print('top_strike:', top_strike, strike_range)
            for i, s in enumerate(strike_range[:-1]):
                df_delta = df[df.apply(lambda r: put_filter(r) and (s >= r.strike > strike_range[i+1]) and (min_delta < r.Delta <= max_delta), axis=1)]
                df_delta = df_delta.sort_values(by='pctProfit', ascending=False).head(5)
                res.append(df_delta)
        csv_file = os.path.join(report_dir, 'put.csv')
        pd.concat(res).to_csv(csv_file, index=False)
        ts_min = df.load_dt.min()
        ts_max = df.load_dt.max()
        html_body = f'<div>[{ts_min}, {ts_max}] <a href="put.csv">put.csv</a></div>\n'
        n_break = len(res) // len(delta_ranges)
        for i, _df in enumerate(res):
            html_body += self.df_to_html_table(_df)
            if (i + 1) % n_break == 0:
                html_body += '<hr>\n'
        html_file = os.path.join(report_dir, 'put.html')
        with open(html_file, 'w') as wfo:
            wfo.write(self.finalize_html(html_body))
        self.logger.info(f'search_put_options saved {html_file}')
        return res

    def search_call_options(self, df, dte_ub=56):
        report_dir = self.report_dir
        call_filter = lambda r: (1 <= r.dte <= dte_ub) and r.Bid > 0 and r.OpenInterest > 0 and r.strike  > r.lastPrice
        res = []
        delta_ranges = [(0, 0.2), (0.2, 0.4), (0.4, 0.5)]
        for symbol in df.symbol.unique():
            for min_delta, max_delta in delta_ranges:
                df_delta = df[df.apply(lambda r: call_filter(r) and (r.symbol == symbol) and (min_delta < r.Delta <= max_delta), axis=1)]
                df_delta = df_delta.sort_values(by='pctProfit', ascending=False).head(5)
                res.append(df_delta)
        csv_file = os.path.join(report_dir, 'call.csv')
        pd.concat(res).to_csv(csv_file, index=False)
        symbol_count = len(res) // len(delta_ranges)
        ts_min = df.load_dt.min()
        ts_max = df.load_dt.max()
        html_body = f'<div>[{ts_min}, {ts_max}] <a href="call.csv">call.csv</a></div>\n'
        for i, _df in enumerate(res):
            html_body += self.df_to_html_table(_df)
            if (i + 1) % symbol_count == 0:
                html_body += '<hr>\n'
        html_file = os.path.join(report_dir, 'call.html')
        with open(html_file, 'w') as wfo:
            wfo.write(self.finalize_html(html_body))
        self.logger.info(f'search_call_options saved {html_file}')
        return res

    def separate_df(self, df, call_or_put):
        _df = df.loc[:, ['_', call_or_put, '__']]
        _df.columns = [c[1] for c in _df.columns]
        return _df

    def concat_put_call_options(self, df):
        ignored_cols = ['Change', 'Last', 'BidSize', 'AskSize', 'TimeValue', 'IntrinsicValue']

        dfc = pd.concat([df._, df.call, df.__.loc[:, ['expirationDate']]], axis=1)
        dfc_cols = dfc.columns
        dfc = dfc.drop(columns=[c for c in ignored_cols if c in dfc_cols])
        dfc['type'] = 'C'
        dfc['mid'] = (dfc.Bid + dfc.Ask)/2
        dfc['overpaid'] = (dfc.strike + dfc.mid)/dfc.lastPrice - 1

        dfp = pd.concat([df._, df.put,  df.__.loc[:, ['expirationDate']]], axis=1)
        dfp_cols = dfp.columns
        dfp = dfp.drop(columns=[c for c in ignored_cols if c in dfp_cols])
        dfp['type'] = 'P'
        dfp['mid'] = (dfp.Bid + dfp.Ask)/2
        dfp['overpaid'] = (dfp.strike - dfp.mid)/dfp.lastPrice - 1

        dfcp = pd.concat([dfc, dfp], ignore_index=True)#.drop(columns=['Bid', 'Ask'])
        dfcp = dfcp.rename(columns={'expirationDate': 'expDt', 'ImpliedVolatility': 'ImpVola'})
        dfcp['expDt'] = pd.to_datetime(dfcp.expDt).dt.strftime('%F')
        dfcp['leverage'] = dfcp.lastPrice/dfcp.mid*dfcp.Delta
        return dfcp

    def process_data(self, symlist):
        log = self.logger
        df_q, df_s, df_v = self.get_quote_df(symlist)
        save_quote_data(df_q, df_s, df_v)
        df = self.build_option_df(symlist)
        if df is None:
            log.warning(f'process_data: failed to load option data for {len(symlist)} symbols')
        else:
            df_call = self.separate_df(df, 'call')
            log.info(f'process_data: search_call_options df_call {df_call.shape}')
            call_res = self.search_call_options(df_call)
            df_put  = self.separate_df(df, 'put')
            log.info(f'process_data: search_put_options df_put {df_put.shape}')
            put_res  = self.search_put_options(df_put)
        return df

    def df_to_html_table(self, df):
        cols = [c for c in df.columns if not re.search(r'(Gamma|Theta|Vega|Rho)', c)]
        html = '<table><tr><th>'
        html += '</th><th>'.join(cols)
        html += '</th></tr>\n'
        for i, row in df.iterrows():
            html += '<tr><td>' + '</td><td>'.join([str(row[col]) for col in cols]) + '</td></tr>\n'
        html += '</table>\n'
        return html

    def finalize_html(self, body):
        html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
            font-family: Arial, sans-serif;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #e6e6e6;
        }
    </style>
</head>
<body>
""" + body + "</body></html>\n"
        return html

    '''
    Option history related methods:
    '''
    def get_option_files(self):
        option_files = [x for x in glob(os.path.join(self.report_dir, '*_options~*.csv')) if x.split('-')[-1].split('.')[0] <= '1625']
        return sorted(option_files, key=lambda x: x.split('~')[-1])

    def each_line_with_symbol(self, symbol, data_files, include_headers):
        for data_file in data_files:
            with open(data_file) as fo:
                if include_headers:
                    include_headers = False
                    yield fo.readline() + fo.readline()
                for line in fo:
                    if line.split(',')[0] == symbol:
                        yield line

    def cache_option_history(self, symbol):
        cache_csv_files = sorted(glob(os.path.join(self.report_dir, f'option-{symbol}~*.csv')))
        all_option_files = self.get_option_files()
        if len(cache_csv_files) == 0:
            cache_csv_file = os.path.join(self.report_dir, f'option-{symbol}~{all_option_files[-1].split("~")[-1]}')
            with open(cache_csv_file, 'w') as wfo:
                for line in self.each_line_with_symbol(symbol, all_option_files, True):
                    wfo.write(line)
        else:
            cache_csv_file = cache_csv_files[-1]
            last_cache_ts = cache_csv_file.split('~')[-1]
            new_option_files = [f for f in all_option_files if f.split('~')[-1] > last_cache_ts]
            if len(new_option_files) == 0:
                return cache_csv_file
            tmp_csv_file = cache_csv_file.replace('~', '~' + new_option_files[-1].split("~")[-1] + '~')
            os.rename(cache_csv_file, tmp_csv_file)
            with open(tmp_csv_file, 'a') as afo:
                for line in self.each_line_with_symbol(symbol, new_option_files, False):
                    afo.write(line)
            cache_csv_file = '~'.join(tmp_csv_file.split('~')[:-1])
            os.rename(tmp_csv_file, cache_csv_file)
        return cache_csv_file

    def read_option_history(self, symbol, call_put, max_dte):
        cache_csv_file = self.cache_option_history(symbol)
        _df = self.read_csv_file(cache_csv_file)
        _df = self.separate_df(_df, call_put)
        _df = _df[(_df.Bid > 0)].rename(columns={'expirationDate': 'expDt', 'ImpliedVolatility': 'ImpVola'})
        _df['expDt'] = pd.to_datetime(_df.expDt).dt.strftime('%F')
        today = datetime.now().strftime('%F')
        max_exp_dt = (datetime.now() + timedelta(days=max_dte)).strftime('%F')
        _df = _df[(_df.expDt >= today) & (_df.expDt <= max_exp_dt)].sort_values(by=['expDt', 'strike'])
        _df['mid'] = (_df.Bid + _df.Ask)/2
        selected_cols = ['expDt', 'strike', 'dte', 'mid', 'pctSpread', 'lastPrice', 'Delta', 'BidSize', 'AskSize', 'ImpVola', 'OpenInterest', 'load_dt']
        neglected_cols = [c for c in _df.columns if c not in selected_cols]
        _df = _df.loc[:, selected_cols]
        print('Max_exp_dt:', max_exp_dt, 'Cols neglected:', neglected_cols)
        return _df

    def cache_leaps_history(self, symbol):
        data_dir = self.report_dir
        cache_csv_files = sorted(glob(os.path.join(data_dir, f'leaps-{symbol}~*.csv')))
        if len(cache_csv_files) == 0:
            options_data_files = sorted(glob(os.path.join(data_dir, '*_options~*.csv')))
            options_data_files = [f for f in options_data_files if not os.path.exists(f.replace('_options~', '_leaps~'))]
            cache_csv_file = os.path.join(data_dir, f'leaps-{symbol}~{options_data_files[-1].split("~")[-1]}')
            with open(cache_csv_file, 'w') as wfo:
                for line in self.each_line_with_symbol(symbol, options_data_files, True):
                    wfo.write(line)
        else:
            cache_csv_file = cache_csv_files[-1]
            last_cache_ts = cache_csv_file.split('~')[-1]
            leaps_data_files = sorted(glob(os.path.join(data_dir, '*_leaps~*.csv')))
            leaps_data_files = [f for f in leaps_data_files if f.split('~')[-1] > last_cache_ts]
            if len(leaps_data_files) > 0:
                tmp_csv_file = cache_csv_file.replace('~', '~' + leaps_data_files[-1].split("~")[-1] + '~')
                os.rename(cache_csv_file, tmp_csv_file)
                with open(tmp_csv_file, 'a') as afo:
                    for line in self.each_line_with_symbol(symbol, leaps_data_files, False):
                        afo.write(line)
                cache_csv_file = '~'.join(tmp_csv_file.split('~')[:-1])
                os.rename(tmp_csv_file, cache_csv_file)
        return cache_csv_file

    def read_leaps_history(self, symbol, min_dte=180):
        cache_csv_file = self.cache_leaps_history(symbol)
        _df = self.read_csv_file(cache_csv_file)
        _df = self.separate_df(_df, 'call')
        _df = self.sort_leaps_df(_df)
        min_exp_dt = (datetime.now() + timedelta(days=min_dte)).strftime('%F')
        _df = _df[_df.expDt >=min_exp_dt]
        print('Min_exp_dt:', min_exp_dt)
        return _df

def save_quote_data(df_q, df_s, df_v):
    df_v.to_excel('volatility.xlsx', index=None)
    df_s.to_excel('short_interest.xlsx')
    info_cols = 'name sector sharesOutstanding marketCap indices equitySummaryRating equitySummaryScore'.split(' ')
    info_cols = [c for c in info_cols if c in df_q.columns]
    for col in 'sharesOutstanding marketCap equitySummaryScore'.split(' '):
        if col in df_q.columns:
            df_q[col] = pd.to_numeric(df_q[col], errors='coerce')
    df_q.loc[:, ['symbol'] + info_cols].to_excel('info.xlsx', index=None)
    quote_cols = ['symbol', 'tick', 'volume', 'askPrice', 'bidPrice', 'lastPrice', 'netChgToday', 'openPrice', 'previousClose',
                'askSize', 'bidSize', 'lastTime', 'pctChgToday', 'peRatio',
                'yearHighDate', 'yearHighPrice', 'yearLowDate', 'yearLowPrice', 'earningQtrReportDate']
    quote_cols = [c for c in quote_cols if c in df_q.columns]
    df_q[quote_cols].to_excel('quotes.xlsx', index=None)

def wait_for_all_files_to_be_updated(dir_list, symlist, max_file_age, logger):
    logger.info(f'wait_for_all_files_to_be_updated on {len(symlist)} symbols, max_file_age={max_file_age}')
    while True:
        stale_count = 0
        for _dir in dir_list:
            all_files = [os.path.join(_dir, symbol) for symbol in symlist]
            now = time.time()
            fresh_files = [f for f in all_files if os.path.exists(f) and now - os.path.getmtime(f) <= max_file_age]
            diff_count = len(all_files) - len(fresh_files)
            stale_count += diff_count
            if diff_count > 0:
                awhile = 5
                logger.debug(f'Sleep {awhile} seconds for {stale_count} stale files.')
                time.sleep(awhile)
        if stale_count == 0:
            logger.info(f'Done waiting.')
            return

def get_symbol_list(symbol_file):
    if os.path.exists(symbol_file):
        with open(symbol_file) as fo:
            return [_.rstrip() for _ in fo]
    return []

def main(symbol_file, logger, chain_dir='chain', max_file_age=180):
    quotes_dir = 'quotes'
    report_dir = 'data'
    self = OptionFinder(logger, chain_dir, quotes_dir, report_dir, max_file_age)
    log = self.logger
    while True:
        symlist = get_symbol_list(symbol_file)
        if len(symlist) == 0:
            time.sleep(2)
            continue
        wait_for_all_files_to_be_updated([chain_dir, quotes_dir], symlist, max_file_age, logger)
        chain_files = [os.path.join(chain_dir, symbol) for symbol in symlist]
        quote_files = [os.path.join(quotes_dir, symbol) for symbol in symlist]
        mtime_changed = 0
        for f in chain_files + quote_files:
            mtime = os.path.getmtime(f)
            if self.last_mtime.get(f) != mtime:
                mtime_changed += 1
                self.last_mtime[f] = mtime
        if mtime_changed != 0:
            logger.info(f'main: {mtime_changed} files changed.')
            #time.sleep(5)
            #continue
        self.process_data(symlist)

if __name__ == '__main__':
    log_file = os.path.join(os.path.dirname(sys.argv[0]), 'logs', os.path.basename(sys.argv[0]).replace('.py', '') + '.log')
    print('Logger:', log_file)
    sys.stdout.flush()
    symbol_file = sys.argv[1]
    logger = get_rotating_logger("", log_file)
    main(symbol_file, logger, chain_dir='chain', max_file_age=300)
