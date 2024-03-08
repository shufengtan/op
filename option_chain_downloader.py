import requests
import os
import re
import time
from multiprocessing import Process
from subprocess import getoutput
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import BQuarterEnd

def list_business_days(days=365):
    today = datetime.today()
    end_day = today + timedelta(days=days)
    return pd.date_range(start=today, end=end_day, freq=BDay())

def is_third_friday(date_obj):
    # Check if the day is a Friday (weekday index 4) and falls between the 15th and 21st
    return date_obj.weekday() == 4 and 15 <= date_obj.day <= 21

def is_last_business_day_of_quarter(date_obj):
    # Check if the day is the last business day of the quarter
    quarter_end = BQuarterEnd()
    return date_obj == quarter_end.rollforward(date_obj)

def expiration_date_type(date_obj):
    if is_last_business_day_of_quarter(date_obj):
        return '|Q'
    if is_third_friday(date_obj):
        return ''
    return '|W'

def list_option_strike_days(days=365):
    business_days = list_business_days(days)
    mm_dd_yyyy = [d.strftime('%m/%d/%Y') for d in business_days]
    mmm_dd_yyyy_w = [d.strftime('%b %d %Y') + expiration_date_type(d) for d in business_days]
    return mm_dd_yyyy, mmm_dd_yyyy_w

class OptionChainDownloader(object):
    def __init__(self, data_dir, output_dir, cookie_file, days=100):
        self.data_dir = data_dir
        self.output_dir = output_dir
        for d in (data_dir, output_dir):
            if not os.path.exists(d):
                os.mkdir(d)
        self.cookie_file = cookie_file
        self.days = days
        self.sym_proc = {}

    def read_cookie(self):
        with open(self.cookie_file) as fo:
            text = fo.read().rstrip().split('; ')
            cookie = dict([tuple(x.split('=', 1)) for x in text])
            self.cookie = cookie
            return cookie

    def get_slo_chain_data(self, symbol):
        days = self.days
        expiration_dates, settlement_types = list_option_strike_days(days)
        url = 'https://digital.fidelity.com/ftgw/digital/options-research/api/slo-chain?strikes=All&adjustedOptionsData=true'
        url += '&symbol=' + symbol
        url += '&expirationDates=' + ','.join(expiration_dates)
        url += '&settlementTypes=' + ','.join(settlement_types).replace(' ', '%20')
        session = requests.Session()
        session.cookies.update(self.cookie)
        resp = session.get(url)
        if resp.status_code != 200:
            with open('COOKIE_EXPIRED', 'w') as wfo:
                wfo.write('\n')
            return
        data_file = os.path.join(self.data_dir, symbol)
        with open(data_file, 'w') as wfo:
            wfo.write(resp.text)
        return resp.text

    def parallel_get_data(self, symbol_list, rps=1):
        wait_time = 1.0/rps
        for symbol in symbol_list:
            if os.path.exists('COOKIE_EXPIRED'):
                print('COOKIE_EXPIRED')
                break
            t0 = time.perf_counter()
            proc = Process(target=self.get_slo_chain_data, args=(symbol,))
            self.sym_proc[symbol] = proc
            proc.start()
            et = time.perf_counter() - t0
            if et >= wait_time:
                print(f'.', end=' ')
            else:
                time.sleep(wait_time - et)
        return

    def count_zombies(self):
        return len([x for x in getoutput(f'/usr/bin/ps --ppid {os.getpid()} -oargs').splitlines() if x.find('<defunct>') > 0])

    def kill_zombies(self):
        symbol_list = list(self.sym_proc.keys())
        n_alive = 0
        killed = 0
        for symbol in symbol_list:
            proc = self.sym_proc[symbol]
            if proc.is_alive():
                n_alive += 1
            elif proc.exitcode is not None:
                proc.join(0.1)
                proc.close()
                killed += 1
                self.sym_proc.pop(symbol)
        print(f'killed {killed} zombies, left {n_alive} children alive.')

    def create_option_chain_df(self, symbol):
        data_file = os.path.join(self.data_dir, symbol)
        with open(data_file) as fo:
            option_chains = json.load(fo)
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
        for col in ['callSelection', 'putSelection', 'contractType', 'expirationDate', 'optionPeriodicity', 'settlementType']:
            df[col] = df[col].astype(str)
        for col in ['strike', 'callLast', 'callChange', 'callBid', 'callBidSize', 'callAsk',
           'callAskSize', 'callVolume', 'callOpenInterest',
           'callTimeValue', 'callImpliedVolatility', 'callIntrinsicValue',
           'callDelta', 'callGamma', 'callTheta', 'callVega', 'callRho', 'putLast',
           'putChange', 'putBid', 'putBidSize', 'putAsk', 'putAskSize',
           'putVolume', 'putOpenInterest', 'putTimeValue',
           'putImpliedVolatility', 'putIntrinsicValue', 'putDelta', 'putGamma',
           'putTheta', 'putVega', 'putRho',
           'daysToExpiration']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def download_option_chain(self, symbol_list, days=120, batch_size=10, rps=1):
        for idx in range(0, len(symbol_list), batch_size):
            self.parallel_get_data(symbol_list[idx:idx+batch_size], rps)

if __name__ == '__main__':
    data_dir = 'json'
    output_dir = 'op'
    cookie_file = 'slo-chain-cookie.txt'
    ocd = OptionChainDownloader(data_dir, output_dir, cookie_file, days=100)
    cookie = ocd.read_cookie()
    symbol_list = 'QQQ SPY NVDA MSFT META'.split(' ')
    ocd.download_option_chain(symbol_list, 120, 10, rps=10)
