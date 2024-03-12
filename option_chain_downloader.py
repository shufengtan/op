import requests
import os
import sys
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
    api_url = 'https://digital.fidelity.com/ftgw/digital/options-research/api'
    def __init__(self, chain_dir, quotes_dir, cookie_file, days=100):
        self.chain_dir = chain_dir
        self.quotes_dir = quotes_dir
        for d in (chain_dir, quotes_dir):
            if not os.path.exists(d):
                os.mkdir(d)
        self.cookie_file = cookie_file
        self.cookie_time = 0
        self.days = days
        self.sym_proc = {}
        self.abort_signal_file = cookie_file + '.expired'

    def read_cookie(self):
        _cookie_time = os.path.getmtime(self.cookie_file)
        if _cookie_time > self.cookie_time:
            with open(self.cookie_file) as fo:
                text = fo.read().rstrip().split('; ')
                cookie = dict([tuple(x.split('=', 1)) for x in text])
                self.cookie = cookie
                self.cookie_time = _cookie_time
            if os.path.exists(self.abort_signal_file) and os.path.getmtime(self.abort_signal_file) < _cookie_time:
                os.unlink(self.abort_signal_file)
            return True
        else:
            sys.stderr.write('x')
            return False

    def get_slo_chain_data(self, symbol):
        days = self.days
        expiration_dates, settlement_types = list_option_strike_days(days)
        url = self.api_url + '/slo-chain?strikes=All&adjustedOptionsData=true'
        url += '&symbol=' + symbol
        url += '&expirationDates=' + ','.join(expiration_dates)
        url += '&settlementTypes=' + ','.join(settlement_types).replace(' ', '%20')
        session = requests.Session()
        session.cookies.update(self.cookie)
        resp = session.get(url)
        if resp.status_code != 200:
            with open(self.abort_signal_file, 'w') as wfo:
                wfo.write(f'{resp.status_code}')
            return
        chain_file = os.path.join(self.chain_dir, symbol)
        with open(chain_file, 'w') as wfo:
            wfo.write(resp.text)
        return resp.text

    def get_quotes(self, symbol):
        url = self.api_url + '/quotes?symbols=' + symbol
        session = requests.Session()
        session.cookies.update(self.cookie)
        resp = session.get(url)
        if resp.status_code != 200:
            with open(self.abort_signal_file, 'w') as wfo:
                wfo.write(f'{resp.status_code}')
            return
        quotes_file = os.path.join(self.quotes_dir, symbol)
        with open(quotes_file, 'w') as wfo:
            wfo.write(resp.text)
        return resp.text

    def parallel_get_data(self, symbol_list, rps=1):
        wait_time = 1.0/rps
        for symbol in symbol_list:
            if os.path.exists(self.abort_signal_file):
                with open(self.abort_signal_file) as fo:
                    print(fo.read())
                break
            for target in [self.get_slo_chain_data, self.get_quotes]:
                t0 = time.perf_counter()
                proc = Process(target=target, args=(symbol,))
                self.sym_proc[symbol] = proc
                proc.start()
                et = time.perf_counter() - t0
                if et >= wait_time:
                    sys.stderr.write('.')
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

    def download_option_chain(self, symbol_list, days=120, batch_size=10, rps=1):
        for idx in range(0, len(symbol_list), batch_size):
            self.parallel_get_data(symbol_list[idx:idx+batch_size], rps)

if __name__ == '__main__':
    chain_dir = 'chain'
    quotes_dir = 'quotes'
    cookie_file = 'cookie.txt'
    ocd = OptionChainDownloader(chain_dir, output_dir, cookie_file, days=100)
    cookie = ocd.read_cookie()
    symbol_list = 'QQQ SPY NVDA MSFT META'.split(' ')
    ocd.download_option_chain(symbol_list, 120, 10, rps=10)
