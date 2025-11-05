import requests
import os
import sys
import re
import time
from multiprocessing import Process
from subprocess import getoutput
import pandas as pd
import random
from glob import glob
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import BQuarterEnd
import logging
from logging.handlers import RotatingFileHandler

def get_rotating_logger(log_name, log_file):
    logger = logging.getLogger(log_name)
    handler = RotatingFileHandler(log_file, maxBytes=299792458, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.level = logging.INFO
    return logger

def is_good_friday(date_obj):
    if date_obj.strftime('%F') in '2026-04-03 2027-03-26 2028-04-14 2029-03-30 2030-04-19 2031-04-11 2032-03-26 2033-04-15 2034-04-07':
        return True
    elif date_obj.strftime('%F') > '2034-04-07':
        raise "is_good_friday() function is out of date."
    else:
        return False

def is_holiday(date_obj):
    if is_good_friday(date_obj):
        return 'GoodFriday'
    fixed_dates = ['01-01', '06-19', '07-04', '12-25']
    mmdd = date_obj.strftime('%m-%d')
    if mmdd in fixed_dates:
        return mmdd
    month = date_obj.month
    day = date_obj.day
    weekday = date_obj.weekday()
    if month == 1 and weekday == 0 and 15 <= day <= 21:
        return mmdd
    if month == 2 and weekday == 0 and 15 <= day <= 21:
        return mmdd
    if month == 5 and weekday == 0 and 25 <= day <= 31:
        return mmdd
    if month == 6 and ((day == 18 and weekday == 4) or (day == 20 and weekday == 0)):
        return mmdd
    if month == 7 and ((day == 3 and weekday == 4) or (day == 5 and weekday == 0)):
        return mmdd
    if month == 9 and weekday == 0 and  1 <= day <=  6:
        return mmdd
    if month == 11 and weekday == 3 and 22 <= day <= 28:
        return mmdd
    if month == 12 and ((day == 24 and weekday == 4) or (day == 26 and weekday == 0)):
        return mmdd
    return

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

def exp_date(d):
    return d.strftime('%m/%d/%Y')

def settlement_date_type(d, settlement_type):
    return d.strftime('%b %d %Y') + settlement_type

def get_regular_option_days(days=100):
    today = datetime.today()
    end_day = today + timedelta(days=days)
    option_days = pd.date_range(start=today, end=end_day, freq=BDay())
    option_day0 = option_days[0]
    res_days = []
    res_days2 = []
    for d in option_days:
        if is_holiday(d):
            continue
        if d.year - option_day0.year == 2:
            if d.month == 6 and d.day == 18 and d.weekday() == 3:
                #print(d.strftime('%F'), '18 before Juneteen')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|W'))
            elif d.month in [1, 12] and is_third_friday(d):
                #print(d.strftime('%F'), d.year)
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|W'))
            continue
        if d.month == 6 and d.day < 21:
            # third Friday special case
            if d.day == 18 and d.weekday() == 3:
                #print(d.strftime('%F'), 'Q 18 before Junteen')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|Q'))
            elif is_third_friday(d):
                #print(d.strftime('%F'), '')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, ''))
            continue
        elif d.month in [3, 9, 12] and is_third_friday(d):
            #print(d.strftime('%F'), '(Q)')
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, ''))
            continue
        if d.year == option_day0.year or (d.year == option_day0.year + 1 and d.month <= option_day0.month):
            if (d+timedelta(days=1)).month != d.month or (d.weekday() == 4 and (d+timedelta(days=3)).month != d.month):
                if d.month in [3, 6, 9, 12]:
                    #print(d.strftime('%F'), 'Q')
                    res_days.append(exp_date(d))
                    res_days2.append(settlement_date_type(d, '|Q'))
                else:
                    #print(d.strftime('%F'), 'W (ME)', d.year)
                    res_days.append(exp_date(d))
                    res_days2.append(settlement_date_type(d, '|W'))
                continue
        if is_third_friday(d):
            #print(d.strftime('%F'))
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, ''))
            continue
        nd = (d-option_day0).days
        if nd <= 42 and d.weekday() == 4:
            #print(d.strftime('%F'), 'W')
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, '|W'))
            continue
        if nd <= 14:
            #print(d.strftime('%F'), 'W')
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, '|W'))
            continue
    return res_days, res_days2

def get_leaps_option_days():
    today = datetime.today()
    start_day = today + timedelta(days=90)
    end_day = start_day + timedelta(days=365 + 182)
    option_days = pd.date_range(start=start_day, end=end_day, freq=BDay())
    option_day0 = option_days[0]
    res_days = []
    res_days2 = []
    for d in option_days:
        if is_holiday(d):
            continue
        if d.strftime('%F') == '2025-04-17':
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, ""))
            continue
        if d.year - option_day0.year == 2:
            if d.month == 6 and d.day == 18 and d.weekday() == 3:
                #print(d.strftime('%F'), '18 before Juneteen')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, ''))
            elif d.month in [1, 12] and is_third_friday(d):
                #print(d.strftime('%F'), d.year)
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, ''))
            continue
        if d.month == 6 and d.day < 21:
            # third Friday special case
            if d.day == 18 and d.weekday() == 3:
                #print(d.strftime('%F'), 'Q 18 before Junteen')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|Q'))
            elif is_third_friday(d):
                #print(d.strftime('%F'), '')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, ''))
            continue
        if (d+timedelta(days=1)).month != d.month or (d.weekday() == 4 and (d+timedelta(days=3)).month != d.month):
            if d.month in [3, 6, 9, 12] and (d - today).days <= 366:
                #print(d.strftime('%F'), 'Q')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|Q'))
            elif (d - today).days <= 240:
                #print(d.strftime('%F'), 'W')
                res_days.append(exp_date(d))
                res_days2.append(settlement_date_type(d, '|W'))
                continue
        if d.month in [1, 3, 6, 9, 12] and is_third_friday(d):
            #print(d.strftime('%F'))
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, ''))
            continue
        if ((d - today).days <= 240) and d.weekday() == 4:
            res_days.append(exp_date(d))
            res_days2.append(settlement_date_type(d, '|W'))
            #print(d.strftime('## %F'))
    return res_days, res_days2

class OptionChainDownloader(object):
    api_url = 'https://digital.fidelity.com/ftgw/digital/options-research/api'
    def __init__(self, chain_dir, quotes_dir, leaps_dir, cookie_file, logger, strikes):
        '''days: 
        '''
        self.chain_dir = chain_dir
        self.quotes_dir = quotes_dir
        self.leaps_dir = leaps_dir
        for d in (chain_dir, quotes_dir, leaps_dir):
            if not os.path.exists(d):
                os.mkdir(d)
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Priority': 'u=0, i'
        }   
        self.cookie_file = cookie_file
        self.cookie_time = 0
        self.logger = logger
        self.sym_proc = {}
        self.abort_signal_file = cookie_file + '.expired'
        self.strikes = strikes

    def read_cookie(self):
        if not os.path.exists(self.cookie_file) or os.path.getsize(self.cookie_file) == 0:
            return False
        _cookie_time = os.path.getmtime(self.cookie_file)
        if _cookie_time > self.cookie_time:
            with open(self.cookie_file) as fo:
                text = fo.read().rstrip().split('; ')
                cookie = dict([tuple(x.split('=', 1)) for x in text])
                self.cookie = cookie
                self.cookie_time = _cookie_time
        if os.path.exists(self.abort_signal_file) and os.path.getmtime(self.abort_signal_file) < _cookie_time:
            os.unlink(self.abort_signal_file)
        if os.path.exists(self.abort_signal_file):
            return False
        else:
            return True

    def get_url(self, url, retries=3):
        log = self.logger
        if not self.read_cookie():
            log.warning('get_url failed to read_cookie')
            return
        session = self.session
        session.cookies.update(self.cookie)
        error = None
        resp = None
        for attempt in range(1, retries+1):
            try:
                resp = session.get(url)
            except requests.exceptions.TooManyRedirects as e:
                error = e
                log.warning(f'get_url TooManyRedirects: {error}')
                break
            except ValueError as e:
                error = e
                log.warning(f'get_url ValueError: {error}')
                break
            except requests.exceptions.ConnectionError as e:
                log.warning(f'get_url ConnectionError: {e}. Retry in 0.5 seconds')
                time.sleep(0.5)
                continue
            if resp.status_code == 200:
                text = resp.text
                if text is not None and text.find('Log in to') >= 0:
                    log.warning('get_url error: login is required')
                    error = 'login is required'
                else:
                    return resp.text
            if resp.status_code == 500:
                log.warning(f'get_url http error {resp.status_code}: will create empty file.\n{resp.text}')
                return ''
            if resp.status_code == 403 and attempt <= retries:
                awhile = 2**attempt
                log.warning(f'get_url http status code {resp.status_code}. Retry in {awhile} seconds')
                time.sleep(awhile)
        if error is None and resp is not None:
            log.warning(f'get_url http error {resp.status_code}: {resp.text}')
            error = resp.status_code
        if error is not None:
            with open(self.abort_signal_file, 'w') as wfo:
                wfo.write(f'{error}')
        return

    def get_option_data(self, symbol, strikes, expiration_dates, settlement_types, save_dir):
        url = self.api_url + f'/slo-chain?&adjustedOptionsData=true'
        url += '&symbol=' + symbol
        url += f'&strikes={strikes}'
        url += '&expirationDates=' + ','.join(expiration_dates)
        url += '&settlementTypes=' + ','.join(settlement_types).replace(' ', '%20')
        resp_text = self.get_url(url)
        if resp_text is None or (resp_text != '' and resp_text.find('{"callsAndPuts":') < 0):
            self.logger.warning(f'get_option_data failed to get callsAndPuts data: {resp_text[:200].replace('\n', ' ') if resp_text else resp_text}')
            return
        if resp_text is not None:
            chain_file = os.path.join(save_dir, symbol)
            with open(chain_file, 'w') as wfo:
                wfo.write(resp_text)
            return resp_text

    def get_slo_chain_data(self, symbol, strikes=None):
        strikes = self.strikes if strikes is None else strikes
        expiration_dates, settlement_types = get_regular_option_days()
        return self.get_option_data(symbol, strikes, expiration_dates, settlement_types, self.chain_dir)

    def get_leaps_data(self, symbol, strikes='ALL'):
        expiration_dates, settlement_types = get_leaps_option_days()
        return self.get_option_data(symbol, strikes, expiration_dates, settlement_types, self.leaps_dir)

    def get_quotes(self, symbol):
        url = self.api_url + '/quotes?symbols=' + symbol
        resp_text = self.get_url(url)
        if resp_text is None or resp_text == '' or resp_text[0] != '{':
            self.logger.warning(f"get_quotes failed to get json data: {resp_text[:50] if resp_text else resp_text}")
            return
        if resp_text is not None:
            quotes_file = os.path.join(self.quotes_dir, symbol)
            with open(quotes_file, 'w') as wfo:
                wfo.write(resp_text)
            return resp_text

    def parallel_get_data(self, symbol_list, rps=1):
        wait_time = 1.0/rps if rps > 0 else 1.0
        for symbol in symbol_list:
            if os.path.exists(self.abort_signal_file):
                with open(self.abort_signal_file) as fo:
                    self.logger.warning(f'parallel_get_data detected abort file {self.abort_signal_file}: {fo.read()}')
                break
            for target in [self.get_slo_chain_data, self.get_quotes, self.get_leaps_data]:
                t0 = time.perf_counter()
                proc = Process(target=target, args=(symbol,))
                self.sym_proc[symbol] = proc
                proc.start()
                et = time.perf_counter() - t0
                if et < wait_time:
                    time.sleep(wait_time - et)
        return 3*len(symbol_list)

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
        self.logger.info(f'killed {killed} zombies, left {n_alive} children alive.')

    def download_option_chain(self, symbol_list, batch_size=10, rps=1):
        get_chain_file = lambda s: os.path.join(self.chain_dir, s)
        get_mtime = lambda s: os.path.getmtime(get_chain_file(s)) if os.path.exists(get_chain_file(s)) else 0
        symbol_list = sorted(symbol_list, key=get_mtime)
        dl_count = 0
        if batch_size == 0:
            cookie_expired = False
            for symbol in symbol_list:
                for target in [self.get_quotes, self.get_slo_chain_data, self.get_leaps_data]:
                    if target(symbol):
                        dl_count += 1
                        time.sleep(0.005+random.random()*0.005)
                    elif os.path.exists(self.abort_signal_file) and not self.read_cookie():
                        cookie_expired = True
                        break
                if cookie_expired:
                    break
            self.logger.info(f'download_option_chain fetched {dl_count} files sequentially.')
        else:
            for idx in range(0, len(symbol_list), batch_size):
                dl_count += self.parallel_get_data(symbol_list[idx:idx+batch_size], rps)
            self.logger.info(f'download_option_chain fetched {dl_count} files in parallel with batch size {batch_size}')
        return dl_count

    def save_option_data(self, symlist):
        from option_finder import OptionFinder
        finder1 = OptionFinder(self.logger, chain_dir=self.chain_dir)
        finder2 = OptionFinder(self.logger, chain_dir=self.leaps_dir)
        finder1.get_quote_df(symlist)
        finder2.last_price = finder1.last_price
        df1 = finder1.build_option_df(symlist)
        ts = time.strftime('%F-%H%M')
        if df1 is None:
            self.logger.warning(f'save_option_data failed to build regular option dataframe for {len(symlist)} symbols.')
        else:
            output_file1 = os.path.join(finder1.report_dir, f"{len(symlist)}_options~{ts}.csv")
            df1.to_csv(output_file1, index=None)
            self.logger.info(f'save_option_data wrote {output_file1}')
        df2 = finder2.build_option_df(symlist)
        if df2 is None:
            self.logger.warning(f'save_option_data failed to build leaps dataframe for {len(symlist)} symbols.')
        else:
            output_file2 = os.path.join(finder2.report_dir, f"{len(symlist)}_leaps~{ts}.csv")
            df2.to_csv(output_file2, index=None)
            self.logger.info(f'save_option_data wrote {output_file2}')

def main():
    chain_dir = 'chain'
    quotes_dir = 'quotes'
    leaps_dir = 'leaps'
    cookie_file = 'cookie.txt'
    log_file = os.path.join(os.path.dirname(sys.argv[0]), 'logs', os.path.basename(sys.argv[0]).replace('.py', '') + '.log')

    print('Logger:', log_file)
    sys.stdout.flush()
    logger = get_rotating_logger("", log_file)
    symbol_file = sys.argv[1]
    ocd = OptionChainDownloader(chain_dir, quotes_dir, leaps_dir, cookie_file, logger, strikes='ALL')
    batch_size = 0 # Download sequentially
    while True:
        if not os.path.exists(symbol_file):
            time.sleep(2)
            continue
        with open(symbol_file) as fo:
            symlist = [_.rstrip() for _ in fo]
        if len(symlist) == 0:
            time.sleep(2)
            continue
        if os.path.exists(ocd.abort_signal_file) and not ocd.read_cookie():
            time.sleep(1)
            if batch_size > 0:
                ocd.kill_zombies()
            continue
        now = datetime.now()
        seconds = now.hour*3600 + now.minute*60 + now.second
        market_open = 9*3600 + 30*60
        if seconds < market_open:
            time.sleep(market_open - seconds)
        else:
            market_close = 16*3600
            if seconds >= market_close:
                time.sleep(10)
                continue
        logger.info(f'BEGIN downloading {len(symlist)} symbols')
        dl_count = ocd.download_option_chain(symlist, batch_size=batch_size, rps=1)
        if dl_count > 0:
            leaps_mtimes = [os.path.getmtime(f) for f in glob(os.path.join(leaps_dir, '*')) if '.' not in f]
            if len(leaps_mtimes) > 0 and max(leaps_mtimes) - min(leaps_mtimes) < 300:
                ocd.save_option_data(symlist)
                awhile = 595 - max(leaps_mtimes) + min(leaps_mtimes)
                logger.info(f'FINISH downloading. Sleep {awhile + 5} seconds')
                time.sleep(5)
                if batch_size > 0:
                    ocd.kill_zombies()
                time.sleep(awhile)

if __name__ == '__main__':
    main()