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
from pandas.tseries.offsets import BDay, BQuarterEnd, BMonthEnd
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
    elif date_obj.strftime('%F') > '2035-02-28':
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
    return date_obj == BQuarterEnd().rollforward(date_obj)

def is_last_day_of_month(date_obj):
    return date_obj == BMonthEnd().rollforward(date_obj)

def expiration_date_type(date_obj):
    if is_last_business_day_of_quarter(date_obj):
        return '|Q'
    if is_third_friday(date_obj):
        return ''
    return '|W'

def exp_date(d):
    return d.strftime('%m/%d/%Y')

def settlement_date_type(d, exp_type):
    return d.strftime('%b %d %Y') + exp_type

def get_option_expiration_dates(days=548, start_date=None):
    today = pd.Timestamp.now().normalize()
    if today.weekday() > 4:
        today += timedelta(days=7-today.weekday())
    if start_date is None:
        start_date = today
    end_date = start_date + timedelta(days=days)
    option_dates = pd.date_range(start=start_date, end=end_date, freq=BDay())
    juneteens = [pd.to_datetime(f'{y}-06-19') for y in range(option_dates[0].year, option_dates[-1].year+1)]
    juneteens_on_friday = [_d - timedelta(days=_d.weekday()-3) for _d in juneteens if _d.weekday() in [4 ,5]]
    date0 = option_dates[0]
    res_dates = []
    res_dates2 = []
    for d_i in option_dates:
        if is_holiday(d_i):
            continue
        if d_i in juneteens_on_friday:
            res_dates.append(exp_date(d_i))
            res_dates2.append(settlement_date_type(d_i, ''))
            continue
        exp_type = None
        days_from_today = (d_i - today).days
        if days_from_today > 364:
            # only third Friday, all regular
            if not (d_i.month in [1, 3, 6, 9, 12] and is_third_friday(d_i)):
                continue
            exp_type = ''
        elif days_from_today > 182:
            # third Fridy every month + last trading day every quarter
            if not (is_third_friday(d_i) or (d_i.month in [3, 6, 9, 12] and is_last_business_day_of_quarter(d_i))):
                continue
        elif days_from_today > 56:
            # twice every month: third Friday + last trading day of month (also quarter)
            if not (is_third_friday(d_i) or is_last_day_of_month(d_i)):
                continue
        elif days_from_today > 14:
            # every week + last trading day of the month (also quarter)
            if not(d_i.weekday() == 4 or is_last_day_of_month(d_i)):
                continue
        res_dates.append(exp_date(d_i))
        res_dates2.append(settlement_date_type(d_i, exp_type or expiration_date_type(d_i)))
    return res_dates, res_dates2

class OptionChainDownloader(object):
    api_url = 'https://digital.fidelity.com/ftgw/digital/options-research/api'
    def __init__(self, chain_dir, quotes_dir, cookie_file, logger, strikes):
        '''days: 
        '''
        self.chain_dir = chain_dir
        self.quotes_dir = quotes_dir
        for _d in (chain_dir, quotes_dir):
            if _d is not None and not os.path.exists(_d):
                os.mkdir(_d)
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
        _symbol = symbol.replace('-', '/')
        url = self.api_url + f'/slo-chain?&adjustedOptionsData=true'
        url += '&symbol=' + _symbol
        url += f'&strikes={strikes}'
        url += '&expirationDates=' + ','.join(expiration_dates)
        url += '&settlementTypes=' + ','.join(settlement_types).replace(' ', '%20')
        resp_text = self.get_url(url)
        if resp_text is None or (resp_text != '' and resp_text.find('{"callsAndPuts":') < 0):
            self.logger.warning(f'get_option_data {_symbol} failed to get callsAndPuts data: {resp_text[:200].replace('\n', ' ') if resp_text else resp_text}')
            return
        if resp_text is not None:
            chain_file = os.path.join(save_dir, symbol.replace('/', '-'))
            with open(chain_file, 'w') as wfo:
                wfo.write(resp_text)
            return resp_text

    def get_slo_chain_data(self, symbol, strikes=None):
        strikes = self.strikes if strikes is None else strikes
        expiration_dates, settlement_types = get_option_expiration_dates()
        return self.get_option_data(symbol, strikes, expiration_dates, settlement_types, self.chain_dir)

    def get_quotes(self, symbol):
        _symbol = symbol.replace('-', '/')
        url = self.api_url + '/quotes?symbols=' + _symbol
        resp_text = self.get_url(url)
        if resp_text is None or resp_text == '' or resp_text[0] != '{':
            self.logger.warning(f"get_quotes {_symbol} failed to get json data: {resp_text[:50] if resp_text else resp_text}")
            return
        if resp_text is not None:
            quotes_file = os.path.join(self.quotes_dir, symbol.replace('/', '-'))
            with open(quotes_file, 'w') as wfo:
                wfo.write(resp_text)
            return resp_text

    def parallel_get_data(self, symbol_list, rps=1):
        target_list = []
        if self.chain_dir is not None:
            target_list.append(self.get_slo_chain_data)
        if self.quotes_dir is not None:
            target_list.append(self.get_quotes)
        if len(target_list) == 0:
            return 0
        wait_time = 1.0/rps if rps > 0 else 1.0
        dl_count = 0
        for symbol in symbol_list:
            if os.path.exists(self.abort_signal_file):
                with open(self.abort_signal_file) as fo:
                    self.logger.warning(f'parallel_get_data detected abort file {self.abort_signal_file}: {fo.read()}')
                break
            for target in target_list:
                t0 = time.perf_counter()
                proc = Process(target=target, args=(symbol,))
                self.sym_proc[symbol] = proc
                proc.start()
                et = time.perf_counter() - t0
                if et < wait_time:
                    time.sleep(wait_time - et)
                dl_count += 1
        return dl_count

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
        dl_count = 0
        cookie_expired = False
        if batch_size == 0:
            # In sequential mode, sort symbol by mtime so the oldest file will be updated first
            get_mtime = lambda s: (lambda _f: os.path.getmtime(_f) if os.path.exists(_f) else 0)(os.path.join(self.chain_dir, s))
            symbol_list = sorted(symbol_list, key=get_mtime)
            for symbol in symbol_list:
                for target in [self.get_slo_chain_data, self.get_quotes]:
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
            get_size = lambda s: (lambda _f: os.path.getsize(_f) if os.path.exists(_f) else 0)(os.path.join(self.chain_dir, s))
            symbol_list = sorted(symbol_list, key=get_size)
            for idx in range(0, len(symbol_list), batch_size):
                if os.path.exists(self.abort_signal_file) and not self.read_cookie():
                    self.logger.warning(f'download_option_chain found abort signal.')
                    break
                dl_count += self.parallel_get_data(symbol_list[idx:idx+batch_size], rps)
            self.logger.info(f'download_option_chain requested {dl_count} files in parallel with batch size {batch_size}')
        return dl_count

    def save_option_data(self, symlist):
        from option_finder import OptionFinder
        finder1 = OptionFinder(self.logger, chain_dir=self.chain_dir)
        finder1.get_quote_df(symlist)
        df1 = finder1.build_option_df(symlist)
        ts = time.strftime('%F-%H%M')
        if df1 is None:
            self.logger.warning(f'save_option_data failed to build regular option dataframe for {len(symlist)} symbols.')
        else:
            output_file1 = os.path.join(finder1.report_dir, f"{len(symlist)}_options~{ts}.csv")
            df1.to_csv(output_file1, index=None)
            self.logger.info(f'save_option_data wrote {output_file1}')

    def get_cookie_age(self):
        return time.time() - (os.path.getmtime(self.cookie_file) if os.path.exists(self.cookie_file) else 0)

def wait_till_market_open(logger):
    while True:
        all_exp_dates = get_option_expiration_dates(3)[0]
        now = pd.Timestamp.today()
        #print(now.strftime('%m/%d/%Y'), all_exp_dates)
        if now.strftime('%m/%d/%Y') in all_exp_dates:
            hours = now - now.normalize()
            if hours > timedelta(hours=16):
                time_to_go = (now.normalize() + timedelta(hours=24+9.5) - now).total_seconds()
                logger.info(f'Sleep for {int(time_to_go/60)} minutes till the next market opening.')
                time.sleep(time_to_go)
                continue
            if hours < timedelta(hours=9.5):
                time_to_go = (timedelta(hours=9.5) - hours).total_seconds()
                logger.info(f'Sleep for {int(time_to_go/60)} minutes till the market opening.')
                time.sleep(time_to_go)
                continue
        else:
            next_market_open = pd.Timestamp(all_exp_dates[0]) + timedelta(hours=9.5)
            time_to_go = (next_market_open - now).total_seconds()
            logger.info(f'Sleep for {int(time_to_go/60)} minutes till market opening on {next_market_open.strftime("%F %T")} {now.strftime("%F %T")}')
            time.sleep(time_to_go)
            continue
        break

def wait_to_open_symbol_file(symbol_file):
    while True:
        if not os.path.exists(symbol_file):
            time.sleep(2)
            continue
        with open(symbol_file) as fo:
            symlist = [_.rstrip() for _ in fo]
        if len(symlist) == 0:
            time.sleep(2)
            continue
        else:
            return symlist

def main(batch_size=5):
    '''set batch_size to 0 for sequential download'''
    chain_dir = 'chain'
    quotes_dir = 'quotes'
    cookie_file = 'cookie.txt'
    log_file = os.path.join(os.path.expanduser('~/logs'), os.path.basename(sys.argv[0]).replace('.py', '') + '.log')

    print('Logger:', log_file)
    sys.stdout.flush()
    logger = get_rotating_logger("", log_file)
    symbol_file = sys.argv[1]
    ocd = OptionChainDownloader(chain_dir, quotes_dir, cookie_file, logger, strikes='ALL')
    file_ripe_age = 10
    while True:
        if os.path.exists(ocd.abort_signal_file) and not ocd.read_cookie() or ocd.get_cookie_age() >= 3510:
            time.sleep(5)
            continue
        wait_till_market_open(logger)
        symlist = wait_to_open_symbol_file(symbol_file)
        wait_till_market_open(logger)
        logger.info(f'BEGIN downloading {len(symlist)} symbols')
        dl_count = ocd.download_option_chain(symlist, batch_size=batch_size, rps=5)
        if dl_count == 0:
            logger.warning(f'No file downloaded batch_size = {batch_size}')
            time.sleep(1)
        while True:
            all_mtimes = [(lambda x: os.path.getmtime(x) if os.path.exists(x) else 0)(os.path.join(chain_dir, s)) for s in symlist]
            max_mtime = max(all_mtimes)
            min_age = time.time() - max_mtime
            if min_age >= file_ripe_age:
                logger.info(f'Newest file is over {int(min_age)} seconds old, ripe age is {file_ripe_age}')
                break
            else:
                time.sleep(1)
        if batch_size > 0:
            ocd.kill_zombies()
        logger.info(f'END downloading. Cookie is {int(ocd.get_cookie_age())} seconds old.')
        time.sleep(5)

if __name__ == '__main__':
    main()
