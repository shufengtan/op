import requests
import os
import sys
import time
from multiprocessing import Process
from subprocess import getoutput
import pandas as pd
import random
from option_expiration_dates import get_option_expiration_dates
import logging
from logging.handlers import RotatingFileHandler

def get_rotating_logger(log_name, log_file):
    logger = logging.getLogger(log_name)
    handler = RotatingFileHandler(log_file, maxBytes=299792458, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(handler)
    logger.level = logging.INFO
    return logger

class FidelityOptionDataDownloader(object):
    # API samples as of May 13, 2026
    #          https://digital.fidelity.com/ftgw/digital/api-exp-options-research/api/slo-chain/v1?strikes=All&expirationDates=05%2F15%2F2026&settlementTypes=May%2015%202026%7CM&symbol=SPY&adjustedOptionsData=true
    #          https://digital.fidelity.com/ftgw/digital/api-exp-options-research/api/quotes/v1?symbol=SPY
    api_url = 'https://digital.fidelity.com/ftgw/digital/api-exp-options-research/api'
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
        url = self.api_url + f'/slo-chain/v1?adjustedOptionsData=true'
        url += '&symbol=' + _symbol
        url += f'&strikes={strikes}'
        url += '&expirationDates=' + ','.join(expiration_dates).replace('/', '%2F').replace(',', '%2C')
        url += '&settlementTypes=' + ','.join(settlement_types).replace(' ', '%20').replace('|', '%7C').replace(',', '%2C')
        print(len(expiration_dates), expiration_dates[-1], url)
        resp_text = self.get_url(url)
        if resp_text is None or (resp_text != '' and resp_text.find('{"callsAndPuts":') < 0):
            self.logger.warning(f'get_option_data {_symbol} failed to get callsAndPuts data: {resp_text[:200].replace('\n', ' ') if resp_text else resp_text}')
            return
        if resp_text is not None:
            chain_file = os.path.join(save_dir, symbol.replace('/', '-'))
            with open(chain_file, 'w') as wfo:
                wfo.write(resp_text)
            return resp_text

    def get_slo_chain_data(self, symbol, strikes=None, num_exp_dt=None):
        strikes = self.strikes if strikes is None else strikes
        expiration_dates, settlement_types = get_option_expiration_dates()
        if num_exp_dt is None:
            num_exp_dt = 25 if symbol == 'QQQ' else 20 if symbol == 'SPY' else -1
        return self.get_option_data(symbol, strikes, expiration_dates[:num_exp_dt], settlement_types[:num_exp_dt], self.chain_dir)

    def get_quotes(self, symbol):
        _symbol = symbol.replace('-', '/')
        # https://digital.fidelity.com/ftgw/digital/api-exp-options-research/api/quotes/v1?symbol=SPY
        url = self.api_url + '/quotes/v1?symbol=' + _symbol
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

def main(batch_size=5):
    '''set batch_size to 0 for sequential download'''
    from market_data_timer import wait_till_market_open, wait_to_open_symbol_file
    from ntfy import Ntfy
    chain_dir = os.path.expanduser('~/lab/chain')
    quotes_dir = os.path.expanduser('~/lab/quotes')
    cookie_file = os.path.expanduser('~/lab/cookie.txt')
    log_file = os.path.join(os.path.expanduser('~/logs'), os.path.basename(sys.argv[0]).replace('.py', '') + '.log')

    print('Logger:', log_file)
    sys.stdout.flush()
    logger = get_rotating_logger("", log_file)
    symbol_file = sys.argv[1]
    ntfy_topic = sys.argv[2]
    ntfyer = Ntfy(ntfy_topic)
    ocd = FidelityOptionDataDownloader(chain_dir, quotes_dir, cookie_file, logger, strikes='ALL')
    file_ripe_age = 10
    while True:
        if os.path.exists(ocd.abort_signal_file) and not ocd.read_cookie() or ocd.get_cookie_age() >= 3510:
            print('.', end='')
            time.sleep(5)
            continue
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
        cookie_age = int(ocd.get_cookie_age())
        if cookie_age > 3300:
            if time.time() - ntfyer.last_mesg_ts >= 300:
                ntfyer.send_alert(f"{cookie_age} seconds")
        logger.info(f'END downloading. Cookie is {cookie_age} seconds old.')
        time.sleep(5)

if __name__ == '__main__':
    main()
