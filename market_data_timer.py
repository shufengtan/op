import os
import time
from datetime import timedelta
import pandas as pd
from option_expiration_dates import get_option_expiration_dates

def wait_till_market_open(logger):
    while True:
        all_exp_dates = get_option_expiration_dates(3)[0]
        now = pd.Timestamp.today()
        logger.debug(f'wait_till_market_open: next option expiration dates are {all_exp_dates}')
        if now.strftime('%m/%d/%Y') in all_exp_dates:
            hours = now - now.normalize()
            if hours > timedelta(hours=16.25):
                time_to_go = (now.normalize() + timedelta(hours=24+9.5) - now).total_seconds()
                logger.info(f'wait_till_market_open: sleep for {int(time_to_go/60)} minutes till the next market opening.')
                time.sleep(time_to_go)
                continue
            if hours < timedelta(hours=9.5):
                time_to_go = (timedelta(hours=9.5) - hours).total_seconds()
                logger.info(f'wait_till_market_open: sleep for {int(time_to_go/60)} minutes till the market opening.')
                time.sleep(time_to_go)
                continue
        else:
            next_market_open = pd.Timestamp(all_exp_dates[0]) + timedelta(hours=9.5)
            time_to_go = (next_market_open - now).total_seconds()
            logger.info(f'wait_till_market_open: sleep for {int(time_to_go/60)} minutes till market opening on {next_market_open.strftime("%F %T")}')
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