import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay, BQuarterEnd, BMonthEnd

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

if __name__ == '__main__':
    expDt_list, expDt_list2 = get_option_expiration_dates()
    for d1, d2 in zip(expDt_list, expDt_list2):
        print(d1, d2)