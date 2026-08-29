#!/usr/bin/env python

import pandas as pd
import requests
import io

def fetch_nasdaq_100():
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:145.0) Gecko/20100101 Firefox/145.0'}
    resp = requests.get("https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies", headers=headers)
    try:
        tables = pd.read_html(io.StringIO(resp.text))
        for table in tables:
            if "Ticker" in table.columns:
                nasdaq_100 = table[['Ticker', 'Company']]
                break
        else:
            raise Exception("Could not find NASDAQ 100 table on wikipedia")
        return nasdaq_100
    except Exception as e:
        print(f"Error fetch NASDAQ 100 list: {e}")
        return None

if __name__ == '__main__':
    df = fetch_nasdaq_100()
    if df.shape[0] >= 100:
        import os
        csv_file = os.path.expanduser('~/lab/components/nasdaq100.csv')
        df.to_csv(csv_file, index=None)
        print(f'Updated {csv_file}')
