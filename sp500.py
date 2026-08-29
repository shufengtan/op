#!/usr/bin/env python
import bs4
import requests
import pandas

def get_sp500_symbols(url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:145.0) Gecko/20100101 Firefox/145.0'}
    response = requests.get(url, headers=headers)
    soup = bs4.BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    lol = []
    for row in table.find_all('tr'):
        all_td = row.find_all('td')
        if len(all_td) >= 3:
            ticker = all_td[0].text.strip().replace('Template:', '')
            sector = all_td[2].text.strip()
            lol.append([ticker, sector])
    return pandas.DataFrame(lol, columns=['symbol', 'sector'])

if __name__ == '__main__':
    df = get_sp500_symbols()
    print(df.groupby('sector').count().sort_values(by='symbol', ascending=False))
    if df.shape[0] >= 500:
        import os
        csv_file = os.path.expanduser('~/lab/components/sp500.csv')
        df.to_csv(csv_file, index=None)
        print(f'Updated {csv_file}')
