url = 'https://api.exchange.coinbase.com/products/{}/candles?granularity={}&start={}&end={}'

import numpy as np
import pandas as pd
import requests
import json
import time
import datetime


ctime = lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%dT%H:%M:%SZ')
dtime = lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S')

def FixDate(f):
    def Handle(*a, **b):
        df = f(*a, **b)
        df['Time'] = list(map(dtime, df['Time']))
        return df[::-1]
    return Handle

@FixDate
def FetchCryptoData(ticker, gran=60, fetches=10):
    T1 = int(time.time())
    delta = gran*300
    T0 = T1 - delta

    data = []
    for f in range(fetches):
        XT0 = ctime(T0)
        XT1 = ctime(T1)
        resp = requests.get(url.format(ticker, gran, XT0, XT1)).json()
        data += resp
        print("Fetches: ", f+1)
        time.sleep(1.4)
        T0 -= delta
        T1 -= delta

    return pd.DataFrame(data, columns=['Time','Low','High','Open','Close','Volume'])


dataset = FetchCryptoData('BTC-USD', fetches=6)
dataset.to_csv('BitcoinData.csv')
