
from kafka import KafkaProducer
import json
import numpy as np
import time
import ccxt


#Connect binance
binance= ccxt.binance()
#ftx = ccxt.ftx()

p = KafkaProducer(bootstrap_servers=['Localhost:9092'])
i=0

while True:
    # Bitcoin
    btc_usdt_ohlcv = binance.fetch_ohlcv('BTC/USDT','1m',limit=1)
    # Ethereum
    eth_usdt_ohlcv = binance.fetch_ohlcv('ETH/USDT','1m',limit=1)
    # Ripple (XRP)
    xrp_usdt_ohlcv = binance.fetch_ohlcv('XRP/USDT','1m',limit=1)
    data = {'id' : i,
            'BTC': btc_usdt_ohlcv ,
            'ETH': eth_usdt_ohlcv,
            'XRP': xrp_usdt_ohlcv }
    p.send('Binance', json. dumps(data).encode('utf-8'))
    p.flush()
    i += 1
    print(data) 
    time.sleep(5)

