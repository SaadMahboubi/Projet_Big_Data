from kafka import KafkaProducer
import json
import numpy as np
import time
import requests

p = KafkaProducer(bootstrap_servers=['Localhost:9092'])
urlCrypto = "https://cryptopanic.com/api/v1/posts/?auth_token=47148a380f771faca768a23729def10f987e93f7&currencies=BTC,ETH,XRP&filter=hot"
i=0
def make_requests(url):
  r = requests.get(url)
  if r.ok :
    print(f"status ok {r.status_code}")
    return r.json()
  else :
    print(f"errors status : {r.status_code}")


while True:
  #Connect CryptoPanic
  contenu = make_requests(urlCrypto)

  #print(contenu)
  resultats = contenu["results"]
  datacrypto= [{'currencies':x["currencies"], "title":x["title"],"published_at":x["published_at"],"votes":x["votes"], "url": x["url"]} for x in resultats]

  data = {'post': datacrypto}
  p.send('CryptoPanic', json.dumps(data).encode('utf-8'))
  print(data)
  p.flush()
  i += 1
  time.sleep(3600*24)
