from kafka import KafkaConsumer
import json
import pprint
from pymongo import MongoClient
import threading
import time
import datetime

client = MongoClient('localhost', 27017)
cCryptoPanic = KafkaConsumer('CryptoPanic', bootstrap_servers=['kafka:9093'], api_version=(2,6,0))
cBinance = KafkaConsumer('Binance', bootstrap_servers=['kafka:9093'], api_version=(2,6,0))

#Création de db
db = client.database

#Création de post 
postCryptoPanic = db.posts
postXRP = db.coursXRP
postBTC = db.coursBTC
postETH = db.coursETH


postXRP.drop()
postBTC.drop()
postETH.drop()
postCryptoPanic.drop()

def addDataBaseCrypto(msg):
    #récupération du message
    msg.offset
    post = json.loads(msg.value)

    #insértion dans un dataset
    for i in post["post"]:
        ts = time.mktime(datetime.datetime.strptime(i["published_at"], "%Y-%m-%dT%H:%M:%SZ").timetuple())
        if ts >= ((time.time())-(24*3600)):
            postCryptoPanic.insert_one(i).inserted_id

    print(db.list_collection_names())
    for postCrypto in postCryptoPanic.find():
        pprint.pprint(postCrypto)

    
    
def addDataBaseBinance(msg):
    #récupération du message
    msg.offset
    data = json.loads(msg.value)

    #insértion dans un dataset

    dataXRP = {"id" : data['id'], "XRP" : data["BTC"]}
    postXRP.insert_one(dataXRP).inserted_id

    dataBTC = {"id" : data['id'], "BTC" : data["BTC"]}
    postBTC.insert_one(dataBTC).inserted_id

    dataETH = {"id" : data['id'], "ETH" : data["ETH"]}
    postETH.insert_one(dataETH).inserted_id

    print(db.list_collection_names())
    for postxrp in postXRP.find():
        pprint.pprint(postxrp)

    print(db.list_collection_names())
    for postbtc in postBTC.find():
        pprint.pprint(postbtc)

    print(db.list_collection_names())
    for posteth in postETH.find():
        pprint.pprint(posteth)
    

def mainCryptoPanic():
    for msg1 in cCryptoPanic:
        addDataBaseCrypto(msg1)

def mainBinance():
    for msg2 in cBinance:
        addDataBaseBinance(msg2)

#Execution de deux fonction différente en même temps 
#Afin de pouvoir recevoir deux message en même temps
CryptoPanic_thread = threading.Thread(target=mainCryptoPanic)
Binance_thread = threading.Thread(target=mainBinance)

CryptoPanic_thread.start()
Binance_thread.start()

CryptoPanic_thread.join()
Binance_thread.join()