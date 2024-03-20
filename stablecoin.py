import pandas as pd 
import numpy as np
import requests
import json
from datetime import datetime
import time
import math
import duckdb
from tabulate import tabulate

baseUrl = "https://stablecoins.llama.fi"



# stable coins 

necessary = ['id','name','symbol','pegMechanism']

def get_scoin_data():
    scoin = requests.get(baseUrl + '/stablecoins?includePrices=true')
    scoin = json.loads(scoin.text)
    
    if 'peggedAssets' in scoin:
        scoin = scoin['peggedAssets']
    else:
        print("No 'peggedAssets' key found in the first response.")
        scoin = []
    scoin_data=pd.json_normalize(scoin)
    scoin_data= scoin_data.filter(items=necessary)
    return scoin_data

df=get_scoin_data()
print(df)


# stable coins charts - all 

def get_sc_all_data():
    sc_all = requests.get(baseUrl + '/stablecoincharts/all?stablecoin=1')
    sc_all = json.loads(sc_all.text)
    sc_all_data=pd.json_normalize(sc_all)
    sc_all_data= sc_all_data.drop(['totalUnreleased.peggedUSD', 'totalMintedUSD.peggedUSD','totalBridgedToUSD.peggedUSD'], axis=1)
    return sc_all_data

df_all=get_sc_all_data()
print(df_all)


# stable coins charts chains(ethereum)

def get_sc_chain_data():
    sc_chain = requests.get(baseUrl + '/stablecoincharts/Ethereum?stablecoin=1')
    sc_chain = json.loads(sc_chain.text)
    sc_chain_data=pd.json_normalize(sc_chain)
    return sc_chain_data

df_chain=get_sc_chain_data()
print(df_chain)


# stable coin prices

def get_sc_price_data():
    sc_price = requests.get(baseUrl + '/stablecoinprices')
    sc_price = json.loads(sc_price.text)
    sc_price_data=pd.json_normalize(sc_price)
    return sc_price_data

df_price=get_sc_price_data()
print(df_price)


conn = duckdb.connect('defillama_database.duckdb')

conn.register('stable_coin', df )
conn.register('sc_all', df_all )
conn.register('sc_chain', df_chain )
conn.register('sc_price', df_price )




result = conn.execute("SELECT * FROM stable_coin").fetchall()
result_2 = conn.execute("SELECT * FROM sc_all").fetchall()
result_3 = conn.execute("SELECT * FROM sc_chain").fetchall()
result_4 = conn.execute("SELECT * FROM sc_price").fetchall()


print(result)
print(result_2)
print(result_3)
print(result_4)
