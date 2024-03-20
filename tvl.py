import pandas as pd 
import numpy as np
import requests
import json
from datetime import datetime
import time
import math
import duckdb
from tabulate import tabulate

baseUrl = "https://api.llama.fi"



# tvl protocol 

necessary = ['id','name','symbol','chain','audits','category','twitter','listedAt','slug','tvl']

def get_protocol_data():
    protocol = requests.get(baseUrl + '/protocols')
    protocol = json.loads(protocol.text)
    protocol_data=pd.json_normalize(protocol)
    protocol_data= protocol_data.filter(items=necessary)
    return protocol_data

df=get_protocol_data()
#print(df)



#tvl historical chain
def get_protocol_histchain_data():
    histchain = requests.get(baseUrl + '/v2/historicalChainTvl')
    histchain = json.loads(histchain.text)
    histchain_data=pd.json_normalize(histchain)
    return histchain_data

df_histchain=get_protocol_histchain_data()
#print(df_histchain)


# tvl historical chain ethereum

def get_eth_data():
    eth = requests.get(baseUrl + '/v2/historicalChainTvl/Ethereum')
    eth = json.loads(eth.text)
    eth_data=pd.json_normalize(eth)
    return eth_data

df_eth=get_eth_data()
print(df_eth)



# #tvl chains

def get_chain_data():
    chain = requests.get(baseUrl + '/v2/chains')
    chain = json.loads(chain.text)
    chain_data=pd.json_normalize(chain)
    return chain_data

df_chain=get_chain_data()
print(df_chain)



conn = duckdb.connect('defillama_database.duckdb')
conn.register('tvl_protocol', df )
conn.register('tvl_histchain', df_histchain )
conn.register('tvl_eth', df_eth )
conn.register('tvl_chain', df_chain )



# Query the data from the DuckDB table
result = conn.execute("SELECT * FROM tvl_protocol ").fetchall()
result_2 = conn.execute("SELECT * FROM tvl_histchain").fetchall()
result_3 = conn.execute("SELECT * FROM tvl_eth").fetchall()
result_4 = conn.execute("SELECT * FROM tvl_chain").fetchall()



print(result)
print(result_2)
print(result_3)
print(result_4)
















