import pandas as pd 
import numpy as np
import requests
import json
from datetime import datetime
import time
import math
import duckdb
from tabulate import tabulate


baseUrl = "https://yields.llama.fi"


#yield pools endpoint
def get_pool_data():
    pools = requests.get(baseUrl + '/pools')
    pool = json.loads(pools.text)
    
    if 'data' in pool:
        actual = pool['data']
    else:
        print("No 'data' key found in the first response.")
        actual = []
    
    pull_data=pd.json_normalize(actual)
    
    pull_data= pull_data.drop(['apyReward', 'rewardTokens','apyPct1D','apyPct7D','apyPct30D',
                            'stablecoin','poolMeta','mu','sigma','outlier','underlyingTokens',
                            'il7d','apyBase7d','apyMean30d','volumeUsd1d','volumeUsd7d',
                            'apyBaseInception'], axis=1)
    
    return pull_data

df=get_pool_data()
print(df)


# yield Chart
def get_chart_data():
    charts = requests.get(baseUrl + '/chart/747c1d2a-c668-4682-b9f9-296708a3dd90')
    chart_data = json.loads(charts.text)
    
    if 'data' in chart_data:
        urs = chart_data['data']
    else:
        print("No 'data' key found in the first response.")
        urs = []
    
    y_chart = pd.json_normalize(urs)
    y_chart= y_chart.drop(['apyReward', 'il7d','apyBase7d'], axis=1)
    return(y_chart)

df_2 = get_chart_data()
print(df_2)    


conn = duckdb.connect('defillama_database.duckdb')
conn.register('yield_pools', df )
conn.register('yield_chart', df_2 )


# Query the data from the DuckDB table
result = conn.execute("SELECT * FROM yield_pools ").fetchall()
result_2 = conn.execute("SELECT * FROM yield_chart").fetchall()


print(result)
print(result_2)

















