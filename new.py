import pandas as pd
import requests
import duckdb
import datetime
import math
import time
import json

# Establish connection to DuckDB
con = duckdb.connect(database=':memory:', read_only=False)

baseUrl = 'https://api.llama.fi'

# Fetch protocols data
protocols = requests.get(baseUrl + '/protocols')

# Convert protocols data to DataFrame
protocolData = pd.DataFrame.from_dict(protocols.json())

# Create table and insert data for protocolData
con.execute("""
    CREATE TABLE protocol_data (
        slug VARCHAR,
        name VARCHAR,
        chain VARCHAR
    )
""")
for index, row in protocolData.iterrows():
    con.execute(f"INSERT INTO protocol_data VALUES (?, ?, ?)", (row['slug'], row['name'], row['chain']))

# Example for creating a table for another endpoint
# Assuming the endpoint returns a nested JSON structure
historicTVL = requests.get(baseUrl + '/v2/historicalChainTvl')
historicTVLData = pd.json_normalize(historicTVL.json())

# Create table and insert data for historicTVLData
con.execute("""
    CREATE TABLE historic_tvl_data (
        date int,
        tvl float
    )
""")
for index, row in historicTVLData.iterrows():
    # Insert data into the table
    # Adjust the INSERT statement based on the actual structure of historicTVLData
    con.execute(f"INSERT INTO historic_tvl_data VALUES ( ?, ?)", (row['date'], row['tvl']))

# Create table and insert data for Chains
chains = requests.get(baseUrl + '/v2/chains')
chainsData = pd.json_normalize(chains.json())

con.execute("""
    CREATE TABLE chains_data (
        gecko_id varchar,
        tvl float,
        tokenSymbol varchar,
        cmcId int ,
        name varchar,
        chainId varchar
    )
""")
for index, row in chainsData.iterrows():
    # Insert data into the table
    # Adjust the INSERT statement based on the actual structure of historicTVLData
    con.execute(f"INSERT INTO chains_data VALUES ( ?, ?,?,?,?,?)", (row['gecko_id'], row['tvl'],row['tokenSymbol'],row['cmcId'],row['name'],row['chainId']))

#  This is the code for 3 tables only and there are total 37 tables#
