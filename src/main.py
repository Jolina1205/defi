# This is the code we got from repo



# -*- coding: utf-8 -*-
"""
DeFi Llama API - Python Tutorial

@author: AdamGetbags
"""

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

# Fetch protocol data by slug
slug = protocols.json()[20]['slug']
oneProtocol = requests.get(baseUrl + '/protocol/' + slug)

# Fetch historical chain tvl data
historicTVL = requests.get(baseUrl + '/v2/historicalChainTvl')

# Fetch chain name
chain = protocolData.chain[5]
historicChainTVL = requests.get(baseUrl + '/v2/historicalChainTvl/' + chain)
historicChainTVLData = pd.DataFrame.from_dict(historicChainTVL.json())

# Fetch simple tvl data
slug = protocols.json()[20]['slug']
simpleTVL = requests.get(baseUrl + '/tvl/' + slug)

# Fetch all chains tvl
allTVL = requests.get(baseUrl + '/v2/chains')
allTVLData = pd.DataFrame.from_dict(allTVL.json())

# Set coins base url
coinsUrl = 'https://coins.llama.fi'

# Input data
chainName = 'arbitrum'
contractAddress = '0x6C2C06790b3E3E3c38e12Ee22F8183b37a13EE55'
coins = chainName + ':' + contractAddress

# Get current price data
coinPrice = requests.get(coinsUrl + '/prices/current/' + coins)

# Sample address from docs
coins2= 'ethereum:0x69b4B4390Bd1f0aE84E090Fe8af7AbAd2d95Cc8C'

# Timestamp data
ts = str(1676000000)

# Get cross-sectional historical price data
historicPrice = requests.get(
    coinsUrl + '/prices/historical/' + ts + '/' + coins)

# Sample date
print(datetime.datetime.fromtimestamp(1672560000))

# Price chart data
chartData = requests.get(coinsUrl + '/chart/' + coins +
                         '?start=1672560000' +
                         '&span=10' +
                         '&period=1d' +
                         '&searchWidth=600')

# Data may truncate with span // increase searchWidth if data is truncated
chartData = requests.get(coinsUrl + '/chart/' + coins +
                         '?start=1672560000' +
                         '&span=100' +
                         '&period=1d' +
                         '&searchWidth=600')

# Get to the price data
priceData = pd.DataFrame.from_dict(
    chartData.json()['coins'][
        list(chartData.json()['coins'].keys())[0]]['prices'])

# Percentage change data
pctChgData = requests.get(coinsUrl + '/percentage/' + coins +
                           '?timestamp=1672560000' +
                           '&lookForward=false' +
                           '&period=4w')

tsNow = str(math.floor(time.time()))
closestBlock = requests.get(coinsUrl + '/block/' + chainName + '/' + tsNow)

# Bridges base url
bridgesBaseUrl = 'https://bridges.llama.fi'

# List all bridges
bridges = requests.get(bridgesBaseUrl + '/bridges/' + '?includeChains=true')

# Bridge ID
bridgeID = str(bridges.json()['bridges'][1]['id'])

# Bridge summary
bridgeSummary = requests.get(bridgesBaseUrl + '/bridge/' + bridgeID)

# Bridge / chain volume
bridgeVolume = requests.get(bridgesBaseUrl + '/bridgevolume/' +
                             chainName +
                             '?id=2')

# 24hr bridge stats
dailyBridgeStats = requests.get(bridgesBaseUrl + '/bridgedaystats/' +
                                ts + '/' +
                                'ethereum' +
                                '?id=5')

# Transactions by bridge, address

bridgeTransactions = requests.get(bridgesBaseUrl + '/transactions/' +
                                  '1' + '?' +
                                  'starttimestamp=1667260800' +
                                  '&endtimestamp=1667347200' +
                                  '&sourcechain=Polygon'
                                  '&address=' + coins2 +
                                  '&limit=200')

# Dex overview
dexOverview = requests.get(baseUrl + '/overview/dexs' +
                           '?excludeTotalDataChart=false' +
                           '&excludeTotalDataChartBreakdown=false' +
                           '&dataType=dailyVolume')

# Dex overview by chain
dexOverviewByChain = requests.get(baseUrl + '/overview/dexs/' + chainName +
                                  '?excludeTotalDataChart=false' +
                                  '&excludeTotalDataChartBreakdown=false' +
                                  '&dataType=dailyVolume')

# Options dex overview
optionsDexData = requests.get(baseUrl + '/overview/options/' +
                               '?excludeTotalDataChart=false' +
                               '&excludeTotalDataChartBreakdown=false' +
                               '&dataType=dailyPremiumVolume')

# Options dex by chain
optionsDexByChain = requests.get(baseUrl + '/overview/options/' + chainName +
                                  '/?excludeTotalDataChart=false' +
                                  '&excludeTotalDataChartBreakdown=false' +
                                  '&dataType=dailyPremiumVolume')

optionsProtocol = optionsDexByChain.json()['protocols'][0]['module']

# Options summary by option dex
optionSummary = requests.get(baseUrl + '/summary/options/' + optionsProtocol +
                              '/?dataType=dailyPremiumVolume')

# Stablecoins endpoints url
stablecoinsUrl = 'https://stablecoins.llama.fi'

# Get stablecoins data
stablecoins = requests.get(stablecoinsUrl + '/stablecoins/' +
                            '?includePrices=true')

stblCoinID = stablecoins.json()['peggedAssets'][0]['id']

# Get stablecoin chart data
stblCoinChart = requests.get(stablecoinsUrl + '/stablecoincharts/all' +
                              '?stablecoin=' + stblCoinID)

# Get stablecoin chart data by chain
stblCoinChartByChain = requests.get(stablecoinsUrl + '/stablecoincharts/' +
                                     chainName +
                                     '?stablecoin=' + stblCoinID)

# Get historic mktcap chain distribution of stablecoin
stblCoinHistory = requests.get(stablecoinsUrl + '/stablecoin/' +
                                 stblCoinID)

# Stablecoin chain data
stblCoinChains = requests.get(stablecoinsUrl + '/stablecoinchains')

# Historical stablecoin price data
stblCoinPrices = requests.get(stablecoinsUrl + '/stablecoinprices')

# Yields url
yieldsUrl = 'https://yields.llama.fi'

# Pool data
poolData = requests.get(yieldsUrl + '/pools')

poolID = poolData.json()['data'][1]['pool']

# APY and TVL data for pool
poolData = requests.get(yieldsUrl + '/chart/' + poolID)

decoderUrl = 'https://abi-decoder.llama.fi'

# Fetch signature endpoint
fetchSig = requests.get(decoderUrl + '/fetch/signature/' + '?functions=')
