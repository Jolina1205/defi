# We can fetch only these endpoints
# For other end point there occurs an error that : This endpoint doesn't exist


import duckdb
import requests
import pandas as pd

api_endpoints = [
    "https://api.llama.fi/protocols",
    "https://api.llama.fi/v2/chains",
    "https://api.llama.fi/v2/historicalChainTvl",
    "https://api.llama.fi/overview/dexs",
    "https://api.llama.fi/overview/options",
    "https://api.llama.fi/overview/fees"
    
]

# Connect to an in-memory database
con = duckdb.connect(":memory:")

def fetch_and_transform(url, table_name):
    # Fetch data as JSON
    response = requests.get(url)
    data = response.json()

    # Check if data is a list for normalization
    if isinstance(data, list):
        df = pd.json_normalize(data)
    else:
        df = pd.DataFrame([data]) # Wrap single object in DataFrame

    # Convert complex data types to string to avoid casting issues
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)

    # Register DataFrame as a table in DuckDB
    con.register(table_name, df)
    
    # Print the DataFrame in a tabular form
    print(f"Data for {table_name}:")
    print(df)
    print("\n")

for url in api_endpoints:
    table_name = url.split("/")[-1] # Extract table name from URL
    fetch_and_transform(url, table_name)
