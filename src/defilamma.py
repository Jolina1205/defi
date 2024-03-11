import logging
import requests
import json
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)


BASE_API_URL = "https://api.llama.fi"

URLS = {
    "dex": f"{BASE_API_URL}/overview/dexs",
    "dex_summary": f"{BASE_API_URL}/summary/dexs/",
    "protocols": f"{BASE_API_URL}/protocols",
}


def pull_data_by_api(api_url: str, params: dict | None = None):
    response = requests.get(api_url, params=params if params else None)
    response.raise_for_status()
    data = json.loads(response.text)
    logging.info(f"Successfully, Pulled data from {api_url}")
    return data


def defilamma_dex_pipeline(api_url: str):

    # [X] 1. API -> raw
    json_data = pull_data_by_api(api_url=api_url)

    # [X] 2. raw -> normalize(3N)
    if "protocols" in json_data:
        df_protocols = pd.DataFrame(json_data["protocols"])
        logging.info("Dataframe created from protocols")
        return df_protocols
    else:
        logging.info(
            "Error: No protocols found in json_data. check the log file in data"
        )
    # [ ] 3. notmalize -> DuckeDB


def defilamma_dex_historical_pipeline(api_url: str):

    params = {
        "excludeTotalDataChart": "false",
        "excludeTotalDataChartBreakdown": "false",
        "dataType": "dailyVolume",
    }

    # [ ] replace the data pull from csv with DuckDB
    df_dex = pd.read_csv("data/dex_df.csv")
    modules = df_dex["module"].to_list()
    modules = modules[0:2]

    # [X] 1. API -> raw
    dex_hist_list = []
    for m in modules:
        url = f"{api_url}{m}"
        logging.info(f"Pulling data from {url}")
        json_data = pull_data_by_api(api_url=url, params=params)
        dex_hist_list.append(json_data)

        # [X] 2. raw -> normalize(3N)
    clean_dex_hist_list = []
    for p in dex_hist_list:
        # data inside totalDataChartBreakdown: -> date | chain | protocol | value
        for d in p:
            if "totalDataChartBreakdown" in p:
                chart_bd = p["totalDataChartBreakdown"]
                for bd in chart_bd:
                    for k, v in bd[1].items():
                        for e, f in v.items():
                            clean_dex_hist_list.append(
                                {
                                    "chain": k,
                                    "protcol": e,
                                    "value": f,
                                    "date": bd[0],
                                    "dex_name": p["name"],
                                    "defillamaId": p["defillamaId"],
                                }
                            )
    # List of keys to remove
    keys_to_remove = [
        "totalDataChart",
        "totalDataChartBreakdown",
        "chains",
        "methodology",
    ]
    for key in keys_to_remove:
        for dex in dex_hist_list:
            if key in dex:
                logging.info(f"Removed {key} from dex_hist_list")
                del dex[key]

    df_dex_meta_data = pd.json_normalize(dex_hist_list)

    df_dex_hist_list = pd.DataFrame(clean_dex_hist_list)
    logging.info("Dataframe created from dex_hist_list")

    # [ ] 3. normalize -> DuckeDB
    df_dex_meta_data.to_csv("data/dex_meta_data.csv")
    df_dex_hist_list.to_csv("data/dex_hist_list.csv")
    return None


if __name__ == "__main__":

    dex_summary = defilamma_dex_historical_pipeline(URLS["dex_summary"])

    # 2262	ethereum	0x RFQ	2154034.7018919014	1707609600	0x	2116
    # 2263	polygon	0x RFQ	0.0	1707609600	0x	2116
