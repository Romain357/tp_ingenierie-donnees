import requests
import pandas as pd
import os

URL_STATIONS = "https://data.airpl.org/api/v1/mesure/station-list/"


def extraire_stations():
    url = URL_STATIONS
    all_results = []
    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data.get("results", []))
        url = data.get("next")
        print(f"{len(all_results)} stations récupérées...")

    df_stations = pd.DataFrame(all_results)

    os.makedirs("/tmp/data", exist_ok=True)
    df_stations.to_csv("/tmp/data/stations.csv", index=False, encoding="utf-8")
    return df_stations