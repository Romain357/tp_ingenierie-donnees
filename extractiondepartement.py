import requests
import pandas as pd
import os

URL_DEPARTEMENTS = "https://data.airpl.org/api/v1/mesure/departement/"


def extraire_stations():
    url = URL_DEPARTEMENTS
    all_results = []
    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        all_results.extend(data.get("results", []))
        url = data.get("next")
        print(f"{len(all_results)} départements récupérées...")

    df_departements = pd.DataFrame(all_results)

    os.makedirs("/tmp/data", exist_ok=True)
    df_departements.to_csv("/tmp/data/departements.csv", index=False, encoding="utf-8")
    return df_departements