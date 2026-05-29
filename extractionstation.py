import requests
import pandas as pd

URL_STATIONS = "https://data.airpl.org/api/v1/mesure/station-list/"


def extraire_stations():
    url = URL_STATIONS
    all_results = []

    while url:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = response.json()

        all_results.extend(data.get("results", []))

        # passe automatiquement à la page suivante
        url = data.get("next")

        print(f"{len(all_results)} lignes récupérées...")

    return pd.DataFrame(all_results)


df_stations = extraire_stations()

print(df_stations.shape)
print(df_stations.head())

df_stations.to_csv(
    "tmp/data/stations.csv",
    index=False,
    encoding="utf-8"
)
